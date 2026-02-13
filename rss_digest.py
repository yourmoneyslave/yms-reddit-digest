import os
import json
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote_plus

import feedparser

BASE_DIR = Path(__file__).resolve().parent
STATE_PATH = BASE_DIR / "state.json"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    return {}

def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")

def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def parse_entry_time(entry) -> float:
    # RSS provides published_parsed most of the time
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return time.mktime(entry.published_parsed)
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return time.mktime(entry.updated_parsed)
    return time.time()

def build_feeds() -> list[tuple[str, str]]:
    queries = [
        ("Findom general", "findom OR \"financial domination\""),
        ("Paypig", "paypig OR \"pay pig\""),
        ("Beginner findomme", "\"beginner findomme\" OR \"new findomme\" OR \"starting findom\""),
        ("Platforms", "\"findom platform\" OR loyalfans OR fansly OR onlyfans"),
        ("TeamViewer", "teamviewer AND (findom OR femdom)"),
        ("Telegram", "findom telegram OR \"findom telegram group\""),
        ("Femdom in media", "\"femdom movies\" OR \"femdom tv\" OR \"mainstream femdom\""),
        ("Manga comics", "\"findom manga\" OR \"findom comic\" OR \"findom comics\" OR doujin"),
        ("Findom stories", "\"findom stories\" OR \"paypig stories\""),
        ("Findom forum", "\"findom forum\" OR \"financial domination forum\""),
    ]

    feeds = []
    for name, q in queries:
        encoded_q = quote_plus(q)
        url = f"https://www.reddit.com/search.rss?q={encoded_q}&sort=new&t=week"
        feeds.append((name, url))
    return feeds

def score_item(title: str) -> int:
    t = title.lower()
    score = 0
    hot_terms = [
        "how to", "beginner", "start", "advice", "help", "platform", "loyalfans",
        "paypig", "findom", "financial domination", "teamviewer", "telegram"
    ]
    for term in hot_terms:
        if term in t:
            score += 1

    # bonus for question style
    if "?" in title:
        score += 2
    return score

def send_email(subject: str, body_text: str, body_html: str) -> None:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pwd = os.getenv("SMTP_PASS")
    mail_to = os.getenv("MAIL_TO")
    mail_from = os.getenv("MAIL_FROM", user)

    if not all([host, user, pwd, mail_to, mail_from]):
        raise RuntimeError("Missing SMTP env vars")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = mail_to

    part1 = MIMEText(body_text, "plain", "utf-8")
    part2 = MIMEText(body_html, "html", "utf-8")
    msg.attach(part1)
    msg.attach(part2)

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.sendmail(mail_from, [mail_to], msg.as_string())


def run():
    state = load_state()
    seen = set(state.get("seen_ids", []))

    last_run_ts = float(state.get("last_run_utc_ts") or 0)
    backfill_hours = int(os.getenv("BACKFILL_HOURS", "168"))
    min_ts = max(
        last_run_ts,
        (utc_now() - timedelta(hours=backfill_hours)).timestamp()
    )

    max_items = int(os.getenv("MAX_ITEMS_PER_RUN", "120"))

    collected = []
    feeds = build_feeds()

    for feed_name, feed_url in feeds:
        d = feedparser.parse(feed_url)
        for entry in d.entries[:200]:
            eid = getattr(entry, "id", None) or getattr(entry, "link", None)
            if not eid or eid in seen:
                continue

            created_ts = parse_entry_time(entry)
            if created_ts < min_ts:
                continue

            title = (getattr(entry, "title", "") or "").strip()
            link = (getattr(entry, "link", "") or "").strip()
            if not title or not link:
                continue

            item = {
                "id": eid,
                "created_utc": created_ts,
                "created_iso": iso(created_ts),
                "feed": feed_name,
                "title": title,
                "url": link,
                "priority": score_item(title),
            }
            collected.append(item)
            seen.add(eid)

            if len(collected) >= max_items:
                break
        if len(collected) >= max_items:
            break

    collected.sort(key=lambda x: (x["priority"], x["created_utc"]), reverse=True)

    run_stamp = utc_now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"queue_{run_stamp}.json"
    out_path.write_text(json.dumps(collected, indent=2), encoding="utf-8")

    # build email body
    top = collected[:20]
    lines = []
    lines.append("YMS Reddit queue (manual actions)\n")
    lines.append("Suggested routine: pick 3 threads, reply with value, no selling.\n")
    lines.append(f"Items collected: {len(collected)}")
    lines.append(f"Saved: {out_path}\n")

    for i, it in enumerate(top, start=1):
        lines.append(f"{i}. [{it['feed']}] (prio {it['priority']}) {it['title']}")
        lines.append(f"   {it['url']}")
        lines.append("   Action: reply with 3 to 6 sentences, add your perspective, link only if truly relevant.\n")

    if len(collected) == 0:
        lines.append("No new items in the selected backfill window.")

subject = f"YMS Reddit queue: {len(collected)} new items"

# plain text
body_text = "\n".join(lines)

# html
def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def badge(feed_name: str) -> str:
    t = (feed_name or "").lower()
    if "findomme" in t or "platform" in t:
        return '<span class="b b-findomme">FINDOMME</span>'
    if "paypig" in t:
        return '<span class="b b-paypig">PAYPIG</span>'
    if "manga" in t or "media" in t:
        return '<span class="b b-media">MEDIA</span>'
    return '<span class="b b-general">GENERAL</span>'

rows = []
top = collected[:20]
for i, it in enumerate(top, start=1):
    title = esc(it.get("title", ""))
    url = it.get("url", "")
    feed = esc(it.get("feed", ""))
    prio = it.get("priority", 0)

    rows.append(f"""
      <tr>
        <td class="num">{i}</td>
        <td class="meta">
          {badge(it.get("feed",""))}
          <div class="feed">{feed}</div>
          <div class="prio">prio {prio}</div>
        </td>
        <td class="title">{title}</td>
        <td class="cta">
          <a class="btn" href="{url}">Open</a>
        </td>
      </tr>
    """)

body_html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{
      font-family: Arial, Helvetica, sans-serif;
      background: #f6f7f9;
      margin: 0;
      padding: 24px;
      color: #111;
    }}
    .card {{
      max-width: 900px;
      margin: 0 auto;
      background: #fff;
      border-radius: 12px;
      padding: 20px;
      border: 1px solid #e6e8ee;
    }}
    h1 {{
      font-size: 18px;
      margin: 0 0 8px 0;
    }}
    .sub {{
      font-size: 13px;
      color: #444;
      margin-bottom: 14px;
      line-height: 1.4;
    }}
    .kpis {{
      font-size: 13px;
      margin: 10px 0 16px 0;
      color: #222;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border-top: 1px solid #eef0f4;
      padding: 10px 8px;
      vertical-align: top;
    }}
    th {{
      text-align: left;
      color: #333;
      font-weight: 700;
      background: #fafbfc;
      border-top: 1px solid #e6e8ee;
    }}
    .num {{
      width: 36px;
      color: #666;
    }}
    .meta {{
      width: 170px;
    }}
    .feed {{
      font-weight: 700;
      margin-top: 6px;
    }}
    .prio {{
      color: #666;
      margin-top: 2px;
    }}
    .title {{
      line-height: 1.35;
    }}
    .cta {{
      width: 90px;
      text-align: right;
    }}
    .btn {{
      display: inline-block;
      padding: 8px 12px;
      border-radius: 10px;
      text-decoration: none;
      border: 1px solid #d6dbe6;
      font-weight: 700;
      color: #111;
      background: #fff;
    }}
    .b {{
      display: inline-block;
      font-size: 11px;
      padding: 3px 8px;
      border-radius: 999px;
      border: 1px solid #d6dbe6;
      font-weight: 700;
    }}
    .b-paypig {{ background: #fff7ed; border-color: #fed7aa; }}
    .b-findomme {{ background: #eef2ff; border-color: #c7d2fe; }}
    .b-media {{ background: #f0fdf4; border-color: #bbf7d0; }}
    .b-general {{ background: #f8fafc; border-color: #e2e8f0; }}
    .footer {{
      margin-top: 14px;
      font-size: 12px;
      color: #555;
      line-height: 1.4;
    }}
    .rule {{
      margin-top: 10px;
      padding: 10px 12px;
      background: #fafbfc;
      border: 1px solid #eef0f4;
      border-radius: 10px;
      font-size: 12px;
      color: #333;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>YMS Reddit queue</h1>
    <div class="sub">Pick 3 threads, reply with value, no selling. Usually no links in the comment.</div>

    <div class="kpis">
      <b>Items collected:</b> {len(collected)}<br>
      <b>Top shown:</b> {min(20, len(collected))}<br>
      <b>Saved file:</b> queue JSON committed in the repo output folder
    </div>

    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Type</th>
          <th>Title</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows) if rows else '<tr><td colspan="4">No new items in the selected window.</td></tr>'}
      </tbody>
    </table>

    <div class="rule">
      Reply formula: 1 line context, 2 to 4 practical points, 1 question at the end.
    </div>

    <div class="footer">
      Tip: focus on threads with low comment count and clear questions. Your profile and pinned posts do the linking.
    </div>
  </div>
</body>
</html>
"""

send_email(subject, body_text, body_html)

    # update state
    state["seen_ids"] = list(seen)[-10000:]
    state["last_run_utc_ts"] = time.time()
    save_state(state)

if __name__ == "__main__":
    run()
