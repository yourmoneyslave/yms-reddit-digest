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
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return time.mktime(entry.published_parsed)
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return time.mktime(entry.updated_parsed)
    return time.time()


def hours_ago(ts: float) -> int:
    return max(0, int((utc_now().timestamp() - ts) // 3600))


def build_feeds() -> list[tuple[str, str]]:
    queries = [
        ("Findom general", 'findom OR "financial domination"'),
        ("Paypig", 'paypig OR "pay pig"'),
        ("Beginner findomme", '"beginner findomme" OR "new findomme" OR "starting findom"'),
        ("Platforms", '"findom platform" OR loyalfans OR fansly OR onlyfans'),
        ("Telegram", 'findom telegram OR "findom telegram group"'),
        ("Findom stories", '"findom stories" OR "paypig stories"'),
        ("Findom forum", '"findom forum" OR "financial domination forum"'),
    ]

    feeds: list[tuple[str, str]] = []
    for name, q in queries:
        encoded_q = quote_plus(q)
        url = f"https://www.reddit.com/search.rss?q={encoded_q}&sort=new&t=week"
        feeds.append((name, url))
    return feeds


def classify(feed_name: str, title: str) -> tuple[str, list[str]]:
    """Classify into one of: FINDOMME, PAYPIG, GENERAL.

    Returns: (kind, reasons)
    """
    f = (feed_name or "").lower()
    t = (title or "").lower()
    reasons: list[str] = []

    # FINDOMME
    if any(k in f for k in ["findomme", "platform"]) or any(
        k in t for k in ["findomme", "domme", "loyalfans", "fansly", "onlyfans"]
    ):
        if any(k in f for k in ["findomme", "platform"]):
            reasons.append("feed-signal")
        for k in ["findomme", "domme", "loyalfans", "fansly", "onlyfans"]:
            if k in t:
                reasons.append(f"kw:{k}")
        return "FINDOMME", reasons[:6]

    # PAYPIG
    if "paypig" in f or any(k in t for k in ["paypig", "pay pig", "submissive", "slave"]):
        if "paypig" in f:
            reasons.append("feed-signal")
        for k in ["paypig", "pay pig", "submissive", "slave"]:
            if k in t:
                reasons.append(f"kw:{k}")
        return "PAYPIG", reasons[:6]

    return "GENERAL", ["fallback"]


# Scoring rules per category. Keep this small and opinionated, then iterate.
CATEGORY_SCORING: dict[str, dict] = {
    "FINDOMME": {
        "threshold": int(os.getenv("THRESHOLD_FINDOMME", "12")),
        "target_feeds": ["beginner findomme", "platforms"],
        "bonus_terms": [
            ("beginner", 4, "beginner"),
            ("starting", 3, "start"),
            ("how do i", 3, "how-do-i"),
            ("how to", 3, "how-to"),
            ("pricing", 3, "pricing"),
            ("rates", 2, "rates"),
            ("boundar", 3, "boundaries"),
            ("screen", 2, "screening"),
            ("platform", 3, "platform"),
            ("loyalfans", 3, "loyalfans"),
            ("fansly", 3, "fansly"),
            ("onlyfans", 3, "onlyfans"),
        ],
        "penalty_terms": [
            ("megathread", -5, "megathread"),
            ("weekly thread", -5, "megathread"),
            ("daily thread", -5, "megathread"),
            ("monthly thread", -5, "megathread"),
            ("looking for domme", -3, "solicitation"),
            ("dm me", -3, "solicitation"),
            ("telegram", -2, "telegram"),
        ],
    },
    "PAYPIG": {
        "threshold": int(os.getenv("THRESHOLD_PAYPIG", "11")),
        "target_feeds": ["paypig"],
        "bonus_terms": [
            ("addict", 4, "addiction"),
            ("can't stop", 4, "compulsion"),
            ("can’t stop", 4, "compulsion"),
            ("budget", 3, "budget"),
            ("debt", 3, "debt"),
            ("boundar", 3, "boundaries"),
            ("safe", 2, "safety"),
            ("rules", 2, "rules"),
            ("how to", 3, "how-to"),
            ("advice", 2, "advice"),
            ("help", 2, "help"),
        ],
        "penalty_terms": [
            ("megathread", -5, "megathread"),
            ("weekly thread", -5, "megathread"),
            ("daily thread", -5, "megathread"),
            ("monthly thread", -5, "megathread"),
            ("tribute", -2, "solicitation"),
            ("dm me", -3, "solicitation"),
        ],
    },
    "GENERAL": {
        "threshold": int(os.getenv("THRESHOLD_GENERAL", "10")),
        "target_feeds": ["findom general", "telegram", "findom stories", "findom forum"],
        "bonus_terms": [
            ("how to", 3, "how-to"),
            ("how do i", 3, "how-do-i"),
            ("beginner", 2, "beginner"),
            ("boundar", 3, "boundaries"),
            ("advice", 2, "advice"),
            ("help", 2, "help"),
        ],
        "penalty_terms": [
            ("megathread", -5, "megathread"),
            ("weekly thread", -5, "megathread"),
            ("daily thread", -5, "megathread"),
            ("monthly thread", -5, "megathread"),
        ],
    },
}


def compute_score(kind: str, feed_name: str, title: str, age_h: int) -> tuple[int, list[str]]:
    t = (title or "").lower()
    f = (feed_name or "").lower()
    score = 0
    reasons: list[str] = []

    # question bonus
    if "?" in (title or ""):
        score += 4
        reasons.append("question")

    rules = CATEGORY_SCORING.get(kind) or CATEGORY_SCORING["GENERAL"]

    # category bonuses
    for term, pts, label in rules.get("bonus_terms", []):
        if term in t:
            score += pts
            reasons.append(label)

    # feed relevance (category specific)
    target_feeds = [x.lower() for x in rules.get("target_feeds", [])]
    if any(tf in f for tf in target_feeds):
        score += 2
        reasons.append("target-feed")

    # category penalties
    for term, pts, label in rules.get("penalty_terms", []):
        if term in t:
            score += pts
            reasons.append(label)

    # age penalty
    if age_h <= 2:
        score += 3
        reasons.append("fresh")
    elif age_h <= 6:
        score += 2
        reasons.append("recent")
    elif age_h <= 12:
        score += 1
    elif age_h >= 48:
        score -= 3
        reasons.append("old")

    return score, reasons[:6]


def suggested_opening(kind: str, title: str) -> str:
    t = (title or "").lower()

    if kind == "FINDOMME":
        if "platform" in t or "loyalfans" in t or "onlyfans" in t or "fansly" in t:
            return "Platform choice matters less than positioning, boundaries, and consistency, especially at the beginning."
        if "attract" in t or "get paypigs" in t or "marketing" in t:
            return "Most beginners focus on promotion first, but what really converts is clarity, authority, and a repeatable structure."
        return "A lot of beginner dommes underestimate how much paypig psychology drives everything, not just content or pricing."

    if kind == "PAYPIG":
        if "addict" in t or "addiction" in t or "can’t stop" in t:
            return "If this feels compulsive rather than consensual fun, the first step is boundaries and a realistic plan, not shame."
        if "safe" in t or "boundar" in t:
            return "The safest way to approach this is to define boundaries first, then choose dynamics that respect them."
        return "Most beginners get stuck because they only see the fantasy part, but the real challenge is balance and structure."

    return "If you are new to this, focus on understanding the dynamics first, the rest becomes much clearer after that."


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

    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.sendmail(mail_from, [mail_to], msg.as_string())


def run():
    state = load_state()
    seen = set(state.get("seen_ids", []))

    last_run_ts = float(state.get("last_run_utc_ts") or 0)
    backfill_hours = int(os.getenv("BACKFILL_HOURS", "168"))
    min_ts = max(last_run_ts, (utc_now() - timedelta(hours=backfill_hours)).timestamp())

    max_items = int(os.getenv("MAX_ITEMS_PER_RUN", "120"))

    collected: list[dict] = []
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

            age_h = hours_ago(created_ts)
            kind, kind_reasons = classify(feed_name, title)
            score, score_reasons = compute_score(kind, feed_name, title, age_h)

            # category threshold gate (fewer items, higher precision)
            threshold = int((CATEGORY_SCORING.get(kind) or CATEGORY_SCORING["GENERAL"]).get("threshold", 10))
            if score < threshold:
                continue

            opening = suggested_opening(kind, title)

            why = {
                "kind": kind_reasons,
                "score": score_reasons,
                "threshold": threshold,
            }

            item = {
                "id": eid,
                "created_utc": created_ts,
                "created_iso": iso(created_ts),
                "age_hours": age_h,
                "feed": feed_name,
                "kind": kind,
                "title": title,
                "url": link,
                "score": score,
                "signals": score_reasons,
                "why": why,
                "opening": opening,
            }
            collected.append(item)
            seen.add(eid)

            if len(collected) >= max_items:
                break
        if len(collected) >= max_items:
            break

    collected.sort(key=lambda x: (x["score"], -x["age_hours"]), reverse=True)

    run_stamp = utc_now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"queue_{run_stamp}.json"
    out_path.write_text(json.dumps(collected, indent=2), encoding="utf-8")

    # split
    high_priority = [x for x in collected if x["score"] >= 14 and x["age_hours"] <= 12][:10]
    paypig = [x for x in collected if x["kind"] == "PAYPIG"][:10]
    findomme = [x for x in collected if x["kind"] == "FINDOMME"][:10]
    other = [x for x in collected if x["kind"] not in ["PAYPIG", "FINDOMME"]][:10]

    # plain text
    lines: list[str] = []
    lines.append("YMS Reddit queue (manual actions)")
    lines.append("")
    lines.append("Routine: pick 3 threads, reply with value, usually no links in the comment.")
    lines.append("")
    lines.append(f"Items collected: {len(collected)}")
    lines.append(f"Saved: {out_path}")
    lines.append("")

    def add_block(name: str, items: list[dict]):
        lines.append(name)
        lines.append("-" * len(name))
        if not items:
            lines.append("None")
            lines.append("")
            return
        for i, it in enumerate(items, start=1):
            why = it.get("why") or {}
            kind_why = ", ".join(why.get("kind", []) or [])
            score_why = ", ".join(why.get("score", []) or [])
            thr = why.get("threshold", "?")
            lines.append(
                f"{i}. [{it['kind']}] score {it['score']} (thr {thr}) age {it['age_hours']}h"
            )
            lines.append(f"   Why kind: {kind_why or '-'}")
            lines.append(f"   Why score: {score_why or '-'}")
            lines.append(f"   {it['title']}")
            lines.append(f"   {it['url']}")
            lines.append(f"   Opening: {it['opening']}")
            lines.append("")
        lines.append("")

    add_block("HIGH PRIORITY (do these first)", high_priority)
    add_block("PAYPIG LEADS", paypig)
    add_block("FINDOMME LEADS", findomme)
    add_block("OTHER", other)

    subject = f"YMS Reddit leads: {len(collected)} new items"
    body_text = "\n".join(lines)

    # HTML helpers
    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def badge(kind: str) -> str:
        if kind == "PAYPIG":
            return '<span class="b b-paypig">PAYPIG</span>'
        if kind == "FINDOMME":
            return '<span class="b b-findomme">FINDOMME</span>'
        return '<span class="b b-general">GENERAL</span>'

    def render_table(items: list[dict]) -> str:
        if not items:
            return '<div class="empty">No items.</div>'

        rows = []
        for i, it in enumerate(items, start=1):
            why = it.get("why") or {}
            kind_why = ", ".join(why.get("kind", []) or [])
            score_why = ", ".join(why.get("score", []) or [])
            thr = why.get("threshold", "?")
            rows.append(
                f"""
                <tr>
                  <td class="num">{i}</td>
                  <td class="meta">
                    {badge(it.get("kind","GENERAL"))}
                    <div class="feed">{esc(it.get("feed",""))}</div>
                    <div class="mini">score {it.get("score",0)} (thr {thr}) · {it.get("age_hours",0)}h</div>
                    <div class="mini"><b>Why kind:</b> {esc(kind_why or "-")}</div>
                    <div class="mini"><b>Why score:</b> {esc(score_why or "-")}</div>
                  </td>
                  <td class="title">
                    <div class="t">{esc(it.get("title",""))}</div>
                    <div class="opening"><b>Opening:</b> {esc(it.get("opening",""))}</div>
                  </td>
                  <td class="cta">
                    <a class="btn" href="{it.get("url","")}">Open</a>
                  </td>
                </tr>
                """
            )

        return f"""
        <table>
          <thead>
            <tr>
              <th>#</th>
              <th>Type</th>
              <th>Thread</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
        """

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
      max-width: 980px;
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
      line-height: 1.5;
    }}
    h2 {{
      font-size: 14px;
      margin: 18px 0 8px 0;
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
      width: 220px;
    }}
    .feed {{
      font-weight: 700;
      margin-top: 6px;
    }}
    .mini {{
      color: #666;
      margin-top: 2px;
      font-size: 12px;
      line-height: 1.35;
    }}
    .title {{
      line-height: 1.35;
    }}
    .t {{
      margin-bottom: 6px;
    }}
    .opening {{
      color: #333;
      font-size: 12px;
      background: #fafbfc;
      border: 1px solid #eef0f4;
      border-radius: 10px;
      padding: 8px 10px;
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
    .rule {{
      margin-top: 12px;
      padding: 10px 12px;
      background: #fafbfc;
      border: 1px solid #eef0f4;
      border-radius: 10px;
      font-size: 12px;
      color: #333;
      line-height: 1.4;
    }}
    .empty {{
      font-size: 13px;
      color: #555;
      padding: 10px 0;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>YMS Reddit leads</h1>
    <div class="sub">Pick 3 threads, reply with value, usually no links in the comment. Your profile and pinned posts do the linking.</div>

    <div class="kpis">
      <b>Items collected:</b> {len(collected)}<br>
      <b>Saved file:</b> queue JSON committed in the repo output folder
    </div>

    <div class="rule">
      Reply formula: 1 line context, 2 to 4 practical points, 1 question at the end.
    </div>

    <h2>HIGH PRIORITY</h2>
    {render_table(high_priority)}

    <h2>PAYPIG LEADS</h2>
    {render_table(paypig)}

    <h2>FINDOMME LEADS</h2>
    {render_table(findomme)}

    <h2>OTHER</h2>
    {render_table(other)}
  </div>
</body>
</html>
"""

    send_email(subject, body_text, body_html)

    state["seen_ids"] = list(seen)[-10000:]
    state["last_run_utc_ts"] = time.time()
    save_state(state)


if __name__ == "__main__":
    run()
