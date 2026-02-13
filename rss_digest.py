import os
import json
import time
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from pathlib import Path

import feedparser
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
STATE_PATH = BASE_DIR / "state.json"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

load_dotenv()

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
    # name, url
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
        url = f"https://www.reddit.com/search.rss?q={q}&sort=new&t=week"
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

def send_email(subject: str, body: str) -> None:
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    pwd = os.getenv("SMTP_PASS")
    mail_to = os.getenv("MAIL_TO")
    mail_from = os.getenv("MAIL_FROM", user)

    if not all([host, user, pwd, mail_to, mail_from]):
        raise RuntimeError("Missing SMTP env vars in .env")

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = mail_to

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
    send_email(subject, "\n".join(lines))

    # update state
    state["seen_ids"] = list(seen)[-10000:]
    state["last_run_utc_ts"] = time.time()
    save_state(state)

if __name__ == "__main__":
    run()
