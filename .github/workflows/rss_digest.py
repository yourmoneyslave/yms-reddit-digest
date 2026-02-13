import os
import json
import time
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

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

def hours_ago(ts: float) -> int:
    return int((utc_now().timestamp() - ts) // 3600)

def parse_entry_time(entry) -> float:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return time.mktime(entry.published_parsed)
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return time.mktime(entry.updated_parsed)
    return time.time()

def mk_feed_url(query: str) -> str:
    q = quote(query)
    return f"https://www.reddit.com/search.rss?q={q}&sort=new&t=week"

def build_feeds() -> list[tuple[str, str, str]]:
    # (bucket, name, query)
    return [
        ("PAYPIG", "Beginner paypig", "paypig OR \"pay pig\" AND (beginner OR start OR advice OR help OR safe OR boundaries)"),
        ("PAYPIG", "Findom slave", "\"findom slave\" OR \"financial domination\" AND (rules OR boundaries OR addicted OR addiction OR shame OR guilt)"),
        ("PAYPIG", "TeamViewer", "teamviewer AND (findom OR femdom)"),

        ("FINDOMME", "Beginner findomme", "findomme AND (beginner OR start OR advice OR help OR \"how do\" OR mistakes)"),
        ("FINDOMME", "Platforms", "(loyalfans OR fansly OR onlyfans) AND (findom OR femdom OR findomme OR domme)"),
        ("FINDOMME", "Attract paypigs", "(attract OR marketing OR \"get subs\" OR \"find paypigs\") AND (findomme OR domme OR findom)"),

        ("CURIOSITY", "What is findom", "(findom OR \"financial domination\") AND (\"what is\" OR normal OR \"is it\" OR psychology)"),

        ("MEDIA", "Femdom movies tv", "\"femdom movies\" OR \"femdom tv\" OR \"mainstream femdom\""),
        ("MEDIA", "Manga comics", "\"findom manga\" OR \"findom comic\" OR \"findom comics\" OR doujin"),
        ("COMMUNITY", "Telegram", "findom telegram OR \"findom telegram group\" OR \"findom telegram channel\""),
        ("COMMUNITY", "Forum", "findom forum OR \"financial domination forum\""),
    ]

def classify_title(title: str) -> str:
    t = title.lower()
    if any(k in t for k in ["findomme", "domme", "loyalfans", "fansly", "onlyfans"]):
        return "FINDOMME"
    if any(k in t for k in ["paypig", "pay pig", "findom slave"]):
        return "PAYPIG"
    if any(k in t for k in ["movie", "movies", "tv", "television", "manga", "comic", "comics", "doujin"]):
        return "MEDIA"
    return "CURIOSITY"

def score_item(title: str, bucket: str) -> tuple[int, list[str]]:
    t = title.lower()
    score = 0
    reasons = []

    if "?" in title:
        score += 3
        reasons.append("question")

    for term, pts, label in [
        ("beginner", 2, "beginner"),
        ("start", 1, "start"),
        ("how to", 2, "how-to"),
        ("advice", 1, "advice"),
        ("help", 1, "help"),
        ("platform", 2, "platform"),
        ("loyalfans", 2, "loyalfans"),
        ("teamviewer", 2, "teamviewer"),
        ("telegram", 1, "telegram"),
        ("addict", 2, "addiction"),
        ("boundar", 2, "boundaries"),
    ]:
        if term in t:
            score += pts
            reasons.append(label)

    if bucket in ["PAYPIG", "FINDOMME"]:
        score += 1
        reasons.append("target")

    return score, reasons[:5]

def send_email(subject: str, body: str) -> None:
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ["SMTP_USER"]
    pwd = os.environ["SMTP_PASS"]
    mail_to = os.environ["MAIL_TO"]
    mail_from = os.environ.get("MAIL_FROM", user)

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
    backfill_hours = int(os.environ.get("BACKFILL_HOURS", "168"))
    min_ts = max(last_run_ts, (utc_now() - timedelta(hours=backfill_hours)).timestamp())

    max_items = int(os.environ.get("MAX_ITEMS_PER_RUN", "120"))

    collected = []
    feeds = build_feeds()

    for bucket, name, query in feeds:
        url = mk_feed_url(query)
        d = feedparser.parse(url)

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

            predicted = classify_title(title)
            score, reasons = score_item(title, bucket)

            item = {
                "id": eid,
                "created_utc": created_ts,
                "created_iso": iso(created_ts),
                "age_hours": hours_ago(created_ts),
                "bucket": bucket,
                "predicted": predicted,
                "feed": name,
                "title": title,
                "url": link,
                "score": score,
                "reasons": reasons,
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

    top = collected[:20]
    lines = []
    lines.append("YMS Reddit leads, pick 3 and reply with value. Usually no links in the comment.")
    lines.append("")
    lines.append(f"New items: {len(collected)}")
    lines.append(f"Saved queue: {out_path}")
    lines.append("")
    lines.append("HIGH PRIORITY")
    lines.append("")

    shown = 0
    for it in top:
        shown += 1
        lines.append(f"{shown}. {it['bucket']} lead, {it['feed']}")
        lines.append(f"   Age: {it['age_hours']}h, Score: {it['score']}, Signals: {', '.join(it['reasons'])}")
        lines.append(f"   Title: {it['title']}")
        lines.append(f"   Link:  {it['url']}")
        lines.append("   Suggested action: reply with 3 to 6 sentences, add 1 question at the end, avoid selling.")
        lines.append("")

    if len(collected) == 0:
        lines.append("No new items in the selected backfill window.")

    subject = f"YMS Reddit leads: {len(collected)} new items"
    send_email(subject, "\n".join(lines))

    state["seen_ids"] = list(seen)[-10000:]
    state["last_run_utc_ts"] = time.time()
    save_state(state)

if __name__ == "__main__":
    run()
