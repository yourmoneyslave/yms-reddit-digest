import base64
import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import smtplib
from email.mime.text import MIMEText

ROOT = Path(__file__).resolve().parents[1]
KEYWORDS_CSV = ROOT / "data" / "keywords.csv"
PROMPT_FILE = ROOT / "prompts" / "template_v1.txt"
CONFIG_FILE = ROOT / "config.json"
LINKS_FILE = ROOT / "internal_links.json"

AUTO_SCHEDULE = os.getenv("AUTO_SCHEDULE", "false").lower() == "true"
SCHEDULE_HOUR_UTC = int(os.getenv("SCHEDULE_HOUR_UTC", "7"))


def load_config() -> dict:
    return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))


def load_links() -> dict:
    return json.loads(LINKS_FILE.read_text(encoding="utf-8"))


def slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")[:80] or "draft"


def read_first_todo_row() -> tuple[int, dict, list[dict]]:
    rows: list[dict] = []
    with KEYWORDS_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    for idx, r in enumerate(rows):
        if (r.get("status") or "").strip().lower() == "todo":
            return idx, r, rows

    return -1, {}, rows


def write_rows(rows: list[dict], fieldnames: list[str]) -> None:
    with KEYWORDS_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_output_text(resp: dict) -> str:
    parts: list[str] = []
    for item in resp.get("output", []):
        if item.get("type") != "message":
            continue
        for c in item.get("content", []):
            ctype = c.get("type")
            if ctype == "output_text":
                parts.append(c.get("text", ""))
            elif ctype == "refusal":
                parts.append(c.get("refusal", ""))
    return "".join(parts).strip()


def sanitize_content_html(html: str) -> str:
    if not html:
        return html

    # remove any accidental H1 to avoid double H1 in WP
    html = re.sub(r"<h1\b[^>]*>.*?</h1>", "", html, flags=re.IGNORECASE | re.DOTALL)

    # replace em dash if it slipped in
    html = html.replace("—", ", ")

    return html.strip()


def send_notification_email(post_id: int, title: str, cluster: str, wp_status: str, date_gmt: str | None):
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASS")

    mail_from = os.environ.get("MAIL_FROM") or smtp_user
    to_email = os.environ.get("MAIL_TO")

    if not all([smtp_host, smtp_user, smtp_password, to_email]):
        print("Email config missing, skipping notification.")
        return

    wp_base = os.environ["WP_BASE_URL"].rstrip("/")
    edit_link = f"{wp_base}/wp-admin/post.php?post={post_id}&action=edit"

    subject = f"[YMS] New Post: {title} ({wp_status})"
    when = f"\nScheduled (date_gmt): {date_gmt}\n" if date_gmt else "\n"

    body = f"""New post created.

Title: {title}
Cluster: {cluster}
Status: {wp_status}{when}
Edit link:
{edit_link}

--
YourMoneySlave SEO Bot
"""

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_email

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        print("Notification email sent successfully.")
    except Exception as e:
        print(f"Failed to send notification email: {e}")


def openai_call(payload: dict) -> dict:
    api_key = os.environ["OPENAI_API_KEY"]
    url = "https://api.openai.com/v1/responses"
    headers = {"Authorization": f"Bearer {api_key}"}

    r = requests.post(url, headers=headers, json=payload, timeout=90)
    if not r.ok:
        raise RuntimeError(f"OpenAI HTTP {r.status_code}: {r.text[:2000]}")
    return r.json()


def humanize_text(content_html: str) -> str:
    model = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
    max_out = int(os.environ.get("OPENAI_HUMANIZE_MAX_OUTPUT_TOKENS", "650"))
    bump_out = int(os.environ.get("OPENAI_HUMANIZE_MAX_OUTPUT_TOKENS_BUMP", "1100"))

    prompt = (
        "Rewrite the following article to sound more naturally human.\n"
        "Keep meaning identical.\n"
        "Do not change structure.\n"
        "Vary sentence rhythm.\n"
        "Reduce predictability.\n"
        "Avoid em dashes.\n"
        "Return only the rewritten HTML.\n\n"
        "ARTICLE:\n"
        f"{content_html}"
    )

    payload = {
        "model": model,
        "input": [
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]}
        ],
        "reasoning": {"effort": "low"},
        "max_output_tokens": max_out,
        "store": False,
    }

    data = openai_call(payload)

    # retry once if truncated
    if data.get("status") == "incomplete":
        details = data.get("incomplete_details") or {}
        if details.get("reason") == "max_output_tokens":
            payload["max_output_tokens"] = bump_out
            data = openai_call(payload)

    text = extract_output_text(data)
    return text.strip() if text else content_html


def inject_personal_block(content_html: str, keyword: str) -> str:
    variations = [
        f"<p><strong>My perspective:</strong> I used to misunderstand {keyword} when I first explored it. Over time I noticed that what really matters is consistency, not intensity.</p>",
        f"<p><strong>My perspective:</strong> With {keyword}, I have seen people focus on the wrong signals. The real difference is usually subtle.</p>",
        f"<p><strong>My perspective:</strong> Not everyone agrees on how {keyword} should work. From what I have observed, clarity beats drama every time.</p>",
    ]

    block = variations[hash(keyword) % len(variations)]

    if "<h2>FAQ</h2>" in content_html:
        return content_html.replace("<h2>FAQ</h2>", block + "<h2>FAQ</h2>")
    return content_html + "\n" + block


def openai_generate_json(keyword: str, links: list[str]) -> dict:
    model = os.environ.get("OPENAI_MODEL", "gpt-5-mini")

    prompt_template = os.environ.get("PROMPT_OVERRIDE") or PROMPT_FILE.read_text(encoding="utf-8")
    prompt = prompt_template.replace("{KEYWORD}", keyword)

    if len(links) < 3:
        raise RuntimeError("Internal links mapping must contain at least 3 URLs")

    prompt = (
        prompt.replace("{INTERNAL_LINK_1}", links[0])
        .replace("{INTERNAL_LINK_2}", links[1])
        .replace("{INTERNAL_LINK_3}", links[2])
    )

    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["title", "slug", "excerpt", "content_html", "tags", "meta_description"],
        "properties": {
            "title": {"type": "string", "minLength": 4, "maxLength": 120},
            "slug": {"type": "string", "minLength": 3, "maxLength": 120},
            "excerpt": {"type": "string", "minLength": 20, "maxLength": 220},
            "content_html": {"type": "string", "minLength": 200, "maxLength": 8000},
            "tags": {
                "type": "array",
                "minItems": 3,
                "maxItems": 10,
                "items": {"type": "string", "minLength": 2, "maxLength": 30},
            },
            "meta_description": {"type": "string", "minLength": 50, "maxLength": 160},
        },
    }

    payload = {
        "model": model,
        "input": [
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]}
        ],
        "reasoning": {"effort": "low"},
        "max_output_tokens": int(os.environ.get("OPENAI_MAX_OUTPUT_TOKENS", "1100")),
        "text": {
            "format": {
                "type": "json_schema",
                "name": "wp_draft",
                "strict": True,
                "schema": schema,
            }
        },
        "store": False,
    }

    data = openai_call(payload)

    # retry once if truncated
    if data.get("status") == "incomplete":
        details = data.get("incomplete_details") or {}
        if details.get("reason") == "max_output_tokens":
            bumped = int(os.environ.get("OPENAI_MAX_OUTPUT_TOKENS_BUMP", "2400"))
            payload["max_output_tokens"] = bumped
            data = openai_call(payload)

    text = extract_output_text(data)
    if not text:
        snippet = json.dumps(data, ensure_ascii=False)[:2000]
        raise RuntimeError(f"OpenAI returned empty text. Response snippet: {snippet}")

    try:
        obj = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse JSON from model output: {e}\nRaw:\n{text[:2000]}")

    # Post processing: humanize + personal block + sanitize
    obj["content_html"] = humanize_text(obj["content_html"])
    obj["content_html"] = inject_personal_block(obj["content_html"], keyword)
    obj["content_html"] = sanitize_content_html(obj["content_html"])
    obj["content_html"] = obj["content_html"].replace("However,", "Still,")

    obj["slug"] = slugify(obj.get("slug") or obj.get("title") or keyword)
    return obj


def _wp_headers() -> tuple[str, dict]:
    base_url = os.environ["WP_BASE_URL"].rstrip("/")
    wp_user = os.environ["WP_USER"]
    wp_app_password = os.environ["WP_APP_PASSWORD"]

    token = base64.b64encode(f"{wp_user}:{wp_app_password}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }
    return base_url, headers

def wp_fetch_recent_links_by_category(base_url: str, headers: dict, category_id: int, limit: int = 2) -> list[str]:
    if not category_id or int(category_id) <= 0:
        return []

    url = f"{base_url}/wp-json/wp/v2/posts"
    params = {
        "status": "publish",
        "per_page": limit,
        "orderby": "date",
        "order": "desc",
        "categories": str(int(category_id)),
        "_fields": "link",
    }
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    items = r.json()
    out: list[str] = []
    for it in items:
        link = (it.get("link") or "").strip()
        if link:
            out.append(link)
    return out

def _iso_gmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_last_scheduled_post_date_gmt(base_url: str, headers: dict) -> datetime | None:
    url = f"{base_url}/wp-json/wp/v2/posts?status=future&per_page=1&orderby=date&order=desc"
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    items = r.json()
    if not items:
        return None

    s = items[0].get("date_gmt")
    if not s:
        return None

    # WP date_gmt is usually "YYYY-MM-DDTHH:MM:SS"
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def compute_next_slot_gmt(last_scheduled_gmt: datetime | None) -> datetime:
    now = datetime.now(timezone.utc)

    slot = now.replace(hour=SCHEDULE_HOUR_UTC, minute=0, second=0, microsecond=0)
    if now >= slot:
        slot = slot + timedelta(days=1)

    if last_scheduled_gmt is None:
        return slot

    candidate = max(slot, last_scheduled_gmt.replace(microsecond=0) + timedelta(days=1))
    return candidate


def wp_create_post(post: dict, guides_category_id: int) -> tuple[int, str, str | None]:
    base_url, headers = _wp_headers()

    payload = {
        "status": "draft",
        "title": post["title"],
        "slug": post["slug"],
        "excerpt": post["excerpt"],
        "content": post["content_html"],
        "categories": [int(guides_category_id)],
    }

    date_gmt_out: str | None = None
    if AUTO_SCHEDULE:
        last_gmt = get_last_scheduled_post_date_gmt(base_url, headers)
        next_gmt = compute_next_slot_gmt(last_gmt)
        date_gmt_out = _iso_gmt(next_gmt)
        payload["status"] = "future"
        payload["date_gmt"] = date_gmt_out

    url = f"{base_url}/wp-json/wp/v2/posts"
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    r.raise_for_status()
    created = r.json()

    post_id = int(created["id"])
    wp_status = str(created.get("status") or payload["status"])
    return post_id, wp_status, date_gmt_out


def ensure_csv_fields(rows: list[dict]) -> list[str]:
    # ensure the CSV keeps these columns even if older rows did not have them
    desired = ["keyword", "cluster", "status", "wp_post_id", "last_error", "created_at", "published_at"]

    if not rows:
        return desired

    current = list(rows[0].keys())
    for col in desired:
        if col not in current:
            current.append(col)

    for r in rows:
        for col in desired:
            r.setdefault(col, "")

    return current


def main() -> int:
    if not KEYWORDS_CSV.exists():
        print(f"Missing {KEYWORDS_CSV}. Create it first.", file=sys.stderr)
        return 2

    cfg = load_config()
    guides_id = int(cfg["wp_guides_category_id"])

    idx, row, rows = read_first_todo_row()
    if idx < 0:
        print("No todo keywords found. Nothing to do.")
        return 0

    fieldnames = ensure_csv_fields(rows)

    keyword = (row.get("keyword") or "").strip()
    if not keyword:
        rows[idx]["status"] = "error"
        rows[idx]["last_error"] = "Empty keyword"
        write_rows(rows, fieldnames)
        return 1

    try:
        cluster = (row.get("cluster") or "").strip().lower() or "default"
        links_map = load_links()
        links = links_map.get(cluster, links_map["default"])
        base_url, headers = _wp_headers()
        ids = cfg.get("wp_cluster_category_ids", {})

        extra_links: list[str] = []

        if cluster == "paypig_entry":
            extra_links += wp_fetch_recent_links_by_category(base_url, headers, ids.get("paypig_entry_for_paypigs", 0), 1)
            extra_links += wp_fetch_recent_links_by_category(base_url, headers, ids.get("paypig_entry_mistresses", 0), 1)
        elif cluster == "paypig_psychology":
            extra_links += wp_fetch_recent_links_by_category(base_url, headers, ids.get("paypig_psychology_for_paypigs", 0), 2)
        elif cluster == "domme_training":
            extra_links += wp_fetch_recent_links_by_category(base_url, headers, ids.get("domme_training_for_findommes", 0), 2)
        elif cluster == "session_dynamics":
            extra_links += wp_fetch_recent_links_by_category(base_url, headers, ids.get("session_dynamics_findom_educational", 0), 2)

        extra_links = [u for u in extra_links if u]
        extra_links = list(dict.fromkeys(extra_links))[:2]

        prompt_template = PROMPT_FILE.read_text(encoding="utf-8")

        if extra_links:
            prompt_template += (
                "\n\nAdditional internal links.\n"
                "Insert each link naturally in a relevant section of the article.\n"
                "Do not place them next to each other.\n"
                "Links:\n"
                + "\n".join(extra_links)
                + "\n"
            )

        os.environ["PROMPT_OVERRIDE"] = prompt_template

        if extra_links:
            print("Extra internal links:", extra_links)        

        post = openai_generate_json(keyword, links)
        post_id, wp_status, date_gmt = wp_create_post(post, guides_id)

        print(f"AUTO_SCHEDULE={AUTO_SCHEDULE}, wp_status={wp_status}, date_gmt={date_gmt}")

        send_notification_email(post_id, post["title"], cluster, wp_status, date_gmt)

        # Update CSV row status to match pipeline states
        rows[idx]["status"] = "future" if wp_status == "future" else "draft"
        rows[idx]["wp_post_id"] = str(post_id)
        rows[idx]["last_error"] = ""
        # published_at remains empty until a later job marks it

        print(f"Created post_id={post_id} for keyword='{keyword}' in category_id={guides_id}")

    except Exception as e:
        rows[idx]["status"] = "error"
        rows[idx]["last_error"] = f"{type(e).__name__}: {e}"
        print(f"ERROR for keyword='{keyword}': {e}", file=sys.stderr)

    write_rows(rows, fieldnames)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
