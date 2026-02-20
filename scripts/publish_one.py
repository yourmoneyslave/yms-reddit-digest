import base64
import csv
import json
import os
import re
import sys
from pathlib import Path

import requests
import smtplib
from email.mime.text import MIMEText

ROOT = Path(__file__).resolve().parents[1]
KEYWORDS_CSV = ROOT / "data" / "keywords.csv"
PROMPT_FILE = ROOT / "prompts" / "template_v1.txt"
CONFIG_FILE = ROOT / "config.json"
LINKS_FILE = ROOT / "internal_links.json"


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


def send_notification_email(post_id: int, title: str, cluster: str):
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

    subject = f"[YMS] New Draft: {title}"
    body = f"""New draft created.

Title: {title}
Cluster: {cluster}

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
        "temperature": 0.4,
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
    personal_block = (
        f"<p><strong>My perspective:</strong> In my experience with {keyword}, "
        "most beginners overthink things. I have seen patterns repeat again and again. "
        "The difference usually comes down to awareness and boundaries, not intensity.</p>"
    )

    if "<h2>FAQ</h2>" in content_html:
        return content_html.replace("<h2>FAQ</h2>", personal_block + "<h2>FAQ</h2>")
    return content_html + "\n" + personal_block


def openai_generate_json(keyword: str, links: list[str]) -> dict:
    model = os.environ.get("OPENAI_MODEL", "gpt-5-mini")

    prompt_template = PROMPT_FILE.read_text(encoding="utf-8")
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
        "temperature": float(os.environ.get("OPENAI_TEMPERATURE", "0.2")),
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

    obj["slug"] = slugify(obj.get("slug") or obj.get("title") or keyword)
    return obj


def wp_create_draft(post: dict, guides_category_id: int) -> int:
    base_url = os.environ["WP_BASE_URL"].rstrip("/")
    wp_user = os.environ["WP_USER"]
    wp_app_password = os.environ["WP_APP_PASSWORD"]

    token = base64.b64encode(f"{wp_user}:{wp_app_password}".encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "status": "draft",
        "title": post["title"],
        "slug": post["slug"],
        "excerpt": post["excerpt"],
        "content": post["content_html"],
        "categories": [int(guides_category_id)],
    }

    url = f"{base_url}/wp-json/wp/v2/posts"
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    r.raise_for_status()
    created = r.json()
    return int(created["id"])


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

    keyword = (row.get("keyword") or "").strip()
    if not keyword:
        rows[idx]["status"] = "error"
        rows[idx]["last_error"] = "Empty keyword"
        fieldnames = list(rows[0].keys()) if rows else ["keyword", "cluster", "status", "wp_post_id", "last_error"]
        write_rows(rows, fieldnames)
        return 1

    try:
        cluster = (row.get("cluster") or "").strip().lower() or "default"
        links_map = load_links()
        links = links_map.get(cluster, links_map["default"])

        post = openai_generate_json(keyword, links)
        post_id = wp_create_draft(post, guides_id)
        send_notification_email(post_id, post["title"], cluster)

        rows[idx]["status"] = "done"
        rows[idx]["wp_post_id"] = str(post_id)
        rows[idx]["last_error"] = ""
        print(f"Created draft post_id={post_id} for keyword='{keyword}' in category_id={guides_id}")

    except Exception as e:
        rows[idx]["status"] = "error"
        rows[idx]["last_error"] = f"{type(e).__name__}: {e}"
        print(f"ERROR for keyword='{keyword}': {e}", file=sys.stderr)

    fieldnames = list(rows[0].keys()) if rows else ["keyword", "cluster", "status", "wp_post_id", "last_error"]
    write_rows(rows, fieldnames)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
