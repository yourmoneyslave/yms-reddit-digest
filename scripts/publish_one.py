import base64
import csv
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
KEYWORDS_CSV = ROOT / "data" / "keywords.csv"
PROMPT_FILE = ROOT / "prompts" / "template_v1.txt"
CONFIG_FILE = ROOT / "config.json"


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-")[:80] or "draft"


def load_config() -> dict:
    return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))


def read_first_todo_row() -> tuple[int, dict, list[dict]]:
    rows = []
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


def openai_generate_json(keyword: str) -> dict:
    api_key = os.environ["OPENAI_API_KEY"]
    model = os.environ.get("OPENAI_MODEL", "gpt-5-mini")

    prompt_template = PROMPT_FILE.read_text(encoding="utf-8")
    prompt = prompt_template.replace("{KEYWORD}", keyword)

    # Placeholders for internal links, you can later map them per cluster
    prompt = (
        prompt.replace("{INTERNAL_LINK_1}", "https://yourmoneyslave.com/findom-telegram/")
              .replace("{INTERNAL_LINK_2}", "https://yourmoneyslave.com/forum/")
              .replace("{INTERNAL_LINK_3}", "https://yourmoneyslave.com/movies/")
    )

    url = "https://api.openai.com/v1/responses"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

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
        "input": prompt,
        "max_output_tokens": int(os.environ.get("OPENAI_MAX_OUTPUT_TOKENS", "900")),
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "wp_draft",
                "schema": schema,
                "strict": True,
            },
        },
    }

    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=90)
    r.raise_for_status()
    data = r.json()

    # With json_schema, the parsed JSON is typically in output_text as JSON.
    # We safely parse the concatenated text.
    text = data.get("output_text", "").strip()
    if not text:
        raise RuntimeError("OpenAI returned empty output_text")

    try:
        obj = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse JSON from model output: {e}\nRaw:\n{text}")

    # Safety: normalize slug if needed
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
        # tags via names requires extra step (create/get tag IDs), keep it minimal for now
    }

    url = f"{base_url}/wp-json/wp/v2/posts"
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=90)
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
        post = openai_generate_json(keyword)
        post_id = wp_create_draft(post, guides_id)

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
