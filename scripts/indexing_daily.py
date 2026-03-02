import base64
import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import requests
from scripts.indexing_submit import get_access_token, submit_url


def wp_headers() -> tuple[str, dict]:
    base_url = os.environ["WP_BASE_URL"].rstrip("/")
    wp_user = os.environ["WP_USER"]
    wp_app_password = os.environ["WP_APP_PASSWORD"]

    token = base64.b64encode(f"{wp_user}:{wp_app_password}".encode("utf-8")).decode("utf-8")
    headers = {"Authorization": f"Basic {token}"}
    return base_url, headers


def fetch_published_posts_last_24h(base_url: str, headers: dict) -> List[Dict[str, Any]]:
    after = (datetime.now(timezone.utc) - timedelta(hours=24)).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    url = f"{base_url}/wp-json/wp/v2/posts"
    params = {
        "status": "publish",
        "per_page": 30,
        "orderby": "date",
        "order": "desc",
        "after": after,
        "_fields": "id,link,date_gmt",
    }

    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def main() -> int:
    base_url, headers = wp_headers()
    posts = fetch_published_posts_last_24h(base_url, headers)

    if not posts:
        print("No published posts in last 24h.")
        return 0

    token = get_access_token()
    ok = 0
    fail = 0

    for p in posts:
        url = (p.get("link") or "").strip()
        if not url:
            continue
        try:
            resp = submit_url(url, token)
            print(f"[OK] {url} -> {json.dumps(resp, ensure_ascii=False)}")
            ok += 1
        except Exception as e:
            print(f"[ERROR] {url}: {e}")
            fail += 1

    print(f"Done. ok={ok} fail={fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
