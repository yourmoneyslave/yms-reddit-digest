import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/indexing"]


def get_access_token() -> str:
    raw = os.environ.get("GSC_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("Missing env GSC_SERVICE_ACCOUNT_JSON")

    info = json.loads(raw)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    creds.refresh(Request())
    if not creds.token:
        raise RuntimeError("Failed to obtain access token")
    return creds.token


def submit_url(url: str, token: str) -> Dict[str, Any]:
    endpoint = "https://indexing.googleapis.com/v3/urlNotifications:publish"
    payload = {"url": url, "type": "URL_UPDATED"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(endpoint, headers=headers, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Indexing API HTTP {r.status_code}: {r.text[:2000]}")
    return r.json()


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

        last_err = None
        for attempt in range(1, 4):
            try:
                resp = submit_url(url, token)
                print(f"[OK] {url} -> {json.dumps(resp, ensure_ascii=False)}")
                ok += 1
                last_err = None
                break
            except Exception as e:
                last_err = e
                print(f"[WARN] {url} attempt={attempt} failed: {e}")
                time.sleep(2 * attempt)

        if last_err is not None:
            print(f"[ERROR] {url}: {last_err}")
            fail += 1

    print(f"Done. ok={ok} fail={fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
