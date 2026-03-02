import json
import os
import sys
import time
from typing import Dict, Any

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
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    r = requests.post(endpoint, headers=headers, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"Indexing API HTTP {r.status_code}: {r.text[:2000]}")
    return r.json()


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/indexing_submit.py <url>", file=sys.stderr)
        return 2

    url = sys.argv[1].strip()
    if not url.startswith("http"):
        print("URL must start with http/https", file=sys.stderr)
        return 2

    token = get_access_token()

    # simple retry
    last_err = None
    for attempt in range(1, 4):
        try:
            resp = submit_url(url, token)
            print(json.dumps(resp, ensure_ascii=False))
            return 0
        except Exception as e:
            last_err = e
            print(f"[WARN] attempt={attempt} failed: {e}", file=sys.stderr)
            time.sleep(2 * attempt)

    print(f"[ERROR] failed after retries: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
