import os
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Any

from openai import OpenAI

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CSV_PATH = DATA_DIR / "keywords.csv"
SOURCES_PATH = DATA_DIR / "keyword_sources.json"

KEYWORDS_PER_CLUSTER = int(os.getenv("KEYWORDS_PER_CLUSTER", "10"))
MIN_WORDS = int(os.getenv("MIN_WORDS", "4"))
MAX_TOTAL_TODO = int(os.getenv("MAX_TOTAL_TODO", "50"))

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", "0.7"))

CSV_HEADERS = [
    "keyword",
    "cluster",
    "status",
    "wp_post_id",
    "last_error",
    "created_at",
    "published_at",
]

VALID_STATUS = {"todo", "draft", "future", "published", "error"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def norm_kw(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def word_count(s: str) -> int:
    return len([w for w in re.split(r"\s+", s.strip()) if w])


def load_sources() -> Dict[str, Any]:
    if not SOURCES_PATH.exists():
        raise FileNotFoundError(f"Missing {SOURCES_PATH}")
    return json.loads(SOURCES_PATH.read_text(encoding="utf-8"))


def ensure_csv_exists() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()


def read_existing_keywords_and_todo_count() -> (Set[str], int):
    existing: Set[str] = set()
    todo_count = 0
    if not CSV_PATH.exists():
        return existing, 0

    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            kw = norm_kw(row.get("keyword", ""))
            if kw:
                existing.add(kw)
            if row.get("status", "").strip().lower() == "todo":
                todo_count += 1
    return existing, todo_count


def append_rows(rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        for r in rows:
            writer.writerow(r)


def call_openai_for_cluster(
    client: OpenAI,
    cluster_name: str,
    seeds: List[str],
    global_cfg: Dict[str, Any],
    count: int,
) -> List[str]:
    blacklist = global_cfg.get("blacklist", [])
    fmt = global_cfg.get("format_rules", {})
    gen = global_cfg.get("generation_rules", {})

    prompt = f"""
You are generating SEO keyword phrases for a blog in English.

Cluster: {cluster_name}

Seed topics:
{json.dumps(seeds, ensure_ascii=False)}

Rules:
- Return exactly {count} keyword phrases.
- Each keyword must be a single line.
- Use long tail phrases, not single generic words.
- Min words: {fmt.get("min_words", 4)}
- Max words: {fmt.get("max_words", 12)}
- Avoid explicit language: {gen.get("avoid_explicit_language", True)}
- Focus on intent types: {json.dumps(gen.get("search_intent_types", []))}
- Tone: {gen.get("tone", "educational")}
- Avoid duplicates and near-duplicates in your own list.
- Do NOT include any of these blacklisted terms (case-insensitive):
{json.dumps(blacklist, ensure_ascii=False)}

Output format:
Return a valid JSON array of strings, nothing else.
""".strip()

    resp = client.responses.create(
        model=OPENAI_MODEL,
        input=prompt,
        temperature=OPENAI_TEMPERATURE,
    )

    text = (resp.output_text or "").strip()
    try:
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("Not a JSON list")
        out = []
        for x in data:
            if isinstance(x, str):
                out.append(x.strip())
        return out
    except Exception as e:
        raise RuntimeError(f"OpenAI returned non-JSON output for cluster {cluster_name}: {e}\nRaw:\n{text}")


def filter_keywords(
    kws: List[str],
    existing_norm: Set[str],
    global_cfg: Dict[str, Any],
) -> List[str]:
    blacklist = [b.lower() for b in global_cfg.get("blacklist", [])]
    fmt = global_cfg.get("format_rules", {})

    min_words = int(fmt.get("min_words", MIN_WORDS))
    max_words = int(fmt.get("max_words", 999))

    filtered: List[str] = []
    seen_local: Set[str] = set()

    for kw in kws:
        k = kw.strip()
        if not k:
            continue

        nk = norm_kw(k)

        if nk in existing_norm:
            continue
        if nk in seen_local:
            continue

        wc = word_count(k)
        if wc < min_words or wc > max_words:
            continue

        bad = False
        for b in blacklist:
            if b and b in nk:
                bad = True
                break
        if bad:
            continue

        filtered.append(k)
        seen_local.add(nk)

    return filtered


def main() -> int:
    ensure_csv_exists()

    sources = load_sources()
    global_cfg = sources.get("global", {})
    clusters = sources.get("clusters", [])

    existing_norm, todo_count = read_existing_keywords_and_todo_count()
    remaining_capacity = max(0, MAX_TOTAL_TODO - todo_count)
    if remaining_capacity <= 0:
        print(f"MAX_TOTAL_TODO reached ({MAX_TOTAL_TODO}). Nothing to do.")
        return 0

    client = OpenAI()

    rows_to_append: List[Dict[str, str]] = []
    total_added = 0

    for c in clusters:
        if remaining_capacity <= 0:
            break

        cluster_name = c.get("cluster", "").strip()
        seeds = c.get("seed_topics", [])
        if not cluster_name or not isinstance(seeds, list) or len(seeds) == 0:
            continue

        want = min(KEYWORDS_PER_CLUSTER, remaining_capacity)
        try:
            raw = call_openai_for_cluster(client, cluster_name, seeds, global_cfg, want)
            good = filter_keywords(raw, existing_norm, global_cfg)
        except Exception as e:
            print(f"[ERROR] cluster={cluster_name}: {e}")
            continue

        now = utc_now_iso()
        for kw in good:
            rows_to_append.append(
                {
                    "keyword": kw,
                    "cluster": cluster_name,
                    "status": "todo",
                    "wp_post_id": "",
                    "last_error": "",
                    "created_at": now,
                    "published_at": "",
                }
            )
            existing_norm.add(norm_kw(kw))
            total_added += 1
            remaining_capacity -= 1
            if remaining_capacity <= 0:
                break

        print(f"[OK] cluster={cluster_name} generated={len(raw)} kept={len(good)}")

    append_rows(rows_to_append)
    print(f"Done. Added {total_added} keywords to {CSV_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
