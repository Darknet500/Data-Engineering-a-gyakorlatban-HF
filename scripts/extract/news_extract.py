from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "news"
NEWSAPI_URL = "https://newsapi.org/v2/everything"


def fetch_news(api_key: str, query: str, page_size: int) -> dict[str, Any]:
    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "apiKey": api_key,
    }
    response = requests.get(NEWSAPI_URL, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "ok":
        raise RuntimeError(f"NewsAPI returned non-ok status: {payload}")
    return payload


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key or api_key == "put_your_newsapi_key_here":
        raise RuntimeError("NEWS_API_KEY is missing. Put a real key in .env before running the DAG.")

    query = os.getenv("NEWS_QUERY", 'python OR "data engineering" OR "artificial intelligence"')
    page_size = int(os.getenv("NEWS_PAGE_SIZE", "50"))
    run_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    run_date = run_ts[:10]

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    payload = fetch_news(api_key, query, page_size)
    payload["source"] = "newsapi"
    payload["query"] = query
    payload["run_timestamp_utc"] = run_ts

    output_path = RAW_DIR / f"news_{run_date}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(payload.get('articles', []))} NewsAPI articles to {output_path}")


if __name__ == "__main__":
    main()
