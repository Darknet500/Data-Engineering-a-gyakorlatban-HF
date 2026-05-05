from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "news"

load_dotenv(PROJECT_ROOT / ".env")

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
NEWS_QUERY = os.getenv("NEWS_QUERY", "data engineering OR airflow OR python")
NEWS_LANGUAGE = os.getenv("NEWS_LANGUAGE", "en")
NEWS_PAGE_SIZE = int(os.getenv("NEWS_PAGE_SIZE", "50"))


def extract_news(
    query: str,
    language: str = "en",
    page_size: int = 50,
) -> list[dict[str, Any]]:
    if not NEWS_API_KEY:
        raise ValueError("NEWS_API_KEY is missing from .env")

    url = "https://newsapi.org/v2/everything"
    params = {
        "q": query,
        "language": language,
        "pageSize": page_size,
        "sortBy": "publishedAt",
        "apiKey": NEWS_API_KEY,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if payload.get("status") != "ok":
        raise RuntimeError(f"NewsAPI returned non-ok payload: {payload}")

    return payload.get("articles", [])


def main() -> None:
    run_date = datetime.now(UTC).strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    articles = extract_news(
        query=NEWS_QUERY,
        language=NEWS_LANGUAGE,
        page_size=NEWS_PAGE_SIZE,
    )

    output_file = OUTPUT_DIR / f"{run_date}_news.json"

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(articles)} articles to {output_file}")


if __name__ == "__main__":
    main()
