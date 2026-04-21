from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
 
def get_project_root() -> Path: 
    return Path(__file__).resolve().parents[2]

def fetch_newsapi_article(
        api_key: str,
        query: str,
        language: str,
        page_size: str
) -> pd.DataFrame:
    url = "https://newsapi.org/v2/everything"
    
    params = {
        "q": query,
        "language": language,
        "pageSize": page_size,
        "sortBy": "publishedAt",
        "apiKey": api_key
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()

    if payload.get("status") != "ok":
        raise RuntimeError(f"Newsapi error:  {payload}")
    
    articles = payload.get("articles", [])
    extracted_at = datetime.now(timezone.utc).isoformat()

    rows = []
    for article in articles:
        source = article.get("source", [])
        rows.append(
            {
                "extracted_at_utc": extracted_at,
                "source": "newsapi",
                "news_source_id": source.get("id"),
                "news_source_name": source.get("name"),
                "author": article.get("author"),
                "title": article.get("title"),
                "description": article.get("description"),
                "published_at": article.get("publishedAt"),
                "url": article.get("url"),
                "query": query,
            }
        )
    
    return pd.DataFrame(rows)


def main() -> None:
    load_dotenv()

    api_key = os.getenv("NEWS_API_KEY")
    query = os.getenv("NEWS_QUERY", "data engineering OR airflow OR python")
    language = os.getenv("NEWS_LANGUAGE", "en")
    page_size = int(os.getenv("NEWS_PAGE_SIZE", 50))

    if not api_key:
        raise ValueError("Missing NEWS_API_KEY in .env")
    
    project_root = get_project_root()
    output_dir = project_root / "data" / "raw" / "newsapi"
    ensure_directory(output_dir)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"newsapi_raw_{timestamp}.csv"

    df = fetch_newsapi_article(
        api_key=api_key,
        query=query,
        language=language,
        page_size=page_size
    )

    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} NewsAPI records to {output_path}")


if __name__ == "__main__":
    main()