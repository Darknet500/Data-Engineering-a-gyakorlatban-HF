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


DEMO_ARTICLES = [
    {
        "title": "Data Engineering pipelines are transforming modern analytics",
        "description": "A look at how batch and streaming pipelines enable scalable data processing.",
        "content": "Data engineering with Python, Airflow, and Spark enables organisations to build reliable ETL pipelines.",
    },
    {
        "title": "Python remains the top language for data science in 2024",
        "description": "Survey results show Python dominates machine learning and analytics workflows.",
        "content": "Python's rich ecosystem of pandas, sqlalchemy, and data engineering libraries keeps it at the top.",
    },
    {
        "title": "Artificial Intelligence investment reaches record highs",
        "description": "Venture capital flows into AI startups focusing on generative models and automation.",
        "content": "Artificial intelligence adoption is accelerating across industries with data engineering as the backbone.",
    },
    {
        "title": "Apache Airflow 3.0 brings major orchestration improvements",
        "description": "The new release introduces a reworked task execution model and improved UI.",
        "content": "Airflow remains the go-to orchestration tool for data engineering teams building DAG-based pipelines.",
    },
    {
        "title": "How modern data stacks combine Python, dbt, and cloud warehouses",
        "description": "A practical guide to building end-to-end data engineering solutions.",
        "content": "Data engineering practitioners rely on Python scripts, dbt transformations, and cloud storage for their pipelines.",
    },
]

def write_demo_data(query: str, run_ts: str, run_date: str) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": "ok",
        "source": "demo_mode",
        "query": query,
        "run_timestamp_utc": run_ts,
        "totalResults": len(DEMO_ARTICLES),
        "articles": DEMO_ARTICLES,
    }
    output_path = RAW_DIR / f"news_{run_date}.json"
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DEMO] Wrote {len(DEMO_ARTICLES)} synthetic news articles to {output_path}")




def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")

    query = os.getenv("NEWS_QUERY", 'python OR "data engineering" OR "artificial intelligence"')
    run_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    run_date = run_ts[:10]

    if os.getenv("DEMO_MODE", "false").lower() == "true":
        write_demo_data(query, run_ts, run_date)
        return

    api_key = os.getenv("NEWS_API_KEY")
    if not api_key or api_key == "put_your_newsapi_key_here":
        raise RuntimeError("NEWS_API_KEY is missing. Put a real key in .env or set DEMO_MODE=true.")

    page_size = int(os.getenv("NEWS_PAGE_SIZE", "50"))

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
