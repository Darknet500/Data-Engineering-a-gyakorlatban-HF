import os
import json
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv


load_dotenv()

NEWS_API_KEY = os.getenv("NEWS_API_KEY")
OUTPUT_DIR = Path("data/raw/news")


def extract_news(query: str, page_size: int = 10):
    url = "https://newsapi.org/v2/everything"

    params = {
        "q": query,
        "pageSize": page_size,
        "sortBy": "publishedAt",
        "language": "en",
        "apiKey": NEWS_API_KEY,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    return response.json().get("articles", [])


def main():
    topics = ["python", "data engineering", "artificial intelligence"]

    run_date = datetime.utcnow().strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for topic in topics:
        articles = extract_news(topic)
        output_file = OUTPUT_DIR / f"{run_date}_{topic.replace(' ', '_')}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(articles)} articles to {output_file}")


if __name__ == "__main__":
    main()