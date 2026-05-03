import json
import os
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv
from googleapiclient.discovery import build


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "youtube"

load_dotenv(PROJECT_ROOT / ".env")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_TOPICS = os.getenv(
    "YOUTUBE_TOPICS",
    "python,data engineering,artificial intelligence",
)
YOUTUBE_MAX_RESULTS = int(os.getenv("YOUTUBE_MAX_RESULTS", "10"))


def get_topics():
    return [
        topic.strip()
        for topic in YOUTUBE_TOPICS.split(",")
        if topic.strip()
    ]


def extract_youtube_videos(query: str, max_results: int = 10):
    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEY is missing from .env")

    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    search_response = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        maxResults=max_results,
        order="relevance",
    ).execute()

    video_ids = [
        item["id"]["videoId"]
        for item in search_response.get("items", [])
        if "id" in item and "videoId" in item["id"]
    ]

    if not video_ids:
        print(f"No videos found for topic: {query}")
        return []

    videos_response = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(video_ids),
    ).execute()

    return videos_response.get("items", [])


def main():
    run_date = datetime.now(UTC).strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for topic in get_topics():
        videos = extract_youtube_videos(topic, YOUTUBE_MAX_RESULTS)
        output_file = OUTPUT_DIR / f"{run_date}_{topic.replace(' ', '_')}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(videos, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(videos)} videos to {output_file}")


if __name__ == "__main__":
    main()