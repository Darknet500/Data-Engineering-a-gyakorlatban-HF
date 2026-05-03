from datetime import datetime, UTC
from pathlib import Path
import os
import json

from dotenv import load_dotenv
from googleapiclient.discovery import build


load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
OUTPUT_DIR = Path("data/raw/youtube")


def extract_youtube_videos(query: str, max_results: int = 10):
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    search_response = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        maxResults=max_results,
        order="relevance"
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
        id=",".join(video_ids)
    ).execute()

    return videos_response.get("items", [])


def main():
    topics = ["python", "data engineering", "artificial intelligence"]

    run_date = datetime.now(UTC).strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for topic in topics:
        videos = extract_youtube_videos(topic)
        output_file = OUTPUT_DIR / f"{run_date}_{topic.replace(' ', '_')}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(videos, f, ensure_ascii=False, indent=2)

        print(f"Saved {len(videos)} videos to {output_file}")


if __name__ == "__main__":
    main()