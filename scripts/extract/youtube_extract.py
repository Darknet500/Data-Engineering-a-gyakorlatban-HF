from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from googleapiclient.discovery import build

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "youtube"


def safe_filename(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip().lower())
    return value.strip("_") or "topic"


def parse_topics() -> list[str]:
    topics_raw = os.getenv("YOUTUBE_TOPICS") or os.getenv("YOUTUBE_QUERY") or "python,data engineering,artificial intelligence"
    return [topic.strip() for topic in topics_raw.split(",") if topic.strip()]


def youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def search_video_ids(client: Any, topic: str, max_results: int) -> list[str]:
    request = client.search().list(
        part="id",
        q=topic,
        type="video",
        order="relevance",
        maxResults=max_results,
        safeSearch="none",
    )
    response = request.execute()
    video_ids: list[str] = []
    for item in response.get("items", []):
        video_id = item.get("id", {}).get("videoId")
        if video_id:
            video_ids.append(video_id)
    return video_ids


def fetch_video_details(client: Any, video_ids: list[str]) -> list[dict[str, Any]]:
    if not video_ids:
        return []
    request = client.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(video_ids),
        maxResults=len(video_ids),
    )
    response = request.execute()
    return response.get("items", [])


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key or api_key == "put_your_youtube_key_here":
        raise RuntimeError("YOUTUBE_API_KEY is missing. Put a real key in .env before running the DAG.")

    max_results = int(os.getenv("YOUTUBE_MAX_RESULTS", "10"))
    topics = parse_topics()
    run_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    run_date = run_ts[:10]
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    client = youtube_client(api_key)
    total_videos = 0
    for topic in topics:
        video_ids = search_video_ids(client, topic, max_results)
        videos = fetch_video_details(client, video_ids)
        payload = {
            "source": "youtube_data_api",
            "topic": topic,
            "run_timestamp_utc": run_ts,
            "video_count": len(videos),
            "items": videos,
        }
        output_path = RAW_DIR / f"youtube_{run_date}_{safe_filename(topic)}.json"
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        total_videos += len(videos)
        print(f"Wrote {len(videos)} YouTube videos for topic='{topic}' to {output_path}")

    print(f"YouTube extraction finished: {total_videos} videos across {len(topics)} topics")


if __name__ == "__main__":
    main()
