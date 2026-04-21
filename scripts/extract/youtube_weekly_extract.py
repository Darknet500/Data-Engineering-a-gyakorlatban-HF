import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from googleapiclient.discovery import build

def ensure_directory(path: Path) -> None: 
    path.mkdir(parents=True, exist_ok=True)

def get_project_root() -> Path: 
    return Path(__file__).resolve().parents[2]


def fetch_youtube_videos(api_key: str, query: str, max_results: int) -> list[dict]:
    youtube = build("youtube", "v3", developerKey=api_key)

    search_response = (
        youtube.search()
        .list(
            q=query,
            part="snippet",
            type="video",
            maxResults=max_results,
            order="viewCount",
        )
        .execute()
    )

    video_ids = [
        item["id"]["videoId"]
        for item in search_response.get("items", [])
        if "videoId" in item.get("id", [])
    ]

    if not video_ids:
        return []
    
    videos_response = (
        youtube.videos()
        .list(
            part="snippet,statistics",
            id=",".join(video_ids),
        )
        .execute()
    )

    extracted_at = datetime.now(timezone.utc).isoformat()

    records: list[dict] = []

    for item in videos_response.get("items", []):
        snippet = item.get("snippet", [])
        statistics = item.get("statistics", [])

        records.append(
            {
            "extracted_at_utc": extracted_at,
                "source": "youtube",
                "video_id": item.get("id"),
                "title": snippet.get("title"),
                "channel_title": snippet.get("channelTitle"),
                "published_at": snippet.get("publishedAt"),
                "query": query,
                "view_count": int(statistics.get("viewCount", 0)),
                "like_count": int(statistics.get("likeCount", 0)) if statistics.get("likeCount") else 0,
                "comment_count": int(statistics.get("commentCount", 0)) if statistics.get("commentCount") else 0,
            }
        )

    return records


def save_json(records: list[dict], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

def main() -> None:
    load_dotenv()

    api_key = os.getenv("YOUTUBE_API_KEY")
    query = os.getenv("YOUTUBE_QUERY", "data engineering")
    max_results = int(os.getenv("YOUTUBE_MAX_RESULTS", "10"))

    if not api_key:
        raise ValueError("Missing YOUTUBE_API_KEY in .env")
    
    project_root = get_project_root()
    output_dir = project_root / "data" / "raw" / "youtube"
    ensure_directory(output_dir)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"youtube_raw_{timestamp}.json"

    records = fetch_youtube_videos(api_key=api_key, query=query, max_results=max_results)
    save_json(records, output_path)

    print(f"Saved {len(records)} YouTube records to: {output_path}")

if __name__ == "__main__":
    main()