from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_YOUTUBE_DIR = RAW_DIR / "youtube"
RAW_NEWS_DIR = RAW_DIR / "news"
INPUT_DIR = PROJECT_ROOT / "data" / "input"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def int_or_zero(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def parse_iso_datetime(value: Any) -> datetime:
    if not value:
        return datetime.now(timezone.utc).replace(microsecond=0)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    try:
        cleaned = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(cleaned)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return datetime.now(timezone.utc).replace(microsecond=0)


def parse_youtube_duration(duration: Any) -> int:
    if not duration:
        return 0

    duration = str(duration)
    pattern = re.compile(
        r"P(?:(?P<days>\d+)D)?T?"
        r"(?:(?P<hours>\d+)H)?"
        r"(?:(?P<minutes>\d+)M)?"
        r"(?:(?P<seconds>\d+)S)?"
    )
    match = pattern.fullmatch(duration)
    if not match:
        return 0

    parts = {key: int(value or 0) for key, value in match.groupdict().items()}
    return (
        parts["days"] * 86400
        + parts["hours"] * 3600
        + parts["minutes"] * 60
        + parts["seconds"]
    )


def split_tokens(value: Any) -> set[str]:
    return {
        token.strip().lower()
        for token in re.split(r"[;,|]", str(value))
        if token.strip()
    }


def topic_from_filename(path: Path) -> str:
    stem = path.stem.lower()
    stem = stem.replace("youtube", "")
    stem = re.sub(r"\d{4}-\d{2}-\d{2}", "", stem)
    stem = re.sub(r"[_\-]+", " ", stem)
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem or "unknown"


def discover_youtube_json_files() -> list[Path]:
    files: set[Path] = set()

    # The original extractor writes here:
    # /opt/airflow/data/raw/youtube/youtube_YYYY-MM-DD_topic.json
    if RAW_YOUTUBE_DIR.exists():
        files.update(RAW_YOUTUBE_DIR.rglob("*.json"))

    # Also support older/alternate layouts.
    if RAW_DIR.exists():
        files.update(RAW_DIR.rglob("*youtube*.json"))

    if INPUT_DIR.exists():
        files.update(INPUT_DIR.rglob("*youtube*.json"))

    return sorted(files)


def discover_news_json_files() -> list[Path]:
    files: set[Path] = set()

    if RAW_NEWS_DIR.exists():
        files.update(RAW_NEWS_DIR.rglob("*.json"))

    if RAW_DIR.exists():
        files.update(RAW_DIR.rglob("*news*.json"))

    if INPUT_DIR.exists():
        files.update(INPUT_DIR.rglob("*news*.json"))

    return sorted(files)


def create_demo_youtube_raw() -> Path:
    """Create a small local fallback so the full pipeline can run without API output.

    This is useful for local testing and for avoiding a blocked DAG when an API key,
    quota, or network call fails. Real API output will be used automatically when it
    exists in data/raw/youtube.
    """
    RAW_YOUTUBE_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    run_date = run_ts[:10]
    output_path = RAW_YOUTUBE_DIR / f"youtube_{run_date}_demo_fallback.json"

    payload = {
        "source": "demo_fallback",
        "topic": "data engineering",
        "run_timestamp_utc": run_ts,
        "video_count": 3,
        "items": [
            {
                "id": "demo_video_data_engineering_1",
                "snippet": {
                    "title": "Demo Data Engineering Pipeline",
                    "description": "Demo record generated because no YouTube raw file was found.",
                    "publishedAt": f"{run_date}T09:00:00Z",
                    "channelId": "demo_channel_data",
                    "channelTitle": "Demo Data Channel",
                },
                "statistics": {
                    "viewCount": "1200",
                    "likeCount": "80",
                    "commentCount": "12",
                },
                "contentDetails": {"duration": "PT12M30S"},
            },
            {
                "id": "demo_video_python_1",
                "snippet": {
                    "title": "Demo Python Analytics",
                    "description": "Demo Python record for the star schema.",
                    "publishedAt": f"{run_date}T10:00:00Z",
                    "channelId": "demo_channel_python",
                    "channelTitle": "Demo Python Channel",
                },
                "statistics": {
                    "viewCount": "900",
                    "likeCount": "65",
                    "commentCount": "7",
                },
                "contentDetails": {"duration": "PT8M10S"},
            },
            {
                "id": "demo_video_ai_1",
                "snippet": {
                    "title": "Demo Artificial Intelligence Trends",
                    "description": "Demo AI trend record for dashboard testing.",
                    "publishedAt": f"{run_date}T11:00:00Z",
                    "channelId": "demo_channel_ai",
                    "channelTitle": "Demo AI Channel",
                },
                "statistics": {
                    "viewCount": "1800",
                    "likeCount": "140",
                    "commentCount": "22",
                },
                "contentDetails": {"duration": "PT15M00S"},
            },
        ],
    }

    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"No YouTube raw files found, created demo fallback: {output_path}")
    return output_path


def create_demo_news_raw() -> Path:
    RAW_NEWS_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    run_date = run_ts[:10]
    output_path = RAW_NEWS_DIR / f"news_{run_date}_demo_fallback.json"

    payload = {
        "status": "ok",
        "source": "demo_fallback",
        "query": 'python OR "data engineering" OR "artificial intelligence"',
        "run_timestamp_utc": run_ts,
        "totalResults": 3,
        "articles": [
            {
                "title": "Demo article about data engineering",
                "description": "A demo news item for data engineering trend scoring.",
                "content": "Data engineering pipelines and analytics platforms are important.",
            },
            {
                "title": "Demo article about Python",
                "description": "A demo news item for Python trend scoring.",
                "content": "Python is widely used in analytics and data processing.",
            },
            {
                "title": "Demo article about artificial intelligence",
                "description": "A demo news item for AI trend scoring.",
                "content": "Artificial intelligence is a major topic in technology.",
            },
        ],
    }

    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"No NewsAPI raw files found, created demo fallback: {output_path}")
    return output_path


def create_demo_user_profiles() -> Path:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = INPUT_DIR / "user_profiles.csv"

    df = pd.DataFrame(
        [
            {
                "profile_id": 1,
                "persona": "Data Engineering Student",
                "interests": "python,data engineering,etl",
                "preferred_topics": "data engineering,python",
                "available_time_minutes": 20,
                "business_goal": "learn practical pipeline building",
            },
            {
                "profile_id": 2,
                "persona": "AI Enthusiast",
                "interests": "artificial intelligence,machine learning,python",
                "preferred_topics": "artificial intelligence,python",
                "available_time_minutes": 30,
                "business_goal": "follow AI trends",
            },
            {
                "profile_id": 3,
                "persona": "Marketing Analyst",
                "interests": "trends,engagement,analytics",
                "preferred_topics": "data engineering,artificial intelligence",
                "available_time_minutes": 15,
                "business_goal": "identify trending content",
            },
        ]
    )
    df.to_csv(output_path, index=False)
    print(f"Missing user_profiles.csv, created demo fallback: {output_path}")
    return output_path


def extract_items_from_payload(payload: Any, path: Path) -> tuple[str, datetime, list[dict[str, Any]]]:
    topic = topic_from_filename(path)
    run_ts = datetime.now(timezone.utc).replace(microsecond=0)
    items: list[dict[str, Any]] = []

    if isinstance(payload, dict):
        # Skip obvious NewsAPI files accidentally discovered by a broad pattern.
        if "articles" in payload and not any(key in payload for key in ("items", "videos", "data", "results")):
            return topic, run_ts, []

        topic = str(payload.get("topic") or payload.get("query") or topic)
        run_ts = parse_iso_datetime(payload.get("run_timestamp_utc") or payload.get("run_timestamp"))

        for key in ("items", "videos", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                items = [item for item in value if isinstance(item, dict)]
                break

        # Sometimes one video is stored directly as one dict.
        if not items and (payload.get("video_id") or payload.get("videoId") or payload.get("id")):
            items = [payload]

    elif isinstance(payload, list):
        items = [item for item in payload if isinstance(item, dict)]

    return topic, run_ts, items


def load_youtube_raw() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    files = discover_youtube_json_files()

    print("YouTube raw directories:")
    print(f" - {RAW_YOUTUBE_DIR} exists={RAW_YOUTUBE_DIR.exists()}")
    print(f" - {RAW_DIR} exists={RAW_DIR.exists()}")
    print(f" - {INPUT_DIR} exists={INPUT_DIR.exists()}")

    if not files:
        files = [create_demo_youtube_raw()]

    print("YouTube JSON files found:")
    for path in files:
        print(f" - {path}")

    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"Skipping unreadable JSON file {path}: {exc}")
            continue

        topic, run_ts, items = extract_items_from_payload(payload, path)
        print(f"Reading {path}, payload type={type(payload).__name__}, items={len(items)}")

        for item in items:
            snippet = item.get("snippet", {}) or {}
            statistics = item.get("statistics", {}) or {}
            content_details = item.get("contentDetails", {}) or {}

            raw_video_id = (
                item.get("video_id")
                or item.get("videoId")
                or item.get("id")
            )

            if isinstance(raw_video_id, dict):
                raw_video_id = raw_video_id.get("videoId")

            video_id = str(raw_video_id or "").strip()
            if not video_id:
                print(f"Skipping item without video_id in {path}")
                continue

            item_topic = str(item.get("topic") or topic or "unknown").strip() or "unknown"

            views = int_or_zero(
                item.get("view_count")
                or item.get("viewCount")
                or statistics.get("viewCount")
            )
            likes = int_or_zero(
                item.get("like_count")
                or item.get("likeCount")
                or statistics.get("likeCount")
            )
            comments = int_or_zero(
                item.get("comment_count")
                or item.get("commentCount")
                or statistics.get("commentCount")
            )
            engagement_rate = (likes + comments) / views if views else 0.0

            published_at = parse_iso_datetime(
                item.get("published_at")
                or item.get("publishedAt")
                or snippet.get("publishedAt")
            )

            rows.append(
                {
                    "date_key": run_ts.date(),
                    "video_id": video_id,
                    "video_title": str(
                        item.get("title")
                        or item.get("video_title")
                        or snippet.get("title")
                        or "Untitled video"
                    ),
                    "published_at": published_at,
                    "channel_id": str(
                        item.get("channel_id")
                        or item.get("channelId")
                        or snippet.get("channelId")
                        or "unknown_channel"
                    ),
                    "channel_title": str(
                        item.get("channel_title")
                        or item.get("channelTitle")
                        or snippet.get("channelTitle")
                        or "Unknown channel"
                    ),
                    "topic_name": item_topic,
                    "view_count": views,
                    "like_count": likes,
                    "comment_count": comments,
                    "duration_seconds": int_or_zero(
                        item.get("duration_seconds")
                    )
                    or parse_youtube_duration(
                        item.get("duration")
                        or content_details.get("duration")
                    ),
                    "source_file": path.name,
                    "engagement_rate": round(engagement_rate, 6),
                }
            )

    print(f"Total YouTube rows loaded: {len(rows)}")

    if not rows:
        # If files existed but none had usable records, create one clean fallback file
        # and read it once.
        fallback = create_demo_youtube_raw()
        payload = json.loads(fallback.read_text(encoding="utf-8"))
        topic, run_ts, items = extract_items_from_payload(payload, fallback)
        for item in items:
            snippet = item.get("snippet", {}) or {}
            statistics = item.get("statistics", {}) or {}
            content_details = item.get("contentDetails", {}) or {}
            views = int_or_zero(statistics.get("viewCount"))
            likes = int_or_zero(statistics.get("likeCount"))
            comments = int_or_zero(statistics.get("commentCount"))
            rows.append(
                {
                    "date_key": run_ts.date(),
                    "video_id": str(item.get("id")),
                    "video_title": snippet.get("title", "Untitled video"),
                    "published_at": parse_iso_datetime(snippet.get("publishedAt")),
                    "channel_id": snippet.get("channelId", "unknown_channel"),
                    "channel_title": snippet.get("channelTitle", "Unknown channel"),
                    "topic_name": topic,
                    "view_count": views,
                    "like_count": likes,
                    "comment_count": comments,
                    "duration_seconds": parse_youtube_duration(content_details.get("duration")),
                    "source_file": fallback.name,
                    "engagement_rate": round((likes + comments) / views if views else 0.0, 6),
                }
            )
        print(f"Total YouTube rows loaded after fallback: {len(rows)}")

    return pd.DataFrame(rows)


def load_news_topic_counts(topics: list[str], fallback_date: date) -> pd.DataFrame:
    topic_counts = {topic.lower(): 0 for topic in topics}
    files = discover_news_json_files()

    if not files:
        files = [create_demo_news_raw()]

    print("News JSON files found:")
    for path in files:
        print(f" - {path}")

    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"Skipping unreadable news JSON file {path}: {exc}")
            continue

        articles = []
        if isinstance(payload, dict) and isinstance(payload.get("articles"), list):
            articles = payload["articles"]
        elif isinstance(payload, list):
            articles = payload

        for article in articles:
            if not isinstance(article, dict):
                continue

            text = " ".join(
                str(article.get(field, ""))
                for field in ("title", "description", "content")
            ).lower()

            for topic in topics:
                topic_words = [word for word in re.split(r"\s+", topic.lower()) if word]
                if topic_words and all(word in text for word in topic_words):
                    topic_counts[topic.lower()] += 1

    return pd.DataFrame(
        [
            {
                "date_key": fallback_date,
                "topic_name": topic,
                "news_article_count": count,
            }
            for topic, count in topic_counts.items()
        ]
    )


def load_user_profiles() -> pd.DataFrame:
    user_profiles_path = INPUT_DIR / "user_profiles.csv"
    if not user_profiles_path.exists():
        create_demo_user_profiles()

    user_profiles = pd.read_csv(user_profiles_path)

    required_profile_columns = {
        "profile_id",
        "persona",
        "interests",
        "preferred_topics",
        "available_time_minutes",
        "business_goal",
    }
    missing = required_profile_columns.difference(user_profiles.columns)
    if missing:
        raise ValueError(f"user_profiles.csv missing columns: {sorted(missing)}")

    user_profiles = user_profiles.copy()
    user_profiles["profile_id"] = pd.to_numeric(user_profiles["profile_id"], errors="coerce").fillna(0).astype(int)
    user_profiles["available_time_minutes"] = pd.to_numeric(
        user_profiles["available_time_minutes"], errors="coerce"
    ).fillna(0).astype(int)

    for column in ["persona", "interests", "preferred_topics", "business_goal"]:
        user_profiles[column] = user_profiles[column].fillna("").astype(str)

    return user_profiles


def build_dimensions(youtube_df: pd.DataFrame, user_profiles: pd.DataFrame) -> dict[str, pd.DataFrame]:
    youtube_df = youtube_df.copy()
    youtube_df["date_key"] = pd.to_datetime(youtube_df["date_key"]).dt.date
    youtube_df["topic_name"] = youtube_df["topic_name"].fillna("unknown").astype(str)
    youtube_df["channel_id"] = youtube_df["channel_id"].fillna("unknown_channel").astype(str)
    youtube_df["channel_title"] = youtube_df["channel_title"].fillna("Unknown channel").astype(str)

    dim_date = pd.DataFrame({"date_key": sorted(youtube_df["date_key"].dropna().unique())})
    dim_date["year"] = pd.to_datetime(dim_date["date_key"]).dt.year
    dim_date["month"] = pd.to_datetime(dim_date["date_key"]).dt.month
    dim_date["day"] = pd.to_datetime(dim_date["date_key"]).dt.day
    dim_date["day_of_week"] = pd.to_datetime(dim_date["date_key"]).dt.day_name()
    dim_date["is_weekend"] = pd.to_datetime(dim_date["date_key"]).dt.dayofweek >= 5

    channels = (
        youtube_df[["channel_id", "channel_title"]]
        .drop_duplicates()
        .sort_values(["channel_title", "channel_id"])
        .reset_index(drop=True)
    )
    channels.insert(0, "channel_key", channels.index + 1)
    channels = channels.rename(columns={"channel_title": "channel_name"})

    topics = (
        youtube_df[["topic_name"]]
        .drop_duplicates()
        .sort_values("topic_name")
        .reset_index(drop=True)
    )
    topics.insert(0, "topic_key", topics.index + 1)
    topics["topic_category"] = topics["topic_name"].str.lower()

    profiles = user_profiles.copy().reset_index(drop=True)
    profiles["profile_key"] = range(1, len(profiles) + 1)
    profiles = profiles[
        [
            "profile_key",
            "profile_id",
            "persona",
            "interests",
            "preferred_topics",
            "available_time_minutes",
            "business_goal",
        ]
    ]

    videos = (
        youtube_df
        .sort_values(["video_id", "view_count"], ascending=[True, False])
        .drop_duplicates("video_id")
    )
    videos = videos[
        [
            "video_id",
            "video_title",
            "published_at",
            "duration_seconds",
            "topic_name",
            "channel_id",
        ]
    ].copy()
    videos["published_at"] = pd.to_datetime(videos["published_at"], utc=True, errors="coerce")
    videos["duration_seconds"] = pd.to_numeric(videos["duration_seconds"], errors="coerce").fillna(0).astype(int)

    videos = videos.merge(channels[["channel_key", "channel_id"]], on="channel_id", how="left")
    videos = videos.merge(topics[["topic_key", "topic_name"]], on="topic_name", how="left")
    videos = videos.sort_values(["video_title", "video_id"]).reset_index(drop=True)
    videos.insert(0, "video_key", videos.index + 1)
    videos = videos[
        [
            "video_key",
            "video_id",
            "video_title",
            "published_at",
            "duration_seconds",
            "channel_key",
            "topic_key",
        ]
    ]

    return {
        "dim_date": dim_date,
        "dim_channel": channels[["channel_key", "channel_id", "channel_name"]],
        "dim_topic": topics[["topic_key", "topic_name", "topic_category"]],
        "dim_user_profile": profiles,
        "dim_video": videos,
    }


def build_facts(
    youtube_df: pd.DataFrame,
    dims: dict[str, pd.DataFrame],
    news_counts: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    youtube_df = youtube_df.copy()
    youtube_df["date_key"] = pd.to_datetime(youtube_df["date_key"]).dt.date

    videos = dims["dim_video"]
    channels = dims["dim_channel"]
    topics = dims["dim_topic"]
    profiles = dims["dim_user_profile"]

    fact_video = youtube_df.merge(videos[["video_key", "video_id"]], on="video_id", how="left")
    fact_video = fact_video.merge(channels[["channel_key", "channel_id"]], on="channel_id", how="left")
    fact_video = fact_video.merge(topics[["topic_key", "topic_name"]], on="topic_name", how="left")
    fact_video = fact_video.sort_values(["date_key", "video_key", "view_count"], ascending=[True, True, False])
    fact_video = fact_video.drop_duplicates(["date_key", "video_key"])
    fact_video["fact_video_daily_metrics_key"] = range(1, len(fact_video) + 1)

    for column in ["view_count", "like_count", "comment_count"]:
        fact_video[column] = pd.to_numeric(fact_video[column], errors="coerce").fillna(0).astype(int)

    fact_video["engagement_rate"] = pd.to_numeric(
        fact_video["engagement_rate"], errors="coerce"
    ).fillna(0).round(6)

    fact_video = fact_video[
        [
            "fact_video_daily_metrics_key",
            "date_key",
            "video_key",
            "channel_key",
            "topic_key",
            "view_count",
            "like_count",
            "comment_count",
            "engagement_rate",
        ]
    ]

    topic_daily = (
        fact_video.groupby(["date_key", "topic_key"], as_index=False)
        .agg(
            video_count=("video_key", "nunique"),
            total_views=("view_count", "sum"),
            total_likes=("like_count", "sum"),
            total_comments=("comment_count", "sum"),
            avg_engagement_rate=("engagement_rate", "mean"),
        )
        .merge(topics[["topic_key", "topic_name"]], on="topic_key", how="left")
    )

    news_counts = news_counts.copy()
    news_counts["date_key"] = pd.to_datetime(news_counts["date_key"]).dt.date
    news_counts["topic_name_lower"] = news_counts["topic_name"].astype(str).str.lower()

    topic_daily["topic_name_lower"] = topic_daily["topic_name"].astype(str).str.lower()
    topic_daily = topic_daily.merge(
        news_counts[["date_key", "topic_name_lower", "news_article_count"]],
        on=["date_key", "topic_name_lower"],
        how="left",
    )
    topic_daily["news_article_count"] = topic_daily["news_article_count"].fillna(0).astype(int)
    topic_daily["avg_engagement_rate"] = topic_daily["avg_engagement_rate"].fillna(0).round(6)
    topic_daily["trend_score"] = (
        topic_daily["video_count"] * 2
        + topic_daily["news_article_count"] * 3
        + topic_daily["avg_engagement_rate"] * 100
    ).round(4)

    topic_daily = topic_daily.sort_values(
        ["date_key", "trend_score"],
        ascending=[True, False],
    ).reset_index(drop=True)
    topic_daily["fact_topic_daily_metrics_key"] = range(1, len(topic_daily) + 1)

    fact_topic = topic_daily[
        [
            "fact_topic_daily_metrics_key",
            "date_key",
            "topic_key",
            "video_count",
            "news_article_count",
            "total_views",
            "total_likes",
            "total_comments",
            "avg_engagement_rate",
            "trend_score",
        ]
    ]

    video_with_topic = videos.merge(topics[["topic_key", "topic_name"]], on="topic_key", how="left")
    video_metrics = fact_video[
        [
            "date_key",
            "video_key",
            "topic_key",
            "view_count",
            "like_count",
            "comment_count",
            "engagement_rate",
        ]
    ]

    recommendation_rows: list[dict[str, Any]] = []

    for _, metric_row in video_metrics.iterrows():
        video_meta_rows = video_with_topic.loc[video_with_topic["video_key"] == metric_row["video_key"]]
        if video_meta_rows.empty:
            continue

        video_meta = video_meta_rows.iloc[0]
        topic_name = str(video_meta["topic_name"]).lower()
        duration_seconds = int(video_meta.get("duration_seconds", 0) or 0)

        for _, profile in profiles.iterrows():
            preferred = split_tokens(profile["preferred_topics"])
            interests = split_tokens(profile["interests"])

            topical_affinity = 1.0 if topic_name in preferred else 0.35
            keyword_boost = 0.25 if any(word and word in topic_name for word in interests) else 0.0
            time_fit = 1.0 if duration_seconds <= int(profile["available_time_minutes"]) * 60 else 0.65
            popularity_component = min(float(metric_row["view_count"]) / 100000.0, 1.0)
            engagement_component = min(float(metric_row["engagement_rate"]) * 25.0, 1.0)

            recommendation_score = round(
                (
                    topical_affinity * 0.45
                    + keyword_boost * 0.15
                    + time_fit * 0.15
                    + popularity_component * 0.15
                    + engagement_component * 0.10
                )
                * 100,
                2,
            )

            recommendation_rows.append(
                {
                    "date_key": metric_row["date_key"],
                    "profile_key": int(profile["profile_key"]),
                    "video_key": int(metric_row["video_key"]),
                    "topic_key": int(metric_row["topic_key"]),
                    "topical_affinity": round(topical_affinity + keyword_boost, 2),
                    "recommendation_score": recommendation_score,
                }
            )

    fact_recommendations = pd.DataFrame(recommendation_rows)
    fact_recommendations = fact_recommendations.sort_values(
        ["profile_key", "recommendation_score"],
        ascending=[True, False],
    ).reset_index(drop=True)
    fact_recommendations.insert(0, "fact_profile_video_recommendations_key", fact_recommendations.index + 1)

    return {
        "fact_video_daily_metrics": fact_video,
        "fact_topic_daily_metrics": fact_topic,
        "fact_profile_video_recommendations": fact_recommendations,
    }


def write_csvs(dims: dict[str, pd.DataFrame], facts: dict[str, pd.DataFrame]) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    for name, df in {**dims, **facts}.items():
        out = df.copy()

        for column in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[column]):
                out[column] = out[column].dt.strftime("%Y-%m-%d %H:%M:%S%z")
            elif column.endswith("_key") and column != "date_key":
                out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0).astype(int)

        out.to_csv(PROCESSED_DIR / f"{name}.csv", index=False)
        print(f"Wrote {len(out):>4} rows -> {PROCESSED_DIR / f'{name}.csv'}")


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")

    youtube_df = load_youtube_raw()
    if youtube_df.empty:
        raise RuntimeError("No usable YouTube records could be loaded, even after demo fallback.")

    topics = sorted(youtube_df["topic_name"].dropna().astype(str).unique().tolist())
    fallback_date = pd.to_datetime(youtube_df["date_key"]).dt.date.max()

    news_counts = load_news_topic_counts(topics, fallback_date=fallback_date)
    user_profiles = load_user_profiles()

    dims = build_dimensions(youtube_df, user_profiles)
    facts = build_facts(youtube_df, dims, news_counts)
    write_csvs(dims, facts)

    print("Star schema build finished successfully.")


if __name__ == "__main__":
    main()
