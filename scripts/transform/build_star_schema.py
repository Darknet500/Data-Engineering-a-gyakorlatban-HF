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
RAW_YOUTUBE_DIR = PROJECT_ROOT / "data" / "raw" / "youtube"
RAW_NEWS_DIR = PROJECT_ROOT / "data" / "raw" / "news"
INPUT_DIR = PROJECT_ROOT / "data" / "input"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def int_or_zero(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def parse_iso_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    cleaned = value.replace("Z", "+00:00")
    return datetime.fromisoformat(cleaned)


def parse_youtube_duration(duration: str | None) -> int:
    if not duration:
        return 0
    pattern = re.compile(r"P(?:(?P<days>\d+)D)?T?(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?")
    match = pattern.fullmatch(duration)
    if not match:
        return 0
    parts = {key: int(value or 0) for key, value in match.groupdict().items()}
    return parts["days"] * 86400 + parts["hours"] * 3600 + parts["minutes"] * 60 + parts["seconds"]


def split_tokens(value: str) -> set[str]:
    return {token.strip().lower() for token in re.split(r"[;,|]", str(value)) if token.strip()}


def load_youtube_raw() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted(RAW_YOUTUBE_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        topic = payload.get("topic", "unknown")
        run_ts = parse_iso_datetime(payload.get("run_timestamp_utc"))
        for item in payload.get("items", []):
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            content_details = item.get("contentDetails", {})
            video_id = item.get("id")
            if isinstance(video_id, dict):
                video_id = video_id.get("videoId")
            video_id = str(video_id or "").strip()
            if not video_id:
                continue
            views = int_or_zero(statistics.get("viewCount"))
            likes = int_or_zero(statistics.get("likeCount"))
            comments = int_or_zero(statistics.get("commentCount"))
            engagement_rate = (likes + comments) / views if views else 0.0
            rows.append(
                {
                    "date_key": run_ts.date(),
                    "video_id": video_id,
                    "video_title": snippet.get("title", ""),
                    "published_at": parse_iso_datetime(snippet.get("publishedAt")),
                    "channel_id": snippet.get("channelId", "unknown"),
                    "channel_title": snippet.get("channelTitle", "unknown"),
                    "topic_name": topic,
                    "view_count": views,
                    "like_count": likes,
                    "comment_count": comments,
                    "duration_seconds": parse_youtube_duration(content_details.get("duration")),
                    "source_file": path.name,
                    "engagement_rate": round(engagement_rate, 6),
                }
            )
    return pd.DataFrame(rows)


def load_news_topic_counts(topics: list[str], fallback_date: date) -> pd.DataFrame:
    topic_counts = {topic.lower(): 0 for topic in topics}
    for path in sorted(RAW_NEWS_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for article in payload.get("articles", []):
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
            {"date_key": fallback_date, "topic_name": topic, "news_article_count": count}
            for topic, count in topic_counts.items()
        ]
    )


def build_dimensions(youtube_df: pd.DataFrame, user_profiles: pd.DataFrame) -> dict[str, pd.DataFrame]:
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

    profiles = user_profiles.copy()
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

    videos = youtube_df.sort_values(["video_id", "view_count"], ascending=[True, False]).drop_duplicates("video_id")
    videos = videos[["video_id", "video_title", "published_at", "duration_seconds", "topic_name", "channel_id"]].copy()
    videos = videos.merge(channels[["channel_key", "channel_id"]], on="channel_id", how="left")
    videos = videos.merge(topics[["topic_key", "topic_name"]], on="topic_name", how="left")
    videos = videos.sort_values(["video_title", "video_id"]).reset_index(drop=True)
    videos.insert(0, "video_key", videos.index + 1)
    videos = videos[["video_key", "video_id", "video_title", "published_at", "duration_seconds", "channel_key", "topic_key"]]

    return {
        "dim_date": dim_date,
        "dim_channel": channels[["channel_key", "channel_id", "channel_name"]],
        "dim_topic": topics[["topic_key", "topic_name", "topic_category"]],
        "dim_user_profile": profiles,
        "dim_video": videos,
    }


def build_facts(youtube_df: pd.DataFrame, dims: dict[str, pd.DataFrame], news_counts: pd.DataFrame) -> dict[str, pd.DataFrame]:
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
    news_counts["topic_name"] = news_counts["topic_name"].str.lower()
    topic_daily["topic_name_lower"] = topic_daily["topic_name"].str.lower()
    topic_daily = topic_daily.merge(
        news_counts.rename(columns={"topic_name": "topic_name_lower"}),
        on=["date_key", "topic_name_lower"],
        how="left",
    )
    topic_daily["news_article_count"] = topic_daily["news_article_count"].fillna(0).astype(int)
    topic_daily["trend_score"] = (
        topic_daily["video_count"] * 2
        + topic_daily["news_article_count"] * 3
        + topic_daily["avg_engagement_rate"].fillna(0) * 100
    ).round(4)
    topic_daily = topic_daily.sort_values(["date_key", "trend_score"], ascending=[True, False]).reset_index(drop=True)
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
    video_metrics = fact_video[["date_key", "video_key", "topic_key", "view_count", "like_count", "comment_count", "engagement_rate"]]
    recommendation_rows: list[dict[str, Any]] = []
    for _, metric_row in video_metrics.iterrows():
        video_meta = video_with_topic.loc[video_with_topic["video_key"] == metric_row["video_key"]].iloc[0]
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
                (topical_affinity * 0.45 + keyword_boost * 0.15 + time_fit * 0.15 + popularity_component * 0.15 + engagement_component * 0.10) * 100,
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
    fact_recommendations = fact_recommendations.sort_values(["profile_key", "recommendation_score"], ascending=[True, False]).reset_index(drop=True)
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
        out.to_csv(PROCESSED_DIR / f"{name}.csv", index=False)
        print(f"Wrote {len(out):>4} rows -> {PROCESSED_DIR / f'{name}.csv'}")


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    youtube_df = load_youtube_raw()
    if youtube_df.empty:
        raise RuntimeError("No YouTube raw records found. Run scripts/extract/youtube_extract.py first.")

    topics = sorted(youtube_df["topic_name"].dropna().unique().tolist())
    fallback_date = youtube_df["date_key"].max() if not youtube_df.empty else datetime.now(timezone.utc).date()
    news_counts = load_news_topic_counts(topics, fallback_date=fallback_date)

    user_profiles_path = INPUT_DIR / "user_profiles.csv"
    if not user_profiles_path.exists():
        raise FileNotFoundError(f"Missing required user CSV: {user_profiles_path}")
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

    dims = build_dimensions(youtube_df, user_profiles)
    facts = build_facts(youtube_df, dims, news_counts)
    write_csvs(dims, facts)


if __name__ == "__main__":
    main()
