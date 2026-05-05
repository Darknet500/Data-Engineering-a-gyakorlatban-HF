from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

REQUIRED_PROCESSED_FILES = [
    "dim_date.csv",
    "dim_channel.csv",
    "dim_topic.csv",
    "dim_user_profile.csv",
    "dim_video.csv",
    "fact_video_daily_metrics.csv",
    "fact_topic_daily_metrics.csv",
    "fact_profile_video_recommendations.csv",
]

UNIQUE_KEYS = {
    "dim_date.csv": ["date_key"],
    "dim_channel.csv": ["channel_key"],
    "dim_topic.csv": ["topic_key"],
    "dim_user_profile.csv": ["profile_key"],
    "dim_video.csv": ["video_key"],
    "fact_video_daily_metrics.csv": ["date_key", "video_key"],
    "fact_topic_daily_metrics.csv": ["date_key", "topic_key"],
    "fact_profile_video_recommendations.csv": ["date_key", "profile_key", "video_key"],
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def load_csv(name: str) -> pd.DataFrame:
    path = PROCESSED_DIR / name
    require(path.exists(), f"Missing processed output: {path}")
    df = pd.read_csv(path)
    require(not df.empty, f"Processed output is empty: {path}")
    return df


def validate_unique_keys(tables: dict[str, pd.DataFrame]) -> None:
    for filename, columns in UNIQUE_KEYS.items():
        df = tables[filename]
        duplicated = df.duplicated(subset=columns).sum()
        require(duplicated == 0, f"{filename} has {duplicated} duplicate key rows for {columns}")


def validate_metrics(tables: dict[str, pd.DataFrame]) -> None:
    metric_columns = {
        "fact_video_daily_metrics.csv": ["view_count", "like_count", "comment_count", "engagement_rate"],
        "fact_topic_daily_metrics.csv": [
            "video_count",
            "news_article_count",
            "total_views",
            "total_likes",
            "total_comments",
            "avg_engagement_rate",
            "trend_score",
        ],
        "fact_profile_video_recommendations.csv": ["topical_affinity", "recommendation_score"],
    }
    for filename, columns in metric_columns.items():
        df = tables[filename]
        for column in columns:
            require(column in df.columns, f"{filename} missing metric column {column}")
            require((pd.to_numeric(df[column], errors="coerce").fillna(0) >= 0).all(), f"{filename}.{column} contains negative values")


def validate_foreign_keys(tables: dict[str, pd.DataFrame]) -> None:
    date_keys = set(tables["dim_date.csv"]["date_key"].astype(str))
    video_keys = set(tables["dim_video.csv"]["video_key"])
    channel_keys = set(tables["dim_channel.csv"]["channel_key"])
    topic_keys = set(tables["dim_topic.csv"]["topic_key"])
    profile_keys = set(tables["dim_user_profile.csv"]["profile_key"])

    dim_video = tables["dim_video.csv"]
    require(set(dim_video["channel_key"]).issubset(channel_keys), "dim_video.channel_key has missing references")
    require(set(dim_video["topic_key"]).issubset(topic_keys), "dim_video.topic_key has missing references")

    fact_video = tables["fact_video_daily_metrics.csv"]
    require(set(fact_video["date_key"].astype(str)).issubset(date_keys), "fact_video_daily_metrics.date_key has missing references")
    require(set(fact_video["video_key"]).issubset(video_keys), "fact_video_daily_metrics.video_key has missing references")
    require(set(fact_video["channel_key"]).issubset(channel_keys), "fact_video_daily_metrics.channel_key has missing references")
    require(set(fact_video["topic_key"]).issubset(topic_keys), "fact_video_daily_metrics.topic_key has missing references")

    fact_topic = tables["fact_topic_daily_metrics.csv"]
    require(set(fact_topic["date_key"].astype(str)).issubset(date_keys), "fact_topic_daily_metrics.date_key has missing references")
    require(set(fact_topic["topic_key"]).issubset(topic_keys), "fact_topic_daily_metrics.topic_key has missing references")

    recs = tables["fact_profile_video_recommendations.csv"]
    require(set(recs["date_key"].astype(str)).issubset(date_keys), "fact_profile_video_recommendations.date_key has missing references")
    require(set(recs["profile_key"]).issubset(profile_keys), "fact_profile_video_recommendations.profile_key has missing references")
    require(set(recs["video_key"]).issubset(video_keys), "fact_profile_video_recommendations.video_key has missing references")
    require(set(recs["topic_key"]).issubset(topic_keys), "fact_profile_video_recommendations.topic_key has missing references")


def main() -> None:
    youtube_files = list((RAW_DIR / "youtube").glob("*.json"))
    news_files = list((RAW_DIR / "news").glob("*.json"))
    require(youtube_files, "No raw YouTube JSON files found")
    require(news_files, "No raw NewsAPI JSON files found")

    tables = {filename: load_csv(filename) for filename in REQUIRED_PROCESSED_FILES}
    validate_unique_keys(tables)
    validate_metrics(tables)
    validate_foreign_keys(tables)
    print("Validation passed: raw files, processed CSVs, keys, metrics, and references look good.")


if __name__ == "__main__":
    main()
