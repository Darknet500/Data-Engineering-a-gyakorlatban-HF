from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_YOUTUBE_DIR = PROJECT_ROOT / "data" / "raw" / "youtube"
RAW_NEWS_DIR = PROJECT_ROOT / "data" / "raw" / "news"
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


def collect_errors() -> list[str]:
    errors: list[str] = []

    if not list(RAW_YOUTUBE_DIR.glob("*.json")):
        errors.append(f"No YouTube raw JSON files found in {RAW_YOUTUBE_DIR}")

    if not list(RAW_NEWS_DIR.glob("*.json")):
        errors.append(f"No NewsAPI raw JSON files found in {RAW_NEWS_DIR}")

    for file_name in REQUIRED_PROCESSED_FILES:
        file_path = PROCESSED_DIR / file_name

        if not file_path.exists():
            errors.append(f"Missing processed file: {file_path}")
            continue

        df = pd.read_csv(file_path)
        if df.empty:
            errors.append(f"Processed file is empty: {file_path}")

    return errors


def check_unique_keys(
    df: pd.DataFrame,
    file_name: str,
    key_columns: list[str],
    errors: list[str],
) -> None:
    duplicate_count = df.duplicated(subset=key_columns).sum()

    if duplicate_count > 0:
        errors.append(
            f"{file_name} has {duplicate_count} duplicate rows for key {key_columns}"
        )


def check_non_negative_metrics(
    df: pd.DataFrame,
    file_name: str,
    metric_columns: list[str],
    errors: list[str],
) -> None:
    for column in metric_columns:
        if column not in df.columns:
            continue

        negative_count = (df[column] < 0).sum()

        if negative_count > 0:
            errors.append(
                f"{file_name} has {negative_count} negative values in column {column}"
            )


def run_data_quality_checks(errors: list[str]) -> None:
    fact_video = pd.read_csv(PROCESSED_DIR / "fact_video_daily_metrics.csv")
    fact_topic = pd.read_csv(PROCESSED_DIR / "fact_topic_daily_metrics.csv")
    fact_recommendations = pd.read_csv(
        PROCESSED_DIR / "fact_profile_video_recommendations.csv"
    )

    dim_date = pd.read_csv(PROCESSED_DIR / "dim_date.csv")
    dim_channel = pd.read_csv(PROCESSED_DIR / "dim_channel.csv")
    dim_topic = pd.read_csv(PROCESSED_DIR / "dim_topic.csv")
    dim_user_profile = pd.read_csv(PROCESSED_DIR / "dim_user_profile.csv")
    dim_video = pd.read_csv(PROCESSED_DIR / "dim_video.csv")

    check_unique_keys(dim_date, "dim_date.csv", ["date_key"], errors)
    check_unique_keys(dim_channel, "dim_channel.csv", ["channel_key"], errors)
    check_unique_keys(dim_channel, "dim_channel.csv", ["channel_id"], errors)
    check_unique_keys(dim_topic, "dim_topic.csv", ["topic_key"], errors)
    check_unique_keys(dim_topic, "dim_topic.csv", ["topic_name"], errors)
    check_unique_keys(
        dim_user_profile,
        "dim_user_profile.csv",
        ["user_profile_key"],
        errors,
    )
    check_unique_keys(dim_video, "dim_video.csv", ["video_key"], errors)
    check_unique_keys(dim_video, "dim_video.csv", ["video_id"], errors)

    check_unique_keys(
        fact_video,
        "fact_video_daily_metrics.csv",
        ["date_key", "video_key"],
        errors,
    )

    check_unique_keys(
        fact_topic,
        "fact_topic_daily_metrics.csv",
        ["date_key", "topic_key"],
        errors,
    )

    check_unique_keys(
        fact_recommendations,
        "fact_profile_video_recommendations.csv",
        ["date_key", "user_profile_key", "video_key"],
        errors,
    )

    check_non_negative_metrics(
        fact_video,
        "fact_video_daily_metrics.csv",
        ["views", "likes", "comments", "engagement_rate"],
        errors,
    )

    check_non_negative_metrics(
        fact_topic,
        "fact_topic_daily_metrics.csv",
        [
            "youtube_video_count",
            "youtube_total_views",
            "youtube_total_likes",
            "youtube_total_comments",
            "news_article_count",
            "avg_engagement_rate",
            "topic_trend_score",
        ],
        errors,
    )

    check_non_negative_metrics(
        fact_recommendations,
        "fact_profile_video_recommendations.csv",
        ["topic_affinity", "recommendation_score"],
        errors,
    )


def main() -> None:
    errors = collect_errors()

    if not errors:
        run_data_quality_checks(errors)

    if errors:
        print("Pipeline validation failed:")
        for error in errors:
            print(f"- {error}")
        sys.exit(1)

    print("Pipeline validation passed successfully.")


if __name__ == "__main__":
    main()