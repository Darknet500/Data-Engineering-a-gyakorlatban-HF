from __future__ import annotations

import json
import math
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_YOUTUBE_DIR = PROJECT_ROOT / "data" / "raw" / "youtube"
RAW_NEWS_DIR = PROJECT_ROOT / "data" / "raw" / "news"
INPUT_DIR = PROJECT_ROOT / "data" / "input"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

USER_PROFILES_FILE = INPUT_DIR / "user_profiles.csv"


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_date(value: Any) -> date | None:
    if not value:
        return None

    parsed_value = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed_value):
        return None

    return parsed_value.date()


def parse_youtube_duration(duration: str | None) -> int:
    """
    Converts YouTube ISO-8601 durations like PT1H2M3S, PT15M, PT42S
    into total seconds.
    """
    if not duration:
        return 0

    pattern = re.compile(
        r"^PT"
        r"(?:(?P<hours>\d+)H)?"
        r"(?:(?P<minutes>\d+)M)?"
        r"(?:(?P<seconds>\d+)S)?$"
    )

    match = pattern.match(duration)
    if not match:
        return 0

    hours = safe_int(match.group("hours"))
    minutes = safe_int(match.group("minutes"))
    seconds = safe_int(match.group("seconds"))

    return hours * 3600 + minutes * 60 + seconds


def extract_run_date_and_topic(file_path: Path) -> tuple[date, str]:
    """
    Expected YouTube file names:
    2026-05-03_python.json
    2026-05-03_data_engineering.json
    2026-05-03_artificial_intelligence.json
    """
    stem = file_path.stem
    date_part = stem[:10]
    topic_part = stem[11:] if len(stem) > 11 else "unknown"

    try:
        run_date = datetime.strptime(date_part, "%Y-%m-%d").date()
    except ValueError:
        run_date = datetime.now(UTC).date()

    topic = topic_part.replace("_", " ").strip().lower()
    return run_date, topic


def extract_run_date_from_news_file(file_path: Path) -> date:
    """
    Expected NewsAPI file names:
    2026-05-03_news.json
    """
    stem = file_path.stem
    date_part = stem[:10]

    try:
        return datetime.strptime(date_part, "%Y-%m-%d").date()
    except ValueError:
        return datetime.now(UTC).date()


def read_json_file(file_path: Path) -> Any:
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_video_id(item: dict[str, Any]) -> str | None:
    raw_id = item.get("id")

    if isinstance(raw_id, dict):
        return raw_id.get("videoId")

    if isinstance(raw_id, str):
        return raw_id

    return None


def load_youtube_records() -> pd.DataFrame:
    records: list[dict[str, Any]] = []

    for file_path in sorted(RAW_YOUTUBE_DIR.glob("*.json")):
        run_date, topic = extract_run_date_and_topic(file_path)
        videos = read_json_file(file_path)

        if isinstance(videos, dict):
            videos = videos.get("items", [])

        for item in videos:
            if not isinstance(item, dict):
                continue

            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            content_details = item.get("contentDetails", {})

            video_id = get_video_id(item)
            channel_id = snippet.get("channelId")

            if not video_id or not channel_id:
                continue

            views = safe_int(statistics.get("viewCount"))
            likes = safe_int(statistics.get("likeCount"))
            comments = safe_int(statistics.get("commentCount"))
            engagement_rate = (likes + comments) / views if views > 0 else 0

            records.append(
                {
                    "date_key": run_date,
                    "topic_name": topic,
                    "video_id": video_id,
                    "title": snippet.get("title", ""),
                    "channel_id": channel_id,
                    "channel_name": snippet.get("channelTitle", ""),
                    "publish_date": parse_date(snippet.get("publishedAt")),
                    "duration_seconds": parse_youtube_duration(
                        content_details.get("duration")
                    ),
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "engagement_rate": round(float(engagement_rate), 6),
                }
            )

    if not records:
        raise ValueError(
            f"No YouTube records found. Expected JSON files in {RAW_YOUTUBE_DIR}"
        )

    youtube_df = pd.DataFrame(records)

    youtube_df = youtube_df.sort_values(
        by=["date_key", "topic_name", "video_id", "views"],
        ascending=[True, True, True, False],
    )

    youtube_df = youtube_df.drop_duplicates(
        subset=["date_key", "topic_name", "video_id"],
        keep="first",
    ).reset_index(drop=True)

    return youtube_df


def load_user_profiles() -> pd.DataFrame:
    if not USER_PROFILES_FILE.exists():
        raise FileNotFoundError(f"Missing user profile file: {USER_PROFILES_FILE}")

    profiles = pd.read_csv(USER_PROFILES_FILE)

    required_columns = {
        "profile_name",
        "preferred_topics",
        "available_time_minutes",
    }

    missing_columns = required_columns - set(profiles.columns)
    if missing_columns:
        raise ValueError(f"Missing columns from user_profiles.csv: {missing_columns}")

    profiles = profiles.drop_duplicates(subset=["profile_name"]).reset_index(drop=True)
    profiles.insert(0, "user_profile_key", range(1, len(profiles) + 1))

    profiles["available_time_minutes"] = profiles["available_time_minutes"].apply(
        safe_int
    )

    return profiles[
        [
            "user_profile_key",
            "profile_name",
            "preferred_topics",
            "available_time_minutes",
        ]
    ]


def article_matches_topic(article: dict[str, Any], topic: str) -> bool:
    text = " ".join(
        [
            str(article.get("title") or ""),
            str(article.get("description") or ""),
            str(article.get("content") or ""),
        ]
    ).lower()

    topic_words = topic.lower().split()
    return all(word in text for word in topic_words)


def load_news_topic_counts(topics: list[str]) -> pd.DataFrame:
    """
    Builds daily article counts by topic from NewsAPI raw JSON files.
    """
    records: list[dict[str, Any]] = []

    for file_path in sorted(RAW_NEWS_DIR.glob("*.json")):
        run_date = extract_run_date_from_news_file(file_path)
        articles = read_json_file(file_path)

        if isinstance(articles, dict):
            articles = articles.get("articles", [])

        if not isinstance(articles, list):
            continue

        for article in articles:
            if not isinstance(article, dict):
                continue

            for topic in topics:
                if article_matches_topic(article, topic):
                    records.append(
                        {
                            "date_key": run_date,
                            "topic_name": topic,
                            "news_article_count": 1,
                        }
                    )

    if not records:
        return pd.DataFrame(
            columns=["date_key", "topic_name", "news_article_count"]
        )

    news_counts = (
        pd.DataFrame(records)
        .groupby(["date_key", "topic_name"], as_index=False)
        .agg(news_article_count=("news_article_count", "sum"))
    )

    return news_counts


def profile_topic_affinity(preferred_topics: Any, topic_name: Any) -> float:
    preferred = str(preferred_topics or "").lower()
    topic = str(topic_name or "").lower()
    topic_words = topic.split()

    if topic in preferred:
        return 1.0

    if any(word in preferred for word in topic_words):
        return 0.5

    return 0.0


def build_dimensions(
    youtube_df: pd.DataFrame,
    user_profiles_df: pd.DataFrame,
    news_topic_counts_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    date_values = set(youtube_df["date_key"].dropna().unique())

    if not news_topic_counts_df.empty:
        date_values.update(news_topic_counts_df["date_key"].dropna().unique())

    dim_date = pd.DataFrame({"date_key": sorted(date_values)})
    dim_date["date_key"] = pd.to_datetime(dim_date["date_key"]).dt.date
    date_series = pd.to_datetime(dim_date["date_key"])

    dim_date["day"] = date_series.dt.day
    dim_date["month"] = date_series.dt.month
    dim_date["year"] = date_series.dt.year
    dim_date["week"] = date_series.dt.isocalendar().week.astype(int)
    dim_date["day_of_week"] = date_series.dt.dayofweek + 1

    dim_channel = (
        youtube_df[["channel_id", "channel_name"]]
        .drop_duplicates(subset=["channel_id"])
        .sort_values("channel_name")
        .reset_index(drop=True)
    )
    dim_channel.insert(0, "channel_key", range(1, len(dim_channel) + 1))
    dim_channel["category"] = "youtube"

    topic_values = set(youtube_df["topic_name"].dropna().unique())
    if not news_topic_counts_df.empty:
        topic_values.update(news_topic_counts_df["topic_name"].dropna().unique())

    dim_topic = pd.DataFrame({"topic_name": sorted(topic_values)})
    dim_topic.insert(0, "topic_key", range(1, len(dim_topic) + 1))

    video_base = (
        youtube_df[
            [
                "video_id",
                "title",
                "channel_id",
                "publish_date",
                "duration_seconds",
                "topic_name",
                "views",
            ]
        ]
        .sort_values(["video_id", "views"], ascending=[True, False])
        .drop_duplicates(subset=["video_id"])
        .drop(columns=["views"])
        .reset_index(drop=True)
    )

    dim_video = (
        video_base.merge(
            dim_channel[["channel_key", "channel_id"]],
            on="channel_id",
            how="left",
        )
        .merge(
            dim_topic[["topic_key", "topic_name"]],
            on="topic_name",
            how="left",
        )
    )

    dim_video.insert(0, "video_key", range(1, len(dim_video) + 1))

    dim_video = dim_video[
        [
            "video_key",
            "video_id",
            "title",
            "channel_key",
            "publish_date",
            "duration_seconds",
            "topic_key",
        ]
    ]

    return dim_date, dim_channel, dim_topic, user_profiles_df, dim_video


def build_video_fact(
    youtube_df: pd.DataFrame,
    dim_channel: pd.DataFrame,
    dim_topic: pd.DataFrame,
    dim_video: pd.DataFrame,
) -> pd.DataFrame:
    fact = (
        youtube_df.merge(
            dim_channel[["channel_key", "channel_id"]],
            on="channel_id",
            how="left",
        )
        .merge(
            dim_topic[["topic_key", "topic_name"]],
            on="topic_name",
            how="left",
        )
        .merge(
            dim_video[["video_key", "video_id"]],
            on="video_id",
            how="left",
        )
    )

    fact = fact.sort_values(
        ["date_key", "video_id", "views"],
        ascending=[True, True, False],
    )

    fact = fact.drop_duplicates(
        subset=["date_key", "video_key"],
        keep="first",
    ).reset_index(drop=True)

    fact["video_key"] = fact["video_key"].astype(int)
    fact["channel_key"] = fact["channel_key"].astype(int)
    fact["topic_key"] = fact["topic_key"].astype(int)

    return fact[
        [
            "date_key",
            "video_key",
            "channel_key",
            "topic_key",
            "views",
            "likes",
            "comments",
            "engagement_rate",
        ]
    ]


def build_topic_fact(
    youtube_df: pd.DataFrame,
    dim_topic: pd.DataFrame,
    news_topic_counts_df: pd.DataFrame,
) -> pd.DataFrame:
    youtube_topic_daily = (
        youtube_df.groupby(["date_key", "topic_name"], as_index=False)
        .agg(
            youtube_video_count=("video_id", "nunique"),
            youtube_total_views=("views", "sum"),
            youtube_total_likes=("likes", "sum"),
            youtube_total_comments=("comments", "sum"),
            avg_engagement_rate=("engagement_rate", "mean"),
        )
    )

    topic_fact = youtube_topic_daily.merge(
        news_topic_counts_df,
        on=["date_key", "topic_name"],
        how="outer",
    )

    fill_zero_columns = [
        "youtube_video_count",
        "youtube_total_views",
        "youtube_total_likes",
        "youtube_total_comments",
        "news_article_count",
        "avg_engagement_rate",
    ]

    for column in fill_zero_columns:
        topic_fact[column] = topic_fact[column].fillna(0)

    topic_fact = topic_fact.merge(
        dim_topic[["topic_key", "topic_name"]],
        on="topic_name",
        how="left",
    )

    topic_fact["topic_key"] = topic_fact["topic_key"].astype(int)

    topic_fact["topic_trend_score"] = topic_fact.apply(
        lambda row: round(
            float(
                math.log1p(row["youtube_total_views"])
                + row["avg_engagement_rate"] * 100
                + row["news_article_count"] * 0.25
            ),
            4,
        ),
        axis=1,
    )

    int_columns = [
        "youtube_video_count",
        "youtube_total_views",
        "youtube_total_likes",
        "youtube_total_comments",
        "news_article_count",
    ]

    for column in int_columns:
        topic_fact[column] = topic_fact[column].astype(int)

    topic_fact["avg_engagement_rate"] = topic_fact["avg_engagement_rate"].round(6)

    return topic_fact[
        [
            "date_key",
            "topic_key",
            "youtube_video_count",
            "youtube_total_views",
            "youtube_total_likes",
            "youtube_total_comments",
            "news_article_count",
            "avg_engagement_rate",
            "topic_trend_score",
        ]
    ]


def build_recommendation_fact(
    video_fact_df: pd.DataFrame,
    dim_topic: pd.DataFrame,
    dim_user_profile: pd.DataFrame,
    news_topic_counts_df: pd.DataFrame,
) -> pd.DataFrame:
    video_base = video_fact_df.merge(
        dim_topic[["topic_key", "topic_name"]],
        on="topic_key",
        how="left",
    )

    if news_topic_counts_df.empty:
        news_lookup: dict[tuple[date, str], int] = {}
    else:
        news_lookup = {
            (row["date_key"], row["topic_name"]): int(row["news_article_count"])
            for _, row in news_topic_counts_df.iterrows()
        }

    recommendation_records: list[dict[str, Any]] = []

    for _, video_row in video_base.iterrows():
        for _, profile_row in dim_user_profile.iterrows():
            topic_affinity = profile_topic_affinity(
                profile_row["preferred_topics"],
                video_row["topic_name"],
            )

            news_count = news_lookup.get(
                (video_row["date_key"], video_row["topic_name"]),
                0,
            )

            recommendation_score = (
                math.log1p(video_row["views"])
                + video_row["engagement_rate"] * 100
                + news_count * 0.25
                + topic_affinity * 5
            )

            recommendation_records.append(
                {
                    "date_key": video_row["date_key"],
                    "user_profile_key": int(profile_row["user_profile_key"]),
                    "video_key": int(video_row["video_key"]),
                    "topic_key": int(video_row["topic_key"]),
                    "topic_affinity": round(float(topic_affinity), 4),
                    "recommendation_score": round(float(recommendation_score), 4),
                }
            )

    recommendation_fact = pd.DataFrame(recommendation_records)

    recommendation_fact = recommendation_fact.drop_duplicates(
        subset=["date_key", "user_profile_key", "video_key"],
        keep="first",
    ).reset_index(drop=True)

    return recommendation_fact[
        [
            "date_key",
            "user_profile_key",
            "video_key",
            "topic_key",
            "topic_affinity",
            "recommendation_score",
        ]
    ]


def prepare_dates_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    output = df.copy()

    for column in output.columns:
        if "date" in column:
            output[column] = output[column].apply(
                lambda value: value.isoformat()
                if isinstance(value, (datetime, date))
                else value
            )

    return output


def write_csv(df: pd.DataFrame, file_name: str) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_file = PROCESSED_DIR / file_name
    output_df = prepare_dates_for_csv(df)

    output_df.to_csv(output_file, index=False, lineterminator="\n")
    print(f"Wrote {len(output_df)} rows to {output_file}")


def main() -> None:
    youtube_df = load_youtube_records()
    user_profiles_df = load_user_profiles()

    topics = sorted(youtube_df["topic_name"].dropna().unique())
    news_topic_counts_df = load_news_topic_counts(topics)

    (
        dim_date,
        dim_channel,
        dim_topic,
        dim_user_profile,
        dim_video,
    ) = build_dimensions(youtube_df, user_profiles_df, news_topic_counts_df)

    fact_video_daily_metrics = build_video_fact(
        youtube_df=youtube_df,
        dim_channel=dim_channel,
        dim_topic=dim_topic,
        dim_video=dim_video,
    )

    fact_topic_daily_metrics = build_topic_fact(
        youtube_df=youtube_df,
        dim_topic=dim_topic,
        news_topic_counts_df=news_topic_counts_df,
    )

    fact_profile_video_recommendations = build_recommendation_fact(
        video_fact_df=fact_video_daily_metrics,
        dim_topic=dim_topic,
        dim_user_profile=dim_user_profile,
        news_topic_counts_df=news_topic_counts_df,
    )

    write_csv(dim_date, "dim_date.csv")
    write_csv(dim_channel, "dim_channel.csv")
    write_csv(dim_topic, "dim_topic.csv")
    write_csv(dim_user_profile, "dim_user_profile.csv")
    write_csv(dim_video, "dim_video.csv")
    write_csv(fact_video_daily_metrics, "fact_video_daily_metrics.csv")
    write_csv(fact_topic_daily_metrics, "fact_topic_daily_metrics.csv")
    write_csv(
        fact_profile_video_recommendations,
        "fact_profile_video_recommendations.csv",
    )


if __name__ == "__main__":
    main()