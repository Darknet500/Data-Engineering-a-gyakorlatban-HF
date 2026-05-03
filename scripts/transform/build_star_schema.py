import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_YOUTUBE_DIR = PROJECT_ROOT / "data" / "raw" / "youtube"
RAW_NEWS_DIR = PROJECT_ROOT / "data" / "raw" / "news"
INPUT_DIR = PROJECT_ROOT / "data" / "input"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

USER_PROFILES_FILE = INPUT_DIR / "user_profiles.csv"


def safe_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_date(value):
    if not value:
        return None

    parsed_value = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(parsed_value):
        return None

    return parsed_value.date()


def parse_youtube_duration(duration):
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


def extract_run_date_and_topic(file_path: Path):
    """
    Expected file names:
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


def load_youtube_records():
    records = []

    for file_path in sorted(RAW_YOUTUBE_DIR.glob("*.json")):
        run_date, topic = extract_run_date_and_topic(file_path)

        with open(file_path, "r", encoding="utf-8") as f:
            videos = json.load(f)

        for item in videos:
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            content_details = item.get("contentDetails", {})

            video_id = item.get("id")
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
                    "engagement_rate": round(engagement_rate, 6),
                }
            )

    if not records:
        raise ValueError(
            f"No YouTube records found. Expected JSON files in {RAW_YOUTUBE_DIR}"
        )

    return pd.DataFrame(records)


def load_user_profiles():
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

    return profiles[
        [
            "user_profile_key",
            "profile_name",
            "preferred_topics",
            "available_time_minutes",
        ]
    ]


def load_news_topic_counts(topics):
    """
    Uses NewsAPI raw data to slightly boost videos whose topic appears in news titles,
    descriptions, or content.
    """
    topic_counts = {topic: 0 for topic in topics}

    for file_path in sorted(RAW_NEWS_DIR.glob("*.json")):
        with open(file_path, "r", encoding="utf-8") as f:
            articles = json.load(f)

        for article in articles:
            text = " ".join(
                [
                    str(article.get("title") or ""),
                    str(article.get("description") or ""),
                    str(article.get("content") or ""),
                ]
            ).lower()

            for topic in topics:
                words = topic.lower().split()

                if all(word in text for word in words):
                    topic_counts[topic] += 1

    return topic_counts


def profile_topic_affinity(preferred_topics, topic_name):
    preferred = str(preferred_topics or "").lower()
    topic = str(topic_name or "").lower()

    topic_words = topic.split()

    if topic in preferred:
        return 1.0

    if any(word in preferred for word in topic_words):
        return 0.5

    return 0.0


def build_dimensions(youtube_df, user_profiles_df):
    dim_date = (
        pd.DataFrame({"date_key": sorted(youtube_df["date_key"].dropna().unique())})
        .assign(
            day=lambda df: pd.to_datetime(df["date_key"]).dt.day,
            month=lambda df: pd.to_datetime(df["date_key"]).dt.month,
            year=lambda df: pd.to_datetime(df["date_key"]).dt.year,
            week=lambda df: pd.to_datetime(
                df["date_key"]
            ).dt.isocalendar().week.astype(int),
            day_of_week=lambda df: pd.to_datetime(df["date_key"]).dt.dayofweek + 1,
        )
    )

    dim_channel = (
        youtube_df[["channel_id", "channel_name"]]
        .drop_duplicates()
        .sort_values("channel_name")
        .reset_index(drop=True)
    )
    dim_channel.insert(0, "channel_key", range(1, len(dim_channel) + 1))
    dim_channel["category"] = "youtube"

    dim_topic = (
        youtube_df[["topic_name"]]
        .drop_duplicates()
        .sort_values("topic_name")
        .reset_index(drop=True)
    )
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
            ]
        ]
        .drop_duplicates(subset=["video_id"])
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


def build_fact(youtube_df, dim_channel, dim_topic, dim_video, user_profiles_df):
    topics = sorted(youtube_df["topic_name"].dropna().unique())
    news_topic_counts = load_news_topic_counts(topics)

    fact_base = (
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

    fact_records = []

    for _, video_row in fact_base.iterrows():
        for _, profile_row in user_profiles_df.iterrows():
            affinity = profile_topic_affinity(
                profile_row["preferred_topics"],
                video_row["topic_name"],
            )

            trend_score = (
                math.log1p(video_row["views"])
                + video_row["engagement_rate"] * 100
                + news_topic_counts.get(video_row["topic_name"], 0) * 0.25
                + affinity * 5
            )

            fact_records.append(
                {
                    "date_key": video_row["date_key"],
                    "video_key": int(video_row["video_key"]),
                    "channel_key": int(video_row["channel_key"]),
                    "topic_key": int(video_row["topic_key"]),
                    "user_profile_key": int(profile_row["user_profile_key"]),
                    "views": int(video_row["views"]),
                    "likes": int(video_row["likes"]),
                    "comments": int(video_row["comments"]),
                    "engagement_rate": float(video_row["engagement_rate"]),
                    "trend_score": round(float(trend_score), 4),
                }
            )

    fact = pd.DataFrame(fact_records)

    fact = fact.drop_duplicates(
        subset=["date_key", "video_key", "user_profile_key"]
    ).reset_index(drop=True)

    return fact


def write_csv(df, file_name):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_file = PROCESSED_DIR / file_name
    df.to_csv(output_file, index=False)

    print(f"Wrote {len(df)} rows to {output_file}")


def main():
    youtube_df = load_youtube_records()
    user_profiles_df = load_user_profiles()

    (
        dim_date,
        dim_channel,
        dim_topic,
        dim_user_profile,
        dim_video,
    ) = build_dimensions(youtube_df, user_profiles_df)

    fact_video_daily_metrics = build_fact(
        youtube_df,
        dim_channel,
        dim_topic,
        dim_video,
        user_profiles_df,
    )

    write_csv(dim_date, "dim_date.csv")
    write_csv(dim_channel, "dim_channel.csv")
    write_csv(dim_topic, "dim_topic.csv")
    write_csv(dim_user_profile, "dim_user_profile.csv")
    write_csv(dim_video, "dim_video.csv")
    write_csv(fact_video_daily_metrics, "fact_video_daily_metrics.csv")


if __name__ == "__main__":
    main()