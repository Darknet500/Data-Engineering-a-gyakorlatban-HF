DROP TABLE IF EXISTS fact_profile_video_recommendations CASCADE;
DROP TABLE IF EXISTS fact_topic_daily_metrics CASCADE;
DROP TABLE IF EXISTS fact_video_daily_metrics CASCADE;

DROP TABLE IF EXISTS dim_video CASCADE;
DROP TABLE IF EXISTS dim_user_profile CASCADE;
DROP TABLE IF EXISTS dim_topic CASCADE;
DROP TABLE IF EXISTS dim_channel CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key INT PRIMARY KEY,
    channel_id TEXT UNIQUE NOT NULL,
    channel_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS dim_topic (
    topic_key INT PRIMARY KEY,
    topic_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_user_profile (
    user_profile_key INT PRIMARY KEY,
    profile_name TEXT UNIQUE NOT NULL,
    preferred_topics TEXT,
    available_time_minutes INT
);

CREATE TABLE IF NOT EXISTS dim_video (
    video_key INT PRIMARY KEY,
    video_id TEXT UNIQUE NOT NULL,
    title TEXT,
    channel_key INT REFERENCES dim_channel(channel_key),
    publish_date DATE,
    duration_seconds INT,
    topic_key INT REFERENCES dim_topic(topic_key)
);

CREATE TABLE IF NOT EXISTS fact_video_daily_metrics (
    fact_id SERIAL PRIMARY KEY,
    date_key DATE REFERENCES dim_date(date_key),
    video_key INT REFERENCES dim_video(video_key),
    channel_key INT REFERENCES dim_channel(channel_key),
    topic_key INT REFERENCES dim_topic(topic_key),

    views BIGINT DEFAULT 0 CHECK (views >= 0),
    likes BIGINT DEFAULT 0 CHECK (likes >= 0),
    comments BIGINT DEFAULT 0 CHECK (comments >= 0),
    engagement_rate DOUBLE PRECISION DEFAULT 0,

    UNIQUE (date_key, video_key)
);

CREATE TABLE IF NOT EXISTS fact_topic_daily_metrics (
    fact_id SERIAL PRIMARY KEY,
    date_key DATE REFERENCES dim_date(date_key),
    topic_key INT REFERENCES dim_topic(topic_key),

    youtube_video_count INT DEFAULT 0 CHECK (youtube_video_count >= 0),
    youtube_total_views BIGINT DEFAULT 0 CHECK (youtube_total_views >= 0),
    youtube_total_likes BIGINT DEFAULT 0 CHECK (youtube_total_likes >= 0),
    youtube_total_comments BIGINT DEFAULT 0 CHECK (youtube_total_comments >= 0),
    news_article_count INT DEFAULT 0 CHECK (news_article_count >= 0),
    avg_engagement_rate DOUBLE PRECISION DEFAULT 0,
    topic_trend_score DOUBLE PRECISION DEFAULT 0,

    UNIQUE (date_key, topic_key)
);

CREATE TABLE IF NOT EXISTS fact_profile_video_recommendations (
    recommendation_id SERIAL PRIMARY KEY,
    date_key DATE REFERENCES dim_date(date_key),
    user_profile_key INT REFERENCES dim_user_profile(user_profile_key),
    video_key INT REFERENCES dim_video(video_key),
    topic_key INT REFERENCES dim_topic(topic_key),

    topic_affinity DOUBLE PRECISION DEFAULT 0,
    recommendation_score DOUBLE PRECISION DEFAULT 0,

    UNIQUE (date_key, user_profile_key, video_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_video_daily_date
    ON fact_video_daily_metrics(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_video_daily_topic
    ON fact_video_daily_metrics(topic_key);

CREATE INDEX IF NOT EXISTS idx_fact_topic_daily_date
    ON fact_topic_daily_metrics(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_topic_daily_topic
    ON fact_topic_daily_metrics(topic_key);

CREATE INDEX IF NOT EXISTS idx_fact_recommendation_profile
    ON fact_profile_video_recommendations(user_profile_key);

CREATE INDEX IF NOT EXISTS idx_fact_recommendation_video
    ON fact_profile_video_recommendations(video_key);

CREATE INDEX IF NOT EXISTS idx_fact_recommendation_topic
    ON fact_profile_video_recommendations(topic_key);