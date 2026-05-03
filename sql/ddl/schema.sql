DROP TABLE IF EXISTS fact_video_daily_metrics CASCADE;
DROP TABLE IF EXISTS dim_user_profile CASCADE;
DROP TABLE IF EXISTS dim_topic CASCADE;
DROP TABLE IF EXISTS dim_video CASCADE;
DROP TABLE IF EXISTS dim_channel CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    week INT,
    day_of_week INT
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key SERIAL PRIMARY KEY,
    channel_id TEXT UNIQUE,
    channel_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXISTS dim_topic (
    topic_key SERIAL PRIMARY KEY,
    topic_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_user_profile (
    user_profile_key SERIAL PRIMARY KEY,
    profile_name TEXT UNIQUE,
    preferred_topics TEXT,
    available_time_minutes INT
);

CREATE TABLE IF NOT EXISTS dim_video (
    video_key SERIAL PRIMARY KEY,
    video_id TEXT UNIQUE,
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
    user_profile_key INT REFERENCES dim_user_profile(user_profile_key),
    views BIGINT,
    likes BIGINT,
    comments BIGINT,
    engagement_rate FLOAT,
    trend_score FLOAT,
    UNIQUE (date_key, video_key, user_profile_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_date 
ON fact_video_daily_metrics(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_video 
ON fact_video_daily_metrics(video_key);

CREATE INDEX IF NOT EXISTS idx_fact_topic 
ON fact_video_daily_metrics(topic_key);