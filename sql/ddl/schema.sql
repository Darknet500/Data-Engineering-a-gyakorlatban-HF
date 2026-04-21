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
    channel_ TEXT UNIQUE,
    channel_name TEXT,
    category TEXT
);

CREATE TABLE IF NOT EXIST dim_video (
    video_key SERIAL PRIMARY KEY,
    video_id TEXT UNIQUE,
    title TEXT,
    channel_key INT REFERENCES dim_channel(channel_key),
    publish_date DATE,
    duration INT, --seconds
    topic TEXT,
);

CREATE TABLE IF NOT EXIST  dim_topic (
    topic_key SERIAL PRIMARY KEY,
    topic_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXIST dim_user_profile (
    user_profile_key SERIAL PRIMARY KEY,
    profile_name TEXT,
    preffered_topics TEXT,
    available_time_minutes INT
);

CREATE TABLE IF NOT EXIST fact_video_daily_metrics (
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

CREATE INDEX IF NOT EXIST idx_fact_date ON fact_video_daily_metrcics(date_key);
CREATE INDEX IF NOT EXIST idx_fact_video ON fact_video_daily_metrcics(video_key);
CREATE INDEX IF NOT EXIST idx_fact_topic ON fact_video_daily_metrcics(topic_key);

