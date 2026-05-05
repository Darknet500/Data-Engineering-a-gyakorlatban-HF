DROP TABLE IF EXISTS fact_profile_video_recommendations CASCADE;
DROP TABLE IF EXISTS fact_topic_daily_metrics CASCADE;
DROP TABLE IF EXISTS fact_video_daily_metrics CASCADE;
DROP TABLE IF EXISTS dim_video CASCADE;
DROP TABLE IF EXISTS dim_user_profile CASCADE;
DROP TABLE IF EXISTS dim_topic CASCADE;
DROP TABLE IF EXISTS dim_channel CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE dim_channel (
    channel_key INT PRIMARY KEY,
    channel_id TEXT NOT NULL UNIQUE,
    channel_name TEXT NOT NULL
);

CREATE TABLE dim_topic (
    topic_key INT PRIMARY KEY,
    topic_name TEXT NOT NULL UNIQUE,
    topic_category TEXT NOT NULL
);

CREATE TABLE dim_user_profile (
    profile_key INT PRIMARY KEY,
    profile_id INT NOT NULL UNIQUE,
    persona TEXT NOT NULL,
    interests TEXT NOT NULL,
    preferred_topics TEXT NOT NULL,
    available_time_minutes INT NOT NULL CHECK (available_time_minutes >= 0),
    business_goal TEXT NOT NULL
);

CREATE TABLE dim_video (
    video_key INT PRIMARY KEY,
    video_id TEXT NOT NULL UNIQUE,
    video_title TEXT NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE,
    duration_seconds INT NOT NULL CHECK (duration_seconds >= 0),
    channel_key INT NOT NULL REFERENCES dim_channel(channel_key),
    topic_key INT NOT NULL REFERENCES dim_topic(topic_key)
);

CREATE TABLE fact_video_daily_metrics (
    fact_video_daily_metrics_key INT PRIMARY KEY,
    date_key DATE NOT NULL REFERENCES dim_date(date_key),
    video_key INT NOT NULL REFERENCES dim_video(video_key),
    channel_key INT NOT NULL REFERENCES dim_channel(channel_key),
    topic_key INT NOT NULL REFERENCES dim_topic(topic_key),
    view_count BIGINT NOT NULL CHECK (view_count >= 0),
    like_count BIGINT NOT NULL CHECK (like_count >= 0),
    comment_count BIGINT NOT NULL CHECK (comment_count >= 0),
    engagement_rate NUMERIC(12, 6) NOT NULL CHECK (engagement_rate >= 0),
    UNIQUE (date_key, video_key)
);

CREATE TABLE fact_topic_daily_metrics (
    fact_topic_daily_metrics_key INT PRIMARY KEY,
    date_key DATE NOT NULL REFERENCES dim_date(date_key),
    topic_key INT NOT NULL REFERENCES dim_topic(topic_key),
    video_count INT NOT NULL CHECK (video_count >= 0),
    news_article_count INT NOT NULL CHECK (news_article_count >= 0),
    total_views BIGINT NOT NULL CHECK (total_views >= 0),
    total_likes BIGINT NOT NULL CHECK (total_likes >= 0),
    total_comments BIGINT NOT NULL CHECK (total_comments >= 0),
    avg_engagement_rate NUMERIC(12, 6) NOT NULL CHECK (avg_engagement_rate >= 0),
    trend_score NUMERIC(12, 4) NOT NULL CHECK (trend_score >= 0),
    UNIQUE (date_key, topic_key)
);

CREATE TABLE fact_profile_video_recommendations (
    fact_profile_video_recommendations_key INT PRIMARY KEY,
    date_key DATE NOT NULL REFERENCES dim_date(date_key),
    profile_key INT NOT NULL REFERENCES dim_user_profile(profile_key),
    video_key INT NOT NULL REFERENCES dim_video(video_key),
    topic_key INT NOT NULL REFERENCES dim_topic(topic_key),
    topical_affinity NUMERIC(6, 2) NOT NULL CHECK (topical_affinity >= 0),
    recommendation_score NUMERIC(8, 2) NOT NULL CHECK (recommendation_score >= 0),
    UNIQUE (date_key, profile_key, video_key)
);

CREATE INDEX idx_fact_video_date ON fact_video_daily_metrics(date_key);
CREATE INDEX idx_fact_video_topic ON fact_video_daily_metrics(topic_key);
CREATE INDEX idx_fact_topic_date ON fact_topic_daily_metrics(date_key);
CREATE INDEX idx_fact_recommendation_profile_score ON fact_profile_video_recommendations(profile_key, recommendation_score DESC);
