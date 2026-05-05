CREATE OR REPLACE VIEW vw_topic_trends AS
SELECT
    f.date_key,
    t.topic_name,
    f.video_count,
    f.news_article_count,
    f.total_views,
    f.total_likes,
    f.total_comments,
    f.avg_engagement_rate,
    f.trend_score,
    RANK() OVER (PARTITION BY f.date_key ORDER BY f.trend_score DESC) AS daily_topic_rank
FROM fact_topic_daily_metrics f
JOIN dim_topic t ON t.topic_key = f.topic_key;

CREATE OR REPLACE VIEW vw_top_videos AS
SELECT
    f.date_key,
    t.topic_name,
    c.channel_name,
    v.video_id,
    v.video_title,
    v.published_at,
    v.duration_seconds,
    f.view_count,
    f.like_count,
    f.comment_count,
    f.engagement_rate,
    (f.like_count + f.comment_count) AS engagement_count,
    RANK() OVER (PARTITION BY f.date_key ORDER BY f.view_count DESC) AS daily_view_rank
FROM fact_video_daily_metrics f
JOIN dim_video v ON v.video_key = f.video_key
JOIN dim_channel c ON c.channel_key = f.channel_key
JOIN dim_topic t ON t.topic_key = f.topic_key;

CREATE OR REPLACE VIEW vw_profile_recommendations AS
SELECT
    r.date_key,
    p.persona,
    p.business_goal,
    p.available_time_minutes,
    t.topic_name,
    c.channel_name,
    v.video_id,
    v.video_title,
    v.duration_seconds,
    f.view_count,
    f.engagement_rate,
    r.topical_affinity,
    r.recommendation_score,
    RANK() OVER (PARTITION BY r.date_key, r.profile_key ORDER BY r.recommendation_score DESC) AS recommendation_rank
FROM fact_profile_video_recommendations r
JOIN dim_user_profile p ON p.profile_key = r.profile_key
JOIN dim_video v ON v.video_key = r.video_key
JOIN dim_channel c ON c.channel_key = v.channel_key
JOIN dim_topic t ON t.topic_key = r.topic_key
JOIN fact_video_daily_metrics f ON f.date_key = r.date_key AND f.video_key = r.video_key;

CREATE OR REPLACE VIEW vw_daily_pipeline_summary AS
SELECT
    d.date_key,
    COUNT(DISTINCT fv.video_key) AS videos_loaded,
    COUNT(DISTINCT fv.channel_key) AS channels_loaded,
    COUNT(DISTINCT fv.topic_key) AS topics_loaded,
    SUM(fv.view_count) AS total_views,
    SUM(fv.like_count) AS total_likes,
    SUM(fv.comment_count) AS total_comments,
    AVG(fv.engagement_rate) AS avg_engagement_rate
FROM dim_date d
LEFT JOIN fact_video_daily_metrics fv ON fv.date_key = d.date_key
GROUP BY d.date_key;
