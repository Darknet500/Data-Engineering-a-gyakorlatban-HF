CREATE OR REPLACE VIEW vw_topic_trends AS
SELECT
    d.date_key,
    t.topic_name,
    COUNT(DISTINCT v.video_id) AS video_count,
    SUM(f.views) AS total_views,
    SUM(f.likes) AS total_likes,
    SUM(f.comments) AS total_comments,
    AVG(f.engagement_rate) AS avg_engagement_rate,
    AVG(f.trend_score) AS avg_trend_score
FROM fact_video_daily_metrics f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_topic t ON f.topic_key = t.topic_key
JOIN dim_video v ON f.video_key = v.video_key
GROUP BY
    d.date_key,
    t.topic_name;


CREATE OR REPLACE VIEW vw_top_videos AS
SELECT
    d.date_key,
    t.topic_name,
    c.channel_name,
    v.title,
    v.video_id,
    f.views,
    f.likes,
    f.comments,
    f.engagement_rate,
    f.trend_score
FROM fact_video_daily_metrics f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_topic t ON f.topic_key = t.topic_key
JOIN dim_channel c ON f.channel_key = c.channel_key
JOIN dim_video v ON f.video_key = v.video_key;


CREATE OR REPLACE VIEW vw_profile_recommendations AS
SELECT
    d.date_key,
    u.profile_name,
    u.preferred_topics,
    t.topic_name,
    c.channel_name,
    v.title,
    v.video_id,
    f.views,
    f.engagement_rate,
    f.trend_score
FROM fact_video_daily_metrics f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_user_profile u ON f.user_profile_key = u.user_profile_key
JOIN dim_topic t ON f.topic_key = t.topic_key
JOIN dim_channel c ON f.channel_key = c.channel_key
JOIN dim_video v ON f.video_key = v.video_key;