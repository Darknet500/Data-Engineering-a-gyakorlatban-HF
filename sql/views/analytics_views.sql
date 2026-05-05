DROP VIEW IF EXISTS vw_profile_recommendations;
DROP VIEW IF EXISTS vw_top_videos;
DROP VIEW IF EXISTS vw_topic_trends;

CREATE VIEW vw_topic_trends AS
SELECT
    d.date_key,
    d.year,
    d.month,
    d.week,
    t.topic_name,
    f.youtube_video_count,
    f.youtube_total_views,
    f.youtube_total_likes,
    f.youtube_total_comments,
    f.news_article_count,
    f.avg_engagement_rate,
    f.topic_trend_score
FROM fact_topic_daily_metrics f
JOIN dim_date d
    ON f.date_key = d.date_key
JOIN dim_topic t
    ON f.topic_key = t.topic_key;

CREATE VIEW vw_top_videos AS
SELECT
    d.date_key,
    d.year,
    d.month,
    d.week,
    t.topic_name,
    c.channel_name,
    v.title,
    v.video_id,
    v.publish_date,
    v.duration_seconds,
    f.views,
    f.likes,
    f.comments,
    f.engagement_rate,
    ft.topic_trend_score
FROM fact_video_daily_metrics f
JOIN dim_date d
    ON f.date_key = d.date_key
JOIN dim_topic t
    ON f.topic_key = t.topic_key
JOIN dim_channel c
    ON f.channel_key = c.channel_key
JOIN dim_video v
    ON f.video_key = v.video_key
LEFT JOIN fact_topic_daily_metrics ft
    ON f.date_key = ft.date_key
    AND f.topic_key = ft.topic_key;

CREATE VIEW vw_profile_recommendations AS
SELECT
    d.date_key,
    d.year,
    d.month,
    d.week,
    u.profile_name,
    u.preferred_topics,
    u.available_time_minutes,
    t.topic_name,
    c.channel_name,
    v.title,
    v.video_id,
    v.duration_seconds,
    fvd.views,
    fvd.likes,
    fvd.comments,
    fvd.engagement_rate,
    r.topic_affinity,
    r.recommendation_score
FROM fact_profile_video_recommendations r
JOIN dim_date d
    ON r.date_key = d.date_key
JOIN dim_user_profile u
    ON r.user_profile_key = u.user_profile_key
JOIN dim_topic t
    ON r.topic_key = t.topic_key
JOIN dim_video v
    ON r.video_key = v.video_key
JOIN dim_channel c
    ON v.channel_key = c.channel_key
LEFT JOIN fact_video_daily_metrics fvd
    ON r.date_key = fvd.date_key
    AND r.video_key = fvd.video_key;
