from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow"

DEFAULT_ARGS = {
    "owner": "dehf",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="social_media_trend_pipeline",
    description="Daily YouTube + NewsAPI social media trend data pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dehf", "youtube", "newsapi", "star-schema"],
) as dag:
    extract_youtube = BashOperator(
        task_id="extract_youtube",
        bash_command=f"python {PROJECT_ROOT}/scripts/extract/youtube_extract.py",
    )

    extract_news = BashOperator(
        task_id="extract_news",
        bash_command=f"python {PROJECT_ROOT}/scripts/extract/news_extract.py",
    )

    upload_raw_to_minio = BashOperator(
        task_id="upload_raw_to_minio",
        bash_command=f"python {PROJECT_ROOT}/scripts/load/upload_to_minio.py --zone raw",
    )

    transform_star_schema = BashOperator(
        task_id="transform_star_schema",
        bash_command=f"python {PROJECT_ROOT}/scripts/transform/build_star_schema.py",
    )

    validate_outputs = BashOperator(
        task_id="validate_outputs",
        bash_command=f"python {PROJECT_ROOT}/scripts/validate/validate_pipeline_outputs.py",
    )

    upload_processed_to_minio = BashOperator(
        task_id="upload_processed_to_minio",
        bash_command=f"python {PROJECT_ROOT}/scripts/load/upload_to_minio.py --zone processed",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python {PROJECT_ROOT}/scripts/load/load_to_postgres.py",
    )

    create_analytics_views = BashOperator(
        task_id="create_analytics_views",
        bash_command=f"python {PROJECT_ROOT}/scripts/load/create_views.py",
    )

    [extract_youtube, extract_news] >> upload_raw_to_minio >> transform_star_schema
    transform_star_schema >> validate_outputs >> upload_processed_to_minio >> load_to_postgres >> create_analytics_views
