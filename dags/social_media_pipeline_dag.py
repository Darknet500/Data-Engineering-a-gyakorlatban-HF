from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


SCRIPTS_DIR = "/opt/airflow/scripts"


default_args = {
    "owner": "benedek",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="social_media_trend_pipeline",
    description="Daily YouTube and NewsAPI trend pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-engineering", "youtube", "newsapi"],
) as dag:
    extract_youtube = BashOperator(
        task_id="extract_youtube",
        bash_command=f"python {SCRIPTS_DIR}/extract/youtube_extract.py",
    )

    extract_news = BashOperator(
        task_id="extract_news",
        bash_command=f"python {SCRIPTS_DIR}/extract/news_extract.py",
    )

    upload_raw_to_minio = BashOperator(
        task_id="upload_raw_to_minio",
        bash_command=f"python {SCRIPTS_DIR}/load/upload_to_minio.py --zone raw",
    )

    transform_star_schema = BashOperator(
        task_id="transform_star_schema",
        bash_command=f"python {SCRIPTS_DIR}/transform/build_star_schema.py",
    )

    validate_outputs = BashOperator(
        task_id="validate_outputs",
        bash_command=f"python {SCRIPTS_DIR}/validate/validate_pipeline_outputs.py",
    )

    upload_processed_to_minio = BashOperator(
        task_id="upload_processed_to_minio",
        bash_command=f"python {SCRIPTS_DIR}/load/upload_to_minio.py --zone processed",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python {SCRIPTS_DIR}/load/load_to_postgres.py",
    )

    create_analytics_views = BashOperator(
        task_id="create_analytics_views",
        bash_command=f"python {SCRIPTS_DIR}/load/create_views.py",
    )

    [extract_youtube, extract_news] >> upload_raw_to_minio
    upload_raw_to_minio >> transform_star_schema
    transform_star_schema >> validate_outputs
    validate_outputs >> upload_processed_to_minio
    upload_processed_to_minio >> load_to_postgres
    load_to_postgres >> create_analytics_views
