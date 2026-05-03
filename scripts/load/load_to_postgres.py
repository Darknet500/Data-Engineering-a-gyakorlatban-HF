import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

load_dotenv(PROJECT_ROOT / ".env")


def get_database_url():
    required_env_vars = [
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ]

    missing_vars = [name for name in required_env_vars if not os.getenv(name)]

    if missing_vars:
        raise ValueError(f"Missing PostgreSQL environment variables: {missing_vars}")

    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB"),
    )


def read_processed_csv(file_name):
    file_path = PROCESSED_DIR / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"Missing processed file: {file_path}")

    return pd.read_csv(file_path)


def truncate_tables(engine):
    truncate_sql = """
        TRUNCATE TABLE
            fact_video_daily_metrics,
            dim_video,
            dim_user_profile,
            dim_topic,
            dim_channel,
            dim_date
        RESTART IDENTITY CASCADE;
    """

    with engine.begin() as connection:
        connection.execute(text(truncate_sql))

    print("Truncated existing warehouse tables.")


def load_table(engine, table_name, file_name):
    df = read_processed_csv(file_name)

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    print(f"Loaded {len(df)} rows into {table_name}")


def main():
    print("DB:", os.getenv("POSTGRES_DB"))
    print("USER:", os.getenv("POSTGRES_USER"))
    print("PASSWORD:", os.getenv("POSTGRES_PASSWORD"))
    print("HOST:", os.getenv("POSTGRES_HOST", "localhost"))
    print("PORT:", os.getenv("POSTGRES_PORT", "5432"))


    engine = create_engine(get_database_url())

    truncate_tables(engine)

    load_order = [
        ("dim_date", "dim_date.csv"),
        ("dim_channel", "dim_channel.csv"),
        ("dim_topic", "dim_topic.csv"),
        ("dim_user_profile", "dim_user_profile.csv"),
        ("dim_video", "dim_video.csv"),
        ("fact_video_daily_metrics", "fact_video_daily_metrics.csv"),
    ]

    for table_name, file_name in load_order:
        load_table(engine, table_name, file_name)

    print("PostgreSQL load finished successfully.")


if __name__ == "__main__":
    main()