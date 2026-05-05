from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

LOAD_ORDER = [
    "dim_date",
    "dim_channel",
    "dim_topic",
    "dim_user_profile",
    "dim_video",
    "fact_video_daily_metrics",
    "fact_topic_daily_metrics",
    "fact_profile_video_recommendations",
]


def database_url() -> str:
    user = os.getenv("POSTGRES_USER", "dehf")
    password = os.getenv("POSTGRES_PASSWORD", "dehf")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "dehf")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def read_table_csv(table_name: str) -> pd.DataFrame:
    path = PROCESSED_DIR / f"{table_name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing processed file for {table_name}: {path}")
    return pd.read_csv(path)


def load_table(engine, table_name: str, df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError(f"Refusing to load empty table: {table_name}")
    df.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    print(f"Loaded {len(df):>4} rows into {table_name}")


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    engine = create_engine(database_url())

    with engine.begin() as conn:
        tables = ", ".join(LOAD_ORDER[::-1])
        conn.execute(text(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE"))
        print("Truncated target star-schema tables")

    for table_name in LOAD_ORDER:
        df = read_table_csv(table_name)
        load_table(engine, table_name, df)

    print("PostgreSQL load finished")


if __name__ == "__main__":
    main()
