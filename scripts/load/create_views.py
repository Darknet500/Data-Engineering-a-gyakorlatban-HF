from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


PROJECT_ROOT = Path(__file__).resolve().parents[2]
VIEWS_FILE = PROJECT_ROOT / "sql" / "views" / "analytics_views.sql"

load_dotenv(PROJECT_ROOT / ".env")


def get_database_url() -> URL:
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


def main() -> None:
    if not VIEWS_FILE.exists():
        raise FileNotFoundError(f"Missing SQL views file: {VIEWS_FILE}")

    sql = VIEWS_FILE.read_text(encoding="utf-8")

    engine = create_engine(get_database_url())

    with engine.begin() as connection:
        connection.exec_driver_sql(sql)

    print("Analytics views created successfully.")


if __name__ == "__main__":
    main()