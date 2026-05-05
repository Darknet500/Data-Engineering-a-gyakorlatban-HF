from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VIEWS_SQL = PROJECT_ROOT / "sql" / "views" / "analytics_views.sql"


def database_url() -> str:
    user = os.getenv("POSTGRES_USER", "dehf")
    password = os.getenv("POSTGRES_PASSWORD", "dehf")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "dehf")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    for char in sql:
        if char == "'":
            in_single_quote = not in_single_quote
        if char == ";" and not in_single_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    if not VIEWS_SQL.exists():
        raise FileNotFoundError(f"Missing analytics view SQL: {VIEWS_SQL}")
    engine = create_engine(database_url())
    sql_text = VIEWS_SQL.read_text(encoding="utf-8")
    statements = split_sql(sql_text)
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    print(f"Created/refreshed {len(statements)} analytics view statements from {VIEWS_SQL}")


if __name__ == "__main__":
    main()
