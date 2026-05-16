"""
Metabase automated provisioning script.

Runs once after Metabase starts to:
  1. Complete the first-time setup wizard (create admin user)
  2. Add the PostgreSQL warehouse as a database connection
  3. Create four analytical questions (one per SQL view)
  4. Create a dashboard and place all four questions on it
"""

import os
import sys
import time
import requests

# ── Config from environment ────────────────────────────────────────────────────
METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@dehf.local")
ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "Admin1234!")
SITE_NAME = os.getenv("METABASE_SITE_NAME", "DEHF Analytics")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "dehf")
PG_USER = os.getenv("POSTGRES_USER", "dehf")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dehf")

# ── Questions to create (name, SQL, display type, visualization settings) ──────
QUESTIONS = [
    {
        "name": "Topic Trends – Daily Rankings",
        "sql": """
SELECT
    date_key,
    topic_name,
    video_count,
    news_article_count,
    total_views,
    avg_engagement_rate,
    trend_score,
    daily_topic_rank
FROM vw_topic_trends
ORDER BY date_key DESC, daily_topic_rank
LIMIT 100
""".strip(),
        "display": "bar",
        "visualization_settings": {
            "graph.dimensions": ["topic_name"],
            "graph.metrics": ["trend_score"],
            "graph.x_axis.title_text": "Topic",
            "graph.y_axis.title_text": "Trend Score",
        },
    },
    {
        "name": "Top Videos – Views & Engagement",
        "sql": """
SELECT
    date_key,
    topic_name,
    channel_name,
    video_title,
    view_count,
    like_count,
    comment_count,
    engagement_rate,
    daily_view_rank
FROM vw_top_videos
WHERE daily_view_rank <= 20
ORDER BY date_key DESC, daily_view_rank
""".strip(),
        "display": "table",
        "visualization_settings": {
            "table.pivot": False,
            "column_settings": {
                '["name","engagement_rate"]': {"number_style": "percent", "decimals": 2}
            },
        },
    },
    {
        "name": "Profile Recommendations – Top 3 per Persona",
        "sql": """
SELECT
    date_key,
    persona,
    business_goal,
    topic_name,
    video_title,
    duration_seconds,
    recommendation_score,
    recommendation_rank
FROM vw_profile_recommendations
WHERE recommendation_rank <= 3
ORDER BY date_key DESC, persona, recommendation_rank
""".strip(),
        "display": "table",
        "visualization_settings": {"table.pivot": False},
    },
    {
        "name": "Daily Pipeline Summary",
        "sql": """
SELECT
    date_key,
    videos_loaded,
    channels_loaded,
    topics_loaded,
    total_views,
    total_likes,
    total_comments,
    ROUND(avg_engagement_rate::numeric, 4) AS avg_engagement_rate
FROM vw_daily_pipeline_summary
ORDER BY date_key DESC
LIMIT 30
""".strip(),
        "display": "line",
        "visualization_settings": {
            "graph.dimensions": ["date_key"],
            "graph.metrics": ["total_views", "videos_loaded"],
            "graph.x_axis.title_text": "Date",
            "graph.y_axis.title_text": "Count",
        },
    },
]

# Dashboard grid layout: 2-column, 2-row, each card takes half the width
CARD_LAYOUT = [
    {"col": 0, "row": 0, "size_x": 12, "size_y": 8},
    {"col": 12, "row": 0, "size_x": 12, "size_y": 8},
    {"col": 0, "row": 8, "size_x": 12, "size_y": 8},
    {"col": 12, "row": 8, "size_x": 12, "size_y": 8},
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def wait_for_metabase(timeout: int = 180) -> None:
    print(f"Waiting for Metabase at {METABASE_URL} …")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                print("Metabase is up.")
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    sys.exit("Timed out waiting for Metabase to become healthy.")


def get_setup_token() -> str | None:
    r = requests.get(f"{METABASE_URL}/api/session/properties", timeout=10)
    r.raise_for_status()
    return r.json().get("setup-token")


def initial_setup(token: str) -> str:
    """Complete the first-time wizard; returns the session token."""
    payload = {
        "token": token,
        "user": {
            "first_name": "Admin",
            "last_name": "DEHF",
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
            "site_name": SITE_NAME,
        },
        "prefs": {"site_name": SITE_NAME, "allow_tracking": False},
    }
    r = requests.post(f"{METABASE_URL}/api/setup", json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def login() -> str:
    r = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["id"]


def add_database(session: str) -> int:
    headers = {"X-Metabase-Session": session}
    # Check if already added
    r = requests.get(f"{METABASE_URL}/api/database", headers=headers, timeout=10)
    r.raise_for_status()
    for db in r.json().get("data", []):
        if db.get("engine") == "postgres" and db["details"].get("dbname") == PG_DB:
            print(f"Database '{PG_DB}' already connected (id={db['id']}).")
            return db["id"]

    payload = {
        "engine": "postgres",
        "name": "DEHF Warehouse",
        "details": {
            "host": PG_HOST,
            "port": PG_PORT,
            "dbname": PG_DB,
            "user": PG_USER,
            "password": PG_PASSWORD,
            "ssl": False,
            "tunnel-enabled": False,
        },
        "auto_run_queries": True,
        "is_full_sync": True,
    }
    r = requests.post(
        f"{METABASE_URL}/api/database", json=payload, headers=headers, timeout=30
    )
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"Database added (id={db_id}).")
    return db_id


def sync_database(session: str, db_id: int) -> None:
    """Trigger a metadata sync so views appear in the query builder."""
    headers = {"X-Metabase-Session": session}
    requests.post(
        f"{METABASE_URL}/api/database/{db_id}/sync_schema",
        headers=headers,
        timeout=10,
    )
    print("Database sync triggered.")


def create_question(session: str, db_id: int, q: dict) -> int:
    headers = {"X-Metabase-Session": session}
    # Skip if a card with the same name already exists
    r = requests.get(f"{METABASE_URL}/api/card", headers=headers, timeout=10)
    r.raise_for_status()
    for card in r.json():
        if card.get("name") == q["name"]:
            print(f"  Question '{q['name']}' already exists (id={card['id']}).")
            return card["id"]

    payload = {
        "name": q["name"],
        "dataset_query": {
            "type": "native",
            "native": {"query": q["sql"]},
            "database": db_id,
        },
        "display": q["display"],
        "visualization_settings": q["visualization_settings"],
    }
    r = requests.post(
        f"{METABASE_URL}/api/card", json=payload, headers=headers, timeout=30
    )
    r.raise_for_status()
    card_id = r.json()["id"]
    print(f"  Created question '{q['name']}' (id={card_id}).")
    return card_id


def create_dashboard(session: str, card_ids: list[int]) -> int:
    headers = {"X-Metabase-Session": session}
    dashboard_name = "Social Media Analytics"

    # Skip if already exists
    r = requests.get(f"{METABASE_URL}/api/dashboard", headers=headers, timeout=10)
    r.raise_for_status()
    for dash in r.json():
        if dash.get("name") == dashboard_name:
            print(f"Dashboard '{dashboard_name}' already exists (id={dash['id']}).")
            return dash["id"]

    r = requests.post(
        f"{METABASE_URL}/api/dashboard",
        json={"name": dashboard_name},
        headers=headers,
        timeout=15,
    )
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"Dashboard created (id={dash_id}).")

    for card_id, layout in zip(card_ids, CARD_LAYOUT):
        payload = {"cardId": card_id, **layout}
        r = requests.post(
            f"{METABASE_URL}/api/dashboard/{dash_id}/dashcard",
            json=payload,
            headers=headers,
            timeout=15,
        )
        if not r.ok:
            # Older Metabase versions use /cards instead of /dashcard
            r = requests.post(
                f"{METABASE_URL}/api/dashboard/{dash_id}/cards",
                json=payload,
                headers=headers,
                timeout=15,
            )
        r.raise_for_status()
        print(f"  Added card {card_id} to dashboard.")

    return dash_id


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    wait_for_metabase()

    setup_token = get_setup_token()
    if setup_token:
        print("Running first-time setup …")
        session = initial_setup(setup_token)
    else:
        print("Setup already done, logging in …")
        session = login()

    print("Adding database connection …")
    db_id = add_database(session)
    sync_database(session, db_id)

    # Brief pause so the sync can register the views
    time.sleep(5)

    print("Creating questions …")
    card_ids = [create_question(session, db_id, q) for q in QUESTIONS]

    print("Building dashboard …")
    dash_id = create_dashboard(session, card_ids)

    print(
        f"\nDone! Open Metabase at {METABASE_URL} and navigate to "
        f"Dashboard #{dash_id} to see the pre-built analytics."
    )


if __name__ == "__main__":
    main()
