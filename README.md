# Social Media Trend Pipeline

**Katona Benedek – LNU506**
Data Engineering a Gyakorlatban – Opcionális Házi Feladat

End-to-end batch data engineering pipeline that ingests **YouTube Data API** and **NewsAPI** data daily, transforms it into a star-schema data warehouse in PostgreSQL, stores raw and processed artefacts in MinIO (S3-compatible), and serves analytical results via SQL views and an auto-provisioned Metabase dashboard. The entire stack runs locally through **Docker Compose**.

---

## Architecture

```
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  YouTube Data    │  │    NewsAPI       │  │  user_profiles   │
│  API  (REST)     │  │   (REST API)     │  │    (CSV file)    │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │                     │                      │
         └──────────┬──────────┘                      │
                    ▼                                 │
          ┌─────────────────┐                         │
          │  Landing Zone   │                         │
          │  MinIO  "raw"   │  ← JSON files           │
          └────────┬────────┘                         │
                   │                                  │
                   ▼                                  │
          ┌─────────────────────────────────────┐     │
          │ Pandas Transform (build_star_schema)│◄────┘
          │ • null handling / type coercion     │
          │ • engagement_rate calculation       │
          │ • news mention counting             │
          │ • trend_score aggregation           │
          │ • recommendation scoring            │
          └───────────────┬─────────────────────┘
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
   ┌──────────────────┐      ┌──────────────────────┐
   │  Processed CSVs  │      │   PostgreSQL (dehf)  │
   │ MinIO "processed"│      │   Star Schema        │
   └──────────────────┘      │   (5 dims + 3 facts) │
                             └──────────┬───────────┘
                                        │
                             ┌──────────▼───────────┐
                             │ SQL Analytical Views │
                             │ + Metabase Dashboard │
                             │  (auto-provisioned)  │
                             └──────────────────────┘

                   All steps orchestrated by Apache Airflow
```

**Data flow summary:**

| Stage | Tool | Output |
|---|---|---|
| Extract | Python + APIs | JSON files in `data/raw/` |
| Landing zone | MinIO bucket `raw` | S3-compatible object storage |
| Transform | Pandas | CSVs in `data/processed/` |
| Processed store | MinIO bucket `processed` | S3-compatible object storage |
| Load | SQLAlchemy | Star schema in PostgreSQL |
| Serve | SQL views + Metabase | Analytical dashboards |
| Orchestrate | Apache Airflow DAG | Daily scheduled pipeline |

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.3 |
| Extraction | Python 3.11, `google-api-python-client`, `requests` |
| Transformation | Pandas |
| Landing zone | MinIO (S3-compatible) |
| Data warehouse | PostgreSQL 15 |
| BI dashboard | Metabase v0.49.10 (auto-provisioned) |
| Infrastructure | Docker Compose |

---

## Prerequisites

- **Docker** ≥ 24 and **Docker Compose** ≥ 2.20
- (Optional for real data) YouTube Data API v3 key and NewsAPI key

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/darknet500/data-engineering-a-gyakorlatban-hf.git
cd data-engineering-a-gyakorlatban-hf
cp .env.example .env
```

Edit `.env` and set your API keys **or** enable demo mode to run without keys:

```dotenv
# Option A – real API keys
YOUTUBE_API_KEY=AIza...
NEWS_API_KEY=abc123...

# Option B – synthetic demo data (no API keys needed)
DEMO_MODE=true
```

### 2. Build and start all services

```bash
docker compose up --build -d
```

This starts: PostgreSQL, MinIO (with bucket creation), Airflow (init + webserver + scheduler), Metabase, and the Metabase auto-provisioning init container.

First-time build takes a few minutes because the Airflow image is compiled with all Python dependencies.

### 3. Verify services are healthy

```bash
docker compose ps
```

All containers should show `healthy` or `running`. The `metabase_init` container will exit with code 0 once provisioning is complete (~2 minutes after startup).

### 4. Trigger the pipeline

Open the Airflow UI at **http://localhost:8080** (login: `airflow` / `airflow`).

1. Find the `social_media_trend_pipeline` DAG.
2. Toggle it **On** (the slider on the left).
3. Click **Trigger DAG** (▶) to start an immediate run.

The DAG executes eight tasks in sequence:

```
extract_youtube ──┐
                  ├──► upload_raw_to_minio ──► transform_star_schema
extract_news ─────┘         ──► validate_outputs ──► upload_processed_to_minio
                                    ──► load_to_postgres ──► create_analytics_views
```

### 5. Explore the data

**Metabase** – http://localhost:3000

Log in with the credentials from your `.env` (`METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD`). The **Social Media Analytics** dashboard is pre-built automatically by the `metabase-init` container — no manual setup required.

> **First run only:** if the `metabase_data` volume is brand-new, `metabase-init` completes the setup wizard programmatically. If Metabase was previously set up manually, it logs in with the existing credentials instead.

**MinIO Console** – http://localhost:9001 (login: `minioadmin` / `minioadmin`)

**PostgreSQL direct** (from host):
```bash
psql -h localhost -p 5432 -U dehf -d dehf
```

---

## Analytical Queries

Three documented queries against the star schema / SQL views:

### Query 1 – Top trending topics today

Ranks every topic by its composite trend score (video activity + news coverage + engagement).

```sql
SELECT
    topic_name,
    video_count,
    news_article_count,
    ROUND(avg_engagement_rate::numeric, 4)  AS avg_engagement,
    trend_score,
    daily_topic_rank
FROM vw_topic_trends
WHERE date_key = CURRENT_DATE
ORDER BY daily_topic_rank;
```

| topic_name | video_count | news_article_count | avg_engagement | trend_score | daily_topic_rank |
|---|---|---|---|---|---|
| artificial intelligence | 2 | 2 | 0.0312 | 10.12 | 1 |
| data engineering | 2 | 3 | 0.0280 | 10.08 | 2 |
| python | 2 | 2 | 0.0145 | 9.45 | 3 |

---

### Query 2 – Top 10 videos by engagement rate (last 7 days)

Finds the best-performing videos regardless of raw view count — useful for spotting high-quality niche content.

```sql
SELECT
    date_key,
    topic_name,
    channel_name,
    video_title,
    view_count,
    ROUND(engagement_rate::numeric, 4) AS engagement_rate,
    daily_view_rank
FROM vw_top_videos
WHERE date_key >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY engagement_rate DESC
LIMIT 10;
```

| date_key | topic_name | channel_name | video_title | view_count | engagement_rate | daily_view_rank |
|---|---|---|---|---|---|---|
| 2026-05-16 | data engineering | Demo Channel | Demo: Introduction to Data Engineering | 125 000 | 0.0520 | 3 |
| 2026-05-16 | python | Demo Channel | Demo: Introduction to Python | 98 000 | 0.0480 | 5 |

---

### Query 3 – Personalised top-3 recommendations per user persona

Shows the highest-scoring video recommendation for each of the three user profiles, with the score breakdown visible through supporting columns.

```sql
SELECT
    persona,
    business_goal,
    topic_name,
    video_title,
    duration_seconds,
    view_count,
    ROUND(engagement_rate::numeric, 4)      AS engagement_rate,
    ROUND(recommendation_score::numeric, 1) AS recommendation_score,
    recommendation_rank
FROM vw_profile_recommendations
WHERE date_key = CURRENT_DATE
  AND recommendation_rank <= 3
ORDER BY persona, recommendation_rank;
```

| persona | business_goal | topic_name | video_title | recommendation_score | recommendation_rank |
|---|---|---|---|---|---|
| Junior Data Engineer | Learn pipeline design | data engineering | Demo: Introduction to Data Engineering | 82.5 | 1 |
| Marketing Analyst | Content strategy | python | Demo: Introduction to Python | 74.0 | 1 |
| ML Startup Founder | Stay current on AI | artificial intelligence | Demo: Advanced Artificial Intelligence | 91.2 | 1 |

---

## Project Structure

```
├── dags/
│   └── social_media_pipeline_dag.py   # Airflow DAG (8 tasks, @daily)
├── scripts/
│   ├── extract/
│   │   ├── youtube_extract.py         # YouTube Data API v3 + demo mode
│   │   └── news_extract.py            # NewsAPI + demo mode
│   ├── transform/
│   │   └── build_star_schema.py       # Pandas: clean, aggregate, build schema
│   ├── load/
│   │   ├── load_to_postgres.py        # SQLAlchemy → PostgreSQL
│   │   ├── create_views.py            # CREATE OR REPLACE VIEW
│   │   └── upload_to_minio.py         # MinIO S3 upload
│   ├── validate/
│   │   └── validate_pipeline_outputs.py  # Referential integrity checks
│   └── setup/
│       └── metabase_setup.py          # Metabase auto-provisioning via REST API
├── sql/
│   ├── ddl/schema.sql                 # Table definitions + indexes
│   └── views/analytics_views.sql     # 4 analytical SQL views
├── data/
│   ├── input/user_profiles.csv        # Static user persona definitions
│   ├── raw/                           # Landing zone (YouTube + News JSON)
│   └── processed/                     # Transformed CSVs
├── docker/airflow/Dockerfile
├── docker-compose.yaml
├── .env.example
└── docs/
    ├── architecture.md
    ├── ER-diagram.png
    └── taskOrder.png
```

---

## Reproducibility notes

- **No hardcoded secrets** – all credentials live in `.env` (gitignored); `.env.example` provides the full template.
- **Idempotent pipeline** – `TRUNCATE … RESTART IDENTITY CASCADE` before every load; re-running the DAG produces the same result.
- **Demo mode** – set `DEMO_MODE=true` in `.env` to run the full pipeline with synthetic data and no external API keys.
- **Auto-provisioned Metabase** – `metabase-init` container wires up the DB connection and dashboard on first launch; no manual wizard required.
