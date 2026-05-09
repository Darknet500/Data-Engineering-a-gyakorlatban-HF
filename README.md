# Social Media Trend Pipeline

**Katona Benedek – LNU506**
Data Engineering a Gyakorlatban – Opcionális Házi Feladat

End-to-end batch data engineering pipeline that ingests **YouTube Data API** and **NewsAPI** data daily, transforms it into a star-schema data warehouse in PostgreSQL, stores raw and processed artefacts in MinIO (S3-compatible), and serves analytical results via SQL views and a Metabase dashboard. The entire stack runs locally through **Docker Compose**.

---

## Architecture

```
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  YouTube Data    │  │    NewsAPI       │  │  user_profiles   │
│  API  (REST)     │  │   (REST API)     │  │    (CSV file)    │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │                     │                      │
         └──────────┬──────────┘                      │
                    ▼                                  │
          ┌─────────────────┐                         │
          │  Landing Zone   │                         │
          │  MinIO  "raw"   │  ← JSON files           │
          └────────┬────────┘                         │
                   │                                  │
                   ▼                                  │
          ┌──────────────────────────────────────┐    │
          │   Pandas Transform (build_star_schema)│◄───┘
          │   • null handling / type coercion     │
          │   • engagement_rate calculation       │
          │   • news mention counting             │
          │   • trend_score aggregation           │
          │   • recommendation scoring            │
          └───────────────┬──────────────────────┘
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
                             │  SQL Analytical Views │
                             │  + Metabase Dashboard │
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
| BI dashboard | Metabase |
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

This starts: PostgreSQL, MinIO (with bucket creation), Airflow (init + webserver + scheduler), Metabase.

First-time build takes a few minutes because the Airflow image is compiled with all Python dependencies.

### 3. Verify services are healthy

```bash
docker compose ps
```

All containers should show `healthy` or `running`. Typically ready within 60–90 seconds.

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
Initial setup wizard runs on first visit. Connect to PostgreSQL:
- Host: `postgres`, Port: `5432`, Database: `dehf`, User: `dehf`, Password: `dehf`

**MinIO Console** – http://localhost:9001 (login: `minioadmin` / `minioadmin`)

**PostgreSQL direct** (from host):
