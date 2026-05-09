# Technikai Dokumentáció – Social Media Trend Pipeline

**Hallgató:** Katona Benedek (LNU506)
---

## 1. Architektúra és tervezési döntések

### 1.1 Áttekintés

A pipeline célja napi rendszerességgel összegyűjteni és elemzésre alkalmassá tenni a YouTube és a hírmedia által generált, közösségi-médiával kapcsolatos adatokat. A rendszer három szimulált felhasználói profil számára személyre szabott videóajánlókat és trendpontszámokat is kiszámol.

A teljes stack helyi Docker Compose-alapú környezetben fut, amely bármely gépen reprodukálhatóan elindítható.

### 1.2 Komponensek és tervezési indoklás

| Komponens | Választott eszköz | Indoklás |
|---|---|---|
| Orchestration | Apache Airflow 2.9.3 | Iparági szabvány Python-alapú orkesztrációs eszköz; DAG-ok segítségével a lépések közötti függőségek és az idempotens újrafuttatás egyszerűen megvalósítható |
| Landing zone | MinIO | S3-kompatibilis objektumtároló; lokálisan futtatható,  a nyers JSON és a feldolgozott CSV fájlok elkülönülten kerülnek tárolásra |
| Adattárház | PostgreSQL 15 | Megbízható relációs adatbázis; a csillag séma SQL-ben természetesen fejezhető ki; könnyen csatlakoztatható Metabase-hez |
| Transzformáció | Pandas | Könnyen olvasható, gyorsan fejleszthető Python-könyvtár batch transzformációhoz |
| BI dashboard | Metabase | Nyílt forráskódú, PostgreSQL-lel azonnal összeköthető BI eszköz; nulla konfigurációval futtatható Docker konténerben |
| Infrastruktúra | Docker Compose | Egyetlen paranccsal (`docker compose up --build`) elindítható, reprodukálható környezet; az összes konfiguráció verzókezelésben tárolható |

### 1.3 Adatforrások

1. **YouTube Data API v3** (REST API, strukturált JSON)
   Az API a keresési lekérdezésekre videó-azonosítókat ad vissza, amelyeket egy második kérés bővít statisztikákkal (`viewCount`, `likeCount`, `commentCount`) és metaadatokkal (`snippet`, `contentDetails`). Témakörönként külön JSON fájl készül.

2. **NewsAPI** (REST API, félig strukturált JSON)
   A napi hírszabadoszlopból az API cikkcímeket, leírásokat és tartalmakat ad vissza. Az extrakciós szkript egész napi lekérdezési ablakban hívja az API-t.

3. **user_profiles.csv** (statikus CSV fájl)
   A három felhasználói profil (Junior Data Engineer, ML Startup Founder, Marketing Analyst) személyre szabott metaadatokat tartalmaz: érdeklődési körök, preferált témák, rendelkezésre álló percek és üzleti cél. Ez a forrás a rekomendációs pontszám kiszámítását teszi lehetővé.


---

## 2. Adatmodell – Csillag séma

A transzformáció eredménye egy csillag séma PostgreSQL-ben, amely **3 ténytáblát** és **5 dimenziótáblát** tartalmaz.

### 2.1 Dimenziótáblák

```
dim_date
  date_key (PK, DATE)
  year, month, day
  day_of_week
  is_weekend

dim_channel
  channel_key (PK, INT)
  channel_id  (UNIQUE TEXT)
  channel_name

dim_topic
  topic_key (PK, INT)
  topic_name (UNIQUE TEXT)
  topic_category

dim_user_profile
  profile_key (PK, INT)
  profile_id  (UNIQUE INT)
  persona
  interests
  preferred_topics
  available_time_minutes
  business_goal

dim_video
  video_key (PK, INT)
  video_id   (UNIQUE TEXT)
  video_title
  published_at (TIMESTAMPTZ)
  duration_seconds
  channel_key (FK → dim_channel)
  topic_key   (FK → dim_topic)
```

### 2.2 Ténytáblák

```
fact_video_daily_metrics                  ← fő ténytábla
  fact_video_daily_metrics_key (PK)
  date_key    (FK → dim_date)
  video_key   (FK → dim_video)
  channel_key (FK → dim_channel)
  topic_key   (FK → dim_topic)
  view_count, like_count, comment_count
  engagement_rate                         ← (likes+comments)/views

fact_topic_daily_metrics                  ← aggregált témakör-metrikák
  fact_topic_daily_metrics_key (PK)
  date_key   (FK → dim_date)
  topic_key  (FK → dim_topic)
  video_count, news_article_count
  total_views, total_likes, total_comments
  avg_engagement_rate
  trend_score                             ← video_count×2 + news×3 + engagement×100

fact_profile_video_recommendations        ← személyre szabott ajánlók
  fact_profile_video_recommendations_key (PK)
  date_key    (FK → dim_date)
  profile_key (FK → dim_user_profile)
  video_key   (FK → dim_video)
  topic_key   (FK → dim_topic)
  topical_affinity
  recommendation_score                    ← 0–100 pont
```


### 2.3 ER-diagram

```
dim_date ──────────────────┐
                           │ date_key
dim_channel ──────────┐    │
                      │    ▼
dim_topic ────────┐   │  fact_video_daily_metrics
                  │   │    │ video_key
dim_video ────────┼───┘    │
  ├── channel_key ┘        │
  └── topic_key ──┘        │
                           │
dim_date ──────────────────┤
                           │ date_key
dim_topic ─────────────────► fact_topic_daily_metrics
                           │
dim_date ──────────────────┤
                           │ date_key
dim_user_profile ──────────► fact_profile_video_recommendations
dim_video ─────────────────┘
dim_topic ─────────────────┘
```
![ER Diagram](ER-diagram.png)

### 2.4 Aggregációk és adattisztítás

A `build_star_schema.py` szkript a következő lépéseket hajtja végre:

1. **Null kezelés:** `view_count`, `like_count`, `comment_count` → 0 ha hiányzik; `topic_name` → `"unknown"` ha üres.
2. **Típuskonverzió:** ISO 8601 időbélyegek → `datetime` objektumok; YouTube ISO 8601 duration (`PT12M30S`) → egész másodpercek.
3. **Engagement rate:** `(likes + comments) / views` per videó.
4. **Hírszámolás:** Az NewsAPI cikkek szövegéből megszámoljuk, hány cikk tartalmazza az adott témakör kulcsszavait.
5. **Trend score:** `video_count × 2 + news_article_count × 3 + avg_engagement_rate × 100` – a három komponens ötvözi a videós és híres aktivitást, súlyozva a valós interakciók arányát.
6. **Rekomendációs pontszám:** Témabeli affinitás (45%), kulcsszó-egyezés (15%), időilleszkedés (15%), népszerűség (15%), engagement (10%) – 0–100 skálán.

---

## 3. Pipeline futása és eredmény

### 3.1 Airflow DAG

A `social_media_trend_pipeline` DAG napi ütemezéssel (`@daily`) fut, `catchup=False` beállítással (tehát nem fut vissza a múltba). Az összes task `BashOperator`-ral hívja meg a Python szkripteket, amelyek a `/opt/airflow/scripts/` könyvtárból töltődnek be.

**Taskok sorrendje és függőségei:**
![**Taskok sorrendje és függőségei:**](taskOrder.png)


**Idempotencia:** A `load_to_postgres.py` az összes ténytáblát és dimenziótáblát `TRUNCATE … RESTART IDENTITY CASCADE` utasítással üríti a betöltés előtt, így a DAG újrafuttatva ugyanazt az állapotot produkálja.

### 3.2 Minőségellenőrzés

A `validate_pipeline_outputs.py` a következőket ellenőrzi:
- Minden szükséges feldolgozott CSV fájl létezik és nem üres.
- Egyediségi kényszerek: minden dimenzió- és ténytábla elsődleges kulcsa egyedi.
- Nem negatív metrikák: nézettség, engagement, trend score ≥ 0.
- Referenciális integritás: idegenkulcs-hivatkozások teljessége (date_key, video_key, channel_key, topic_key, profile_key).

### 3.3 Mintaeredmény (demo módban)

Demo módban (`DEMO_MODE=true`) 3 téma × 2 videó = 6 szintetikus videórekord és 5 hír kerül betöltésre.

**Mintakimenet – `vw_topic_trends`:**

```
date_key   | topic_name              | video_count | news_article_count | trend_score | rank
-----------+-------------------------+-------------+--------------------+-------------+-----
2026-05-09 | artificial intelligence |           2 |                  2 |      10.xxx |    1
2026-05-09 | data engineering        |           2 |                  3 |      10.xxx |    2
2026-05-09 | python                  |           2 |                  2 |       9.xxx |    3
```

**Mintakimenet – `vw_profile_recommendations` (Top-3 per persona):**

```
persona              | topic_name              | video_title                          | recommendation_score | rank
---------------------+-------------------------+--------------------------------------+----------------------+-----
Junior Data Engineer | data engineering        | Demo: Introduction to Data_Engineering|                 ...  |    1
ML Startup Founder   | artificial intelligence | Demo: Advanced Artificial_Intelligence|                 ...  |    1
Marketing Analyst    | python                  | Demo: Introduction to Python          |                 ...  |    1
```

### 3.4 SQL nézetek

Az alábbi négy view jön létre automatikusan a `create_analytics_views` task során:

| View neve | Leírás |
|---|---|
| `vw_topic_trends` | Napi témakör-trendek, rangsorolva trend score szerint |
| `vw_top_videos` | Napi legtöbbet nézett videók, csatorna- és témakör-metaadatokkal |
| `vw_profile_recommendations` | Személyre szabott videóajánlók profilonként |
| `vw_daily_pipeline_summary` | Napi pipeline-összesítő (videók, csatornák, nézettség) |

---

## 4. Reprodukálhatóság

Az infrastruktúra teljes egészében Docker Compose-ban van definiálva. A telepítési lépések:

```bash
git clone <repo_url>
cd data-engineering-a-gyakorlatban-hf
cp .env.example .env
# .env szerkesztése (API kulcsok VAGY DEMO_MODE=true)
docker compose up --build -d
```

A PostgreSQL DDL (`sql/ddl/schema.sql`) automatikusan lefut az adatbázis első indulásakor a `docker-entrypoint-initdb.d/` mechanizmus révén. A MinIO vödröket a `minio-create-bucket` init-konténer hozza létre. Az Airflow adatbázis-migrálást és az admin felhasználó létrehozását az `airflow-init` konténer végzi el.

Egyetlen külső függőség az API kulcsok (vagy `DEMO_MODE=true`).
