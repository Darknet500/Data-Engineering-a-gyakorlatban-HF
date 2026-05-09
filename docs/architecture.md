# Technikai Dokumentáció – Social Media Trend Pipeline

**Hallgató:** Katona Benedek (LNU506)
**Tárgy:** Data Engineering a Gyakorlatban – Opcionális Házi Feladat

---

## 1. Architektúra és tervezési döntések

### 1.1 Áttekintés

A pipeline célja napi rendszerességgel összegyűjteni és elemzésre alkalmassá tenni a YouTube és a hírmedia által generált, közösségi-médiával kapcsolatos adatokat. A rendszer három szimulált felhasználói profil számára személyre szabott videóajánlókat és trendpontszámokat is kiszámol.

A teljes stack helyi Docker Compose-alapú környezetben fut, amely bármely gépen reprodukálhatóan elindítható.

### 1.2 Komponensek és tervezési indoklás

| Komponens | Választott eszköz | Indoklás |
|---|---|---|
| Orchestration | Apache Airflow 2.9.3 | Iparági szabvány Python-alapú orkesztrációs eszköz; DAG-ok segítségével a lépések közötti függőségek és az idempotens újrafuttatás egyszerűen megvalósítható |
| Landing zone | MinIO | S3-kompatibilis objektumtároló; lokálisan futtatható, egyszerűen cserélhető éles AWS S3-ra; a nyers JSON és a feldolgozott CSV fájlok elkülönülten kerülnek tárolásra |
| Adattárház | PostgreSQL 15 | Megbízható relációs adatbázis; a csillag séma SQL-ben természetesen fejezhető ki; könnyen csatlakoztatható Metabase-hez |
| Transzformáció | Pandas | Könnyen olvasható, gyorsan fejleszthető Python-könyvtár batch transzformációhoz; a feladat méreténél (néhány száz sor/nap) a Spark-overhead nem indokolt |
| BI dashboard | Metabase | Nyílt forráskódú, PostgreSQL-lel azonnal összeköthető BI eszköz; nulla konfigurációval futtatható Docker konténerben |
| Infrastruktúra | Docker Compose | Egyetlen paranccsal (`docker compose up --build`) elindítható, reprodukálható környezet; az összes konfiguráció verzókezelésben tárolható |

### 1.3 Adatforrások

1. **YouTube Data API v3** (REST API, strukturált JSON)
   Az API a keresési lekérdezésekre videó-azonosítókat ad vissza, amelyeket egy második kérés bővít statisztikákkal (`viewCount`, `likeCount`, `commentCount`) és metaadatokkal (`snippet`, `contentDetails`). Témakörönként külön JSON fájl készül.

2. **NewsAPI** (REST API, félig strukturált JSON)
   A napi hírszabadoszlopból az API cikkcímeket, leírásokat és tartalmakat ad vissza. Az extrakciós szkript egész napi lekérdezési ablakban hívja az API-t.

3. **user_profiles.csv** (statikus CSV fájl)
   A három felhasználói profil (Junior Data Engineer, ML Startup Founder, Marketing Analyst) személyre szabott metaadatokat tartalmaz: érdeklődési körök, preferált témák, rendelkezésre álló percek és üzleti cél. Ez a forrás a rekomendációs pontszám kiszámítását teszi lehetővé.

A két külső API + egy lokális CSV fájl felel meg a "legalább két különböző adatforrás" feltételének; az adatok JSON és CSV formátumban érkeznek, teljesítve a strukturálatlan/szemisturkturált adatforrás követelményét.

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