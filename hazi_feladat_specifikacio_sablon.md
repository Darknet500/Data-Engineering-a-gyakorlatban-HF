# Házi Feladat Specifikáció
**Data Engineering** - Opcionális házi feladat

---

## 1. Hallgató adatai

| | |
|---|---|
| **Név** |Katona Benedek|
| **Neptun-kód** |LNU506|
| **E-mail** |b3n3d3k.katona@gmail.com|

---

## 2. Témaválasztás

| | |
|---|---|
| **Választott téma** |Közösségi média aktivitás|

**Rövid leírás** *(2–4 mondat: milyen üzleti/elemzési kérdést old meg a pipeline? Milyen forrásadatokból indul ki, és milyen eredményt produkál?)*

 A pipeline célja, hogy YouTube Data API és Google Trends adatokból heti szintű közösségimédia-trend elemzést készítsen. A rendszer különböző témákhoz vagy piaci kategóriákhoz kapcsolódó videókat, nézettségi és engagement mutatókat, valamint keresési trendeket gyűjt, majd ezeket egy elemzésre kész adattárházba tölti. Az eredmény egy lekérdezhető adatmodell, amely alkalmas arra, hogy feltárja, mely témák és videók trendelnek adott időszakban, illetve hogyan kapcsolódnak ezek különböző felhasználói vagy üzleti profilokhoz.

---

## 3. Tervezett pipeline elemei

| Elem | Tervezett megoldás / eszköz |
|---|---|
| **Adatforrások** *(min. 2)* | Youtube Data API, Google Trends adatkinyerés pythreads segítségével, User CSV fájl |
| **Feldolgozási mód** | Batch Daily |
| **Landing zone** *(nyers tároló)* | MinIO |
| **Adatmodell típusa** | Csillag séma 1 fact table: video_daily_metrics, 5 dim table: dim_vide, dim_chanel, dim_topic, dim_date, dim_user_Profile|
| **Adattárház / adatplatform** | SQL, pandas|
| **Transzformáció** | Pandas, PostgreSQL |
| **Orchestration eszköz** | Apache Airflow |
| **Infrastruktúra** | Docker Compose  |
| **Adatkiszolgálás** | SQL nézetek, Metabase dashboard |

---
