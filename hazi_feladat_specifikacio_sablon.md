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

> _Ide írja a leírást._ A tervezett pipeline youtube-ról és google-ról gyűjt valós statisztikákat, különböző usereknek. A userek vagy magánfelhasználók akiknek a rendelkezésre álló idejük alapján hozna létre egy megnézendő videók listát, azalapján hogy mennyi idejük van egy héten videót nézni, vagy KKV-knek hozna létre egy listát hogy a piacukon milyen trending videók vannak a héten amik alapján fejleszthetik a marketingjüket. A pipeline valós friss adatokat a YouTube Data API-t és Google Trends segítségével valósítom meg.

---

## 3. Tervezett pipeline elemei

| Elem | Tervezett megoldás / eszköz |
|---|---|
| **Adatforrások** *(min. 2)* | Youtube Data API, Google Trends, User CSV fájl |
| **Feldolgozási mód** | Batch |
| **Landing zone** *(nyers tároló)* | MinIO |
| **Adatmodell típusa** | pl. Csillag séma – 1 ténytábla + 2 dimenziótábla |
| **Adattárház / adatplatform** | pl. PostgreSQL, DuckDB, Snowflake |
| **Transzformáció** | pl. dbt, Pandas, Spark SQL |
| **Orchestration eszköz** | pl. Apache Airflow, Dagster, Prefect |
| **Infrastruktúra** | pl. Docker Compose, Terraform + AWS |
| **Adatkiszolgálás** | pl. SQL nézetek, Metabase dashboard, REST API |

---
