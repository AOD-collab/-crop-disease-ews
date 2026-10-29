
### D.4 `spark/batch_analytics.py` — analítika batch ho JDBC

**Fundamentu Spark ba seksaun ne'e (Teoria).** *JDBC* (Java Database Connectivity) mak padraun Java atu ko'alia ho base de dadus. Spark uza driver JDBC PostgreSQL atu **lee** tabela ba DataFrame (`spark.read.jdbc`) no **hakerek** DataFrame ba tabela (`df.write.mode(...).jdbc`). *Agregasaun* = kombina liña barak ba valór ida (konta, soma, média). `groupBy(...).agg(...)` = fahe liña ba grupu, depois agrega kada grupu. *Window function* = kalkula valór ba kada liña tuir grupu liña sira (ezemplu "númeru 1 iha grupu").

#### D.4.1 Docstring, import no konstante

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 1-21}}

**2. Objetivu**

Esplika job batch: kalkula númeru hotu ba dashboard husi PostgreSQL no hakerek tabela analítika. Define konstante ba fatin la iha naran, fonte teste, no regex atu fahe `DATABASE_URL`.

**3. Fundamentu Python**

- **`re.compile(padraun)`** — prepara regex dala ida atu uza dala barak.
- **Grupu regex** `( ... )` kaer parte testu; `(?: ... )?` = grupu la kaer no opsionál.
- **`import sys`** — `sys.exit(kódigu)` remata programa ho kódigu.
- **`argparse`** — lee argumentu liña komando (`--watch`).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–5** — Docstring: job batch Spark; kalkula figura dashboard husi PostgreSQL (images, predictions, users), hakerek tabela analítika. Imajen ida konta dala ida ho prediksaun foun liu; imajen teste la tama; loron = loron Timor-Leste. Presiza PostgreSQL (Spark lee ho JDBC).
- **Liña 7** — Oinsá halai: `python -m spark.batch_analytics [--watch]`.
- **Liña 8** — Remata docstring.
- **Liña 10** — `import argparse`.
- **Liña 11** — `import re`.
- **Liña 12** — `import sys`.
- **Liña 13** — `import time`.
- **Liña 15–17** — Husi `config.settings`: `DATABASE_URL`, `LOCAL_TZ_OFFSET_HOURS` (fuso Timor), `ANALYTICS_CHECK_SECONDS` (30), `ANALYTICS_MAX_AGE_SECONDS` (600).
- **Liña 19** — `UNKNOWN_LOCATION = "Unknown location"` — naran ba fatin mamuk.
- **Liña 20** — `SCALE_TEST_SOURCE = "scalability_test"` — komentáriu: hanesan `storage/database.SCALE_TEST_SOURCE`.
- **Liña 21** — `_POSTGRES_URL_RE = re.compile(r"postgresql\+psycopg2://(?:([^:]+):([^@]*)@)?([^/]+)/(.+)")` — grupu 4: utilizadór, password, host:port, naran base de dadus.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* (password la loos): `postgresql+psycopg2://ews:***@localhost:5432/crop_ews` → grupu `("ews", "***", "localhost:5432", "crop_ews")`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `UNKNOWN_LOCATION`, `SCALE_TEST_SOURCE` | `str` |
| `_POSTGRES_URL_RE` | `re.Pattern` |
| `LOCAL_TZ_OFFSET_HOURS` | `int` |
| `ANALYTICS_*_SECONDS` | `float` |

**7. Ligasaun OOP**

`_POSTGRES_URL_RE` mak objetu `re.Pattern` ho métodu `.match()`. Arkivu ne'e la define klase.

**8. Impaktu integrasaun**

`run_all.py` hahú `python -m spark.batch_analytics --watch` (Modulu 06). Tabela ne'ebé job hakerek dashboard lee iha `spark/analytics_reader.py`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `SCALE_TEST_SOURCE` importante?
**R:** Atu imajen teste eskalabilidade (`source = 'scalability_test'`) la tama iha estatístika agrikultór.

#### D.4.2 `jdbc_url_and_properties` no `build_spark`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 24-56}}

**2. Objetivu**

Konverte `DATABASE_URL` (formatu SQLAlchemy) ba URL JDBC no dict propriedade (driver, utilizadór, password) ne'ebé Spark presiza. Harii SparkSession ho driver JDBC PostgreSQL.

**3. Fundamentu Python**

- **`match.groups()`** — fó tuple ho grupu regex hotu; unpacking ba variavel 4.
- **`raise RuntimeError(...)`** ho string naruk iha parénteze (string adjasente kombina automátiku).
- **Return tuple** `return jdbc_url, properties`.

**4. Esplikasaun liña-ba-liña**

- **Liña 24** — `def jdbc_url_and_properties() -> tuple:`
- **Liña 25** — Docstring: `DATABASE_URL` → (URL JDBC, propriedade); hamosu erru se la'ós PostgreSQL.
- **Liña 26** — `match = _POSTGRES_URL_RE.match(DATABASE_URL)`.
- **Liña 27** — `if not match:` — la'ós PostgreSQL (ezemplu SQLite):
- **Liña 28–33** — `raise RuntimeError(...)` — mensajen: job presiza PostgreSQL; leitór JDBC Spark presiza driver JDBC loloos; projetu la lori jar SQLite JDBC.
- **Liña 34** — `user, password, host_port, dbname = match.groups()`.
- **Liña 35** — `jdbc_url = f"jdbc:postgresql://{host_port}/{dbname}"` — formatu URL JDBC.
- **Liña 36** — `properties = {"driver": "org.postgresql.Driver"}` — klase driver Java.
- **Liña 37** — `if user:`
- **Liña 38** — `properties["user"] = user`.
- **Liña 39** — `if password is not None:` — (password mamuk `""` mós aseita).
- **Liña 40** — `properties["password"] = password`.
- **Liña 41** — `return jdbc_url, properties`.
- **Liña 44** — `def build_spark():`
- **Liña 45** — `from pyspark.sql import SparkSession`.
- **Liña 46** — `spark = (`
- **Liña 47** — `SparkSession.builder`.
- **Liña 48** — `.appName("ImageSystem-CropDiseaseEWS-Analytics")`.
- **Liña 49** — `.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")` — download driver JDBC PostgreSQL 42.7.3.
- **Liña 50** — Komentáriu: loron foti iha Timor-Leste Time esplísitu (`_local_day`).
- **Liña 51** — `.config("spark.sql.session.timeZone", "UTC")` — Spark trata tempu hotu iha UTC.
- **Liña 52** — `.config("spark.ui.showConsoleProgress", "false")`.
- **Liña 53** — `.getOrCreate()`.
- **Liña 54** — `)`.
- **Liña 55** — `spark.sparkContext.setLogLevel("WARN")`.
- **Liña 56** — `return spark`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `DATABASE_URL = "postgresql+psycopg2://ews:***@localhost:5432/crop_ews"` → `("jdbc:postgresql://localhost:5432/crop_ews", {"driver": "org.postgresql.Driver", "user": "ews", "password": "***"})`. Ho `DATABASE_URL = "sqlite:///ews.db"` → `RuntimeError`.

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `jdbc_url_and_properties()` | `tuple[str, dict[str, str]]` | `RuntimeError` |
| `build_spark()` | `SparkSession` | erru JVM |

**7. Ligasaun OOP**

Builder SparkSession (haree D.1.2). Not implemented in the current codebase: klase ba konfigurasaun JDBC — dict simples mak uza.

**8. Impaktu integrasaun**

Rezultadu uza iha `read_inputs` no `write_tables`. Password mai husi variavel ambiente/`config/settings.py` — dokumentu ne'e la hatudu valór loloos.

**9. Checkpoint ba principiante**

**P:** Tanba sá URL SQLAlchemy (`postgresql+psycopg2://`) la bele uza direta iha Spark?
**R:** Spark uza Java JDBC, ne'ebé presiza formatu `jdbc:postgresql://host:port/db` no driver Java `org.postgresql.Driver`.

#### D.4.3 Query input, `read_inputs`, `_local_day` no `_location`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 59-89}}

**2. Objetivu**

Define query SQL ne'ebé Spark lee (koluna presiza de'it — **nunka** email ka hash password), lee tabela 4 ho JDBC, no funsaun ajuda rua: loron Timor-Leste no limpeza fatin.

**3. Fundamentu Python**

- **String multi-liña** `"""..."""` no **f-string** multi-liña.
- **Generator iha `tuple(...)`**: `tuple(f(x) for x in (...))` harii tuple husi rezultadu kada elementu.
- **Import lokál** iha funsaun Spark.

**4. Esplikasaun liña-ba-liña**

- **Liña 59** — Komentáriu seksaun "inputs".
- **Liña 60** — Komentáriu: koluna presiza de'it (nunka email ka hash password) — privasidade.
- **Liña 61–63** — `_IMAGES_SQL` — subquery husi `images`: `image_id`, `user_id`, `location`, `status` (cast ba `VARCHAR(32)` tanba enum PostgreSQL), tempu 4; `WHERE source <> 'scalability_test'`; alias `i`.
- **Liña 64–67** — `_PREDICTIONS_SQL` — `prediction_id`, `image_id`, `predicted_class`, `risk_score`, `risk_level` (cast `VARCHAR(16)`), `predicted_at`, `processing_latency_ms`; alias `p`.
- **Liña 68** — `_USERS_SQL` — `user_id`, `is_deleted` husi `users`.
- **Liña 69–70** — `_SCALE_TEST_ITEMS_SQL` — `run_id`, `status`, `predicted_class`, `risk_level`, `location` husi `scale_test_items`.
- **Liña 73** — `def read_inputs(spark, jdbc_url, properties) -> tuple:`
- **Liña 74–75** — `return tuple(spark.read.jdbc(jdbc_url, sql, properties=properties) for sql in (...))` — DataFrame 4 tuir orden: images, predictions, users, scale_test_items. JDBC simu subquery `(SELECT ...) AS x` hanesan "tabela".
- **Liña 78** — Komentáriu seksaun "computation".
- **Liña 79** — `def _local_day(column):`
- **Liña 80** — Docstring: loron kalendáriu Timor-Leste husi koluna tempu.
- **Liña 81** — Import `col`, `expr`, `to_date`.
- **Liña 82** — `return to_date(col(column) + expr(f"INTERVAL {int(LOCAL_TZ_OFFSET_HOURS)} HOURS"))` — tempu UTC + oras fuso (Timor = +9) → data.
- **Liña 85** — `def _location():`
- **Liña 86** — Docstring: fatin limpa hanesan streaming; mamuk → `UNKNOWN_LOCATION`.
- **Liña 87** — Import funsaun Spark.
- **Liña 88** — `loc = initcap(regexp_replace(trim(coalesce(col("location"), lit(""))), r"\s+", " "))` — null → `""`, trim, espasu ida, Title Case.
- **Liña 89** — `return when(loc == "", lit(UNKNOWN_LOCATION)).otherwise(loc)`.

**5. Walkthrough ezekusaun**

- `_local_day("predicted_at")` ba `2026-10-20 16:30:00 UTC` → `16:30 + 9 h = 2026-10-21 01:30` → data `2026-10-21` (loron Timor, la'ós loron UTC).
- `_location()` ba `"  aileu  "` → `"Aileu"`; ba `null` → `"Unknown location"`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `_IMAGES_SQL` ... | `str` |
| `read_inputs` return | `tuple[DataFrame, DataFrame, DataFrame, DataFrame]` |
| `_local_day(column: str)` | return `Column` (tipu `date`) |
| `_location()` | return `Column` (`string`) |

**7. Ligasaun OOP**

`spark.read` fó objetu `DataFrameReader`; `.jdbc(...)` mak nia métodu. `when(...).otherwise(...)` — chaining métodu objetu `Column`. Projetu la define klase.

**8. Impaktu integrasaun**

Lee husi tabela PostgreSQL ne'ebé streaming job hakerek (Modulu 04). Regra fatin hanesan ho `parse_events` liña 108, entaun "Aileu" iha streaming no analítika hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá `_local_day` aumenta oras 9?
**R:** Tempu rai iha UTC. Fotografia ne'ebé foti 01:30 dadeer iha Timor mak 16:30 loron uluk iha UTC. Aumenta fuso hatudu loron loos ba agrikultór Timor.

#### D.4.4 `photos_with_latest_prediction` no funsaun ajuda agregasaun

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 92-127}}

**2. Objetivu**

Kombina imajen ho nia prediksaun **foun liu** (imajen ida = liña ida), no funsaun ajuda ki'ik ne'ebé harii agregasaun: konta kondisionál, konta tuir grupu, risku aas tuir grupu, no aumenta `generated_at`.

**3. Fundamentu Python**

- **`*keys`** — funsaun simu koluna grupu barak: `_count_photos(df, "location", "day")` → `keys = ("location", "day")`; `groupBy(*keys)` haruka hotu fali.
- **`from pyspark.sql import functions as F`** — naran badak `F`.

**4. Esplikasaun liña-ba-liña**

- **Liña 92** — `def photos_with_latest_prediction(images, predictions):`
- **Liña 93** — Docstring: liña ida kada imajen ho prediksaun foun liu (null se la iha).
- **Liña 94** — `from pyspark.sql import Window`.
- **Liña 95** — `from pyspark.sql.functions import col, row_number`.
- **Liña 96–97** — `newest = Window.partitionBy("image_id").orderBy(col("predicted_at").desc_nulls_last(), col("prediction_id").desc())` — janela: grupu tuir `image_id`, orden foun → tuan (null iha ikus), empate → `prediction_id` boot liu.
- **Liña 98** — `latest = (predictions.withColumn("_rn", row_number().over(newest))` — númeru 1, 2, 3... iha kada grupu.
- **Liña 99** — `.filter(col("_rn") == 1).drop("_rn", "prediction_id"))` — hela númeru 1 de'it; soe koluna ajuda.
- **Liña 100** — `return images.join(latest, "image_id", "left")` — *left join*: imajen hotu mantein; imajen la ho prediksaun hetan null.
- **Liña 103** — `def _count_if(cond):`
- **Liña 104** — Docstring: númeru liña ne'ebé `cond` loos.
- **Liña 105** — `from pyspark.sql import functions as F`.
- **Liña 106** — `return F.sum(F.when(cond, 1).otherwise(0)).cast("long")` — 1 se loos, 0 se lae, soma.
- **Liña 109** — `def _count_photos(df, *keys):`
- **Liña 110** — Docstring: liña kada `keys`, iha koluna `photos`.
- **Liña 111** — Import `F`.
- **Liña 112** — `return df.groupBy(*keys).agg(F.count("*").alias("photos"))`.
- **Liña 115** — `def _high_risk_by(df, *keys):`
- **Liña 116** — Docstring: kada `keys`: total analiza, high (risku HIGH) no high_pct.
- **Liña 117** — Import `F`.
- **Liña 118** — `return (df.filter(F.col("risk_level").isNotNull()).groupBy(*keys)` — imajen ho risku de'it.
- **Liña 119** — `.agg(_count_if(F.col("risk_level") == "HIGH").alias("high"), F.count("*").alias("total"))`.
- **Liña 120** — `.withColumn("high_pct", F.bround(F.col("high") / F.col("total") * 100, 1)))` — persentajen ho desimál 1 (*banker's rounding* `bround`).
- **Liña 123** — `def _stamped(tables: dict) -> dict:`
- **Liña 124** — Docstring: aumenta koluna `generated_at`.
- **Liña 125** — Import `F`.
- **Liña 126** — `now = F.current_timestamp()`.
- **Liña 127** — `return {name: df.withColumn("generated_at", now) for name, df in tables.items()}`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* — prediksaun ba imajen `a1`:

| prediction_id | predicted_at | predicted_class | `_rn` |
|---|---|---|---|
| 7 | 2026-10-20 02:00 | blight | 1 |
| 3 | 2026-10-19 08:00 | healthy | 2 |

Hela liña `_rn = 1` (blight). `_high_risk_by(located, "location")` ba Aileu ho risku 10 (HIGH 3) → `high=3, total=10, high_pct=30.0`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return |
|---|---|---|
| `photos_with_latest_prediction` | DataFrame rua | DataFrame |
| `_count_if` | `Column` | `Column` (`long`) |
| `_count_photos` | DataFrame, `*keys: str` | DataFrame (`keys` + `photos`) |
| `_high_risk_by` | DataFrame, `*keys` | DataFrame (`keys`, `high`, `total`, `high_pct`) |
| `_stamped` | `dict[str, DataFrame]` | `dict[str, DataFrame]` |

**7. Ligasaun OOP**

`Window` mak klase PySpark; `partitionBy(...).orderBy(...)` fó objetu `WindowSpec`. Funsaun ajuda sira hatudu **abstrasaun** (prinsípiu dezeñu): kódigu `compute` uza naran klaru (`_count_if`) no la repete espresaun naruk. Ne'e funsaun, la'ós klase.

**8. Impaktu integrasaun**

`refresh` bolu `photos_with_latest_prediction` (liña 228). Regra "prediksaun foun liu" hanesan ho `LATEST_PREDICTION_CTE` iha `storage/database.py` (dashboard), entaun númeru Spark no lista dashboard hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá *left* join no la'ós join normál?
**R:** Atu imajen ne'ebé seidauk iha prediksaun (ezemplu `FAILED` ka iha prosesu) mós konta iha total imajen.

#### D.4.5 `compute` — figura prinsipál dashboard

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 130-167}}

**2. Objetivu**

Kalkula distribuisaun moras, moras ne'ebé deteta barak liu, no tabela sumáriu ho figura prinsipál hotu (total, healthy, diseased, uncertain, risku aas, submisaun 24h, latênsia média...).

**3. Fundamentu Python**

- **Tuple assignment** `is_healthy, is_uncertain = cls == "healthy", cls == "uncertain"`.
- **Funsaun lokál** `avg_seconds(later, earlier)`.
- **Argumentu barak** iha `agg(...)` — kada ida koluna rezultadu ida.

**4. Esplikasaun liña-ba-liña**

- **Liña 130** — `def compute(photos, users) -> dict:`
- **Liña 131** — Docstring: `{naran tabela: DataFrame}` ba tabela analítika agrikultór hotu.
- **Liña 132** — Import `F`.
- **Liña 134** — `cls = F.lower(F.col("predicted_class"))` — klase letra ki'ik.
- **Liña 135** — `has_class = F.col("predicted_class").isNotNull()`.
- **Liña 136** — `is_healthy, is_uncertain = cls == "healthy", cls == "uncertain"`.
- **Liña 138** — `def avg_seconds(later, earlier):`
- **Liña 139** — `diff = F.col(later).cast("double") - F.col(earlier).cast("double")` — timestamp → segundu (double), subtrai.
- **Liña 140** — `return F.bround(F.avg(diff), 1)` — média, desimál 1.
- **Liña 142** — `distribution = _count_photos(photos.filter(has_class), "predicted_class")` — fotografia kada klase.
- **Liña 143** — `most_detected = (distribution.filter(F.lower(F.col("predicted_class")) != "healthy")` — moras de'it.
- **Liña 144** — `.orderBy(F.col("photos").desc(), F.col("predicted_class"))` — barak liu uluk; empate → alfabétiku.
- **Liña 145** — `.limit(1).select(F.col("predicted_class").alias("most_detected_disease")))`.
- **Liña 147** — `recent = F.col("uploaded_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")` — upload iha oras 24 ikus.
- **Liña 148** — `totals = photos.agg(` — agrega DataFrame tomak (liña ida).
- **Liña 149** — `F.count("*").alias("total_images"),`
- **Liña 150** — `_count_if(has_class & is_healthy).alias("total_healthy"),`
- **Liña 151** — `_count_if(has_class & ~is_healthy & ~is_uncertain).alias("total_diseased"),`
- **Liña 152** — `_count_if(has_class & is_uncertain).alias("total_uncertain"),`
- **Liña 153** — `_count_if(~has_class).alias("total_unclassified"),`
- **Liña 154** — `_count_if(F.col("risk_level") == "HIGH").alias("total_high_risk"),`
- **Liña 155** — `_count_if(F.col("predicted_at").isNotNull()).alias("total_processed"),`
- **Liña 156** — `_count_if(F.col("status") == "FAILED").alias("total_failed"),`
- **Liña 157** — `_count_if(recent).alias("submissions_24h"),`
- **Liña 158** — `F.countDistinct(F.when(recent, F.col("user_id"))).alias("active_farmers_24h"),` — agrikultór diferente iha 24h.
- **Liña 159** — `F.bround(F.avg("processing_latency_ms"), 1).alias("avg_processing_latency_ms"),`
- **Liña 160** — `avg_seconds("uploaded_at", "captured_at").alias("avg_capture_to_upload_seconds"),`
- **Liña 161** — `avg_seconds("ingested_at", "uploaded_at").alias("avg_upload_to_ingestion_seconds"),`
- **Liña 162** — `avg_seconds("predicted_at", "uploaded_at").alias("avg_end_to_end_latency_seconds"),`
- **Liña 163** — `)`.
- **Liña 164** — `active_users = users.agg(_count_if(~F.coalesce(F.col("is_deleted"), F.lit(False))).alias("total_users"))` — utilizadór la hamoos.
- **Liña 165** — `summary = (totals.crossJoin(active_users)` — kombina liña ida ho liña ida.
- **Liña 166** — `.join(most_detected, how="left")` — komentáriu: la iha liña → null.
- **Liña 167** — `.withColumn("summary_id", F.lit(1)))` — chave fixu 1.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fotografia 10: healthy 4, blight 3, gray_leaf_spot 2, uncertain 1; HIGH 3. `distribution` = `[(healthy,4),(blight,3),(gray_leaf_spot,2),(uncertain,1)]`. `most_detected` = `blight` (la konta healthy; 3 > 2 > 1). `totals`: `total_images=10, total_healthy=4, total_diseased=5, total_uncertain=1, total_unclassified=0, total_high_risk=3`.

Nota: `most_detected` la hasai `"uncertain"` — se uncertain barak liu moras hotu, nia bele sai `most_detected_disease`. Ne'e hahalok kódigu atuál (Implementadu).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `photos`, `users` | DataFrame |
| `cls`, `has_class`, `is_healthy`, `is_uncertain`, `recent` | `Column` |
| `distribution`, `most_detected`, `totals`, `active_users`, `summary` | DataFrame |
| `summary` | liña ida, koluna 17: 14 husi `totals` + `total_users` + `most_detected_disease` + `summary_id` (`generated_at` aumenta iha `_stamped`) |

**7. Ligasaun OOP**

Hotu mak objetu DataFrame/Column PySpark ho métodu chaining. Projetu la define klase.

**8. Impaktu integrasaun**

`summary` sai tabela `analytics_summary` → `analytics_reader.kpi_summary()` → kartaun KPI iha dashboard.

**9. Checkpoint ba principiante**

**P:** Tanba sá `crossJoin` seguru iha ne'e?
**R:** `totals` no `active_users` iha liña **ida** kada ida, entaun crossJoin fó liña ida de'it.

#### D.4.6 `compute` (parte 2) no `compute_scale_test`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 169-201}}

**2. Objetivu**

Kalkula figura tuir fatin no loron (tendénsia), tabela sumáriu loron-loron kada fatin, no fó fali dict ho tabela 7. `compute_scale_test` kalkula tabela 3 ba pájina teste eskalabilidade kada `run_id`.

**3. Fundamentu Python**

- **Dict literal** ho chave = naran tabela, valór = DataFrame.
- **Bolu funsaun iha valór dict** (`_count_photos(...)`).

**4. Esplikasaun liña-ba-liña**

- **Liña 169** — `located = photos.withColumn("location", _location())` — fatin limpu.
- **Liña 170** — `classified = (located.filter(F.col("predicted_at").isNotNull() & has_class)` — fotografia klasifika ona.
- **Liña 171** — `.withColumn("day", _local_day("predicted_at")))` — loron Timor.
- **Liña 172** — `location_daily = (located.withColumn("summary_date", _local_day("uploaded_at"))` — loron upload.
- **Liña 173** — `.groupBy("location", "summary_date")`.
- **Liña 174** — `.agg(F.count("*").alias("total_images"),`
- **Liña 175** — `_count_if(F.col("predicted_class") == "healthy").alias("total_healthy"),`
- **Liña 176–177** — `_count_if(has_class & (F.col("predicted_class") != "healthy") & (F.col("predicted_class") != "uncertain")).alias("total_diseased"),`
- **Liña 178** — `F.avg("risk_score").alias("avg_risk_score"),`
- **Liña 179** — `_count_if(F.col("risk_level") == "HIGH").alias("high_risk_count")))`.
- **Liña 181** — `return _stamped({` — tabela hotu ho `generated_at`:
- **Liña 182** — `"analytics_summary": summary,`
- **Liña 183** — `"analytics_disease_distribution": distribution,`
- **Liña 184** — `"analytics_risk_distribution": _count_photos(photos.filter(F.col("risk_level").isNotNull()), "risk_level"),` — kuantu LOW/MEDIUM/HIGH.
- **Liña 185** — `"analytics_high_risk_by_location": _high_risk_by(located, "location"),`
- **Liña 186** — `"analytics_disease_trend": _count_photos(classified, "location", "day", "predicted_class"),`
- **Liña 187** — `"analytics_disease_daily": _count_photos(classified, "day", "predicted_class"),`
- **Liña 188** — `"batch_location_daily_summary": location_daily,`
- **Liña 189** — `})`.
- **Liña 192** — `def compute_scale_test(items) -> dict:`
- **Liña 193** — Docstring: tabela ba pájina teste, kada `run_id`.
- **Liña 194** — Import `F`.
- **Liña 195** — `done = (items.filter((F.col("status") == "COMPLETED") & F.col("predicted_class").isNotNull())` — item remata ho klase.
- **Liña 196** — `.withColumn("location", _location()))`.
- **Liña 197** — `return _stamped({`
- **Liña 198** — `"analytics_scale_test_disease_by_location": _count_photos(done, "run_id", "location", "predicted_class"),`
- **Liña 199** — `"analytics_scale_test_disease_distribution": _count_photos(done, "run_id", "predicted_class"),`
- **Liña 200** — `"analytics_scale_test_high_risk_by_location": _high_risk_by(done, "run_id", "location"),`
- **Liña 201** — `})`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fotografia 3 iha Aileu, upload 2026-10-21 (loron Timor): healthy 1, blight 2 (risk 0.97, 0.85, healthy 0.05). `batch_location_daily_summary` → `("Aileu", 2026-10-21, total_images=3, total_healthy=1, total_diseased=2, avg_risk_score≈0.623, high_risk_count=2)`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `located`, `classified`, `location_daily`, `done` | DataFrame |
| `compute` return | `dict[str, DataFrame]` — chave 7 |
| `compute_scale_test` return | `dict[str, DataFrame]` — chave 3 |

**7. Ligasaun OOP**

Not implemented in the current codebase: klase ba tabela analítika iha Spark — dict naran → DataFrame mak uza.

**8. Impaktu integrasaun**

`refresh` kombina dict rua (`{**a, **b}`) no haruka ba `write_tables`. Naran chave tenke hanesan ho naran tabela iha `storage/schema.sql`/`storage/database.py` (Modulu 04), tanba `write_tables` uza `CREATE TABLE ... (LIKE naran)`.

**9. Checkpoint ba principiante**

**P:** Diferensa entre `analytics_disease_trend` no `analytics_disease_daily`?
**R:** `trend` grupu tuir fatin + loron + klase; `daily` grupu tuir loron + klase de'it (fatin hotu hamutuk).

#### D.4.7 `write_tables`, `refresh` no `run` — JDBC write sink

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 204-247}}

**2. Objetivu**

Hakerek DataFrame hotu ba PostgreSQL. Mekanizmu: (1) harii tabela `<naran>_staging` mamuk ho estrutura hanesan; (2) Spark hakerek ho JDBC `mode("append")` ba staging; (3) iha **transasaun ida**, hamoos tabela loloos, kopia husi staging, no soe staging. Dashboard nunka lee tabela mamuk.

**3. Fundamentu Python**

- **`with engine.begin() as conn:`** — transasaun SQLAlchemy: `COMMIT` se bloku remata di'ak, `ROLLBACK` se erru.
- **`try/finally`** — `photos.unpersist()` halai beibeik.
- **`spark or build_spark()`** — se `spark` mak `None`, harii foun.
- **`owns_spark`** — hatene se funsaun ne'e mak harii sesaun (no tenke taka).

**4. Esplikasaun liña-ba-liña**

- **Liña 204** — Komentáriu seksaun "output".
- **Liña 205** — `def write_tables(tables: dict, jdbc_url, properties) -> None:`
- **Liña 206–207** — Docstring: hakerek ba kópia `_staging`, depois troka hotu iha transasaun ida, atu dashboard nunka lee tabela mamuk.
- **Liña 208** — `from sqlalchemy import text`.
- **Liña 209** — `from storage.database import engine`.
- **Liña 210** — `with engine.begin() as conn:` — transasaun 1.
- **Liña 211** — `for name in tables:`
- **Liña 212** — `conn.execute(text(f"DROP TABLE IF EXISTS {name}_staging"))` — hamoos staging tuan.
- **Liña 213** — `conn.execute(text(f"CREATE TABLE {name}_staging (LIKE {name})"))` — kopia estrutura (koluna + tipu).
- **Liña 214** — `for name, df in tables.items():`
- **Liña 215** — `df.write.mode("append").jdbc(jdbc_url, f"{name}_staging", properties=properties)` — **Spark JDBC write**: hatama liña DataFrame ba staging (`INSERT` batch husi partisaun kada ida).
- **Liña 216** — `with engine.begin() as conn:` — transasaun 2 (atómiku).
- **Liña 217** — `for name in tables:`
- **Liña 218** — `conn.execute(text(f"DELETE FROM {name}"))`.
- **Liña 219** — `conn.execute(text(f"INSERT INTO {name} SELECT * FROM {name}_staging"))`.
- **Liña 220** — `conn.execute(text(f"DROP TABLE {name}_staging"))`.
- **Liña 223** — `def refresh(spark) -> float:`
- **Liña 224** — Docstring: rekalkula tomak dala ida; fó segundu.
- **Liña 225** — `started = time.monotonic()`.
- **Liña 226** — `jdbc_url, properties = jdbc_url_and_properties()`.
- **Liña 227** — `images, predictions, users, scale_test_items = read_inputs(spark, jdbc_url, properties)`.
- **Liña 228** — `photos = photos_with_latest_prediction(images, predictions).cache()` — `cache()` rai rezultadu iha memória tanba tabela barak uza nia.
- **Liña 229** — `try:`
- **Liña 230** — `write_tables({**compute(photos, users), **compute_scale_test(scale_test_items)}, jdbc_url, properties)` — kombina tabela 10 no hakerek.
- **Liña 231** — `finally:`
- **Liña 232** — `photos.unpersist()` — liberta memória cache.
- **Liña 233** — `return time.monotonic() - started`.
- **Liña 236** — `def run(spark=None) -> float:`
- **Liña 237** — Docstring: kalkula dala ida.
- **Liña 238** — `from storage.database import init_db`.
- **Liña 239** — `jdbc_url_and_properties()` — komentáriu: falla lalais ba SQLite molok hahú JVM.
- **Liña 240** — `init_db()` — komentáriu: tabela analítika iha ho nia tipu.
- **Liña 241** — `owns_spark = spark is None`.
- **Liña 242** — `spark = spark or build_spark()`.
- **Liña 243** — `try:`
- **Liña 244** — `return refresh(spark)`.
- **Liña 245** — `finally:`
- **Liña 246** — `if owns_spark:`
- **Liña 247** — `spark.stop()` — taka sesaun ne'ebé funsaun harii.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* ba tabela `analytics_risk_distribution`: (1) `DROP TABLE IF EXISTS analytics_risk_distribution_staging`; `CREATE TABLE analytics_risk_distribution_staging (LIKE analytics_risk_distribution)`. (2) Spark JDBC hatama `[(LOW, 5, t), (MEDIUM, 2, t), (HIGH, 3, t)]`. (3) Transasaun: `DELETE FROM analytics_risk_distribution` → `INSERT ... SELECT * FROM ..._staging` → `DROP TABLE ..._staging`. Dashboard ne'ebé lee durante pasu 3 haree dadus tuan (transasaun seidauk commit) ka foun — nunka mamuk.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception / efeitu |
|---|---|---|---|
| `write_tables` | `dict`, `str`, `dict` | `None` | DDL + DML iha PostgreSQL; erru SQL/JDBC sa'e |
| `refresh` | `SparkSession` | `float` (segundu) | erru sa'e (watch kaer) |
| `run` | `SparkSession ka None` | `float` | `RuntimeError` (la'ós PostgreSQL) |

**7. Ligasaun OOP**

`df.write` fó objetu `DataFrameWriter`; `.mode("append").jdbc(...)` mak builder/chaining. `engine` mak objetu SQLAlchemy `Engine` (partilla husi `storage/database.py`). `conn` mak objetu `Connection` iha context manager transasaun.

**8. Impaktu integrasaun**

Ne'e mak **mekanizmu loloos Spark hakerek ba PostgreSQL**: JDBC `mode("append")` ba tabela staging, depois SQLAlchemy troka. Streaming job (D.2) **la** uza Spark JDBC — nia uza SQLAlchemy liuhusi `persist_result`. Spark JDBC `mode("overwrite")` Not implemented in the current codebase (kódigu uza staging + transasaun atu evita tabela mamuk).

**9. Checkpoint ba principiante**

**P:** Tanba sá la hakerek direta ho `mode("overwrite")` ba tabela loloos?
**R:** `overwrite` hamoos tabela uluk, depois hakerek — durante tempu ne'e dashboard bele haree tabela mamuk. Staging + transasaun troka hotu dala ida.

#### D.4.8 `data_signature` no `watch`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 250-293}}

**2. Objetivu**

`data_signature` kalkula "impresaun digitál" badak husi dadus (kontajen no tempu máximu). `watch` kontrola impresaun ne'e kada 30 s; se muda, ka liu 600 s desde kálkulu ikus ("24h ikus" muda ho tempu), nia halai `refresh`.

**3. Fundamentu Python**

- **`tuple(a) + tuple(b) + (x, y)`** — kombina tuple.
- **Komparasaun tuple** `signature != last_signature`.
- **`except KeyboardInterrupt:`** — Ctrl+C.
- **`flush=True`** iha `print` — hakerek kedas ba konsola.
- **`time.strftime('%H:%M:%S')`** — formatu oras.

**4. Esplikasaun liña-ba-liña**

- **Liña 250** — Komentáriu seksaun "watch".
- **Liña 251** — `def data_signature() -> tuple:`
- **Liña 252** — Docstring: impresaun barata; muda bainhira buat ruma ne'ebé figura uza muda.
- **Liña 253** — `from sqlalchemy import func` — funsaun SQL (`count`, `max`).
- **Liña 254** — Import `SessionLocal`, `Image`, `Prediction`, `User`, `ScaleTestItem`.
- **Liña 255** — `with SessionLocal() as s:`
- **Liña 256–257** — `images = s.query(func.count(Image.image_id), func.max(Image.created_at), func.max(Image.ingested_at), func.max(Image.processed_at)).one()` — liña ida ho valór 4.
- **Liña 258** — `predictions = s.query(func.count(Prediction.prediction_id), func.max(Prediction.predicted_at)).one()`.
- **Liña 259** — `users = s.query(func.count(User.user_id)).scalar()` — valór ida.
- **Liña 260** — `active_users = s.query(func.count(User.user_id)).filter(User.is_deleted.is_(False)).scalar()`.
- **Liña 261** — `scale_test = s.query(func.count(ScaleTestItem.image_id), func.max(ScaleTestItem.completed_at)).one()`.
- **Liña 262–263** — `scale_test_done = (s.query(func.count(ScaleTestItem.image_id)).filter(ScaleTestItem.status == "COMPLETED").scalar())`.
- **Liña 264** — `return tuple(images) + tuple(predictions) + (users, active_users) + tuple(scale_test) + (scale_test_done,)` — tuple ho valór 11.
- **Liña 267** — `def watch() -> int:`
- **Liña 268–269** — Docstring: rekalkula bainhira dadus muda, no minimu kada `ANALYTICS_MAX_AGE_SECONDS`.
- **Liña 270** — `from storage.database import init_db`.
- **Liña 271** — `jdbc_url_and_properties()` — falla lalais se la'ós PostgreSQL.
- **Liña 272** — `init_db()`.
- **Liña 273** — `spark = build_spark()` — sesaun ida ba loop tomak.
- **Liña 274–275** — `print(...)` — "Watching the database every 30s; recomputing ... when it changes".
- **Liña 276** — `last_signature, last_run = None, 0.0`.
- **Liña 277** — `try:`
- **Liña 278** — `while True:`
- **Liña 279** — `try:`
- **Liña 280** — `signature = data_signature()`.
- **Liña 281** — `stale = time.monotonic() - last_run >= ANALYTICS_MAX_AGE_SECONDS` — tuan liu 600 s?
- **Liña 282** — `if signature != last_signature or stale:`
- **Liña 283** — `seconds = refresh(spark)`.
- **Liña 284** — `last_signature, last_run = signature, time.monotonic()`.
- **Liña 285–286** — `print(...)` — "HH:MM:SS analytics recomputed (N photos) in X.Xs".
- **Liña 287** — `except Exception as e:` — komentáriu: base de dadus ka Spark problema; koko fali.
- **Liña 288** — `print(f"[spark-analytics] recompute failed, retrying: {e}", flush=True)`.
- **Liña 289** — `time.sleep(ANALYTICS_CHECK_SECONDS)` — hein 30 s.
- **Liña 290** — `except KeyboardInterrupt:`
- **Liña 291** — `return 0`.
- **Liña 292** — `finally:`
- **Liña 293** — `spark.stop()`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* volta 1: `last_signature = None` → refresh → `[spark-analytics] 09:00:00 analytics recomputed (120 photos) in 8.4s`. Volta 2 (30 s depois): la iha fotografia foun, `stale = False` → la halo buat ida. Fotografia foun mai → `count(Image)` muda 120 → 121 → refresh. Se PostgreSQL mate → "recompute failed, retrying" no koko fali 30 s depois.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `data_signature` return | `tuple` ho elementu 11 (`int`, `datetime ka None`) |
| `signature`, `last_signature` | `tuple ka None` |
| `last_run` | `float` |
| `stale` | `bool` |
| `watch` return | `int` (0) |

**7. Ligasaun OOP**

`s.query(...)` — métodu objetu `Session`; `Image.image_id` mak atributu **klase** ORM (koluna), la'ós atributu instánsia. Not implemented in the current codebase: padraun *Observer* (notifikasaun base de dadus) — kódigu uza *polling* (husu kada 30 s).

**8. Impaktu integrasaun**

`run_all.py` hahú `--watch`. Kada streaming job hakerek prediksaun foun, impresaun muda no analítika atualiza iha ~30 s.

**9. Checkpoint ba principiante**

**P:** Tanba sá iha `stale` maske dadus la muda?
**R:** Figura "submissions_24h" no "active_farmers_24h" muda ho tempu: fotografia ne'ebé tama oras 25 liubá tenke sai husi figura maske la iha dadus foun.

#### D.4.9 `main` — liña komando

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 296-307}}

**2. Objetivu**

Lee argumentu liña komando: `--watch` → `watch()`; se lae, kalkula dala ida no hakerek tempu.

**3. Fundamentu Python**

- **`argparse.ArgumentParser`** — objetu ne'ebé define no lee argumentu.
- **`action="store_true"`** — argumentu `--watch` sai `True` se iha.
- **`argv=None`** — se `None`, lee `sys.argv` (permite teste haruka argumentu).
- **`sys.exit(main())`** — kódigu saída programa.

**4. Esplikasaun liña-ba-liña**

- **Liña 296** — `def main(argv=None) -> int:`
- **Liña 297** — `p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)` — ajuda (`--help`) hatudu docstring modulu.
- **Liña 298** — `p.add_argument("--watch", action="store_true", help="keep the analytics tables up to date")`.
- **Liña 299** — `args = p.parse_args(argv)`.
- **Liña 300** — `if args.watch:`
- **Liña 301** — `return watch()`.
- **Liña 302** — `print(f"[spark-analytics] analytics recomputed in {run():.1f}s")` — halai dala ida.
- **Liña 303** — `return 0`.
- **Liña 306** — `if __name__ == "__main__":`
- **Liña 307** — `sys.exit(main())`.

**5. Walkthrough ezekusaun**

`python -m spark.batch_analytics` → `args.watch = False` → `run()` → print "analytics recomputed in 9.2s" (*ezemplu ilustrativu*) → saída 0. `python -m spark.batch_analytics --watch` → loop `watch()`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `p` | `argparse.ArgumentParser` |
| `args` | `argparse.Namespace` (`args.watch: bool`) |
| return | `int` |

**7. Ligasaun OOP**

`ArgumentParser` mak klase biblioteka padraun; `p` mak objetu; `add_argument` no `parse_args` mak métodu. `args` mak objetu `Namespace` ho atributu dinámiku.

**8. Impaktu integrasaun**

`run_all.py` hahú prosesu ho `--watch` (Modulu 06).

**9. Checkpoint ba principiante**

**P:** Saida mak `args.watch` se ita la hakerek `--watch`?
**R:** `False`, tanba `action="store_true"` fó `False` hanesan default.
