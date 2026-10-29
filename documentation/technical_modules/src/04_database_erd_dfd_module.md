# 04 — Modulu Base de Dadus PostgreSQL, ERD no DFD

*Sistema Crop Disease Early Warning System (EWS) — dokumentasaun téknika ba base de dadus (PostgreSQL + SQLAlchemy), Entity-Relationship Diagram (ERD) no Data Flow Diagram (DFD), ba estudante ne'ebé foin hahú programasaun. Fonte úniku: kódigu iha repozitóriu (branch `main`, commit `302818f`). Password base de dadus troka ho `***`.*

{{coverage docker-compose.yml 52-75}}
{{coverage storage/database.py 1-654}}
{{coverage storage/init_db.py 1-51}}
{{coverage storage/schema.sql 1-363}}

> **Oinsá lee.** Bloku kódigu kada ida tuir pasu 9: (1) Kódigu orijinál, (2) Objetivu, (3) Fundamentu Python (ba SQL: fundamentu SQL), (4) Esplikasaun liña-ba-liña, (5) Walkthrough ezekusaun, (6) Tipu dadus, (7) Ligasaun OOP, (8) Impaktu integrasaun, (9) Checkpoint ba principiante. Etiketa: *Implementadu*, *Inferidu*, *Teoria*, *Ezemplu ilustrativu*.

## A. Introdusaun no Objetivu

### A.1 Papél base de dadus iha sistema

PostgreSQL 16 (iha Docker) mak memória permanente sistema: utilizadór, fotografia, prediksaun CNN, tempu kada pasu pipeline, eventu rejeitadu, rezultadu teste eskalabilidade, revizaun admin, auditoria armazenamentu, no tabela analítika ne'ebé Spark kalkula. Kódigu Python ko'alia ho base de dadus liuhusi **SQLAlchemy ORM** (`storage/database.py`): kada **klase** Python = **tabela**; kada **objetu** = **liña**.

| Grupu | Tabela | Na'in hakerek |
|---|---|---|
| Operasional | `users`, `images`, `predictions`, `processing_events`, `rejected_events` | Spark (`storage/uploads.py`, `storage/results.py`), dashboard, `init_db` |
| Teste eskalabilidade | `scale_test_runs`, `scale_test_items` | `scalability/` |
| Admin | `image_reviews`, `storage_audit_log` | Dashboard |
| Analítika (Spark de'it) | `analytics_summary`, `analytics_disease_distribution`, `analytics_risk_distribution`, `analytics_high_risk_by_location`, `analytics_disease_trend`, `analytics_disease_daily`, `batch_location_daily_summary` | `spark/batch_analytics.py` (JDBC) |
| Analítika teste | `analytics_scale_test_disease_by_location`, `analytics_scale_test_disease_distribution`, `analytics_scale_test_high_risk_by_location` | `spark/batch_analytics.py` |
| View | `v_tableau_images`, `v_admin_image_catalog` | `init_db()` harii fali |

Total: **tabela 19, view 2** (husi modelu ORM no `schema.sql`).

### A.2 Problema ne'ebé modulu ne'e rezolve

- **Dadus la lakon** bainhira laptop mate (volume Docker `crop_disease_pg_data`).
- **Kódigu ida ba base de dadus rua**: `DATABASE_URL` hili PostgreSQL (normál) ka SQLite (teste lokál) — kódigu seluk la muda.
- **Esquema evolui** la ho ferramenta migrasaun: `init_db()` harii tabela foun no aumenta koluna ne'ebé falta (`_add_missing_columns`).
- **BI seguru**: Tableau lee view `v_tableau_images` ho utilizadór *read-only* `tableau_reader` — nunka tabela `users` (email, hash password).
- **Imajen ida = prediksaun foun liu ida**: CTE `LATEST_PREDICTION_CTE` (`ROW_NUMBER()`) uza iha view, dashboard no Spark.

### A.3 Fundamentu Python no SQL (foun iha modulu ne'e)

Modulu 01–03 hanorin ona klase, `__init__`, herança, enum ho `str`, decorator, `with`, exception. **Lembra:** klase = molde; objetu = buat harii husi molde; herança = klase foun simu hotu husi klase inan. Konseitu **foun**:

**ORM SQLAlchemy (Python)**

1. **`Base = declarative_base()`** — klase baze; klase hotu ne'ebé herda `Base` sai tabela.
2. **`__tablename__ = "users"`** — **atributu klase** ho naran tabela.
3. **`Column(String, primary_key=True, default=new_uuid)`** — atributu klase ne'ebé deskreve koluna: tipu, chave primária, valór default (funsaun bolu bainhira liña foun).
4. **`ForeignKey("users.user_id")`** — koluna hatudu ba chave primária tabela seluk.
5. **`relationship("Image", back_populates="user")`** — atributu navegasaun objetu: `user.images` (lista) no `image.user` (objetu). Ne'e la'ós koluna.
6. **`cascade="all, delete-orphan"`** — hamoos imajen → hamoos mós nia prediksaun (iha ORM).
7. **`__table_args__ = (Index(...),)`** — índise ba buka lalais.
8. **Mixin** — klase ki'ik ne'ebé fó koluna komún: `class AnalyticsSummary(_SparkOutput, Base)` — **herança múltipla**.
9. **`create_engine(url)`** — objetu konesaun (pool). **`sessionmaker(bind=engine)`** — fábrika sesaun. **`session.get(Klase, id)`**, **`session.add(obj)`**, **`session.commit()`**.
10. **Decorator ho argumentu** — `@event.listens_for(engine, "connect")` rejista funsaun atu halai kada konesaun foun.
11. **Type hint jenériku** — `list[tuple[str, str]]`.

**SQL (Teoria)**

| SQL | Signifika |
|---|---|
| `CREATE TABLE IF NOT EXISTS t (...)` | harii tabela se seidauk iha |
| `PRIMARY KEY` | identifika liña úniku |
| `REFERENCES t(c)` / `FOREIGN KEY` | liga ba tabela seluk (integridade referensiál) |
| `UNIQUE`, `NOT NULL`, `DEFAULT`, `CHECK (...)` | regra valór |
| `CREATE INDEX` | buka lalais tuir koluna |
| `CREATE VIEW v AS SELECT ...` | "tabela virtuál" husi query |
| `WITH latest AS (...)` | CTE (sub-query ho naran) |
| `ROW_NUMBER() OVER (PARTITION BY x ORDER BY y DESC)` | númeru 1, 2, 3 kada grupu |
| `LEFT JOIN` | mantein liña hotu husi tabela karuk |
| `GRANT SELECT ON v TO r` | fó permisaun lee |
| `TIMESTAMPTZ` | tempu ho fuso |

**ERD (Teoria):** entidade (tabela), atributu (koluna), relasaun ho kardinalidade (`||--o{` = ida ba zero-ka-barak). **DFD (Teoria):** *entidade esternu* (kaixa), *prosesu* (sírkulu/kaixa ho númeru), *data store* (base de dadus/arkivu), *fluxu dadus* (seta ho naran). Kontestu (nível 0) = sistema hanesan prosesu ida; nível 1 = sistema fahe ba prosesu prinsipál.

## B. Arkivu Fonte Loloos

| Arkivu | Liña | Konteúdu |
|---|---|---|
| `docker-compose.yml` | 52–75 | Serbisu `postgres` (16), port localhost, volume, healthcheck |
| `storage/database.py` | 654 | Enum 4, modelu ORM 19, `engine`, `SessionLocal`, migrasaun koluna, `LATEST_PREDICTION_CTE`, view 2, `tableau_reader`, `init_db` |
| `storage/init_db.py` | 51 | `run()` — `init_db()` + admin dahuluk |
| `storage/schema.sql` | 363 | DDL PostgreSQL ekivalente (manuál / relatóriu) |

**Dependénsia:** `SQLAlchemy`, `psycopg2` (driver PostgreSQL ba SQLAlchemy), PostgreSQL 16, `config.settings` (`DATABASE_URL`, `LOCAL_TZ_OFFSET_HOURS`, `TABLEAU_DB_USER`, `TABLEAU_DB_PASSWORD`, `BOOTSTRAP_ADMIN_*`), `storage/auth.hash_password`.

## C. Arkitetura no Fluxu Ezekusaun

### C.1 Input, prosesu, output

| | Input | Prosesu | Output |
|---|---|---|---|
| Inisializa | `DATABASE_URL` | `init_db()`: `create_all` → `_add_missing_columns` → migrasaun → view 2 → `tableau_reader` | Tabela 19 + view 2 |
| Bootstrap admin | `BOOTSTRAP_ADMIN_*` | `init_db.run()` | Liña `users` ho `role=ADMIN` (se tabela mamuk) |
| Streaming (Modulu 01) | eventu Kafka | `register_upload`, `mark_processing`, `extract_metadata`, `persist_result` | `images`, `predictions`, `processing_events`, `rejected_events` |
| Analítika (Modulu 01) | tabela operasional (JDBC) | `batch_analytics` | tabela `analytics_*` |
| BI | view | Tableau / dashboard | relatóriu |

### C.2 ERD — relasaun ho chave estranjeira (FK)

Relasaun ne'ebé **implementa** ho `ForeignKey`/`REFERENCES`:

```mermaid
erDiagram
    users ||--o{ images : "user_id (FK)"
    images ||--o{ predictions : "image_id (FK)"
    images ||--o{ processing_events : "image_id (FK)"
    users {
        VARCHAR user_id PK
        VARCHAR username UK
        VARCHAR email UK
        VARCHAR password_hash
        ENUM role
        ENUM status
        BOOLEAN is_deleted
        TIMESTAMPTZ created_at
    }
    images {
        VARCHAR image_id PK
        VARCHAR user_id FK
        VARCHAR location
        VARCHAR image_path
        VARCHAR source
        VARCHAR content_hash
        TIMESTAMPTZ captured_at
        TIMESTAMPTZ uploaded_at
        TIMESTAMPTZ ingested_at
        TIMESTAMPTZ processed_at
        TIMESTAMPTZ result_published_at
        FLOAT gps_latitude
        FLOAT gps_longitude
        ENUM status
        TEXT error_message
    }
    predictions {
        VARCHAR prediction_id PK
        VARCHAR image_id FK
        VARCHAR predicted_class
        FLOAT prediction_confidence
        BOOLEAN is_uncertain
        FLOAT lesion_score
        FLOAT risk_score
        ENUM risk_level
        VARCHAR model_version
        TIMESTAMPTZ predicted_at
        INTEGER processing_latency_ms
    }
    processing_events {
        VARCHAR event_id PK
        VARCHAR image_id FK
        TIMESTAMPTZ kafka_received_at
        TIMESTAMPTZ spark_started_at
        TIMESTAMPTZ prediction_completed_at
        INTEGER total_latency_ms
        ENUM status
    }
```

### C.3 Relasaun lójiku (la ho FK) no tabela mesak

Tabela sira ne'e **la iha FK** ho intensaun (komentáriu `database.py` liña 219–220 no 281–282: atu moris liu factory reset). Relasaun iha ne'e mak **lójiku** (naran koluna hanesan), la'ós FK.

| Tabela | Chave | Relasaun lójiku (la iha FK) |
|---|---|---|
| `rejected_events` | `rejected_event_id` | `image_id` bele la iha iha `images` |
| `scale_test_runs` | `run_id` | 1 → barak `scale_test_items.run_id` |
| `scale_test_items` | `image_id` | `image_id` = liña `images` teste; `source_image_id` = fotografia agrikultór orijinál |
| `image_reviews` | `image_id` | `image_id` = `images.image_id` |
| `storage_audit_log` | `audit_id` | — |
| `analytics_*` (7) | haree D | rezultadu Spark husi `images`/`predictions`/`users` |
| `analytics_scale_test_*` (3) | `run_id` + ... | rezultadu Spark husi `scale_test_items` |

```mermaid
erDiagram
    scale_test_runs ||..o{ scale_test_items : "run_id (lójiku)"
    images ||..o| image_reviews : "image_id (lójiku)"
    images ||..o{ scale_test_items : "image_id (lójiku)"
```

### C.4 DFD — Kontestu (nível 0)

```mermaid
flowchart TD
    F["Agrikultór<br/>(Android)"]
    A["Admin<br/>(dashboard)"]
    T["Tableau Desktop"]
    G["Google Drive"]
    S(("0<br/>Sistema EWS<br/>(Kafka + Spark + CNN + BD)"))
    F -- "fotografia + location + GPS" --> G
    G -- "metadata + bytes fotografia" --> S
    S -- "rezultadu JSON + imajen heatmap" --> G
    G -- "rezultadu + istória" --> F
    A -- "modu, Process/Reprocess, label, reset" --> S
    S -- "KPI, gráfiku, katálogu imajen" --> A
    S -- "v_tableau_images (read-only)" --> T
```

### C.5 DFD — Nível 1

```mermaid
flowchart TD
    G["Google Drive"]
    A["Admin"]
    T["Tableau"]
    P1(("1.0<br/>Deteta upload<br/>drive_watcher"))
    P2(("2.0<br/>Lori eventu<br/>Kafka"))
    P3(("3.0<br/>Rejista no valida<br/>Spark: register_upload,<br/>clean_event"))
    P4(("4.0<br/>Preprocess + klasifika<br/>Spark + CNN + risku"))
    P5(("5.0<br/>Rai + entrega rezultadu<br/>persist_result,<br/>publish_result_to_drive"))
    P6(("6.0<br/>Kalkula analítika<br/>batch_analytics"))
    P7(("7.0<br/>Dashboard / admin"))
    D1[("D1 users")]
    D2[("D2 images")]
    D3[("D3 predictions")]
    D4[("D4 processing_events<br/>rejected_events")]
    D5[("D5 analytics_*")]
    D6[("D6 image_reviews<br/>storage_audit_log")]
    G -- "metadata arkivu foun" --> P1
    P1 -- "eventu drive_upload" --> P2
    A -- "Process / Reprocess" --> P7
    P7 -- "eventu submit" --> P2
    P2 -- "eventu JSON" --> P3
    P3 -- "agrikultór foun" --> D1
    P3 -- "liña INGESTED" --> D2
    P3 -- "eventu validu" --> P4
    G -- "bytes fotografia" --> P4
    P4 -- "EXIF, status" --> D2
    P4 -- "klase, konfiansa, risku" --> P5
    P5 -- "prediksaun" --> D3
    P5 -- "tempu / FAILED" --> D4
    P5 -- "COMPLETED, result_published_at" --> D2
    P5 -- "JSON + heatmap" --> G
    D1 -- "is_deleted" --> P6
    D2 -- "images (JDBC)" --> P6
    D3 -- "predictions (JDBC)" --> P6
    P6 -- "tabela analítika" --> D5
    D5 -- "KPI" --> P7
    P7 -- "label / audit" --> D6
    D2 -- "v_tableau_images" --> T
    D3 -- "v_tableau_images" --> T
```

### C.6 Trasu konkretu: liña sira ne'ebé fotografia ida harii

*Ezemplu ilustrativu* (valór); tabela/koluna loloos.

| Pasu | Kódigu | Tabela | Mudansa |
|---|---|---|---|
| 1 | `get_or_create_farmer` | `users` | `INSERT` (se agrikultór foun): `username=email`, `role='USER'`, `status='ACTIVE'` |
| 2 | `register_upload` | `images` | `INSERT`: `image_id='a1b2...'`, `image_path='gdrive:1AbC'`, `source='android_drive_upload'`, `status='INGESTED'`, `uploaded_at` |
| 3 | `mark_processing` | `images` | `UPDATE status='PROCESSING'` |
| 4 | `extract_metadata` | `images` | `UPDATE camera_make, gps_*, image_width...` |
| 5 | `persist_result` (COMPLETED) | `predictions` | `INSERT`: `predicted_class='blight'`, `prediction_confidence=0.97`, `risk_score=0.97`, `risk_level='HIGH'` |
| 6 | `persist_result` | `images` | `UPDATE status='COMPLETED', processed_at` |
| 7 | `persist_result` | `processing_events` | `INSERT` tempu `kafka_received_at`, `spark_started_at`... |
| 8 | `publish_result_to_drive` | `images` | `UPDATE result_published_at` |
| 9 | `batch_analytics.write_tables` | `analytics_*` | `DELETE` + `INSERT ... SELECT` husi `_staging` |

Se `image_id` la iha liña `images` (ezemplu eventu aat): `persist_result` hatama liña `rejected_events` ho `raw_event` (JSON eventu) no `reason` (`storage/results.py` liña 76–83). Replay: `persist_result` hamoos prediksaun tuan molok hatama foun (*delete-then-insert*, liña 103–104), entaun imajen ida iha prediksaun ida de'it husi streaming.

## D. Esplikasaun Kódigu-ba-Kódigu

### D.1 `docker-compose.yml` — serbisu PostgreSQL

#### D.1.1 Serbisu `postgres` no volume

**1. Kódigu orijinál**

{{code docker-compose.yml 52-75}}

**2. Objetivu**

Hahú PostgreSQL 16 iha Docker, ho utilizadór/password/base de dadus, port só iha localhost, volume atu dadus moris liu restart, no healthcheck.

**3. Fundamentu Python**

*(YAML)* `chave: valór`; lista ho `-`; indentasaun hatudu hierarkia. Password iha arkivu troka ho `***` iha dokumentu ne'e.

**4. Esplikasaun liña-ba-liña**

- **Liña 52** — `postgres:` — naran serbisu.
- **Liña 53** — `image: postgres:16`.
- **Liña 54** — `container_name: crop-disease-ews-postgres`.
- **Liña 55** — `restart: unless-stopped`.
- **Liña 56** — `environment:`.
- **Liña 57** — `POSTGRES_USER: ews_user` — utilizadór aplikasaun.
- **Liña 58** — `POSTGRES_PASSWORD: ***` — password (la hatudu).
- **Liña 59** — `POSTGRES_DB: crop_disease` — naran base de dadus.
- **Liña 60** — `ports:`.
- **Liña 61–63** — Komentáriu: liga ba localhost de'it: Spark, dashboard no Tableau Desktop hotu iha makina ne'e, entaun la iha razaun atu loke base de dadus (no nia password default) ba ema hotu iha Wi-Fi hanesan.
- **Liña 64** — `"127.0.0.1:5432:5432"`.
- **Liña 65** — `volumes:`.
- **Liña 66** — `crop_disease_pg_data:/var/lib/postgresql/data` — dadus iha volume ho naran.
- **Liña 67** — `healthcheck:`.
- **Liña 68** — `test: ["CMD-SHELL", "pg_isready -U ews_user -d crop_disease"]` — kontrola server prontu.
- **Liña 69** — `interval: 5s`.
- **Liña 70** — `timeout: 5s`.
- **Liña 71** — `retries: 10`.
- **Liña 73** — `volumes:` (nível leten) — deklara volume.
- **Liña 74** — `kafka_data:`.
- **Liña 75** — `crop_disease_pg_data:`.

**5. Walkthrough ezekusaun**

`docker compose up -d` → container `crop-disease-ews-postgres` → `pg_isready` kada 5 s to'o "healthy" → aplikasaun konekta `127.0.0.1:5432`.

**6. Tipu dadus**

YAML string/lista; port `5432`.

**7. Ligasaun OOP**

Not implemented in the current codebase. (Konfigurasaun.)

**8. Impaktu integrasaun**

`DATABASE_URL` (variavel ambiente) hatudu ba server ne'e; Spark JDBC (`jdbc:postgresql://127.0.0.1:5432/crop_disease`) no SQLAlchemy uza server hanesan.

**9. Checkpoint ba principiante**

**P:** Saida mak akontese ba dadus se ita halai `docker compose down`?
**R:** Dadus moris, tanba iha volume `crop_disease_pg_data` (hamoos de'it ho `down -v`).

### D.2 `storage/database.py` — modelu ORM

#### D.2.1 Docstring, import, `Base`, `now_utc`, `new_uuid`

**1. Kódigu orijinál**

{{code storage/database.py 1-28}}

**2. Objetivu**

Kamada base de dadus: modelu SQLAlchemy, engine no sesaun. Import tipu koluna, harii `Base` (klase inan ba modelu hotu), no funsaun default rua.

**3. Fundamentu Python**

`declarative_base()` fó **klase** (la'ós objetu). `text as sql_text` — naran seluk. Funsaun default (`now_utc`, `new_uuid`) haruka **la ho** `()` ba `Column(default=...)` atu bolu kada liña foun.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: kamada base de dadus — modelu SQLAlchemy, engine no sesaun; `DATABASE_URL` hili PostgreSQL (normál) ka SQLite (teste lokál); buat seluk la muda.
- **Liña 4** — Remata docstring.
- **Liña 6** — `import enum`.
- **Liña 7** — `import uuid`.
- **Liña 8** — `from datetime import datetime, timezone`.
- **Liña 10–13** — Import SQLAlchemy: `create_engine`, `Column`, tipu (`String`, `Float`, `Integer`, `BigInteger`, `Date`, `DateTime`, `Enum`, `Text`, `Boolean`), `ForeignKey`, `Index`, `event`, `inspect`, `text` (naran `sql_text`).
- **Liña 14** — `declarative_base`, `sessionmaker`, `relationship`.
- **Liña 16–18** — `DATABASE_URL`, `LOCAL_TZ_OFFSET_HOURS`, `TABLEAU_DB_USER`, `TABLEAU_DB_PASSWORD` (segredu — valór la hatudu).
- **Liña 20** — `Base = declarative_base()`.
- **Liña 23** — `def now_utc() -> datetime:`
- **Liña 24** — `return datetime.now(timezone.utc)`.
- **Liña 27** — `def new_uuid() -> str:`
- **Liña 28** — `return str(uuid.uuid4())` — ezemplu `"9f1c2b7e-..."`.

**5. Walkthrough ezekusaun**

`new_uuid()` → `"3d6f4a2c-8b1e-4f0a-9c3d-2e7b5a1f0c9d"` (*ezemplu ilustrativu*). `now_utc()` → `datetime(..., tzinfo=UTC)`.

**6. Tipu dadus**

`Base`: klase (tipu `DeclarativeMeta`); `now_utc() -> datetime`; `new_uuid() -> str`.

**7. Ligasaun OOP**

`Base` mak **klase inan** ba modelu ORM hotu (herança). `declarative_base()` mak fábrika ne'ebé fó klase.

**8. Impaktu integrasaun**

Modulu hotu ne'ebé uza base de dadus import husi arkivu ne'e (`SessionLocal`, modelu, enum).

**9. Checkpoint ba principiante**

**P:** Tanba sá `default=new_uuid` no la'ós `default=new_uuid()`?
**R:** `new_uuid()` sei bolu **dala ida** bainhira klase define (UUID hanesan ba liña hotu). `new_uuid` (funsaun) bolu kada liña foun.

#### D.2.2 Enum `Role`, `UserStatus`, `RiskLevel`, `ProcessingStatus`

**1. Kódigu orijinál**

{{code storage/database.py 31-55}}

**2. Objetivu**

Define lista valór fixu ba papél utilizadór, status utilizadór, nivel risku, no status prosesa fotografia.

**3. Fundamentu Python**

`class X(str, enum.Enum)` — enum ne'ebé mós string (haree Modulu 03 D.6.1).

**4. Esplikasaun liña-ba-liña**

- **Liña 31** — `class Role(str, enum.Enum):`
- **Liña 32** — `USER = "USER"` — agrikultór.
- **Liña 33** — `ADMIN = "ADMIN"`.
- **Liña 36** — `class UserStatus(str, enum.Enum):`
- **Liña 37** — Komentáriu: `PENDING_VERIFICATION` só iha liña tuan (rejistu rasik hasai ona).
- **Liña 38** — `PENDING_VERIFICATION = "PENDING_VERIFICATION"`.
- **Liña 39** — `ACTIVE = "ACTIVE"`.
- **Liña 40** — `INACTIVE = "INACTIVE"`.
- **Liña 41** — `SUSPENDED = "SUSPENDED"`.
- **Liña 44** — `class RiskLevel(str, enum.Enum):`
- **Liña 45** — `LOW = "LOW"`.
- **Liña 46** — `MEDIUM = "MEDIUM"`.
- **Liña 47** — `HIGH = "HIGH"`.
- **Liña 50** — `class ProcessingStatus(str, enum.Enum):`
- **Liña 51** — Komentáriu: liña iha de'it bainhira Spark rejista fotografia.
- **Liña 52** — `INGESTED = "INGESTED"` — rejista husi Spark (ka haruka fali husi Reprocess).
- **Liña 53** — `PROCESSING = "PROCESSING"` — Spark/CNN foti.
- **Liña 54** — `COMPLETED = "COMPLETED"` — prediksaun rai.
- **Liña 55** — `FAILED = "FAILED"` — pasu ruma falla; haree `error_message`.

**5. Walkthrough ezekusaun**

Siklu moris fotografia: `INGESTED → PROCESSING → COMPLETED` ka `FAILED`; Retry: `FAILED → INGESTED` (`pipeline_submit`). `ProcessingStatus.COMPLETED.value` → `"COMPLETED"`.

```mermaid
stateDiagram-v2
    [*] --> INGESTED : register_upload / submit
    INGESTED --> PROCESSING : mark_processing
    PROCESSING --> COMPLETED : persist_result OK
    PROCESSING --> FAILED : erru / InvalidEvent
    FAILED --> INGESTED : Retry / Reprocess
    COMPLETED --> INGESTED : Reprocess
```

**6. Tipu dadus**

Membru kada enum: tipu enum no `str`.

**7. Ligasaun OOP**

**Herança múltipla** (`str` + `Enum`). Kada membru mak **objetu singleton** (`ProcessingStatus.COMPLETED is ProcessingStatus("COMPLETED")` loos). Uza ho `Column(Enum(...))` — SQLAlchemy harii tipu ENUM iha PostgreSQL.

**8. Impaktu integrasaun**

`ProcessingStatus` uza husi Spark (`persist_result`, `mark_processing`), `pipeline_submit`, dashboard. `RiskLevel` husi `ml/risk` (`"HIGH"`...).

**9. Checkpoint ba principiante**

**P:** Tanba sá uza enum duke string livre?
**R:** Enum limita valór ba lista fixu — evita sala ortografia (ezemplu `"COMPLETE"`) no base de dadus rejeita valór la válidu.

#### D.2.3 Modelu `User`

**1. Kódigu orijinál**

{{code storage/database.py 58-86}}

**2. Objetivu**

Tabela `users`: identidade, papél, status, koluna verifikasaun Gmail (husi rejistu tuan; mantein), *soft delete*, tempu, no relasaun ba imajen.

**3. Fundamentu Python**

Atributu klase ho `Column(...)`: `unique=True`, `nullable=False`, `index=True`, `default=...`, `onupdate=now_utc` (atualiza automátiku bainhira liña muda). `relationship(..., back_populates=...)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 58–60** — Komentáriu seksaun `users`.
- **Liña 61** — `class User(Base):`
- **Liña 62** — `__tablename__ = "users"`.
- **Liña 64** — `user_id = Column(String, primary_key=True, default=new_uuid)`.
- **Liña 65** — `username = Column(String, unique=True, nullable=False, index=True)` — agrikultór: email.
- **Liña 66** — `full_name = Column(String, nullable=True)`.
- **Liña 67** — `email = Column(String, unique=True, nullable=False)`.
- **Liña 68** — `password_hash = Column(String, nullable=False)` — hash de'it (nunka password).
- **Liña 69** — `role = Column(Enum(Role), nullable=False, default=Role.USER)`.
- **Liña 70** — `status = Column(Enum(UserStatus), nullable=False, default=UserStatus.ACTIVE)`.
- **Liña 72** — Komentáriu: verifikasaun Gmail (rejistu tuan; koluna mantein).
- **Liña 73** — `email_verified = Column(Boolean, nullable=False, default=False)`.
- **Liña 74** — `verification_code_hash = Column(String, nullable=True)`.
- **Liña 75** — `verification_code_expires_at = Column(DateTime(timezone=True), nullable=True)`.
- **Liña 76** — `verification_attempts = Column(Integer, nullable=False, default=0)`.
- **Liña 77** — `last_verification_sent_at = Column(DateTime(timezone=True), nullable=True)`.
- **Liña 79** — Komentáriu: soft delete.
- **Liña 80** — `is_deleted = Column(Boolean, nullable=False, default=False)`.
- **Liña 81** — `deleted_at = Column(DateTime(timezone=True), nullable=True)`.
- **Liña 83** — `created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)`.
- **Liña 84** — `updated_at = Column(..., default=now_utc, onupdate=now_utc, nullable=False)`.
- **Liña 86** — `images = relationship("Image", back_populates="user")` — `user.images` = lista imajen.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `User(username="agrikultor@example.com", email="agrikultor@example.com", password_hash="<hash>", role=Role.USER)` → `session.add` + `commit` → `user_id` = UUID foun; `status="ACTIVE"`; `created_at` = agora.

**6. Tipu dadus**

| Koluna | Python | PostgreSQL |
|---|---|---|
| `user_id`, `username`, `email`, `password_hash` | `str` | `VARCHAR` |
| `role` | `Role` | ENUM `role` |
| `status` | `UserStatus` | ENUM `userstatus` |
| `email_verified`, `is_deleted` | `bool` | `BOOLEAN` |
| `verification_attempts` | `int` | `INTEGER` |
| `*_at` | `datetime` | `TIMESTAMPTZ` |

**7. Ligasaun OOP**

- **Klase herda `Base`** → tabela.
- **Atributu klase** (`Column`) → koluna; bainhira objetu harii, SQLAlchemy halo sira hanesan **atributu instánsia** (`user.email`).
- `__init__` la hakerek: SQLAlchemy fó konstrutór automátiku ne'ebé simu argumentu ho naran koluna.
- **Relasaun objetu (agregasaun)**: `user.images` ↔ `image.user` (`back_populates`).

**8. Impaktu integrasaun**

Harii husi `farmers.get_or_create_farmer` (agrikultór) no `init_db.run` (admin). `batch_analytics` lee `user_id, is_deleted` de'it (privasidade).

**9. Checkpoint ba principiante**

**P:** Saida mak "soft delete"?
**R:** La hamoos liña; marka `is_deleted = True` no `deleted_at`. Dadus istória (imajen) mantein no admin bele restaura.

#### D.2.4 Modelu `Image`

**1. Kódigu orijinál**

{{code storage/database.py 89-140}}

**2. Objetivu**

Tabela `images`: fotografia ida = liña ida. Fatin, na'in, dalan (`gdrive:<id>`), fonte, hash, tempu kada etapa, metadata EXIF, status, erru, no relasaun ba prediksaun/eventu (cascade).

**3. Fundamentu Python**

`ForeignKey("users.user_id")`; `relationship(..., cascade="all, delete-orphan")`; `__table_args__` tuple ho `Index`.

**4. Esplikasaun liña-ba-liña**

- **Liña 89–91** — Komentáriu seksaun `images`.
- **Liña 92** — `class Image(Base):`
- **Liña 93** — `__tablename__ = "images"`.
- **Liña 95** — `image_id = Column(String, primary_key=True, default=new_uuid)` — iha prátika UUID husi Android.
- **Liña 96** — Komentáriu: naran fatin testu livre husi formuláriu (ezemplu munisípiu).
- **Liña 97** — `location = Column(String, nullable=True)`.
- **Liña 98** — `user_id = Column(String, ForeignKey("users.user_id"), nullable=False)`.
- **Liña 99** — `original_filename = Column(String, nullable=True)`.
- **Liña 100** — `image_path = Column(String, nullable=False)` — komentáriu formatu tuan `storage/images/YYYY/MM/DD/<uuid>.jpg`; foun `gdrive:<id>`.
- **Liña 101** — `source = Column(String, nullable=False, default="android_upload")` — `register_upload` hakerek `"android_drive_upload"`; teste `"scalability_test"`.
- **Liña 102** — Komentáriu: SHA-256 bytes fotografia, atu hatene fotografia hanesan haruka dala rua.
- **Liña 103** — `content_hash = Column(String, nullable=True)`.
- **Liña 105** — Komentáriu: tempu.
- **Liña 106** — `captured_at` — EXIF `DateTimeOriginal`, ka NULL.
- **Liña 107** — `captured_at_source` — `"exif"` | `"client_trusted"` | None.
- **Liña 108** — `uploaded_at` — `nullable=False`, default agora; komentáriu: server harii, nunka konfia cliente.
- **Liña 109** — `ingested_at` — tempu publika/konsume Kafka.
- **Liña 110** — `processed_at` — Spark remata.
- **Liña 111** — Komentáriu: bainhira rezultadu to'o Drive agrikultór; NULL = telemóvel seidauk iha.
- **Liña 112** — `result_published_at`.
- **Liña 114** — Komentáriu: metadata EXIF (`extract_metadata`); NULL se la iha.
- **Liña 115** — `gps_latitude = Column(Float, nullable=True)`.
- **Liña 116** — `gps_longitude`.
- **Liña 117** — `gps_altitude_m`.
- **Liña 118** — Komentáriu: `"exif"` (GPS fotografia, preferidu) ka `"device"` (posisaun telemóvel).
- **Liña 119** — `gps_source`.
- **Liña 120** — `camera_make`.
- **Liña 121** — `camera_model`.
- **Liña 122** — `image_width = Column(Integer, nullable=True)`.
- **Liña 123** — `image_height`.
- **Liña 125** — `status = Column(Enum(ProcessingStatus), nullable=False, default=ProcessingStatus.INGESTED)`.
- **Liña 126** — `error_message = Column(Text, nullable=True)`.
- **Liña 128** — `created_at`.
- **Liña 130** — `user = relationship("User", back_populates="images")`.
- **Liña 131** — `predictions = relationship("Prediction", back_populates="image", cascade="all, delete-orphan")`.
- **Liña 132** — `processing_events = relationship("ProcessingEvent", back_populates="image", cascade="all, delete-orphan")`.
- **Liña 134** — `__table_args__ = (`
- **Liña 135** — `Index("ix_images_location", "location"),`
- **Liña 136** — `Index("ix_images_user_id", "user_id"),`
- **Liña 137** — `Index("ix_images_captured_at", "captured_at"),`
- **Liña 138** — `Index("ix_images_uploaded_at", "uploaded_at"),`
- **Liña 139** — `Index("ix_images_content_hash", "content_hash"),` — buka duplikadu lalais.
- **Liña 140** — `)`.

**5. Walkthrough ezekusaun**

Haree C.6 pasu 2–4, 6, 8. `image.user.email` → email na'in (navegasaun objetu). `session.delete(image)` → ORM hamoos mós `image.predictions` no `image.processing_events` (cascade).

**6. Tipu dadus**

| Koluna | Python | PostgreSQL |
|---|---|---|
| `image_id`, `location`, `user_id`, `image_path`, `source`, `content_hash`, `gps_source`, `camera_*`, `captured_at_source` | `str` | `VARCHAR` |
| `*_at` | `datetime` | `TIMESTAMPTZ` |
| `gps_*` | `float` | `DOUBLE PRECISION` |
| `image_width/height` | `int` | `INTEGER` |
| `status` | `ProcessingStatus` | ENUM |
| `error_message` | `str` | `TEXT` |

**7. Ligasaun OOP**

**Komposisaun (ORM)**: `cascade="all, delete-orphan"` signifika `Image` "na'in" nia `Prediction` no `ProcessingEvent` — sira la moris mesak (moris-mate hamutuk). **Agregasaun**: `Image` ↔ `User` (user moris maske imajen hamoos).

**8. Impaktu integrasaun**

Tabela sentrál: Spark hakerek; dashboard, Tableau, `batch_analytics` lee; `pipeline_submit` haruka fali.

**9. Checkpoint ba principiante**

**P:** Tanba sá `uploaded_at` "nunka konfia cliente"?
**R:** Relójiu telemóvel bele sala; valór mai husi `createdTime` Google Drive (relójiu server).

#### D.2.5 Modelu `Prediction`

**1. Kódigu orijinál**

{{code storage/database.py 143-180}}

**2. Objetivu**

Tabela `predictions`: rezultadu CNN ba imajen ida — klase, konfiansa, uncertain, feature, risku, versaun modelu, tempu, latênsia.

**3. Fundamentu Python**

Hanesan D.2.3–D.2.4 (`Column`, `ForeignKey`, `relationship`, `Index`).

**4. Esplikasaun liña-ba-liña**

- **Liña 143–145** — Komentáriu seksaun `predictions`.
- **Liña 146** — `class Prediction(Base):`
- **Liña 147** — `__tablename__ = "predictions"`.
- **Liña 149** — `prediction_id = Column(String, primary_key=True, default=new_uuid)`.
- **Liña 150** — `image_id = Column(String, ForeignKey("images.image_id"), nullable=False)`.
- **Liña 152** — Komentáriu: klasifikasaun CNN.
- **Liña 153** — `predicted_class = Column(String, nullable=True)` — `healthy`, `blight`, `uncertain`...
- **Liña 154** — `prediction_confidence = Column(Float, nullable=True)`.
- **Liña 155** — Komentáriu: konfiansa okos limite: `predicted_class` = `"uncertain"`, risku la to'o HIGH.
- **Liña 156** — `is_uncertain = Column(Boolean, nullable=False, default=False)`.
- **Liña 158** — Komentáriu: feature imajen (agora só `lesion_score` prenxe).
- **Liña 159** — `lesion_score = Column(Float, nullable=True)` — probabilidade moras.
- **Liña 160** — `discoloration_pct`.
- **Liña 161** — `texture_irregularity`.
- **Liña 162** — `leaf_area_affected_pct`.
- **Liña 164** — Komentáriu: nivel risku (`ml/risk.py`).
- **Liña 165** — `risk_score = Column(Float, nullable=True)`.
- **Liña 166** — `risk_level = Column(Enum(RiskLevel), nullable=True)`.
- **Liña 168** — `model_version = Column(String, nullable=True)`.
- **Liña 169** — `predicted_at = Column(DateTime(timezone=True), nullable=True)`.
- **Liña 170** — `processing_latency_ms = Column(Integer, nullable=True)` — komentáriu: `ingested_at → predicted_at`.
- **Liña 171** — `created_at`.
- **Liña 173** — `image = relationship("Image", back_populates="predictions")`.
- **Liña 175** — `__table_args__ = (`
- **Liña 176** — `Index("ix_predictions_image_id", "image_id"),`
- **Liña 177** — `Index("ix_predictions_predicted_class", "predicted_class"),`
- **Liña 178** — `Index("ix_predictions_risk_level", "risk_level"),`
- **Liña 179** — `Index("ix_predictions_predicted_at", "predicted_at"),`
- **Liña 180** — `)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `Prediction(image_id="a1b2...", predicted_class="blight", prediction_confidence=0.97, lesion_score=0.97, risk_score=0.97, risk_level="HIGH", is_uncertain=False, model_version="se_resnet18-...", processing_latency_ms=3400)`.

**6. Tipu dadus**

`predicted_class`, `model_version`: `VARCHAR`; `*_score`, `*_pct`, `prediction_confidence`: `DOUBLE PRECISION`; `risk_level`: ENUM; `is_uncertain`: `BOOLEAN`; `processing_latency_ms`: `INTEGER`.

**7. Ligasaun OOP**

Klase herda `Base`; relasaun `image` ↔ `Image.predictions`.

**8. Impaktu integrasaun**

`persist_result` hakerek (delete-then-insert ba replay); `_prediction_to_dict` lee ba Drive; `batch_analytics` lee 7 koluna; view uza prediksaun foun liu.

**9. Checkpoint ba principiante**

**P:** Imajen ida bele iha prediksaun barak?
**R:** Esquema permite (FK la úniku), maibé `persist_result` hamoos prediksaun tuan molok hatama foun; view/analítika foti foun liu (`ROW_NUMBER() = 1`).

#### D.2.6 `ProcessingEvent` no `RejectedEvent`

**1. Kódigu orijinál**

{{code storage/database.py 183-215}}

**2. Objetivu**

`processing_events`: tempu pipeline kada prosesa fotografia (liña ida kada tentativa). `rejected_events`: eventu Kafka aat liu atu prosesa (tabela *dead-letter*).

**3. Fundamentu Python**

Hanesan modelu seluk. `RejectedEvent` **la iha** `ForeignKey` (`image_id` bele la iha).

**4. Esplikasaun liña-ba-liña**

- **Liña 183–185** — Komentáriu seksaun: `processing_events`: tempu pipeline kada fotografia.
- **Liña 186** — `class ProcessingEvent(Base):`
- **Liña 187** — `__tablename__ = "processing_events"`.
- **Liña 189** — `event_id = Column(String, primary_key=True, default=new_uuid)`.
- **Liña 190** — `image_id = Column(String, ForeignKey("images.image_id"), nullable=False)`.
- **Liña 191** — `api_published_at` — komentáriu: haruka ba Kafka.
- **Liña 192** — `kafka_received_at`.
- **Liña 193** — `spark_started_at`.
- **Liña 194** — `spark_completed_at`.
- **Liña 195** — `prediction_started_at`.
- **Liña 196** — `prediction_completed_at`.
- **Liña 197** — `total_latency_ms = Column(Integer, nullable=True)`.
- **Liña 198** — `status = Column(Enum(ProcessingStatus), ...)`.
- **Liña 199** — `error_message = Column(Text, nullable=True)`.
- **Liña 200** — `created_at`.
- **Liña 202** — `image = relationship("Image", back_populates="processing_events")`.
- **Liña 205–207** — Komentáriu seksaun: `rejected_events`: eventu Kafka aat liu (tabela dead-letter).
- **Liña 208** — `class RejectedEvent(Base):`
- **Liña 209** — `__tablename__ = "rejected_events"`.
- **Liña 211** — `rejected_event_id = Column(String, primary_key=True, default=new_uuid)`.
- **Liña 212** — `image_id = Column(String, nullable=True)` — komentáriu: bele la koresponde ba liña `Image`.
- **Liña 213** — `raw_event = Column(Text, nullable=True)` — komentáriu: JSON eventu.
- **Liña 214** — `reason = Column(Text, nullable=False)`.
- **Liña 215** — `created_at`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `ProcessingEvent(image_id="a1b2...", kafka_received_at=t0, spark_started_at=t0+0.01s, spark_completed_at=t0+2.1s, total_latency_ms=2100, status="COMPLETED")`. Eventu la ho `image_id` → `RejectedEvent(image_id=None, raw_event='{"kind": "drive_upload", ...}', reason="Missing required field: image_id")`.

**6. Tipu dadus**

`*_at`: `TIMESTAMPTZ`; `total_latency_ms`: `INTEGER`; `raw_event`, `reason`: `TEXT`.

**7. Ligasaun OOP**

`ProcessingEvent` iha komposisaun ho `Image` (cascade). `RejectedEvent` mesak (la iha relasaun).

**8. Impaktu integrasaun**

`persist_result` hakerek rua (Modulu 01). Dashboard hatudu tempu no eventu rejeitadu.

**9. Checkpoint ba principiante**

**P:** Tanba sá `rejected_events.image_id` la iha FK?
**R:** Eventu aat bele iha `image_id` ne'ebé la iha iha `images` (ka la iha `image_id`). FK sei rejeita liña ne'e.

#### D.2.7 Teste eskalabilidade: `ScaleTestRun` no `ScaleTestItem`

**1. Kódigu orijinál**

{{code storage/database.py 218-277}}

**2. Objetivu**

Tabela rua ba teste eskalabilidade: run ida (konfigurasaun, status, sumáriu) no item kada fotografia teste (tempu, rezultadu CNN). La iha FK, atu rezultadu moris liu factory reset.

**3. Fundamentu Python**

Konstante modulu (`SCALE_TEST_SOURCE`); `Column(..., index=True)`; `__table_args__` ho índise koluna rua.

**4. Esplikasaun liña-ba-liña**

- **Liña 218–221** — Komentáriu seksaun: teste eskalabilidade — fotografia teste iha liña rasik; la iha FK, entaun rezultadu moris liu factory reset.
- **Liña 222** — `SCALE_TEST_SOURCE = "scalability_test"` — valór `images.source` ba teste.
- **Liña 223** — `SCALE_TEST_USERNAME = "scalability-test"`.
- **Liña 226** — `class ScaleTestRun(Base):`
- **Liña 227** — `__tablename__ = "scale_test_runs"`.
- **Liña 229** — `run_id = Column(String, primary_key=True)`.
- **Liña 230** — Komentáriu: `PREPARING → RUNNING → COMPLETED | INCOMPLETE | FAILED | ABORTED`.
- **Liña 231** — `status = Column(String, nullable=False, default="PREPARING")`.
- **Liña 232** — `phase` — pasu agora (ba ema lee).
- **Liña 233** — `pid` — prosesu runner, atu hatene run mate.
- **Liña 234** — `requested_size = Column(Integer, nullable=False)` — fotografia hira.
- **Liña 235** — `replay_of` — `run_id` ne'ebé nia hili fotografia reuza.
- **Liña 236** — `publish_results = Column(Boolean, nullable=False, default=False)`.
- **Liña 237** — `drive_results_folder_id`.
- **Liña 238** — `config_json` — konfigurasaun benchmark.
- **Liña 239** — `selection_json` — kontajen + razaun ezklui.
- **Liña 240** — `summary_json` — métrika final.
- **Liña 241** — `error_message`.
- **Liña 242** — `data_cleaned_at` — `images`/`predictions` teste hamoos.
- **Liña 243** — `started_at`.
- **Liña 244** — `finished_at`.
- **Liña 247** — `class ScaleTestItem(Base):`
- **Liña 248** — `__tablename__ = "scale_test_items"`.
- **Liña 250** — `image_id = Column(String, primary_key=True)` — liña `Image` teste nian.
- **Liña 251** — `run_id = Column(String, nullable=False, index=True)`.
- **Liña 252** — `source_image_id` — `image_id` orijinál agrikultór.
- **Liña 253** — `source_drive_file_id`.
- **Liña 254** — `source_user_id` — pasta kópia lokál.
- **Liña 255** — `local_copy_status` — `verified` | `downloaded`.
- **Liña 256** — `download_seconds`.
- **Liña 257** — `size_bytes`.
- **Liña 258** — `status` — `STAGED`, `PUBLISHED`, `COMPLETED`, `FAILED`.
- **Liña 259** — `error_message`.
- **Liña 260** — `published_at`.
- **Liña 261** — `spark_received_at`.
- **Liña 262** — `completed_at`.
- **Liña 263** — `timings_json` — segundu kada pasu husi `process_event`.
- **Liña 264** — `pipeline_seconds` — Spark tomak ba fotografia ida.
- **Liña 265** — `db_write_seconds`.
- **Liña 266** — `drive_publish_seconds`.
- **Liña 267** — Komentáriu: rezultadu CNN, rai iha ne'e de'it (nunka haruka ba agrikultór).
- **Liña 268** — `predicted_class`.
- **Liña 269** — `prediction_confidence`.
- **Liña 270** — `is_uncertain`.
- **Liña 271** — `risk_level` (String, la'ós enum).
- **Liña 272** — Komentáriu: fatin foti fotografia fonte.
- **Liña 273** — `location`.
- **Liña 274** — `gps_latitude`.
- **Liña 275** — `gps_longitude`.
- **Liña 277** — `__table_args__ = (Index("ix_scale_test_items_run_status", "run_id", "status"),)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* run `"run-123"` ho `requested_size=100` → item 100 ho `status="STAGED"` → `PUBLISHED` → `COMPLETED` (ho `predicted_class`, `pipeline_seconds`).

**6. Tipu dadus**

String/Integer/Float/Boolean/Text/DateTime; `status` iha rua mak `String` livre (la'ós enum).

**7. Ligasaun OOP**

Klase herda `Base`. Relasaun ORM: Not implemented in the current codebase (la iha `relationship`/FK — ligasaun lójiku `run_id`).

**8. Impaktu integrasaun**

`spark/preprocessing.scale_test_run_for`, `storage/results.publish_result_to_drive`, `batch_analytics` (`_SCALE_TEST_ITEMS_SQL`), dashboard pájina teste.

**9. Checkpoint ba principiante**

**P:** Tanba sá índise ho koluna rua (`run_id`, `status`)?
**R:** Query sira hanesan "item hira COMPLETED iha run X" uza koluna rua ne'e hamutuk — índise kombinadu halo lalais.

#### D.2.8 `ImageReview` no `StorageAuditLog`

**1. Kódigu orijinál**

{{code storage/database.py 280-311}}

**2. Objetivu**

`image_reviews`: desizaun admin (label ka "discarded") iha pájina "Label photos" — separadu husi prediksaun no moris liu factory reset. `storage_audit_log`: kada asaun armazenamentu destrutivu husi dashboard.

**3. Fundamentu Python**

Hanesan modelu seluk.

**4. Esplikasaun liña-ba-liña**

- **Liña 280–283** — Komentáriu seksaun: `image_reviews` — label admin (ka "discarded") husi pájina Label photos; separadu husi prediksaun; moris liu factory reset.
- **Liña 284** — `class ImageReview(Base):`
- **Liña 285** — `__tablename__ = "image_reviews"`.
- **Liña 287** — `image_id = Column(String, primary_key=True)`.
- **Liña 288** — `decision` — `"labelled"` | `"discarded"`.
- **Liña 289** — `confirmed_class` — só bainhira `labelled`.
- **Liña 290** — `predicted_class_at_review` — buat modelu hatete, atu kompara.
- **Liña 291** — `reviewed_by`.
- **Liña 292** — `reviewed_at`.
- **Liña 295–297** — Komentáriu seksaun: `storage_audit_log` — asaun destrutivu hotu husi dashboard.
- **Liña 298** — `class StorageAuditLog(Base):`
- **Liña 299** — `__tablename__ = "storage_audit_log"`.
- **Liña 301** — `audit_id = Column(String, primary_key=True, default=new_uuid)`.
- **Liña 302** — `action` — `cleanup_delete` | `cache_repair` | `factory_reset`.
- **Liña 303** — `category` — kategoria armazenamentu (`storage/image_cache.py`).
- **Liña 304** — `target` — arkivu, relativu ba pasta kategoria.
- **Liña 305** — `reason`.
- **Liña 306** — `outcome` — `done` | `skipped` | `failed` | `dry_run`.
- **Liña 307** — `detail` — testu erru ka sumáriu.
- **Liña 308** — `performed_by`.
- **Liña 309** — `created_at`.
- **Liña 311** — `__table_args__ = (Index("ix_storage_audit_log_created_at", "created_at"),)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* admin label fotografia `a1b2` hanesan `gray_leaf_spot` (modelu hatete `blight`) → `ImageReview(image_id="a1b2", decision="labelled", confirmed_class="gray_leaf_spot", predicted_class_at_review="blight", reviewed_by="admin")`.

**6. Tipu dadus**

`VARCHAR`/`TEXT`/`TIMESTAMPTZ`.

**7. Ligasaun OOP**

Klase herda `Base`; la iha relasaun ORM.

**8. Impaktu integrasaun**

`v_admin_image_catalog` kombina `image_reviews` (D.2.16). Label foun sai dadus treinu (Modulu 05).

**9. Checkpoint ba principiante**

**P:** Tanba sá revizaun admin la rai iha tabela `predictions`?
**R:** Prediksaun mak buat modelu hatete; revizaun mak buat ema konfirma. Separa sira mantein evidénsia rua no permite kompara.

#### D.2.9 Mixin `_SparkOutput` no `AnalyticsSummary`

**1. Kódigu orijinál**

{{code storage/database.py 314-342}}

**2. Objetivu**

Tabela analítika hotu (ne'ebé Spark hakerek de'it) remata ho koluna `generated_at` — fó liuhusi **mixin** `_SparkOutput`. `analytics_summary`: liña ida ho figura prinsipál no tempu pipeline.

**3. Fundamentu Python**

- **Mixin**: klase ki'ik ne'ebé la'ós tabela (la herda `Base`) maibé fó atributu ba klase seluk.
- **Herança múltipla**: `class AnalyticsSummary(_SparkOutput, Base)` — atributu `generated_at` husi mixin + tabela husi `Base`.
- **Docstring klase**.

**4. Esplikasaun liña-ba-liña**

- **Liña 314–316** — Komentáriu seksaun: tabela analítika, prenxe husi Spark de'it (`spark/batch_analytics.py`).
- **Liña 317** — `class _SparkOutput:`
- **Liña 318** — Docstring: bainhira Spark kalkula liña (tabela analítika hotu remata ho nia).
- **Liña 319** — `generated_at = Column(DateTime(timezone=True), nullable=False)`.
- **Liña 322** — `class AnalyticsSummary(_SparkOutput, Base):`
- **Liña 323** — Docstring: liña ida: figura prinsipál no tempu pipeline.
- **Liña 324** — `__tablename__ = "analytics_summary"`.
- **Liña 326** — `summary_id = Column(Integer, primary_key=True)` — sempre 1.
- **Liña 327** — `total_users` — konta la hamoos.
- **Liña 328** — `total_images`.
- **Liña 329** — `total_healthy`.
- **Liña 330** — `total_diseased` — klasifika, la healthy, la uncertain.
- **Liña 331** — `total_uncertain`.
- **Liña 332** — `total_unclassified` — seidauk iha prediksaun (pending/failed).
- **Liña 333** — `total_high_risk`.
- **Liña 334** — `total_processed` — iha prediksaun.
- **Liña 335** — `total_failed`.
- **Liña 336** — `submissions_24h` — upload iha oras 24 molok `generated_at`.
- **Liña 337** — `active_farmers_24h`.
- **Liña 338** — `most_detected_disease` — klase barak liu exetu healthy.
- **Liña 339** — `avg_processing_latency_ms` — ingested → predicted.
- **Liña 340** — `avg_capture_to_upload_seconds`.
- **Liña 341** — `avg_upload_to_ingestion_seconds`.
- **Liña 342** — `avg_end_to_end_latency_seconds` — uploaded → predicted.

**5. Walkthrough ezekusaun**

`AnalyticsSummary.__table__.columns` inklui `generated_at` (husi mixin) + koluna 17 iha klase = koluna 18. Spark hakerek liña ida ho `summary_id=1` (Modulu 01 D.4.5).

**6. Tipu dadus**

Kontajen `BigInteger` (`BIGINT`); média `Float`; `most_detected_disease` `String`; `generated_at` `TIMESTAMPTZ`.

**7. Ligasaun OOP**

**Herança múltipla + mixin implementadu** — ezemplu klaru reuza kódigu: koluna `generated_at` define dala ida no tabela analítika 10 hotu simu. Orden `(_SparkOutput, Base)`: Python buka atributu uluk iha `_SparkOutput` (MRO — *Method Resolution Order*).

**8. Impaktu integrasaun**

`batch_analytics.write_tables` uza `CREATE TABLE ..._staging (LIKE analytics_summary)` — estrutura husi ORM. `analytics_reader.kpi_summary` lee.

**9. Checkpoint ba principiante**

**P:** Tanba sá `_SparkOutput` la herda `Base`?
**R:** Se herda, SQLAlchemy sei koko halo nia tabela rasik. Mixin só fó koluna ba sub-klase.

#### D.2.10 Tabela analítika agrikultór no `ANALYTICS_TABLES`

**1. Kódigu orijinál**

{{code storage/database.py 345-405}}

**2. Objetivu**

Tabela analítika 6 tan (distribuisaun moras, distribuisaun risku, risku aas tuir fatin, tendénsia moras, moras loron-loron, sumáriu loron-loron kada fatin) no tuple naran tabela 7.

**3. Fundamentu Python**

**Chave primária kompostu**: `primary_key=True` iha koluna barak (ezemplu `location` + `day` + `predicted_class`).

**4. Esplikasaun liña-ba-liña**

- **Liña 345** — `class AnalyticsDiseaseDistribution(_SparkOutput, Base):`
- **Liña 346** — `__tablename__ = "analytics_disease_distribution"`.
- **Liña 348** — `predicted_class = Column(String, primary_key=True)`.
- **Liña 349** — `photos = Column(BigInteger, nullable=False)`.
- **Liña 352** — `class AnalyticsRiskDistribution(_SparkOutput, Base):`
- **Liña 353** — `__tablename__ = "analytics_risk_distribution"`.
- **Liña 355** — `risk_level = Column(String, primary_key=True)`.
- **Liña 356** — `photos`.
- **Liña 359** — `class AnalyticsHighRiskByLocation(_SparkOutput, Base):`
- **Liña 360** — Docstring: kada munisípiu: fotografia analiza no hira HIGH.
- **Liña 361** — `__tablename__ = "analytics_high_risk_by_location"`.
- **Liña 363** — `location = Column(String, primary_key=True)` — mamuk → "Unknown location".
- **Liña 364** — `high`.
- **Liña 365** — `total`.
- **Liña 366** — `high_pct = Column(Float, nullable=False)`.
- **Liña 369** — `class AnalyticsDiseaseTrend(_SparkOutput, Base):`
- **Liña 370** — Docstring: fotografia klasifika kada munisípiu, loron no moras.
- **Liña 371** — `__tablename__ = "analytics_disease_trend"`.
- **Liña 373** — `location = Column(String, primary_key=True)`.
- **Liña 374** — `day = Column(Date, primary_key=True)`.
- **Liña 375** — `predicted_class = Column(String, primary_key=True)`.
- **Liña 376** — `photos`.
- **Liña 379** — `class AnalyticsDiseaseDaily(_SparkOutput, Base):`
- **Liña 380** — Docstring: kada loron no moras, munisípiu hotu hamutuk.
- **Liña 381** — `__tablename__ = "analytics_disease_daily"`.
- **Liña 383** — `day = Column(Date, primary_key=True)`.
- **Liña 384** — `predicted_class = Column(String, primary_key=True)`.
- **Liña 385** — `photos`.
- **Liña 388** — `class BatchLocationDailySummary(_SparkOutput, Base):`
- **Liña 389** — Docstring: kada munisípiu no loron upload: sumáriu batch ba relatóriu/BI.
- **Liña 390** — `__tablename__ = "batch_location_daily_summary"`.
- **Liña 392** — `location = Column(String, primary_key=True)`.
- **Liña 393** — `summary_date = Column(Date, primary_key=True)`.
- **Liña 394** — `total_images`.
- **Liña 395** — `total_healthy`.
- **Liña 396** — `total_diseased`.
- **Liña 397** — `avg_risk_score = Column(Float, nullable=True)`.
- **Liña 398** — `high_risk_count`.
- **Liña 401** — `ANALYTICS_TABLES = (`
- **Liña 402** — `"analytics_summary", "analytics_disease_distribution", "analytics_risk_distribution",`
- **Liña 403** — `"analytics_high_risk_by_location", "analytics_disease_trend", "analytics_disease_daily",`
- **Liña 404** — `"batch_location_daily_summary",`
- **Liña 405** — `)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* `analytics_disease_trend`: `("Aileu", 2026-10-21, "blight", 2, t)`, `("Aileu", 2026-10-21, "healthy", 1, t)`. Chave kompostu garante kombinasaun ida de'it.

**6. Tipu dadus**

`location`, `predicted_class`, `risk_level`: `VARCHAR`; `day`, `summary_date`: `DATE`; kontajen `BIGINT`; `high_pct`, `avg_risk_score`: `DOUBLE PRECISION`.

**7. Ligasaun OOP**

Klase 6 hotu herda mixin + `Base` (reuza `generated_at`).

**8. Impaktu integrasaun**

Naran hanesan ho chave `compute()` Spark (Modulu 01 D.4.6). `ANALYTICS_TABLES` uza iha `TABLEAU_TABLES` (D.2.14) — Tableau bele lee.

**9. Checkpoint ba principiante**

**P:** Tanba sá `analytics_disease_trend` uza chave primária koluna 3?
**R:** Liña ida reprezenta kombinasaun úniku fatin + loron + moras. Chave kompostu garante la iha liña duplikadu ba kombinasaun hanesan.

#### D.2.11 Tabela analítika teste eskalabilidade

**1. Kódigu orijinál**

{{code storage/database.py 408-436}}

**2. Objetivu**

Tabela 3 ho figura moras pájina teste eskalabilidade kada run (la partilla ho Tableau).

**3. Fundamentu Python**

Hanesan D.2.10.

**4. Esplikasaun liña-ba-liña**

- **Liña 408** — Komentáriu: figura teste eskalabilidade, kada run (la partilla ho Tableau).
- **Liña 409** — `class AnalyticsScaleTestDiseaseByLocation(_SparkOutput, Base):`
- **Liña 410** — Docstring: fotografia teste kada run, munisípiu no moras.
- **Liña 411** — `__tablename__ = "analytics_scale_test_disease_by_location"`.
- **Liña 413** — `run_id = Column(String, primary_key=True)`.
- **Liña 414** — `location = Column(String, primary_key=True)` — mamuk → "Unknown location".
- **Liña 415** — `predicted_class = Column(String, primary_key=True)`.
- **Liña 416** — `photos`.
- **Liña 419** — `class AnalyticsScaleTestDiseaseDistribution(_SparkOutput, Base):`
- **Liña 420** — Docstring: fotografia teste kada run no moras.
- **Liña 421** — `__tablename__ = "analytics_scale_test_disease_distribution"`.
- **Liña 423** — `run_id`.
- **Liña 424** — `predicted_class`.
- **Liña 425** — `photos`.
- **Liña 428** — `class AnalyticsScaleTestHighRiskByLocation(_SparkOutput, Base):`
- **Liña 429** — Docstring: kada run no munisípiu: fotografia teste analiza no hira HIGH.
- **Liña 430** — `__tablename__ = "analytics_scale_test_high_risk_by_location"`.
- **Liña 432** — `run_id`.
- **Liña 433** — `location`.
- **Liña 434** — `high`.
- **Liña 435** — `total`.
- **Liña 436** — `high_pct`.

**5. Walkthrough ezekusaun**

`analytics_reader.scale_test_disease_distribution("run-123")` lee tabela ne'e ho `WHERE run_id = :run_id`.

**6. Tipu dadus**

Hanesan D.2.10, plus `run_id VARCHAR`.

**7. Ligasaun OOP**

Herança mixin + `Base`.

**8. Impaktu integrasaun**

Spark `compute_scale_test` (Modulu 01 D.4.6).

**9. Checkpoint ba principiante**

**P:** Tanba sá tabela teste la iha `TABLEAU_TABLES`?
**R:** Komentáriu liña 408: figura teste la partilla ho Tableau — BI agrikultór la kahur ho dadus teste.

#### D.2.12 Engine no sesaun

**1. Kódigu orijinál**

{{code storage/database.py 439-453}}

**2. Objetivu**

Harii `engine` (pool konesaun) husi `DATABASE_URL`, ho opsaun espesífiku SQLite/PostgreSQL, no fábrika sesaun `SessionLocal`.

**3. Fundamentu Python**

- **Decorator ho argumentu** `@event.listens_for(engine, "connect")` iha `if` nia laran — rejista funsaun só ba SQLite.
- **`sessionmaker(...)`** fó **klase** sesaun; `SessionLocal()` harii objetu sesaun.

**4. Esplikasaun liña-ba-liña**

- **Liña 439–441** — Komentáriu seksaun engine/sesaun.
- **Liña 442** — `_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}` — SQLite permite thread barak.
- **Liña 443** — Komentáriu: `pool_pre_ping` troka konesaun ne'ebé mate bainhira PostgreSQL restart.
- **Liña 444–445** — `engine = create_engine(DATABASE_URL, connect_args=_connect_args, future=True, pool_pre_ping=not DATABASE_URL.startswith("sqlite"))`.
- **Liña 447** — `if DATABASE_URL.startswith("sqlite"):`
- **Liña 448** — `@event.listens_for(engine, "connect")`.
- **Liña 449** — `def _sqlite_wal(dbapi_conn, _record):`
- **Liña 450** — Komentáriu: WAL: leitór liur la blokeia hakerek.
- **Liña 451** — `dbapi_conn.execute("PRAGMA journal_mode=WAL")`.
- **Liña 453** — `SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)`.

**5. Walkthrough ezekusaun**

`DATABASE_URL = "postgresql+psycopg2://ews_user:***@127.0.0.1:5432/crop_disease"` → engine PostgreSQL ho `pool_pre_ping=True`. `with SessionLocal() as s:` → sesaun foun; remata bloku → taka (konesaun fila ba pool).

**6. Tipu dadus**

`_connect_args`: `dict`; `engine`: `sqlalchemy.engine.Engine`; `SessionLocal`: `sessionmaker` (bolu → `Session`).

**7. Ligasaun OOP**

`engine` mak objetu partilla (singleton modulu) ho **pool** konesaun (komposisaun interior SQLAlchemy). `SessionLocal` mak **fábrika** (padraun Factory). `@event.listens_for` mak padraun **Observer**: funsaun rejista atu reaje ba eventu "connect".

**8. Impaktu integrasaun**

`engine` uza husi `batch_analytics.write_tables`, `analytics_reader`; `SessionLocal` husi Spark, dashboard, `init_db`, `farmers`...

**9. Checkpoint ba principiante**

**P:** Saida mak `pool_pre_ping` halo?
**R:** Molok uza konesaun husi pool, SQLAlchemy kontrola se nia moris; se PostgreSQL restart, nia troka konesaun mate ho foun (evita erru).

#### D.2.13 Migrasaun koluna: lista no `_add_missing_columns`

**1. Kódigu orijinál**

{{code storage/database.py 456-512}}

**2. Objetivu**

`create_all` harii tabela foun maibé **la aumenta koluna** ba tabela ne'ebé iha ona. Lista ne'e rai koluna ne'ebé aumenta depois versaun dahuluk; `_add_missing_columns` kompara ho base de dadus no halai `ALTER TABLE ... ADD COLUMN` ba sira ne'ebé falta.

**3. Fundamentu Python**

- **Lista tuple** `[("naran", "TIPU"), ...]`.
- **Set comprehension** `{col["name"] for col in ...}`.
- **`inspect(engine)`** — objetu ne'ebé lee estrutura base de dadus.
- **`\`** — kontinua liña.

**4. Esplikasaun liña-ba-liña**

- **Liña 456** — Komentáriu: koluna aumenta depois versaun dahuluk (`create_all` la aumenta koluna).
- **Liña 457** — `_USERS_MIGRATION_COLUMNS = [`
- **Liña 458** — `("full_name", "VARCHAR"),`
- **Liña 459** — `("email_verified", "BOOLEAN NOT NULL DEFAULT 0"),`
- **Liña 460** — `("verification_code_hash", "VARCHAR"),`
- **Liña 461** — `("verification_code_expires_at", "TIMESTAMP"),`
- **Liña 462** — `("verification_attempts", "INTEGER NOT NULL DEFAULT 0"),`
- **Liña 463** — `("last_verification_sent_at", "TIMESTAMP"),`
- **Liña 464** — `("is_deleted", "BOOLEAN NOT NULL DEFAULT 0"),`
- **Liña 465** — `("deleted_at", "TIMESTAMP"),`
- **Liña 466** — `]`.
- **Liña 468** — `_IMAGES_MIGRATION_COLUMNS = [`
- **Liña 469** — `("location", "VARCHAR"),`
- **Liña 470** — `("content_hash", "VARCHAR"),`
- **Liña 471** — `("gps_latitude", "FLOAT"),`
- **Liña 472** — `("gps_longitude", "FLOAT"),`
- **Liña 473** — `("gps_altitude_m", "FLOAT"),`
- **Liña 474** — `("gps_source", "VARCHAR"),`
- **Liña 475** — `("camera_make", "VARCHAR"),`
- **Liña 476** — `("camera_model", "VARCHAR"),`
- **Liña 477** — `("image_width", "INTEGER"),`
- **Liña 478** — `("image_height", "INTEGER"),`
- **Liña 479** — `("result_published_at", "TIMESTAMP WITH TIME ZONE"),`
- **Liña 480** — `]`.
- **Liña 482** — `_PREDICTIONS_MIGRATION_COLUMNS = [`
- **Liña 483** — `("is_uncertain", "BOOLEAN NOT NULL DEFAULT 0"),`
- **Liña 484** — `]`.
- **Liña 486** — `_PROCESSING_EVENTS_MIGRATION_COLUMNS = [`
- **Liña 487** — `("api_published_at", "TIMESTAMP"),`
- **Liña 488** — `]`.
- **Liña 490** — `_SCALE_TEST_ITEMS_MIGRATION_COLUMNS = [`
- **Liña 491** — `("prediction_confidence", "FLOAT"),`
- **Liña 492** — `("is_uncertain", "BOOLEAN"),`
- **Liña 493** — `("risk_level", "VARCHAR"),`
- **Liña 494** — `("location", "VARCHAR"),`
- **Liña 495** — `("gps_latitude", "FLOAT"),`
- **Liña 496** — `("gps_longitude", "FLOAT"),`
- **Liña 497** — `]`.
- **Liña 500** — `def _add_missing_columns(table_name: str, columns: list[tuple[str, str]]):`
- **Liña 501** — `inspector = inspect(engine)`.
- **Liña 502** — `if table_name not in inspector.get_table_names():`
- **Liña 503** — `return` — komentáriu: `create_all` sei harii foun ho koluna hotu.
- **Liña 504** — `existing_columns = {col["name"] for col in inspector.get_columns(table_name)}`.
- **Liña 505** — `is_postgres = engine.dialect.name == "postgresql"`.
- **Liña 506** — `with engine.begin() as conn:` — transasaun.
- **Liña 507** — `for column_name, column_type in columns:`
- **Liña 508** — `if column_name in existing_columns:`
- **Liña 509** — `continue`.
- **Liña 510–511** — `ddl_type = column_type.replace("BOOLEAN NOT NULL DEFAULT 0", "BOOLEAN NOT NULL DEFAULT FALSE") if is_postgres else column_type` — PostgreSQL presiza `FALSE`, la'ós `0`.
- **Liña 512** — `conn.execute(sql_text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}"))`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* base de dadus tuan la iha `images.result_published_at` → `ALTER TABLE images ADD COLUMN result_published_at TIMESTAMP WITH TIME ZONE`. Halai fali → koluna iha → `continue` (idempotent).

**6. Tipu dadus**

Lista: `list[tuple[str, str]]`; `existing_columns`: `set[str]`; `is_postgres`: `bool`; return `None`.

**7. Ligasaun OOP**

`inspector` mak objetu `Inspector` SQLAlchemy; `engine.dialect.name` — atributu objetu. Ferramenta migrasaun formál (Alembic): Not implemented in the current codebase.

**8. Impaktu integrasaun**

`init_db()` bolu ba tabela 5 (D.2.17).

**9. Checkpoint ba principiante**

**P:** Tanba sá `create_all` la sufisiente?
**R:** `create_all` harii tabela ne'ebé la iha, maibé la muda tabela ne'ebé iha ona — koluna foun la tama. Funsaun ne'e aumenta koluna sira ne'e.

#### D.2.14 Migrasaun hamoos, `LATEST_PREDICTION_CTE`, konstante Tableau

**1. Kódigu orijinál**

{{code storage/database.py 515-546}}

**2. Objetivu**

Migrasaun dala ida: hamoos feature "fields" tuan (tabela `field_users`, `fields`, koluna `images.field_id`) no tabela `system_settings` (modu agora iha `processing_mode.json`). Define CTE "prediksaun foun liu" no naran view/tabela Tableau.

**3. Fundamentu Python**

String multi-liña konstante (`"""..."""`); tuple kombina (`("images", "predictions") + ANALYTICS_TABLES`).

**4. Esplikasaun liña-ba-liña**

- **Liña 515** — `def _drop_field_feature(conn):`
- **Liña 516** — Docstring: migrasaun dala ida: hamoos tabela fields no `Image.field_id`.
- **Liña 517** — `inspector = inspect(engine)`.
- **Liña 518** — `is_postgres = engine.dialect.name == "postgresql"`.
- **Liña 520** — `if "images" in inspector.get_table_names():`
- **Liña 521** — `image_columns = {col["name"] for col in inspector.get_columns("images")}`.
- **Liña 522** — `if "field_id" in image_columns:`
- **Liña 523** — `conn.execute(sql_text("DROP INDEX IF EXISTS ix_images_field_id"))`.
- **Liña 524** — `conn.execute(sql_text("ALTER TABLE images DROP COLUMN field_id"))`.
- **Liña 526** — `for table in ("field_users", "fields"):`
- **Liña 527** — `if table in inspector.get_table_names():`
- **Liña 528** — `drop_stmt = f"DROP TABLE IF EXISTS {table}" + (" CASCADE" if is_postgres else "")`.
- **Liña 529** — `conn.execute(sql_text(drop_stmt))`.
- **Liña 532** — `def _drop_system_settings(conn):`
- **Liña 533** — Docstring: hamoos `system_settings` (modu agora iha `processing_mode.json`).
- **Liña 534** — `conn.execute(sql_text("DROP TABLE IF EXISTS system_settings"))`.
- **Liña 537** — Komentáriu: prediksaun foun liu kada imajen (`predicted_at` foun liu, depois `prediction_id`) hanesan `rn = 1`.
- **Liña 538** — `LATEST_PREDICTION_CTE = """WITH latest AS (`
- **Liña 539** — `SELECT p.*, ROW_NUMBER() OVER (PARTITION BY p.image_id`
- **Liña 540** — `ORDER BY p.predicted_at DESC NULLS LAST, p.prediction_id DESC) AS rn` — `NULLS LAST`: prediksaun la ho tempu ba ikus.
- **Liña 541** — `FROM predictions p`
- **Liña 542** — `)"""`.
- **Liña 545** — `TABLEAU_VIEW = "v_tableau_images"`.
- **Liña 546** — `TABLEAU_TABLES = ("images", "predictions") + ANALYTICS_TABLES` — tabela 9 ne'ebé Tableau bele lee.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* CTE: prediksaun `(p7, a1, 2026-10-20)`, `(p3, a1, 2026-10-19)`, `(p9, a1, NULL)` → `rn`: p7=1, p3=2, p9=3 (NULL ikus).

**6. Tipu dadus**

`LATEST_PREDICTION_CTE`, `TABLEAU_VIEW`: `str`; `TABLEAU_TABLES`: `tuple[str, ...]` (elementu 9).

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun + konstante.)

**8. Impaktu integrasaun**

`LATEST_PREDICTION_CTE` reuza iha view 2 no `analytics_reader.images_with_predictions` — regra hanesan ho Spark `photos_with_latest_prediction` (Modulu 01).

**9. Checkpoint ba principiante**

**P:** Tanba sá `NULLS LAST` importante?
**R:** Iha PostgreSQL, `DESC` rai `NULL` uluk. La ho `NULLS LAST`, prediksaun la ho `predicted_at` bele sai "foun liu".

#### D.2.15 View Tableau no `tableau_reader`

**1. Kódigu orijinál**

{{code storage/database.py 549-601}}

**2. Objetivu**

Harii fali view `v_tableau_images`: imajen ida = liña ida ho prediksaun foun liu, tempu iha Timor-Leste Time, la ho imajen teste. Se PostgreSQL no password Tableau iha, harii/atualiza login *read-only* `tableau_reader` ho permisaun SELECT — **nunka** tabela `users`.

**3. Fundamentu Python**

- **Funsaun interior** `local(col)` fó SQL string.
- **f-string grande** ne'ebé harii SQL.
- **`.replace("'", "''")`** — escape aspas SQL.
- **Parámetru SQL ho naran** `:r`.

**4. Esplikasaun liña-ba-liña**

- **Liña 549** — `def _create_tableau_view(conn):`
- **Liña 550–551** — Docstring: view read-only Tableau: liña ida kada imajen ho prediksaun foun liu, tempu Timor-Leste; harii fali kada `init_db()`.
- **Liña 552** — `is_postgres = engine.dialect.name == "postgresql"`.
- **Liña 554** — `def local(col):`
- **Liña 555** — `if is_postgres:`
- **Liña 556** — Komentáriu: UTC → oras Timor-Leste, la importa fuso Tableau.
- **Liña 557** — `return f"({col} AT TIME ZONE 'UTC') + INTERVAL '{LOCAL_TZ_OFFSET_HOURS} hours'"`.
- **Liña 558** — `return f"datetime({col}, '{LOCAL_TZ_OFFSET_HOURS:+d} hours')"` — SQLite.
- **Liña 560** — `conn.execute(sql_text(f"DROP VIEW IF EXISTS {TABLEAU_VIEW}"))`.
- **Liña 561** — `conn.execute(sql_text(f"""`
- **Liña 562** — `CREATE VIEW {TABLEAU_VIEW} AS`
- **Liña 563** — `{LATEST_PREDICTION_CTE}`
- **Liña 564** — `SELECT`
- **Liña 565** — `i.image_id, i.location, i.source, i.status,`
- **Liña 566** — `u.username AS farmer_username,` — username (la'ós email/hash).
- **Liña 567** — `{local('i.captured_at')} AS captured_at,`
- **Liña 568** — `{local('i.uploaded_at')} AS uploaded_at,`
- **Liña 569** — `{local('i.ingested_at')} AS ingested_at,`
- **Liña 570** — `{local('i.processed_at')} AS processed_at,`
- **Liña 571** — `i.gps_latitude, i.gps_longitude, i.gps_source, i.camera_make, i.camera_model,`
- **Liña 572** — `l.predicted_class, l.prediction_confidence, l.is_uncertain,`
- **Liña 573** — `l.lesion_score, l.discoloration_pct, l.texture_irregularity,`
- **Liña 574** — `l.leaf_area_affected_pct, l.risk_score, l.risk_level,`
- **Liña 575** — `l.model_version,`
- **Liña 576** — `{local('l.predicted_at')} AS predicted_at,`
- **Liña 577** — `l.processing_latency_ms`
- **Liña 578** — `FROM images i`
- **Liña 579** — `LEFT JOIN latest l ON l.image_id = i.image_id AND l.rn = 1`
- **Liña 580** — `LEFT JOIN users u ON u.user_id = i.user_id`
- **Liña 581** — `WHERE i.source <> '{SCALE_TEST_SOURCE}'`
- **Liña 582** — `"""))`.
- **Liña 583** — `if is_postgres and TABLEAU_DB_PASSWORD:`
- **Liña 584** — `_ensure_tableau_reader(conn)`.
- **Liña 587** — `def _ensure_tableau_reader(conn):`
- **Liña 588** — Docstring: harii ka atualiza login read-only Tableau no nia permisaun.
- **Liña 589** — `role = TABLEAU_DB_USER.replace('"', "")` — hasai aspas duplu (seguransa identifikadór).
- **Liña 590** — `password = TABLEAU_DB_PASSWORD.replace("'", "''")` — escape literal SQL (valór la hatudu).
- **Liña 591** — `exists = conn.execute(sql_text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}).scalar()`.
- **Liña 592** — `verb = "ALTER" if exists else "CREATE"`.
- **Liña 593–594** — `{verb} ROLE "{role}" WITH LOGIN PASSWORD '...' NOSUPERUSER NOCREATEDB NOCREATEROLE` — privilejiu mínimu.
- **Liña 595** — `db_name = conn.execute(sql_text("SELECT current_database()")).scalar()`.
- **Liña 596** — `GRANT CONNECT ON DATABASE "{db_name}" TO "{role}"`.
- **Liña 597** — `GRANT USAGE ON SCHEMA public TO "{role}"`.
- **Liña 598** — `GRANT SELECT ON {TABLEAU_VIEW} TO "{role}"`.
- **Liña 599** — Komentáriu: tabela raw mós, nunka `users` (hash password, email).
- **Liña 600** — `for table in TABLEAU_TABLES:`
- **Liña 601** — `conn.execute(sql_text(f'GRANT SELECT ON {table} TO "{role}"'))`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* imajen ho `uploaded_at = 2026-10-20 01:15 UTC` → view hatudu `2026-10-20 10:15` (+9 h). Tableau konekta ho `tableau_reader` → `SELECT * FROM users` → *permission denied*.

**6. Tipu dadus**

`local(col: str) -> str`; `role`, `password`, `verb`, `db_name`: `str`; `exists`: `int ka None`.

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun.) Prinsípiu seguransa: *least privilege*.

**8. Impaktu integrasaun**

Tableau Desktop (Modulu 06) lee view; analítika Spark (`analytics_*`) mós bele lee.

**9. Checkpoint ba principiante**

**P:** Tanba sá `tableau_reader` la hetan SELECT ba `users`?
**R:** `users` iha email no hash password — dadus privadu. Tableau presiza só username iha view.

#### D.2.16 View `v_admin_image_catalog`

**1. Kódigu orijinál**

{{code storage/database.py 604-631}}

**2. Objetivu**

Katálogu imajen admin: liña ida kada imajen agrikultór ho agrikultór (email, naran, status), prediksaun foun liu, no desizaun revizaun. Harii fali kada `init_db()`. Tempu hela UTC.

**3. Fundamentu Python**

Hanesan D.2.15. SQL `CAST(x AS VARCHAR(32))` konverte enum ba testu.

**4. Esplikasaun liña-ba-liña**

- **Liña 604** — `ADMIN_CATALOG_VIEW = "v_admin_image_catalog"`.
- **Liña 607** — `def _create_admin_catalog_view(conn):`
- **Liña 608–609** — Docstring: katálogu imajen admin: liña ida kada imajen agrikultór ho agrikultór, prediksaun foun liu no desizaun revizaun; harii fali kada `init_db()`.
- **Liña 610** — `DROP VIEW IF EXISTS ...`.
- **Liña 611** — `conn.execute(sql_text(f"""`
- **Liña 612** — `CREATE VIEW {ADMIN_CATALOG_VIEW} AS`
- **Liña 613** — `{LATEST_PREDICTION_CTE}`
- **Liña 614** — `SELECT`
- **Liña 615** — koluna imajen: `image_id`, `user_id`, `image_path`, `original_filename`, `location`, `source`,
- **Liña 616** — `content_hash`, `CAST(i.status AS VARCHAR(32)) AS status`, `error_message`,
- **Liña 617** — tempu: `captured_at`, `uploaded_at`, `ingested_at`, `processed_at`, `result_published_at`,
- **Liña 618** — GPS/kámara,
- **Liña 619** — `image_width`, `image_height`,
- **Liña 620** — `u.email AS farmer_email, u.full_name AS farmer_name,` — admin de'it.
- **Liña 621** — `CAST(u.status AS VARCHAR(32)) AS farmer_status,`
- **Liña 622** — `u.is_deleted AS farmer_deleted,`
- **Liña 623** — prediksaun: `prediction_id`, `predicted_class`, `prediction_confidence`, `is_uncertain`,
- **Liña 624** — `risk_score`, `CAST(l.risk_level AS VARCHAR(16)) AS risk_level`, `model_version`, `predicted_at`,
- **Liña 625** — revizaun: `decision AS review_decision`, `confirmed_class`, `reviewed_by`, `reviewed_at`
- **Liña 626** — `FROM images i`
- **Liña 627** — `LEFT JOIN latest l ON ... AND l.rn = 1`
- **Liña 628** — `LEFT JOIN users u ON u.user_id = i.user_id`
- **Liña 629** — `LEFT JOIN image_reviews r ON r.image_id = i.image_id`
- **Liña 630** — `WHERE i.source <> '{SCALE_TEST_SOURCE}'`
- **Liña 631** — `"""))`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* imajen `a1b2` ho prediksaun `blight` no revizaun `gray_leaf_spot` → liña ida ho `predicted_class='blight'`, `review_decision='labelled'`, `confirmed_class='gray_leaf_spot'`.

**6. Tipu dadus**

View ho koluna 37 (konta husi lista SELECT liña 615–625).

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

Dashboard "Image management" (`storage/image_catalog.py`) lee view ne'e.

**9. Checkpoint ba principiante**

**P:** Tanba sá view ne'e iha `farmer_email` maibé view Tableau la iha?
**R:** View admin uza husi dashboard (admin autentikadu). View Tableau partilla ho ferramenta BI esternu — privasidade boot liu.

#### D.2.17 `init_db()`

**1. Kódigu orijinál**

{{code storage/database.py 634-654}}

**2. Objetivu**

Harii tabela no koluna ne'ebé falta; seguru atu halai fali (idempotent). Prenxe `result_published_at` ba fotografia tuan; halai migrasaun; harii fali view 2.

**3. Fundamentu Python**

Loop ho tuple pár `(tabela, lista)`; `with engine.begin()` transasaun.

**4. Esplikasaun liña-ba-liña**

- **Liña 634** — `def init_db():`
- **Liña 635** — Docstring: harii tabela no koluna ne'ebé falta; seguru halai fali.
- **Liña 636** — `Base.metadata.create_all(engine)` — tabela 19 (se seidauk iha).
- **Liña 637–638** — `had_published_column = "result_published_at" in {c["name"] for c in inspect(engine).get_columns("images")}`.
- **Liña 639–642** — `for table, columns in (("users", ...), ("images", ...), ("predictions", ...), ("processing_events", ...), ("scale_test_items", ...)):`
- **Liña 643** — `_add_missing_columns(table, columns)`.
- **Liña 644** — `if not had_published_column:`
- **Liña 645** — Komentáriu: fotografia ne'ebé remata molok koluna ne'e iha hetan ona rezultadu.
- **Liña 646** — `with engine.begin() as conn:`
- **Liña 647–648** — `UPDATE images SET result_published_at = processed_at WHERE status = 'COMPLETED' AND result_published_at IS NULL`.
- **Liña 649** — `with engine.begin() as conn:`
- **Liña 650** — `_drop_field_feature(conn)`.
- **Liña 651** — `_drop_system_settings(conn)`.
- **Liña 652** — `_create_tableau_view(conn)`.
- **Liña 653** — `_create_admin_catalog_view(conn)`.

**5. Walkthrough ezekusaun**

Base de dadus foun: `create_all` harii hotu → koluna hotu iha → view 2 harii. Base de dadus tuan: aumenta koluna falta → `UPDATE result_published_at` (dala ida) → view harii fali.

**6. Tipu dadus**

`had_published_column`: `bool`; return `None`.

**7. Ligasaun OOP**

`Base.metadata` — objetu `MetaData` ne'ebé kaer definisaun tabela hotu husi klase ORM (rejistu automátiku bainhira klase herda `Base`).

**8. Impaktu integrasaun**

Bolu husi `spark/streaming_job.run`, `batch_analytics.run/watch`, `init_db.run`, dashboard.

**9. Checkpoint ba principiante**

**P:** Tanba sá seguru halai `init_db()` dala barak?
**R:** Kada pasu kontrola uluk (`IF NOT EXISTS`, koluna iha ka lae, `DROP VIEW IF EXISTS`) — la halo buat ida dala rua.

### D.3 `storage/init_db.py`

#### D.3.1 `run()` — tabela + admin dahuluk

**1. Kódigu orijinál**

{{code storage/init_db.py 1-51}}

**2. Objetivu**

Harii tabela no, se la iha utilizadór ida, harii konta ADMIN dahuluk husi `BOOTSTRAP_ADMIN_USERNAME` / `BOOTSTRAP_ADMIN_PASSWORD`.

**3. Fundamentu Python**

`session.query(User).count()`; harii objetu ORM ho argumentu ho naran; return sedu.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: harii tabela no, se la iha utilizadór, konta ADMIN dahuluk husi `BOOTSTRAP_ADMIN_USERNAME` / `BOOTSTRAP_ADMIN_PASSWORD`.
- **Liña 4** — Liña mamuk docstring.
- **Liña 5** — Halai: `python -m storage.init_db`.
- **Liña 6** — Remata docstring.
- **Liña 8** — `import sys`.
- **Liña 10–12** — Konfigurasaun admin: username, password (segredu), email, naran.
- **Liña 13** — `init_db, SessionLocal, User, Role, UserStatus`.
- **Liña 14** — `hash_password`.
- **Liña 17** — `def run():`
- **Liña 18** — `init_db()`.
- **Liña 19** — `with SessionLocal() as session:`
- **Liña 20** — `existing = session.query(User).count()`.
- **Liña 21** — `if existing > 0:`
- **Liña 22** — `print(...)` — "N user(s) already exist — skipping admin bootstrap."
- **Liña 23** — `return`.
- **Liña 25** — `if not BOOTSTRAP_ADMIN_PASSWORD:`
- **Liña 26–31** — `print(...)` — tabela harii; la iha utilizadór no `BOOTSTRAP_ADMIN_PASSWORD` la define, entaun la harii admin; define no halai fali.
- **Liña 32** — `return`.
- **Liña 34** — `admin = User(`
- **Liña 35** — `username=BOOTSTRAP_ADMIN_USERNAME,`
- **Liña 36** — `full_name=BOOTSTRAP_ADMIN_FULL_NAME,`
- **Liña 37** — `email=BOOTSTRAP_ADMIN_EMAIL,`
- **Liña 38** — `password_hash=hash_password(BOOTSTRAP_ADMIN_PASSWORD),` — rai hash de'it.
- **Liña 39** — `role=Role.ADMIN,`
- **Liña 40** — `status=UserStatus.ACTIVE,`
- **Liña 41** — Komentáriu: konta admin konfigura, entaun marka verifika.
- **Liña 42** — `email_verified=True,`
- **Liña 43** — `)`.
- **Liña 44** — `session.add(admin)`.
- **Liña 45** — `session.commit()`.
- **Liña 46** — `print(...)` — "Bootstrapped ADMIN account '<username>'".
- **Liña 49** — `if __name__ == "__main__":`
- **Liña 50** — `run()`.
- **Liña 51** — `sys.exit(0)`.

**5. Walkthrough ezekusaun**

Instalasaun foun ho `BOOTSTRAP_ADMIN_PASSWORD` define → tabela + admin → login dashboard. Halai fali → "1 user(s) already exist — skipping".

**6. Tipu dadus**

`existing`: `int`; `admin`: `User`; return `None`.

**7. Ligasaun OOP**

Harii **objetu ORM** `User(...)`; `session.add` + `commit` (Unit of Work).

**8. Impaktu integrasaun**

`docker-compose.yml` komentáriu (liña 12) rekomenda halai ida ne'e depois `docker compose up -d`.

**9. Checkpoint ba principiante**

**P:** Tanba sá admin só harii bainhira tabela `users` mamuk?
**R:** Atu la harii admin daruak ka troka password admin ne'ebé iha ona kada vez script halai.

### D.4 `storage/schema.sql` — DDL PostgreSQL

*Nota:* bloku sira ne'e SQL. Pasu 3 "Fundamentu Python" iha ne'e esplika **fundamentu SQL** (haree tabela A.3).

#### D.4.1 Komentáriu no tabela `users`

**1. Kódigu orijinál**

{{code storage/schema.sql 1-30}}

**2. Objetivu**

Arkivu SQL ekivalente ba modelu ORM — ba setup manuál, revizaun, no kapítulu "Database Design" relatóriu. Tabela `users` ho `CHECK` ba papél no status.

**3. Fundamentu Python**

*(SQL)* `--` = komentáriu. `CHECK (kolom IN (...))` = regra valór. Iha ne'e enum mak `VARCHAR + CHECK`; ORM iha PostgreSQL harii tipu ENUM native (diferensa implementasaun; rezultadu valór hanesan).

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — Komentáriu: esquema PostgreSQL EWS.
- **Liña 2–5** — Komentáriu: ekivalente ba modelu SQLAlchemy; aplikasaun (`init_db.py`) harii automátiku liuhusi ORM; arkivu ne'e ba setup manuál, revizaun no relatóriu.
- **Liña 7** — `CREATE TABLE IF NOT EXISTS users (`
- **Liña 8** — `user_id VARCHAR PRIMARY KEY,`
- **Liña 9** — `username VARCHAR UNIQUE NOT NULL,`
- **Liña 10** — `full_name VARCHAR,`
- **Liña 11** — `email VARCHAR UNIQUE NOT NULL,`
- **Liña 12** — `password_hash VARCHAR NOT NULL,`
- **Liña 13** — `role VARCHAR NOT NULL CHECK (role IN ('USER', 'ADMIN')),`
- **Liña 14** — `status VARCHAR NOT NULL DEFAULT 'ACTIVE'`
- **Liña 15** — `CHECK (status IN ('PENDING_VERIFICATION', 'ACTIVE', 'INACTIVE', 'SUSPENDED')),`
- **Liña 17** — Komentáriu: verifikasaun Gmail (feature rejistu).
- **Liña 18** — `email_verified BOOLEAN NOT NULL DEFAULT FALSE,`
- **Liña 19** — `verification_code_hash VARCHAR,` — komentáriu: hash de'it, nunka kódigu.
- **Liña 20** — `verification_code_expires_at TIMESTAMPTZ,`
- **Liña 21** — `verification_attempts INTEGER NOT NULL DEFAULT 0,`
- **Liña 22** — `last_verification_sent_at TIMESTAMPTZ,`
- **Liña 24** — Komentáriu: soft delete (feature CRUD admin).
- **Liña 25** — `is_deleted BOOLEAN NOT NULL DEFAULT FALSE,`
- **Liña 26** — `deleted_at TIMESTAMPTZ,`
- **Liña 28** — `created_at TIMESTAMPTZ NOT NULL DEFAULT now(),`
- **Liña 29** — `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- **Liña 30** — `);`

**5. Walkthrough ezekusaun**

`psql -f storage/schema.sql` → harii tabela se seidauk iha. `INSERT INTO users (..., role) VALUES (..., 'OWNER')` → erru CHECK.

**6. Tipu dadus**

`VARCHAR`, `BOOLEAN`, `INTEGER`, `TIMESTAMPTZ`.

**7. Ligasaun OOP**

Not implemented in the current codebase (SQL). Ekivalente ba klase `User` (D.2.3).

**8. Impaktu integrasaun**

Dokumentasaun/setup manuál; aplikasaun uza ORM.

**9. Checkpoint ba principiante**

**P:** Saida mak `DEFAULT now()`?
**R:** Se `INSERT` la fó valór, PostgreSQL tau tempu agora.

#### D.4.2 Tabela `images` no índise

**1. Kódigu orijinál**

{{code storage/schema.sql 32-68}}

**2. Objetivu**

Tabela `images` ho FK ba `users`, tempu, metadata EXIF, status ho `CHECK`, no índise 5.

**3. Fundamentu Python**

*(SQL)* `REFERENCES users(user_id)` = chave estranjeira. `CREATE INDEX IF NOT EXISTS ix ON t(c)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 32** — `CREATE TABLE IF NOT EXISTS images (`
- **Liña 33** — `image_id VARCHAR PRIMARY KEY,`
- **Liña 34** — `location VARCHAR,` — fatin testu livre.
- **Liña 35** — `user_id VARCHAR NOT NULL REFERENCES users(user_id),`
- **Liña 36** — `original_filename VARCHAR,`
- **Liña 37** — `image_path VARCHAR NOT NULL,`
- **Liña 38** — `source VARCHAR NOT NULL DEFAULT 'android_upload',`
- **Liña 39** — `content_hash VARCHAR,` — SHA-256 ba deteta duplikadu.
- **Liña 41** — `captured_at TIMESTAMPTZ,` — EXIF ka NULL.
- **Liña 42** — `captured_at_source VARCHAR,` — `'exif'` | `'client_trusted'` | NULL.
- **Liña 43** — `uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),` — `createdTime` Drive.
- **Liña 44** — `ingested_at TIMESTAMPTZ,`
- **Liña 45** — `processed_at TIMESTAMPTZ,`
- **Liña 46** — `result_published_at TIMESTAMPTZ,`
- **Liña 48** — Komentáriu: metadata husi EXIF orijinál (NULL se la iha).
- **Liña 49** — `gps_latitude DOUBLE PRECISION,`
- **Liña 50** — `gps_longitude DOUBLE PRECISION,`
- **Liña 51** — `gps_altitude_m DOUBLE PRECISION,`
- **Liña 52** — `gps_source VARCHAR,` — EXIF ka telemóvel.
- **Liña 53** — `camera_make VARCHAR,`
- **Liña 54** — `camera_model VARCHAR,`
- **Liña 55** — `image_width INTEGER,`
- **Liña 56** — `image_height INTEGER,`
- **Liña 58** — `status VARCHAR NOT NULL DEFAULT 'INGESTED'`
- **Liña 59** — `CHECK (status IN ('INGESTED','PROCESSING','COMPLETED','FAILED')),`
- **Liña 60** — `error_message TEXT,`
- **Liña 61** — `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- **Liña 62** — `);`
- **Liña 64** — `CREATE INDEX IF NOT EXISTS ix_images_location ON images(location);`
- **Liña 65** — `CREATE INDEX IF NOT EXISTS ix_images_user_id ON images(user_id);`
- **Liña 66** — `CREATE INDEX IF NOT EXISTS ix_images_captured_at ON images(captured_at);`
- **Liña 67** — `CREATE INDEX IF NOT EXISTS ix_images_uploaded_at ON images(uploaded_at);`
- **Liña 68** — `CREATE INDEX IF NOT EXISTS ix_images_content_hash ON images(content_hash);`

**5. Walkthrough ezekusaun**

`INSERT INTO images (image_id, user_id, image_path) VALUES ('a1', 'la-iha', 'x')` → erru FK (utilizadór la iha).

**6. Tipu dadus**

Haree D.2.4.

**7. Ligasaun OOP**

Ekivalente ba klase `Image`.

**8. Impaktu integrasaun**

Hanesan D.2.4.

**9. Checkpoint ba principiante**

**P:** Saida mak FK `REFERENCES users(user_id)` garante?
**R:** Imajen ida labele iha `user_id` ne'ebé la iha iha `users` (integridade referensiál).

#### D.4.3 `predictions`, `processing_events`, `rejected_events`

**1. Kódigu orijinál**

{{code storage/schema.sql 70-121}}

**2. Objetivu**

DDL tabela operasional tolu tan ho FK, `CHECK` no índise.

**3. Fundamentu Python**

*(SQL)* Hanesan D.4.1–D.4.2.

**4. Esplikasaun liña-ba-liña**

- **Liña 70** — `CREATE TABLE IF NOT EXISTS predictions (`
- **Liña 71** — `prediction_id VARCHAR PRIMARY KEY,`
- **Liña 72** — `image_id VARCHAR NOT NULL REFERENCES images(image_id),`
- **Liña 74** — `predicted_class VARCHAR,`
- **Liña 75** — `prediction_confidence DOUBLE PRECISION,`
- **Liña 77** — `lesion_score DOUBLE PRECISION,`
- **Liña 78** — `discoloration_pct DOUBLE PRECISION,`
- **Liña 79** — `texture_irregularity DOUBLE PRECISION,`
- **Liña 80** — `leaf_area_affected_pct DOUBLE PRECISION,`
- **Liña 82** — `risk_score DOUBLE PRECISION,`
- **Liña 83** — `risk_level VARCHAR CHECK (risk_level IN ('LOW','MEDIUM','HIGH')),`
- **Liña 84** — `is_uncertain BOOLEAN NOT NULL DEFAULT FALSE,`
- **Liña 86** — `model_version VARCHAR,`
- **Liña 87** — `predicted_at TIMESTAMPTZ,`
- **Liña 88** — `processing_latency_ms INTEGER,`
- **Liña 89** — `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- **Liña 90** — `);`
- **Liña 92** — `CREATE INDEX ... ix_predictions_image_id ON predictions(image_id);`
- **Liña 93** — `... ix_predictions_predicted_class ...`
- **Liña 94** — `... ix_predictions_risk_level ...`
- **Liña 95** — `... ix_predictions_predicted_at ...`
- **Liña 97** — `CREATE TABLE IF NOT EXISTS processing_events (`
- **Liña 98** — `event_id VARCHAR PRIMARY KEY,`
- **Liña 99** — `image_id VARCHAR NOT NULL REFERENCES images(image_id),`
- **Liña 100** — `api_published_at TIMESTAMPTZ,` — komentáriu: bainhira eventu publika ba Kafka (`ingested_at` imajen).
- **Liña 101** — `kafka_received_at TIMESTAMPTZ,` — komentáriu: bainhira sink `foreachBatch` Spark simu duni.
- **Liña 102** — `spark_started_at TIMESTAMPTZ,`
- **Liña 103** — `spark_completed_at TIMESTAMPTZ,`
- **Liña 104** — `prediction_started_at TIMESTAMPTZ,`
- **Liña 105** — `prediction_completed_at TIMESTAMPTZ,`
- **Liña 106** — `total_latency_ms INTEGER,`
- **Liña 107** — `status VARCHAR NOT NULL DEFAULT 'INGESTED'`
- **Liña 108** — `CHECK (status IN ('INGESTED','PROCESSING','COMPLETED','FAILED')),`
- **Liña 109** — `error_message TEXT,`
- **Liña 110** — `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- **Liña 111** — `);`
- **Liña 113–114** — Komentáriu: eventu aat/la válidu rejeita molok ka durante Spark, rai ba rastreabilidade duke `print()`.
- **Liña 115** — `CREATE TABLE IF NOT EXISTS rejected_events (`
- **Liña 116** — `rejected_event_id VARCHAR PRIMARY KEY,`
- **Liña 117** — `image_id VARCHAR,` — bele la hanesan imajen ruma.
- **Liña 118** — `raw_event TEXT,`
- **Liña 119** — `reason TEXT NOT NULL,`
- **Liña 120** — `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- **Liña 121** — `);`

**5. Walkthrough ezekusaun**

`INSERT INTO predictions (..., risk_level) VALUES (..., 'CRITICAL')` → erru CHECK.

**6. Tipu dadus**

Haree D.2.5–D.2.6.

**7. Ligasaun OOP**

Ekivalente ba `Prediction`, `ProcessingEvent`, `RejectedEvent`.

**8. Impaktu integrasaun**

Hanesan D.2.5–D.2.6.

**9. Checkpoint ba principiante**

**P:** Tanba sá `predictions.risk_level` permite NULL?
**R:** Liña `FAILED` (ka tuan) bele la iha risku; `CHECK` só aplika bainhira valór la NULL.

#### D.4.4 `scale_test_runs` no `scale_test_items`

**1. Kódigu orijinál**

{{code storage/schema.sql 123-172}}

**2. Objetivu**

DDL tabela teste eskalabilidade (la iha FK ba `images`, atu moris liu factory reset ka limpeza run).

**3. Fundamentu Python**

*(SQL)* Índise koluna rua: `ON scale_test_items (run_id, status)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 123–126** — Komentáriu: run teste (`scalability/run.py`); fotografia teste iha liña `images` ho `source = 'scalability_test'`; tabela rua ne'e la iha FK ba `images` atu moris liu factory reset ka limpeza.
- **Liña 127** — `CREATE TABLE IF NOT EXISTS scale_test_runs (`
- **Liña 128** — `run_id VARCHAR PRIMARY KEY,`
- **Liña 129** — `status VARCHAR NOT NULL,`
- **Liña 130** — `phase VARCHAR,`
- **Liña 131** — `pid INTEGER,`
- **Liña 132** — `requested_size INTEGER NOT NULL,`
- **Liña 133** — `replay_of VARCHAR,`
- **Liña 134** — `publish_results BOOLEAN NOT NULL DEFAULT FALSE,`
- **Liña 135** — `drive_results_folder_id VARCHAR,`
- **Liña 136** — `config_json TEXT,`
- **Liña 137** — `selection_json TEXT,`
- **Liña 138** — `summary_json TEXT,`
- **Liña 139** — `error_message TEXT,`
- **Liña 140** — `data_cleaned_at TIMESTAMPTZ,`
- **Liña 141** — `started_at TIMESTAMPTZ NOT NULL DEFAULT now(),`
- **Liña 142** — `finished_at TIMESTAMPTZ`
- **Liña 143** — `);`
- **Liña 145** — `CREATE TABLE IF NOT EXISTS scale_test_items (`
- **Liña 146** — `image_id VARCHAR PRIMARY KEY,`
- **Liña 147** — `run_id VARCHAR NOT NULL,`
- **Liña 148** — `source_image_id VARCHAR NOT NULL,`
- **Liña 149** — `source_drive_file_id VARCHAR NOT NULL,`
- **Liña 150** — `source_user_id VARCHAR,`
- **Liña 151** — `local_copy_status VARCHAR,`
- **Liña 152** — `download_seconds DOUBLE PRECISION,`
- **Liña 153** — `size_bytes INTEGER,`
- **Liña 154** — `status VARCHAR NOT NULL,`
- **Liña 155** — `error_message TEXT,`
- **Liña 156** — `published_at TIMESTAMPTZ,`
- **Liña 157** — `spark_received_at TIMESTAMPTZ,`
- **Liña 158** — `completed_at TIMESTAMPTZ,`
- **Liña 159** — `timings_json TEXT,`
- **Liña 160** — `pipeline_seconds DOUBLE PRECISION,`
- **Liña 161** — `db_write_seconds DOUBLE PRECISION,`
- **Liña 162** — `drive_publish_seconds DOUBLE PRECISION,`
- **Liña 163** — `predicted_class VARCHAR,`
- **Liña 164** — `prediction_confidence DOUBLE PRECISION,`
- **Liña 165** — `is_uncertain BOOLEAN,`
- **Liña 166** — `risk_level VARCHAR,`
- **Liña 167** — `location VARCHAR,`
- **Liña 168** — `gps_latitude DOUBLE PRECISION,`
- **Liña 169** — `gps_longitude DOUBLE PRECISION`
- **Liña 170** — `);`
- **Liña 171** — `CREATE INDEX IF NOT EXISTS ix_scale_test_items_run_id ON scale_test_items (run_id);`
- **Liña 172** — `CREATE INDEX IF NOT EXISTS ix_scale_test_items_run_status ON scale_test_items (run_id, status);`

**5. Walkthrough ezekusaun**

Factory reset hamoos `images` maibé `scale_test_runs`/`items` hela (la iha FK atu bloke ka cascade).

**6. Tipu dadus**

Haree D.2.7.

**7. Ligasaun OOP**

Ekivalente ba `ScaleTestRun`, `ScaleTestItem`.

**8. Impaktu integrasaun**

Hanesan D.2.7.

**9. Checkpoint ba principiante**

**P:** Default `status` iha SQL no ORM hanesan?
**R:** Lae kompletu: SQL `status VARCHAR NOT NULL` la iha DEFAULT; ORM tau `"PREPARING"`/`"STAGED"` iha Python. Rezultadu hanesan bainhira aplikasaun hakerek.

#### D.4.5 Tabela analítika agrikultór

**1. Kódigu orijinál**

{{code storage/schema.sql 174-243}}

**2. Objetivu**

DDL tabela analítika 7 ne'ebé Spark hakerek: regra (imajen teste la tama, imajen ida konta dala ida ho prediksaun foun liu, fatin mamuk = "Unknown location", loron Timor-Leste).

**3. Fundamentu Python**

*(SQL)* Chave primária kompostu: `PRIMARY KEY (location, day, predicted_class)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 174–179** — Komentáriu: analítika Spark (`batch_analytics.py` — metade batch objetivu 7; streaming mak `streaming_job.py`), dashboard lee (`analytics_reader.py`); Spark troka liña hotu kada run; regra teste/latest/Unknown location/loron Timor.
- **Liña 180** — `CREATE TABLE IF NOT EXISTS analytics_summary (`
- **Liña 181** — `summary_id INTEGER PRIMARY KEY,` — sempre 1.
- **Liña 182** — `total_users BIGINT NOT NULL,` — konta la hamoos.
- **Liña 183** — `total_images BIGINT NOT NULL,`
- **Liña 184** — `total_healthy BIGINT NOT NULL,`
- **Liña 185** — `total_diseased BIGINT NOT NULL,` — klasifika, la healthy, la uncertain.
- **Liña 186** — `total_uncertain BIGINT NOT NULL,`
- **Liña 187** — `total_unclassified BIGINT NOT NULL,` — seidauk prediksaun.
- **Liña 188** — `total_high_risk BIGINT NOT NULL,`
- **Liña 189** — `total_processed BIGINT NOT NULL,`
- **Liña 190** — `total_failed BIGINT NOT NULL,`
- **Liña 191** — `submissions_24h BIGINT NOT NULL,`
- **Liña 192** — `active_farmers_24h BIGINT NOT NULL,`
- **Liña 193** — `most_detected_disease VARCHAR,`
- **Liña 194** — `avg_processing_latency_ms DOUBLE PRECISION,` — ingested → predicted.
- **Liña 195** — `avg_capture_to_upload_seconds DOUBLE PRECISION,`
- **Liña 196** — `avg_upload_to_ingestion_seconds DOUBLE PRECISION,`
- **Liña 197** — `avg_end_to_end_latency_seconds DOUBLE PRECISION,` — uploaded → predicted.
- **Liña 198** — `generated_at TIMESTAMPTZ NOT NULL`
- **Liña 199** — `);`
- **Liña 200** — `CREATE TABLE IF NOT EXISTS analytics_disease_distribution (`
- **Liña 201** — `predicted_class VARCHAR PRIMARY KEY,`
- **Liña 202** — `photos BIGINT NOT NULL,`
- **Liña 203** — `generated_at TIMESTAMPTZ NOT NULL`
- **Liña 204** — `);`
- **Liña 205** — `CREATE TABLE IF NOT EXISTS analytics_risk_distribution (`
- **Liña 206** — `risk_level VARCHAR PRIMARY KEY,`
- **Liña 207** — `photos BIGINT NOT NULL,`
- **Liña 208** — `generated_at TIMESTAMPTZ NOT NULL`
- **Liña 209** — `);`
- **Liña 210** — `CREATE TABLE IF NOT EXISTS analytics_high_risk_by_location (`
- **Liña 211** — `location VARCHAR PRIMARY KEY,`
- **Liña 212** — `high BIGINT NOT NULL,`
- **Liña 213** — `total BIGINT NOT NULL,`
- **Liña 214** — `high_pct DOUBLE PRECISION NOT NULL,`
- **Liña 215** — `generated_at TIMESTAMPTZ NOT NULL`
- **Liña 216** — `);`
- **Liña 217** — `CREATE TABLE IF NOT EXISTS analytics_disease_trend (`
- **Liña 218** — `location VARCHAR NOT NULL,`
- **Liña 219** — `day DATE NOT NULL,`
- **Liña 220** — `predicted_class VARCHAR NOT NULL,`
- **Liña 221** — `photos BIGINT NOT NULL,`
- **Liña 222** — `generated_at TIMESTAMPTZ NOT NULL,`
- **Liña 223** — `PRIMARY KEY (location, day, predicted_class)`
- **Liña 224** — `);`
- **Liña 225** — `CREATE TABLE IF NOT EXISTS analytics_disease_daily (`
- **Liña 226** — `day DATE NOT NULL,`
- **Liña 227** — `predicted_class VARCHAR NOT NULL,`
- **Liña 228** — `photos BIGINT NOT NULL,`
- **Liña 229** — `generated_at TIMESTAMPTZ NOT NULL,`
- **Liña 230** — `PRIMARY KEY (day, predicted_class)`
- **Liña 231** — `);`
- **Liña 232** — Komentáriu: liña ida kada munisípiu kada loron upload, ba relatóriu/BI.
- **Liña 233** — `CREATE TABLE IF NOT EXISTS batch_location_daily_summary (`
- **Liña 234** — `location VARCHAR NOT NULL,`
- **Liña 235** — `summary_date DATE NOT NULL,`
- **Liña 236** — `total_images BIGINT NOT NULL,`
- **Liña 237** — `total_healthy BIGINT NOT NULL,`
- **Liña 238** — `total_diseased BIGINT NOT NULL,`
- **Liña 239** — `avg_risk_score DOUBLE PRECISION,`
- **Liña 240** — `high_risk_count BIGINT NOT NULL,`
- **Liña 241** — `generated_at TIMESTAMPTZ NOT NULL,`
- **Liña 242** — `PRIMARY KEY (location, summary_date)`
- **Liña 243** — `);`

**5. Walkthrough ezekusaun**

Spark `write_tables` harii `analytics_disease_trend_staging (LIKE analytics_disease_trend)` → JDBC append → `DELETE` + `INSERT ... SELECT` (Modulu 01 D.4.7).

**6. Tipu dadus**

Haree D.2.9–D.2.10.

**7. Ligasaun OOP**

Ekivalente ba klase analítika ho mixin.

**8. Impaktu integrasaun**

Spark hakerek, dashboard/Tableau lee.

**9. Checkpoint ba principiante**

**P:** Tanba sá kontajen uza `BIGINT`?
**R:** Spark `count()` fó `long` (64-bit); `BIGINT` hanesan tipu ne'e no la iha risku overflow.

#### D.4.6 Analítika teste, `image_reviews`, `storage_audit_log`

**1. Kódigu orijinál**

{{code storage/schema.sql 245-298}}

**2. Objetivu**

DDL tabela analítika teste 3 (kada run), revizaun admin, no auditoria armazenamentu.

**3. Fundamentu Python**

*(SQL)* Hanesan blokus anteriór.

**4. Esplikasaun liña-ba-liña**

- **Liña 245–247** — Komentáriu: figura moras pájina teste, kada run, husi `scale_test_items` (COMPLETED de'it); separadu husi analítika agrikultór; la partilla ho Tableau.
- **Liña 248** — `CREATE TABLE IF NOT EXISTS analytics_scale_test_disease_by_location (`
- **Liña 249** — `run_id VARCHAR NOT NULL,`
- **Liña 250** — `location VARCHAR NOT NULL,`
- **Liña 251** — `predicted_class VARCHAR NOT NULL,`
- **Liña 252** — `photos BIGINT NOT NULL,`
- **Liña 253** — `generated_at TIMESTAMPTZ NOT NULL,`
- **Liña 254** — `PRIMARY KEY (run_id, location, predicted_class)`
- **Liña 255** — `);`
- **Liña 256** — `CREATE TABLE IF NOT EXISTS analytics_scale_test_disease_distribution (`
- **Liña 257** — `run_id VARCHAR NOT NULL,`
- **Liña 258** — `predicted_class VARCHAR NOT NULL,`
- **Liña 259** — `photos BIGINT NOT NULL,`
- **Liña 260** — `generated_at TIMESTAMPTZ NOT NULL,`
- **Liña 261** — `PRIMARY KEY (run_id, predicted_class)`
- **Liña 262** — `);`
- **Liña 263** — `CREATE TABLE IF NOT EXISTS analytics_scale_test_high_risk_by_location (`
- **Liña 264** — `run_id VARCHAR NOT NULL,`
- **Liña 265** — `location VARCHAR NOT NULL,`
- **Liña 266** — `high BIGINT NOT NULL,`
- **Liña 267** — `total BIGINT NOT NULL,`
- **Liña 268** — `high_pct DOUBLE PRECISION NOT NULL,`
- **Liña 269** — `generated_at TIMESTAMPTZ NOT NULL,`
- **Liña 270** — `PRIMARY KEY (run_id, location)`
- **Liña 271** — `);`
- **Liña 273–275** — Komentáriu: desizaun revizaun admin kada imajen (Label photos): label KONFIRMA ka "discarded" — nunka prediksaun modelu; la iha FK ba `images`, entaun moris liu factory reset hanesan fotografia treinu.
- **Liña 276** — `CREATE TABLE IF NOT EXISTS image_reviews (`
- **Liña 277** — `image_id VARCHAR PRIMARY KEY,`
- **Liña 278** — `decision VARCHAR NOT NULL,` — `labelled` | `discarded`.
- **Liña 279** — `confirmed_class VARCHAR,`
- **Liña 280** — `predicted_class_at_review VARCHAR,`
- **Liña 281** — `reviewed_by VARCHAR,`
- **Liña 282** — `reviewed_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- **Liña 283** — `);`
- **Liña 285–286** — Komentáriu: asaun armazenamentu destrutivu husi dashboard (limpeza cache, repara cache, factory reset); factory reset nunka hamoos.
- **Liña 287** — `CREATE TABLE IF NOT EXISTS storage_audit_log (`
- **Liña 288** — `audit_id VARCHAR PRIMARY KEY,`
- **Liña 289** — `action VARCHAR NOT NULL,`
- **Liña 290** — `category VARCHAR,`
- **Liña 291** — `target VARCHAR,`
- **Liña 292** — `reason TEXT,`
- **Liña 293** — `outcome VARCHAR NOT NULL,` — `done` | `skipped` | `failed` (ORM hatete mós `dry_run`).
- **Liña 294** — `detail TEXT,`
- **Liña 295** — `performed_by VARCHAR,`
- **Liña 296** — `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- **Liña 297** — `);`
- **Liña 298** — `CREATE INDEX IF NOT EXISTS ix_storage_audit_log_created_at ON storage_audit_log (created_at);`

**5. Walkthrough ezekusaun**

Factory reset → `storage_audit_log` hetan liña `action='factory_reset'`; tabela ne'e rasik la hamoos.

**6. Tipu dadus**

Haree D.2.8, D.2.11.

**7. Ligasaun OOP**

Ekivalente ba klase korespondente.

**8. Impaktu integrasaun**

Hanesan D.2.8, D.2.11.

**9. Checkpoint ba principiante**

**P:** Tanba sá auditoria importante ba asaun destrutivu?
**R:** Atu hatene sé, bainhira no tanba sá hamoos arkivu — rastreabilidade no responsabilidade.

#### D.4.7 View rua

**1. Kódigu orijinál**

{{code storage/schema.sql 300-363}}

**2. Objetivu**

Forma PostgreSQL view rua ne'ebé `init_db()` harii: `v_tableau_images` (tempu +9 h) no `v_admin_image_catalog` (tempu UTC). Rua uza prediksaun foun liu no la inklui imajen teste.

**3. Fundamentu Python**

*(SQL)* `CREATE OR REPLACE VIEW`; CTE `WITH latest AS (...)`; `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`; `AT TIME ZONE 'UTC'` + `INTERVAL '9 hours'`.

**4. Esplikasaun liña-ba-liña**

- **Liña 300–304** — Komentáriu: view; `database.py` harii fali rua kada `init_db()`; SQL iha okos mak forma PostgreSQL; rua foti prediksaun FOUN LIU (`predicted_at` foun liu, empate → `prediction_id`) no la inklui imajen teste.
- **Liña 306–307** — Komentáriu: Tableau Desktop lee (login read-only `tableau_reader`); tempu husi UTC ba Timor-Leste (UTC+9).
- **Liña 308** — `CREATE OR REPLACE VIEW v_tableau_images AS`
- **Liña 309** — `WITH latest AS (`
- **Liña 310** — `SELECT p.*,`
- **Liña 311** — `ROW_NUMBER() OVER (`
- **Liña 312** — `PARTITION BY p.image_id`
- **Liña 313** — `ORDER BY p.predicted_at DESC, p.prediction_id DESC` — **la iha** `NULLS LAST` (diferente husi `database.py` liña 540).
- **Liña 314** — `) AS rn`
- **Liña 315** — `FROM predictions p`
- **Liña 316** — `)`
- **Liña 317** — `SELECT`
- **Liña 318** — `i.image_id, i.location, i.source, i.status,`
- **Liña 319** — `u.username AS farmer_username,`
- **Liña 320** — `captured_at` + 9 h.
- **Liña 321** — `uploaded_at` + 9 h.
- **Liña 322** — `ingested_at` + 9 h.
- **Liña 323** — `processed_at` + 9 h.
- **Liña 324** — GPS no kámara.
- **Liña 325** — `predicted_class, prediction_confidence, is_uncertain`.
- **Liña 326** — `lesion_score, discoloration_pct, texture_irregularity`.
- **Liña 327** — `leaf_area_affected_pct, risk_score, risk_level`.
- **Liña 328** — `model_version`.
- **Liña 329** — `predicted_at` + 9 h.
- **Liña 330** — `processing_latency_ms`.
- **Liña 331** — `FROM images i`
- **Liña 332** — `LEFT JOIN latest l ON l.image_id = i.image_id AND l.rn = 1`
- **Liña 333** — `LEFT JOIN users u ON u.user_id = i.user_id`
- **Liña 334** — `WHERE i.source <> 'scalability_test';`
- **Liña 336–337** — Komentáriu: katálogu Image management dashboard (`storage/image_catalog.py`): imajen + agrikultór + prediksaun foun liu + revizaun; tempu UTC.
- **Liña 338** — `CREATE OR REPLACE VIEW v_admin_image_catalog AS`
- **Liña 339** — `WITH latest AS (`
- **Liña 340** — `SELECT p.*,`
- **Liña 341** — `ROW_NUMBER() OVER (`
- **Liña 342** — `PARTITION BY p.image_id`
- **Liña 343** — `ORDER BY p.predicted_at DESC, p.prediction_id DESC`
- **Liña 344** — `) AS rn`
- **Liña 345** — `FROM predictions p`
- **Liña 346** — `)`
- **Liña 347** — `SELECT`
- **Liña 348** — koluna imajen 6.
- **Liña 349** — `content_hash`, `status` (CAST), `error_message`.
- **Liña 350** — tempu 5.
- **Liña 351** — GPS/kámara 5.
- **Liña 352** — `image_width, image_height`.
- **Liña 353** — `farmer_email, farmer_name`.
- **Liña 354** — `farmer_status`.
- **Liña 355** — `farmer_deleted`.
- **Liña 356** — prediksaun 4.
- **Liña 357** — `risk_score`, `risk_level` (CAST), `model_version`, `predicted_at`.
- **Liña 358** — revizaun 4.
- **Liña 359** — `FROM images i`
- **Liña 360** — `LEFT JOIN latest l ...`
- **Liña 361** — `LEFT JOIN users u ...`
- **Liña 362** — `LEFT JOIN image_reviews r ON r.image_id = i.image_id`
- **Liña 363** — `WHERE i.source <> 'scalability_test';`

**5. Walkthrough ezekusaun**

Haree D.2.15–D.2.16.

**6. Tipu dadus**

View: koluna 26 (Tableau) no 37 (admin), konta husi lista SELECT.

**7. Ligasaun OOP**

Not implemented in the current codebase (SQL).

**8. Impaktu integrasaun**

Diferensa ki'ik ho `database.py`: `schema.sql` la iha `NULLS LAST`. Aplikasaun uza versaun `database.py` (ho `NULLS LAST`), tanba `init_db()` harii fali view kada vez.

**9. Checkpoint ba principiante**

**P:** Se ita halai `schema.sql` depois `init_db()`, view ida ne'ebé mak hela?
**R:** `CREATE OR REPLACE` husi `schema.sql` sei troka view; maibé `init_db()` tuirmai (ezemplu bainhira Spark hahú) sei harii fali versaun `database.py`.

## E. Analize OOP

### E.1 Klase iha modulu base de dadus

| Klase | Herda | Tipu | Uza ba |
|---|---|---|---|
| `Role`, `UserStatus`, `RiskLevel`, `ProcessingStatus` | `str`, `enum.Enum` | enum | valór fixu |
| `User`, `Image`, `Prediction`, `ProcessingEvent`, `RejectedEvent`, `ScaleTestRun`, `ScaleTestItem`, `ImageReview`, `StorageAuditLog` | `Base` | modelu ORM | tabela operasional |
| `_SparkOutput` | `object` | mixin | koluna `generated_at` |
| `AnalyticsSummary` + analítika 9 seluk | `_SparkOutput`, `Base` | modelu ORM | tabela Spark |

### E.2 Diagrama klase (ORM)

```mermaid
classDiagram
    class Base
    class SparkOutput["_SparkOutput"]
    SparkOutput : generated_at
    class User {
        user_id
        username
        email
        role
        status
        images
    }
    class Image {
        image_id
        user_id
        image_path
        status
        user
        predictions
        processing_events
    }
    class Prediction {
        prediction_id
        image_id
        predicted_class
        risk_level
        image
    }
    class ProcessingEvent {
        event_id
        image_id
        total_latency_ms
        image
    }
    class AnalyticsSummary {
        summary_id
        total_images
    }
    Base <|-- User
    Base <|-- Image
    Base <|-- Prediction
    Base <|-- ProcessingEvent
    Base <|-- AnalyticsSummary
    SparkOutput <|-- AnalyticsSummary
    User "1" o-- "0..*" Image : images / user
    Image "1" *-- "0..*" Prediction : cascade
    Image "1" *-- "0..*" ProcessingEvent : cascade
```

### E.3 Prinsípiu OOP — evidénsia

| Konseitu | Iha? | Evidénsia |
|---|---|---|
| Klase no objetu | Sin | Klase modelu 19; objetu = liña (`User(...)`, `Image(...)`) |
| `__init__` | Implísitu | SQLAlchemy fó konstrutór automátiku (argumentu ho naran koluna); kódigu projetu la hakerek `__init__` iha modelu |
| `self` | Implísitu | La iha métodu instánsia iha modelu |
| Atributu klase | Sin | `__tablename__`, `__table_args__`, `Column(...)` hotu; membru enum |
| Atributu instánsia | Sin (ORM) | `user.email`, `image.status` — SQLAlchemy halo `Column` sai atributu instánsia |
| Herança | Sin | Modelu hotu → `Base`; enum → `str` + `Enum` |
| **Herança múltipla / mixin** | Sin | `AnalyticsSummary(_SparkOutput, Base)` no analítika 9 seluk |
| Polimorfizmu | Parsiál | Membru enum hahalok hanesan `str` (`ProcessingStatus.COMPLETED == "COMPLETED"`); `Session.get(Klase, id)` funsiona ba klase hotu |
| Enkapsulamentu | Parsiál | SQL subar atrás ORM; funsaun interior `_add_missing_columns`, `_ensure_tableau_reader` (konvensaun `_`) |
| Abstrasaun | Sin | ORM abstrai SQL; `DATABASE_URL` abstrai PostgreSQL/SQLite |
| Komposisaun | Sin (ORM) | `Image` na'in `Prediction`/`ProcessingEvent` (`cascade="all, delete-orphan"`) |
| Agregasaun | Sin | `User` ↔ `Image` (`back_populates`, la iha cascade) |
| Factory | Sin | `sessionmaker` → `SessionLocal()`; `declarative_base()` |
| Observer | Sin | `@event.listens_for(engine, "connect")` (SQLite WAL) |
| Unit of Work / Identity Map | Uza husi biblioteka | `Session` rai mudansa no hakerek iha `commit()` |
| Repository pattern | Not implemented in the current codebase. | Kódigu uza `SessionLocal()` direta iha modulu barak |
| Migrasaun formál (Alembic) | Not implemented in the current codebase. | `_add_missing_columns` mak migrasaun simples |

**Nota aprendizajen Python — `__init__` iha modelu ORM (Teoria):** SQLAlchemy `declarative_base` fó `__init__(self, **kwargs)` ne'ebé tau kada argumentu ho naran koluna hanesan atributu. Tanba ne'e `User(username="x", email="y")` funsiona maske klase `User` la define `__init__`.

## F. Integrasaun

| Komponente | Tabela | Operasaun |
|---|---|---|
| Spark streaming (Modulu 01) | `users`, `images`, `predictions`, `processing_events`, `rejected_events` | INSERT/UPDATE liuhusi ORM |
| Spark batch (Modulu 01) | `images`, `predictions`, `users`, `scale_test_items` → `analytics_*` | JDBC read; JDBC append ba `_staging` + transasaun |
| Kafka produtór `submit` (Modulu 02) | `images` | UPDATE `INGESTED`/`FAILED` |
| Drive (Modulu 03) | `users` (`get_or_create_farmer`), `images.result_published_at` | INSERT/UPDATE |
| Dashboard (Modulu 06) | hotu + view admin | SELECT; INSERT `image_reviews`, `storage_audit_log`; reset |
| Tableau | `v_tableau_images`, `images`, `predictions`, `analytics_*` | SELECT (read-only) |
| `init_db` | esquema | DDL |

## G. Tratamentu Erru no Konfiabilidade

| Mekanizmu | Detallu |
|---|---|
| Integridade | `PRIMARY KEY`, `UNIQUE` (`username`, `email`), FK 3, `NOT NULL`, ENUM/`CHECK` |
| Idempotênsia esquema | `create_all`, `IF NOT EXISTS`, `_add_missing_columns` kontrola uluk, `DROP VIEW IF EXISTS` |
| Konesaun mate | `pool_pre_ping=True` (PostgreSQL) |
| Konkorrénsia SQLite | WAL; `check_same_thread=False` |
| Race agrikultór foun | `UNIQUE email` + `IntegrityError` → rollback (Modulu 03 D.6.3) |
| Replay prediksaun | delete-then-insert iha `persist_result` |
| Analítika la mamuk | staging + transasaun (Modulu 01 D.4.7) |
| Privasidade | `tableau_reader` la iha SELECT ba `users`; Spark lee koluna presiza de'it |
| Persisténsia | Volume Docker `crop_disease_pg_data` |
| Seguransa rede | Port `127.0.0.1:5432` de'it |

**Limitasaun:**

- `schema.sql` la hanesan 100% ho ORM: enum (`VARCHAR + CHECK` vs ENUM native), view la iha `NULLS LAST`, default `status` teste. Inferidu: base de dadus ne'ebé harii ho ORM iha tipu ENUM PostgreSQL (ezemplu `processingstatus`); se ENUM tuan iha valór antigu, nia hela (la kontrola iha sesaun ne'e tanba Docker la halai).
- Migrasaun la hamoos koluna tuan (só aumenta), exetu migrasaun espesífiku (`field_id`).
- FK la iha `ON DELETE CASCADE` iha nível base de dadus; cascade iha ORM de'it.
- Backup automátiku: Not implemented in the current codebase.
- Particionamentu / replikasaun PostgreSQL: Not implemented in the current codebase.

## H. Sumáriu no Pergunta Akadémiku

### H.1 Sumáriu

Base de dadus PostgreSQL 16 (Docker, localhost) ho tabela 19 no view 2. `storage/database.py` define enum 4 no modelu ORM ho SQLAlchemy (klase = tabela, objetu = liña), relasaun (`User`–`Image` agregasaun; `Image`–`Prediction`/`ProcessingEvent` komposisaun ho cascade), mixin `_SparkOutput` ba tabela analítika, engine ho `pool_pre_ping`, `SessionLocal`, migrasaun simples (`_add_missing_columns`), CTE `LATEST_PREDICTION_CTE`, view Tableau (+9 h, read-only, la ho `users`) no view admin. `init_db()` idempotent. `schema.sql` mak DDL ekivalente ba relatóriu. ERD hatudu FK 3 no relasaun lójiku; DFD kontestu no nível 1 hatudu fluxu Drive → Kafka → Spark → BD → dashboard/Tableau.

### H.2 Pergunta no resposta

**P1. FK hira iha esquema no ne'ebé?**
R: Tolu: `images.user_id → users`, `predictions.image_id → images`, `processing_events.image_id → images`. Tabela seluk la iha FK ho intensaun (teste, revizaun, rejeitadu, analítika).

**P2. Oinsá sistema garante "imajen ida konta dala ida"?**
R: `ROW_NUMBER() OVER (PARTITION BY image_id ORDER BY predicted_at DESC NULLS LAST, prediction_id DESC)` no filtra `rn = 1` (view, `analytics_reader`); Spark halo hanesan ho `Window`; streaming mós delete-then-insert.

**P3. Diferensa komposisaun no agregasaun iha ORM ne'e?**
R: `Image.predictions` ho `cascade="all, delete-orphan"` — prediksaun moris-mate ho imajen (komposisaun). `User.images` la iha cascade — imajen la hamoos bainhira utilizadór hamoos (soft delete) (agregasaun).

**P4. Saida mak mixin no iha ne'ebé uza?**
R: Klase ki'ik ne'ebé fó atributu ba klase seluk liuhusi herança múltipla. `_SparkOutput` fó `generated_at` ba tabela analítika 10 (`class AnalyticsSummary(_SparkOutput, Base)`).

**P5. Tanba sá `init_db()` presiza `_add_missing_columns`?**
R: `create_all` la aumenta koluna ba tabela ne'ebé iha ona. Funsaun ne'e kompara ho `inspect` no halai `ALTER TABLE ADD COLUMN`.

**P6. Oinsá Tableau asesu dadus la ho vaza email agrikultór?**
R: Login `tableau_reader` (NOSUPERUSER) ho SELECT de'it ba `v_tableau_images` + `images`, `predictions`, `analytics_*`. View hatudu `farmer_username`, la'ós email/hash; `users` la hetan GRANT.

**P7. Tanba sá tempu iha view Tableau aumenta oras 9?**
R: Tempu rai UTC; `(col AT TIME ZONE 'UTC') + INTERVAL '9 hours'` fó oras Timor-Leste la importa fuso Tableau.

**P8. Saida mak diferensa entre `schema.sql` no ORM?**
R: Enum (`VARCHAR + CHECK` vs ENUM native), view la iha `NULLS LAST`, default `status` teste iha Python. Aplikasaun uza ORM; `schema.sql` ba dokumentasaun/setup manuál.

**P9. DFD nível 1 iha prosesu hira no data store hira?**
R: Prosesu 7 (1.0 deteta upload … 7.0 dashboard) no data store 6 (D1 `users`, D2 `images`, D3 `predictions`, D4 eventu, D5 analítika, D6 revizaun/auditoria).

**P10. Saida mak akontese bainhira PostgreSQL restart durante Spark halai?**
R: `pool_pre_ping` troka konesaun mate. Se `persist_result` falla, `process_and_persist` kaer; fotografia hela `PROCESSING` no `_auto_resend`/`requeue_stuck` haruka fali (Modulu 01).
