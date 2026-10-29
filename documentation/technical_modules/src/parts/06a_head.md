# 06 — Modulu Integrasaun Fim-ba-Fim (End-to-End)

*Sistema Crop Disease Early Warning System (EWS) — dokumentasaun téknika ba fluxu tomak: husi agrikultór foti fotografia iha telemóvel to'o rezultadu fila ba telemóvel, inklui oinsá hahú sistema (`run_all.py`), fila upload Android, dashboard admin (modu MANUAL, Process, Reprocess, Retry), no pontu entrega importante iha Spark. Ba estudante ne'ebé foin hahú programasaun. Fonte úniku: kódigu iha repozitóriu (branch `main`, commit `302818f`).*

{{coverage run_all.py 1-328}}
{{coverage android_app/app/src/main/java/com/cropdisease/app/UploadQueue.kt 1-249}}
{{coverage android_app/app/src/main/java/com/cropdisease/app/UploadWorker.kt 1-207}}
{{coverage dashboard/app.py 213-263 552-560 574-649}}
{{coverage dashboard/image_management_page.py 204-207 226-239}}
{{coverage storage/pipeline_submit.py 26-57}}
{{coverage spark/streaming_job.py 203-299}}

> **Oinsá lee.** Bloku kódigu kada ida tuir pasu 9: (1) Kódigu orijinál, (2) Objetivu, (3) Fundamentu Python (ba Kotlin: fundamentu Kotlin), (4) Esplikasaun liña-ba-liña, (5) Walkthrough ezekusaun, (6) Tipu dadus, (7) Ligasaun OOP, (8) Impaktu integrasaun, (9) Checkpoint ba principiante. Etiketa: *Implementadu*, *Inferidu*, *Teoria*, *Ezemplu ilustrativu*. Modulu 01–05 esplika kada komponente ho detallu; modulu ne'e hatudu oinsá sira liga.

## A. Introdusaun no Objetivu

### A.1 Papél modulu ne'e

Modulu ne'e hatán pergunta "saida mak akontese, tuir orden, bainhira agrikultór haruka fotografia ida?" no "oinsá sistema hahú?". Komponente sira:

| # | Komponente | Fatin | Janela / prosesu |
|---|---|---|---|
| 1 | Aplikasaun Android (fila upload + worker) | `UploadQueue.kt`, `UploadWorker.kt`, `DriveUploader.kt` | telemóvel |
| 2 | Google Drive | pasta `images/<email>`, `results/<email>` | nuvem |
| 3 | Drive watcher | `kafka/drive_watcher.py` | `EWS-DriveWatcher` |
| 4 | Kafka (Docker) | `docker-compose.yml` | container |
| 5 | Monitor Kafka | `kafka/monitor.py` | `EWS-KafkaMonitor` |
| 6 | Spark streaming + CNN | `spark/streaming_job.py`, `ml/` | `EWS-Spark` |
| 7 | Spark analítika | `spark/batch_analytics.py --watch` | `EWS-SparkAnalytics` |
| 8 | PostgreSQL (Docker) | `docker-compose.yml` | container |
| 9 | Dashboard (Streamlit) | `dashboard/app.py` | `EWS-Dashboard` (http://localhost:8501) |
| 10 | Lansadór | `run_all.py` | terminál |

### A.2 Problema ne'ebé modulu ne'e rezolve

- **Komponente barak atu hahú** → `run_all.py` hahú hotu ho orden loloos (Docker → BD → serbisu), kada ida iha janela rasik, rai PID, no hapara hotu ho `--stop`.
- **Upload rihun iha rede la estável** → `UploadQueue` (SQLite iha telemóvel) + `UploadWorker` (WorkManager, foreground, retry, paralelu 3).
- **Admin hakarak kontrola** → modu MANUAL: fotografia hein iha Drive; "Process", "Process all", "Switch to AUTOMATIC".
- **Fotografia falla** → "Reprocess"/"Retry processing" → eventu `submit` foun.

### A.3 Fundamentu Python no Kotlin (foun iha modulu ne'e)

**Lembra** (Modulu 01–05): klase, objetu, `self`, `__init__`, herança, `with`, thread, JSON, enum, Kotlin `object`/`data class`/`sealed class`/null-safety.

**Python**

1. **`subprocess.Popen(...)`** — hahú programa seluk no la hein; **`subprocess.run(...)`** — hahú no hein (fó `returncode`, `stdout`).
2. **`shutil.which("docker")`** — buka programa iha `PATH` (ka `None`).
3. **`socket.connect_ex((host, port))`** — `0` se port loke.
4. **`os.kill(pid, 0)`** — kontrola prosesu moris (Linux); `tasklist`/`taskkill` iha Windows.
5. **Streamlit (Teoria)** — dashboard web husi script Python. Script halai fali tomak kada interasaun. `st.button(...)` fó `True` iha execusaun ne'ebé ema klik; `st.rerun()` halai fali script; `st.session_state` rai valór entre execusaun; `@st.cache_data(ttl=60)` rai rezultadu funsaun 60 s; `@st.fragment(run_every=N)` halai parte ida kada N segundu.

**Kotlin**

| Kotlin | Signifika |
|---|---|
| `class A private constructor(...) : B(...)` | konstrutór privadu (labele harii husi liur) + herança |
| `companion object { ... }` | membru "estátiku" klase (hanesan atributu/métodu klase Python) |
| `@Volatile` + `synchronized(this) { }` | singleton thread-safe (double-checked locking) |
| `override fun onCreate(...)` | troka métodu klase inan |
| `SQLiteOpenHelper` | klase Android ba base de dadus SQLite lokál |
| `db.beginTransaction()/setTransactionSuccessful()/endTransaction()` | transasaun |
| `ContentValues().apply { put(...) }` | "dict" valór ba SQLite; `apply` konfigura objetu |
| `fun Cursor.toItem(): Item` | *extension function* — aumenta métodu ba klase seluk |
| `buildList { add(...) }` | harii lista |
| `Worker` / `WorkManager` / `Result.success()/retry()/failure()` | serbisu background Android ne'ebé moris liu app taka no reboot |
| `Executors.newFixedThreadPool(3)`, `Callable`, `invokeAll` | pool thread Java |
| `for ((a, b) in lista) when (b) { is X -> ... }` | destructuring + `when` ba tipu |

## B. Arkivu Fonte Loloos

| Arkivu | Liña | Klase / funsaun | Serbisu |
|---|---|---|---|
| `run_all.py` | 328 | `start_service`, `pid_alive`, `stop_pid`, `read_pid_file`, `find_service_windows`, `spark_streaming_alive`, `count_stuck_images`, `port_in_use`, `wait_for_port`, `main` | Hahú/hapara sistema tomak |
| `UploadQueue.kt` | 249 | `class UploadQueue : SQLiteOpenHelper`, `Item`, `Counts` | Fila upload permanente iha telemóvel |
| `UploadWorker.kt` | 207 | `class UploadWorker : Worker` | Upload background ho notifikasaun |
| `dashboard/app.py` | 213–263, 552–560, 574–649 | `_switch_to_automatic_and_drain`, `_pending_drive_photos`, botaun modu, Reprocess, pájina Pending Review | Kontrola admin |
| `dashboard/image_management_page.py` | 204–207, 226–239 | botaun Retry, `_retry` | Retry fotografia FAILED |
| `storage/pipeline_submit.py` | 26–57 | `UPLOAD_REJECTED_PREFIX`, `NotResubmittable`, `is_upload_rejected`, `submit_image_for_processing` | Haruka fali ba Kafka |
| `spark/streaming_job.py` | 203–299 | `process_and_persist`, `foreach_batch_sink`, `_upload_result`, `_process_one` | Pontu entrega Spark |

## C. Arkitetura no Fluxu Ezekusaun

### C.1 Arkitetura jerál

```mermaid
flowchart TD
    subgraph Telemovel["Telemóvel agrikultór"]
        A1["Ekran upload"] --> A2["UploadQueue (SQLite)"]
        A2 --> A3["UploadWorker (WorkManager)"]
        A3 --> A4["DriveUploader"]
    end
    subgraph Nuvem["Google Drive (konta sentrál)"]
        G1["images/email/image_id.jpg"]
        G2["results/email/image_id.json<br/>+ _processed.jpg"]
    end
    subgraph Laptop["Laptop (run_all.py)"]
        W["EWS-DriveWatcher"] --> K["Kafka (Docker)"]
        K --> M["EWS-KafkaMonitor"]
        K --> S["EWS-Spark<br/>preprocess + CNN + risku"]
        S --> D[("PostgreSQL (Docker)")]
        D --> SA["EWS-SparkAnalytics"]
        SA --> D
        D --> DB["EWS-Dashboard"]
        DB -- "Process / Reprocess / Retry" --> K
    end
    A4 --> G1
    G1 --> W
    S --> G1
    S --> G2
    G2 --> A1
```

### C.2 Estadu fotografia (husi telemóvel to'o rezultadu)

```mermaid
stateDiagram-v2
    [*] --> QUEUED : enqueue (telemóvel)
    QUEUED --> UPLOADED : DriveUploader Uploaded
    QUEUED --> REJECTED : tipu sala / boot liu
    QUEUED --> FAILED_PHONE : la iha asesu pasta
    FAILED_PHONE --> QUEUED : Retry failed
    UPLOADED --> HELD : modu MANUAL
    HELD --> INGESTED : Process (dashboard)
    UPLOADED --> INGESTED : watcher → Kafka → register_upload
    INGESTED --> PROCESSING : mark_processing
    PROCESSING --> COMPLETED : persist_result
    PROCESSING --> FAILED : erru
    FAILED --> INGESTED : Reprocess / Retry
    COMPLETED --> DELIVERED : publish_result_to_drive
    DELIVERED --> [*]
```

(`QUEUED/UPLOADED/REJECTED/FAILED_PHONE` = estadu iha `UploadQueue` telemóvel; `HELD` = la publika iha watcher; `INGESTED...COMPLETED/FAILED` = `images.status` iha PostgreSQL; `DELIVERED` = `result_published_at` iha.)

### C.3 Sekuénsia: hahú sistema (`run_all.py`)

```mermaid
sequenceDiagram
    participant U as Admin
    participant R as run_all.py
    participant DK as Docker
    participant DB as init_db
    participant W as Janela EWS-*
    U->>R: python run_all.py
    R->>R: guard double start (.run_all.pids.json)
    R->>DK: docker compose up -d kafka
    R->>R: wait_for_port(9092)
    R->>DK: docker compose up -d postgres
    R->>R: wait_for_port(5432)
    R->>DB: python -m storage.init_db
    R->>W: EWS-Spark, EWS-SparkAnalytics, EWS-KafkaMonitor, EWS-Dashboard, EWS-DriveWatcher
    R->>R: rai PID iha .run_all.pids.json
    R->>R: spark_streaming_alive (to'o 30 s)
    R->>R: count_stuck_images
    R-->>U: Up. Dashboard http://localhost:8501
```

### C.4 Sekuénsia: fotografia ida (modu AUTOMATIC)

```mermaid
sequenceDiagram
    participant F as Agrikultór
    participant Q as UploadQueue
    participant UW as UploadWorker
    participant G as Google Drive
    participant W as drive_watcher
    participant K as Kafka
    participant S as Spark
    participant C as CNN
    participant P as PostgreSQL
    F->>Q: enqueue(Item ho image_id)
    Q->>UW: WorkManager start
    UW->>G: DriveUploader.upload (resumable)
    UW->>Q: markUploaded
    W->>G: list_changes
    W->>K: eventu drive_upload
    K->>S: micro-batch
    S->>P: register_upload, mark_processing
    S->>G: download fotografia
    S->>C: predict(model_input)
    C-->>S: klase, konfiansa, lesion_score
    S->>P: persist_result COMPLETED
    S->>G: _processed.jpg + image_id.json
    F->>G: findResults / resultsSince
    G-->>F: rezultadu + risku
```

### C.5 Sekuénsia: modu MANUAL no Process

```mermaid
sequenceDiagram
    participant A as Admin
    participant DB as Dashboard
    participant M as processing_mode.json
    participant W as drive_watcher
    participant I as drive_inventory
    participant K as Kafka
    A->>DB: Switch to MANUAL
    DB->>M: set_processing_mode(MANUAL)
    W->>M: get_processing_mode
    W-->>W: held (la publika)
    A->>DB: pájina Pending Review
    DB->>I: unprocessed_photos (Drive menus BD)
    I-->>DB: lista fotografia
    A->>DB: Process ka Process all
    DB->>I: send_for_processing
    I->>K: eventu drive_upload
    A->>DB: Switch to AUTOMATIC
    DB->>I: unprocessed_photos + send_for_processing
```

### C.6 Trasu ho tempu (*Ezemplu ilustrativu*)

Tempu sira iha ne'e mak ilustrativu (la mede iha sesaun ne'e); orden no funsaun loloos.

| t (s) | Fatin | Asaun |
|---|---|---|
| 0.0 | Telemóvel | `UploadQueue.enqueue` (image_id `a1b2...`) |
| 0.1 | Telemóvel | `UploadWorker.doWork` → token → `myFolders` |
| 0.3–3.0 | Telemóvel → Drive | `uploadImage` resumable (2.4 MB) |
| 3.0 | Telemóvel | `markUploaded`, notifikasaun "1 of 1" |
| 3.0–6.0 | Laptop | watcher `list_changes` (kada 1–3 s) hetan arkivu |
| 6.1 | Laptop | eventu → Kafka (offset 57); monitor hakerek liña |
| 7.0 | Laptop | Spark micro-batch (trigger 1 s) |
| 7.1 | Laptop | `register_upload` (`INGESTED`), `mark_processing` |
| 7.1–8.3 | Laptop | download Drive (paralelu ho `clean_event`) |
| 8.5 | Laptop | `prepare_image` + `model_input` |
| 8.6 | Laptop | CNN `predict` → `northern_leaf_blight` 0.95 |
| 8.9 | Laptop | Grad-CAM + `render_processed_image` |
| 9.0 | Laptop | `persist_result` → `COMPLETED` |
| 9.0–10.5 | Laptop → Drive | `_upload_result` → `_processed.jpg` + `.json` |
| ≤ 40 | Laptop | `batch_analytics.watch` (kada 30 s) atualiza `analytics_*` |
| depois | Telemóvel | `resultsSince` → rezultadu iha ekran |
