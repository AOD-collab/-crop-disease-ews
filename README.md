# Crop Disease Early Warning System for Maize — Image System

A real-time / near-real-time Big Data pipeline that turns real maize leaf
photos into disease classifications and risk alerts, built for the DIT
Capstone Project (Big Data, S1 Siencia Komputador, 2026).

**Simulation has been completely removed.** There is no `data_source/`
package, no synthetic feature generator, no fake predictions. Every value
that reaches the database comes from a real uploaded photo, a real EXIF
tag, a real CNN analysis, or a server-generated timestamp. If a
component is unavailable (no trained model, no Kafka broker, a corrupted
photo), the system reports that clearly instead of inventing an answer.

---


> **Architecture update — Google Drive.** The Android app now talks only to
> Google Drive: farmers sign in with Google and upload photos to their own
> private Drive folder `images/<email>/`. On the laptop,
> `kafka/drive_watcher.py` finds new photos in every farmer's folder with the
> Drive API and publishes their Drive file id + metadata to Kafka; Spark
> downloads each photo, processes it and writes `results/<email>/<image_id>.json`
> plus a marked-up `<image_id>_processed.jpg` back to Drive, where the app
> shows the original and processed photo side by side:
> **Android → Google Drive → Kafka → Spark → Google Drive → Android.**
> No server of ours, REST API or ngrok sits between the phone and the
> laptop. Setup: section 19a, "Google Drive setup". (The old REST API, which
> the app used before Google Drive, has been removed.)

## 1. Project objective

Detect maize leaf disease early enough for a farmer or field officer to
act, by streaming real field photos through a Big Data pipeline —
ingestion, Kafka, Spark Structured Streaming, CNN classification, rule-based
risk scoring — into a dashboard that surfaces high-risk fields as alerts.

## 2. Problem statement

Manual maize field inspection is slow, infrequent, and doesn't scale across
many fields. By the time visible symptoms are noticed and reported,
disease can already have spread. A system that processes photos as they're
captured — rather than in a periodic manual survey — can surface risk
sooner and let a wider group of USERs (farmers/field officers) contribute
observations that an ADMIN can monitor centrally.

## 3. Real image source

Photos come from real people, using a real camera: the **USER (Android
app)** captures or picks a real photo and uploads it, unchanged, to their
own Google Drive folder. There is no synthetic image generator anywhere in
this codebase.

## 4. USER role

The USER is a farmer / field officer using the Android app. A USER signs in
with Google, picks a location from a dropdown, captures/picks photos,
uploads them, and sees each photo's result and their own upload history.
Access is enforced by Google Drive itself: every farmer has a private
`images/<email>/` folder they can write to and a `results/<email>/` folder
they can only read, shared with that Google account alone. An account is
given access by the ADMIN (dashboard "Register farmer", or
`python scripts/drive_share_user.py <email>`); until then the app reports
that the account cannot upload yet. The laptop decides who uploaded a
file from Google's own owner field, never from anything the phone claims.

## 5. ADMIN role

The ADMIN uses the web dashboard (`dashboard/app.py`): register farmers,
manage users, view all images/predictions system-wide, monitor
risk/disease distribution, view real-time results and high-risk alerts,
switch between AUTOMATIC and MANUAL processing, label photos and train /
deploy the model. Dashboard login checks the ADMIN's bcrypt password hash
(`storage/auth.py`). An ADMIN cannot delete their own account or
demote/suspend themselves (self-lockout prevention).

## 5a. Location (replaces the old Field/FieldUser feature)

Earlier versions of this system required an ADMIN to create `Field`
records and explicitly assign each USER to one before they could upload
anything — the Field/FieldUser tables implemented a per-user upload
permission boundary. That feature has been **removed** and replaced with
a much simpler model: each image just carries a free-text `location`
string (e.g. a municipality), chosen from a fixed dropdown on the Android
upload form (with an "Other" option for anywhere not listed).

What this means concretely:

- **No field setup required.** A registered farmer can upload
  immediately — there's no field to create or assignment to wait on first.
- **`location` is a display/filter label, not an access-control
  boundary.** Any registered farmer can upload with any location string, including one nobody has used before. This is a deliberate
  simplification — if you need per-user upload restrictions again, that
  access-control layer no longer exists and would need to be rebuilt.
- **Nullable.** Omitting a location entirely is allowed; the image is
  still processed normally, it just won't be grouped under anything in
  the location-based dashboard views.
- The phone sends `location` as a plain Drive file property (not a
  foreign key); the Drive watcher stores it on the image row.

## 6. Image collection

```
GOOGLE SIGN-IN → PICK LOCATION → CAPTURE/SELECT REAL PHOTOS → UPLOAD TO DRIVE
    → PROCESSING (async, via Drive watcher + Kafka + Spark) → VIEW RESULT
```

The Android app never generates a fake image — it always sends the actual
bytes of a photo the phone captured or the user picked from their gallery.

## 7. EXIF timestamp — `captured_at`

`storage/exif_utils.py` reads the JPEG's `DateTimeOriginal` EXIF tag (found in
the "Exif" sub-IFD, not the base IFD — this actually tripped up the first
implementation and was fixed after a failing test caught it). If no valid
EXIF timestamp exists, `captured_at = NULL` — the system never invents one.
If the Android client supplies a trusted capture timestamp instead (Drive
file property `captured_at_client`), it's used as a documented fallback and the
row records `captured_at_source = "client_trusted"` so it's clear where the
value came from.

## 8. Upload timestamp — `uploaded_at`

Taken from Google Drive's own `createdTime` for the uploaded file (Google's
clock, not the phone's) by the Drive watcher — never trusted from the
client, regardless of what a client claims. This lets the system compute `capture_to_upload_latency`
honestly.

## 9. Kafka

Apache Kafka is the real streaming transport between the Drive watcher and Spark.
Topic: `maize-disease-images` (`kafka/config.py`). The producer uses
**confluent-kafka**, not kafka-python — this project's own package is also
named `kafka/`, and importing `kafka-python` (whose import name is also
`kafka`) from inside a package of the same name causes an import collision.
confluent-kafka imports as `confluent_kafka`, avoiding that entirely.

If no broker is reachable at `KAFKA_BOOTSTRAP_SERVERS`, `kafka/producer.py`
falls back to a local JSON-lines file queue — this is an **infrastructure**
fallback (so the pipeline is testable without installing a Kafka cluster),
never a data fallback: the events themselves are always real upload events.

## 10. Spark Structured Streaming

`spark/streaming_job.py` is the real streaming-processing engine. It
subscribes to the Kafka topic, parses/cleans/validates each event, and
inside `foreachBatch` calls `spark/preprocessing.py` to read the photo (its
verified local copy, or Google Drive), run the CNN, compute a rule-based
risk score, persist the result to the database and write the result back
to the farmer's Drive folder.

Two kinds of event are never processed (`_process_one`): an event carrying
a `run_id` (a scalability-test event from before the test got its own
topic, section 32) and an event whose image is still `PENDING_REVIEW` — in
MANUAL mode only the admin's **Process** releases a photo, and it sets the
image to `INGESTED` before publishing, so an event that finds the photo
still held is an old one (e.g. Spark replaying the topic after a factory
reset that kept Kafka's messages) and must not bypass manual mode.

The processing/persistence code (`spark/preprocessing.py`) depends only on
SQLAlchemy, not pyspark, so the full clean → CNN → risk → DB path can run
without a broker or a multi-node cluster (e.g. `python -m
storage.requeue_stuck --inline`).

## 11. Image processing

Per event: read the photo (the local copy in `storage/downloaded_photos/`
when there is one, otherwise a download from Google Drive, saved as the
local copy) → read its EXIF metadata (GPS, capture time, camera) →
Spark's image preprocessing: `prepare_image` (upright, at most
`ANALYSIS_MAX_PX` on the longer side) then `model_input` (resize the shorter
side to the model's 224 px, centre 224 × 224 crop, normalise with the
checkpoint's mean/std) → `ml.inference.predict()` (the CNN, section 12) → the model's
`lesion_score` (1 − P(healthy)) feeds a separate, rule-based risk score
(`spark/preprocessing.compute_risk`) → a marked-up image (`ml/render.py`).
Corrupted/missing images raise `InvalidEvent` and the image is marked
`FAILED` with an error message — never given a fake prediction.

## 12. CNN / ML

`ml/inference.py` runs the trained CNN, and only the CNN: it loads
`ml/trained_model/saved_model_cnn.pt`, an SE-ResNet-18 checkpoint trained on
real photos (4 classes: `common_rust`, `gray_leaf_spot`, `healthy`,
`northern_leaf_blight`), and classifies the input Spark prepared
(`spark/preprocessing.model_input`). It does no image preprocessing itself;
it only tells Spark the input the model expects (`input_spec()`: 224 px,
ImageNet mean/std, read from the checkpoint). There is no pixel-math
fallback, leaf/maize gate or background removal. If the checkpoint is
missing, prediction fails loudly (`ModelNotAvailable`). The marked-up image
the farmer sees (`ml/render.py`) shows the model's Grad-CAM disease area,
placed on the photo by Spark (`heatmap_on_photo`).

Why a centre crop: on the model's held-out test split (646 photos) the crop
scored 95.67 % against 94.74 % for squeezing the whole photo to 224 × 224
(the training project's evaluation); on its 78 real field photos 78.2 %
against 70.5 % (padding 64.1 %, three crops averaged 75.6 %). The crop
leaves out the edges of a non-square photo, so farmers should keep the leaf
in the middle of the picture.

The CNN is trained in a **separate** project, `crop_disease_ews`
(`TRAINING_PROJECT_DIR`, not in this repo), from the dashboard's Model
training page — see `ml/trained_model/README.md`.

## 13. Risk assessment

Kept explicitly separate from CNN classification (DIT manual section 26):
`predicted_class` / `prediction_confidence` come from the CNN;
`risk_score` / `risk_level` are a **rule-based indicator** in
`spark/preprocessing.compute_risk()`: the model's probability that the leaf
is diseased at all (1 − P(healthy)), put into LOW / MEDIUM / HIGH by
`RISK_THRESHOLD_MEDIUM` (0.3) / `RISK_THRESHOLD_HIGH` (0.6). An "uncertain"
(low-confidence) read is capped below HIGH. `discoloration_pct`,
`texture_irregularity` and `leaf_area_affected_pct` are no longer computed
(empty for new photos; the columns stay for older rows).

## 14. PostgreSQL

`storage/database.py` uses SQLAlchemy against `DATABASE_URL`. Point it at
PostgreSQL in production:

```bash
export DATABASE_URL="postgresql+psycopg2://user:password@host:5432/crop_disease"
```

The demo runs on PostgreSQL 16 in Docker (`docker-compose.yml`, `.env`).
SQLite (`sqlite:///storage/app.db`) is still used by the test suite —
moving between them is a one-line env var change, not a rewrite, since
every other module goes through the same ORM session. See
`storage/schema.sql` for the equivalent raw PostgreSQL DDL.

Tables: `users`, `images`, `predictions`, `processing_events`,
`rejected_events`, `system_settings` (processing mode), `scale_test_runs` /
`scale_test_items` (scalability measurements), `image_reviews` (the
administrator's confirmed label or discard — never a model prediction) and
`storage_audit_log` (destructive storage actions). Views, recreated by every
`init_db()`: `v_tableau_images` (read by Tableau through the read-only
`tableau_reader` account) and `v_admin_image_catalog` (the Image
management page). Both pick each image's latest prediction by
`predicted_at`, ties broken by `prediction_id`, and leave out
scalability-test rows.

## 15. Image storage

PostgreSQL is the catalog, Google Drive the repository, local storage a
managed cache:

| Where | What | Kept until |
|---|---|---|
| Drive `images/<email>/` | the farmer's original photos (`images.image_path` = `gdrive:<file id>`) | the farmer or a reset with "delete Drive files" removes them |
| Drive `results/<email>/` | `<image_id>.json` + `<image_id>_processed.jpg` the app shows | factory reset with "delete Drive files" |
| `storage/downloaded_photos/<user_id>/<image_id>.jpg` | byte-for-byte cache of the Drive original, checked against `images.content_hash` (SHA-256) before use; a missing or wrong copy is downloaded again and checked before it is stored | only an administrator-confirmed clean-up (section 31) |
| `REVIEW_DIR` (`data/review`) | EXIF-stripped copies waiting on the Label photos page | labelled (moved to training) or discarded |
| `LABELLED_TRAIN_DIR/<class>/` | confirmed training images | never cleaned by the app |
| `storage/scalability_reports/<run_id>/` | a scalability run's staged photos, its before/after pair and CSV files | clean-up of finished runs |
| `storage/images/` | photos from the previous (REST upload) design | factory reset |

Processed images are kept in Drive only. Local files are always named by
server-generated ids, so a client filename can never escape a storage root,
and original bytes are kept unmodified.

## 16. Dashboard

`dashboard/app.py` (Streamlit) is the ADMIN web application, behind its own
login gate (ADMIN accounts only): Overview, Recent submissions, Pending
review (MANUAL mode), Farmer management, Image management (section 31),
Risk alerts, Analytics, System status, Label photos, Model training,
Scalability test (section 32) and System settings (processing mode,
factory reset).

The dashboard's figures and charts (Overview, Risk alerts, Analytics) are
computed by **Spark**: `spark/batch_analytics.py` reads `images`,
`predictions` and `users` over JDBC, aggregates them with Spark DataFrames
and writes the `analytics_*` tables (plus `batch_location_daily_summary`),
which the dashboard reads (`spark/analytics.py`). The `EWS-SparkAnalytics`
service that `run_all.py` starts (`python -m spark.batch_analytics --watch`)
checks the database every `ANALYTICS_CHECK_SECONDS` (30) and recomputes
when anything changed, and at least every `ANALYTICS_MAX_AGE_SECONDS` (600).
Rules: scalability-test photos are
left out, each photo counts once with its latest prediction, days are
Timor-Leste days. Needs PostgreSQL (Spark's JDBC reader); on SQLite the
charts stay empty. Row-level lists (recent submissions, HIGH-risk photos)
are read directly from the database.

## 17. Real-time architecture

```
   Farmer (Android app, Google sign-in)
         │  photo + municipality (+ phone GPS for in-app camera photos)
         ▼
   Google Drive  images/<email>/
         │
         ▼
   Drive watcher (kafka/drive_watcher.py) ── MANUAL mode: image held as PENDING_REVIEW
         │  AUTOMATIC mode (or the admin's "Process")      until the admin releases it
         ▼
   Apache Kafka  maize-disease-images
         │
         ▼
   Spark Structured Streaming (spark/streaming_job.py)
         │  clean_event → read photo (local cache or Drive) → EXIF metadata
         │  → prepare_image → model_input → CNN (ml/inference.py) → risk
         ▼
   PostgreSQL (images, predictions, processing_events)
         │                                  │
         ▼                                  ▼
   Google Drive results/<email>/      ADMIN dashboard (dashboard/app.py)
   (result JSON + marked-up image)     Tableau (v_tableau_images, read-only)
         │
         ▼
   Farmer's app shows the result

   Scalability test (scalability/): its own Kafka topic, consumer group and
   worker process — never the topic, Spark job or tables above (section 32).
```

### Watching the real-time pipeline live

`python run_all.py` opens one console window per component. Together they show a
farmer photo moving through the pipeline as it happens:

```
EWS-DriveWatcher  [drive_watcher] new photo 1c94cbc2 from ana@gmail.com (Baucau) -> Kafka topic 'maize-disease-images' (Drive file …)
EWS-KafkaMonitor  [kafka 13:57:41] p0 offset 812: photo 1c94cbc2 from ana@gmail.com (Baucau), uploaded 10/10/2026 13:57 TLT, Drive file -> waiting for Spark
EWS-Spark         [spark 13:57:42] [kafka]  batch 37: 1 photo event(s) received from Kafka
                  [spark 13:57:45] [photo]  1c94cbc2  ana@gmail.com  Baucau  -> common_rust 91%  risk HIGH  (download 1.93s  preprocess 0.14s  cnn 0.05s  render 0.30s  total 2.81s)
                  [spark 13:57:46] [drive]  1c94cbc2  result + processed image delivered to ana@gmail.com's Drive (0.84s)
                  [spark 13:57:46] [spark]  batch 37 committed: 1 ok, 0 failed, 0 skipped in 3.95s
```

`EWS-KafkaMonitor` (`python -m kafka.monitor`, `--from-start` to also list what is
already in the topic) is read-only: it reads by direct partition assignment (no
consumer group, no committed offsets), and Spark keeps its position in its own
checkpoint, so it cannot take or delay an event. It also watches the scalability
test's own topic and prints a one-line summary per second while a test runs
(events published, and how far the test's worker has read them); `--live-only`
leaves that out. Test photos never appear in the EWS-Spark window: the test is
processed by its own worker, not by Spark. Skip it with `run_all.py --no-kafka-monitor`; turn Spark's per-photo lines
off with `SPARK_LIVE_LOG=false` (errors and `[ALERT]` lines always print). The
scalability test has its own live window (section 32).

## 18. Installation

```bash
git clone <this repo>
cd crop_disease_image_system
pip install -r requirements.txt
```

`torch`/`torchvision` (needed for the trained CNN) and `pyspark`/
`confluent-kafka` (needed for real streaming) are the heaviest
dependencies; torch is required, since every prediction comes from the
CNN. `spark/preprocessing.py` itself has no pyspark dependency, so the
processing tests run without it.

## 19. Configuration

All configuration is via environment variables (`config/settings.py`) —
nothing is hard-coded, and there is no simulation configuration of any
kind:

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | SQLAlchemy connection string | `sqlite:///storage/app.db` |
| `IMAGE_STORAGE_PATH` | Where uploaded photos are saved | `storage/images` |
| `MAX_IMAGE_SIZE` | Upload size limit (bytes) | `15728640` (15 MB) |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9092` |
| `KAFKA_TOPIC` | Kafka topic name | `maize-disease-images` |
| `KAFKA_USE_LOCAL_FALLBACK` | Fall back to local queue if Kafka is down | `true` |
| `MODEL_PATH` | Path to the CNN checkpoint | `ml/trained_model/saved_model_cnn.pt` |
| `BOOTSTRAP_ADMIN_USERNAME` / `BOOTSTRAP_ADMIN_PASSWORD` | First admin account, created by `storage/init_db.py` | *(unset — no bootstrap admin without this)* |
| `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_FULL_NAME` | Email/name on that bootstrap admin (created pre-verified) | `admin@example.local` / `System Administrator` |
| `RISK_THRESHOLD_HIGH` / `RISK_THRESHOLD_MEDIUM` | Risk score cutoffs | `0.6` / `0.3` |

## 19a. Google Drive setup (once)

1. **Google Cloud project** (<https://console.cloud.google.com/>, signed in
   with the central Google account): create a project, enable the **Google
   Drive API**, and set up the **OAuth consent screen** (External, scope
   `https://www.googleapis.com/auth/drive`). While the app is in *Testing*,
   add the central account and every farmer's Gmail as **test users**;
   Google then expires the laptop's refresh token after 7 days, so step 2 is
   repeated weekly.
2. **Two OAuth clients in that same project:** a **Desktop app** client for
   the laptop (download its JSON to `storage/google_client_secret.json`),
   and an **Android** client with package name `com.cropdisease.app` and the
   SHA-1 of the key that signs the APK (nothing secret goes into the app).
3. **Laptop:** `python scripts/drive_authorize.py`, signing in with the
   central account. It saves the token and creates
   `CropDiseaseEWS/images` and `CropDiseaseEWS/results`; those files stay
   on the laptop (`.gitignore`).
4. **Each farmer:** `python scripts/drive_share_user.py farmer@gmail.com`
   (or the dashboard's **Register farmer**) creates `images/<email>/`
   (upload) and `results/<email>/` (read), shared only with that account.
   Until then the app says the account cannot upload yet.

## 20. Kafka startup

```bash
# Install/start Kafka + Zookeeper (or use a managed Kafka service), then:
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# create the topic once:
kafka-topics.sh --create --topic maize-disease-images \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

No Kafka installed? Leave `KAFKA_USE_LOCAL_FALLBACK=true` (the default) —
the Drive watcher and the dashboard will automatically use the local
file-queue fallback and log that they did so.

## 21. Spark startup

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming_job.py
```

No multi-node cluster? `spark-submit` also runs this job in local mode
(`--master local[*]`, the default when no master is set), which still needs
a reachable Kafka broker.

The analytics job (section 16) is a second Spark application:

```bash
python -m spark.batch_analytics            # compute once
python -m spark.batch_analytics --watch    # keep the dashboard's figures current
```

The first run downloads the PostgreSQL JDBC driver from Maven Central
(`org.postgresql:postgresql:42.7.3`).

## 22. REST API (removed)

The FastAPI server the Android app used before Google Drive (`api/`, with
JWT login, Gmail self-registration and `/admin/*` endpoints) has been
removed: the app talks only to Google Drive, and the dashboard does
everything an ADMIN needs. The few account helpers the rest of the system
uses (e-mail normalisation, bcrypt password hashing) are in
`storage/auth.py`. Existing database columns from that feature
(`email_verified`, `verification_code_hash`, ...) are kept unchanged.

## 23. Dashboard startup

```bash
streamlit run dashboard/app.py
```

Log in with the bootstrap ADMIN account (see `BOOTSTRAP_ADMIN_USERNAME` /
`BOOTSTRAP_ADMIN_PASSWORD` above).

## 24. Database initialization

```bash
python -m storage.init_db
```

Creates all tables if missing, and — only if the `users` table is
completely empty and `BOOTSTRAP_ADMIN_PASSWORD` is set — creates one ADMIN
account so there's a way to log in and start creating users. If the
`users` table already existed from before the Gmail-registration feature,
this also adds the new columns it needs (`full_name`, `email_verified`,
`verification_code_hash`, etc.) via lightweight `ALTER TABLE` migrations —
existing rows and existing ADMIN/USER accounts are left untouched.

## 25. Verification

The automated pytest suite has been removed from the project. To check a
running system: `python run_all.py` reports whether the Spark job came up
and whether any photo is stuck; the dashboard's System Status page checks
Google Drive, Kafka, the database, Spark and the Drive watcher;
`scripts/benchmark_inference.py` measures the CNN; and the scalability test
(section 32) measures the whole pipeline under load.

## 26. Performance evaluation

Spark (`spark/batch_analytics.py`, stored in `analytics_summary`, read by
`spark/analytics.performance_metrics()` and surfaced by
`scripts/perf_report.py`) computes, from real recorded timestamps only:

- `avg_capture_to_upload_seconds` — only for images with a real EXIF/
  trusted capture timestamp; `null` otherwise (never estimated)
- `avg_upload_to_ingestion_seconds` — upload to the Kafka event
- `avg_processing_latency_ms` — Kafka event to prediction saved
  (`predictions.processing_latency_ms`, stored in **milliseconds**; it
  includes waiting in Kafka, reading the photo, preprocessing and the CNN)
- `avg_end_to_end_latency_seconds` — upload to prediction
- `total_processed` / `total_failed`

Units: a name ending `_ms` is milliseconds, `_s` / `_seconds` is seconds.

Throughput under load is measured by the scalability test (section 32) on this laptop.

## 27. Limitations

Being explicit about what's implemented vs. what needs infrastructure this
environment doesn't have, per DIT manual section 58:

**Running on this laptop:** a single-node Kafka broker and PostgreSQL 16
in Docker, the Spark Structured Streaming job in local mode reading Kafka,
the CNN on the GPU, the Drive watcher, the dashboard and Tableau. The
largest scalability test processed 6,977 photos from a local folder (one
iteration, 0 failures).

**Not available or not demonstrated:** a multi-node Kafka/Spark cluster
(everything shares one laptop, so the scalability test can only give the
real-time pipeline priority, not separate hardware); the model's accuracy
on real farmer photos (its ~96 % validation accuracy is on the training
dataset's own photos); and rejection of photos that are not maize — every
photo gets one of the four classes, or "uncertain" below 50 % confidence.

## 28. Android app (USER client)

`android_app/` is a native Kotlin app that talks only to Google Drive
(never the laptop or the database). Screens: **Login** (Google sign-in,
`LoginActivity.kt` + `DriveAuth.kt`), **Home** (`HomeActivity.kt`),
**Upload** (`MainActivity.kt`: take or pick one or several photos, choose
the municipality; the in-app camera adds the phone's position),
**History** (`HistoryActivity.kt`: each photo next to its marked-up
result, kept on the phone by `HistoryStore.kt`) and **Settings**. Uploads
are queued and retried in the background (`UploadQueue.kt`,
`UploadWorker.kt`); all times are shown in Timor-Leste Time
(`TimorTime.kt`). The app has English and Tetun texts (`values-tet`).

See `android_app/README.md` for build instructions (the debug APK builds
with Gradle and JDK 17).

## 29. Future improvements

- Measure the model on labelled farmer photos and retrain with field photos
- A "not maize" class so unrelated photos are rejected
- Push notifications for HIGH-risk results
- A multi-node Kafka/Spark deployment, so tests and live processing use separate hardware
- A proper Alembic migration setup for schema evolution (currently `Base.metadata.create_all` plus small additive migrations)

## 30. Processing modes (AUTOMATIC / MANUAL)

Stored in `system_settings` (`processing_mode`) and switched from the
dashboard sidebar. **AUTOMATIC**: the Drive
watcher publishes every new photo to Kafka at once. **MANUAL**: the photo is
stored as `PENDING_REVIEW` and waits on the Pending review page until an
admin presses **Process** (or switches back to AUTOMATIC, which releases
everything held). Releasing a photo marks it `INGESTED` before its event is
published (`storage/pipeline_submit.py`), and the Spark job skips any event
whose photo is still `PENDING_REVIEW`, so an old Kafka event can never
bypass MANUAL mode. The scalability test pauses while a farmer photo is
`INGESTED`/`PROCESSING` (held photos don't count), and the factory reset
refuses to run while the Spark job or the Drive watcher is running.

## 31. Image management

The dashboard's **Image management** page (`dashboard/image_management_page.py`):

- **Counts** — database records (farmer images by status, with a
  prediction, awaiting labels) next to files on this laptop (cached
  originals, disk use, missing/invalid copies, potentially orphaned files).
- **Gallery** — newest upload first (image id breaks ties), 12 per page,
  thumbnails made in memory from the verified local copy; filters for image
  id, farmer, municipality, upload dates, status, predicted disease, risk,
  review status and local copy, combined with AND and run as parameterised
  SQL on `v_admin_image_catalog` (`storage/image_catalog.py`).
- **Details** — the original and the processed image side by side (local
  copy or Drive; Drive download verified against the SHA-256 before it is
  cached), the model prediction and the administrator's confirmed label in
  separate boxes, timestamps, errors, identical uploads (same SHA-256, kept
  as separate records), Retry for a FAILED image, a link to Label photos.
- **Storage** — disk use per category (a hard-linked file counted once),
  reconciliation of the cache against the catalog (matched, invalid, wrong
  folder, orphan, missing), a dry-run clean-up preview of exact files with
  reasons, and a confirmed clean-up that re-checks every file, never touches
  review or training images, photos being processed or a running test,
  and writes each outcome to `storage_audit_log`.

## 32. Scalability test

Isolated from the real-time pipeline: its own Kafka topic
(`SCALABILITY_KAFKA_TOPIC`), consumer group and worker process, the same
`prepare_image` + `model_input` + CNN code, no `images`/`predictions` rows, no Drive results.
Real-time processing has priority. The page shows one before/after
preprocessing pair and two averages — execution time per iteration and
processing time (preprocessing + CNN) per photo, both in seconds.

## 33. Factory reset

System settings → Factory reset (or `python -m storage.reset --yes`) first
shows, counted at that moment, what it deletes (images, predictions,
processing events, rejected events, Spark checkpoints, the Kafka file queue)
and what it keeps (users, label decisions, audit log, scalability
measurements, cached originals, review and training images). Optionally it
also empties the Kafka topic and the farmers' Drive folders. It refuses to
run while the Spark job or the Drive watcher is running, is written to
`storage_audit_log`, and reports the cached photos that no record matches
afterwards (they are never deleted automatically).

---

## Project structure

```
crop_disease_image_system/
├── run_all.py               starts Kafka/PostgreSQL (Docker), Spark, dashboard, Drive watcher
├── android_app/             farmer app (Kotlin) — Google sign-in, upload, history, settings
├── config/                  settings.py (env-driven), localtime.py (Timor-Leste Time)
├── kafka/
│   ├── drive_watcher.py      Google Drive images/<email>/ → Kafka (respects MANUAL mode)
│   ├── producer.py           confluent-kafka + resilient local file-queue fallback
│   └── monitor.py            read-only live view of the topics (EWS-KafkaMonitor window)
├── spark/
│   ├── streaming_job.py      Spark Structured Streaming: Kafka → per-photo processing
│   ├── preprocessing.py      validation, EXIF, image preprocessing (model_input), CNN call, risk, persistence, Drive results
│   ├── analytics.py          reads the Spark-computed analytics for the dashboard
│   ├── batch_analytics.py    Spark analytics job (JDBC): every dashboard figure; --watch keeps them current
│   └── data_quality.py       duplicate / outlier / missing-value checks
├── ml/
│   ├── inference.py          the CNN: loads the checkpoint, classifies Spark's prepared input
│   ├── render.py             marked-up image (Grad-CAM + banner)
│   └── trained_model/        saved_model_cnn.pt
├── storage/
│   ├── database.py           models, views (v_tableau_images, v_admin_image_catalog), migrations
│   ├── init_db.py            create tables + bootstrap the first admin
│   ├── auth.py               e-mail normalisation, bcrypt password hashing
│   ├── image_storage.py      where photos live (local path / gdrive:<id>), safe local writes
│   ├── exif_utils.py         EXIF capture time, GPS, camera
│   ├── image_catalog.py      the admin image catalog (parameterised SQL)
│   ├── image_cache.py        managed local cache: verify, repair, reconcile, audited clean-up
│   ├── reviews.py            administrator label decisions (image_reviews)
│   ├── drive_client.py       Google Drive API
│   ├── pipeline_submit.py    publish an image to Kafka (AUTOMATIC upload / MANUAL "Process")
│   ├── requeue_stuck.py      re-publish / process images stranded before Spark
│   ├── reset.py              factory reset (+ preview, audit)
│   └── schema.sql            raw PostgreSQL DDL
├── scalability/              isolated scalability test (dataset, run, worker, report, monitor)
├── dashboard/                Streamlit admin dashboard (app.py + one module per larger page)
├── scripts/                  Drive authorisation, farmer sharing, benchmarks, performance report
├── requirements.txt, docker-compose.yml
└── README.md
```
