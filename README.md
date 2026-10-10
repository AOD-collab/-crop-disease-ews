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
> No REST API or ngrok is needed between the phone and the laptop. Setup:
> [DRIVE_SETUP.md](DRIVE_SETUP.md). The REST API code is kept unchanged (tests,
> scripts), but the app no longer uses it.

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

Photos come from real people, using a real camera:
- The **USER (Android app)** captures or picks a real photo and uploads it.
- Any other real HTTP client (a browser form, `curl`, a drone controller
  script) can also call `POST /upload-image` — the API doesn't care what
  took the photo, only that it's a real image file.

There is no synthetic image generator anywhere in this codebase.

## 4. USER role

The USER is a farmer / field officer using the Android app. A USER can:
register themselves (see "4a. Self-registration & Gmail verification"
below), login, pick a location from a dropdown, capture/pick a
photo, preview it, upload it, watch its processing status, view their own
prediction results, and view their own upload history. A USER cannot reach
admin endpoints and cannot read another USER's images or predictions — this
is enforced in the API (`api/deps.py`), not just hidden in the UI. A USER
can never register themselves as an ADMIN — the role is always decided
server-side (`POST /auth/register` ignores any client-supplied role).

## 4a. Self-registration & Gmail verification

New USERs create their own account from the Android app's login screen
("Register" link) instead of an ADMIN provisioning every account by hand.

```
Register (Full Name, Gmail, Password, Confirm Password)
        │
        ▼
Server validation (Gmail format, password policy, uniqueness)
        │
        ▼
Account created — status = PENDING_VERIFICATION, email_verified = false
        │
        ▼
6-digit code generated, hashed, stored with an expiry, emailed to the Gmail address
        │
        ▼
Verify Gmail screen — enter the code (Resend available once it's due)
        │
   ┌────┴────┐
Correct    Incorrect / expired / too many attempts
   │            │
   ▼            ▼
status = ACTIVE   Error shown, code stays valid / resend offered
email_verified = true
   │
   ▼
Login (POST /auth/login) → normal USER client
```

Key implementation points (see `api/verification.py`, `api/mailer.py`,
`api/server.py`):

- The registration endpoint checks the address is a *structurally valid*
  Gmail address, but that alone never marks the account verified — only
  entering the emailed code does. Merely ending in `@gmail.com` proves
  nothing about who controls the mailbox.
- The verification code is a cryptographically random 6-digit number
  (`secrets.randbelow`), stored only as a keyed HMAC-SHA256 hash — never in
  plain text — expires after `VERIFICATION_CODE_TTL_MINUTES` (default 10),
  is single-use, allows at most `VERIFICATION_CODE_MAX_ATTEMPTS` (default
  5) incorrect tries, and resends are throttled by
  `VERIFICATION_RESEND_COOLDOWN_SECONDS` (default 60). It is never returned
  by any API response.
- `POST /auth/login` only issues a token for `status = ACTIVE` accounts —
  `PENDING_VERIFICATION`, `INACTIVE`, and `SUSPENDED` are all rejected, with
  a distinct message for each so a USER knows why they can't log in yet.
- Endpoints: `POST /auth/register`, `POST /auth/verify-email`,
  `POST /auth/resend-verification` (see section 22 for the full endpoint
  list). All three are rate-limited per client IP (`api/rate_limit.py`), in
  addition to the per-account attempt/cooldown limits above.
- Sending the code uses real SMTP (`api/mailer.py`) configured entirely via
  environment variables — `MAIL_HOST`, `MAIL_PORT`, `MAIL_USERNAME`,
  `MAIL_PASSWORD`, `MAIL_FROM`, `MAIL_FROM_NAME` (see section 19). If
  `MAIL_HOST` isn't set (e.g. local dev without SMTP credentials), the
  email is written to `storage/mail_outbox/` instead of being sent — the
  same "real component with a local fallback" pattern as Kafka's local
  queue — so the whole flow is runnable and testable without a real mail
  server.

## 5. ADMIN role

The ADMIN uses the web dashboard (`dashboard/app.py`) and/or the
`/admin/*` REST endpoints. An ADMIN can: manage users (full CRUD — create,
read with search/filter/pagination, update including Gmail changes and
status transitions, soft-delete/deactivate), view all images/predictions
system-wide, monitor risk/disease distribution, view real-time results,
view high-risk alerts, and view performance metrics. Every `/admin/*`
endpoint is protected server-side by `api/deps.py::require_admin` — the
dashboard/UI never being shown a button is not the enforcement mechanism.
An ADMIN cannot delete their own account or demote/suspend themselves
(self-lockout prevention).

## 5a. Location (replaces the old Field/FieldUser feature)

Earlier versions of this system required an ADMIN to create `Field`
records and explicitly assign each USER to one before they could upload
anything — the Field/FieldUser tables implemented a per-user upload
permission boundary. That feature has been **removed** and replaced with
a much simpler model: each image just carries a free-text `location`
string (e.g. a municipality), chosen from a fixed dropdown on the Android
upload form (with an "Other" option for anywhere not listed).

What this means concretely:

- **No admin setup required.** A newly registered/verified USER can
  upload immediately — there's no field to create or assignment to wait
  on first.
- **`location` is a display/filter label, not an access-control
  boundary.** Any authenticated, active USER can upload with any location
  string, including one nobody has used before. This is a deliberate
  simplification — if you need per-user upload restrictions again, that
  access-control layer no longer exists and would need to be rebuilt.
- **Nullable.** Omitting a location entirely is allowed; the image is
  still processed normally, it just won't be grouped under anything in
  the location-based dashboard views.
- The upload endpoint (`POST /upload-image`) accepts `location` as a
  plain form field (not a foreign key), and `GET /admin/analytics`
  exposes `location_summary` (risk/volume grouped by location) in place
  of the old `field_summary`.

## 6. Image collection

```
USER LOGIN → PICK LOCATION → CAPTURE/SELECT REAL PHOTO → PREVIEW → UPLOAD
    → PROCESSING (async, via Kafka + Spark) → VIEW RESULT
```

The Android app never generates a fake image — it always sends the actual
bytes of a photo the phone captured or the user picked from their gallery.

## 7. EXIF timestamp — `captured_at`

`api/exif_utils.py` reads the JPEG's `DateTimeOriginal` EXIF tag (found in
the "Exif" sub-IFD, not the base IFD — this actually tripped up the first
implementation and was fixed after a failing test caught it). If no valid
EXIF timestamp exists, `captured_at = NULL` — the system never invents one.
If the Android client supplies a trusted capture timestamp instead (form
field `captured_at_client`), it's used as a documented fallback and the
row records `captured_at_source = "client_trusted"` so it's clear where the
value came from.

## 8. Upload timestamp — `uploaded_at`

Generated by the server (`now_utc()` in `storage/database.py`) the moment
the file is received — never trusted from the client, regardless of what a
client claims. This lets the system compute `capture_to_upload_latency`
honestly.

## 9. Kafka

Apache Kafka is the real streaming transport between the API and Spark.
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
SQLAlchemy, not pyspark, so `tests/test_streaming_pipeline.py` exercises the
full clean → CNN → risk → DB path (and the `streaming_job.py` query itself
in Spark **local mode**, with a file source standing in for Kafka) without
needing a broker or a multi-node cluster.

## 11. Image processing

Per event: read the photo (the local copy in `storage/downloaded_photos/`
when there is one, otherwise a download from Google Drive, saved as the
local copy) → read its EXIF metadata (GPS, capture time, camera) →
`prepare_image` (upright, at most `ANALYSIS_MAX_PX` on the longer side) →
`ml.inference.predict()` (the CNN, section 12) → the model's
`lesion_score` (1 − P(healthy)) feeds a separate, rule-based risk score
(`spark/preprocessing.compute_risk`) → a marked-up image (`ml/render.py`).
Corrupted/missing images raise `InvalidEvent` and the image is marked
`FAILED` with an error message — never given a fake prediction.

## 12. CNN / ML

`ml/inference.py` runs the trained CNN, and only the CNN: it loads
`ml/trained_model/saved_model_cnn.pt`, a real ResNet18 checkpoint trained
on real photos (4 classes: `common_rust`, `gray_leaf_spot`, `healthy`,
`northern_leaf_blight`). There is no pixel-math fallback, leaf/maize gate
or background removal: the photo goes to the model as taken (upright, at
most `ANALYSIS_MAX_PX`). If the checkpoint is missing, prediction fails
loudly (`ModelNotAvailable`). The marked-up image the farmer sees
(`ml/render.py`) shows the model's Grad-CAM disease area.

Training the CNN itself happens in a **separate** project
(`maize_model_training/`, not included in this repo) — see
`ml/trained_model/README.md` for the handoff format.

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
server-generated ids, so a client filename can never escape a storage root
(`tests/test_image_validation.py`), and original bytes are kept unmodified.

## 16. Dashboard

`dashboard/app.py` (Streamlit) is the ADMIN web application, behind its own
login gate (ADMIN accounts only): Overview, Recent submissions, Pending
review (MANUAL mode), Farmer management, Image management (section 31),
Risk alerts, Analytics, Performance, System status, Label photos, Model
training, Scalability test (section 32) and System settings (processing
mode, factory reset). Every time on the Performance page is in seconds.

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
         │  → prepare_image → CNN (ml/inference.py) → rule-based risk
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
already in the topic) is read-only: its own consumer group, no committed offsets,
and Spark keeps its position in its own checkpoint, so it cannot take or delay an
event. Skip it with `run_all.py --no-kafka-monitor`; turn Spark's per-photo lines
off with `SPARK_LIVE_LOG=false` (errors and `[ALERT]` lines always print). The
scalability test has its own live window (SCALABILITY_TEST.md).

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
| `JWT_SECRET` | **Must be set** in production | *(empty — fails loudly if unset)* |
| `JWT_EXPIRE_MINUTES` | Token lifetime | `480` |
| `BOOTSTRAP_ADMIN_USERNAME` / `BOOTSTRAP_ADMIN_PASSWORD` | First admin account, created by `storage/init_db.py` | *(unset — no bootstrap admin without this)* |
| `BOOTSTRAP_ADMIN_EMAIL` / `BOOTSTRAP_ADMIN_FULL_NAME` | Email/name on that bootstrap admin (created pre-verified — see section 4a) | `admin@example.local` / `System Administrator` |
| `RISK_THRESHOLD_HIGH` / `RISK_THRESHOLD_MEDIUM` | Risk score cutoffs | `0.6` / `0.3` |
| `SYSTEM_NAME` | Name shown in verification emails | `Crop Disease Early Warning System` |
| `MAIL_HOST` / `MAIL_PORT` / `MAIL_USERNAME` / `MAIL_PASSWORD` | SMTP credentials for sending verification emails — **never commit these** | *(unset — falls back to `storage/mail_outbox/` files instead of sending)* |
| `MAIL_FROM` / `MAIL_FROM_NAME` | "From" address/name on verification emails | `MAIL_USERNAME` / `SYSTEM_NAME` |
| `MAIL_USE_TLS` | STARTTLS when sending | `true` |
| `MAIL_OUTBOX_DIR` | Local dev fallback location when `MAIL_HOST` is unset | `storage/mail_outbox` |
| `VERIFICATION_CODE_TTL_MINUTES` | Gmail code expiry | `10` |
| `VERIFICATION_CODE_MAX_ATTEMPTS` | Incorrect-code attempts allowed before lockout | `5` |
| `VERIFICATION_RESEND_COOLDOWN_SECONDS` | Minimum gap between resend requests | `60` |
| `PASSWORD_MIN_LENGTH` | Minimum password length for registration/admin-created users | `8` |

## 20. Kafka startup

```bash
# Install/start Kafka + Zookeeper (or use a managed Kafka service), then:
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
# create the topic once:
kafka-topics.sh --create --topic maize-disease-images \
  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

No Kafka installed? Leave `KAFKA_USE_LOCAL_FALLBACK=true` (the default) —
the API will automatically use the local file-queue fallback and log that
it did so.

## 21. Spark startup

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming_job.py
```

No multi-node cluster? `spark-submit` also runs this job in local mode
(`--master local[*]`, the default when no master is set), which still needs
a reachable Kafka broker. The processing logic on its own is covered
without any external infra by `pytest tests/test_streaming_pipeline.py`.

## 22. API startup

```bash
python -m storage.init_db   # creates tables + bootstrap ADMIN, once
uvicorn api.server:app --host 0.0.0.0 --port 8000
```

Registration/user-management endpoints added by this feature (all under
the existing FastAPI app, no separate service):

```
POST   /auth/register              full_name, email, password, confirm_password
POST   /auth/verify-email          email, code
POST   /auth/resend-verification   email
POST   /auth/login                 username (email works here too), password

GET    /admin/users                ?search=&role=&status_filter=&page=&page_size=&include_deleted=
POST   /admin/users                full_name, email, password, role, [status]
PUT    /admin/users/{id}           full_name, email, role, status
DELETE /admin/users/{id}?confirm=true    soft-delete
```

`GET /admin/users` returns `{"items": [...], "total", "page", "page_size",
"total_pages"}` rather than a bare list, so the admin UI can paginate.

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

## 25. Testing

```bash
pytest tests/ -v
```

The whole suite passes against an isolated temp SQLite DB, temp storage
folders, the real CNN checkpoint, an in-memory fake of Google Drive and the
local Kafka fallback — no external infrastructure is needed (the Spark tests
auto-skip if pyspark/JVM aren't present). Run it to see the current count.
Some of the test files:

- `test_drive_flow.py` — the whole Android → Drive → Kafka → Spark → Drive flow, private folders, MANUAL mode holding uploads, and an old (replayed) event never bypassing MANUAL mode
- `test_image_management.py` — the SQL catalog (images with and without predictions, latest prediction with a deterministic tie-break, parameterised filters), cache verification and repair from Drive, orphans, duplicate content kept as separate records, dry-run and confirmed clean-up with audit, factory-reset preview, label decisions kept apart from predictions, admin-only access
- `test_scalability.py` — the isolated scalability test: no production rows, topic or Drive results; real-time priority; the two averages from measured data only; one before/after pair
- `test_reset.py` — the factory reset, refused while the Spark job or the Drive watcher runs

- `test_auth.py` — password hashing, JWT issue/verify, tampered-token rejection
- `test_exif.py` — real EXIF extraction, missing/malformed EXIF, never inventing a timestamp
- `test_risk_assessment.py` — risk scoring behavior, event validation/cleaning
- `test_streaming_pipeline.py` — runs `spark/streaming_job.py`'s own code in **Spark local mode**: `parse_events()` decoding a Kafka-style JSON value against `EVENT_SCHEMA`, `foreach_batch_sink()` persisting a real `Prediction` row from a real image, its `DataFrame.isEmpty()` guard short-circuiting an empty micro-batch, an invalid event being marked `FAILED` (never given a fake prediction), and a full file-source → `foreachBatch` → DB run that advances a real Structured Streaming checkpoint. Builds every DataFrame by reading JSON files (pure-JVM scan) to stay clear of the executor-side Python-worker crash on this Windows box (see `DOKUMENTASAUN_TETUN.md` §10)
- `test_image_validation.py` — empty/corrupted/oversized files, path-traversal safety, no filename collisions
- `test_api.py` — full HTTP integration: login, upload workflow with a free-text `location` (including omitting it, and an arbitrary/unrecognized value — location is a display label, not an access-control boundary), USER data isolation (a USER cannot read another USER's image), role enforcement (a USER cannot reach `/admin/*` even by trying to smuggle `role=ADMIN` into a request), admin user management
- `test_registration.py` — Gmail registration/verification: invalid Gmail addresses, mismatched/weak passwords, duplicate-email rejection, pending accounts unable to log in, the full register → verify → login flow (reading the code from `storage/mail_outbox/`, the local fallback used since no SMTP server is configured for tests), single-use codes, attempt limits, resend issuing a fresh code, admin user CRUD (default-pending vs. explicit-active user creation, self-lockout prevention on delete/role/status, soft delete excluding a user from the default listing, Gmail changes resetting verification), and a Pydantic v2 regression (`UpdateUserRequest` fields must be `Optional[str] = None`, not a bare `str = None` — the latter rejects an explicit JSON `null`, which a real API client sending `{"role": null}` to mean "no change" would hit as a confusing 422)
- `test_dashboard_users.py` — runs the actual Streamlit dashboard script end-to-end via `streamlit.testing.v1.AppTest` (not just an import/syntax check) against a real temp database, targeting a real bug this exact technique caught during review: the "Edit user" form's Status dropdown always reflects *some* value (Streamlit widgets can't represent "untouched"), and an earlier version compared it against `target.status.value` *after* an email-change had already mutated that same field in memory — so editing only the email while leaving Status showing its pre-existing value silently overwrote the PENDING_VERIFICATION reset back to ACTIVE and skipped sending a new code. Fixed by capturing the original status before any mutation; this test proves the full loop (edit email in the dashboard → code lands in `storage/mail_outbox/` → verify → log in with the new address) actually works

The API server, upload workflow, and Kafka local-file fallback were also
exercised live end-to-end (real `uvicorn` process, real HTTP requests, real
image bytes) during development — this caught and led
to fixing several real bugs (EXIF sub-IFD lookup, a passlib/bcrypt
incompatibility, a SQLite naive/aware datetime mismatch, and a pandas
`NaT`-serializes-as-string issue in the analytics endpoint).

## 26. Performance evaluation

`spark/analytics.performance_metrics()` (surfaced at `/admin/system-status`
and the dashboard's Performance tab) computes, from real recorded
timestamps only:

- `avg_capture_to_upload_seconds` — only for images with a real EXIF/
  trusted capture timestamp; `null` otherwise (never estimated)
- `avg_upload_to_ingestion_seconds` — upload to the Kafka event
- `avg_processing_latency_ms` — Kafka event to prediction saved
  (`predictions.processing_latency_ms`, stored in **milliseconds**; it
  includes waiting in Kafka, reading the photo, preprocessing and the CNN)
- `avg_end_to_end_latency_seconds` — upload to prediction
- `total_processed` / `total_failed`

Units: a name ending `_ms` is milliseconds, `_s` / `_seconds` is seconds.
The dashboard's Performance page shows all four in **seconds** (the
millisecond value divided by 1000) and explains each span.

Throughput under load is measured by the scalability test (section 32,
[SCALABILITY_TEST.md](SCALABILITY_TEST.md)) on this laptop.

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
dashboard sidebar or `PUT /admin/processing-mode`. **AUTOMATIC**: the Drive
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
`prepare_image` + CNN code, no `images`/`predictions` rows, no Drive results.
Real-time processing has priority. The page shows one before/after
preprocessing pair and two averages — execution time per iteration and
processing time (preprocessing + CNN) per photo, both in seconds. Details:
[SCALABILITY_TEST.md](SCALABILITY_TEST.md).

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
├── api/                     REST API (kept; the app no longer uses it), auth, image storage helpers
├── android_app/             farmer app (Kotlin) — Google sign-in, upload, history, settings
├── config/                  settings.py (env-driven), localtime.py (Timor-Leste Time)
├── kafka/
│   ├── drive_watcher.py      Google Drive images/<email>/ → Kafka (respects MANUAL mode)
│   └── producer.py           confluent-kafka + resilient local file-queue fallback
├── spark/
│   ├── streaming_job.py      Spark Structured Streaming: Kafka → per-photo processing
│   ├── preprocessing.py      validation, EXIF, prepare_image, CNN call, risk, persistence, Drive results
│   ├── analytics.py          dashboard/Tableau analytics and performance metrics
│   ├── batch_analytics.py    Spark batch job (JDBC) — daily summary per municipality
│   └── data_quality.py       duplicate / outlier / missing-value checks
├── ml/
│   ├── inference.py          the CNN (and model_input_image for display)
│   ├── render.py             marked-up image (Grad-CAM + banner)
│   └── trained_model/        saved_model_cnn.pt
├── storage/
│   ├── database.py           models, views (v_tableau_images, v_admin_image_catalog), migrations
│   ├── image_catalog.py      the admin image catalog (parameterised SQL)
│   ├── image_cache.py        managed local cache: verify, repair, reconcile, audited clean-up
│   ├── reviews.py            administrator label decisions (image_reviews)
│   ├── drive_client.py       Google Drive API
│   ├── pipeline_submit.py    publish an image to Kafka (AUTOMATIC upload / MANUAL "Process")
│   ├── reset.py              factory reset (+ preview, audit)
│   └── schema.sql            raw PostgreSQL DDL
├── scalability/              isolated scalability test (dataset, run, worker, report, monitor)
├── dashboard/                Streamlit admin dashboard (app.py + one module per larger page)
├── scripts/                  Drive authorisation, farmer sharing, benchmarks, source-code PDF
├── tests/                    pytest suite, no external infrastructure required
├── SCALABILITY_TEST.md, INFERENCE_BENCHMARK.md, DRIVE_SETUP.md, DOKUMENTASAUN_TETUN.md
├── requirements.txt, docker-compose.yml
└── README.md
```
