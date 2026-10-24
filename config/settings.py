"""
Crop Disease Early Warning System — IMAGE SYSTEM
Standalone configuration. No dependency on the sensor system, and no
simulation configuration of any kind — every value here configures a real
component (database, image storage, Kafka, the trained model, auth).
"""

import os
from pathlib import Path

# Project root = the folder containing this config/ folder. Used so that
# storage paths, the SQLite dev DB, etc. always resolve to the same
# location regardless of what directory a script/streamlit/spark job is
# launched from.
BASE_DIR = Path(__file__).resolve().parent.parent

# Load a local .env (if present) so the DB URL, admin bootstrap, Drive and
# model settings don't have to be exported in every shell (streamlit,
# spark, init_db). Real environment variables always win — load_dotenv()
# does not override values already present in the environment. Optional
# dependency: if python-dotenv isn't installed, .env is simply ignored.
try:
    from dotenv import load_dotenv

    load_dotenv(BASE_DIR / ".env")
except ModuleNotFoundError:
    pass

# --------------------------------------------------------------------
# Database — PostgreSQL in production, SQLite for local dev/testing.
# storage/database.py reads this one URL; nothing else needs to branch on
# which backend is active (SQLAlchemy abstracts the dialect).
# --------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'storage' / 'app.db'}",
)

# Read-only PostgreSQL login for Tableau (or any BI tool). When the password
# is set, init_db() creates/updates this role and lets it SELECT the
# v_tableau_images view plus the images and predictions tables — never
# users (password hashes), and it cannot change any data.
# Spark Structured Streaming checkpoints (Kafka offsets). One root shared by
# spark/streaming_job.py and storage/reset.py; tests point it at a temp dir
# so running the suite never wipes the real job's progress.
SPARK_CHECKPOINT_ROOT = Path(os.getenv("SPARK_CHECKPOINT_ROOT", str(BASE_DIR / "storage" / "spark_checkpoints")))

TABLEAU_DB_USER = os.getenv("TABLEAU_DB_USER", "tableau_reader")
TABLEAU_DB_PASSWORD = os.getenv("TABLEAU_DB_PASSWORD", "")

# --------------------------------------------------------------------
# Image storage — real uploaded photos live on disk (or a mounted volume /
# object store in production) under IMAGE_STORAGE_PATH/YYYY/MM/DD/<uuid>.ext
# --------------------------------------------------------------------
IMAGE_STORAGE_PATH = Path(os.getenv("IMAGE_STORAGE_PATH", str(BASE_DIR / "storage" / "images")))
MAX_IMAGE_SIZE = int(os.getenv("MAX_IMAGE_SIZE", str(15 * 1024 * 1024)))  # 15 MB default
ALLOWED_IMAGE_CONTENT_TYPES = {"image/jpeg", "image/png"}

# --------------------------------------------------------------------
# Kafka — real streaming transport. kafka/producer.py falls back to a local
# file-backed queue ONLY when a broker isn't reachable (infrastructure
# fallback, not data simulation) so the pipeline is still runnable/testable
# without a Kafka cluster installed.
# --------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "maize-disease-images")
KAFKA_USE_LOCAL_FALLBACK = os.getenv("KAFKA_USE_LOCAL_FALLBACK", "true").lower() == "true"
LOCAL_QUEUE_DIR = Path(os.getenv("LOCAL_QUEUE_DIR", str(BASE_DIR / "storage" / "local_queue")))

# --------------------------------------------------------------------
# ML model
# --------------------------------------------------------------------
IMAGE_MODEL = {
    # The trained CNN does all the analysis; there is no pixel-math
    # fallback. A missing checkpoint makes processing fail loudly.
    "weights_path": os.getenv("MODEL_PATH", str(BASE_DIR / "ml" / "trained_model" / "saved_model_cnn.pt")),
    "confidence_threshold": float(os.getenv("MODEL_CONFIDENCE_THRESHOLD", "0.5")),
    # "auto" = the NVIDIA GPU (RTX 3050) when this Python has a CUDA build
    # of PyTorch, else the CPU; "cuda" / "cpu" to force one (ml/inference.py).
    "device": os.getenv("MODEL_DEVICE", "auto"),
    # Inference speed (ml/inference.py; measured by scripts/benchmark_inference.py):
    #   MODEL_BATCH_SIZE    >1: Spark's photo threads share ONE batched GPU forward
    #                       pass of up to this many photos; 1 = each photo alone.
    #   MODEL_BATCH_WAIT_MS how long the batcher waits for more photos to join.
    #   MODEL_PRECISION     fp32 (the trained weights as they are) or fp16
    #                       (autocast on the GPU; fp32 stays the fallback on CPU).
    #   MODEL_PIN_MEMORY    page-locked host buffer for the CPU->GPU copy.
    "batch_size": int(os.getenv("MODEL_BATCH_SIZE", "1")),
    "batch_wait_ms": float(os.getenv("MODEL_BATCH_WAIT_MS", "5")),
    "precision": os.getenv("MODEL_PRECISION", "fp32").lower(),
    "pin_memory": os.getenv("MODEL_PIN_MEMORY", "false").lower() == "true",   # measured: no gain
    #   MODEL_TORCH_THREADS PyTorch CPU threads per process when the model runs on
    #                       the GPU (0 = PyTorch's default). Spark already runs
    #                       SPARK_PARALLEL_PHOTOS threads, each using this many.
    # Defaults keep the predictions exactly as before (FP32, one photo per
    # forward pass); only the thread count changed (same numbers, ~half the
    # CPU). For the scalability test, MODEL_BATCH_SIZE=8 + MODEL_PRECISION=fp16
    # measured fastest for the CNN itself (scripts/benchmark_inference.py).
    "torch_threads": int(os.getenv("MODEL_TORCH_THREADS", "1")),
    # Supported disease classes come FROM the loaded checkpoint at runtime
    # (checkpoint["class_names"]) — never hard-coded here. See
    # ml/inference.py.
}

# Training project (separate crop_disease_ews folder with its own venv,
# dataset and train.py — see dashboard/training_page.py). Every analysed
# farmer photo is copied to REVIEW_DIR; an admin labels it on the
# dashboard's "Label photos" page, which moves it to
# LABELLED_TRAIN_DIR/<class>/, and train.py adds those to its training split.
TRAINING_PROJECT_DIR = Path(os.getenv("TRAINING_PROJECT_DIR", r"C:\Users\user\OneDrive\Pictures\crop_disease_ews"))
REVIEW_DIR = Path(os.getenv("REVIEW_DIR", str(TRAINING_PROJECT_DIR / "data" / "review")))
LABELLED_TRAIN_DIR = Path(os.getenv("LABELLED_TRAIN_DIR", str(TRAINING_PROJECT_DIR / "data" / "train")))
SAVE_PHOTOS_FOR_REVIEW = os.getenv("SAVE_PHOTOS_FOR_REVIEW", "true").lower() == "true"

# Permanent local copy of every photo Spark downloads from Google Drive,
# byte-for-byte as uploaded (EXIF included): DOWNLOADED_PHOTOS_DIR/<user_id>/<image_id>.<ext>.
# Nothing in the app deletes it — not the factory reset, not the labelling page.
DOWNLOADED_PHOTOS_DIR = Path(os.getenv("DOWNLOADED_PHOTOS_DIR", str(BASE_DIR / "storage" / "downloaded_photos")))
SAVE_DOWNLOADED_PHOTOS = os.getenv("SAVE_DOWNLOADED_PHOTOS", "true").lower() == "true"

# Scalability test reports, logs and the one-job-at-a-time lock (scalability/).
SCALABILITY_REPORTS_DIR = Path(os.getenv("SCALABILITY_REPORTS_DIR",
                                         str(BASE_DIR / "storage" / "scalability_reports")))

# --------------------------------------------------------------------
# Accounts
# --------------------------------------------------------------------
# Bootstrap admin account, only used by storage/init_db.py on first run when
# the users table is empty. Change/rotate this in production.
BOOTSTRAP_ADMIN_USERNAME = os.getenv("BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "")
BOOTSTRAP_ADMIN_EMAIL = os.getenv("BOOTSTRAP_ADMIN_EMAIL", "admin@example.local")
BOOTSTRAP_ADMIN_FULL_NAME = os.getenv("BOOTSTRAP_ADMIN_FULL_NAME", "System Administrator")


# --------------------------------------------------------------------
# Risk thresholds (rule-based risk assessment — see spark/preprocessing.py)
# --------------------------------------------------------------------
RISK_THRESHOLDS = {
    "high": float(os.getenv("RISK_THRESHOLD_HIGH", "0.6")),
    "medium": float(os.getenv("RISK_THRESHOLD_MEDIUM", "0.3")),
}

# --------------------------------------------------------------------
# EXIF DateTimeOriginal carries no timezone info at all — it's a plain
# "YYYY:MM:DD HH:MM:SS" local wall-clock reading. Fixed offset for
# Timor-Leste (UTC+9), attached in storage/exif_utils.py so captured_at isn't
# silently stored as if it were UTC.
# --------------------------------------------------------------------
EXIF_LOCAL_TZ_OFFSET_HOURS = int(os.getenv("EXIF_LOCAL_TZ_OFFSET_HOURS", "9"))

# Times are STORED in UTC everywhere and only SHOWN in Timor-Leste Time
# (TLT, UTC+9, no daylight saving — so a fixed offset is exact and needs no
# tz database, which Windows Python doesn't ship). See config/localtime.py.
LOCAL_TZ_OFFSET_HOURS = int(os.getenv("LOCAL_TZ_OFFSET_HOURS", str(EXIF_LOCAL_TZ_OFFSET_HOURS)))
LOCAL_TZ_LABEL = os.getenv("LOCAL_TZ_LABEL", "TLT")
LOCAL_DATETIME_FORMAT = os.getenv("LOCAL_DATETIME_FORMAT", "%d/%m/%Y %H:%M")

# Spark micro-batch size: at most this many Kafka events per trigger, so a
# backlog of thousands of uploads is worked off in small batches that each
# commit (progress visible, a crash replays one batch — not all of it). 100
# (was 25): fewer waits for a batch's slowest photo with 8 at a time, and a
# replay is cheap because photos already COMPLETED are skipped.
SPARK_MAX_EVENTS_PER_BATCH = int(os.getenv("SPARK_MAX_EVENTS_PER_BATCH", "100"))
# How often Spark checks Kafka for new photos (was a fixed 10 s: a photo
# waited ~5-10 s just for the next micro-batch). Checking an empty topic is
# cheap; an empty micro-batch returns immediately.
SPARK_TRIGGER_SECONDS = float(os.getenv("SPARK_TRIGGER_SECONDS", "1"))
# Photos of one micro-batch processed at the same time. Most of a photo's
# time is Google Drive round trips (metadata, download, two uploads back),
# so several farmers' photos overlap instead of queueing behind each other.
# 8 measured ~15% faster than 4 on real photos (2026-10-09).
SPARK_PARALLEL_PHOTOS = int(os.getenv("SPARK_PARALLEL_PHOTOS", "8"))
# Results (JSON + marked-up image) are uploaded to Drive by this many
# background workers, so a photo's thread moves on instead of waiting for
# Google; each micro-batch still commits only after its uploads finished.
# Each upload mostly waits on Google, so more workers overlap more of them.
# Measured 2026-10-09, 100 photos, results all in Drive: 4 -> 0.73 photos/s,
# 8 -> 0.95, 16 -> 1.71, 32 -> 2.23. 16 keeps clear of Google's per-account
# write limits on big batches; raise it here if needed.
SPARK_DRIVE_UPLOAD_WORKERS = int(os.getenv("SPARK_DRIVE_UPLOAD_WORKERS", "16"))
# false (default): Spark trusts the Drive watcher's check of a photo it
# registered and doesn't ask Drive about the file again. true: re-check every
# photo's Drive metadata in Spark too (0.5 s+ per photo).
SPARK_RECHECK_DRIVE_FILE = os.getenv("SPARK_RECHECK_DRIVE_FILE", "false").lower() == "true"
# Longer side, in pixels, of the copy of a photo that Spark analyses
# (spark/preprocessing.prepare_image). Every step already works at <= 600 px.
ANALYSIS_MAX_PX = int(os.getenv("ANALYSIS_MAX_PX", "1280"))
# Spark analytics service (python -m spark.batch_analytics --watch): how often
# it checks the database for changes, and the longest it lets the dashboard's
# figures go without a recompute (the "last 24 h" numbers move with time).
ANALYTICS_CHECK_SECONDS = float(os.getenv("ANALYTICS_CHECK_SECONDS", "30"))
ANALYTICS_MAX_AGE_SECONDS = float(os.getenv("ANALYTICS_MAX_AGE_SECONDS", "600"))

# Scalability test (scalability/): its OWN Kafka topic and consumer group, read
# by its own process — never the production topic or the live Spark job.
# Engine that consumes the test topic and runs preprocessing + CNN:
#   spark  (default) the test's own Spark Structured Streaming query
#          (scalability/spark_engine.py), same shape as the live job
#   python a plain Kafka consumer + thread pool (scalability/worker.py)
SCALABILITY_ENGINE = os.getenv("SCALABILITY_ENGINE", "spark").lower()
SCALABILITY_KAFKA_TOPIC = os.getenv("SCALABILITY_KAFKA_TOPIC", "maize-disease-images-scalability-test")
SCALABILITY_CONSUMER_GROUP = os.getenv("SCALABILITY_CONSUMER_GROUP", "scalability-test")
# Photos the test worker processes at the same time; same default as Spark so
# the test measures the same per-photo workload.
SCALABILITY_WORKERS = int(os.getenv("SCALABILITY_WORKERS", str(SPARK_PARALLEL_PHOTOS)))
# Real-time processing has priority: while a farmer photo has been INGESTED or
# PROCESSING within this many minutes, the test starts no new photo.
SCALABILITY_LIVE_WINDOW_MINUTES = float(os.getenv("SCALABILITY_LIVE_WINDOW_MINUTES", "10"))

# ============================================================================
# Google Drive (Android -> Drive -> Kafka -> Spark -> Drive -> Android)
# ============================================================================
# One CENTRAL Google account stores every image and result. Its OAuth token
# lives only on this laptop/server (created once by
# `python scripts/drive_authorize.py`) and is never shipped to Android.
GOOGLE_DRIVE_CLIENT_SECRET_FILE = Path(os.getenv(
    "GOOGLE_DRIVE_CLIENT_SECRET_FILE", str(BASE_DIR / "storage" / "google_client_secret.json")))
GOOGLE_DRIVE_TOKEN_FILE = Path(os.getenv(
    "GOOGLE_DRIVE_TOKEN_FILE", str(BASE_DIR / "storage" / "google_drive_token.json")))
GOOGLE_DRIVE_FOLDERS_FILE = Path(os.getenv(
    "GOOGLE_DRIVE_FOLDERS_FILE", str(BASE_DIR / "storage" / "google_drive_folders.json")))
GOOGLE_DRIVE_ROOT_FOLDER_NAME = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_NAME", "CropDiseaseEWS")


def _drive_folder_id(env_name: str, key: str) -> str:
    """Folder IDs come from the environment/.env if set, otherwise from the
    JSON file scripts/drive_authorize.py writes."""
    value = os.getenv(env_name, "")
    if value:
        return value
    try:
        import json
        return json.loads(GOOGLE_DRIVE_FOLDERS_FILE.read_text()).get(key, "")
    except (OSError, ValueError):
        return ""


GOOGLE_DRIVE_IMAGES_FOLDER_ID = _drive_folder_id("GOOGLE_DRIVE_IMAGES_FOLDER_ID", "images_folder_id")
GOOGLE_DRIVE_RESULTS_FOLDER_ID = _drive_folder_id("GOOGLE_DRIVE_RESULTS_FOLDER_ID", "results_folder_id")
