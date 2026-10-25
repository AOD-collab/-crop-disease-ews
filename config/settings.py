"""
All settings of the image system. Every value can be set in .env or the
environment.
"""

import os
from pathlib import Path

# the project folder, so paths work from wherever a script is started
BASE_DIR = Path(__file__).resolve().parent.parent

# read .env if python-dotenv is installed (real environment variables win)
try:
    from dotenv import load_dotenv

    load_dotenv(BASE_DIR / ".env")
except ModuleNotFoundError:
    pass

# --------------------------------------------------------------------
# Database (PostgreSQL; SQLite for local tests)
# --------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"sqlite:///{BASE_DIR / 'storage' / 'app.db'}",
)

# Spark's Kafka checkpoints
SPARK_CHECKPOINT_ROOT = Path(os.getenv("SPARK_CHECKPOINT_ROOT", str(BASE_DIR / "storage" / "spark_checkpoints")))

# read-only PostgreSQL login for Tableau (created by init_db when a password is set)
TABLEAU_DB_USER = os.getenv("TABLEAU_DB_USER", "tableau_reader")
TABLEAU_DB_PASSWORD = os.getenv("TABLEAU_DB_PASSWORD", "")

# --------------------------------------------------------------------
# Image storage
# --------------------------------------------------------------------
IMAGE_STORAGE_PATH = Path(os.getenv("IMAGE_STORAGE_PATH", str(BASE_DIR / "storage" / "images")))
MAX_IMAGE_SIZE = int(os.getenv("MAX_IMAGE_SIZE", str(15 * 1024 * 1024)))  # 15 MB default
ALLOWED_IMAGE_CONTENT_TYPES = {"image/jpeg", "image/png"}

# --------------------------------------------------------------------
# Kafka (falls back to a local file queue while the broker is down)
# --------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "maize-disease-images")
KAFKA_USE_LOCAL_FALLBACK = os.getenv("KAFKA_USE_LOCAL_FALLBACK", "true").lower() == "true"
LOCAL_QUEUE_DIR = Path(os.getenv("LOCAL_QUEUE_DIR", str(BASE_DIR / "storage" / "local_queue")))

# --------------------------------------------------------------------
# ML model
# --------------------------------------------------------------------
IMAGE_MODEL = {
    # the trained CNN does all the analysis; no model = processing fails
    "weights_path": os.getenv("MODEL_PATH", str(BASE_DIR / "ml" / "trained_model" / "saved_model_cnn.pt")),
    "confidence_threshold": float(os.getenv("MODEL_CONFIDENCE_THRESHOLD", "0.5")),
    # "auto" = GPU if PyTorch sees one, else CPU
    "device": os.getenv("MODEL_DEVICE", "auto"),
    # speed: batch photos into one GPU pass (>1), fp16 on the GPU, pinned memory
    "batch_size": int(os.getenv("MODEL_BATCH_SIZE", "1")),
    "batch_wait_ms": float(os.getenv("MODEL_BATCH_WAIT_MS", "5")),
    "precision": os.getenv("MODEL_PRECISION", "fp32").lower(),
    "pin_memory": os.getenv("MODEL_PIN_MEMORY", "false").lower() == "true",   # measured: no gain
    # CPU threads per process when the model runs on the GPU
    "torch_threads": int(os.getenv("MODEL_TORCH_THREADS", "1")),
    # the disease classes come from the model file, never from here
}

# training project: analysed photos go to REVIEW_DIR, labelled ones to LABELLED_TRAIN_DIR
TRAINING_PROJECT_DIR = Path(os.getenv("TRAINING_PROJECT_DIR", r"C:\Users\user\OneDrive\Pictures\crop_disease_ews"))
REVIEW_DIR = Path(os.getenv("REVIEW_DIR", str(TRAINING_PROJECT_DIR / "data" / "review")))
LABELLED_TRAIN_DIR = Path(os.getenv("LABELLED_TRAIN_DIR", str(TRAINING_PROJECT_DIR / "data" / "train")))
SAVE_PHOTOS_FOR_REVIEW = os.getenv("SAVE_PHOTOS_FOR_REVIEW", "true").lower() == "true"

# a permanent copy of every photo downloaded from Drive (never deleted by the app)
DOWNLOADED_PHOTOS_DIR = Path(os.getenv("DOWNLOADED_PHOTOS_DIR", str(BASE_DIR / "storage" / "downloaded_photos")))
SAVE_DOWNLOADED_PHOTOS = os.getenv("SAVE_DOWNLOADED_PHOTOS", "true").lower() == "true"

# Spark's processed image of each photo (the CNN's input square)
PROCESSED_IMAGES_DIR = Path(os.getenv("PROCESSED_IMAGES_DIR", str(BASE_DIR / "storage" / "processed_images")))

# scalability test reports, logs and lock
SCALABILITY_REPORTS_DIR = Path(os.getenv("SCALABILITY_REPORTS_DIR",
                                         str(BASE_DIR / "storage" / "scalability_reports")))

# --------------------------------------------------------------------
# Accounts
# --------------------------------------------------------------------
# first admin account, created by init_db only when there are no users
BOOTSTRAP_ADMIN_USERNAME = os.getenv("BOOTSTRAP_ADMIN_USERNAME", "admin")
BOOTSTRAP_ADMIN_PASSWORD = os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "")
BOOTSTRAP_ADMIN_EMAIL = os.getenv("BOOTSTRAP_ADMIN_EMAIL", "admin@example.local")
BOOTSTRAP_ADMIN_FULL_NAME = os.getenv("BOOTSTRAP_ADMIN_FULL_NAME", "System Administrator")


# --------------------------------------------------------------------
# Risk thresholds (ml/risk.py)
# --------------------------------------------------------------------
RISK_THRESHOLDS = {
    "high": float(os.getenv("RISK_THRESHOLD_HIGH", "0.6")),
    "medium": float(os.getenv("RISK_THRESHOLD_MEDIUM", "0.3")),
}

# EXIF time has no zone: it is read as Timor-Leste time (UTC+9)
EXIF_LOCAL_TZ_OFFSET_HOURS = int(os.getenv("EXIF_LOCAL_TZ_OFFSET_HOURS", "9"))

# times are stored in UTC and shown in Timor-Leste Time (config/localtime.py)
LOCAL_TZ_OFFSET_HOURS = int(os.getenv("LOCAL_TZ_OFFSET_HOURS", str(EXIF_LOCAL_TZ_OFFSET_HOURS)))
LOCAL_TZ_LABEL = os.getenv("LOCAL_TZ_LABEL", "TLT")
LOCAL_DATETIME_FORMAT = os.getenv("LOCAL_DATETIME_FORMAT", "%d/%m/%Y %H:%M")

# at most this many Kafka events per Spark micro-batch
SPARK_MAX_EVENTS_PER_BATCH = int(os.getenv("SPARK_MAX_EVENTS_PER_BATCH", "100"))
# how often Spark checks Kafka
SPARK_TRIGGER_SECONDS = float(os.getenv("SPARK_TRIGGER_SECONDS", "1"))
# photos processed at the same time (mostly waiting on Drive)
SPARK_PARALLEL_PHOTOS = int(os.getenv("SPARK_PARALLEL_PHOTOS", "8"))
# background workers uploading results to Drive
SPARK_DRIVE_UPLOAD_WORKERS = int(os.getenv("SPARK_DRIVE_UPLOAD_WORKERS", "16"))
# true: Spark asks Drive about every photo again (slower)
SPARK_RECHECK_DRIVE_FILE = os.getenv("SPARK_RECHECK_DRIVE_FILE", "false").lower() == "true"
# Spark re-sends photos still owed a result: every N seconds (0 = off), after M, at most MAX
SPARK_AUTO_RESEND_SECONDS = float(os.getenv("SPARK_AUTO_RESEND_SECONDS", "300"))
SPARK_AUTO_RESEND_AFTER_SECONDS = int(os.getenv("SPARK_AUTO_RESEND_AFTER_SECONDS", "600"))
SPARK_AUTO_RESEND_MAX = int(os.getenv("SPARK_AUTO_RESEND_MAX", "50"))
# longer side of the copy Spark analyses
ANALYSIS_MAX_PX = int(os.getenv("ANALYSIS_MAX_PX", "1280"))
# analytics service: check interval and longest time between recomputes
ANALYTICS_CHECK_SECONDS = float(os.getenv("ANALYTICS_CHECK_SECONDS", "30"))
ANALYTICS_MAX_AGE_SECONDS = float(os.getenv("ANALYTICS_MAX_AGE_SECONDS", "600"))

# scalability test: its own topic and consumer; engine "spark" or "python"
SCALABILITY_ENGINE = os.getenv("SCALABILITY_ENGINE", "spark").lower()
SCALABILITY_KAFKA_TOPIC = os.getenv("SCALABILITY_KAFKA_TOPIC", "maize-disease-images-scalability-test")
SCALABILITY_CONSUMER_GROUP = os.getenv("SCALABILITY_CONSUMER_GROUP", "scalability-test")
# photos the test processes at the same time
SCALABILITY_WORKERS = int(os.getenv("SCALABILITY_WORKERS", str(SPARK_PARALLEL_PHOTOS)))
# a farmer photo in progress within this many minutes pauses the test
SCALABILITY_LIVE_WINDOW_MINUTES = float(os.getenv("SCALABILITY_LIVE_WINDOW_MINUTES", "10"))

# ============================================================================
# Google Drive (one central account; token made by scripts/drive_authorize.py)
# ============================================================================
GOOGLE_DRIVE_CLIENT_SECRET_FILE = Path(os.getenv(
    "GOOGLE_DRIVE_CLIENT_SECRET_FILE", str(BASE_DIR / "storage" / "google_client_secret.json")))
GOOGLE_DRIVE_TOKEN_FILE = Path(os.getenv(
    "GOOGLE_DRIVE_TOKEN_FILE", str(BASE_DIR / "storage" / "google_drive_token.json")))
GOOGLE_DRIVE_FOLDERS_FILE = Path(os.getenv(
    "GOOGLE_DRIVE_FOLDERS_FILE", str(BASE_DIR / "storage" / "google_drive_folders.json")))
GOOGLE_DRIVE_ROOT_FOLDER_NAME = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_NAME", "CropDiseaseEWS")


def _drive_folder_id(env_name: str, key: str) -> str:
    """Folder id from the environment, else from the file drive_authorize.py writes."""
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
