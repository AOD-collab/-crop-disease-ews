"""
Data Storage layer — SQLAlchemy models + engine/session.

Backend is selected purely by DATABASE_URL (config/settings.py):
    postgresql+psycopg2://user:pass@host:5432/dbname   -> production
    sqlite:///storage/app.db                            -> local dev/test

Nothing else in the codebase branches on which database is active — every
other module goes through the ORM session here, so moving from SQLite to
PostgreSQL is a one-line env var change, not a rewrite (DIT manual, section
13).

Tables (see storage/schema.sql for the equivalent raw PostgreSQL DDL,
including indexes):
    users        — login accounts, USER or ADMIN role
    images       — uploaded photo metadata (captured_at, uploaded_at,
                   free-text `location`, ...)
    predictions  — CNN classification + rule-based risk assessment output
    processing_events — per-image pipeline timing, for performance evaluation
    scale_test_runs / scale_test_items — scalability test runs and their
                   per-photo measurements (scalability/run.py)
    image_reviews — the administrator's confirmed label (or discard) per image
    storage_audit_log — destructive storage actions from the dashboard
    v_admin_image_catalog (view) — images + farmer + latest prediction + review

NOTE: the `fields`/`field_users` tables and the per-user field-assignment
access control they implemented have been removed in favor of a simple
free-text `location` string carried directly on each image (see
`_drop_field_feature()` below for the migration off any pre-existing
database, and README section 5a/6 for what replaced it and why the access-
control implication of that removal matters).
"""

import enum
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    create_engine, Column, String, Float, Integer, BigInteger, Date, DateTime, ForeignKey,
    Enum, Text, Boolean, Index, event, inspect, text as sql_text,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

from config.settings import (
    DATABASE_URL, LOCAL_TZ_OFFSET_HOURS, TABLEAU_DB_USER, TABLEAU_DB_PASSWORD,
)

Base = declarative_base()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def new_uuid() -> str:
    return str(uuid.uuid4())


class Role(str, enum.Enum):
    USER = "USER"
    ADMIN = "ADMIN"


class UserStatus(str, enum.Enum):
    # PENDING_VERIFICATION: self-registered through the removed REST API,
    # Gmail never confirmed. Kept so existing rows stay valid.
    PENDING_VERIFICATION = "PENDING_VERIFICATION"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUSPENDED = "SUSPENDED"


class RiskLevel(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class ProcessingStatus(str, enum.Enum):
    # A row only exists once Spark has registered the photo
    # (spark/ingestion.register_upload); a photo waiting in MANUAL mode is
    # still only a file in Drive. (The PostgreSQL enum type also keeps the
    # old UPLOADED / PENDING_REVIEW labels; no row uses them.)
    INGESTED = "INGESTED"          # registered by Spark (or sent again by Reprocess)
    PROCESSING = "PROCESSING"      # Spark/CNN picked it up
    COMPLETED = "COMPLETED"        # prediction stored
    FAILED = "FAILED"              # any stage failed — see error_message


# ----------------------------------------------------------------------
# users
# ----------------------------------------------------------------------
class User(Base):
    __tablename__ = "users"

    user_id = Column(String, primary_key=True, default=new_uuid)
    username = Column(String, unique=True, nullable=False, index=True)
    full_name = Column(String, nullable=True)
    email = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(Enum(Role), nullable=False, default=Role.USER)
    # Default stays ACTIVE so every existing/programmatic user-creation path
    # (bootstrap admin, ORM-created test fixtures, etc.) keeps working
    # unchanged. (Self-registration through the removed REST API set
    # PENDING_VERIFICATION instead.)
    status = Column(Enum(UserStatus), nullable=False, default=UserStatus.ACTIVE)

    # --- Gmail verification (the removed REST API's self-registration;
    # columns kept so the schema and existing rows stay unchanged) ---
    email_verified = Column(Boolean, nullable=False, default=False)
    # Only a hash of the verification code was ever stored, never the code.
    verification_code_hash = Column(String, nullable=True)
    verification_code_expires_at = Column(DateTime(timezone=True), nullable=True)
    verification_attempts = Column(Integer, nullable=False, default=0)
    last_verification_sent_at = Column(DateTime(timezone=True), nullable=True)

    # --- Soft delete (section 9: "Prefer soft deletion if the existing
    # system architecture supports it") ---
    is_deleted = Column(Boolean, nullable=False, default=False)
    deleted_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=now_utc, onupdate=now_utc, nullable=False)

    images = relationship("Image", back_populates="user")


# ----------------------------------------------------------------------
# images
# ----------------------------------------------------------------------
class Image(Base):
    __tablename__ = "images"

    image_id = Column(String, primary_key=True, default=new_uuid)
    # Free-text place name chosen on the upload form (e.g. a municipality) —
    # replaces the old `field_id` FK + fields/field_users tables. Nullable:
    # a location is a label for filtering/display, not an access-control
    # boundary (see the README note on what this removal means for
    # per-user upload restrictions, which no longer exist).
    location = Column(String, nullable=True)
    user_id = Column(String, ForeignKey("users.user_id"), nullable=False)
    original_filename = Column(String, nullable=True)
    image_path = Column(String, nullable=False)   # storage/images/YYYY/MM/DD/<uuid>.jpg
    source = Column(String, nullable=False, default="android_upload")
    # SHA-256 of the uploaded bytes — identifies repeat uploads of the same
    # photo (same content re-submitted, possibly under a new filename).
    content_hash = Column(String, nullable=True)

    # --- timestamp pipeline (see README "Timestamp pipeline") ---
    captured_at = Column(DateTime(timezone=True), nullable=True)   # EXIF DateTimeOriginal, or NULL
    captured_at_source = Column(String, nullable=True)             # "exif" | "client_trusted" | None
    uploaded_at = Column(DateTime(timezone=True), nullable=False, default=now_utc)  # server-generated, never client-trusted
    ingested_at = Column(DateTime(timezone=True), nullable=True)   # Kafka publish/consume time
    processed_at = Column(DateTime(timezone=True), nullable=True)  # Spark processing completion
    # When the result last reached the farmer's Drive /results folder. NULL
    # for a COMPLETED Drive photo = the phone hasn't got it yet (e.g. Spark
    # stopped mid-upload); a replayed batch then uploads it instead of skipping.
    result_published_at = Column(DateTime(timezone=True), nullable=True)

    # --- photo metadata read from the file's EXIF by Spark
    # (spark/preprocessing.extract_metadata); NULL when the photo has none ---
    gps_latitude = Column(Float, nullable=True)
    gps_longitude = Column(Float, nullable=True)
    gps_altitude_m = Column(Float, nullable=True)
    # Where the coordinates came from: "exif" (the photo's own GPS tags —
    # where it was taken, always preferred) or "device" (the phone's position
    # when the in-app camera took it, sent by the app as a fallback). NULL
    # when there are no coordinates.
    gps_source = Column(String, nullable=True)
    camera_make = Column(String, nullable=True)
    camera_model = Column(String, nullable=True)
    image_width = Column(Integer, nullable=True)
    image_height = Column(Integer, nullable=True)

    status = Column(Enum(ProcessingStatus), nullable=False, default=ProcessingStatus.INGESTED)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)

    user = relationship("User", back_populates="images")
    predictions = relationship("Prediction", back_populates="image", cascade="all, delete-orphan")
    processing_events = relationship("ProcessingEvent", back_populates="image", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_images_location", "location"),
        Index("ix_images_user_id", "user_id"),
        Index("ix_images_captured_at", "captured_at"),
        Index("ix_images_uploaded_at", "uploaded_at"),
        Index("ix_images_content_hash", "content_hash"),
    )


# ----------------------------------------------------------------------
# predictions
# ----------------------------------------------------------------------
class Prediction(Base):
    __tablename__ = "predictions"

    prediction_id = Column(String, primary_key=True, default=new_uuid)
    image_id = Column(String, ForeignKey("images.image_id"), nullable=False)

    # --- CNN classification output (section 26: kept separate from risk) ---
    predicted_class = Column(String, nullable=True)
    prediction_confidence = Column(Float, nullable=True)
    # True when prediction_confidence fell below IMAGE_MODEL["confidence_threshold"]
    # — predicted_class is then the literal string "uncertain" rather than a
    # disease name, and risk_level is capped below HIGH (see
    # spark/preprocessing.py compute_risk()).
    is_uncertain = Column(Boolean, nullable=False, default=False)

    # --- CV features feeding the rule-based risk assessment ---
    lesion_score = Column(Float, nullable=True)
    discoloration_pct = Column(Float, nullable=True)
    texture_irregularity = Column(Float, nullable=True)
    leaf_area_affected_pct = Column(Float, nullable=True)

    # --- rule-based risk assessment (documented as a risk INDICATOR, not a
    # disease probability — see spark/preprocessing.py compute_risk()) ---
    risk_score = Column(Float, nullable=True)
    risk_level = Column(Enum(RiskLevel), nullable=True)

    model_version = Column(String, nullable=True)
    predicted_at = Column(DateTime(timezone=True), nullable=True)
    processing_latency_ms = Column(Integer, nullable=True)  # ingested_at -> predicted_at
    created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)

    image = relationship("Image", back_populates="predictions")

    __table_args__ = (
        Index("ix_predictions_image_id", "image_id"),
        Index("ix_predictions_predicted_class", "predicted_class"),
        Index("ix_predictions_risk_level", "risk_level"),
        Index("ix_predictions_predicted_at", "predicted_at"),
    )


# ----------------------------------------------------------------------
# processing_events (optional, performance evaluation — section 19/44)
# ----------------------------------------------------------------------
class ProcessingEvent(Base):
    __tablename__ = "processing_events"

    event_id = Column(String, primary_key=True, default=new_uuid)
    image_id = Column(String, ForeignKey("images.image_id"), nullable=False)
    # When the event was published to Kafka — the image's ingested_at (was
    # misleadingly stored in kafka_received_at before; see persist_result()).
    api_published_at = Column(DateTime(timezone=True), nullable=True)
    # When Spark's foreachBatch sink actually received/started handling this
    # event — genuinely Spark-side, not the publish time.
    kafka_received_at = Column(DateTime(timezone=True), nullable=True)
    spark_started_at = Column(DateTime(timezone=True), nullable=True)
    spark_completed_at = Column(DateTime(timezone=True), nullable=True)
    prediction_started_at = Column(DateTime(timezone=True), nullable=True)
    prediction_completed_at = Column(DateTime(timezone=True), nullable=True)
    total_latency_ms = Column(Integer, nullable=True)
    status = Column(Enum(ProcessingStatus), nullable=False, default=ProcessingStatus.INGESTED)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)

    image = relationship("Image", back_populates="processing_events")


# ----------------------------------------------------------------------
# rejected_events — malformed/invalid Kafka events that clean_event() (or
# persist_result()'s unknown-image_id guard) rejected before they became a
# ProcessingEvent row (e.g. because image_id didn't match any Image at all).
# Without this table those events vanished with only a stdout print().
# ----------------------------------------------------------------------
class RejectedEvent(Base):
    __tablename__ = "rejected_events"

    rejected_event_id = Column(String, primary_key=True, default=new_uuid)
    image_id = Column(String, nullable=True)  # may not correspond to a real Image row
    raw_event = Column(Text, nullable=True)   # JSON-serialized event payload, best-effort
    reason = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)


# ----------------------------------------------------------------------
# Scalability tests (scalability/run.py, dashboard "Scalability test" page).
# A run re-processes photos from the Drive collection through the normal
# Kafka -> Spark -> CNN pipeline. Each photo gets a NEW Image row with
# source = SCALE_TEST_SOURCE (never the farmer's own row), owned by the
# SCALE_TEST_USERNAME system account. No foreign keys to `images`, so the
# measurements survive a factory reset or a run clean-up.
# ----------------------------------------------------------------------
SCALE_TEST_SOURCE = "scalability_test"
SCALE_TEST_USERNAME = "scalability-test"


class ScaleTestRun(Base):
    __tablename__ = "scale_test_runs"

    run_id = Column(String, primary_key=True)
    # PREPARING -> RUNNING -> COMPLETED | INCOMPLETE (some photos never
    # finished) | FAILED (the run itself broke) | ABORTED (runner died)
    status = Column(String, nullable=False, default="PREPARING")
    phase = Column(String, nullable=True)            # human-readable current step
    pid = Column(Integer, nullable=True)             # runner process, to detect a dead run
    requested_size = Column(Integer, nullable=False)
    replay_of = Column(String, nullable=True)        # run_id whose photo selection was reused
    publish_results = Column(Boolean, nullable=False, default=False)
    drive_results_folder_id = Column(String, nullable=True)
    config_json = Column(Text, nullable=True)        # benchmark configuration
    selection_json = Column(Text, nullable=True)     # counts + exclusion reasons
    summary_json = Column(Text, nullable=True)       # final metrics (scalability/report.py)
    error_message = Column(Text, nullable=True)
    data_cleaned_at = Column(DateTime(timezone=True), nullable=True)  # images/predictions removed
    started_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)
    finished_at = Column(DateTime(timezone=True), nullable=True)


class ScaleTestItem(Base):
    __tablename__ = "scale_test_items"

    image_id = Column(String, primary_key=True)      # the test's own Image row
    run_id = Column(String, nullable=False, index=True)
    source_image_id = Column(String, nullable=False)       # the farmer's original image_id
    source_drive_file_id = Column(String, nullable=False)
    source_user_id = Column(String, nullable=True)         # folder of the local copy
    local_copy_status = Column(String, nullable=True)      # verified | downloaded
    download_seconds = Column(Float, nullable=True)
    size_bytes = Column(Integer, nullable=True)
    status = Column(String, nullable=False, default="STAGED")  # STAGED, PUBLISHED, COMPLETED, FAILED
    error_message = Column(Text, nullable=True)
    published_at = Column(DateTime(timezone=True), nullable=True)
    spark_received_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    timings_json = Column(Text, nullable=True)       # per-step seconds from process_event
    pipeline_seconds = Column(Float, nullable=True)  # whole Spark handling of this photo
    db_write_seconds = Column(Float, nullable=True)
    drive_publish_seconds = Column(Float, nullable=True)
    # The CNN's result for the test photo, kept ONLY here for the test page's
    # "where the disease is" charts — never an Image/Prediction row, never
    # sent to a farmer (scalability/worker.py).
    predicted_class = Column(String, nullable=True)
    prediction_confidence = Column(Float, nullable=True)
    is_uncertain = Column(Boolean, nullable=True)
    risk_level = Column(String, nullable=True)
    # Where the source photo was taken: its municipality (the farmer's upload
    # form, Drive appProperties) and GPS (the photo's EXIF, else the phone's
    # position the app sent). Read while staging, before the timer starts.
    location = Column(String, nullable=True)
    gps_latitude = Column(Float, nullable=True)
    gps_longitude = Column(Float, nullable=True)

    __table_args__ = (Index("ix_scale_test_items_run_status", "run_id", "status"),)


# ----------------------------------------------------------------------
# image_reviews — the administrator's decision on the "Label photos" page
# (dashboard/labelling_page.py): a CONFIRMED disease label, or "discarded".
# Kept apart from predictions on purpose: a model prediction is never a
# confirmed label. No foreign key to `images`, so the decision (and the
# training photo it produced) outlives a factory reset, like
# LABELLED_TRAIN_DIR/labels_log.csv, which is still written too.
# ----------------------------------------------------------------------
class ImageReview(Base):
    __tablename__ = "image_reviews"

    image_id = Column(String, primary_key=True)
    decision = Column(String, nullable=False)              # "labelled" | "discarded"
    confirmed_class = Column(String, nullable=True)        # set only when decision == "labelled"
    predicted_class_at_review = Column(String, nullable=True)  # what the model said, for comparison
    reviewed_by = Column(String, nullable=True)
    reviewed_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)


# ----------------------------------------------------------------------
# storage_audit_log — every destructive storage action from the dashboard
# (cache clean-up, cache repair, factory reset): what, why, the outcome and
# who. Never cleared by the factory reset.
# ----------------------------------------------------------------------
class StorageAuditLog(Base):
    __tablename__ = "storage_audit_log"

    audit_id = Column(String, primary_key=True, default=new_uuid)
    action = Column(String, nullable=False)       # cleanup_delete | cache_repair | factory_reset
    category = Column(String, nullable=True)      # storage category (storage/image_cache.py)
    target = Column(String, nullable=True)        # file, relative to its category folder
    reason = Column(Text, nullable=True)
    outcome = Column(String, nullable=False)      # done | skipped | failed | dry_run
    detail = Column(Text, nullable=True)          # error text or a summary
    performed_by = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)

    __table_args__ = (Index("ix_storage_audit_log_created_at", "created_at"),)


# ----------------------------------------------------------------------
# Analytics computed by Spark (spark/batch_analytics.py) and shown by the
# dashboard. Spark replaces the rows of every table on each run; nothing
# else writes them. Scalability-test images are left out; each image counts
# once, with its latest prediction; days are Timor-Leste days.
# ----------------------------------------------------------------------
class _SparkOutput:
    """When Spark computed the row (every analytics table ends with it)."""
    generated_at = Column(DateTime(timezone=True), nullable=False)


class AnalyticsSummary(_SparkOutput, Base):
    """One row: the headline figures and the pipeline timings."""
    __tablename__ = "analytics_summary"

    summary_id = Column(Integer, primary_key=True)                  # always 1
    total_users = Column(BigInteger, nullable=False)                # accounts not deleted
    total_images = Column(BigInteger, nullable=False)
    total_healthy = Column(BigInteger, nullable=False)
    total_diseased = Column(BigInteger, nullable=False)             # classified, not healthy, not uncertain
    total_uncertain = Column(BigInteger, nullable=False)
    total_unclassified = Column(BigInteger, nullable=False)         # no prediction yet (pending / failed)
    total_high_risk = Column(BigInteger, nullable=False)
    total_processed = Column(BigInteger, nullable=False)            # has a prediction
    total_failed = Column(BigInteger, nullable=False)
    submissions_24h = Column(BigInteger, nullable=False)            # uploaded in the 24 h before generated_at
    active_farmers_24h = Column(BigInteger, nullable=False)
    most_detected_disease = Column(String, nullable=True)           # most frequent class except healthy
    avg_processing_latency_ms = Column(Float, nullable=True)        # ingested -> predicted
    avg_capture_to_upload_seconds = Column(Float, nullable=True)
    avg_upload_to_ingestion_seconds = Column(Float, nullable=True)
    avg_end_to_end_latency_seconds = Column(Float, nullable=True)   # uploaded -> predicted


class AnalyticsDiseaseDistribution(_SparkOutput, Base):
    __tablename__ = "analytics_disease_distribution"

    predicted_class = Column(String, primary_key=True)
    photos = Column(BigInteger, nullable=False)


class AnalyticsRiskDistribution(_SparkOutput, Base):
    __tablename__ = "analytics_risk_distribution"

    risk_level = Column(String, primary_key=True)
    photos = Column(BigInteger, nullable=False)


class AnalyticsHighRiskByLocation(_SparkOutput, Base):
    """Per municipality: analysed photos and how many are HIGH risk."""
    __tablename__ = "analytics_high_risk_by_location"

    location = Column(String, primary_key=True)                     # blank -> "Unknown location"
    high = Column(BigInteger, nullable=False)
    total = Column(BigInteger, nullable=False)
    high_pct = Column(Float, nullable=False)


class AnalyticsDiseaseTrend(_SparkOutput, Base):
    """Photos classified per municipality, day and disease."""
    __tablename__ = "analytics_disease_trend"

    location = Column(String, primary_key=True)
    day = Column(Date, primary_key=True)
    predicted_class = Column(String, primary_key=True)
    photos = Column(BigInteger, nullable=False)


class AnalyticsDiseaseDaily(_SparkOutput, Base):
    """Photos classified per day and disease, all municipalities together."""
    __tablename__ = "analytics_disease_daily"

    day = Column(Date, primary_key=True)
    predicted_class = Column(String, primary_key=True)
    photos = Column(BigInteger, nullable=False)


class BatchLocationDailySummary(_SparkOutput, Base):
    """Per municipality and upload day: the batch summary for reports/BI."""
    __tablename__ = "batch_location_daily_summary"

    location = Column(String, primary_key=True)
    summary_date = Column(Date, primary_key=True)
    total_images = Column(BigInteger, nullable=False)
    total_healthy = Column(BigInteger, nullable=False)
    total_diseased = Column(BigInteger, nullable=False)
    avg_risk_score = Column(Float, nullable=True)
    high_risk_count = Column(BigInteger, nullable=False)


ANALYTICS_TABLES = (
    "analytics_summary", "analytics_disease_distribution", "analytics_risk_distribution",
    "analytics_high_risk_by_location", "analytics_disease_trend", "analytics_disease_daily",
    "batch_location_daily_summary",
)


# The scalability test page's disease figures, one set per test run, from
# the CNN results kept in scale_test_items (COMPLETED photos only). Kept
# apart from the farmer analytics above and not shared with Tableau.
class AnalyticsScaleTestDiseaseByLocation(_SparkOutput, Base):
    """Test photos per run, municipality and disease."""
    __tablename__ = "analytics_scale_test_disease_by_location"

    run_id = Column(String, primary_key=True)
    location = Column(String, primary_key=True)                     # blank -> "Unknown location"
    predicted_class = Column(String, primary_key=True)
    photos = Column(BigInteger, nullable=False)


class AnalyticsScaleTestDiseaseDistribution(_SparkOutput, Base):
    """Test photos per run and disease."""
    __tablename__ = "analytics_scale_test_disease_distribution"

    run_id = Column(String, primary_key=True)
    predicted_class = Column(String, primary_key=True)
    photos = Column(BigInteger, nullable=False)


class AnalyticsScaleTestHighRiskByLocation(_SparkOutput, Base):
    """Per run and municipality: analysed test photos and how many are HIGH risk."""
    __tablename__ = "analytics_scale_test_high_risk_by_location"

    run_id = Column(String, primary_key=True)
    location = Column(String, primary_key=True)                     # blank -> "Unknown location"
    high = Column(BigInteger, nullable=False)
    total = Column(BigInteger, nullable=False)
    high_pct = Column(Float, nullable=False)


# ----------------------------------------------------------------------
# Engine / session
# ----------------------------------------------------------------------
_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
# pool_pre_ping (PostgreSQL): Spark, the Drive watcher and the dashboard run for
# days; after the Postgres container restarts, every pooled connection is dead
# and its next use failed — a photo was marked FAILED, a watcher scan aborted.
# The ping (one round trip on checkout) replaces a dead connection instead.
engine = create_engine(DATABASE_URL, connect_args=_connect_args, future=True,
                       pool_pre_ping=not DATABASE_URL.startswith("sqlite"))

if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _sqlite_wal(dbapi_conn, _record):
        # WAL lets outside readers (a live Tableau connection, DB Browser)
        # read while Spark/the dashboard write, instead of a reader's lock making
        # writers fail with "database is locked". Persistent on the file.
        dbapi_conn.execute("PRAGMA journal_mode=WAL")

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


# Columns added by the Gmail-registration feature. `Base.metadata.create_all`
# only creates missing TABLES, not missing COLUMNS on a table that already
# exists — so a database created before this feature shipped needs these
# added explicitly. (SQLite and PostgreSQL both support plain
# `ALTER TABLE ... ADD COLUMN`, which is all this needs.)
_USERS_MIGRATION_COLUMNS = [
    ("full_name", "VARCHAR"),
    ("email_verified", "BOOLEAN NOT NULL DEFAULT 0"),
    ("verification_code_hash", "VARCHAR"),
    ("verification_code_expires_at", "TIMESTAMP"),
    ("verification_attempts", "INTEGER NOT NULL DEFAULT 0"),
    ("last_verification_sent_at", "TIMESTAMP"),
    ("is_deleted", "BOOLEAN NOT NULL DEFAULT 0"),
    ("deleted_at", "TIMESTAMP"),
]

_IMAGES_MIGRATION_COLUMNS = [
    ("location", "VARCHAR"),
    ("content_hash", "VARCHAR"),
    ("gps_latitude", "FLOAT"),
    ("gps_longitude", "FLOAT"),
    ("gps_altitude_m", "FLOAT"),
    ("gps_source", "VARCHAR"),
    ("camera_make", "VARCHAR"),
    ("camera_model", "VARCHAR"),
    ("image_width", "INTEGER"),
    ("image_height", "INTEGER"),
    ("result_published_at", "TIMESTAMP WITH TIME ZONE"),
]

_PREDICTIONS_MIGRATION_COLUMNS = [
    ("is_uncertain", "BOOLEAN NOT NULL DEFAULT 0"),
]

_PROCESSING_EVENTS_MIGRATION_COLUMNS = [
    ("api_published_at", "TIMESTAMP"),
]

_SCALE_TEST_ITEMS_MIGRATION_COLUMNS = [
    ("prediction_confidence", "FLOAT"),
    ("is_uncertain", "BOOLEAN"),
    ("risk_level", "VARCHAR"),
    ("location", "VARCHAR"),
    ("gps_latitude", "FLOAT"),
    ("gps_longitude", "FLOAT"),
]


def _add_missing_columns(table_name: str, columns: list[tuple[str, str]]):
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return  # create_all will make it fresh, with every column already
    existing_columns = {col["name"] for col in inspector.get_columns(table_name)}
    is_postgres = engine.dialect.name == "postgresql"
    with engine.begin() as conn:
        for column_name, column_type in columns:
            if column_name in existing_columns:
                continue
            ddl_type = column_type.replace("BOOLEAN NOT NULL DEFAULT 0", "BOOLEAN NOT NULL DEFAULT FALSE") \
                if is_postgres else column_type
            conn.execute(sql_text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}"))


def _drop_field_feature(conn):
    """One-time migration off the removed fields/field_users tables and
    Image.field_id column. Both SQLite (3.35+) and PostgreSQL support
    plain `ALTER TABLE ... DROP COLUMN`, and both support plain `DROP
    TABLE ... [CASCADE]`, so no dialect branching is needed here beyond
    CASCADE, which PostgreSQL requires to also drop the FK-dependent index/
    constraint and SQLite doesn't understand at all.
    """
    inspector = inspect(engine)
    is_postgres = engine.dialect.name == "postgresql"

    if "images" in inspector.get_table_names():
        image_columns = {col["name"] for col in inspector.get_columns("images")}
        if "field_id" in image_columns:
            conn.execute(sql_text("DROP INDEX IF EXISTS ix_images_field_id"))
            conn.execute(sql_text("ALTER TABLE images DROP COLUMN field_id"))

    for table in ("field_users", "fields"):
        if table in inspector.get_table_names():
            drop_stmt = f"DROP TABLE IF EXISTS {table}" + (" CASCADE" if is_postgres else "")
            conn.execute(sql_text(drop_stmt))


def _drop_system_settings(conn):
    """system_settings held only the image-processing mode, which now lives
    in storage/processing_mode.json (the Drive watcher reads it without a
    database)."""
    conn.execute(sql_text("DROP TABLE IF EXISTS system_settings"))


# Each image's latest prediction (newest predicted_at, ties broken by the
# larger prediction_id — the rule of spark/batch_analytics.py) as `latest`,
# row number rn = 1; used by both views and the dashboard's photo list.
LATEST_PREDICTION_CTE = """WITH latest AS (
            SELECT p.*, ROW_NUMBER() OVER (PARTITION BY p.image_id
                       ORDER BY p.predicted_at DESC NULLS LAST, p.prediction_id DESC) AS rn
            FROM predictions p
        )"""


TABLEAU_VIEW = "v_tableau_images"
TABLEAU_TABLES = ("images", "predictions") + ANALYTICS_TABLES


def _create_tableau_view(conn):
    """Read-only view a live Tableau connection reads instead of the raw
    tables: one row per image with its LATEST prediction (mirrors
    spark/analytics_reader.images_with_predictions, which drops replayed
    at-least-once predictions) and timestamps shifted from stored UTC to
    Timor-Leste Time, so Tableau shows the same numbers as the dashboard.
    Dropped and recreated on every init_db() so it tracks schema changes."""
    is_postgres = engine.dialect.name == "postgresql"

    def local(col):
        if is_postgres:
            # timestamptz -> naive UTC wall time -> naive TLT wall time, so
            # Tableau shows local time whatever its session time zone is
            return f"({col} AT TIME ZONE 'UTC') + INTERVAL '{LOCAL_TZ_OFFSET_HOURS} hours'"
        return f"datetime({col}, '{LOCAL_TZ_OFFSET_HOURS:+d} hours')"

    conn.execute(sql_text(f"DROP VIEW IF EXISTS {TABLEAU_VIEW}"))
    conn.execute(sql_text(f"""
        CREATE VIEW {TABLEAU_VIEW} AS
        {LATEST_PREDICTION_CTE}
        SELECT
            i.image_id, i.location, i.source, i.status,
            u.username AS farmer_username,
            {local('i.captured_at')} AS captured_at,
            {local('i.uploaded_at')} AS uploaded_at,
            {local('i.ingested_at')} AS ingested_at,
            {local('i.processed_at')} AS processed_at,
            i.gps_latitude, i.gps_longitude, i.gps_source, i.camera_make, i.camera_model,
            l.predicted_class, l.prediction_confidence, l.is_uncertain,
            l.lesion_score, l.discoloration_pct, l.texture_irregularity,
            l.leaf_area_affected_pct, l.risk_score, l.risk_level,
            l.model_version,
            {local('l.predicted_at')} AS predicted_at,
            l.processing_latency_ms
        FROM images i
        LEFT JOIN latest l ON l.image_id = i.image_id AND l.rn = 1
        LEFT JOIN users u ON u.user_id = i.user_id
        WHERE i.source <> '{SCALE_TEST_SOURCE}'
    """))
    if is_postgres and TABLEAU_DB_PASSWORD:
        _ensure_tableau_reader(conn)


def _ensure_tableau_reader(conn):
    """Create (or re-password) the read-only BI login and grant it SELECT on
    the view alone. Re-run on every init_db() because DROP VIEW above also
    drops the view's grants."""
    role = TABLEAU_DB_USER.replace('"', "")
    password = TABLEAU_DB_PASSWORD.replace("'", "''")
    exists = conn.execute(sql_text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}).scalar()
    verb = "ALTER" if exists else "CREATE"
    conn.execute(sql_text(f"""{verb} ROLE "{role}" WITH LOGIN PASSWORD '{password}'
                             NOSUPERUSER NOCREATEDB NOCREATEROLE"""))
    db_name = conn.execute(sql_text("SELECT current_database()")).scalar()
    conn.execute(sql_text(f'GRANT CONNECT ON DATABASE "{db_name}" TO "{role}"'))
    conn.execute(sql_text(f'GRANT USAGE ON SCHEMA public TO "{role}"'))
    conn.execute(sql_text(f'GRANT SELECT ON {TABLEAU_VIEW} TO "{role}"'))
    # The raw tables too, so Tableau can model images -> predictions
    # (image_id) itself. Never `users`: it holds password hashes and emails.
    for table in TABLEAU_TABLES:
        conn.execute(sql_text(f'GRANT SELECT ON {table} TO "{role}"'))


ADMIN_CATALOG_VIEW = "v_admin_image_catalog"


def _create_admin_catalog_view(conn):
    """The administrator's image catalog (dashboard Image management page,
    storage/image_catalog.py): one row per FARMER image — never a
    scalability-test image — with its farmer, its latest prediction (newest
    predicted_at, ties broken by prediction_id, the same rule as the Tableau
    view) and the administrator's review decision. An image without a
    prediction or review still appears (LEFT JOINs). Times stay UTC; the
    dashboard converts them. Dropped and recreated on every init_db()."""
    conn.execute(sql_text(f"DROP VIEW IF EXISTS {ADMIN_CATALOG_VIEW}"))
    conn.execute(sql_text(f"""
        CREATE VIEW {ADMIN_CATALOG_VIEW} AS
        {LATEST_PREDICTION_CTE}
        SELECT
            i.image_id, i.user_id, i.image_path, i.original_filename, i.location, i.source,
            i.content_hash, CAST(i.status AS VARCHAR(32)) AS status, i.error_message,
            i.captured_at, i.uploaded_at, i.ingested_at, i.processed_at, i.result_published_at,
            i.gps_latitude, i.gps_longitude, i.gps_source, i.camera_make, i.camera_model,
            i.image_width, i.image_height,
            u.email AS farmer_email, u.full_name AS farmer_name,
            CAST(u.status AS VARCHAR(32)) AS farmer_status,
            u.is_deleted AS farmer_deleted,
            l.prediction_id, l.predicted_class, l.prediction_confidence, l.is_uncertain,
            l.risk_score, CAST(l.risk_level AS VARCHAR(16)) AS risk_level, l.model_version, l.predicted_at,
            r.decision AS review_decision, r.confirmed_class, r.reviewed_by, r.reviewed_at
        FROM images i
        LEFT JOIN latest l ON l.image_id = i.image_id AND l.rn = 1
        LEFT JOIN users u ON u.user_id = i.user_id
        LEFT JOIN image_reviews r ON r.image_id = i.image_id
        WHERE i.source <> '{SCALE_TEST_SOURCE}'
    """))


def init_db():
    """Create all tables if they don't exist yet, then migrate any that
    already existed before this feature added new columns. Safe to call
    repeatedly."""
    Base.metadata.create_all(engine)
    had_published_column = "result_published_at" in {
        c["name"] for c in inspect(engine).get_columns("images")}
    for table, columns in (("users", _USERS_MIGRATION_COLUMNS), ("images", _IMAGES_MIGRATION_COLUMNS),
                           ("predictions", _PREDICTIONS_MIGRATION_COLUMNS),
                           ("processing_events", _PROCESSING_EVENTS_MIGRATION_COLUMNS),
                           ("scale_test_items", _SCALE_TEST_ITEMS_MIGRATION_COLUMNS)):
        _add_missing_columns(table, columns)
    if not had_published_column:
        # Photos finished before the column existed already had their result
        # uploaded at the time: don't treat them as "not in Drive yet".
        with engine.begin() as conn:
            conn.execute(sql_text("UPDATE images SET result_published_at = processed_at "
                                  "WHERE status = 'COMPLETED' AND result_published_at IS NULL"))
    with engine.begin() as conn:
        _drop_field_feature(conn)
        _drop_system_settings(conn)
        _create_tableau_view(conn)
        _create_admin_catalog_view(conn)


