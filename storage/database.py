"""
Database layer: SQLAlchemy models, engine and session. DATABASE_URL picks
PostgreSQL (normal) or SQLite (local tests); nothing else changes.
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
    # PENDING_VERIFICATION: only on old rows (self-registration was removed)
    PENDING_VERIFICATION = "PENDING_VERIFICATION"
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUSPENDED = "SUSPENDED"


class RiskLevel(str, enum.Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class ProcessingStatus(str, enum.Enum):
    # a row exists only once Spark registered the photo
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
    status = Column(Enum(UserStatus), nullable=False, default=UserStatus.ACTIVE)

    # --- Gmail verification (old self-registration; columns kept) ---
    email_verified = Column(Boolean, nullable=False, default=False)
    verification_code_hash = Column(String, nullable=True)
    verification_code_expires_at = Column(DateTime(timezone=True), nullable=True)
    verification_attempts = Column(Integer, nullable=False, default=0)
    last_verification_sent_at = Column(DateTime(timezone=True), nullable=True)

    # --- soft delete ---
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
    # free-text place name from the upload form (e.g. a municipality)
    location = Column(String, nullable=True)
    user_id = Column(String, ForeignKey("users.user_id"), nullable=False)
    original_filename = Column(String, nullable=True)
    image_path = Column(String, nullable=False)   # storage/images/YYYY/MM/DD/<uuid>.jpg
    source = Column(String, nullable=False, default="android_upload")
    # SHA-256 of the photo bytes, to spot the same photo sent twice
    content_hash = Column(String, nullable=True)

    # --- timestamps ---
    captured_at = Column(DateTime(timezone=True), nullable=True)   # EXIF DateTimeOriginal, or NULL
    captured_at_source = Column(String, nullable=True)             # "exif" | "client_trusted" | None
    uploaded_at = Column(DateTime(timezone=True), nullable=False, default=now_utc)  # server-generated, never client-trusted
    ingested_at = Column(DateTime(timezone=True), nullable=True)   # Kafka publish/consume time
    processed_at = Column(DateTime(timezone=True), nullable=True)  # Spark processing completion
    # when the result reached the farmer's Drive; NULL = the phone doesn't have it yet
    result_published_at = Column(DateTime(timezone=True), nullable=True)

    # --- EXIF metadata (spark/preprocessing.extract_metadata); NULL when missing ---
    gps_latitude = Column(Float, nullable=True)
    gps_longitude = Column(Float, nullable=True)
    gps_altitude_m = Column(Float, nullable=True)
    # "exif" (the photo's GPS, preferred) or "device" (the phone's position)
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

    # --- CNN classification ---
    predicted_class = Column(String, nullable=True)
    prediction_confidence = Column(Float, nullable=True)
    # confidence below the threshold: predicted_class is "uncertain", risk capped below HIGH
    is_uncertain = Column(Boolean, nullable=False, default=False)

    # --- image features (now only lesion_score is filled) ---
    lesion_score = Column(Float, nullable=True)
    discoloration_pct = Column(Float, nullable=True)
    texture_irregularity = Column(Float, nullable=True)
    leaf_area_affected_pct = Column(Float, nullable=True)

    # --- risk level (ml/risk.py) ---
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
# processing_events: per-photo pipeline timings
# ----------------------------------------------------------------------
class ProcessingEvent(Base):
    __tablename__ = "processing_events"

    event_id = Column(String, primary_key=True, default=new_uuid)
    image_id = Column(String, ForeignKey("images.image_id"), nullable=False)
    api_published_at = Column(DateTime(timezone=True), nullable=True)   # sent to Kafka
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
# rejected_events: Kafka events too broken to process (dead-letter table)
# ----------------------------------------------------------------------
class RejectedEvent(Base):
    __tablename__ = "rejected_events"

    rejected_event_id = Column(String, primary_key=True, default=new_uuid)
    image_id = Column(String, nullable=True)  # may not correspond to a real Image row
    raw_event = Column(Text, nullable=True)   # JSON-serialized event payload, best-effort
    reason = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), default=now_utc, nullable=False)


# ----------------------------------------------------------------------
# scalability tests: test photos get their own rows; no foreign keys, so the
# results survive a factory reset
# ----------------------------------------------------------------------
SCALE_TEST_SOURCE = "scalability_test"
SCALE_TEST_USERNAME = "scalability-test"


class ScaleTestRun(Base):
    __tablename__ = "scale_test_runs"

    run_id = Column(String, primary_key=True)
    # PREPARING -> RUNNING -> COMPLETED | INCOMPLETE | FAILED | ABORTED
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
    # the CNN result, kept only here (never sent to a farmer)
    predicted_class = Column(String, nullable=True)
    prediction_confidence = Column(Float, nullable=True)
    is_uncertain = Column(Boolean, nullable=True)
    risk_level = Column(String, nullable=True)
    # where the source photo was taken
    location = Column(String, nullable=True)
    gps_latitude = Column(Float, nullable=True)
    gps_longitude = Column(Float, nullable=True)

    __table_args__ = (Index("ix_scale_test_items_run_status", "run_id", "status"),)


# ----------------------------------------------------------------------
# image_reviews: the admin's label (or "discarded") from the Label photos page;
# kept apart from predictions and survives a factory reset
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
# storage_audit_log: every destructive storage action from the dashboard
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
# Analytics tables, filled only by Spark (spark/batch_analytics.py)
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


# scalability-test figures, per test run (not shared with Tableau)
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
# pool_pre_ping replaces connections that died when PostgreSQL restarted
engine = create_engine(DATABASE_URL, connect_args=_connect_args, future=True,
                       pool_pre_ping=not DATABASE_URL.startswith("sqlite"))

if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _sqlite_wal(dbapi_conn, _record):
        # WAL: outside readers don't block writers
        dbapi_conn.execute("PRAGMA journal_mode=WAL")

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


# columns added after the first release (create_all doesn't add columns)
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
    """One-time migration: drops the removed fields tables and Image.field_id."""
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
    """Drops system_settings (the mode now lives in processing_mode.json)."""
    conn.execute(sql_text("DROP TABLE IF EXISTS system_settings"))


# each image's latest prediction (newest predicted_at, then prediction_id) as rn = 1
LATEST_PREDICTION_CTE = """WITH latest AS (
            SELECT p.*, ROW_NUMBER() OVER (PARTITION BY p.image_id
                       ORDER BY p.predicted_at DESC NULLS LAST, p.prediction_id DESC) AS rn
            FROM predictions p
        )"""


TABLEAU_VIEW = "v_tableau_images"
TABLEAU_TABLES = ("images", "predictions") + ANALYTICS_TABLES


def _create_tableau_view(conn):
    """Tableau's read-only view: one row per image with its latest prediction,
    times in Timor-Leste Time. Recreated by every init_db()."""
    is_postgres = engine.dialect.name == "postgresql"

    def local(col):
        if is_postgres:
            # UTC -> Timor-Leste wall time, whatever Tableau's time zone
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
    """Creates or updates the read-only Tableau login and its grants."""
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
    # raw tables too, never `users` (password hashes, emails)
    for table in TABLEAU_TABLES:
        conn.execute(sql_text(f'GRANT SELECT ON {table} TO "{role}"'))


ADMIN_CATALOG_VIEW = "v_admin_image_catalog"


def _create_admin_catalog_view(conn):
    """The admin image catalog: one row per farmer image with its farmer,
    latest prediction and review decision. Recreated by every init_db()."""
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
    """Creates missing tables and columns. Safe to run again."""
    Base.metadata.create_all(engine)
    had_published_column = "result_published_at" in {
        c["name"] for c in inspect(engine).get_columns("images")}
    for table, columns in (("users", _USERS_MIGRATION_COLUMNS), ("images", _IMAGES_MIGRATION_COLUMNS),
                           ("predictions", _PREDICTIONS_MIGRATION_COLUMNS),
                           ("processing_events", _PROCESSING_EVENTS_MIGRATION_COLUMNS),
                           ("scale_test_items", _SCALE_TEST_ITEMS_MIGRATION_COLUMNS)):
        _add_missing_columns(table, columns)
    if not had_published_column:
        # photos finished before this column existed already got their result
        with engine.begin() as conn:
            conn.execute(sql_text("UPDATE images SET result_published_at = processed_at "
                                  "WHERE status = 'COMPLETED' AND result_published_at IS NULL"))
    with engine.begin() as conn:
        _drop_field_feature(conn)
        _drop_system_settings(conn)
        _create_tableau_view(conn)
        _create_admin_catalog_view(conn)


