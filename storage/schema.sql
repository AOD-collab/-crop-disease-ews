-- Crop Disease Early Warning System — PostgreSQL schema
-- Equivalent to the SQLAlchemy models in storage/database.py. Running the
-- application (storage/init_db.py) creates these automatically via the
-- ORM; this file is provided for manual setup, review, and the capstone
-- report's "Database Design" chapter.

CREATE TABLE IF NOT EXISTS users (
    user_id         VARCHAR PRIMARY KEY,
    username        VARCHAR UNIQUE NOT NULL,
    full_name       VARCHAR,
    email           VARCHAR UNIQUE NOT NULL,
    password_hash   VARCHAR NOT NULL,
    role            VARCHAR NOT NULL CHECK (role IN ('USER', 'ADMIN')),
    status          VARCHAR NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('PENDING_VERIFICATION', 'ACTIVE', 'INACTIVE', 'SUSPENDED')),

    -- Gmail verification (registration feature)
    email_verified                BOOLEAN NOT NULL DEFAULT FALSE,
    verification_code_hash        VARCHAR,        -- hash only, never the plain code
    verification_code_expires_at  TIMESTAMPTZ,
    verification_attempts         INTEGER NOT NULL DEFAULT 0,
    last_verification_sent_at     TIMESTAMPTZ,

    -- Soft delete (admin CRUD feature)
    is_deleted      BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at      TIMESTAMPTZ,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS images (
    image_id            VARCHAR PRIMARY KEY,
    location             VARCHAR,                -- free-text place name chosen on the upload form
    user_id             VARCHAR NOT NULL REFERENCES users(user_id),
    original_filename   VARCHAR,
    image_path          VARCHAR NOT NULL,
    source               VARCHAR NOT NULL DEFAULT 'android_upload',
    content_hash         VARCHAR,                 -- SHA-256 of the uploaded bytes, for duplicate detection

    captured_at          TIMESTAMPTZ,             -- EXIF DateTimeOriginal, or NULL
    captured_at_source   VARCHAR,                 -- 'exif' | 'client_trusted' | NULL
    uploaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),  -- server-generated
    ingested_at          TIMESTAMPTZ,             -- Kafka publish/consume time
    processed_at         TIMESTAMPTZ,             -- Spark processing completion
    result_published_at  TIMESTAMPTZ,             -- result reached Drive /results (NULL: not yet)

    status               VARCHAR NOT NULL DEFAULT 'UPLOADED'
                          CHECK (status IN ('UPLOADED','PENDING_REVIEW','INGESTED','PROCESSING','COMPLETED','FAILED')),
    error_message        TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_images_location      ON images(location);
CREATE INDEX IF NOT EXISTS ix_images_user_id       ON images(user_id);
CREATE INDEX IF NOT EXISTS ix_images_captured_at   ON images(captured_at);
CREATE INDEX IF NOT EXISTS ix_images_uploaded_at   ON images(uploaded_at);
CREATE INDEX IF NOT EXISTS ix_images_content_hash  ON images(content_hash);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id            VARCHAR PRIMARY KEY,
    image_id                 VARCHAR NOT NULL REFERENCES images(image_id),

    predicted_class           VARCHAR,
    prediction_confidence     DOUBLE PRECISION,

    lesion_score               DOUBLE PRECISION,
    discoloration_pct          DOUBLE PRECISION,
    texture_irregularity       DOUBLE PRECISION,
    leaf_area_affected_pct     DOUBLE PRECISION,

    risk_score                 DOUBLE PRECISION,
    risk_level                 VARCHAR CHECK (risk_level IN ('LOW','MEDIUM','HIGH')),
    is_uncertain                BOOLEAN NOT NULL DEFAULT FALSE,

    model_version               VARCHAR,
    predicted_at                 TIMESTAMPTZ,
    processing_latency_ms        INTEGER,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_predictions_image_id         ON predictions(image_id);
CREATE INDEX IF NOT EXISTS ix_predictions_predicted_class  ON predictions(predicted_class);
CREATE INDEX IF NOT EXISTS ix_predictions_risk_level       ON predictions(risk_level);
CREATE INDEX IF NOT EXISTS ix_predictions_predicted_at     ON predictions(predicted_at);

CREATE TABLE IF NOT EXISTS processing_events (
    event_id                     VARCHAR PRIMARY KEY,
    image_id                     VARCHAR NOT NULL REFERENCES images(image_id),
    api_published_at              TIMESTAMPTZ,    -- when the API published this event to Kafka
    kafka_received_at             TIMESTAMPTZ,    -- when Spark's foreachBatch sink actually received it
    spark_started_at              TIMESTAMPTZ,
    spark_completed_at            TIMESTAMPTZ,
    prediction_started_at         TIMESTAMPTZ,
    prediction_completed_at       TIMESTAMPTZ,
    total_latency_ms              INTEGER,
    status                        VARCHAR NOT NULL DEFAULT 'INGESTED'
                                   CHECK (status IN ('UPLOADED','PENDING_REVIEW','INGESTED','PROCESSING','COMPLETED','FAILED')),
    error_message                 TEXT,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Malformed/invalid events rejected before or during Spark processing, kept
-- for traceability instead of a bare stdout print() (see spark/preprocessing.py).
CREATE TABLE IF NOT EXISTS rejected_events (
    rejected_event_id            VARCHAR PRIMARY KEY,
    image_id                     VARCHAR,        -- may not match any real image
    raw_event                    TEXT,
    reason                        TEXT NOT NULL,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Runtime-toggleable key/value settings shared across the API and dashboard
-- processes (currently just the AUTOMATIC/MANUAL image-processing mode).
CREATE TABLE IF NOT EXISTS system_settings (
    key                            VARCHAR PRIMARY KEY,
    value                          VARCHAR NOT NULL,
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Scalability test runs (scalability/run.py). Test photos get their own
-- images rows with source = 'scalability_test'; these two tables hold the
-- run and per-photo measurements, with no FK to images so they survive a
-- factory reset or a run clean-up.
CREATE TABLE IF NOT EXISTS scale_test_runs (
    run_id                         VARCHAR PRIMARY KEY,
    status                         VARCHAR NOT NULL,
    phase                          VARCHAR,
    pid                            INTEGER,
    requested_size                 INTEGER NOT NULL,
    replay_of                      VARCHAR,
    publish_results                BOOLEAN NOT NULL DEFAULT FALSE,
    drive_results_folder_id        VARCHAR,
    config_json                    TEXT,
    selection_json                 TEXT,
    summary_json                   TEXT,
    error_message                  TEXT,
    data_cleaned_at                TIMESTAMPTZ,
    started_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at                    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS scale_test_items (
    image_id                       VARCHAR PRIMARY KEY,
    run_id                         VARCHAR NOT NULL,
    source_image_id                VARCHAR NOT NULL,
    source_drive_file_id           VARCHAR NOT NULL,
    source_user_id                 VARCHAR,
    local_copy_status              VARCHAR,
    download_seconds               DOUBLE PRECISION,
    size_bytes                     INTEGER,
    status                         VARCHAR NOT NULL,
    error_message                  TEXT,
    published_at                   TIMESTAMPTZ,
    spark_received_at              TIMESTAMPTZ,
    completed_at                   TIMESTAMPTZ,
    timings_json                   TEXT,
    pipeline_seconds               DOUBLE PRECISION,
    db_write_seconds               DOUBLE PRECISION,
    drive_publish_seconds          DOUBLE PRECISION,
    predicted_class                VARCHAR
);
CREATE INDEX IF NOT EXISTS ix_scale_test_items_run_id ON scale_test_items (run_id);
CREATE INDEX IF NOT EXISTS ix_scale_test_items_run_status ON scale_test_items (run_id, status);

-- Batch aggregation output of spark/batch_analytics.py (manual objective 7's
-- batch-processing half — streaming is spark/streaming_job.py). Overwritten
-- on each batch run, one row per location per day.
CREATE TABLE IF NOT EXISTS batch_location_daily_summary (
    location                       VARCHAR NOT NULL,
    summary_date                   DATE NOT NULL,
    total_images                   INTEGER NOT NULL,
    total_healthy                  INTEGER NOT NULL,
    total_diseased                 INTEGER NOT NULL,
    avg_risk_score                 DOUBLE PRECISION,
    high_risk_count                INTEGER NOT NULL,
    generated_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (location, summary_date)
);

-- The administrator's review decision per image (Label photos page): a
-- CONFIRMED label or "discarded" — never a model prediction. No FK to
-- images, so it outlives a factory reset like the training photos do.
CREATE TABLE IF NOT EXISTS image_reviews (
    image_id                       VARCHAR PRIMARY KEY,
    decision                       VARCHAR NOT NULL,            -- labelled | discarded
    confirmed_class                VARCHAR,
    predicted_class_at_review      VARCHAR,
    reviewed_by                    VARCHAR,
    reviewed_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Destructive storage actions from the dashboard (cache clean-up, cache
-- repair, factory reset). Never cleared by the factory reset.
CREATE TABLE IF NOT EXISTS storage_audit_log (
    audit_id                       VARCHAR PRIMARY KEY,
    action                         VARCHAR NOT NULL,
    category                       VARCHAR,
    target                         VARCHAR,
    reason                         TEXT,
    outcome                        VARCHAR NOT NULL,            -- done | skipped | failed
    detail                         TEXT,
    performed_by                   VARCHAR,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_storage_audit_log_created_at ON storage_audit_log (created_at);

-- The view v_admin_image_catalog (images + farmer + latest prediction +
-- review decision, scalability-test images excluded) is created by
-- storage/database._create_admin_catalog_view() on every init_db().
