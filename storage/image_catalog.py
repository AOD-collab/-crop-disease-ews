"""
The administrator's image catalog: PostgreSQL is the source of truth for
every farmer image's metadata, prediction, processing status and review
decision. Everything here reads the view v_admin_image_catalog
(storage/database._create_admin_catalog_view) through SQLAlchemy Core, so
every filter value is a bound parameter â€” no SQL is built from user input.

Scalability-test images are not in the view, so they never appear here.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, Float, MetaData, String, Table, Text, and_, func, or_, select, true,
)

from config.settings import LOCAL_TZ_OFFSET_HOURS
from storage.database import ADMIN_CATALOG_VIEW, SessionLocal, engine

catalog = Table(
    ADMIN_CATALOG_VIEW, MetaData(),
    Column("image_id", String, primary_key=True), Column("user_id", String), Column("image_path", String),
    Column("original_filename", String), Column("location", String), Column("source", String),
    Column("content_hash", String), Column("status", String), Column("error_message", Text),
    Column("captured_at", DateTime(timezone=True)), Column("uploaded_at", DateTime(timezone=True)),
    Column("ingested_at", DateTime(timezone=True)), Column("processed_at", DateTime(timezone=True)),
    Column("result_published_at", DateTime(timezone=True)),
    Column("gps_latitude", Float), Column("gps_longitude", Float), Column("gps_source", String),
    Column("camera_make", String), Column("camera_model", String),
    Column("image_width", Float), Column("image_height", Float),
    Column("farmer_email", String), Column("farmer_name", String), Column("farmer_status", String),
    Column("farmer_deleted", Boolean),
    Column("prediction_id", String), Column("predicted_class", String), Column("prediction_confidence", Float),
    Column("is_uncertain", Boolean), Column("risk_score", Float), Column("risk_level", String),
    Column("model_version", String), Column("predicted_at", DateTime(timezone=True)),
    Column("review_decision", String), Column("confirmed_class", String), Column("reviewed_by", String),
    Column("reviewed_at", DateTime(timezone=True)),
)

PROCESSING_STATUSES = ("INGESTED", "PROCESSING")
NO_PREDICTION = "(no prediction)"
NO_RISK = "(no risk level)"


@dataclass
class CatalogFilters:
    """Every field is optional; set ones are combined with AND."""
    image_id: str = ""                        # full id or its beginning
    farmer: str = ""                          # user_id
    location: str = ""
    uploaded_from: date | None = None         # Timor-Leste calendar dates, inclusive
    uploaded_to: date | None = None
    statuses: list = field(default_factory=list)
    predicted_classes: list = field(default_factory=list)   # may include NO_PREDICTION
    risk_levels: list = field(default_factory=list)         # may include NO_RISK


def _local_day_start_utc(day: date) -> datetime:
    """Midnight Timor-Leste Time of `day`, as an aware UTC datetime."""
    tlt = timezone(timedelta(hours=LOCAL_TZ_OFFSET_HOURS))
    return datetime.combine(day, time.min, tzinfo=tlt).astimezone(timezone.utc)


def _db_time(dt: datetime):
    """SQLite stores naive UTC text, PostgreSQL timestamptz: compare in kind."""
    return dt.replace(tzinfo=None) if engine.dialect.name == "sqlite" else dt


def _conditions(f: CatalogFilters) -> list:
    c = catalog.c
    out = []
    if f.image_id.strip():
        out.append(c.image_id.like(f"{_escape_like(f.image_id.strip())}%", escape="\\"))
    if f.farmer:
        out.append(c.user_id == f.farmer)
    if f.location:
        out.append(c.location == f.location)
    if f.uploaded_from:
        out.append(c.uploaded_at >= _db_time(_local_day_start_utc(f.uploaded_from)))
    if f.uploaded_to:
        out.append(c.uploaded_at < _db_time(_local_day_start_utc(f.uploaded_to + timedelta(days=1))))
    if f.statuses:
        out.append(c.status.in_(list(f.statuses)))
    if f.predicted_classes:
        out.append(_in_or_none(c.predicted_class, f.predicted_classes, NO_PREDICTION, c.prediction_id))
    if f.risk_levels:
        out.append(_in_or_none(c.risk_level, f.risk_levels, NO_RISK, c.risk_level))
    return out


def _in_or_none(column, values, none_choice: str, none_column):
    """column IN values — and, when `none_choice` ("no prediction" / "no
    risk") is among them, rows whose none_column IS NULL too."""
    real = [x for x in values if x != none_choice]
    parts = [column.in_(real)] if real else []
    if none_choice in values:
        parts.append(none_column.is_(None))
    return or_(*parts)


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _rows(stmt) -> list[dict]:
    with SessionLocal() as s:
        return [dict(r._mapping) for r in s.execute(stmt)]


def query(filters: CatalogFilters | None = None, limit: int | None = None, offset: int = 0) -> list[dict]:
    """Matching images, newest upload first; image_id breaks ties so the
    order is the same on every page load."""
    filters = filters or CatalogFilters()
    stmt = (select(catalog).where(and_(true(), *_conditions(filters)))
            .order_by(catalog.c.uploaded_at.desc(), catalog.c.image_id))
    if limit is not None:
        stmt = stmt.limit(limit).offset(offset)
    return _rows(stmt)


def count(filters: CatalogFilters | None = None) -> int:
    filters = filters or CatalogFilters()
    with SessionLocal() as s:
        return s.execute(select(func.count()).select_from(catalog)
                         .where(and_(true(), *_conditions(filters)))).scalar_one()


def get(image_id: str) -> dict | None:
    rows = _rows(select(catalog).where(catalog.c.image_id == image_id))
    return rows[0] if rows else None


def same_content(image_id: str, content_hash: str | None) -> list[dict]:
    """Other farmer images with identical bytes (same SHA-256): the same
    photo uploaded again, possibly by another farmer. Reported, never merged."""
    if not content_hash:
        return []
    return _rows(select(catalog.c.image_id, catalog.c.user_id, catalog.c.farmer_email, catalog.c.uploaded_at,
                        catalog.c.status)
                 .where(catalog.c.content_hash == content_hash, catalog.c.image_id != image_id)
                 .order_by(catalog.c.uploaded_at, catalog.c.image_id))


def summary() -> dict:
    """Database record counts (not files)."""
    c = catalog.c
    with SessionLocal() as s:
        by_status = dict(s.execute(select(c.status, func.count()).group_by(c.status)).all())
        with_prediction = s.execute(select(func.count()).select_from(catalog)
                                    .where(c.prediction_id.isnot(None))).scalar_one()
    by_status = {getattr(k, "value", k): v for k, v in by_status.items()}
    return {
        "total": sum(by_status.values()),
        "completed": by_status.get("COMPLETED", 0),
        "processing": sum(by_status.get(k, 0) for k in PROCESSING_STATUSES),
        "failed": by_status.get("FAILED", 0),
        "with_prediction": with_prediction,
    }


def options() -> dict:
    """Values that exist, for the filter drop-downs."""
    c = catalog.c
    with SessionLocal() as s:
        farmers = s.execute(select(c.user_id, c.farmer_email).distinct()
                            .order_by(c.farmer_email)).all()
        locations = [r[0] for r in s.execute(select(c.location).where(c.location.isnot(None))
                                             .distinct().order_by(c.location))]
        classes = [r[0] for r in s.execute(select(c.predicted_class).where(c.predicted_class.isnot(None))
                                           .distinct().order_by(c.predicted_class))]
        statuses = [getattr(r[0], "value", r[0]) for r in s.execute(select(c.status).distinct())]
    return {"farmers": {uid: email or uid for uid, email in farmers}, "locations": locations,
            "classes": classes, "statuses": sorted(statuses)}


def all_ids() -> dict:
    """{image_id: (user_id, content_hash, status)} for every farmer image â€”
    what the cache reconciliation compares the files against."""
    c = catalog.c
    return {r["image_id"]: (r["user_id"], r["content_hash"], getattr(r["status"], "value", r["status"]))
            for r in _rows(select(c.image_id, c.user_id, c.content_hash, c.status))}
