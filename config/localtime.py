"""
Timor-Leste Time (TLT, UTC+9) for DISPLAY only.

Every timestamp is stored in UTC (storage/database.now_utc). SQLite hands
them back naive, so a naive value is treated as UTC, never as local time.
Timor-Leste has no daylight saving, so a fixed UTC+9 offset is exact.
"""
from datetime import datetime, timedelta, timezone

from config.settings import LOCAL_TZ_OFFSET_HOURS, LOCAL_TZ_LABEL, LOCAL_DATETIME_FORMAT

LOCAL_TZ = timezone(timedelta(hours=LOCAL_TZ_OFFSET_HOURS), LOCAL_TZ_LABEL)


def to_local(dt):
    """UTC (aware or naive-UTC) datetime -> aware Timor-Leste datetime."""
    if dt is None or dt != dt:          # None, or pandas NaT (never equal to itself)
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ)


def format_local(dt, with_seconds: bool = False) -> str:
    """e.g. "28/09/2026 14:05 TLT"; "" for a missing value."""
    local = to_local(dt)
    if local is None:
        return ""
    fmt = LOCAL_DATETIME_FORMAT + (":%S" if with_seconds else "")
    return f"{local.strftime(fmt)} {LOCAL_TZ_LABEL}"


def now_local():
    return datetime.now(LOCAL_TZ)


def series_to_local(series):
    """pandas datetime Series (UTC or naive-UTC) -> Timor-Leste tz-aware.
    Comparisons and differences are unchanged; only the display is local."""
    import pandas as pd
    s = pd.to_datetime(series, utc=True, errors="coerce")
    return s.dt.tz_convert(LOCAL_TZ)
