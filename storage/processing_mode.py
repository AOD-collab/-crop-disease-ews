"""
The image-processing mode (AUTOMATIC / MANUAL), kept in a small JSON file
so the Drive watcher can read it without a database connection.

    AUTOMATIC  the Drive watcher publishes every new upload to Kafka at once
    MANUAL     the watcher publishes nothing; the dashboard's Pending Review
               page lists the Drive photos that have no database row yet and
               the administrator sends them with "Process"

The dashboard writes the file, the watcher reads it on every check. It used
to be a row in system_settings; migrate_from_database() copies that value
once (storage/init_db.py runs it at start-up).
"""

import enum
import json
import os
from pathlib import Path

from config.settings import BASE_DIR

MODE_FILE = Path(os.getenv("PROCESSING_MODE_FILE", str(BASE_DIR / "storage" / "processing_mode.json")))


class ProcessingMode(str, enum.Enum):
    # AUTOMATIC: an upload is published to Kafka the moment the watcher sees
    # it and Spark processes it immediately.
    AUTOMATIC = "AUTOMATIC"
    # MANUAL: nothing is published until an admin clicks "Process".
    MANUAL = "MANUAL"


def get_processing_mode() -> ProcessingMode:
    """Current mode. A missing or unreadable file means AUTOMATIC."""
    try:
        return ProcessingMode(json.loads(MODE_FILE.read_text(encoding="utf-8"))["mode"])
    except (OSError, ValueError, KeyError, TypeError):
        return ProcessingMode.AUTOMATIC


def set_processing_mode(mode) -> ProcessingMode:
    """Persist the mode (a ProcessingMode or its string value; ValueError for
    anything else). Written to a temp file and renamed, so a reader never
    sees half a file."""
    mode = ProcessingMode(mode)
    MODE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = MODE_FILE.with_name(MODE_FILE.name + ".tmp")
    tmp.write_text(json.dumps({"mode": mode.value}), encoding="utf-8")
    tmp.replace(MODE_FILE)
    return mode


def migrate_from_database() -> None:
    """One-time: when there is no mode file yet, take the value the old
    system_settings row holds, so a system left in MANUAL stays in MANUAL."""
    if MODE_FILE.exists():
        return
    from storage.database import SessionLocal, SystemSetting
    try:
        with SessionLocal() as s:
            row = s.get(SystemSetting, "processing_mode")
            value = row.value if row is not None else None
    except Exception:  # noqa: BLE001 - no table / no database: AUTOMATIC
        value = None
    try:
        set_processing_mode(value or ProcessingMode.AUTOMATIC)
    except ValueError:
        set_processing_mode(ProcessingMode.AUTOMATIC)
