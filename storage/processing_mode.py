"""
AUTOMATIC / MANUAL processing mode, kept in a JSON file so the Drive watcher
can read it without a database. MANUAL: photos wait on Pending Review.
"""

import enum
import json
import os
from pathlib import Path

from config.settings import BASE_DIR

MODE_FILE = Path(os.getenv("PROCESSING_MODE_FILE", str(BASE_DIR / "storage" / "processing_mode.json")))


class ProcessingMode(str, enum.Enum):
    # every upload is processed at once
    AUTOMATIC = "AUTOMATIC"
    # nothing is processed until an admin clicks "Process"
    MANUAL = "MANUAL"


def get_processing_mode() -> ProcessingMode:
    """Current mode. A missing or unreadable file means AUTOMATIC."""
    try:
        return ProcessingMode(json.loads(MODE_FILE.read_text(encoding="utf-8"))["mode"])
    except (OSError, ValueError, KeyError, TypeError):
        return ProcessingMode.AUTOMATIC


def set_processing_mode(mode) -> ProcessingMode:
    """Saves the mode (temp file + rename, so a reader never sees half a file)."""
    mode = ProcessingMode(mode)
    MODE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = MODE_FILE.with_name(MODE_FILE.name + ".tmp")
    tmp.write_text(json.dumps({"mode": mode.value}), encoding="utf-8")
    tmp.replace(MODE_FILE)
    return mode
