"""
Where photo bytes live: Drive photos ("gdrive:<file id>") with a local copy
in DOWNLOADED_PHOTOS_DIR, and old local-disk photos.
"""

import re
from pathlib import Path

from config.settings import BASE_DIR, IMAGE_STORAGE_PATH, DOWNLOADED_PHOTOS_DIR


def resolve_image_path(stored: str) -> Path:
    """images.image_path -> a real path on this machine (also fixes paths
    stored on another machine). Raises FileNotFoundError if not found."""
    p = Path(stored)
    if p.exists():
        return p

    if not p.is_absolute():
        candidate = (BASE_DIR / p).resolve()
        if candidate.exists():
            return candidate

    parts = p.parts
    if "images" in parts:
        tail = Path(*parts[parts.index("images") + 1:])
        candidate = (IMAGE_STORAGE_PATH / tail).resolve()
        if candidate.exists():
            return candidate

    raise FileNotFoundError(stored)


def to_stored_path(file_path: Path) -> str:
    """The project-relative form stored in images.image_path."""
    try:
        rel = Path(file_path).resolve().relative_to(BASE_DIR.resolve())
        return rel.as_posix()
    except ValueError:
        # image storage lives outside the project (custom IMAGE_STORAGE_PATH)
        return str(Path(file_path).resolve())


# ---------------------------------------------------------------------------
# Google Drive storage (new uploads from the Android app)
# ---------------------------------------------------------------------------
# a Drive photo is stored as "gdrive:<drive_file_id>" in images.image_path
DRIVE_PATH_PREFIX = "gdrive:"


def is_drive_path(stored: str) -> bool:
    return bool(stored) and stored.startswith(DRIVE_PATH_PREFIX)


def to_drive_path(drive_file_id: str) -> str:
    return f"{DRIVE_PATH_PREFIX}{drive_file_id}"


def drive_file_id_from_path(stored: str) -> str:
    if not is_drive_path(stored):
        raise ValueError(f"Not a Google Drive image path: {stored!r}")
    return stored[len(DRIVE_PATH_PREFIX):]


def load_image_bytes(stored: str) -> bytes:
    """An image's bytes, from Drive or local disk. Raises FileNotFoundError."""
    if is_drive_path(stored):
        from storage import drive_client
        try:
            return drive_client.download_file(drive_file_id_from_path(stored))
        except drive_client.DriveError as e:
            raise FileNotFoundError(f"{stored} ({e})") from e
    return resolve_image_path(stored).read_bytes()


# ids must be plain uuids, so a path can't point outside DOWNLOADED_PHOTOS_DIR
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _local_copy_base(user_id, image_id):
    if not (user_id and image_id and _SAFE_ID_RE.match(str(user_id)) and _SAFE_ID_RE.match(str(image_id))):
        return None
    return Path(DOWNLOADED_PHOTOS_DIR) / str(user_id) / str(image_id)


def downloaded_photo_path(user_id, image_id):
    """The local copy of a Drive photo, or None."""
    base = _local_copy_base(user_id, image_id)
    if base is None:
        return None
    for ext in (".jpg", ".png"):
        path = base.with_name(base.name + ext)
        if path.is_file():
            return path
    return None


def store_downloaded_photo(user_id: str, image_id: str, original_bytes: bytes) -> Path:
    """Saves the downloaded Drive photo unchanged (via a temp file). Raises on failure."""
    ext = ".png" if original_bytes.startswith(b"\x89PNG") else ".jpg"
    base = _local_copy_base(user_id, image_id)
    if base is None:
        raise ValueError(f"unsafe user_id/image_id for a local copy: {user_id!r}/{image_id!r}")
    target = base.with_name(base.name + ext)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_bytes(original_bytes)
    tmp.replace(target)
    return target
