"""
Persistent image storage per DIT manual section 12.

- Legacy local-disk photos (storage/images/YYYY/MM/DD/<uuid>.<ext>, written
  by the removed REST API) are still found through resolve_image_path().
- Google Drive photos are recorded as "gdrive:<file id>" and downloaded on
  demand; Spark keeps a permanent byte-for-byte copy of each one under
  DOWNLOADED_PHOTOS_DIR.
"""

import re
from pathlib import Path

from config.settings import BASE_DIR, IMAGE_STORAGE_PATH, DOWNLOADED_PHOTOS_DIR


def resolve_image_path(stored: str) -> Path:
    """Turn whatever is in images.image_path into a real path on THIS host.

    Historically the upload code stored an absolute path, so a DB carried across
    machines / moved project folders (e.g. OneDrive -> A:\\final) pointed at
    files that no longer exist there even though the bytes came along under
    storage/images/. This resolver is the single place that copes with it:

      1. stored path exists as-is                 -> use it
      2. relative path                            -> resolve under BASE_DIR
      3. stale absolute path with an `images/`
         segment                                  -> rebase that tail under
                                                     the current
                                                     IMAGE_STORAGE_PATH
    Raises FileNotFoundError only when none of those find the file.
    """
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
    """The portable form written to images.image_path: relative to the
    project root with forward slashes, so it survives a host/folder move."""
    try:
        rel = Path(file_path).resolve().relative_to(BASE_DIR.resolve())
        return rel.as_posix()
    except ValueError:
        # image storage lives outside the project (custom IMAGE_STORAGE_PATH)
        return str(Path(file_path).resolve())


# ---------------------------------------------------------------------------
# Google Drive storage (new uploads from the Android app)
# ---------------------------------------------------------------------------
# A Drive-stored image is recorded in the SAME images.image_path column as
# "gdrive:<drive_file_id>", so no database schema change is needed and old
# local-disk rows keep working exactly as before.
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
    """Reads an image's bytes wherever it lives: Google Drive for
    "gdrive:<id>" paths, local disk (resolve_image_path) otherwise.
    Raises FileNotFoundError if the file can't be found in either place."""
    if is_drive_path(stored):
        from storage import drive_client
        try:
            return drive_client.download_file(drive_file_id_from_path(stored))
        except drive_client.DriveError as e:
            raise FileNotFoundError(f"{stored} ({e})") from e
    return resolve_image_path(stored).read_bytes()


# user_id / image_id are uuids; anything else (separators, "..") could point
# outside DOWNLOADED_PHOTOS_DIR. A plain name check, not Path.resolve():
# resolve() on Windows can return a different form of the same folder while
# another thread is creating it, which made valid copies look unsafe.
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _local_copy_base(user_id, image_id):
    if not (user_id and image_id and _SAFE_ID_RE.match(str(user_id)) and _SAFE_ID_RE.match(str(image_id))):
        return None
    return Path(DOWNLOADED_PHOTOS_DIR) / str(user_id) / str(image_id)


def downloaded_photo_path(user_id, image_id):
    """The saved local copy of a Drive photo,
    DOWNLOADED_PHOTOS_DIR/<user_id>/<image_id>.jpg (or .png), or None when
    there is none (or the ids aren't safe file names)."""
    base = _local_copy_base(user_id, image_id)
    if base is None:
        return None
    for ext in (".jpg", ".png"):
        path = base.with_name(base.name + ext)
        if path.is_file():
            return path
    return None


def store_downloaded_photo(user_id: str, image_id: str, original_bytes: bytes) -> Path:
    """Writes the bytes downloaded from Drive to
    DOWNLOADED_PHOTOS_DIR/<user_id>/<image_id>.jpg (.png for PNGs), unchanged
    (EXIF included), via a temp file so a half-written copy never looks
    valid. Nothing in the app deletes these files. Raises on failure."""
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
