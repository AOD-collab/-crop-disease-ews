"""
Extracts the real camera capture time from JPEG EXIF metadata
(DateTimeOriginal), per DIT manual section 7.

If there is no valid EXIF timestamp, returns (None, None) — the caller
stores captured_at = NULL. We never invent a capture time and never
silently substitute the upload time (section 7/42).
"""

from datetime import datetime, timezone, timedelta
from io import BytesIO
from typing import Optional, Tuple

from PIL import Image, ExifTags

from config.settings import EXIF_LOCAL_TZ_OFFSET_HOURS

_LOCAL_TZ = timezone(timedelta(hours=EXIF_LOCAL_TZ_OFFSET_HOURS))

_DATETIME_ORIGINAL_TAG = next(
    (tag_id for tag_id, name in ExifTags.TAGS.items() if name == "DateTimeOriginal"),
    36867,  # standard EXIF tag id for DateTimeOriginal, used as a fallback
)


def _captured_at(exif) -> Tuple[Optional[datetime], Optional[str]]:
    """(captured_at, "exif") from a photo's EXIF block, or (None, None) if no
    valid EXIF DateTimeOriginal is present."""
    if not exif:
        return None, None

    # DateTimeOriginal (36867) lives in the "Exif" sub-IFD (pointer tag
    # 0x8769 in the base IFD), not the base IFD itself — check there first,
    # falling back to the base IFD for cameras/tools that write it flat.
    raw_value = None
    try:
        sub_ifd = exif.get_ifd(_EXIF_IFD)
        raw_value = sub_ifd.get(_DATETIME_ORIGINAL_TAG)
    except Exception:
        pass
    if not raw_value:
        raw_value = exif.get(_DATETIME_ORIGINAL_TAG)
    if not raw_value:
        return None, None

    # EXIF datetime format: "YYYY:MM:DD HH:MM:SS", with no timezone info at
    # all — it's a local wall-clock reading, so attach the configured local
    # offset (Timor-Leste, UTC+9 by default) rather than let it be stored as
    # if it were already UTC.
    try:
        dt = datetime.strptime(str(raw_value).strip(), "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None, None

    # Returned in UTC (same instant): SQLite drops a tz offset on write, so
    # a +09:00 value stored as-is came back 9 hours off.
    dt = dt.replace(tzinfo=_LOCAL_TZ).astimezone(timezone.utc)
    return dt, "exif"


# ---------------------------------------------------------------------------
# Full photo metadata (run by Spark, spark/preprocessing.extract_metadata)
# ---------------------------------------------------------------------------
_EXIF_IFD = 0x8769
_GPS_IFD = 0x8825
_TAG_MAKE, _TAG_MODEL = 0x010F, 0x0110
_TAG_OFFSET_TIME_ORIGINAL = 0x9011          # e.g. "+09:00" (newer phones write it)
_GPS_LAT_REF, _GPS_LAT, _GPS_LON_REF, _GPS_LON = 1, 2, 3, 4
_GPS_ALT_REF, _GPS_ALT = 5, 6


def _rational(value) -> float:
    if isinstance(value, tuple) and len(value) == 2:      # (num, den) from old writers
        return float(value[0]) / float(value[1]) if value[1] else 0.0
    return float(value)


def _dms_to_degrees(dms, ref) -> Optional[float]:
    try:
        d, m, s = (_rational(v) for v in dms)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    deg = d + m / 60.0 + s / 3600.0
    if str(ref).strip().upper() in ("S", "W"):
        deg = -deg
    return deg


def _clean_text(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", "ignore")
    text = str(value).replace("\x00", "").strip()
    return text[:100] or None


def extract_gps(exif) -> dict:
    """(latitude, longitude, altitude_m) from the EXIF GPS block, or Nones.
    Android redacts a photo's location for apps without
    ACCESS_MEDIA_LOCATION by zeroing it, so exactly 0,0 counts as missing,
    and anything outside the valid lat/long range is dropped."""
    out = {"gps_latitude": None, "gps_longitude": None, "gps_altitude_m": None}
    try:
        gps = exif.get_ifd(_GPS_IFD)
    except Exception:
        return out
    if not gps:
        return out
    lat = _dms_to_degrees(gps.get(_GPS_LAT), gps.get(_GPS_LAT_REF, "N")) if gps.get(_GPS_LAT) else None
    lon = _dms_to_degrees(gps.get(_GPS_LON), gps.get(_GPS_LON_REF, "E")) if gps.get(_GPS_LON) else None
    if lat is None or lon is None or not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return out
    if abs(lat) < 1e-9 and abs(lon) < 1e-9:
        return out
    out["gps_latitude"], out["gps_longitude"] = round(lat, 6), round(lon, 6)
    if gps.get(_GPS_ALT) is not None:
        try:
            alt = _rational(gps.get(_GPS_ALT))
            ref = gps.get(_GPS_ALT_REF, 0)
            if ref in (1, b"\x01"):                        # below sea level
                alt = -alt
            out["gps_altitude_m"] = round(alt, 1)
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    return out


def extract_image_metadata(image_bytes: bytes) -> dict:
    """Everything useful the photo file itself says about where/when/how it
    was taken. Missing values are None — never guessed:
        captured_at, captured_at_source   EXIF DateTimeOriginal read with the
                                          phone's OffsetTimeOriginal when it
                                          wrote one, else as Timor-Leste
                                          time; returned in UTC
        gps_latitude, gps_longitude,      EXIF GPS block (camera location
        gps_altitude_m                    tagging must be on)
        camera_make, camera_model         e.g. "samsung", "SM-A155F"
        image_width, image_height         pixel size, EXIF-upright
    Must run on the ORIGINAL bytes: background cleaning re-encodes the photo
    and drops its EXIF."""
    meta = {
        "captured_at": None, "captured_at_source": None,
        "gps_latitude": None, "gps_longitude": None, "gps_altitude_m": None,
        "camera_make": None, "camera_model": None,
        "image_width": None, "image_height": None,
    }
    try:
        img = Image.open(BytesIO(image_bytes))
        exif = img.getexif()
        w, h = img.size
    except Exception:
        return meta
    orientation = exif.get(0x0112) if exif else None
    if orientation in (5, 6, 7, 8):                       # stored sideways
        w, h = h, w
    meta["image_width"], meta["image_height"] = w, h
    if not exif:
        return meta

    meta["camera_make"] = _clean_text(exif.get(_TAG_MAKE))
    meta["camera_model"] = _clean_text(exif.get(_TAG_MODEL))
    meta.update(extract_gps(exif))

    captured_at, source = _captured_at(exif)
    if captured_at is not None:
        try:
            offset = _clean_text(exif.get_ifd(_EXIF_IFD).get(_TAG_OFFSET_TIME_ORIGINAL))
        except Exception:
            offset = None
        if offset:
            try:
                tz = datetime.strptime(offset, "%z").tzinfo
                wall_clock = captured_at.astimezone(_LOCAL_TZ).replace(tzinfo=None)
                captured_at = wall_clock.replace(tzinfo=tz).astimezone(timezone.utc)
            except ValueError:
                pass
        # a camera clock set in the future is wrong, not a capture time
        if captured_at <= datetime.now(timezone.utc) + timedelta(minutes=5):
            meta["captured_at"], meta["captured_at_source"] = captured_at, source
    return meta
