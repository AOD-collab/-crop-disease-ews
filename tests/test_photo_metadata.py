"""EXIF photo metadata (GPS / capture time / camera) read by Spark, and
Timor-Leste Time display (config/localtime.py)."""
from datetime import datetime, timezone, timedelta
from io import BytesIO

import piexif
from PIL import Image

from api.exif_utils import extract_image_metadata
from config.localtime import format_local, to_local

TLT = timezone(timedelta(hours=9))


def _dms(value: float):
    value = abs(value)
    d = int(value)
    m = int((value - d) * 60)
    s = round(((value - d) * 60 - m) * 60 * 100)
    return ((d, 1), (m, 1), (s, 100))


def jpeg_with_metadata(lat=-8.5569, lon=125.5603, alt=12.0, when="2026:09:20 08:15:00",
                       offset=None, make=b"samsung", model=b"SM-A155F", size=(64, 32),
                       orientation=None, base: bytes = None) -> bytes:
    """A JPEG with the EXIF blocks a phone writes (default: Dili)."""
    exif = {"0th": {}, "Exif": {}, "GPS": {}}
    if make:
        exif["0th"][piexif.ImageIFD.Make] = make
    if model:
        exif["0th"][piexif.ImageIFD.Model] = model
    if orientation:
        exif["0th"][piexif.ImageIFD.Orientation] = orientation
    if when:
        exif["Exif"][piexif.ExifIFD.DateTimeOriginal] = when.encode()
    if offset:
        exif["Exif"][piexif.ExifIFD.OffsetTimeOriginal] = offset.encode()
    if lat is not None:
        exif["GPS"] = {
            piexif.GPSIFD.GPSLatitudeRef: b"S" if lat < 0 else b"N",
            piexif.GPSIFD.GPSLatitude: _dms(lat),
            piexif.GPSIFD.GPSLongitudeRef: b"W" if lon < 0 else b"E",
            piexif.GPSIFD.GPSLongitude: _dms(lon),
            piexif.GPSIFD.GPSAltitudeRef: 0,
            piexif.GPSIFD.GPSAltitude: (int(alt * 10), 10),
        }
    img = Image.open(BytesIO(base)) if base else Image.new("RGB", size, "green")
    buf = BytesIO()
    img.convert("RGB").save(buf, format="JPEG", exif=piexif.dump(exif))
    return buf.getvalue()


def test_reads_gps_capture_time_and_camera():
    meta = extract_image_metadata(jpeg_with_metadata())
    assert abs(meta["gps_latitude"] - -8.5569) < 1e-4
    assert abs(meta["gps_longitude"] - 125.5603) < 1e-4
    assert meta["gps_altitude_m"] == 12.0
    assert meta["captured_at"] == datetime(2026, 9, 20, 8, 15, tzinfo=TLT)
    assert meta["captured_at_source"] == "exif"
    assert (meta["camera_make"], meta["camera_model"]) == ("samsung", "SM-A155F")
    assert (meta["image_width"], meta["image_height"]) == (64, 32)


def test_phone_written_utc_offset_wins_over_the_timor_default():
    meta = extract_image_metadata(jpeg_with_metadata(offset="+08:00"))
    assert meta["captured_at"] == datetime(2026, 9, 20, 8, 15, tzinfo=timezone(timedelta(hours=8)))
    assert meta["captured_at"].utcoffset() == timedelta(0)     # stored as UTC


def test_redacted_zero_gps_is_treated_as_missing():
    # Android zeroes the location for apps without ACCESS_MEDIA_LOCATION
    meta = extract_image_metadata(jpeg_with_metadata(lat=0.0, lon=0.0))
    assert meta["gps_latitude"] is None and meta["gps_longitude"] is None


def test_future_capture_time_from_a_wrong_camera_clock_is_ignored():
    meta = extract_image_metadata(jpeg_with_metadata(when="2099:01:01 00:00:00"))
    assert meta["captured_at"] is None


def test_sideways_photo_reports_upright_size():
    meta = extract_image_metadata(jpeg_with_metadata(orientation=6))
    assert (meta["image_width"], meta["image_height"]) == (32, 64)


def test_photo_without_exif_gives_only_its_size():
    buf = BytesIO()
    Image.new("RGB", (20, 10), "green").save(buf, format="JPEG")
    meta = extract_image_metadata(buf.getvalue())
    assert (meta["image_width"], meta["image_height"]) == (20, 10)
    assert all(meta[k] is None for k in meta if k not in ("image_width", "image_height"))


def test_garbage_bytes_never_invent_metadata():
    assert all(v is None for v in extract_image_metadata(b"nope").values())


# ------------------------------------------------------- Timor-Leste Time
def test_utc_is_shown_as_timor_leste_time():
    assert format_local(datetime(2026, 9, 28, 3, 5, tzinfo=timezone.utc)) == "28/09/2026 12:05 TLT"


def test_naive_database_value_is_treated_as_utc():
    # SQLite gives stored UTC values back without tzinfo
    assert format_local(datetime(2026, 9, 28, 20, 0)) == "29/09/2026 05:00 TLT"


def test_iso_strings_and_missing_values():
    assert to_local("2026-09-28T03:05:00Z").hour == 12
    assert format_local(None) == ""
    import pandas as pd
    assert format_local(pd.NaT) == ""
