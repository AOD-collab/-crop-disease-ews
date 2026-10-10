from datetime import datetime, timezone, timedelta
from io import BytesIO

import piexif
from PIL import Image

from api.exif_utils import extract_captured_at

_LOCAL_TZ = timezone(timedelta(hours=9))  # matches EXIF_LOCAL_TZ_OFFSET_HOURS default


def _jpeg_with_exif_datetime(dt_str: str) -> bytes:
    img = Image.new("RGB", (32, 32), color="green")
    exif_dict = {"Exif": {piexif.ExifIFD.DateTimeOriginal: dt_str.encode()}}
    exif_bytes = piexif.dump(exif_dict)
    buf = BytesIO()
    img.save(buf, format="JPEG", exif=exif_bytes)
    return buf.getvalue()


def test_extracts_valid_exif_datetime():
    image_bytes = _jpeg_with_exif_datetime("2026:08:30 14:25:10")
    captured_at, source = extract_captured_at(image_bytes)
    assert captured_at == datetime(2026, 8, 30, 14, 25, 10, tzinfo=_LOCAL_TZ)
    assert captured_at.tzinfo is not None
    assert source == "exif"


def test_missing_exif_returns_none():
    img = Image.new("RGB", (32, 32), color="green")
    buf = BytesIO()
    img.save(buf, format="JPEG")  # no EXIF at all
    captured_at, source = extract_captured_at(buf.getvalue())
    assert captured_at is None
    assert source is None


def test_malformed_exif_datetime_returns_none():
    image_bytes = _jpeg_with_exif_datetime("not-a-date")
    captured_at, source = extract_captured_at(image_bytes)
    assert captured_at is None
    assert source is None


def test_never_invents_a_capture_time_for_non_image_bytes():
    captured_at, source = extract_captured_at(b"not an image at all")
    assert captured_at is None
    assert source is None
