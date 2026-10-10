"""Photo location: the phone position the Android app sends for in-app
camera photos (appProperties gps_*) is stored by the Drive watcher, and
Spark's EXIF stage prefers the photo's own GPS tags but never blanks the
phone position when the photo has none."""

import uuid

import pytest

from kafka import drive_watcher
from spark import preprocessing
from storage.database import (
    Base, engine, init_db, SessionLocal, Image, ProcessingMode, set_processing_mode,
)


@pytest.fixture(autouse=True)
def fresh_db():
    Base.metadata.drop_all(engine)
    init_db()
    with SessionLocal() as s:
        set_processing_mode(s, ProcessingMode.MANUAL)  # hold uploads: no Kafka needed
    yield
    Base.metadata.drop_all(engine)


@pytest.mark.parametrize("props, expected", [
    ({"gps_lat": "-8.556100", "gps_lon": "125.578300"}, (-8.5561, 125.5783)),
    ({}, None),
    ({"gps_lat": "-8.5"}, None),                       # half a coordinate
    ({"gps_lat": "abc", "gps_lon": "125.5"}, None),
    ({"gps_lat": "-95", "gps_lon": "125.5"}, None),    # out of range
    ({"gps_lat": "0", "gps_lon": "0"}, None),          # "no fix" placeholder
])
def test_device_gps_parsing(props, expected):
    assert drive_watcher._device_gps(props) == expected


def _drive_file(props_extra):
    image_id = str(uuid.uuid4())
    return image_id, {
        "id": "driveFile" + image_id[:8],
        "name": f"{image_id}.jpg",
        "mimeType": "image/jpeg",
        "size": "2048",
        "createdTime": "2026-10-01T02:00:00Z",
        "owners": [{"emailAddress": "farmer@example.com", "displayName": "Farmer"}],
        "appProperties": {"image_id": image_id, "location": "Dili", **props_extra},
    }


def test_watcher_stores_device_gps():
    image_id, meta = _drive_file({"gps_lat": "-8.556100", "gps_lon": "125.578300", "gps_acc_m": "12"})
    assert drive_watcher.register_drive_file(meta, folder_email="farmer@example.com")["status"] == "pending_review"
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert (img.gps_latitude, img.gps_longitude, img.gps_source) == (-8.5561, 125.5783, "device")
        assert img.location == "Dili"


def test_watcher_without_gps_leaves_it_empty():
    image_id, meta = _drive_file({})
    drive_watcher.register_drive_file(meta, folder_email="farmer@example.com")
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert img.gps_latitude is None and img.gps_source is None


def _exif(lat=None, lon=None):
    return {"gps_latitude": lat, "gps_longitude": lon, "gps_altitude_m": 10.0 if lat else None,
            "camera_make": "samsung", "camera_model": "SM-A145F", "image_width": 4000,
            "image_height": 3000, "captured_at": None, "captured_at_source": None}


def _registered(props_extra):
    image_id, meta = _drive_file(props_extra)
    drive_watcher.register_drive_file(meta, folder_email="farmer@example.com")
    return image_id


def test_exif_gps_wins_over_device(monkeypatch):
    image_id = _registered({"gps_lat": "-8.5561", "gps_lon": "125.5783"})
    monkeypatch.setattr(preprocessing, "extract_image_metadata", lambda b: _exif(-9.31, 125.256))
    preprocessing.extract_metadata({"image_id": image_id}, b"")
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert (img.gps_latitude, img.gps_longitude, img.gps_source) == (-9.31, 125.256, "exif")
        assert img.camera_make == "samsung"


def test_device_gps_kept_when_photo_has_no_gps_tags(monkeypatch):
    image_id = _registered({"gps_lat": "-8.5561", "gps_lon": "125.5783"})
    monkeypatch.setattr(preprocessing, "extract_image_metadata", lambda b: _exif())
    preprocessing.extract_metadata({"image_id": image_id}, b"")
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        assert (img.gps_latitude, img.gps_longitude, img.gps_source) == (-8.5561, 125.5783, "device")
        assert img.camera_model == "SM-A145F"  # other metadata still stored
