"""
Farmer photo -> data/review/ -> admin labels it on the "Label photos" page
-> data/train/<class>/ (dashboard/labelling_page.py,
spark/preprocessing.save_for_review). REVIEW_DIR / LABELLED_TRAIN_DIR point
at a temp folder (tests/conftest.py), never the real training project.
"""

import csv
import shutil
from io import BytesIO

import pytest
from PIL import Image as PILImage

import config.settings as settings
from dashboard import labelling_page
from spark.preprocessing import save_for_review
from tests.conftest import make_leaf_jpeg_bytes
from tests.test_dashboard_users import (  # noqa: F401 - fixtures used by the page test
    _logged_in, _all_text, _seed_admin_and_farmer, drive, fresh_db,
)


@pytest.fixture(autouse=True)
def _empty_folders():
    for d in (settings.REVIEW_DIR, settings.LABELLED_TRAIN_DIR):
        shutil.rmtree(d, ignore_errors=True)
    yield
    for d in (settings.REVIEW_DIR, settings.LABELLED_TRAIN_DIR):
        shutil.rmtree(d, ignore_errors=True)


def _jpeg_with_gps() -> bytes:
    img = PILImage.open(BytesIO(make_leaf_jpeg_bytes(64)))
    exif = PILImage.Exif()
    exif[0x8825] = {1: "S", 2: (8.0, 33.0, 0.0), 3: "E", 4: (125.0, 34.0, 0.0)}  # GPS IFD
    exif[0x0112] = 6                                                            # rotated 90°
    out = BytesIO()
    img.save(out, format="JPEG", exif=exif)
    return out.getvalue()


def test_save_for_review_strips_exif_and_keeps_photo_upright():
    save_for_review("img-1", _jpeg_with_gps())

    saved = PILImage.open(settings.REVIEW_DIR / "img-1.jpg")
    assert not saved.getexif()          # no GPS / orientation tags left
    assert saved.size == (64, 64)
    assert labelling_page.pending_photos() == [settings.REVIEW_DIR / "img-1.jpg"]


def test_save_for_review_never_fails_the_diagnosis():
    save_for_review("img-bad", b"not an image")     # just logs
    assert labelling_page.pending_photos() == []


def test_label_moves_photo_to_train_class_and_logs_it():
    save_for_review("img-2", make_leaf_jpeg_bytes(32))
    photo = labelling_page.pending_photos()[0]

    target = labelling_page.label_photo(photo, "common_rust", ai_class="gray_leaf_spot", admin="admin1")

    assert target == settings.LABELLED_TRAIN_DIR / "common_rust" / "img-2.jpg" and target.exists()
    assert labelling_page.pending_photos() == []
    assert labelling_page.labelled_counts()["common_rust"] == 1
    with open(settings.LABELLED_TRAIN_DIR / "labels_log.csv", encoding="utf-8") as f:
        row = list(csv.DictReader(f))[0]
    assert (row["image_id"], row["label"], row["ai_prediction"], row["labelled_by"]) == \
        ("img-2", "common_rust", "gray_leaf_spot", "admin1")


def test_discard_removes_photo_without_training_on_it():
    save_for_review("img-3", make_leaf_jpeg_bytes(32))
    labelling_page.discard_photo(labelling_page.pending_photos()[0], admin="admin1")
    assert labelling_page.pending_photos() == []
    assert sum(labelling_page.labelled_counts().values()) == 0


def test_discard_all_empties_review_but_keeps_labelled_photos():
    for i in range(3):
        save_for_review(f"img-all-{i}", make_leaf_jpeg_bytes(32))
    labelling_page.label_photo(labelling_page.pending_photos()[0], "healthy")

    assert labelling_page.discard_all(admin="admin1") == 2

    assert labelling_page.pending_photos() == []
    assert labelling_page.labelled_counts()["healthy"] == 1
    with open(settings.LABELLED_TRAIN_DIR / "labels_log.csv", encoding="utf-8") as f:
        assert [r["label"] for r in csv.DictReader(f)] == ["healthy", "discarded", "discarded"]


def test_page_delete_all_needs_confirmation(drive):  # noqa: F811
    save_for_review("img-6", make_leaf_jpeg_bytes(32))
    save_for_review("img-7", make_leaf_jpeg_bytes(32))
    _seed_admin_and_farmer()

    at = _logged_in("Label Photos")
    delete = [b for b in at.button if b.label == "Delete all"][0]
    assert delete.disabled

    at.checkbox(key="discard_all_confirm").check().run()
    [b for b in at.button if b.label == "Delete all"][0].click().run()

    assert labelling_page.pending_photos() == []
    assert "Deleted 2 waiting photo(s)." in _all_text(at)


def test_unknown_class_is_refused():
    save_for_review("img-4", make_leaf_jpeg_bytes(32))
    with pytest.raises(ValueError):
        labelling_page.label_photo(labelling_page.pending_photos()[0], "pest_damage")


def test_page_shows_waiting_photos_and_labels_one(drive):  # noqa: F811
    save_for_review("img-5", make_leaf_jpeg_bytes(32))
    _seed_admin_and_farmer()

    at = _logged_in("Label Photos")
    assert "<h1>Label photos</h1>" in _all_text(at)
    assert "img-5" in _all_text(at)

    [b for b in at.button if b.label == "Northern leaf blight"][0].click().run()

    assert (settings.LABELLED_TRAIN_DIR / "northern_leaf_blight" / "img-5.jpg").exists()
    assert "No photos waiting" in _all_text(at)
