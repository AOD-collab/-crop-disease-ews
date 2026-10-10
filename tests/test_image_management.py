"""
Image management: the SQL catalog (view v_admin_image_catalog,
storage/image_catalog.py), the managed local cache (storage/image_cache.py),
review decisions (storage/reviews.py), the factory-reset preview, and the
dashboard's Image management page.
"""

import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import pytest

import config.settings as settings
import api.image_storage as image_storage
from storage import image_cache, image_catalog, reviews
from storage.database import (
    SessionLocal, Image, Prediction, ProcessingStatus, RiskLevel, User, Role, UserStatus, ImageReview,
    StorageAuditLog, ScaleTestRun, SCALE_TEST_SOURCE,
)
from storage.image_catalog import CatalogFilters, NO_PREDICTION
from tests.conftest import make_leaf_jpeg_bytes
from tests.test_drive_flow import fresh_db, drive, share, _scan, _event_for, _run_spark  # noqa: F401


@pytest.fixture(autouse=True)
def dirs(tmp_path, monkeypatch):
    """Every storage category in this test's own folder."""
    paths = {"DOWNLOADED_PHOTOS_DIR": tmp_path / "downloaded_photos", "REVIEW_DIR": tmp_path / "review",
             "LABELLED_TRAIN_DIR": tmp_path / "train", "SCALABILITY_REPORTS_DIR": tmp_path / "scale",
             "IMAGE_STORAGE_PATH": tmp_path / "images"}
    for name, p in paths.items():
        p.mkdir(parents=True)
        monkeypatch.setattr(settings, name, p)
    monkeypatch.setattr(image_storage, "DOWNLOADED_PHOTOS_DIR", paths["DOWNLOADED_PHOTOS_DIR"])
    return paths


T0 = datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc)      # 12:00 in Timor-Leste


def _farmer(email="farmer@gmail.com") -> str:
    with SessionLocal() as s:
        u = User(username=email, email=email, full_name=email.split("@")[0], password_hash="x",
                 role=Role.USER, status=UserStatus.ACTIVE)
        s.add(u)
        s.commit()
        return u.user_id


def _image(user_id, image_id, content=None, uploaded=T0, status=ProcessingStatus.COMPLETED,
           location="Dili", cached=True, source="android_drive_upload") -> bytes:
    content = content if content is not None else make_leaf_jpeg_bytes(48)
    with SessionLocal() as s:
        s.add(Image(image_id=image_id, user_id=user_id, image_path=f"gdrive:DRV-{image_id}", location=location,
                    content_hash=hashlib.sha256(content).hexdigest(), uploaded_at=uploaded, status=status,
                    source=source))
        s.commit()
    if cached:
        image_storage.store_downloaded_photo(user_id, image_id, content)
    return content


def _predict(image_id, cls, conf, at, risk=RiskLevel.LOW, prediction_id=None):
    with SessionLocal() as s:
        s.add(Prediction(prediction_id=prediction_id or f"p-{image_id}-{cls}-{at.timestamp()}", image_id=image_id,
                         predicted_class=cls, prediction_confidence=conf, risk_level=risk, predicted_at=at,
                         model_version="cnn-resnet18:test.pt"))
        s.commit()


# --------------------------------------------------------------- catalog (SQL)
def test_catalog_lists_images_with_and_without_predictions_newest_first():
    uid = _farmer()
    _image(uid, "img-a", uploaded=T0)
    _image(uid, "img-b", uploaded=T0 + timedelta(hours=1), status=ProcessingStatus.PENDING_REVIEW)
    _image(uid, "img-c", uploaded=T0 + timedelta(hours=1))           # same time as img-b: id breaks the tie
    _predict("img-a", "common_rust", 0.9, T0 + timedelta(minutes=5), RiskLevel.HIGH)

    rows = image_catalog.query()
    assert [r["image_id"] for r in rows] == ["img-b", "img-c", "img-a"]
    by_id = {r["image_id"]: r for r in rows}
    assert by_id["img-a"]["predicted_class"] == "common_rust" and by_id["img-a"]["risk_level"] == "HIGH"
    assert by_id["img-b"]["predicted_class"] is None and by_id["img-b"]["status"] == "PENDING_REVIEW"
    assert by_id["img-a"]["farmer_email"] == "farmer@gmail.com"


def test_latest_prediction_wins_with_a_deterministic_tie_breaker():
    uid = _farmer()
    _image(uid, "img-a")
    _predict("img-a", "healthy", 0.6, T0, prediction_id="p1")
    _predict("img-a", "common_rust", 0.8, T0 + timedelta(hours=1), prediction_id="p2")
    assert image_catalog.get("img-a")["predicted_class"] == "common_rust"
    _image(uid, "img-b")
    _predict("img-b", "healthy", 0.6, T0, prediction_id="pa")
    _predict("img-b", "gray_leaf_spot", 0.7, T0, prediction_id="pb")      # same time: higher id wins
    assert image_catalog.get("img-b")["predicted_class"] == "gray_leaf_spot"
    assert image_catalog.count() == 2                                      # one row per image


def test_scalability_test_images_never_appear_in_the_catalog():
    uid = _farmer()
    _image(uid, "farmer-photo")
    _image(uid, "legacy-test-photo", source=SCALE_TEST_SOURCE)
    assert [r["image_id"] for r in image_catalog.query()] == ["farmer-photo"]
    assert image_catalog.summary()["total"] == 1


def test_filters_combine_and_are_parameterised():
    a, b = _farmer("a@gmail.com"), _farmer("b@gmail.com")
    _image(a, "aaa-1", location="Dili", uploaded=T0)
    _image(a, "aaa-2", location="Baucau", uploaded=T0 + timedelta(days=2), status=ProcessingStatus.FAILED)
    _image(b, "bbb-1", location="Dili", uploaded=T0 + timedelta(days=1))
    _predict("aaa-1", "common_rust", 0.9, T0, RiskLevel.HIGH)

    def ids(**kw):
        return [r["image_id"] for r in image_catalog.query(CatalogFilters(**kw))]
    assert ids(farmer=a) == ["aaa-2", "aaa-1"]
    assert ids(farmer=a, location="Dili") == ["aaa-1"]
    assert ids(statuses=["FAILED"]) == ["aaa-2"]
    assert ids(predicted_classes=[NO_PREDICTION]) == ["aaa-2", "bbb-1"]
    assert ids(risk_levels=["HIGH"]) == ["aaa-1"]
    assert ids(image_id="aaa") == ["aaa-2", "aaa-1"]
    assert ids(image_id="%") == [] and ids(image_id="aa_-1") == []          # wildcards are literal
    assert ids(image_id="x' OR '1'='1") == []                              # never part of the SQL text
    assert ids(uploaded_from=date(2026, 10, 2), uploaded_to=date(2026, 10, 2)) == ["bbb-1"]  # TLT dates


# --------------------------------------------------------------- cache verification
def test_cache_check_finds_valid_missing_and_corrupted_copies():
    uid = _farmer()
    _image(uid, "good")
    _image(uid, "gone", cached=False)
    _image(uid, "bad")
    image_storage.downloaded_photo_path(uid, "bad").write_bytes(b"replaced with something else")
    rows = {r["image_id"]: r for r in image_catalog.query()}
    check = {k: image_cache.check_cached(r["user_id"], k, r["content_hash"]).status for k, r in rows.items()}
    assert check == {"good": "valid", "gone": "missing", "bad": "invalid"}


def test_display_uses_a_valid_copy_and_repairs_a_bad_one_from_drive():
    uid = _farmer()
    content = _image(uid, "img")
    row = image_catalog.get("img")
    calls = []

    def download(file_id):
        calls.append(file_id)
        return content
    shown = image_cache.original_for_display(row, download=download)
    assert calls == [] and shown.source.startswith("Local copy")              # no download

    image_storage.downloaded_photo_path(uid, "img").write_bytes(b"corrupt")
    shown = image_cache.original_for_display(row, performed_by="admin", download=download)
    assert calls == ["DRV-img"] and "verified" in shown.source
    assert image_storage.downloaded_photo_path(uid, "img").read_bytes() == content     # repaired
    with SessionLocal() as s:
        assert s.query(StorageAuditLog).filter_by(action="cache_repair", outcome="done").count() == 1


def test_a_download_that_does_not_match_the_checksum_is_not_cached():
    uid = _farmer()
    _image(uid, "img", cached=False)
    shown = image_cache.original_for_display(image_catalog.get("img"), download=lambda fid: b"other bytes")
    assert "does not match" in shown.source
    assert image_storage.downloaded_photo_path(uid, "img") is None


def test_thumbnail_does_not_modify_the_original():
    uid = _farmer()
    content = _image(uid, "img", content=make_leaf_jpeg_bytes(400))
    path = image_storage.downloaded_photo_path(uid, "img")
    from io import BytesIO
    from PIL import Image as PILImage
    thumb = PILImage.open(BytesIO(image_cache.thumbnail(str(path), max_px=100)))
    assert max(thumb.size) <= 100 and path.read_bytes() == content


# --------------------------------------------------------------- duplicates and orphans
def test_same_photo_from_two_farmers_stays_two_records():
    a, b = _farmer("a@gmail.com"), _farmer("b@gmail.com")
    content = make_leaf_jpeg_bytes(48)
    _image(a, "from-a", content=content)
    _image(b, "from-b", content=content, uploaded=T0 + timedelta(hours=1))
    same = image_catalog.same_content("from-a", image_catalog.get("from-a")["content_hash"])
    assert [s["image_id"] for s in same] == ["from-b"] and same[0]["user_id"] == b
    assert image_catalog.count() == 2


def test_reconcile_matches_files_to_records_and_flags_orphans(dirs):
    a, b = _farmer("a@gmail.com"), _farmer("b@gmail.com")
    content = _image(a, "registered")
    image_storage.store_downloaded_photo(a, "old-reset-photo", make_leaf_jpeg_bytes(48))     # no record
    image_storage.store_downloaded_photo(a, "same-bytes-old-id", content)                   # duplicate content
    image_storage.store_downloaded_photo(b, "registered", make_leaf_jpeg_bytes(48))          # same name, other farmer
    _image(a, "not-cached", cached=False)
    (dirs["DOWNLOADED_PHOTOS_DIR"] / a / "half.jpg.tmp").write_bytes(b"x")

    verdicts = {(r.rel, r.verdict) for r in image_cache.reconcile()}
    assert (f"{a}/registered.jpg", "matched") in verdicts
    assert (f"{a}/old-reset-photo.jpg", "orphan") in verdicts
    assert (f"{a}/same-bytes-old-id.jpg", "orphan_duplicate") in verdicts
    assert (f"{b}/registered.jpg", "wrong_folder") in verdicts
    assert (f"{a}/not-cached.*", "missing") in verdicts
    assert (f"{a}/half.jpg.tmp", "temporary") in verdicts


# --------------------------------------------------------------- clean-up
def _old(path):
    past = time.time() - 2 * image_cache.TEMP_MIN_AGE_SECONDS
    os.utime(path, (past, past))


def test_cleanup_preview_is_a_dry_run_and_cleanup_removes_only_what_was_confirmed(dirs):
    uid = _farmer()
    _image(uid, "keep-me")
    image_storage.store_downloaded_photo(uid, "orphan-1", make_leaf_jpeg_bytes(48))
    image_storage.store_downloaded_photo(uid, "orphan-2", make_leaf_jpeg_bytes(48))
    (dirs["REVIEW_DIR"] / "orphan-1.jpg").write_bytes(b"review copy")
    (dirs["LABELLED_TRAIN_DIR"] / "healthy").mkdir()
    (dirs["LABELLED_TRAIN_DIR"] / "healthy" / "orphan-1.jpg").write_bytes(b"training copy")

    preview = image_cache.cleanup_preview(["orphan_cache"])
    assert sorted(c.rel for c in preview) == [f"{uid}/orphan-1.jpg", f"{uid}/orphan-2.jpg"]
    assert all(image_storage.downloaded_photo_path(uid, i) for i in ("orphan-1", "orphan-2"))  # nothing gone

    dry = image_cache.cleanup(["orphan_cache"], [f"{uid}/orphan-1.jpg"], performed_by="admin", dry_run=True)
    assert dry["deleted"] == 1 and image_storage.downloaded_photo_path(uid, "orphan-1") is not None

    report = image_cache.cleanup(["orphan_cache"], [f"{uid}/orphan-1.jpg"], performed_by="admin")
    assert report["deleted"] == 1
    assert image_storage.downloaded_photo_path(uid, "orphan-1") is None
    assert image_storage.downloaded_photo_path(uid, "orphan-2") is not None          # not confirmed
    assert image_storage.downloaded_photo_path(uid, "keep-me") is not None
    assert (dirs["REVIEW_DIR"] / "orphan-1.jpg").exists()                            # review copy kept
    assert (dirs["LABELLED_TRAIN_DIR"] / "healthy" / "orphan-1.jpg").exists()        # training kept
    with SessionLocal() as s:
        log = s.query(StorageAuditLog).filter_by(action="cleanup_delete").all()
    assert [(r.target, r.outcome, r.performed_by) for r in log] == [(f"{uid}/orphan-1.jpg", "done", "admin")]


def test_cleanup_never_touches_photos_being_processed_or_stale_confirmations(dirs):
    uid = _farmer()
    _image(uid, "busy", status=ProcessingStatus.PROCESSING)
    image_storage.downloaded_photo_path(uid, "busy").write_bytes(b"corrupt")       # invalid, but in use
    _image(uid, "broken")
    image_storage.downloaded_photo_path(uid, "broken").write_bytes(b"corrupt")
    preview = image_cache.cleanup_preview(["invalid_cache"])
    assert [c.rel for c in preview] == [f"{uid}/broken.jpg"]

    # repaired between the preview and the confirmation -> skipped, not deleted
    content = image_storage.downloaded_photo_path(uid, "broken")
    with SessionLocal() as s:
        h = s.get(Image, "broken").content_hash
    good = make_leaf_jpeg_bytes(48)
    with SessionLocal() as s:
        s.get(Image, "broken").content_hash = hashlib.sha256(good).hexdigest()
        s.commit()
    content.write_bytes(good)
    report = image_cache.cleanup(["invalid_cache"], [f"{uid}/broken.jpg"], performed_by="admin")
    assert report["deleted"] == 0 and report["skipped"] and content.exists()
    assert h  # (the original checksum was set)


def test_cleanup_of_temporary_and_scalability_files(dirs):
    uid = _farmer()
    tmp = dirs["DOWNLOADED_PHOTOS_DIR"] / uid
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / "old.jpg.tmp").write_bytes(b"x")
    _old(tmp / "old.jpg.tmp")
    (tmp / "new.jpg.tmp").write_bytes(b"x")                                    # may still be being written
    for run_id, status in (("scale-done", "COMPLETED"), ("scale-live", "RUNNING")):
        folder = dirs["SCALABILITY_REPORTS_DIR"] / run_id / "images"
        folder.mkdir(parents=True)
        (folder / "a.jpg").write_bytes(b"a")
        (folder / "b.jpg").write_bytes(b"b")
        with SessionLocal() as s:
            s.add(ScaleTestRun(run_id=run_id, status=status, requested_size=2,
                               summary_json=json.dumps({"comparison": {"before": "images/a.jpg"}})))
            s.commit()
    rels = sorted(c.rel for c in image_cache.cleanup_preview(["temporary", "scalability_images"]))
    assert rels == [f"{uid}/old.jpg.tmp", "scale-done/images/b.jpg"]          # comparison image + live run kept


# --------------------------------------------------------------- review decisions
def test_label_decisions_are_stored_apart_from_predictions(dirs):
    from dashboard import labelling_page
    uid = _farmer()
    _image(uid, "img-1")
    _image(uid, "img-2")
    _predict("img-1", "gray_leaf_spot", 0.9, T0)
    (dirs["REVIEW_DIR"] / "img-1.jpg").write_bytes(make_leaf_jpeg_bytes(48))
    (dirs["REVIEW_DIR"] / "img-2.jpg").write_bytes(make_leaf_jpeg_bytes(48))
    assert image_cache.review_state("img-1", None) == "awaiting_label"

    labelling_page.label_photo(dirs["REVIEW_DIR"] / "img-1.jpg", "healthy", "gray_leaf_spot", "admin")
    labelling_page.discard_photo(dirs["REVIEW_DIR"] / "img-2.jpg", None, "admin")
    row = image_catalog.get("img-1")
    assert row["predicted_class"] == "gray_leaf_spot" and row["confirmed_class"] == "healthy"
    assert image_cache.review_state("img-1", row["review_decision"]) == "labelled"
    assert image_catalog.get("img-2")["review_decision"] == "discarded"
    assert image_catalog.get("img-2")["confirmed_class"] is None


def test_labels_log_backfill_adds_only_missing_decisions(dirs):
    log = dirs["LABELLED_TRAIN_DIR"] / reviews.LOG_NAME
    log.write_text("image_id,label,ai_prediction,labelled_by,labelled_at\n"
                   "a,discarded,healthy,admin,2026-10-01T10:00:00\n"
                   "a,common_rust,healthy,admin,2026-10-02T10:00:00\n"
                   "b,healthy,,admin,2026-10-03T10:00:00\n", encoding="utf-8")
    reviews.record("b", "discarded", None, None, "other")
    assert reviews.import_labels_log() == 1
    assert reviews.import_labels_log() == 0                                   # idempotent
    with SessionLocal() as s:
        a, b = s.get(ImageReview, "a"), s.get(ImageReview, "b")
    assert (a.decision, a.confirmed_class) == ("labelled", "common_rust")       # last line wins
    assert b.decision == "discarded"                                            # existing row untouched


# --------------------------------------------------------------- factory reset
def test_factory_reset_previews_both_sides_and_keeps_cache_review_training(dirs):
    from storage.reset import reset_preview, factory_reset
    uid = _farmer()
    _image(uid, "img-1")
    _predict("img-1", "healthy", 0.9, T0)
    (dirs["REVIEW_DIR"] / "img-1.jpg").write_bytes(b"r")
    reviews.record("img-1", "labelled", "healthy", "healthy", "admin")

    preview = reset_preview()
    assert preview["deleted_rows"]["images"] == 1 and preview["deleted_rows"]["predictions"] == 1
    assert preview["kept_rows"]["image_reviews"] == 1
    assert preview["kept_files"][image_cache.CATEGORIES["original_cache"]] == 1

    summary = factory_reset(performed_by="admin")
    assert summary["images_deleted"] == 1
    assert image_storage.downloaded_photo_path(uid, "img-1") is not None        # cached copy kept
    assert (dirs["REVIEW_DIR"] / "img-1.jpg").exists()
    assert summary["retained_unmatched_now"] == 1                              # reported, not deleted
    with SessionLocal() as s:
        assert s.get(ImageReview, "img-1") is not None
        assert s.query(StorageAuditLog).filter_by(action="factory_reset", performed_by="admin").count() == 1


# --------------------------------------------------------------- dashboard page
def test_page_requires_an_admin_login(drive):
    from streamlit.testing.v1 import AppTest
    from tests.test_dashboard_users import DASHBOARD_APP_PATH, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    at = AppTest.from_file(DASHBOARD_APP_PATH, default_timeout=60).run()
    at.text_input[0].input("farmer1@gmail.com").run()
    at.text_input[1].input("goodpass1").run()
    at.button[0].click().run()
    assert any("ADMIN accounts only" in e.value for e in at.error)
    assert not [b for b in at.sidebar.button if (b.key or "") == "nav_Images"]


def test_page_shows_counts_gallery_and_details_side_by_side(drive):
    from tests.test_dashboard_users import _logged_in, _all_text, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes(128), location="Baucau")
    _scan()
    _run_spark(_event_for(image_id))
    with SessionLocal() as s:                                                  # a legacy test image: never shown
        uid = s.query(User).filter_by(username="farmer1@gmail.com").one().user_id
    _image(uid, "scale-test-row", source=SCALE_TEST_SOURCE)

    at = _logged_in("Images")
    assert not at.exception, at.exception
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Farmer images"] == "1" and metrics["Completed"] == "1"
    assert metrics["With a prediction"] == "1" and metrics["Cached original photos"] == "2"
    text = _all_text(at)
    assert "1 image record(s)" in text and "farmer1@gmail.com · Baucau" in text
    assert "scale-te" not in text

    [b for b in at.button if b.key == f"im_open_{image_id}"][0].click().run()
    assert not at.exception, at.exception
    captions = [c for el in at.get("image") for c in el.captions if c]
    assert any(c.startswith("Original — Local copy (verified)") for c in captions)
    assert any(c.startswith("Processed —") for c in captions)
    assert "Model prediction" in _all_text(at) and "Administrator review" in _all_text(at)


def test_page_filters_and_reset(drive):
    from tests.test_dashboard_users import _logged_in, _all_text, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    with SessionLocal() as s:
        uid = s.query(User).filter_by(username="farmer1@gmail.com").one().user_id
    _image(uid, "img-ok")
    _image(uid, "img-failed", status=ProcessingStatus.FAILED, uploaded=T0 + timedelta(hours=1))
    at = _logged_in("Images")
    assert "2 image record(s)" in _all_text(at)
    at.multiselect(key="im_status").set_value(["FAILED"]).run()
    assert "1 image record(s)" in _all_text(at)
    [b for b in at.button if b.label == "Reset filters"][0].click().run()
    assert "2 image record(s)" in _all_text(at)
    at.selectbox(key="im_cache").set_value("Not cached").run()
    assert "No farmer images match these filters." in _all_text(at)


def test_page_storage_tab_previews_before_deleting(drive, dirs):
    from tests.test_dashboard_users import _logged_in, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    with SessionLocal() as s:
        uid = s.query(User).filter_by(username="farmer1@gmail.com").one().user_id
    image_storage.store_downloaded_photo(uid, "orphan", make_leaf_jpeg_bytes(48))
    at = _logged_in("Images")
    assert {m.label: m.value for m in at.metric}["Potentially orphaned files"] == "1"
    at.multiselect(key="im_clean_kinds").set_value(["orphan_cache"]).run()
    [b for b in at.button if b.label.startswith("Preview clean-up")][0].click().run()
    delete = [b for b in at.button if b.label == "Delete 1 file(s)"][0]
    assert delete.disabled                                                     # needs the confirmation box
    assert image_storage.downloaded_photo_path(uid, "orphan") is not None
    at.checkbox(key="im_clean_ok").check().run()
    [b for b in at.button if b.label == "Delete 1 file(s)"][0].click().run()
    assert not at.exception, at.exception
    assert image_storage.downloaded_photo_path(uid, "orphan") is None
