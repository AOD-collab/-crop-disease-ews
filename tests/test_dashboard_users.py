"""
Dashboard tests — the real Streamlit script run via AppTest against a real
temp database, with Google Drive replaced by the same in-memory FakeDrive
the Drive-flow tests use (tests/test_drive_flow.py).

Covers the Google Drive version of the dashboard: Register farmer (private
folders + test-user reminder), read-only Gmail in the edit form, admin
self-protection, the Drive gallery (original + processed image), the live
System Status page, and that every page opens without an error.
"""
import pytest
from pathlib import Path
from streamlit.testing.v1 import AppTest

from api.auth import hash_password
from storage import drive_client
from storage.database import init_db, SessionLocal, Base, engine, User, Role, UserStatus, ProcessingStatus
from tests.conftest import make_leaf_jpeg_bytes
from tests.test_drive_flow import (
    FakeDrive, IMAGES_FOLDER, RESULTS_FOLDER, share, _scan, _event_for, _run_spark,
)

DASHBOARD_APP_PATH = str(Path(__file__).resolve().parent.parent / "dashboard" / "app.py")
PAGES = ["Overview", "Real-Time Monitoring", "Pending Review", "Users", "Images",
         "Risk Alerts", "Analytics", "Performance", "System Status", "Label Photos", "Model Training",
         "Scalability Test", "Maintenance"]


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_REFRESH_SECONDS", "0")   # Real-Time Monitoring would reload forever
    Base.metadata.drop_all(engine)
    init_db()
    import streamlit as st
    st.cache_data.clear()        # dashboard caches Drive lookups; start each test fresh
    from kafka import drive_watcher
    monkeypatch.setattr(drive_watcher, "STATE_FILE", tmp_path / "watcher_state.json")
    yield
    Base.metadata.drop_all(engine)


@pytest.fixture()
def drive(monkeypatch):
    fake = FakeDrive()
    for name in ("get_file_metadata", "download_file", "find_file_in_folder", "list_subfolders",
                 "list_files_in_folders", "ensure_folder", "upload_bytes", "share_with"):
        monkeypatch.setattr(drive_client, name, getattr(fake, name))
    monkeypatch.setattr(drive_client, "_user_folder_cache", {})
    monkeypatch.setattr(drive_client, "images_folder_id", lambda: IMAGES_FOLDER)
    monkeypatch.setattr(drive_client, "results_folder_id", lambda: RESULTS_FOLDER)
    monkeypatch.setattr(drive_client, "is_configured", lambda: True)
    monkeypatch.setattr(drive_client, "google_cloud_project_id", lambda: "crop-disease-ews")
    monkeypatch.setattr(drive_client, "account_info",
                        lambda: {"email": "central@gmail.com", "usage_bytes": 2_000_000_000,
                                 "limit_bytes": 15_000_000_000})
    return fake


def _seed_admin_and_farmer():
    with SessionLocal() as s:
        s.add(User(username="admin", full_name="Admin", email="admin@example.com",
                   password_hash=hash_password("adminpass1"), role=Role.ADMIN,
                   status=UserStatus.ACTIVE, email_verified=True))
        s.add(User(username="farmer1@gmail.com", full_name="Farmer One", email="farmer1@gmail.com",
                   password_hash=hash_password("goodpass1"), role=Role.USER,
                   status=UserStatus.ACTIVE, email_verified=True))
        s.commit()


def _logged_in(page="Users"):
    at = AppTest.from_file(DASHBOARD_APP_PATH, default_timeout=60)
    at.run()
    at.text_input[0].input("admin").run()
    at.text_input[1].input("adminpass1").run()
    at.button[0].click().run()
    _open(at, page)
    return at


def _open(at, page):
    """Click a page's menu button in the sidebar."""
    [b for b in at.sidebar.button if b.key == f"nav_{page}"][0].click().run()
    assert not at.exception, at.exception


def _all_text(at):
    parts = []
    for kind in ("markdown", "caption", "success", "warning", "error", "info"):
        parts += [getattr(e, "value", "") or "" for e in getattr(at, kind)]
    return "\n".join(str(p) for p in parts)


# ----------------------------------------------------------- Farmer Management
def test_register_farmer_creates_private_folders_user_and_test_user_reminder(drive):
    _seed_admin_and_farmer()
    at = _logged_in("Users")
    assert not [b for b in at.button if b.label == "Create user"]          # old password form is gone

    [t for t in at.text_area if "Farmer Gmail" in t.label][0].input(
        "New.Farmer@Gmail.com\nsecond@gmail.com").run()
    [b for b in at.button if b.label == "Register farmer"][0].click().run()
    assert not at.exception, at.exception

    for email in ("new.farmer@gmail.com", "second@gmail.com"):
        images, results = drive.folder_of(email, IMAGES_FOLDER), drive.folder_of(email, RESULTS_FOLDER)
        assert images and results
        assert (images, email, "writer") in drive.shares and (results, email, "reader") in drive.shares
    assert not [x for x in drive.shares if x[0] in (IMAGES_FOLDER, RESULTS_FOLDER)]   # parents never shared

    with SessionLocal() as s:
        u = s.query(User).filter(User.email == "new.farmer@gmail.com").one()
        assert u.role == Role.USER and u.status == UserStatus.ACTIVE

    table = at.dataframe[0].value                                   # list already refreshed
    assert {"new.farmer@gmail.com", "second@gmail.com"} <= set(table["Gmail"])
    for email in ("new.farmer@gmail.com", "second@gmail.com"):
        assert "yes" in table[table["Gmail"] == email].iloc[0]["Drive folders"]
    text = _all_text(at)
    assert "Registered: new.farmer@gmail.com, second@gmail.com" in text
    assert "Test users" in text
    assert "console.cloud.google.com/auth/audience?project=crop-disease-ews" in text


def test_register_farmer_rejects_invalid_email(drive):
    _seed_admin_and_farmer()
    at = _logged_in("Users")
    [t for t in at.text_area if "Farmer Gmail" in t.label][0].input("not-an-email").run()
    [b for b in at.button if b.label == "Register farmer"][0].click().run()
    assert any("Not a valid email" in e.value for e in at.error)
    assert drive.list_subfolders(IMAGES_FOLDER) == []


def test_edit_user_gmail_is_read_only_and_name_status_still_editable(drive):
    _seed_admin_and_farmer()
    at = _logged_in("Users")
    sb = [sb for sb in at.selectbox if sb.label == "Select a user"][0]
    sb.set_value([o for o in sb.options if "farmer1@gmail.com" in o][0]).run()

    gmail = [t for t in at.text_input if t.label.startswith("Gmail address")][0]
    assert gmail.disabled and gmail.value == "farmer1@gmail.com"

    [t for t in at.text_input if t.label == "Full Name" and t.value == "Farmer One"][0].input("Farmer Uno").run()
    [sb for sb in at.selectbox if sb.label == "Status"][-1].set_value("INACTIVE").run()
    [b for b in at.button if b.label == "Save changes"][0].click().run()
    assert not at.exception, at.exception
    with SessionLocal() as s:
        u = s.query(User).filter(User.email == "farmer1@gmail.com").one()
        assert u.full_name == "Farmer Uno" and u.status == UserStatus.INACTIVE and u.email == "farmer1@gmail.com"


def test_admin_cannot_remove_own_admin_role_via_dashboard(drive):
    _seed_admin_and_farmer()
    at = _logged_in("Users")
    sb_edit = [sb for sb in at.selectbox if sb.label == "Select a user"][0]
    sb_edit.set_value([o for o in sb_edit.options if "Admin" in o][0]).run()
    # The page-level filter and the edit form both have a "Role" selectbox;
    # the edit form's is rendered last.
    [sb for sb in at.selectbox if sb.label == "Role"][-1].set_value("USER").run()
    [b for b in at.button if b.label == "Save changes"][0].click().run()
    assert not at.exception, at.exception
    assert any("cannot remove your own admin role" in e.value for e in at.error)
    with SessionLocal() as s:
        assert s.query(User).filter(User.username == "admin").one().role == Role.ADMIN


def test_user_list_shows_drive_folder_status(drive):
    _seed_admin_and_farmer()
    share("farmer1@gmail.com")
    at = _logged_in("Users")
    table = at.dataframe[0].value
    row = table[table["Gmail"] == "farmer1@gmail.com"].iloc[0]
    assert "yes" in row["Drive folders"]


# ----------------------------------------------------------- Images gallery
def test_images_gallery_shows_drive_uploads_with_processed_image(drive):
    _seed_admin_and_farmer()
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes(128), location="Baucau")
    _scan()
    _run_spark(_event_for(image_id))
    with SessionLocal() as s:
        from storage.database import Image
        assert s.get(Image, image_id).status == ProcessingStatus.COMPLETED

    at = _logged_in("Images")                       # the Image management page
    text = _all_text(at)
    assert "farmer1@gmail.com · Baucau" in text
    assert "1 image record(s)" in text
    [b for b in at.button if b.key == f"im_open_{image_id}"][0].click().run()
    captions = [c for el in at.get("image") for c in el.captions if c]
    assert any(c.startswith("Original —") for c in captions)
    assert any(c.startswith("Processed —") for c in captions)       # the marked-up image from Drive


def test_images_gallery_empty_state(drive):
    _seed_admin_and_farmer()
    at = _logged_in("Images")
    assert any("No farmer images yet" in e.value for e in at.info)


# ----------------------------------------------------------- System Status
def test_system_status_reports_drive_database_and_backlog(drive):
    _seed_admin_and_farmer()
    share("farmer1@gmail.com")
    at = _logged_in("System Status")
    text = _all_text(at)
    for title in ("Google Drive", "Drive watcher", "Kafka", "Spark streaming job", "Database", "Pipeline backlog"):
        assert title in text, title
    assert "Connected as **central@gmail.com**" in text
    assert "1 farmer folder(s) registered" in text
    assert "Storage used: 2.00 GB of 15 GB" in text
    assert "SQLite" in text or "PostgreSQL" in text


# ----------------------------------------------------------- every page opens
@pytest.mark.parametrize("page", PAGES)
def test_every_page_opens_without_error(drive, page):
    _seed_admin_and_farmer()
    share("farmer1@gmail.com")
    image_id, _ = drive.phone_upload("farmer1@gmail.com", make_leaf_jpeg_bytes())
    _scan()
    _run_spark(_event_for(image_id))
    at = _logged_in(page)
    assert not at.exception, at.exception


def test_menu_is_buttons_and_disease_predictions_merged_into_analytics(drive):
    _seed_admin_and_farmer()
    at = _logged_in("Overview")
    assert not at.sidebar.radio                                        # no radio list any more
    nav = [b for b in at.sidebar.button if (b.key or "").startswith("nav_")]
    assert [b.key[4:] for b in nav] == PAGES
    assert not any("Disease Predictions" in b.label for b in nav)


def test_risk_alerts_shows_n_of_n_high_risk_per_municipality(drive):
    from datetime import datetime, timezone
    from storage.database import Image, Prediction, RiskLevel
    _seed_admin_and_farmer()
    with SessionLocal() as s:
        farmer = s.query(User).filter(User.username == "farmer1@gmail.com").one()
        for i, (loc, risk) in enumerate([("Dili", RiskLevel.HIGH), ("Dili", RiskLevel.HIGH),
                                         ("Dili", RiskLevel.LOW), ("Baucau", RiskLevel.MEDIUM)]):
            s.add(Image(image_id=f"img{i}", user_id=farmer.user_id, location=loc, image_path=f"x{i}.jpg",
                        uploaded_at=datetime.now(timezone.utc), status=ProcessingStatus.COMPLETED))
            s.add(Prediction(image_id=f"img{i}", predicted_class="common_rust", prediction_confidence=0.9,
                             risk_score=0.7, risk_level=risk, predicted_at=datetime.now(timezone.utc)))
        s.commit()

    at = _logged_in("Risk Alerts")

    assert "**2 of 4** analysed maize photos are HIGH risk (50%), across 2 municipalities." in _all_text(at)
    table = at.dataframe[0].value
    assert list(table["location"]) == ["Dili", "Baucau"]
    assert list(table["summary"]) == ["2 of 3 maize photos", "0 of 1 maize photo"]


def test_performance_page_shows_every_time_in_seconds(drive):
    """processing_latency_ms is stored in milliseconds; the page converts it
    so every metric there is in seconds, and a real 0 is not shown as "—"."""
    from datetime import datetime, timedelta, timezone
    from storage.database import Image, Prediction
    _seed_admin_and_farmer()
    t = datetime(2026, 10, 1, 3, 0, tzinfo=timezone.utc)
    with SessionLocal() as s:
        farmer = s.query(User).filter(User.username == "farmer1@gmail.com").one()
        s.add(Image(image_id="img1", user_id=farmer.user_id, image_path="x.jpg", uploaded_at=t, ingested_at=t,
                    status=ProcessingStatus.COMPLETED))
        s.add(Prediction(image_id="img1", predicted_class="healthy", prediction_confidence=0.9,
                         predicted_at=t + timedelta(seconds=4), processing_latency_ms=1500))
        s.commit()
    at = _logged_in("Performance")
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Avg Kafka → prediction"] == "1.50 s"                 # 1500 ms
    assert metrics["Avg upload → Kafka"] == "0.0 s"                     # zero, not "—"
    assert metrics["Avg upload → prediction"] == "4.0 s"
    assert metrics["Avg capture → upload"] == "—"                       # no capture time recorded


def test_open_page_is_highlighted_and_title_changes(drive):
    _seed_admin_and_farmer()
    at = _logged_in("Images")
    nav = {b.key: b for b in at.sidebar.button if (b.key or "").startswith("nav_")}
    assert nav["nav_Images"].proto.type == "primary"                   # corn-yellow button
    assert all(b.proto.type == "secondary" for k, b in nav.items() if k != "nav_Images")
    assert any("<h1>Image management</h1>" in m.value for m in at.markdown)
    _open(at, "Risk Alerts")
    nav = {b.key: b for b in at.sidebar.button if (b.key or "").startswith("nav_")}
    assert nav["nav_Risk Alerts"].proto.type == "primary" and nav["nav_Images"].proto.type == "secondary"
    assert any("<h1>Risk alerts</h1>" in m.value for m in at.markdown)


# ----------------------------------------------------------- Pending Review
def test_pending_review_auto_refresh_watches_held_photos(drive, monkeypatch):
    monkeypatch.setenv("DASHBOARD_REFRESH_SECONDS", "5")
    _seed_admin_and_farmer()
    from storage.database import Image
    with SessionLocal() as s:
        farmer = s.query(User).filter(User.email == "farmer1@gmail.com").one()
        s.add(Image(user_id=farmer.user_id, image_path="missing/held.jpg",
                    status=ProcessingStatus.PENDING_REVIEW))
        s.commit()
    at = _logged_in("Pending Review")
    text = _all_text(at)
    assert "1** image(s) awaiting processing" in text
    assert "Checks for new uploads every 5s" in text


# ----------------------------------------------------------- admin login rules
def _try_login(username="admin", password="adminpass1"):
    at = AppTest.from_file(DASHBOARD_APP_PATH, default_timeout=60)
    at.run()
    at.text_input[0].input(username).run()
    at.text_input[1].input(password).run()
    at.button[0].click().run()
    assert not at.exception, at.exception
    return at


@pytest.mark.parametrize("change,message", [
    ({"is_deleted": True}, "Invalid username or password."),
    ({"status": UserStatus.INACTIVE}, "This account is not active."),
    ({"status": UserStatus.SUSPENDED}, "This account is not active."),
])
def test_deleted_or_inactive_admin_cannot_log_in(change, message):
    _seed_admin_and_farmer()
    with SessionLocal() as s:
        admin = s.query(User).filter(User.username == "admin").one()
        for k, v in change.items():
            setattr(admin, k, v)
        s.commit()
    at = _try_login()
    assert "admin_user" not in at.session_state
    assert any(message in e.value for e in at.error)


def test_open_session_ends_when_admin_is_deactivated():
    _seed_admin_and_farmer()
    at = _logged_in("Overview")
    assert "admin_user" in at.session_state
    with SessionLocal() as s:
        s.query(User).filter(User.username == "admin").one().status = UserStatus.INACTIVE
        s.commit()
    at.run()
    assert not at.exception, at.exception
    assert "admin_user" not in at.session_state
    assert any("sign in again" in w.value for w in at.warning)
