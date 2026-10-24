"""
ADMIN Web Dashboard (Streamlit) — DIT manual sections 27-31, 47.

This is the ADMIN application. It reads the database directly and the
central Google Drive through storage/drive_client.py (it's a trusted,
server-side admin tool; the farmers' Android app talks only to Google
Drive) and requires an ADMIN login before showing anything.

Run from the project root:
    streamlit run dashboard/app.py
"""

import re
import sys
from datetime import timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import or_ as sa_or

from storage.database import (
    SessionLocal, User, Image, Prediction, Role, UserStatus, ProcessingStatus,
    ProcessingMode, get_processing_mode, set_processing_mode, init_db, now_utc, SCALE_TEST_SOURCE,
    SCALE_TEST_USERNAME,
)
from storage.reset import factory_reset, _python_command_lines
from storage.pipeline_submit import submit_image_for_processing, is_upload_rejected
from storage.requeue_stuck import count_owed
from config.localtime import format_local, now_local

# Table columns: same "28/09/2026 21:37" Timor-Leste Time as everywhere else.
_TLT_COL = st.column_config.DatetimeColumn(format="DD/MM/YYYY HH:mm", timezone="Asia/Dili")
TIME_COLUMNS = {c: _TLT_COL for c in ("captured_at", "uploaded_at", "processed_at", "predicted_at")}
from storage.auth import verify_password, normalize_email
from storage.image_storage import is_drive_path, drive_file_id_from_path, resolve_image_path, downloaded_photo_path
from storage import drive_client
from spark import analytics

st.set_page_config(page_title="Crop Disease EWS — Admin Dashboard", layout="wide", page_icon="🌽")


# ------------------------------------------------------------------
# Shared color language (mirrors android_app's colors.xml so the admin
# dashboard and the mobile app read as one system, not two). Status colors
# are reserved for verdicts — never reused for an arbitrary chart series.
# The maize brand colours (green + yellow) live in .streamlit/config.toml
# and the CSS in _inject_style() below.
# ------------------------------------------------------------------
COLOR_GOOD = "#0CA30C"        # healthy / LOW risk
COLOR_WARNING = "#FAB219"     # MEDIUM risk
COLOR_CRITICAL = "#D03B3B"    # HIGH risk / diseased

RISK_COLOR_MAP = {"LOW": COLOR_GOOD, "MEDIUM": COLOR_WARNING, "HIGH": COLOR_CRITICAL}
# Disease / status colours are shared with the other dashboard pages.
from dashboard.theme import STATUS_BADGE_COLOR, CATEGORICAL_HUES, DISEASE_COLOR_MAP  # noqa: E402


# The app's own logo (set with scripts/set_app_logo.py), reused here so the
# dashboard and the Android app look like one product.
_LOGO = PROJECT_ROOT / "android_app" / "app" / "src" / "main" / "res" / "drawable" / "app_logo.png"


def _inject_style():
    """Green + yellow maize theme. The sidebar is the one bold element (dark
    leaf green, menu as buttons, the open page in corn yellow); the rest
    stays quiet. Colours: see COLOR_* above."""
    st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:opsz,wght@12..96,600;12..96,700&display=swap');
:root { --leaf:#1F4D2B; --stalk:#2E7D32; --husk:#E4EEDA; --kernel:#F5C518; --kernel-soft:rgba(245,197,24,.18); }

/* ---- sidebar: dark leaf green ---- */
[data-testid="stSidebar"] { background: var(--leaf); }
[data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label,
[data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3,
[data-testid="stSidebar"] small, [data-testid="stSidebar"] [data-testid="stCaptionContainer"] { color: #E9F2E4; }
[data-testid="stSidebar"] hr { border-color: rgba(233,242,228,.16); margin: .9rem 0; }
[data-testid="stSidebar"] code { background: var(--kernel); color: var(--leaf); }
.ews-brand { font-family: 'Bricolage Grotesque', system-ui, sans-serif; font-weight: 700;
             font-size: 1.15rem; line-height: 1.15; color: #FFFFFF; margin: .35rem 0 0; }
.ews-brand span { display: block; font-weight: 600; font-size: .82rem; color: var(--kernel); margin-top: .15rem; }

/* other sidebar buttons (Log out, processing mode): outlined */
[data-testid="stSidebar"] .stButton button {
  background: transparent; color: #E9F2E4; border: 1px solid rgba(233,242,228,.35); border-radius: 10px;
  transition: background-color .15s ease, border-color .15s ease, color .15s ease; }
[data-testid="stSidebar"] .stButton button:hover { background: var(--kernel-soft); border-color: var(--kernel); color: #FFF6CF; }

/* menu buttons: flat, left-aligned; hover = yellow glow; open page = corn yellow */
[data-testid="stSidebar"] [class*="st-key-nav_"] button {
  justify-content: flex-start; border-color: transparent; padding: .5rem .8rem; }
[data-testid="stSidebar"] [class*="st-key-nav_"] button > div { justify-content: flex-start; width: 100%; }
[data-testid="stSidebar"] [class*="st-key-nav_"] button p { text-align: left; font-size: .95rem; }
[data-testid="stSidebar"] [class*="st-key-nav_"] button:hover { background: var(--kernel-soft); border-color: rgba(245,197,24,.45); }
[data-testid="stSidebar"] [class*="st-key-nav_"] { margin-bottom: -.55rem; }
[data-testid="stSidebar"] button[kind="primary"], [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] {
  background: var(--kernel) !important; border-color: var(--kernel) !important; }
[data-testid="stSidebar"] button[kind="primary"] p, [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] p {
  color: var(--leaf) !important; font-weight: 700; }
[data-testid="stSidebar"] button:focus-visible { outline: 2px solid var(--kernel); outline-offset: 2px; }

/* ---- page header ---- */
.ews-page-head h1 { font-family: 'Bricolage Grotesque', system-ui, sans-serif; font-weight: 700;
                    font-size: 2.1rem; letter-spacing: -.01em; padding: 0; margin: 0; }
.ews-page-head h1::after { content: ""; display: block; width: 3rem; height: .3rem; border-radius: 3px;
                           background: var(--kernel); margin-top: .45rem; }
.ews-page-head p { opacity: .72; margin: .6rem 0 1.2rem; max-width: 70ch; }

/* ---- numbers: a stalk-green edge, no shadows ---- */
[data-testid="stMetric"] { border: 1px solid rgba(46,125,50,.22); border-left: 4px solid var(--stalk);
                           border-radius: 10px; padding: .75rem 1rem; }
h2, h3 { font-family: 'Bricolage Grotesque', system-ui, sans-serif; }

@media (prefers-reduced-motion: reduce) { [data-testid="stSidebar"] .stButton button { transition: none; } }
</style>""", unsafe_allow_html=True)


# ------------------------------------------------------------------
# Login gate — ADMIN only (section 27: the web dashboard is the ADMIN app)
# ------------------------------------------------------------------
def login_screen():
    _, mid, _ = st.columns([1, 1.2, 1])
    with mid:
        if _LOGO.exists():
            st.image(str(_LOGO), width=120)
        st.markdown('<div class="ews-page-head"><h1>Admin sign in</h1>'
                    '<p>Maize Disease Early Warning System</p></div>', unsafe_allow_html=True)
        _login_form()


def _login_form():
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Log in", type="primary", width="stretch")

    if submitted:
        init_db()
        session = SessionLocal()
        try:
            user = session.query(User).filter(User.username == username).first()
            if user is None or not verify_password(password, user.password_hash) or user.is_deleted:
                st.error("Invalid username or password.")
                return
            if user.role != Role.ADMIN:
                st.error("This dashboard is for ADMIN accounts only.")
                return
            if user.status != UserStatus.ACTIVE:
                st.error("This account is not active.")
                return
            st.session_state["admin_user"] = {"user_id": user.user_id, "username": user.username}
            st.rerun()
        finally:
            session.close()


_inject_style()

def _admin_still_allowed(user_id: str) -> bool:
    """Re-checked on every page load: an admin deleted, deactivated or
    demoted after signing in loses the open dashboard session too (the
    login form alone used to be the only check)."""
    with SessionLocal() as s:
        user = s.get(User, user_id)
        return (user is not None and not user.is_deleted
                and user.role == Role.ADMIN and user.status == UserStatus.ACTIVE)


if "admin_user" in st.session_state and not _admin_still_allowed(st.session_state["admin_user"]["user_id"]):
    del st.session_state["admin_user"]
    st.warning("Your admin access was removed or deactivated. Please sign in again.")

if "admin_user" not in st.session_state:
    login_screen()
    st.stop()


# ------------------------------------------------------------------
# Dashboard
# ------------------------------------------------------------------
with st.sidebar:
    if _LOGO.exists():
        st.image(str(_LOGO), width=84)
    st.markdown('<div class="ews-brand">Maize Disease<span>Early Warning System · Admin</span></div>',
                unsafe_allow_html=True)
    st.divider()

# Menu as buttons. The page names below are what every `elif page == "..."`
# compares against; only the icon + label shown is cosmetic.
_PAGES = ["Overview", "Real-Time Monitoring", "Pending Review", "Users", "Images",
          "Risk Alerts", "Analytics", "System Status", "Label Photos", "Model Training",
          "Scalability Test", "Maintenance"]
_NAV_ICONS = {
    "Overview": "📊", "Real-Time Monitoring": "🕒", "Pending Review": "📥",
    "Users": "👥", "Images": "🖼️",
    "Risk Alerts": "🚨", "Analytics": "📈",
    "System Status": "🩺", "Label Photos": "🏷️", "Model Training": "🧠", "Scalability Test": "🧪",
    "Maintenance": "🛠️",
}
_NAV_LABELS = {
    "Overview": "Overview", "Real-Time Monitoring": "Recent submissions",
    "Pending Review": "Pending review", "Users": "Farmer management", "Images": "Image management",
    "Risk Alerts": "Risk alerts",
    "System Status": "System status", "Label Photos": "Label photos", "Model Training": "Model training",
    "Scalability Test": "Scalability test", "Maintenance": "System settings",
}
if st.session_state.get("page") not in _PAGES:
    st.session_state["page"] = "Overview"
for _p in _PAGES:
    if st.sidebar.button(f"{_NAV_ICONS[_p]}  {_NAV_LABELS.get(_p, _p)}", key=f"nav_{_p}",
                         type="primary" if st.session_state["page"] == _p else "secondary",
                         width="stretch"):
        st.session_state["page"] = _p
        st.rerun()
page = st.session_state["page"]


# ------------------------------------------------------------------
# Image-processing mode switch (AUTOMATIC / MANUAL). Persisted in the
# system_settings table so the Drive watcher sees the same value.
# ------------------------------------------------------------------
def _switch_to_manual():
    s = SessionLocal()
    try:
        set_processing_mode(s, ProcessingMode.MANUAL)
    finally:
        s.close()


def _switch_to_automatic_and_drain():
    """Switch to AUTOMATIC and immediately push every held (PENDING_REVIEW)
    image into the pipeline — same as clicking 'Process all'. Returns
    (released, failed)."""
    s = SessionLocal()
    try:
        set_processing_mode(s, ProcessingMode.AUTOMATIC)
        held = (s.query(Image)
                .filter(Image.status == ProcessingStatus.PENDING_REVIEW).all())
        released = failed = 0
        for img in held:
            try:
                submit_image_for_processing(s, img)
                released += 1
            except Exception:  # noqa: BLE001 - marked FAILED inside; keep going
                failed += 1
        return released, failed
    finally:
        s.close()


with SessionLocal() as _s:
    current_mode = get_processing_mode(_s)

st.sidebar.divider()
st.sidebar.markdown(f"**Processing mode** &nbsp; `{current_mode.value}`")

_mode_msg = st.session_state.pop("_mode_switch_msg", None)
if _mode_msg:
    st.sidebar.success(_mode_msg)

if current_mode == ProcessingMode.AUTOMATIC:
    st.sidebar.caption("Uploads are queued for the CNN pipeline immediately.")
    if st.sidebar.button("Switch to MANUAL", icon="🔀"):
        _switch_to_manual()
        st.rerun()
else:
    st.sidebar.caption("Uploads wait in **Pending Review** until you press Process. "
                       "Switching back to AUTOMATIC releases everything held.")
    if st.sidebar.button("Switch to AUTOMATIC", icon="🔀"):
        released, failed = _switch_to_automatic_and_drain()
        msg = f"Switched to AUTOMATIC — released {released} held image(s)"
        if failed:
            msg += f", {failed} failed to publish"
        st.session_state["_mode_switch_msg"] = msg + "."
        st.rerun()
st.sidebar.divider()
st.sidebar.caption(f"Signed in as **{st.session_state['admin_user']['username']}**")
if st.sidebar.button("Log out", icon="🚪", key="logout"):
    del st.session_state["admin_user"]
    st.rerun()

# DASHBOARD_REFRESH_SECONDS (.env): how often Real-Time Monitoring reloads
# itself and Pending Review checks for new uploads; 0 turns auto-refresh off.
import os
_REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "10") or 0)
auto_refresh = page == "Real-Time Monitoring" and _REFRESH_SECONDS > 0
if auto_refresh:
    st.sidebar.caption(f"Real-Time Monitoring auto-refreshes every {_REFRESH_SECONDS}s.")

# One clear title + one line per page, instead of the same long title everywhere.
_PAGE_HEAD = {
    "Overview": ("Overview", "How many leaves were checked, what was found, and what is still waiting."),
    "Real-Time Monitoring": ("Recent submissions", "The latest results, as soon as Spark finishes each photo."),
    "Pending Review": ("Pending review", "Uploads held in manual mode until you release them."),
    "Users": ("Farmer management", "Register farmers, and pause or remove their access."),
    "Images": ("Image management", "Find, compare and track every farmer photo, and manage the local photo "
                                   "cache."),
    "Risk Alerts": ("Risk alerts", "Leaves rated high risk, newest first."),
    "Analytics": ("Analytics", "Disease trends in every municipality over time, and the overall risk picture."),
    "System Status": ("System status", "Whether every part of the pipeline is running right now."),
    "Label Photos": ("Label photos", "Put farmer photos in the right disease class so the next training learns from them."),
    "Model Training": ("Model training","Retrain the disease model on this laptop's GPU and choose when the system uses it."),
    "Scalability Test": ("Scalability test", "Run photos from Google Drive or a folder through Kafka → Spark → "
                                              "CNN and measure how fast the pipeline is."),
    "Maintenance": ("System settings", "Reset the dashboard's history."),
}
_title, _subtitle = _PAGE_HEAD[page]
st.markdown(f'<div class="ews-page-head"><h1>{_title}</h1><p>{_subtitle}</p></div>', unsafe_allow_html=True)


def load_data():
    return analytics.images_with_predictions()


def _spark_analytics_note() -> None:
    """Under the Spark-computed figures: when Spark computed them, or that
    they appear once the EWS-SparkAnalytics service has computed them."""
    when = analytics.analytics_generated_at()
    if when is not None:
        st.caption(f"⚡ Computed by Spark at {format_local(when)}")
    else:
        st.info("The figures appear here once Spark has computed them "
                "(the EWS-SparkAnalytics service that run_all.py starts).")


# ------------------------------------------------------------------
# Google Drive helpers (Farmer Management, Images gallery). Cached so
# pages stay fast; nothing is saved to disk — images are fetched from
# Drive only to be shown.
# ------------------------------------------------------------------
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@st.cache_data(ttl=60, show_spinner=False)
def _farmer_folder_emails():
    """Emails that have a private images/<email> folder in Drive, or None if
    Drive isn't reachable."""
    if not drive_client.is_configured():
        return None
    try:
        return {f["name"].lower() for f in drive_client.list_subfolders(drive_client.images_folder_id())}
    except Exception:  # noqa: BLE001
        return None


@st.cache_data(ttl=3600, max_entries=300, show_spinner=False)
def _drive_bytes(file_id: str) -> bytes:
    return drive_client.download_file(file_id)


@st.cache_data(ttl=300, max_entries=300, show_spinner=False)
def _processed_file_id(image_id: str, email: str):
    folder = drive_client.user_folder_id(email, drive_client.USER_RESULTS_KIND)
    if not folder:
        return None
    found = drive_client.find_file_in_folder(f"{image_id}_processed.jpg", folder)
    return found["id"] if found else None


def _original_image(image_path: str, user_id=None, image_id=None):
    """The original photo for st.image(): the saved local copy of a Drive
    photo when there is one (same bytes, no download), else Drive (cached)."""
    try:
        if is_drive_path(image_path):
            local = downloaded_photo_path(user_id, image_id)
            if local is not None:
                return str(local)
            return _drive_bytes(drive_file_id_from_path(image_path))
        return str(resolve_image_path(image_path))       # photos from the previous design
    except Exception:  # noqa: BLE001 - missing file / Drive hiccup: show "(not found)"
        return None


def _processed_image(image_id: str, email):
    if not email:
        return None
    try:
        file_id = _processed_file_id(image_id, email)
        return _drive_bytes(file_id) if file_id else None
    except Exception:  # noqa: BLE001
        return None


# ------------------------------------------------------------------
# System Status checks
# ------------------------------------------------------------------
@st.cache_data(ttl=15, show_spinner="Checking the system…")
def _system_status() -> dict:
    from config import settings
    out = {"checked_at": now_local()}
    procs = _python_command_lines()

    # Google Drive
    if not drive_client.is_configured():
        out["drive"] = ("bad", ["Not set up on this laptop — run `python scripts\\drive_authorize.py`."])
    else:
        try:
            info = drive_client.account_info()
            n = len(drive_client.list_subfolders(drive_client.images_folder_id()))
            lines = [f"Connected as **{info['email']}**", f"{n} farmer folder(s) registered"]
            if info["usage_bytes"] is not None:
                used = info["usage_bytes"] / 1e9
                lines.append(f"Storage used: {used:.2f} GB"
                             + (f" of {info['limit_bytes'] / 1e9:.2f} GB" if info["limit_bytes"] else ""))
            level = "ok"
            if info["limit_bytes"] and info["usage_bytes"] and info["usage_bytes"] > 0.9 * info["limit_bytes"]:
                level = "warn"
                lines.append("Storage is over 90% full.")
            out["drive"] = (level, lines)
        except Exception as e:  # noqa: BLE001
            out["drive"] = ("bad", [f"Cannot reach Drive: {str(e)[:160]}",
                                    "If it worked before, the login may have expired — re-run "
                                    "`python scripts\\drive_authorize.py`."])

    # Drive watcher
    if procs is None:
        out["watcher"] = ("unknown", ["Could not check running processes on this computer."])
    elif "kafka.drive_watcher" in procs or "drive_watcher.py" in procs:
        out["watcher"] = ("ok", ["Running — checks every farmer's folder every few seconds."])
    else:
        out["watcher"] = ("bad", ["Not running — new uploads are not picked up.",
                                  "Start it with `python run_all.py` (or `python -m kafka.drive_watcher`)."])

    # Kafka
    from kafka.config import broker_reachable
    if broker_reachable():
        lines = [f"Broker reachable on {settings.KAFKA_BOOTSTRAP_SERVERS}, topic `{settings.KAFKA_TOPIC}`"]
        try:
            from confluent_kafka import Consumer, TopicPartition
            c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                          "group.id": "ews-dashboard-status", "enable.auto.commit": False})
            topic = c.list_topics(settings.KAFKA_TOPIC, timeout=5).topics.get(settings.KAFKA_TOPIC)
            held = 0
            for p in (topic.partitions if topic and topic.partitions else [0]):
                low, high = c.get_watermark_offsets(TopicPartition(settings.KAFKA_TOPIC, p), timeout=5)
                held += high - low
            c.close()
            lines.append(f"{held} message(s) in the topic since it was last emptied (factory reset)")
        except Exception:  # noqa: BLE001
            lines.append("(message count not available)")
        out["kafka"] = ("ok", lines)
    else:
        lines = [f"Not reachable on {settings.KAFKA_BOOTSTRAP_SERVERS} — start it with `docker compose up -d kafka`."]
        if settings.KAFKA_USE_LOCAL_FALLBACK:
            lines.append("Uploads go to the local file queue instead, which Spark does not read.")
        out["kafka"] = ("bad", lines)

    # Spark
    if procs is None:
        out["spark"] = ("unknown", ["Could not check running processes on this computer."])
    elif "spark.streaming_job" in procs or "streaming_job.py" in procs:
        out["spark"] = ("ok", ["Running — reads Kafka, downloads photos from Drive, "
                               "writes results and processed images back to Drive."])
    else:
        out["spark"] = ("bad", ["Not running — photos wait at INGESTED.",
                                "Check the EWS-Spark window, or restart with `python run_all.py`."])

    # Database
    if settings.DATABASE_URL.startswith("postgresql"):
        kind = "PostgreSQL"
    else:
        kind = "SQLite (" + settings.DATABASE_URL.split("///", 1)[-1] + ")"
    try:
        with SessionLocal() as s_:
            n_users = s_.query(User).filter(User.is_deleted.is_(False)).count()
            n_images = s_.query(Image).count()
            n_preds = s_.query(Prediction).count()
        out["database"] = ("ok", [f"{kind} — connected",
                                  f"{n_users} user(s), {n_images} image(s), {n_preds} prediction(s)"])
    except Exception as e:  # noqa: BLE001
        out["database"] = ("bad", [f"{kind} — cannot connect: {str(e)[:160]}"])

    # Backlog
    try:
        cutoff = now_utc() - timedelta(minutes=2)
        with SessionLocal() as s_:
            stuck = (s_.query(Image)
                     .filter(Image.status.in_([ProcessingStatus.UPLOADED, ProcessingStatus.INGESTED,
                                               ProcessingStatus.PROCESSING]),
                             Image.uploaded_at < cutoff, Image.source != SCALE_TEST_SOURCE).count())
            pending = s_.query(Image).filter(Image.status == ProcessingStatus.PENDING_REVIEW).count()
            failed = (s_.query(Image).filter(Image.status == ProcessingStatus.FAILED,
                                             Image.uploaded_at >= now_utc() - timedelta(hours=24)).count())
            undelivered = count_owed(s_)["undelivered"]
        lines = [f"{stuck} photo(s) waiting longer than 2 minutes",
                 f"{pending} photo(s) held for review (manual mode)",
                 f"{failed} failed in the last 24 hours",
                 f"{undelivered} finished result(s) not delivered to the farmer's Google Drive"]
        stuck += undelivered
        if stuck:
            lines.append("To re-send them: `python -m storage.requeue_stuck --all`")
        out["backlog"] = ("warn" if stuck else "ok", lines)
    except Exception as e:  # noqa: BLE001
        out["backlog"] = ("unknown", [str(e)[:160]])
    return out


# Only the pages below use the full images x predictions table; the others
# no longer load it on every rerun.
df = load_data() if page in ("Real-Time Monitoring", "Risk Alerts", "Analytics") else None

if page == "Overview":
    # Real numbers only, computed by Spark (spark/batch_analytics.py). The
    # mockup this page's layout follows also shows "Active Remote Devices" —
    # this system has no device registry, so that metric isn't something we
    # can honestly report; it's left out rather than faked.
    kpi = analytics.kpi_summary()
    dist = analytics.disease_distribution()

    st.subheader("📊 System Performance")
    if kpi is not None:
        p1, p2, p3 = st.columns(3)
        p1.metric("Submissions (Last 24h)", kpi["submissions_24h"])
        p2.metric("Active Farmers (Last 24h)", kpi["active_farmers_24h"])
        p3.metric("Most Detected Disease", kpi["most_detected_disease"] or "—")
        st.caption(f"{kpi['total_users']} total farmers · {kpi['total_images']} total submissions all-time")

        # Explicit healthy/diseased/uncertain/unclassified breakdown so
        # "unclassified" (not yet processed, or failed) is a labeled bucket
        # instead of a silent gap where healthy+diseased looked like "0 and 0"
        # with no explanation.
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Healthy", kpi["total_healthy"])
        c2.metric("Diseased", kpi["total_diseased"])
        c3.metric("Uncertain (low confidence)", kpi["total_uncertain"])
        c4.metric("Unclassified", kpi["total_unclassified"])
        if kpi["total_unclassified"] and kpi["total_images"]:
            st.caption(
                "Unclassified submissions have no disease classification yet — they are still "
                "pending, or their processing failed."
            )
    _spark_analytics_note()

    st.divider()

    if dist.empty:
        st.caption("No disease breakdown yet.")
    else:
        # "Healthy" is a verdict (status color), every other class is an
        # open-ended identity (fixed categorical order) — never mix the
        # two into one arbitrary cycling palette.
        color_map, hue_i = {}, 0
        for cls in dist["predicted_class"]:
            if str(cls).lower() in DISEASE_COLOR_MAP:
                color_map[cls] = DISEASE_COLOR_MAP[str(cls).lower()]
            else:
                color_map[cls] = CATEGORICAL_HUES[hue_i % len(CATEGORICAL_HUES)]
                hue_i += 1
        st.plotly_chart(
            px.pie(dist, names="predicted_class", values="count", hole=0.55,
                   color="predicted_class", color_discrete_map=color_map),
            width="stretch",
        )

    st.divider()

    st.subheader("📋 Analysis Queue")
    st.caption("The most recent submissions, whatever their status — not just the completed ones.")

    session = SessionLocal()
    try:
        recent_rows = (
            session.query(Image, User)
            .join(User, User.user_id == Image.user_id)
            .filter(Image.source != SCALE_TEST_SOURCE)
            .order_by(Image.uploaded_at.desc())
            .limit(8)
            .all()
        )
        if not recent_rows:
            st.info("No submissions yet.")
        for img, farmer in recent_rows:
            pred = (
                session.query(Prediction)
                .filter(Prediction.image_id == img.image_id)
                .order_by(Prediction.predicted_at.desc())
                .first()
            )
            with st.container(border=True):
                qc1, qc2, qc3, qc4 = st.columns([2, 1, 2, 2])
                with qc1:
                    st.markdown(f"**#{img.image_id[:8]}**")
                    st.caption(f"Farmer: {farmer.full_name or farmer.username}")
                with qc2:
                    thumb = _original_image(img.image_path, img.user_id, img.image_id)
                    if thumb is not None:
                        st.image(thumb, width=64)
                    else:
                        st.caption("(no file)")
                with qc3:
                    st.badge(img.status.value, color=STATUS_BADGE_COLOR.get(img.status.value, "gray"))
                    if pred:
                        st.caption(f"{pred.predicted_class} ({(pred.prediction_confidence or 0) * 100:.2f}% confidence)")
                with qc4:
                    with st.expander("View details"):
                        st.write({
                            "image_id": img.image_id, "location": img.location,
                            # stored UTC, shown in Timor-Leste Time (config/localtime.py)
                            "uploaded_at": format_local(img.uploaded_at),
                            "captured_at": format_local(img.captured_at) or None,
                            "gps": (f"{img.gps_latitude}, {img.gps_longitude}"
                                    if img.gps_latitude is not None else None),
                            "camera": " ".join(filter(None, [img.camera_make, img.camera_model])) or None,
                            "status": img.status.value,
                            "error_message": img.error_message,
                            "prediction": None if pred is None else {
                                "predicted_class": pred.predicted_class,
                                "confidence": pred.prediction_confidence,
                                "risk_level": pred.risk_level.value if pred.risk_level else None,
                                "risk_score": pred.risk_score,
                            },
                        })
                    # Only offer "Reprocess" where it actually means
                    # something — there's no "Flag for Review" feature in
                    # this system, so that mockup action isn't reproduced.
                    # Not for an upload the Drive watcher refused (inactive
                    # account, someone else's folder, ...): that stays refused.
                    if (img.status in (ProcessingStatus.FAILED, ProcessingStatus.PENDING_REVIEW)
                            and not is_upload_rejected(img)):
                        if st.button("Reprocess", icon="🔄", key=f"reprocess_{img.image_id}"):
                            try:
                                submit_image_for_processing(session, img)
                                st.success("Re-queued.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Couldn't re-queue: {e}")
    finally:
        session.close()

elif page == "Real-Time Monitoring":
    st.subheader("Latest incoming processed results")
    recent = df.dropna(subset=["predicted_at"]).sort_values("predicted_at", ascending=False).head(25)
    if recent.empty:
        st.info("No processed images yet.")
    else:
        st.dataframe(
            recent[["location", "predicted_class", "prediction_confidence", "risk_score",
                    "risk_level", "uploaded_at", "predicted_at"]],
            width="stretch", column_config=TIME_COLUMNS,
        )

elif page == "Pending Review":
    st.subheader("Images held for manual processing")
    st.caption(
        "In MANUAL mode an upload is stored but not sent to the CNN pipeline. "
        "Pressing **Process** publishes it to Kafka and marks it INGESTED — the "
        "exact same step automatic mode does on upload — and the Spark job takes "
        "it from there."
    )

    def _held_ids() -> list:
        with SessionLocal() as s:
            return [i for (i,) in s.query(Image.image_id)
                    .filter(Image.status == ProcessingStatus.PENDING_REVIEW)
                    .order_by(Image.uploaded_at.desc())]

    # Auto refresh: a cheap id-only check every _REFRESH_SECONDS; the whole
    # page (with its photos) is only redrawn when the held list changed —
    # new uploads arrived, or photos were processed elsewhere.
    if _REFRESH_SECONDS > 0:
        @st.fragment(run_every=_REFRESH_SECONDS)
        def _watch_held(shown: list):
            if _held_ids() != shown:
                st.rerun(scope="app")
            st.caption(f"🔄 Checks for new uploads every {_REFRESH_SECONDS}s.")

    session = SessionLocal()
    try:
        held = (
            session.query(Image)
            .filter(Image.status == ProcessingStatus.PENDING_REVIEW)
            .order_by(Image.uploaded_at.desc())
            .all()
        )
        if _REFRESH_SECONDS > 0:
            _watch_held([img.image_id for img in held])

        if not held:
            st.success(
                f"Nothing is waiting. Current mode: `{get_processing_mode(session).value}`."
            )
        else:
            st.write(f"**{len(held)}** image(s) awaiting processing.")
            if st.button(f"Process all {len(held)}", icon="⚡", type="primary"):
                ok = fail = 0
                for img in held:
                    try:
                        submit_image_for_processing(session, img)
                        ok += 1
                    except Exception as e:  # noqa: BLE001 - surface, keep going
                        fail += 1
                        st.error(f"{img.image_id[:8]}…: {e}")
                st.success(f"Released {ok} image(s)." + (f" {fail} failed." if fail else ""))
                st.rerun()

            st.divider()
            for img in held:
                with st.container(border=True):
                    c1, c2 = st.columns([4, 1])
                    with c1:
                        st.markdown(f"`{img.image_id}`")
                        st.caption(
                            f"{img.location or 'no location'} · user "
                            f"{img.user_id[:8]}… · uploaded {format_local(img.uploaded_at)}"
                        )
                        # local copy when there is one, else Drive (fetched once, cached)
                        photo = _original_image(img.image_path, img.user_id, img.image_id)
                        if photo is not None:
                            st.image(photo, width=180)
                        else:
                            st.caption("(image file not found on disk or in Google Drive)")
                    if c2.button("Process", key=f"process_{img.image_id}", type="primary"):
                        try:
                            submit_image_for_processing(session, img)
                            st.success(f"Released {img.image_id[:8]}… into the pipeline.")
                            st.rerun()
                        except Exception as e:  # noqa: BLE001
                            st.error(f"Failed to queue: {e}")
    finally:
        session.close()

elif page == "Users":
    PAGE_SIZE = 10

    # ------------------------------------------------------------------
    # Search / filter
    # ------------------------------------------------------------------
    fcol1, fcol2, fcol3 = st.columns([2, 1, 1])
    search_term = fcol1.text_input("Search (name, email, or username)", key="user_search")
    role_filter = fcol2.selectbox("Role", ["All"] + [r.value for r in Role], key="user_role_filter")
    status_filter = fcol3.selectbox("Status", ["All"] + [s.value for s in UserStatus], key="user_status_filter")
    show_deleted = st.checkbox("Include deleted users", value=False, key="user_show_deleted")

    session = SessionLocal()
    try:
        # the scalability test's system account is not a farmer
        query = session.query(User).filter(User.username != SCALE_TEST_USERNAME)
        if not show_deleted:
            query = query.filter(User.is_deleted.is_(False))
        if search_term:
            like = f"%{search_term.strip()}%"
            query = query.filter(sa_or(User.full_name.ilike(like), User.email.ilike(like),
                                        User.username.ilike(like)))
        if role_filter != "All":
            query = query.filter(User.role == Role(role_filter))
        if status_filter != "All":
            query = query.filter(User.status == UserStatus(status_filter))

        total = query.count()
        max_page = max(1, -(-total // PAGE_SIZE))
        page_num = st.number_input("Page", min_value=1, max_value=max_page, value=1, step=1, key="user_page")
        users = (query.order_by(User.created_at.desc())
                 .offset((page_num - 1) * PAGE_SIZE).limit(PAGE_SIZE).all())

        farmer_folders = _farmer_folder_emails()
        st.dataframe(pd.DataFrame([{
            "ID": u.user_id, "Full Name": u.full_name, "Gmail": u.email, "Role": u.role.value,
            "Drive folders": ("—" if u.role == Role.ADMIN else
                              "unknown" if farmer_folders is None else
                              "✅ yes" if u.email in farmer_folders else "❌ not registered"),
            "Status": u.status.value,
            "Created": format_local(u.created_at), "Updated": format_local(u.updated_at),
            "Deleted": u.is_deleted,
        } for u in users]), width="stretch")
        st.caption(f"{total} user(s) total — page {page_num} of {max_page}")
        user_choices = {f"{u.full_name or u.username} ({u.email})": u.user_id for u in users}
    finally:
        session.close()

    st.divider()

    # ------------------------------------------------------------------
    # Register farmer (Google Drive)
    # ------------------------------------------------------------------
    with st.expander("Register farmer", icon="➕", expanded=False):
        st.markdown(
            "Farmers sign in to the app with their **own Google account** — there are no "
            "passwords. Registering a farmer gives them their **private Google Drive "
            "folders** (`images/<email>` to upload, `results/<email>` to read results), "
            "shared only with them. They also appear in the list above straight away."
        )
        with st.form("register_farmer_form", clear_on_submit=True):
            r_emails = st.text_area("Farmer Gmail address(es) — one per line",
                                    placeholder="farmer1@gmail.com\nfarmer2@gmail.com")
            register_submitted = st.form_submit_button("Register farmer")

        if register_submitted:
            emails = [normalize_email(e) for e in r_emails.replace(",", "\n").splitlines() if e.strip()]
            bad = [e for e in emails if not _EMAIL_RE.match(e)]
            if not emails:
                st.error("Enter at least one Gmail address.")
            elif bad:
                st.error("Not a valid email address: " + ", ".join(bad))
            elif not drive_client.is_configured():
                st.error("Google Drive is not set up on this laptop yet — run "
                         "`python scripts\\drive_authorize.py` first (see README 'Google Drive setup').")
            else:
                from kafka.drive_watcher import get_or_create_farmer
                done = []
                for email in emails:
                    try:
                        drive_client.register_farmer(email)
                        with SessionLocal() as s_reg:
                            get_or_create_farmer(s_reg, email, None)
                        done.append(email)
                    except Exception as e:  # noqa: BLE001 - show every failure, keep going
                        st.error(f"{email}: could not create/share the Drive folders — {e}")
                if done:
                    _farmer_folder_emails.clear()
                    st.session_state["_registered_msg"] = "Registered: " + ", ".join(done)
                    st.session_state["_needs_test_user"] = done
                    st.rerun()          # redraw so the list above already shows them

        if st.session_state.get("_registered_msg"):
            st.success(st.session_state.pop("_registered_msg"))
        needs = st.session_state.get("_needs_test_user")
        if needs:
            project = drive_client.google_cloud_project_id()
            link = (f"https://console.cloud.google.com/auth/audience?project={project}"
                    if project else "https://console.cloud.google.com/auth/audience")
            st.warning(
                "**One more step, by hand:** add "
                + ", ".join(f"`{e}`" for e in needs)
                + f" as **Test users** in Google Cloud — [open the Test users page]({link}) "
                "→ **+ Add users** → Save. Google offers no way to do this automatically, "
                "and without it the farmer's Google sign-in is blocked.",
                icon="⚠️",
            )
            if st.button("Done — I added them as test users", key="test_users_done"):
                del st.session_state["_needs_test_user"]
                st.rerun()

    # ------------------------------------------------------------------
    # Update / Delete
    # ------------------------------------------------------------------
    with st.expander("Edit or delete a user", icon="✏️"):
        if not user_choices:
            st.info("No users match the current search/filter.")
        else:
            selected_label = st.selectbox("Select a user", list(user_choices.keys()), key="user_edit_select")
            selected_id = user_choices[selected_label]

            session = SessionLocal()
            try:
                target = session.get(User, selected_id)
                is_self = target.user_id == st.session_state["admin_user"]["user_id"]

                with st.form("edit_user_form"):
                    e_full_name = st.text_input("Full Name", value=target.full_name or "")
                    st.text_input("Gmail address (their Google account — cannot be changed here)",
                                  value=target.email, disabled=True)
                    e_role = st.selectbox("Role", [r.value for r in Role],
                                           index=[r.value for r in Role].index(target.role.value))
                    e_status = st.selectbox(
                        "Status", [s.value for s in UserStatus],
                        index=[s.value for s in UserStatus].index(target.status.value),
                        help="Anything other than ACTIVE blocks this farmer's new uploads "
                             "(the Drive watcher rejects them and tells the app why).")
                    update_submitted = st.form_submit_button("Save changes")

                if update_submitted:
                    if is_self and e_role != Role.ADMIN.value:
                        st.error("You cannot remove your own admin role.")
                    elif is_self and e_status != UserStatus.ACTIVE.value:
                        st.error("You cannot change your own account status.")
                    elif not e_full_name.strip():
                        st.error("Full name cannot be empty.")
                    else:
                        target.full_name = e_full_name.strip()
                        target.role = Role(e_role)
                        if e_status != target.status.value:
                            target.status = UserStatus(e_status)
                            if target.status == UserStatus.ACTIVE:
                                target.email_verified = True
                        session.commit()
                        st.success("User updated.")
                        st.rerun()

                st.divider()
                st.markdown("**Delete user**")
                if is_self:
                    st.caption("You cannot delete your own admin account.")
                else:
                    confirm_delete = st.checkbox(
                        f"I understand this will deactivate and remove {target.full_name or target.email}",
                        key=f"confirm_delete_{target.user_id}",
                    )
                    st.caption("Deleting blocks their future uploads. Their Drive folders and "
                               "past results stay; remove their access in Google Drive if needed.")
                    if st.button("Delete user", icon="🗑️", disabled=not confirm_delete):
                        target.is_deleted = True
                        target.deleted_at = now_utc()
                        target.status = UserStatus.INACTIVE
                        session.commit()
                        st.success("User deleted (soft delete).")
                        st.rerun()
            finally:
                session.close()

elif page == "Images":
    # Image management: PostgreSQL catalog + local cache + Google Drive
    # (dashboard/image_management_page.py)
    from dashboard import image_management_page
    image_management_page.render(processed_image=_processed_image)


elif page == "Risk Alerts":
    by_location = analytics.high_risk_locations()
    if not by_location.empty:
        n_high, n_total = int(by_location["high"].sum()), int(by_location["total"].sum())
        st.subheader("HIGH risk by municipality")
        st.markdown(f"**{n_high} of {n_total}** analysed maize photos are HIGH risk "
                    f"({n_high / n_total * 100:.2f}%), across {len(by_location)} "
                    + ("municipality." if len(by_location) == 1 else "municipalities."))
        st.dataframe(
            by_location.assign(summary=[f"{h} of {t} maize photo{'' if t == 1 else 's'}" for h, t in
                                        zip(by_location["high"], by_location["total"])]),
            column_order=["location", "summary", "high_pct"],
            column_config={
                "location": st.column_config.TextColumn("Municipality"),
                "summary": st.column_config.TextColumn("HIGH risk"),
                "high_pct": st.column_config.ProgressColumn("Share HIGH", format="%.2f%%",
                                                            min_value=0, max_value=100),
            },
            hide_index=True, width="stretch",
        )
        st.caption("Counts only photos that were analysed (photos rejected as not a leaf or not "
                   "maize are left out). Each photo counts once, with its latest result.")
    _spark_analytics_note()
    if not by_location.empty:
        st.divider()
        st.subheader("Latest HIGH risk photos")

    alerts = df[df["risk_level"] == "HIGH"].sort_values("predicted_at", ascending=False)
    if alerts.empty:
        st.success("No active HIGH-risk alerts.")
    else:
        for _, row in alerts.head(20).iterrows():
            with st.container(border=True):
                badge_col, title_col = st.columns([1, 5])
                with badge_col:
                    st.badge("HIGH RISK", icon="🚨", color="red")
                with title_col:
                    st.markdown(f"**{row['location'] or 'Unknown location'}**")
                cols = st.columns(3)
                cols[0].metric("Disease", row["predicted_class"] or "—")
                cols[1].metric("Confidence", f"{(row['prediction_confidence'] or 0)*100:.2f}%")
                cols[2].metric("Risk Score", f"{row['risk_score']:.2f}" if pd.notna(row["risk_score"]) else "—")
                st.caption(
                    f"Captured: {format_local(row['captured_at']) or '—'}  •  "
                    f"Uploaded: {format_local(row['uploaded_at'])}  •  "
                    f"Predicted: {format_local(row['predicted_at'])}"
                    + (f"  •  GPS: {row['gps_latitude']:.5f}, {row['gps_longitude']:.5f}"
                       if pd.notna(row.get("gps_latitude")) else "")
                )

elif page == "Analytics":
    dist = analytics.disease_distribution()
    if not dist.empty:
        st.subheader("🦠 Disease Distribution")
        st.plotly_chart(
            px.bar(dist, x="predicted_class", y="count", color="predicted_class",
                   color_discrete_map=DISEASE_COLOR_MAP, color_discrete_sequence=CATEGORICAL_HUES),
            width="stretch",
        )
        with st.expander("All predictions (table)"):
            st.dataframe(
                df.dropna(subset=["predicted_class"])[
                    ["location", "predicted_class", "prediction_confidence", "risk_level", "predicted_at"]
                ].sort_values("predicted_at", ascending=False),
                width="stretch", column_config=TIME_COLUMNS,
            )
    # Disease trend in every municipality (replaced the risk-score trend):
    # photos per disease per day, one panel per municipality.
    rt = analytics.disease_trend_by_location()
    if not rt.empty:
        st.subheader("📈 Disease Trend by Municipality")
        places = rt.groupby("location")["photos"].sum().sort_values(ascending=False).index.tolist()
        st.caption(f"Photos per disease per day (Timor-Leste Time) in each of the {len(places)} "
                   f"municipalit{'y' if len(places) == 1 else 'ies'}, most photos first. Each panel has its "
                   "own scale so small municipalities stay readable — compare panels by their numbers.")
        fig = px.bar(rt, x="day", y="photos", color="predicted_class", facet_col="location",
                     facet_col_wrap=3, category_orders={"location": places},
                     color_discrete_map=DISEASE_COLOR_MAP, color_discrete_sequence=CATEGORICAL_HUES,
                     labels={"day": "Day", "photos": "Photos", "predicted_class": "Disease"},
                     height=max(320, 260 * -(-len(places) // 3)), facet_row_spacing=0.12)
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=", 1)[-1]))   # "location=Dili" -> "Dili"
        fig.update_yaxes(matches=None, showticklabels=True)
        fig.update_xaxes(showticklabels=True, tickformat="%d/%m", dtick="D1")
        fig.update_layout(barmode="stack", legend_title_text="Disease")
        st.plotly_chart(fig, width="stretch")
    rdist = analytics.risk_distribution()
    if not rdist.empty:
        st.subheader("🥧 Risk Distribution")
        st.plotly_chart(
            px.pie(rdist, names="risk_level", values="count", color="risk_level",
                   color_discrete_map=RISK_COLOR_MAP),
            width="stretch",
        )
    dt = analytics.disease_daily()
    if not dt.empty:
        st.subheader("🦠 Disease Trend")
        fig = px.bar(dt, x="day", y="photos", color="predicted_class",
                     color_discrete_map=DISEASE_COLOR_MAP, color_discrete_sequence=CATEGORICAL_HUES,
                     labels={"day": "Day (Timor-Leste Time)", "photos": "Photos", "predicted_class": "Disease"})
        fig.update_xaxes(tickformat="%d/%m")
        fig.update_layout(barmode="stack", legend_title_text="Disease")
        st.plotly_chart(fig, width="stretch")
    if dist.empty and rt.empty and rdist.empty and dt.empty and analytics.analytics_generated_at() is not None:
        st.info("No analytics available yet — no images have been fully processed.")
    _spark_analytics_note()

elif page == "System Status":
    st.caption("Android → Google Drive → Kafka → Spark → Google Drive → Android. "
               "Checked when you open the page or press Re-check.")
    if st.button("Re-check", icon="🔄"):
        _system_status.clear()
    status = _system_status()

    def _show(title, level, lines):
        icon = {"ok": "✅", "warn": "⚠️", "bad": "❌", "unknown": "❔"}[level]
        with st.container(border=True):
            st.markdown(f"**{icon} {title}**")
            for line in lines:
                st.caption(line)

    left, right = st.columns(2)
    with left:
        _show("Google Drive", *status["drive"])
        _show("Drive watcher (Drive → Kafka)", *status["watcher"])
        _show("Kafka", *status["kafka"])
    with right:
        _show("Spark streaming job (Kafka → model → Drive)", *status["spark"])
        _show("Database", *status["database"])
        _show("Pipeline backlog", *status["backlog"])
    st.caption(f"Checked at {format_local(status['checked_at'], with_seconds=True)}.")

elif page == "Label Photos":
    from dashboard import labelling_page
    labelling_page.render()

elif page == "Model Training":
    from dashboard import training_page
    training_page.render()

elif page == "Scalability Test":
    from dashboard import scalability_page
    scalability_page.render()

elif page == "Maintenance":
    st.subheader("⚠️ Factory reset")
    st.markdown(
        "This clears the **dashboard's history** and reloads it. "
        "**User and admin accounts are kept.** It deletes:\n\n"
        "- all `images`, `predictions`, `processing_events` and `rejected_events` rows "
        "(all disease history, risk alerts, rejected uploads and performance data shown here)\n"
        "- Spark Structured Streaming checkpoints under `storage/spark_checkpoints/`\n"
        "- the Kafka local file-queue under `storage/local_queue/`\n"
        "- old photo files from the previous design under `storage/images/`\n"
        "- the Drive watcher's bookmark, so after the restart **every photo still in Google Drive "
        "is analysed again** (original upload time, location and GPS kept), read from its local "
        "copy without downloading it again. In MANUAL mode they wait in Pending review.\n\n"
        "Optionally (tick below) it also deletes:\n\n"
        "- **Kafka's messages** — recommended: otherwise the restarted Spark job, "
        "whose checkpoints were cleared, replays every old photo and the history comes back\n"
        "- farmers' **photos and results in Google Drive** — so the phone app's History "
        "matches this dashboard. Results go to the central account's Drive trash "
        "(recoverable for 30 days); farmer-owned photos are taken out of their folders. "
        "Farmers' folders and sharing are kept, so they can keep uploading. "
        "Each phone also clears the history it saved, the next time the farmer "
        "opens Home or History (photos still waiting to upload are kept).\n\n"
        "This cannot be undone."
    )
    also_kafka = st.checkbox("Also delete all Kafka messages", value=True)
    also_drive = st.checkbox("Also delete every farmer's photos and results in Google Drive", value=False)
    also_mail = st.checkbox(
        "Also clear the old mail outbox (`storage/mail_outbox/`)",
        value=False,
        help="Only used by the previous password/verification-code login.",
    )
    st.caption(
        "Stop the Spark job and the Drive watcher **before** resetting, and start "
        "them again afterwards."
    )
    from storage.reset import reset_preview
    _preview = reset_preview()
    with st.container(border=True):
        st.markdown("**Counted now: what the reset deletes and what it keeps**")
        _del, _keep = st.columns(2)
        _del.markdown("Deleted")
        _del.dataframe(pd.DataFrame(
            [{"What": f"{k} rows", "Count": v} for k, v in _preview["deleted_rows"].items()]
            + [{"What": k + " (files)", "Count": v} for k, v in _preview["deleted_files"].items()]),
            hide_index=True, width="stretch")
        _keep.markdown("Kept")
        _keep.dataframe(pd.DataFrame(
            [{"What": f"{k} rows", "Count": v} for k, v in _preview["kept_rows"].items()]
            + [{"What": k + " (files)", "Count": v} for k, v in _preview["kept_files"].items()]),
            hide_index=True, width="stretch")
        st.caption(_preview["kept_note"] + " Farmers' Drive photos and results are only deleted if you tick "
                   "that box above.")
    from storage.reset import spark_job_running, drive_watcher_running
    spark_running = spark_job_running()
    watcher_running = drive_watcher_running()
    if spark_running:
        st.error("The Spark streaming job is running. Stop it first (Ctrl+C in the EWS-Spark "
                 "window): resetting under a running job corrupts its checkpoint and Spark "
                 "then fails with \"batch … doesn't exist\".")
    if watcher_running:
        st.error("The Drive watcher is running. Stop it first (Ctrl+C in the EWS-DriveWatcher "
                 "window): an upload that arrives during the reset could be registered and then "
                 "lost with the cleared Kafka messages.")
    confirm = st.text_input('Type RESET to confirm', key="reset_confirm")
    if st.button("Factory reset now", icon="🗑️", type="primary",
                 disabled=confirm != "RESET" or bool(spark_running) or bool(watcher_running)):
        with st.spinner("Wiping data and history…"):
            summary = factory_reset(clear_mail_outbox=also_mail, clear_kafka=also_kafka,
                                    clear_drive=also_drive,
                                    performed_by=st.session_state["admin_user"]["username"])
        # Shown after the rerun below (which reloads the emptied dashboard).
        st.session_state["reset_summary"] = summary
        st.rerun()
    if "reset_summary" in st.session_state:
        summary = st.session_state.pop("reset_summary")
        st.success("Factory reset complete. Users and admins were kept.")
        kafka_status = summary.get("kafka_topic") or ""
        drive_errors = (summary.get("drive_files") or {}).get("errors") or []
        if kafka_status.startswith("FAILED"):
            st.warning(f"Kafka was not cleared: {kafka_status}")
        if drive_errors:
            st.warning(f"{len(drive_errors)} Google Drive file(s) could not be removed — see details below.")
        st.json(summary)

if auto_refresh:
    import time
    time.sleep(_REFRESH_SECONDS)
    st.rerun()
