"""
REST API server (DIT manual sections 11, 33-36).

The Android app (USER) and the admin tooling both talk to this server —
neither talks to the database directly (section 37).

Upload workflow (section 11):
    Android -> POST /upload-image -> validate -> extract EXIF -> generate
    image_id -> save original image -> generate uploaded_at on server ->
    create database image record -> publish Kafka event -> return ack.

This endpoint does NOT run the CNN synchronously — that's Spark Structured
Streaming's job (spark/streaming_job.py), consuming the Kafka event this
endpoint publishes. The response here is an acknowledgement, not a
prediction (section 11/36).

Run locally:
    python -m storage.init_db   # creates tables + bootstrap admin, once
    uvicorn api.server:app --host 0.0.0.0 --port 8000
"""

import math
import re
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, UploadFile, Form, HTTPException, Depends, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
from sqlalchemy import text, or_, func, case, select
from sqlalchemy.orm import Session, aliased

from config.settings import (
    KAFKA_TOPIC, VERIFICATION_CODE_TTL_MINUTES,
)
from storage.database import (
    init_db, User, Image, Prediction,
    Role, UserStatus, ProcessingStatus, ProcessingMode,
    get_processing_mode, set_processing_mode, now_utc,
)
from storage.pipeline_submit import submit_image_for_processing
from api.auth import (
    hash_password, verify_password, create_access_token,
    normalize_email, is_valid_gmail_address, password_policy_errors,
)
from api.deps import get_db, get_current_user, require_admin, assert_owns_image
from api.exif_utils import extract_captured_at
from api.image_storage import (
    save_image, InvalidImage, resolve_image_path, compute_content_hash,
    is_drive_path, downloaded_photo_path, load_image_bytes,
)
from api import verification as vcode
from api.mailer import send_verification_email, EmailSendError
from api.rate_limit import allow as rate_limit_allow
from kafka.producer import get_backend

_kafka_backend = None


def kafka_backend():
    global _kafka_backend
    if _kafka_backend is None:
        _kafka_backend = get_backend(KAFKA_TOPIC)
    return _kafka_backend


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Crop Disease Early Warning System — Image System API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========================================================================
# Schemas
# ========================================================================
class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    user_id: str
    username: str


class RegisterRequest(BaseModel):
    full_name: str
    email: str
    password: str
    confirm_password: str


class VerifyEmailRequest(BaseModel):
    email: str
    code: str


class ResendVerificationRequest(BaseModel):
    email: str


class CreateUserRequest(BaseModel):
    full_name: str
    email: str
    password: str
    # Server always decides the role that actually gets persisted for
    # self-registration (see register() below, which never even reads a
    # role from the client). This field only affects the ADMIN "Create
    # User" tool, and even here is validated against the Role enum.
    role: str = "USER"
    username: Optional[str] = None  # defaults to the (normalized) email if omitted
    # Optional admin override: if omitted, new users go through the normal
    # PENDING_VERIFICATION + emailed-code flow (section 9: "Do not
    # automatically mark an arbitrary Gmail address as verified unless the
    # system has a legitimate verification mechanism"). An admin may
    # explicitly set status=ACTIVE to skip that — a deliberate, logged
    # administrative action, not an automatic bypass.
    status: Optional[str] = None


class UpdateUserRequest(BaseModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    status: Optional[str] = None
    role: Optional[str] = None


class ProcessingModeResponse(BaseModel):
    mode: str  # "AUTOMATIC" | "MANUAL"
    # Non-zero only on a MANUAL -> AUTOMATIC switch: how many images that
    # were held at PENDING_REVIEW got pushed into the pipeline by the
    # switch, and how many failed to publish.
    released: int = 0
    released_failed: int = 0


class SetProcessingModeRequest(BaseModel):
    mode: str


# ========================================================================
# Health
# ========================================================================
@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False
    return {"status": "ok" if db_ok else "degraded", "database": "ok" if db_ok else "unreachable"}


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


_LOGIN_MAX_PER_WINDOW = 10
_LOGIN_WINDOW_SECONDS = 60
_REGISTER_MAX_PER_WINDOW = 5
_REGISTER_WINDOW_SECONDS = 3600
_VERIFY_MAX_PER_WINDOW = 20
_VERIFY_WINDOW_SECONDS = 600
_RESEND_MAX_PER_WINDOW = 5
_RESEND_WINDOW_SECONDS = 3600


# ========================================================================
# Auth
# ========================================================================
@app.post("/auth/login", response_model=TokenResponse)
def login(payload: LoginRequest, request: Request, db: Session = Depends(get_db)):
    if not rate_limit_allow("login", _client_ip(request), _LOGIN_MAX_PER_WINDOW, _LOGIN_WINDOW_SECONDS):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Too many login attempts. Try again shortly.")

    user = db.query(User).filter(User.username == payload.username).first()
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid username or password")
    if user.is_deleted:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid username or password")
    if user.status == UserStatus.PENDING_VERIFICATION:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Please verify your Gmail address before logging in")
    if user.status == UserStatus.SUSPENDED:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Account is suspended. Contact an administrator")
    if user.status != UserStatus.ACTIVE:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Account is deactivated")
    token = create_access_token(user.user_id, user.username, user.role.value)
    return TokenResponse(access_token=token, role=user.role.value, user_id=user.user_id, username=user.username)


# ------------------------------------------------------------------------
# Normal-user self-registration + Gmail verification (registration feature,
# sections 2-4, 7). Never accepts a client-supplied role — every account
# created here is a plain USER, decided entirely server-side.
# ------------------------------------------------------------------------
@app.post("/auth/register")
def register(payload: RegisterRequest, request: Request, db: Session = Depends(get_db)):
    if not rate_limit_allow("register", _client_ip(request), _REGISTER_MAX_PER_WINDOW, _REGISTER_WINDOW_SECONDS):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Too many registration attempts. Try again later.")

    full_name = (payload.full_name or "").strip()
    if not full_name:
        raise HTTPException(400, "Full name is required")

    email = normalize_email(payload.email)
    if not is_valid_gmail_address(email):
        raise HTTPException(400, "Please enter a valid Gmail address (e.g. name@gmail.com)")

    if payload.password != payload.confirm_password:
        raise HTTPException(400, "Password and confirm password do not match")

    policy_errors = password_policy_errors(payload.password)
    if policy_errors:
        raise HTTPException(400, " ".join(policy_errors))

    if db.query(User).filter(or_(User.email == email, User.username == email)).first():
        raise HTTPException(409, "An account with this Gmail address already exists")

    user = User(
        username=email,          # Gmail address doubles as the login identifier
        full_name=full_name,
        email=email,
        password_hash=hash_password(payload.password),
        role=Role.USER,          # never trust/accept a client-supplied role
        status=UserStatus.PENDING_VERIFICATION,
        email_verified=False,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    _issue_and_send_code(db, user)

    return {
        "message": "A verification code has been sent to your Gmail address. "
                   "Enter the code to verify your account.",
        "email": email,
        "status": user.status.value,
    }


def _issue_and_send_code(db: Session, user: User) -> None:
    """Generate a fresh code, store only its hash, and email the plain
    code. Raises HTTPException(502) if sending fails — the pending user
    row still exists so a resend can be tried."""
    code = vcode.generate_code()
    user.verification_code_hash = vcode.hash_code(code)
    user.verification_code_expires_at = vcode.new_expiration()
    user.verification_attempts = 0
    user.last_verification_sent_at = now_utc()
    db.commit()

    try:
        send_verification_email(user.email, user.full_name or user.email, code, VERIFICATION_CODE_TTL_MINUTES)
    except EmailSendError:
        raise HTTPException(502, "Could not send verification email. Please try resending the code shortly.")


@app.post("/auth/verify-email")
def verify_email(payload: VerifyEmailRequest, request: Request, db: Session = Depends(get_db)):
    if not rate_limit_allow("verify", _client_ip(request), _VERIFY_MAX_PER_WINDOW, _VERIFY_WINDOW_SECONDS):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Too many attempts. Try again shortly.")

    email = normalize_email(payload.email)
    user = db.query(User).filter(User.email == email).first()
    # Same generic error whether the account doesn't exist, is already
    # verified, or the code is wrong — don't reveal which (section 4:
    # "display an appropriate error without revealing unnecessary security
    # information").
    generic_error = "Invalid or expired verification code"

    if user is None or user.is_deleted:
        raise HTTPException(400, generic_error)
    if user.status != UserStatus.PENDING_VERIFICATION:
        raise HTTPException(400, "This account is already verified" if user.email_verified else generic_error)
    if vcode.attempts_exhausted(user.verification_attempts):
        raise HTTPException(429, "Too many incorrect attempts. Please request a new code.")
    if vcode.is_expired(user.verification_code_expires_at):
        raise HTTPException(400, "This code has expired. Please request a new one.")

    if not vcode.codes_match(payload.code.strip(), user.verification_code_hash):
        user.verification_attempts += 1
        db.commit()
        raise HTTPException(400, generic_error)

    user.email_verified = True
    user.status = UserStatus.ACTIVE
    user.verification_code_hash = None
    user.verification_code_expires_at = None
    user.verification_attempts = 0
    db.commit()

    return {"message": "Gmail verified successfully. Your account is now active.", "status": user.status.value}


@app.post("/auth/resend-verification")
def resend_verification(payload: ResendVerificationRequest, request: Request, db: Session = Depends(get_db)):
    if not rate_limit_allow("resend", _client_ip(request), _RESEND_MAX_PER_WINDOW, _RESEND_WINDOW_SECONDS):
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "Too many attempts. Try again later.")

    email = normalize_email(payload.email)
    user = db.query(User).filter(User.email == email).first()

    # Always return the same generic acknowledgement whether or not the
    # account exists, so this endpoint can't be used to enumerate
    # registered Gmail addresses.
    generic_ack = {"message": "If an unverified account exists for this address, a new code has been sent."}

    if user is None or user.is_deleted or user.status != UserStatus.PENDING_VERIFICATION:
        return generic_ack

    if vcode.resend_on_cooldown(user.last_verification_sent_at):
        wait = vcode.seconds_until_resend_allowed(user.last_verification_sent_at)
        raise HTTPException(429, f"Please wait {wait} seconds before requesting another code.")

    _issue_and_send_code(db, user)
    return generic_ack


# ========================================================================
# USER — image upload + own data (sections 5, 6, 33)
# ========================================================================
_MAX_BATCH_UPLOAD = 20


def _normalize_location(location: Optional[str]) -> Optional[str]:
    """Case/whitespace-normalized location string so "viqueque ", "Viqueque"
    and "VIQUEQUE " all collapse to the same stored value, instead of only
    trimming whitespace. Mirrors the Spark-side initcap(trim(...)) location
    normalization applied to already-ingested events
    (spark/streaming_job.py) — this is the same rule applied at upload
    time, in one place both upload endpoints share."""
    if not location:
        return None
    collapsed = re.sub(r"\s+", " ", location.strip())
    return collapsed.title() if collapsed else None


def _find_duplicate_image_id(db: Session, content_hash: str, exclude_image_id: str) -> Optional[str]:
    """Returns the image_id of an earlier upload with the same content hash,
    if any (excluding the row just created for the current upload)."""
    existing = (
        db.query(Image.image_id)
        .filter(Image.content_hash == content_hash, Image.image_id != exclude_image_id)
        .order_by(Image.created_at.asc())
        .first()
    )
    return existing[0] if existing else None


@app.post("/upload-image")
async def upload_image(
    photo: UploadFile,
    location: str = Form(None),  # free-text place name chosen on the upload form; optional
    captured_at_client: str = Form(None),  # optional trusted client capture timestamp (section 7)
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    # NOTE: uploads are no longer restricted to an assigned set of fields —
    # any authenticated, active USER may upload with any location string.
    # `location` is a display/filter label, not an authorization boundary.
    # (See README section 6 for the access-control implication of removing
    # the old fields/field_users feature this replaced.)
    location = _normalize_location(location)

    if not photo.content_type or not photo.content_type.startswith("image/"):
        raise HTTPException(400, "Uploaded file must be an image")

    image_bytes = await photo.read()
    content_hash = compute_content_hash(image_bytes)

    try:
        image_id, image_path = save_image(image_bytes, photo.filename, photo.content_type)
    except InvalidImage as e:
        raise HTTPException(400, str(e))

    captured_at, captured_at_source = extract_captured_at(image_bytes)
    if captured_at is None and captured_at_client:
        try:
            captured_at = datetime.fromisoformat(captured_at_client.replace("Z", "+00:00"))
            captured_at_source = "client_trusted"
        except ValueError:
            captured_at, captured_at_source = None, None

    uploaded_at = now_utc()  # server-generated — never trust a client-supplied upload timestamp

    image = Image(
        image_id=image_id,
        location=location,
        user_id=user.user_id,
        original_filename=photo.filename,
        image_path=image_path,
        source="android_upload",
        content_hash=content_hash,
        captured_at=captured_at,
        captured_at_source=captured_at_source,
        uploaded_at=uploaded_at,
        status=ProcessingStatus.UPLOADED,
    )
    db.add(image)
    db.commit()

    duplicate_of = _find_duplicate_image_id(db, content_hash, image_id)

    # MANUAL processing mode: store the image and hold it at PENDING_REVIEW.
    # Publish nothing — an admin releases it later via
    # POST /admin/images/{id}/process (or the dashboard "Process" button),
    # which runs the exact same publish step as the AUTOMATIC branch below.
    # AUTOMATIC mode (the default) skips this block entirely and behaves
    # exactly as it did before this feature.
    if get_processing_mode(db) == ProcessingMode.MANUAL:
        image.status = ProcessingStatus.PENDING_REVIEW
        db.commit()
        return {
            "image_id": image_id,
            "location": location,
            "captured_at": captured_at.isoformat() if captured_at else None,
            "uploaded_at": uploaded_at.isoformat(),
            "status": "pending_review",
            "duplicate_of": duplicate_of,
        }

    try:
        # Shared with the manual-release admin action and the batch-upload
        # endpoint below — one place publishes to Kafka + marks INGESTED,
        # so the three callers can't drift out of sync with each other.
        submit_image_for_processing(db, image, backend=kafka_backend())
    except Exception as e:
        # Storage succeeded but ingestion failed — record this honestly
        # rather than claiming success (section 43).
        raise HTTPException(502, f"Image stored but could not be queued for processing: {e}")

    return {
        "image_id": image_id,
        "location": location,
        "captured_at": captured_at.isoformat() if captured_at else None,
        "uploaded_at": uploaded_at.isoformat(),
        "status": "accepted",
        "duplicate_of": duplicate_of,
    }


@app.post("/upload-images")
async def upload_images(
    photos: list[UploadFile],
    location: str = Form(None),
    captured_at_client: str = Form(None),
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Batch counterpart to /upload-image for the Android app's multi-select
    flow (section 11). `location`/`captured_at_client` apply to every photo
    in the batch. Each photo is processed independently — one rejected or
    failed file does not stop the rest — and the response reports a
    per-file outcome (same status values as /upload-image, plus "rejected"
    for a file that never made it past validation) so the client can show
    partial success instead of an all-or-nothing result.
    """
    if not photos:
        raise HTTPException(400, "No photos provided")
    if len(photos) > _MAX_BATCH_UPLOAD:
        raise HTTPException(400, f"Cannot upload more than {_MAX_BATCH_UPLOAD} images at once")

    location = _normalize_location(location)
    manual_mode = get_processing_mode(db) == ProcessingMode.MANUAL
    results = []

    for photo in photos:
        if not photo.content_type or not photo.content_type.startswith("image/"):
            results.append({"filename": photo.filename, "image_id": None,
                             "status": "rejected", "error": "Uploaded file must be an image"})
            continue

        image_bytes = await photo.read()
        content_hash = compute_content_hash(image_bytes)

        try:
            image_id, image_path = save_image(image_bytes, photo.filename, photo.content_type)
        except InvalidImage as e:
            results.append({"filename": photo.filename, "image_id": None,
                             "status": "rejected", "error": str(e)})
            continue

        captured_at, captured_at_source = extract_captured_at(image_bytes)
        if captured_at is None and captured_at_client:
            try:
                captured_at = datetime.fromisoformat(captured_at_client.replace("Z", "+00:00"))
                captured_at_source = "client_trusted"
            except ValueError:
                captured_at, captured_at_source = None, None

        uploaded_at = now_utc()
        image = Image(
            image_id=image_id,
            location=location,
            user_id=user.user_id,
            original_filename=photo.filename,
            image_path=image_path,
            source="android_upload",
            content_hash=content_hash,
            captured_at=captured_at,
            captured_at_source=captured_at_source,
            uploaded_at=uploaded_at,
            status=ProcessingStatus.UPLOADED,
        )
        db.add(image)
        db.commit()

        duplicate_of = _find_duplicate_image_id(db, content_hash, image_id)

        if manual_mode:
            image.status = ProcessingStatus.PENDING_REVIEW
            db.commit()
            results.append({
                "filename": photo.filename, "image_id": image_id,
                "status": "pending_review",
                "captured_at": captured_at.isoformat() if captured_at else None,
                "uploaded_at": uploaded_at.isoformat(),
                "duplicate_of": duplicate_of,
            })
            continue

        try:
            submit_image_for_processing(db, image, backend=kafka_backend())
            results.append({
                "filename": photo.filename, "image_id": image_id,
                "status": "accepted",
                "captured_at": captured_at.isoformat() if captured_at else None,
                "uploaded_at": uploaded_at.isoformat(),
                "duplicate_of": duplicate_of,
            })
        except Exception as e:
            results.append({
                "filename": photo.filename, "image_id": image_id,
                "status": "failed", "error": f"Image stored but could not be queued for processing: {e}",
            })

    return {"results": results}


@app.get("/my/images")
def my_images(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    images = db.query(Image).filter(Image.user_id == user.user_id).order_by(Image.uploaded_at.desc()).all()
    return [_image_summary(i) for i in images]


@app.get("/my/images/{image_id}")
def my_image_detail(image_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    image = db.get(Image, image_id)
    if image is None:
        raise HTTPException(404, "Image not found")
    assert_owns_image(image, user)
    prediction = db.query(Prediction).filter(Prediction.image_id == image_id).order_by(
        Prediction.predicted_at.desc()).first()
    return {**_image_summary(image), "prediction": _prediction_summary(prediction)}


@app.get("/my/predictions")
def my_predictions(db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    rows = (
        db.query(Prediction, Image)
        .join(Image, Image.image_id == Prediction.image_id)
        .filter(Image.user_id == user.user_id)
        .order_by(Prediction.predicted_at.desc())
        .all()
    )
    return [{"image": _image_summary(img), "prediction": _prediction_summary(pred)} for pred, img in rows]


_HISTORY_OUTCOMES = {"healthy", "diseased", "pending", "failed"}


def _latest_prediction_join(db: Session):
    """A LEFT JOIN target that is exactly one row per image_id: that
    image's most recent Prediction, if any. Needed because an image can
    carry more than one Prediction row (Spark's at-least-once delivery
    before the persist_result idempotency fix, or a re-run of the manual
    "Process" action) — see spark/preprocessing.persist_result docstring.
    Returns (aliased Prediction entity, the underlying subquery) so callers
    can both select the entity and reference subquery columns (e.g.
    `.image_id` to test for "no prediction yet") in the same query."""
    rn = func.row_number().over(
        partition_by=Prediction.image_id,
        order_by=(Prediction.predicted_at.desc(), Prediction.prediction_id.desc()),
    ).label("rn")
    subq = select(Prediction, rn).subquery()
    return aliased(Prediction, subq), subq


def _outcome_expr(latest_pred_image_id, latest_pred_class):
    """Classifies one image into the same four buckets a farmer actually
    cares about: "failed" (the pipeline itself broke), "pending" (still
    queued/processing/awaiting manual review — no prediction yet), or,
    once a prediction exists, "healthy" vs "diseased"."""
    return case(
        (Image.status == ProcessingStatus.FAILED, "failed"),
        (latest_pred_image_id.is_(None), "pending"),
        (func.lower(latest_pred_class) == "healthy", "healthy"),
        else_="diseased",
    )


@app.get("/my/history")
def my_history(
    limit: int = 50,
    offset: int = 0,
    outcome_filter: str = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """The caller's own upload history (sections 6/33) — every image this
    user has ever uploaded, newest first, each tagged with an "outcome"
    (healthy/diseased/pending/failed) plus a summary count across their
    WHOLE history (not just the current page), so a farmer with thousands
    of uploads can see at a glance how many came back diseased vs healthy
    without paging through everything. Scoped strictly to the caller's own
    images — the per-user counterpart to the admin dashboard, which mixes
    every user's images together.
    """
    limit = max(1, min(limit, 200))
    offset = max(offset, 0)
    if outcome_filter is not None and outcome_filter not in _HISTORY_OUTCOMES:
        raise HTTPException(400, f"outcome_filter must be one of {sorted(_HISTORY_OUTCOMES)}")

    LatestPred, subq = _latest_prediction_join(db)
    outcome_expr = _outcome_expr(subq.c.image_id, LatestPred.predicted_class)

    base_query = (
        db.query(Image, LatestPred, outcome_expr.label("outcome"))
        .outerjoin(subq, (subq.c.image_id == Image.image_id) & (subq.c.rn == 1))
        .filter(Image.user_id == user.user_id)
    )
    if outcome_filter is not None:
        base_query = base_query.filter(outcome_expr == outcome_filter)

    total = base_query.count()
    rows = base_query.order_by(Image.uploaded_at.desc()).offset(offset).limit(limit).all()
    items = [
        {**_image_summary(image), "prediction": _prediction_summary(prediction), "outcome": item_outcome}
        for image, prediction, item_outcome in rows
    ]

    summary = {o: 0 for o in _HISTORY_OUTCOMES}
    summary_rows = (
        db.query(outcome_expr.label("outcome"), func.count())
        .select_from(Image)
        .outerjoin(subq, (subq.c.image_id == Image.image_id) & (subq.c.rn == 1))
        .filter(Image.user_id == user.user_id)
        .group_by(outcome_expr)
        .all()
    )
    for o, count in summary_rows:
        summary[o] = count

    return {"total": total, "limit": limit, "offset": offset, "summary": summary, "items": items}


@app.get("/images/{image_id}")
def get_image(image_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    image = db.get(Image, image_id)
    if image is None:
        raise HTTPException(404, "Image not found")
    assert_owns_image(image, user)
    return _image_summary(image)


@app.get("/images/{image_id}/prediction")
def get_image_prediction(image_id: str, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    image = db.get(Image, image_id)
    if image is None:
        raise HTTPException(404, "Image not found")
    assert_owns_image(image, user)
    prediction = db.query(Prediction).filter(Prediction.image_id == image_id).order_by(
        Prediction.predicted_at.desc()).first()
    if prediction is None:
        return {"status": image.status.value, "prediction": None,
                "error_message": image.error_message}
    return {"status": image.status.value, "prediction": _prediction_summary(prediction)}


_MAX_THUMBNAIL_SIZE = 1024


@app.get("/images/{image_id}/file")
def get_image_file(
    image_id: str,
    max_size: int = None,
    db: Session = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Serves the actual uploaded photo (owner or admin only) — what the
    Android app's history/recent-diagnoses screens show as a thumbnail next
    to each past upload, since until now the client had no way to see the
    image it sent, only the prediction data about it.

    `max_size` (optional, capped at 1024px) resizes server-side with PIL
    before sending, so a history list of many rows doesn't pull down full
    original photos (up to MAX_IMAGE_SIZE each) just to show a thumbnail.
    Omit it to get the original file untouched.
    """
    image = db.get(Image, image_id)
    if image is None:
        raise HTTPException(404, "Image not found")
    assert_owns_image(image, user)

    if is_drive_path(image.image_path):
        # A Google Drive upload has no file under IMAGE_STORAGE_PATH (this
        # used to answer 404 for every one of them): serve Spark's kept local
        # copy, else the photo from Drive.
        path = downloaded_photo_path(image.user_id, image.image_id)
        if path is None:
            try:
                original = load_image_bytes(image.image_path)
            except FileNotFoundError:
                raise HTTPException(404, "Image file not found in Google Drive")
            except Exception as e:  # noqa: BLE001 - Drive not configured / unreachable
                raise HTTPException(502, f"Could not fetch the image from Google Drive: {e}")
    else:
        try:
            path = resolve_image_path(image.image_path)
        except FileNotFoundError:
            raise HTTPException(404, "Image file not found on disk")

    if not max_size:
        if path is None:
            media = "image/png" if original.startswith(b"\x89PNG") else "image/jpeg"
            return Response(content=original, media_type=media)
        return FileResponse(str(path))

    max_size = max(16, min(max_size, _MAX_THUMBNAIL_SIZE))
    from io import BytesIO
    from PIL import Image as PILImage

    with PILImage.open(path if path is not None else BytesIO(original)) as im:
        im.thumbnail((max_size, max_size))
        buf = BytesIO()
        im.convert("RGB").save(buf, format="JPEG", quality=80)
    return Response(content=buf.getvalue(), media_type="image/jpeg")


# ========================================================================
# ADMIN — user management, system-wide monitoring (sections 27, 34)
# ========================================================================
_VALID_STATUSES = {s.value for s in UserStatus}


@app.get("/admin/users")
def admin_list_users(
    search: Optional[str] = None,
    role: Optional[str] = None,
    status_filter: Optional[str] = None,
    include_deleted: bool = False,
    page: int = 1,
    page_size: int = 25,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    """Search / filter / pagination for the admin User Management table
    (section 9). `status_filter` maps to the query param `status_filter`
    rather than `status` to avoid clashing with FastAPI's reserved
    handling of the `status` module import used elsewhere in this file."""
    page = max(1, page)
    page_size = max(1, min(page_size, 200))

    query = db.query(User)
    if not include_deleted:
        query = query.filter(User.is_deleted.is_(False))
    if search:
        like = f"%{search.strip()}%"
        query = query.filter(or_(User.full_name.ilike(like), User.email.ilike(like),
                                  User.username.ilike(like)))
    if role:
        if role not in (Role.USER.value, Role.ADMIN.value):
            raise HTTPException(400, "role must be USER or ADMIN")
        query = query.filter(User.role == Role(role))
    if status_filter:
        if status_filter not in _VALID_STATUSES:
            raise HTTPException(400, f"status must be one of {sorted(_VALID_STATUSES)}")
        query = query.filter(User.status == UserStatus(status_filter))

    total = query.count()
    users = (
        query.order_by(User.created_at.desc())
        .offset((page - 1) * page_size).limit(page_size).all()
    )
    return {
        "items": [_user_summary(u) for u in users],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
    }


@app.post("/admin/users")
def admin_create_user(payload: CreateUserRequest, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    if payload.role not in (Role.USER.value, Role.ADMIN.value):
        raise HTTPException(400, "role must be USER or ADMIN")

    full_name = (payload.full_name or "").strip()
    if not full_name:
        raise HTTPException(400, "Full name is required")

    email = normalize_email(payload.email)
    if not email:
        raise HTTPException(400, "Email is required")

    policy_errors = password_policy_errors(payload.password)
    if policy_errors:
        raise HTTPException(400, " ".join(policy_errors))

    username = (payload.username or email).strip()
    if db.query(User).filter(or_(User.username == username, User.email == email)).first():
        raise HTTPException(409, "A user with this username or email already exists")

    # Default: go through the real verification-code flow, same as
    # self-registration — an admin creating a user is not, by itself, a
    # "legitimate verification mechanism" for that Gmail address (section
    # 9). Explicitly requesting status=ACTIVE is a deliberate admin
    # override instead of an automatic one.
    force_active = payload.status == UserStatus.ACTIVE.value
    if payload.status and payload.status not in _VALID_STATUSES:
        raise HTTPException(400, f"status must be one of {sorted(_VALID_STATUSES)}")

    user = User(
        username=username, full_name=full_name, email=email,
        password_hash=hash_password(payload.password), role=Role(payload.role),
        status=UserStatus.ACTIVE if force_active else UserStatus.PENDING_VERIFICATION,
        email_verified=force_active,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    if not force_active:
        _issue_and_send_code(db, user)

    return _user_summary(user)


@app.put("/admin/users/{user_id}")
def admin_update_user(user_id: str, payload: UpdateUserRequest, db: Session = Depends(get_db),
                       admin: User = Depends(require_admin)):
    user = db.get(User, user_id)
    if user is None or user.is_deleted:
        raise HTTPException(404, "User not found")

    email_changed = False

    if payload.full_name is not None:
        full_name = payload.full_name.strip()
        if not full_name:
            raise HTTPException(400, "Full name cannot be empty")
        user.full_name = full_name

    if payload.email is not None:
        new_email = normalize_email(payload.email)
        if not new_email:
            raise HTTPException(400, "Email cannot be empty")
        if new_email != user.email:
            if db.query(User).filter(User.email == new_email, User.user_id != user.user_id).first():
                raise HTTPException(409, "Another user already has this email")
            # Self-registered (and admin-created, by default) accounts log
            # in with their Gmail address as the username (see register()
            # / admin_create_user()) — keep username in sync so changing
            # the email doesn't strand the person needing to log in with
            # an address that no longer appears anywhere on their profile.
            username_mirrors_email = (user.username == user.email)
            if username_mirrors_email and db.query(User).filter(
                    User.username == new_email, User.user_id != user.user_id).first():
                raise HTTPException(409, "Another user already has this username")
            user.email = new_email
            if username_mirrors_email:
                user.username = new_email
            # Changing the Gmail address invalidates any prior proof of
            # ownership — require re-verification of the new address
            # (section 9: "If the Gmail address is changed, reset the
            # email verification state and require verification of the
            # new address").
            user.email_verified = False
            user.status = UserStatus.PENDING_VERIFICATION
            user.verification_code_hash = None
            user.verification_code_expires_at = None
            user.verification_attempts = 0
            email_changed = True

    if payload.role:
        if payload.role not in (Role.USER.value, Role.ADMIN.value):
            raise HTTPException(400, "role must be USER or ADMIN")
        if user.user_id == admin.user_id and payload.role != Role.ADMIN.value:
            raise HTTPException(400, "You cannot remove your own admin role")
        user.role = Role(payload.role)

    if payload.status:
        if payload.status not in _VALID_STATUSES:
            raise HTTPException(400, f"status must be one of {sorted(_VALID_STATUSES)}")
        if user.user_id == admin.user_id and payload.status != UserStatus.ACTIVE.value:
            raise HTTPException(400, "You cannot change your own account status")
        user.status = UserStatus(payload.status)
        if user.status == UserStatus.ACTIVE:
            user.email_verified = True  # admin override — explicit action, see admin_create_user

    # Only actually send a code if the request didn't ALSO override the
    # status back to something else in the same call (e.g. an admin
    # setting email + status=ACTIVE together as a deliberate override) —
    # otherwise this would fire off a pointless verification email for an
    # account that's ending up ACTIVE anyway.
    if email_changed and user.status == UserStatus.PENDING_VERIFICATION:
        _issue_and_send_code(db, user)
    else:
        db.commit()
    return _user_summary(user)


@app.delete("/admin/users/{user_id}")
def admin_delete_user(user_id: str, confirm: bool = False, db: Session = Depends(get_db),
                       admin: User = Depends(require_admin)):
    """Soft-deletes a user (section 9: 'Prefer soft deletion', 'Require
    confirmation', 'Prevent accidental deletion'). Requires `?confirm=true`
    so a bare DELETE can't silently remove an account.

    Known limitation: `email` and `username` are UNIQUE at the database
    level, and this endpoint does not rename them off the deleted row — so
    a soft-deleted account's Gmail address stays permanently reserved and
    cannot be reused by a fresh registration or a new admin-created user
    afterward (both would get 409 'already exists'). This is a deliberate
    trade-off (identifiers are never silently recycled, avoiding any
    ambiguity about which historical images/predictions belong to whom)
    rather than an oversight — but it does mean "delete" here is closer to
    "deactivate and hide" than "free up this Gmail address for someone
    else."""
    if not confirm:
        raise HTTPException(400, "Pass ?confirm=true to permanently deactivate and remove this user")

    user = db.get(User, user_id)
    if user is None or user.is_deleted:
        raise HTTPException(404, "User not found")

    if user.user_id == admin.user_id:
        raise HTTPException(400, "You cannot delete your own admin account")

    user.is_deleted = True
    user.deleted_at = now_utc()
    user.status = UserStatus.INACTIVE
    db.commit()
    return {"user_id": user.user_id, "deleted": True}


# ------------------------------------------------------------------------
# Manual / automatic image-processing mode (the admin "switch button").
#
# AUTOMATIC (default): /upload-image publishes to Kafka immediately — the
#   original pipeline, unchanged.
# MANUAL: /upload-image stores the image at PENDING_REVIEW and publishes
#   nothing until an admin calls POST /admin/images/{id}/process.
# ------------------------------------------------------------------------
@app.get("/admin/processing-mode", response_model=ProcessingModeResponse)
def admin_get_processing_mode(db: Session = Depends(get_db), _: User = Depends(require_admin)):
    return ProcessingModeResponse(mode=get_processing_mode(db).value)


def _drain_pending_review(db: Session) -> tuple[int, int]:
    """Push every image currently held at PENDING_REVIEW into the pipeline,
    exactly as the per-image 'Process' action does. One image failing to
    publish (it's marked FAILED by submit_image_for_processing) doesn't
    abort the rest. Returns (released, failed)."""
    held = db.query(Image).filter(Image.status == ProcessingStatus.PENDING_REVIEW).all()
    released = failed = 0
    for img in held:
        try:
            submit_image_for_processing(db, img, backend=kafka_backend())
            released += 1
        except Exception:
            failed += 1
    return released, failed


@app.put("/admin/processing-mode", response_model=ProcessingModeResponse)
def admin_set_processing_mode(
    payload: SetProcessingModeRequest,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    raw = (payload.mode or "").strip().upper()
    try:
        mode = ProcessingMode(raw)
    except ValueError:
        raise HTTPException(400, "mode must be 'AUTOMATIC' or 'MANUAL'")
    set_processing_mode(db, mode)

    # Switching to AUTOMATIC drains whatever piled up while in MANUAL, so
    # the admin doesn't have to also click "Process all". (No-op when there
    # is nothing held, which is always the case switching the other way.)
    released = released_failed = 0
    if mode == ProcessingMode.AUTOMATIC:
        released, released_failed = _drain_pending_review(db)

    return ProcessingModeResponse(
        mode=mode.value, released=released, released_failed=released_failed
    )


@app.get("/admin/pending-images")
def admin_pending_images(limit: int = 200, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    """Images uploaded while in MANUAL mode and still awaiting the admin's
    'Process' click (status PENDING_REVIEW), newest first."""
    images = (
        db.query(Image)
        .filter(Image.status == ProcessingStatus.PENDING_REVIEW)
        .order_by(Image.uploaded_at.desc())
        .limit(limit)
        .all()
    )
    return [_image_summary(i) for i in images]


@app.post("/admin/images/{image_id}/process")
def admin_process_image(image_id: str, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    """The 'Process' button: release a held image into the same Kafka ->
    Spark pipeline an AUTOMATIC-mode upload uses. Accepts an image at
    PENDING_REVIEW or UPLOADED, or one that FAILED specifically at the
    Kafka-publish step; anything already INGESTED/PROCESSING/COMPLETED is
    rejected so this can't create a duplicate prediction."""
    image = db.get(Image, image_id)
    if image is None:
        raise HTTPException(404, "Image not found")

    releasable = {ProcessingStatus.PENDING_REVIEW, ProcessingStatus.UPLOADED}
    retriable_publish_failure = (
        image.status == ProcessingStatus.FAILED
        and (image.error_message or "").startswith("Kafka publish failed")
    )
    if image.status not in releasable and not retriable_publish_failure:
        raise HTTPException(409, f"Image is not awaiting processing (status {image.status.value})")

    try:
        submit_image_for_processing(db, image, backend=kafka_backend())
    except Exception as e:
        raise HTTPException(502, f"Image could not be queued for processing: {e}")

    return {"image_id": image_id, "status": image.status.value}


@app.get("/admin/images")
def admin_list_images(
    limit: int = 200,
    status_filter: Optional[str] = None,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    query = db.query(Image)
    if status_filter:
        valid = {s.value for s in ProcessingStatus}
        if status_filter not in valid:
            raise HTTPException(400, f"status_filter must be one of {sorted(valid)}")
        query = query.filter(Image.status == ProcessingStatus(status_filter))
    images = query.order_by(Image.uploaded_at.desc()).limit(limit).all()
    return [_image_summary(i) for i in images]


@app.get("/admin/predictions")
def admin_list_predictions(limit: int = 200, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    preds = db.query(Prediction).order_by(Prediction.predicted_at.desc()).limit(limit).all()
    return [_prediction_summary(p) for p in preds]


@app.get("/admin/alerts")
def admin_alerts(limit: int = 100, db: Session = Depends(get_db), _: User = Depends(require_admin)):
    rows = (
        db.query(Prediction, Image)
        .join(Image, Image.image_id == Prediction.image_id)
        .filter(Prediction.risk_level == "HIGH")
        .order_by(Prediction.predicted_at.desc())
        .limit(limit)
        .all()
    )
    return [{"image": _image_summary(img), "prediction": _prediction_summary(pred)} for pred, img in rows]


@app.get("/admin/analytics")
def admin_analytics(_: User = Depends(require_admin)):
    from spark import analytics
    return {
        "kpi": analytics.kpi_summary(),
        "disease_distribution": analytics.to_json_safe_records(analytics.disease_distribution()),
        "risk_distribution": analytics.to_json_safe_records(analytics.risk_distribution()),
        "location_summary": analytics.to_json_safe_records(analytics.location_summary()),
    }


@app.get("/admin/system-status")
def admin_system_status(db: Session = Depends(get_db), _: User = Depends(require_admin)):
    from spark import analytics
    backend = kafka_backend()
    backend_name = getattr(backend, "active_name", type(backend).__name__)
    return {
        "kafka_backend": backend_name,  # "KafkaBackend" (real broker) or "LocalFileBackend" (fallback)
        "database_ok": True,
        "performance": analytics.performance_metrics(),
    }


# ========================================================================
# Serialization helpers
# ========================================================================
def _user_summary(u: User) -> dict:
    # Never includes password_hash, verification_code_hash, or the plain
    # verification code (section 11).
    return {
        "user_id": u.user_id, "username": u.username, "full_name": u.full_name, "email": u.email,
        "role": u.role.value, "status": u.status.value, "email_verified": u.email_verified,
        "created_at": u.created_at.isoformat(),
        "updated_at": u.updated_at.isoformat() if u.updated_at else None,
    }


def _image_summary(i: Image) -> dict:
    return {
        "image_id": i.image_id, "location": i.location, "user_id": i.user_id,
        "original_filename": i.original_filename, "source": i.source, "status": i.status.value,
        "captured_at": i.captured_at.isoformat() if i.captured_at else None,
        "captured_at_source": i.captured_at_source,
        "uploaded_at": i.uploaded_at.isoformat() if i.uploaded_at else None,
        "ingested_at": i.ingested_at.isoformat() if i.ingested_at else None,
        "processed_at": i.processed_at.isoformat() if i.processed_at else None,
        "error_message": i.error_message,
    }


def _prediction_summary(p: Prediction) -> dict:
    if p is None:
        return None
    return {
        "prediction_id": p.prediction_id, "image_id": p.image_id,
        "predicted_class": p.predicted_class, "prediction_confidence": p.prediction_confidence,
        "lesion_score": p.lesion_score, "discoloration_pct": p.discoloration_pct,
        "texture_irregularity": p.texture_irregularity, "leaf_area_affected_pct": p.leaf_area_affected_pct,
        "risk_score": p.risk_score, "risk_level": p.risk_level.value if p.risk_level else None,
        "is_uncertain": p.is_uncertain,
        "model_version": p.model_version,
        "predicted_at": p.predicted_at.isoformat() if p.predicted_at else None,
        "processing_latency_ms": p.processing_latency_ms,
    }
