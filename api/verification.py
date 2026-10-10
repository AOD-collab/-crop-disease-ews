"""
Gmail verification-code generation, storage-hashing, and validation
(registration feature, sections 3/6/11).

The plain code is only ever held in memory long enough to email it — the
database stores a keyed hash (HMAC-SHA256, not a bare `sha256(code)`, so a
leaked database dump alone doesn't let someone brute-force 6-digit codes
offline without also having JWT_SECRET). The code is never returned from
any API response.
"""

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone

from config.settings import (
    JWT_SECRET, VERIFICATION_CODE_LENGTH, VERIFICATION_CODE_TTL_MINUTES,
    VERIFICATION_CODE_MAX_ATTEMPTS, VERIFICATION_RESEND_COOLDOWN_SECONDS,
)


def generate_code() -> str:
    """Cryptographically secure random numeric code, e.g. '048213'."""
    lower = 10 ** (VERIFICATION_CODE_LENGTH - 1)
    upper = (10 ** VERIFICATION_CODE_LENGTH) - 1
    return str(secrets.randbelow(upper - lower + 1) + lower)


def hash_code(code: str) -> str:
    key = (JWT_SECRET or "insecure-dev-only-key").encode("utf-8")
    return hmac.new(key, code.encode("utf-8"), hashlib.sha256).hexdigest()


def codes_match(code: str, code_hash: str) -> bool:
    if not code or not code_hash:
        return False
    return hmac.compare_digest(hash_code(code), code_hash)


def new_expiration(now: datetime = None) -> datetime:
    now = now or datetime.now(timezone.utc)
    return now + timedelta(minutes=VERIFICATION_CODE_TTL_MINUTES)


def is_expired(expires_at: datetime, now: datetime = None) -> bool:
    if expires_at is None:
        return True
    now = now or datetime.now(timezone.utc)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return now >= expires_at


def attempts_exhausted(attempts: int) -> bool:
    return attempts >= VERIFICATION_CODE_MAX_ATTEMPTS


def resend_on_cooldown(last_sent_at: datetime, now: datetime = None) -> bool:
    """True if a resend request must be rejected (section 3/11: 'Limit how
    frequently a user can request a new code')."""
    if last_sent_at is None:
        return False
    now = now or datetime.now(timezone.utc)
    if last_sent_at.tzinfo is None:
        last_sent_at = last_sent_at.replace(tzinfo=timezone.utc)
    return (now - last_sent_at).total_seconds() < VERIFICATION_RESEND_COOLDOWN_SECONDS


def seconds_until_resend_allowed(last_sent_at: datetime, now: datetime = None) -> int:
    if last_sent_at is None:
        return 0
    now = now or datetime.now(timezone.utc)
    if last_sent_at.tzinfo is None:
        last_sent_at = last_sent_at.replace(tzinfo=timezone.utc)
    remaining = VERIFICATION_RESEND_COOLDOWN_SECONDS - (now - last_sent_at).total_seconds()
    return max(0, int(remaining))
