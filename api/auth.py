"""
Password hashing (bcrypt) + JWT issue/verify.

Roles come ONLY from the database, never from anything the client sends
(DIT manual section 14: "Never trust a USER-supplied role").

Uses the `bcrypt` library directly rather than passlib's CryptContext:
passlib's bcrypt backend auto-detection (a version-compatibility probe it
runs on import) throws on recent bcrypt releases that dropped the
`__about__` attribute it looks for, making CryptContext unusable out of the
box with current bcrypt wheels. Calling bcrypt directly avoids that broken
probe entirely and is a well-supported, simpler API for this project's
single hashing scheme.
"""

import re
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

from config.settings import JWT_SECRET, JWT_ALGORITHM, JWT_EXPIRE_MINUTES, PASSWORD_MIN_LENGTH

# Deliberately NOT "endswith @gmail.com" only (registration feature section
# 2: "Do not rely only on checking whether the address ends with
# @gmail.com") — this also rejects structurally invalid local parts.
# Actual control of the mailbox is proven separately, by the confirmation
# code flow (api/verification.py), not by this format check.
_GMAIL_RE = re.compile(r"^[a-zA-Z0-9](?:[a-zA-Z0-9._%+-]{0,63})@gmail\.com$")


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def is_valid_gmail_address(email: str) -> bool:
    email = normalize_email(email)
    if not email or len(email) > 254:
        return False
    local_part = email.split("@")[0] if "@" in email else ""
    if local_part.startswith(".") or local_part.endswith(".") or ".." in local_part:
        return False
    return bool(_GMAIL_RE.match(email))


def password_policy_errors(password: str) -> list[str]:
    """Minimum security requirements (section 2/11). Returns a list of
    human-readable problems; empty list means the password is acceptable."""
    errors = []
    if not password or len(password) < PASSWORD_MIN_LENGTH:
        errors.append(f"Password must be at least {PASSWORD_MIN_LENGTH} characters long.")
    if not re.search(r"[A-Za-z]", password or ""):
        errors.append("Password must contain at least one letter.")
    if not re.search(r"[0-9]", password or ""):
        errors.append("Password must contain at least one number.")
    return errors

# bcrypt only hashes the first 72 bytes of a secret — this is a hard limit
# of the algorithm itself, not a bug, so a very long password is truncated
# deterministically rather than silently accepted-but-ignored past 72 bytes.
_MAX_PASSWORD_BYTES = 72


def hash_password(plain_password: str) -> str:
    password_bytes = plain_password.encode("utf-8")[:_MAX_PASSWORD_BYTES]
    return bcrypt.hashpw(password_bytes, bcrypt.gensalt()).decode("utf-8")


def verify_password(plain_password: str, password_hash: str) -> bool:
    password_bytes = plain_password.encode("utf-8")[:_MAX_PASSWORD_BYTES]
    try:
        return bcrypt.checkpw(password_bytes, password_hash.encode("utf-8"))
    except ValueError:
        return False


def create_access_token(user_id: str, username: str, role: str) -> str:
    if not JWT_SECRET:
        raise RuntimeError(
            "JWT_SECRET is not set. Set it via the environment before issuing "
            "tokens — do not run authentication in production with an empty secret."
        )
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "username": username,
        "role": role,
        "iat": now,
        "exp": now + timedelta(minutes=JWT_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


class InvalidToken(Exception):
    pass


def decode_access_token(token: str) -> dict:
    if not JWT_SECRET:
        raise RuntimeError("JWT_SECRET is not set.")
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError as e:
        raise InvalidToken(str(e)) from e
