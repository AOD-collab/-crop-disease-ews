"""
E-mail normalisation and bcrypt password hashing (bcrypt directly: passlib
breaks on current bcrypt releases).
"""

import bcrypt


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()

# bcrypt only uses the first 72 bytes of a password
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
