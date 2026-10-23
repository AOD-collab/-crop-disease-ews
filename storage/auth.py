"""
Account helpers shared by the dashboard (admin login), the Drive watcher
(auto-created farmer accounts), the scalability test and storage/init_db.py
(bootstrap admin): e-mail normalisation and bcrypt password hashing.

Uses the `bcrypt` library directly rather than passlib's CryptContext:
passlib's bcrypt backend auto-detection (a version-compatibility probe it
runs on import) throws on recent bcrypt releases that dropped the
`__about__` attribute it looks for, making CryptContext unusable out of the
box with current bcrypt wheels.
"""

import bcrypt


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()

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
