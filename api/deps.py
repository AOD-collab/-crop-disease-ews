"""
FastAPI dependencies for authentication, role-based authorization, and USER
data isolation (DIT manual section 32: the backend — not just the UI — must
enforce that a USER can only reach their own data).
"""

from typing import Iterator

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from storage.database import get_session, User, Role, UserStatus
from api.auth import decode_access_token, InvalidToken

_bearer = HTTPBearer(auto_error=False)


def get_db() -> Iterator[Session]:
    yield from get_session()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    db: Session = Depends(get_db),
) -> User:
    if credentials is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing bearer token")
    try:
        payload = decode_access_token(credentials.credentials)
    except InvalidToken:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or expired token")

    user = db.get(User, payload.get("sub"))
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User no longer exists")
    # Defense in depth: admin_delete_user always sets status=INACTIVE
    # alongside is_deleted=True, so the status check below already covers
    # this — but checking is_deleted explicitly too means a soft-deleted
    # account is refused here even if some future code path ever sets
    # is_deleted without also changing status.
    if user.is_deleted:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User no longer exists")
    if user.status != UserStatus.ACTIVE:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Account is deactivated")
    # Role is re-read from the DB row every request — never trusted from the
    # token payload alone being stale, and NEVER from client input.
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != Role.ADMIN:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Admin access required")
    return user


def require_user_or_admin(user: User = Depends(get_current_user)) -> User:
    # Any authenticated, active account (USER or ADMIN) — just here for
    # readability at call sites.
    return user


def assert_owns_image(image, user: User):
    """Raise 403 if `user` (a USER) does not own this image row. ADMIN
    bypasses (system-wide access)."""
    if user.role == Role.ADMIN:
        return
    if image.user_id != user.user_id:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "You do not have access to this image")
