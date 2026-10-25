"""
Farmer accounts made from Google Drive identities. A farmer never registers
with a password: the Google account that owns an upload (Drive's `owners`
field) is the identity. Used by Spark's ingestion step
(storage/uploads.register_upload) and the dashboard's "Register farmer"
button, so both create the account the same way.
"""

import secrets

from sqlalchemy.exc import IntegrityError

from storage.auth import hash_password, normalize_email
from storage.database import User, Role, UserStatus


def get_or_create_farmer(session, email: str, display_name: str):
    email = normalize_email(email)
    user = session.query(User).filter(User.email == email).first()
    if user is not None:
        return user
    user = User(
        username=email,
        full_name=display_name or email,
        email=email,
        # Drive users sign in with Google on the phone; this random password
        # is never shown to anyone, so the account can't log in anywhere else.
        password_hash=hash_password(secrets.token_urlsafe(32)),
        role=Role.USER,
        status=UserStatus.ACTIVE,
        email_verified=True,  # verified by Google sign-in
    )
    session.add(user)
    try:
        session.commit()
    except IntegrityError:
        # A new farmer's first photos arrive together and Spark registers them
        # in parallel: another photo created this account a moment ago.
        session.rollback()
        return session.query(User).filter(User.email == email).one()
    print(f"[farmers] New user from Google Drive: {email}")
    return user
