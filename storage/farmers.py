"""
Farmer accounts come from the Google account that owns the upload in Drive;
farmers never have a password of their own.
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
        # random password nobody knows: farmers sign in with Google on the phone
        password_hash=hash_password(secrets.token_urlsafe(32)),
        role=Role.USER,
        status=UserStatus.ACTIVE,
        email_verified=True,  # verified by Google sign-in
    )
    session.add(user)
    try:
        session.commit()
    except IntegrityError:
        # another photo of the same new farmer created it a moment ago
        session.rollback()
        return session.query(User).filter(User.email == email).one()
    print(f"[farmers] New user from Google Drive: {email}")
    return user
