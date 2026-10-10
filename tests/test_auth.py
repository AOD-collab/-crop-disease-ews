import pytest

from api.auth import hash_password, verify_password, create_access_token, decode_access_token, InvalidToken


def test_password_hash_is_not_plaintext():
    h = hash_password("correct horse battery staple")
    assert h != "correct horse battery staple"
    assert verify_password("correct horse battery staple", h)


def test_wrong_password_fails_verification():
    h = hash_password("correct horse battery staple")
    assert not verify_password("wrong password", h)


def test_token_roundtrip():
    token = create_access_token("user-123", "farmer1", "USER")
    payload = decode_access_token(token)
    assert payload["sub"] == "user-123"
    assert payload["username"] == "farmer1"
    assert payload["role"] == "USER"


def test_tampered_token_is_rejected():
    token = create_access_token("user-123", "farmer1", "USER")
    tampered = token[:-3] + ("abc" if token[-3:] != "abc" else "xyz")
    with pytest.raises(InvalidToken):
        decode_access_token(tampered)
