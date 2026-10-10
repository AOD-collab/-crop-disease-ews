"""kafka.drive_watcher: one Drive file that can't be registered must not stop
the change feed for every other farmer (its bookmark must still advance)."""

import uuid

from kafka import drive_watcher as w
from storage import drive_client
from storage.database import SessionLocal, init_db, Image, User, Role, UserStatus, RejectedEvent
from api.auth import hash_password


class FakeBackend:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(message)


def _photo(owner: str, folder: str) -> dict:
    image_id = str(uuid.uuid4())
    return {"id": f"FILE{uuid.uuid4().hex[:16]}", "name": f"{image_id}.jpg", "mimeType": "image/jpeg",
            "size": "1234", "parents": [folder], "owners": [{"emailAddress": owner}],
            "createdTime": "2026-10-10T05:00:00.000Z", "appProperties": {"image_id": image_id}}


def test_unregistrable_file_is_skipped_and_feed_moves_on(monkeypatch, tmp_path):
    init_db()
    monkeypatch.setattr(w, "STATE_FILE", tmp_path / "state.json")
    monkeypatch.setattr(drive_client, "images_folder_id", lambda: "ROOT")
    # An account whose USERNAME is maria's Gmail but whose e-mail is another
    # address: creating maria's account on her first upload hits the UNIQUE
    # username -> IntegrityError, which used to abort every scan.
    clash = f"maria-{uuid.uuid4().hex[:6]}@gmail.com"
    with SessionLocal() as s:
        s.add(User(username=clash, email=f"work-{clash}", password_hash=hash_password("x1234567"),
                   role=Role.USER, status=UserStatus.ACTIVE, email_verified=True))
        s.commit()
    good_owner = f"ana-{uuid.uuid4().hex[:6]}@gmail.com"
    bad, good = _photo(clash, "F_MARIA"), _photo(good_owner, "F_ANA")
    monkeypatch.setattr(drive_client, "list_changes", lambda token: ([{"file": bad}, {"file": good}], "TOKEN-2"))
    w._save_state({"changes_page_token": "TOKEN-1"})

    backend = FakeBackend()
    outcomes = w.scan_changes({"F_MARIA": clash, "F_ANA": good_owner}, backend=backend)

    assert [o["status"] for o in outcomes] == ["error", "registered"]
    assert w._load_state()["changes_page_token"] == "TOKEN-2"
    with SessionLocal() as s:
        assert s.get(Image, good["appProperties"]["image_id"]) is not None
        assert s.query(RejectedEvent).filter(RejectedEvent.raw_event.contains(bad["id"])).count() == 1
    assert [m["image_id"] for m in backend.sent] == [good["appProperties"]["image_id"]]

    # the next poll (or the 10-minute full scan) meets the same file again: logged only once
    w.scan_changes({"F_MARIA": clash, "F_ANA": good_owner}, backend=backend)
    with SessionLocal() as s:
        assert s.query(RejectedEvent).filter(RejectedEvent.raw_event.contains(bad["id"])).count() == 1
