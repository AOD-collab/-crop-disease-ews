from api.image_storage import save_image, InvalidImage


def test_rejects_empty_file():
    try:
        save_image(b"", "leaf.jpg", "image/jpeg")
        assert False, "should have raised InvalidImage"
    except InvalidImage:
        pass


def test_rejects_corrupted_image_bytes():
    try:
        save_image(b"this is not a real image file" * 10, "leaf.jpg", "image/jpeg")
        assert False, "should have raised InvalidImage"
    except InvalidImage:
        pass


def test_rejects_oversized_file(monkeypatch):
    import api.image_storage as mod
    monkeypatch.setattr(mod, "MAX_IMAGE_SIZE", 10)
    try:
        save_image(b"x" * 1000, "leaf.jpg", "image/jpeg")
        assert False, "should have raised InvalidImage"
    except InvalidImage:
        pass


def test_saves_valid_image_and_prevents_path_traversal(sample_leaf_image_bytes):
    image_id, path = save_image(sample_leaf_image_bytes, "../../etc/passwd.jpg", "image/jpeg")
    from pathlib import Path
    from config.settings import IMAGE_STORAGE_PATH
    saved_path = Path(path)
    assert saved_path.exists()
    # Must be written under the configured storage root, never above it,
    # regardless of what filename the client supplied.
    assert str(saved_path.resolve()).startswith(str(IMAGE_STORAGE_PATH.resolve()))
    assert saved_path.name == f"{image_id}.jpg"


def test_two_uploads_never_collide(sample_leaf_image_bytes):
    _, path1 = save_image(sample_leaf_image_bytes, "leaf.jpg", "image/jpeg")
    _, path2 = save_image(sample_leaf_image_bytes, "leaf.jpg", "image/jpeg")
    assert path1 != path2
