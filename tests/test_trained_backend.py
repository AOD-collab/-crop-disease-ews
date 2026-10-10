"""
Coverage for the trained CNN (ml.inference.predict / disease_heatmap /
model_version), run against the checkpoint shipped at
ml/trained_model/saved_model_cnn.pt.
"""

from pathlib import Path

import pytest

from config.settings import IMAGE_MODEL
from ml import inference
from tests.conftest import make_leaf_jpeg_bytes

WEIGHTS_PATH = Path(__file__).resolve().parent.parent / "ml" / "trained_model" / "saved_model_cnn.pt"


@pytest.fixture()
def trained_backend(monkeypatch):
    if not WEIGHTS_PATH.exists():
        pytest.skip(f"No trained checkpoint at {WEIGHTS_PATH}")
    monkeypatch.setitem(IMAGE_MODEL, "weights_path", str(WEIGHTS_PATH))
    inference._load_trained_model.cache_clear()
    yield
    inference._load_trained_model.cache_clear()


def test_trained_backend_produces_a_real_classification(trained_backend):
    result = inference.predict(make_leaf_jpeg_bytes(128))

    assert result["predicted_class"] in (
        "healthy", "common_rust", "gray_leaf_spot", "northern_leaf_blight",
        inference.UNCERTAIN_CLASS,
    )
    assert result["predicted_class_confidence"] is not None
    assert 0.0 <= result["predicted_class_confidence"] <= 1.0
    assert result["model_confidence"] == result["predicted_class_confidence"]
    assert 0.0 <= result["lesion_score"] <= 1.0
    assert "is_uncertain" in result


def test_prediction_has_no_pixel_math_features(trained_backend):
    """Everything comes from the model: the old colour-rule features are
    gone (None), not computed on the side."""
    result = inference.predict(make_leaf_jpeg_bytes(128))
    assert result["discoloration_pct"] is None
    assert result["texture_irregularity"] is None
    assert result["leaf_area_affected_pct"] is None


def test_trained_backend_flags_low_confidence_as_uncertain(trained_backend, monkeypatch):
    """A confidence_threshold set above 1.0 makes every prediction
    "uncertain" by construction — proves the threshold is actually wired
    in, not just defined and ignored (the original bug)."""
    monkeypatch.setitem(IMAGE_MODEL, "confidence_threshold", 1.1)
    result = inference.predict(make_leaf_jpeg_bytes(128))
    assert result["is_uncertain"] is True
    assert result["predicted_class"] == inference.UNCERTAIN_CLASS


def test_disease_heatmap_matches_photo_size_or_is_none_for_healthy(trained_backend):
    from io import BytesIO
    from PIL import Image
    image_bytes = make_leaf_jpeg_bytes(128)
    heat = inference.disease_heatmap(image_bytes)
    if inference.predict(image_bytes)["predicted_class"] == "healthy":
        assert heat is None
    else:
        assert heat.shape == Image.open(BytesIO(image_bytes)).size[::-1]
        assert 0.0 <= heat.min() and heat.max() <= 1.0 + 1e-6


def test_missing_checkpoint_fails_loudly(monkeypatch, tmp_path):
    """No silent fallback to anything that isn't the model."""
    monkeypatch.setitem(IMAGE_MODEL, "weights_path", str(tmp_path / "missing.pt"))
    inference._load_trained_model.cache_clear()
    try:
        with pytest.raises(inference.ModelNotAvailable):
            inference.predict(make_leaf_jpeg_bytes(64))
    finally:
        inference._load_trained_model.cache_clear()


def test_trained_backend_model_version_is_just_the_filename(trained_backend):
    """Bug fix: model_version() used to store the FULL weights path (e.g.
    'cnn-resnet18:A:\\final\\...\\saved_model_cnn.pt' on Windows) instead of
    just the filename. It also used to hard-code "resnet18" (the deployed
    model is an SE-ResNet-18) and was identical for every deploy; now the
    architecture and deploy time come from the loaded checkpoint."""
    import torch
    checkpoint = torch.load(WEIGHTS_PATH, map_location="cpu", weights_only=False)
    version = inference.model_version()
    arch = checkpoint.get("architecture", "resnet18")
    assert version.startswith(f"cnn-{arch}:saved_model_cnn.pt@")
    assert str(WEIGHTS_PATH.parent) not in version
    if checkpoint.get("deployed_at"):
        assert version.endswith("@" + checkpoint["deployed_at"])


def test_model_version_falls_back_without_a_checkpoint(monkeypatch, tmp_path):
    monkeypatch.setitem(IMAGE_MODEL, "weights_path", str(tmp_path / "missing.pt"))
    inference._load_trained_model.cache_clear()
    try:
        assert inference.model_version() == "cnn-resnet18:missing.pt"
    finally:
        inference._load_trained_model.cache_clear()
