"""
ml/inference.py speed paths: torch.inference_mode, the shared GPU batcher
(MODEL_BATCH_SIZE), FP16 autocast (MODEL_PRECISION) and the reuse of a
photo's prepared input by disease_heatmap(). Every path must give each photo
its own prediction, and the same prediction as the original one-photo FP32
path (within float tolerance).
"""

from concurrent.futures import ThreadPoolExecutor

import pytest
import torch

from config.settings import IMAGE_MODEL
from ml import inference
from tests.conftest import make_leaf_jpeg_bytes


@pytest.fixture()
def settings(monkeypatch):
    """Temporarily change IMAGE_MODEL; the batcher is rebuilt afterwards."""
    def apply(**values):
        for k, v in values.items():
            monkeypatch.setitem(IMAGE_MODEL, k, v)
        inference._load_trained_model()["batcher"] = None
        inference._input_cache.clear()
    yield apply
    inference._load_trained_model()["batcher"] = None


def _photos(n):
    return [make_leaf_jpeg_bytes(64 + 8 * i) for i in range(n)]


def _baseline(photo):
    """The original predict() forward pass: FP32, batch of 1, no_grad."""
    loaded = inference._load_trained_model()
    _, tensor = inference._prepared_input(loaded, photo)
    with torch.no_grad():
        logits = loaded["model"](tensor.unsqueeze(0).to(loaded["device"]))
        return torch.softmax(logits, dim=1)[0].cpu()


def test_predict_keys_unchanged_plus_inference_info(settings):
    settings(batch_size=1, precision="fp32")
    r = inference.predict(_photos(1)[0])
    assert {"lesion_score", "model_confidence", "predicted_class", "predicted_class_confidence",
            "is_uncertain", "discoloration_pct"} <= set(r)
    assert (r["inference"]["batch_size"], r["inference"]["precision"]) == (1, "fp32")
    assert r["inference"]["input_s"] >= 0 and r["inference"]["forward_s"] >= 0


def test_batched_threads_each_get_their_own_prediction(settings):
    settings(batch_size=8, batch_wait_ms=20, precision="fp32")
    photos = _photos(12)
    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda p: inference._probabilities(inference._load_trained_model(), p), photos))
    assert max(size for _, size in results) > 1            # they really ran together
    for photo, (probs, _) in zip(photos, results):
        assert torch.allclose(probs, _baseline(photo), atol=1e-3)


def test_fp16_matches_fp32_classes(settings):
    if not torch.cuda.is_available():
        pytest.skip("FP16 is GPU-only; on the CPU it falls back to FP32")
    settings(batch_size=1, precision="fp16")
    for photo in _photos(4):
        r = inference.predict(photo)
        assert r["inference"]["precision"] == "fp16"
        base = _baseline(photo)
        assert inference.UNCERTAIN_CLASS == r["predicted_class"] or \
            r["predicted_class"] == inference._load_trained_model()["class_names"][int(base.argmax())]
        assert abs(r["predicted_class_confidence"] - float(base.max())) < 0.01


def test_fp16_is_fp32_on_the_cpu(settings, monkeypatch):
    settings(precision="fp16")
    loaded = dict(inference._load_trained_model(), device=torch.device("cpu"))
    assert inference._precision(loaded) == "fp32"


def test_out_of_memory_splits_the_batch_and_shrinks_later_batches(settings, monkeypatch):
    settings(batch_size=8, precision="fp32")
    loaded = inference._load_trained_model()
    batcher = inference._batcher(loaded)
    real = inference._forward

    def tight_memory(loaded_, batch, precision=None, pin_memory=None):
        if batch.shape[0] > 2:
            raise torch.cuda.OutOfMemoryError("CUDA out of memory (test)")
        return real(loaded_, batch, precision, pin_memory)
    monkeypatch.setattr(inference, "_forward", tight_memory)
    photos = _photos(8)
    tensors = [inference._prepared_input(loaded, p)[1] for p in photos]
    probs = inference._forward_safely(loaded, tensors)
    assert len(probs) == 8
    for p, photo in zip(probs, photos):
        assert torch.allclose(p, _baseline(photo), atol=1e-3)
    assert batcher.max_batch <= 2


def test_heatmap_reuses_the_prepared_input(settings, monkeypatch):
    settings(batch_size=1)
    photo = _photos(1)[0]
    inference.predict(photo)
    loaded = inference._load_trained_model()
    calls = []
    real = loaded["preprocess"]
    monkeypatch.setitem(loaded, "preprocess", lambda img: calls.append(1) or real(img))
    inference.disease_heatmap(photo)
    assert calls == []
