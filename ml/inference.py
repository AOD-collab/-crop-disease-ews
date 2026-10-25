"""
The CNN: loads the trained model and classifies a photo Spark already
prepared (spark/preprocessing.model_input). No preprocessing here.

A newly deployed model file (ml/trained_model/saved_model_cnn.pt) is picked
up automatically. predict() returns predicted_class, its confidence,
lesion_score (= 1 - P(healthy)) and is_uncertain.
"""

import os
import threading
from pathlib import Path

from config.settings import IMAGE_MODEL

# the class reported when the confidence is below IMAGE_MODEL["confidence_threshold"]
UNCERTAIN_CLASS = "uncertain"


class ModelNotAvailable(Exception):
    """Raised when no usable checkpoint is found."""


_model_cache = {"key": None, "value": None}
_model_cache_lock = threading.Lock()


def _load_trained_model():
    """The loaded model, cached; reloaded when the model file changes."""
    weights_path = IMAGE_MODEL["weights_path"]
    try:
        st = os.stat(weights_path)
        key = (weights_path, st.st_mtime_ns, st.st_size, st.st_ino)
    except OSError:
        key = (weights_path, None)
    with _model_cache_lock:
        if _model_cache["key"] != key:
            _model_cache["value"] = _load_checkpoint(weights_path)
            _model_cache["key"] = key
        return _model_cache["value"]


def _cache_clear() -> None:
    with _model_cache_lock:
        _model_cache.update(key=None, value=None)


_load_trained_model.cache_clear = _cache_clear


def _pick_device(torch):
    """GPU when available (IMAGE_MODEL["device"] = auto/cuda), else CPU."""
    wanted = IMAGE_MODEL.get("device", "auto")
    if wanted in ("auto", "cuda") and torch.cuda.is_available():
        return torch.device("cuda")
    if wanted == "cuda":
        print("[ml.inference] MODEL_DEVICE=cuda but PyTorch sees no CUDA GPU "
              f"(PyTorch {torch.__version__}) — using the CPU.")
    return torch.device("cpu")


def _device_name(torch, device) -> str:
    return f"GPU ({torch.cuda.get_device_name(device)})" if device.type == "cuda" else "CPU"


def _load_checkpoint(weights_path: str):
    if not os.path.exists(weights_path):
        raise ModelNotAvailable(
            f"No trained model checkpoint was found at '{weights_path}'. Train and "
            f"deploy a model from the dashboard's Model training page (the "
            f"crop_disease_ews training project), or point MODEL_PATH at the right path."
        )

    import torch
    from torchvision import models

    checkpoint = torch.load(weights_path, map_location="cpu")

    architecture = checkpoint.get("architecture", "resnet18")
    class_names = checkpoint["class_names"]
    if architecture == "resnet18":
        model = models.resnet18(weights=None)
        model.fc = torch.nn.Linear(model.fc.in_features, len(class_names))
    elif architecture == "se_resnet18":
        from ml.se_resnet import SEResNet18
        model = SEResNet18(num_classes=len(class_names))
    else:
        raise ModelNotAvailable(
            f"Checkpoint architecture '{architecture}' isn't supported by this "
            f"inference module yet — only 'resnet18' and 'se_resnet18' are wired "
            f"up. Extend _load_trained_model() in ml/inference.py to add it."
        )
    model.load_state_dict(checkpoint["state_dict"])
    device = _pick_device(torch)
    model.to(device).eval()
    threads = int(IMAGE_MODEL.get("torch_threads", 0) or 0)
    if threads > 0 and device.type == "cuda":
        # fewer CPU threads: Spark already runs several photos at once
        torch.set_num_threads(threads)

    # the input the model expects (input_spec)
    img_size = int(checkpoint.get("img_size", 224))
    mean = [float(v) for v in checkpoint.get("mean", [0.485, 0.456, 0.406])]
    std = [float(v) for v in checkpoint.get("std", [0.229, 0.224, 0.225])]

    healthy_class_name = checkpoint.get("healthy_class_name", "healthy")
    if healthy_class_name not in class_names:
        print(
            f"[ml.inference] Warning: healthy_class_name '{healthy_class_name}' not "
            f"found in class_names {class_names}. lesion_score will default to "
            f"(1 - top healthy-like match); check your checkpoint."
        )

    print(f"[ml.inference] Loaded trained model from '{weights_path}' "
          f"(classes: {class_names}) on {_device_name(torch, device)}")

    # the deploy time tells two models with the same file name apart
    deployed = checkpoint.get("deployed_at") or checkpoint.get("source_modified_at")
    if not deployed:
        from datetime import datetime
        deployed = datetime.fromtimestamp(os.path.getmtime(weights_path)).isoformat(timespec="seconds")

    return {
        "model": model,
        "device": device,
        "input_spec": {"img_size": img_size, "mean": mean, "std": std},
        "class_names": class_names,
        "healthy_class_name": healthy_class_name,
        "version": f"cnn-{architecture}:{Path(weights_path).name}@{deployed}",
    }


def input_spec() -> dict:
    """{"img_size", "mean", "std"} the deployed model expects."""
    return dict(_load_trained_model()["input_spec"])


def _as_input(loaded, model_input):
    """The prepared photo as a tensor; refuses the wrong shape."""
    import torch
    tensor = torch.as_tensor(model_input, dtype=torch.float32)
    size = loaded["input_spec"]["img_size"]
    if tuple(tensor.shape) != (3, size, size):
        raise ValueError(f"model input must have shape (3, {size}, {size}) — "
                         f"prepare it with spark.preprocessing.model_input; got {tuple(tensor.shape)}")
    return tensor


def _precision(loaded) -> str:
    """"fp16" only on a GPU when configured, else "fp32"."""
    wanted = str(IMAGE_MODEL.get("precision", "fp32")).lower()
    return "fp16" if wanted == "fp16" and loaded["device"].type == "cuda" else "fp32"


def _forward(loaded, batch, precision: str = None, pin_memory: bool = None):
    """Class probabilities for a batch (softmax always in FP32)."""
    import torch
    precision = precision or _precision(loaded)
    pin_memory = IMAGE_MODEL.get("pin_memory", True) if pin_memory is None else pin_memory
    device = loaded["device"]
    if device.type == "cuda" and pin_memory:
        batch = batch.pin_memory().to(device, non_blocking=True)
    else:
        batch = batch.to(device)
    with torch.inference_mode():
        with torch.autocast(device_type="cuda", dtype=torch.float16,
                            enabled=(precision == "fp16" and device.type == "cuda")):
            logits = loaded["model"](batch)
        return torch.softmax(logits.float(), dim=1).cpu()


def _forward_safely(loaded, tensors: list, precision: str = None) -> list:
    """_forward, halving the batch on a GPU out-of-memory error."""
    import torch
    try:
        probs = _forward(loaded, torch.stack(tensors), precision)
        return list(probs)
    except torch.cuda.OutOfMemoryError:
        torch.cuda.empty_cache()
        if len(tensors) == 1:
            raise
        half = len(tensors) // 2
        _batcher_note_oom(loaded, len(tensors))
        return _forward_safely(loaded, tensors[:half], precision) + _forward_safely(loaded, tensors[half:], precision)


# One thread runs the GPU: photos from Spark's threads are stacked into one
# batch (faster than many small passes); each photo still gets its own result.
class _GpuBatcher:
    def __init__(self, loaded, max_batch: int, wait_ms: float):
        import queue
        self.loaded = loaded
        self.max_batch = max(1, int(max_batch))
        self.wait = max(0.0, float(wait_ms)) / 1000.0
        self.queue = queue.Queue()
        self.thread = threading.Thread(target=self._run, name="cnn-gpu-batcher", daemon=True)
        self.thread.start()

    def submit(self, tensor):
        from concurrent.futures import Future
        fut = Future()
        self.queue.put((tensor, fut))
        return fut

    def _run(self):
        import queue
        import time
        while True:
            items = [self.queue.get()]
            deadline = time.monotonic() + self.wait
            while len(items) < self.max_batch:
                remaining = deadline - time.monotonic()
                try:
                    items.append(self.queue.get(timeout=remaining) if remaining > 0 else self.queue.get_nowait())
                except queue.Empty:
                    break
            try:
                probs = _forward_safely(self.loaded, [t for t, _ in items])
                for (_, fut), p in zip(items, probs):
                    fut.set_result((p, len(items)))
            except BaseException as e:  # noqa: BLE001 - every waiting photo gets the error
                for _, fut in items:
                    if not fut.done():
                        fut.set_exception(e)


_batcher_lock = threading.Lock()


def _batcher_note_oom(loaded, size: int) -> None:
    """After an out-of-memory error, keeps later batches smaller."""
    with _batcher_lock:
        b = loaded.get("batcher")
        if b is not None and b.max_batch >= size:
            b.max_batch = max(1, size // 2)
            print(f"[ml.inference] CUDA out of memory with a batch of {size}; "
                  f"batches are now at most {b.max_batch}")


def _batcher(loaded):
    """The batcher for this model, or None when batching is off."""
    size = int(IMAGE_MODEL.get("batch_size", 1) or 1)
    if size <= 1:
        return None
    with _batcher_lock:
        if loaded.get("batcher") is None:
            loaded["batcher"] = _GpuBatcher(loaded, size, IMAGE_MODEL.get("batch_wait_ms", 5))
        return loaded["batcher"]


def _probabilities(loaded, model_input, timings: dict = None):
    """(probabilities, batch size) for one photo; fills timings if given."""
    import time
    t0 = time.perf_counter()
    tensor = _as_input(loaded, model_input)
    t1 = time.perf_counter()
    batcher = _batcher(loaded)
    if batcher is not None:
        out = batcher.submit(tensor).result()
    else:
        out = _forward_safely(loaded, [tensor])[0], 1
    if timings is not None:
        timings["input_s"], timings["forward_s"] = round(t1 - t0, 4), round(time.perf_counter() - t1, 4)
    return out


def predict(model_input) -> dict:
    """Classifies one prepared photo."""
    loaded = _load_trained_model()
    timings = {}
    probs, batch_size = _probabilities(loaded, model_input, timings)
    result = _result_from_probs(loaded, probs)
    result["inference"] = {"batch_size": batch_size, "precision": _precision(loaded), **timings}
    return result


def _result_from_probs(loaded, probs) -> dict:
    """Probabilities -> class, confidence, lesion_score, is_uncertain."""
    import torch
    class_names = loaded["class_names"]
    healthy_class_name = loaded["healthy_class_name"]

    top_idx = int(torch.argmax(probs))
    predicted_class = class_names[top_idx]
    predicted_class_confidence = float(probs[top_idx])

    if healthy_class_name in class_names:
        p_healthy = float(probs[class_names.index(healthy_class_name)])
    else:
        p_healthy = float(probs[top_idx]) if predicted_class == healthy_class_name else 0.0
    lesion_score = round(max(0.0, min(1.0, 1.0 - p_healthy)), 3)

    # too unsure to name a disease: report "uncertain" (risk is then capped below HIGH)
    confidence_threshold = IMAGE_MODEL.get("confidence_threshold", 0.5)
    is_uncertain = predicted_class_confidence < confidence_threshold
    if is_uncertain:
        predicted_class = UNCERTAIN_CLASS

    return {
        "lesion_score": lesion_score,
        "discoloration_pct": None,
        "texture_irregularity": None,
        "leaf_area_affected_pct": None,
        "model_confidence": round(predicted_class_confidence, 3),
        "predicted_class": predicted_class,
        "predicted_class_confidence": round(predicted_class_confidence, 3),
        "is_uncertain": is_uncertain,
    }


def disease_heatmap(model_input):
    """Grad-CAM: where in the input the model saw the disease (a small 2-D map),
    or None for a healthy leaf. Computed without hooks, so it is thread-safe."""
    loaded = _load_trained_model()

    import torch
    import torch.nn.functional as F

    model = loaded["model"]
    class_names = loaded["class_names"]
    healthy_class_name = loaded["healthy_class_name"]

    tensor = _as_input(loaded, model_input).unsqueeze(0).to(loaded["device"])

    # always FP32: gradients in FP16 would change the map, and it's one photo
    with torch.enable_grad():
        x = model.maxpool(model.relu(model.bn1(model.conv1(tensor))))
        feats = model.layer4(model.layer3(model.layer2(model.layer1(x))))
        logits = model.fc(torch.flatten(model.avgpool(feats), 1))
        top_idx = int(torch.argmax(logits[0]))
        if class_names[top_idx] == healthy_class_name:
            return None
        grads, = torch.autograd.grad(logits[0, top_idx], feats)

    weights = grads.mean(dim=(2, 3), keepdim=True)            # one weight per channel
    cam = F.relu((weights * feats).sum(dim=1, keepdim=True)).detach()
    if float(cam.max()) <= 0:
        return None

    return cam[0, 0].cpu().numpy()


def model_version() -> str:
    """Which model made a prediction: "cnn-<architecture>:<file>@<deployed_at>"."""
    try:
        return _load_trained_model()["version"]
    except Exception:  # noqa: BLE001 - e.g. ModelNotAvailable, or a stub model in a test
        return f"cnn-resnet18:{Path(IMAGE_MODEL['weights_path']).name}"
