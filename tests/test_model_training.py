"""
Coverage for the dashboard's Model training page (dashboard/training_page.py)
and the se_resnet18 checkpoint support it relies on in ml/inference.py.

Nothing here starts a real training run or touches the real
ml/trained_model/saved_model_cnn.pt: every path the page uses is pointed at
a temp folder.
"""

from pathlib import Path

import pytest
import torch

from config.settings import IMAGE_MODEL
from dashboard import training_page
from ml import inference
from ml.se_resnet import SEResNet18
from tests.conftest import make_leaf_jpeg_bytes
from tests.test_dashboard_users import (  # noqa: F401 - fixtures used by the page test
    _logged_in, _all_text, _seed_admin_and_farmer, drive, fresh_db,
)

TRAINING_CLASSES = ["Healthy", "Blight", "Common_Rust", "Gray_Leaf_Spot"]

SAMPLE_LOG = """
GPU detected:
NVIDIA GeForce RTX 3050 6GB Laptop GPU

Resuming after epoch 2 (best val accuracy 90.00%)

Epoch 3/25
Train Loss: 0.2595
Train Accuracy: 90.45%
Validation Loss: 0.1645
Validation Accuracy: 92.88%
âœ“ Best model saved.

Epoch 4/25
Train Loss: 0.2279
Train Accuracy: 91.29%
Validation Loss: 0.1991
Validation Accuracy: 92.50%

Early stopping triggered.

Test Loss: 0.1683
Test Accuracy: 94.58%
Precision: 94.74%
Recall: 94.58%
F1-Score: 94.61%

Training completed.
"""


@pytest.fixture()
def training_paths(tmp_path, monkeypatch):
    """Training project, run state and deployed model all under tmp_path."""
    project = tmp_path / "crop_disease_ews"
    (project / "checkpoints").mkdir(parents=True)
    model_dir = tmp_path / "trained_model"
    model_dir.mkdir()
    for name, value in {
        "TRAINING_DIR": project,
        "TRAIN_SCRIPT": project / "train.py",
        "BEST_CHECKPOINT": project / "checkpoints" / "best_seresnet18_crop_ews.pth",
        "LAST_CHECKPOINT": project / "checkpoints" / "last_seresnet18_crop_ews.pth",
        "RESULTS_DIR": project / "results",
        "STATE_DIR": tmp_path / "state",
        "RUN_FILE": tmp_path / "state" / "run.json",
        "LOG_DIR": tmp_path / "state" / "logs",
        "DEPLOYED_MODEL": model_dir / "saved_model_cnn.pt",
        "BACKUP_DIR": model_dir / "backups",
    }.items():
        monkeypatch.setattr(training_page, name, value)
    training_page._checkpoint_info.clear()
    return training_page


def _write_training_checkpoint(path: Path):
    model = SEResNet18(num_classes=len(TRAINING_CLASSES))
    torch.save({"model_state_dict": model.state_dict(), "class_names": TRAINING_CLASSES,
                "img_size": 224, "val_accuracy": 0.9598, "epoch": 10}, path)


def test_parse_log_reads_epochs_and_test_results():
    parsed = training_page._parse_log(SAMPLE_LOG)
    assert parsed["device"] == "NVIDIA GeForce RTX 3050 6GB Laptop GPU"
    assert parsed["resumed_after"] == 2 and parsed["total_epochs"] == 25
    assert [e["epoch"] for e in parsed["epochs"]] == [3, 4]
    assert parsed["epochs"][0] == {"epoch": 3, "train_loss": 0.2595, "train_acc": 90.45,
                                   "val_loss": 0.1645, "val_acc": 92.88, "best": True}
    assert parsed["epochs"][1]["best"] is False
    assert parsed["test"] == {"loss": 0.1683, "accuracy": 94.58, "precision": 94.74,
                              "recall": 94.58, "f1": 94.61}
    assert parsed["completed"] and parsed["early_stop"] and parsed["error"] is None


def test_parse_log_reports_a_crash():
    parsed = training_page._parse_log("Epoch 1/25\nTraceback (most recent call last):\n  boom\nRuntimeError: x")
    assert not parsed["completed"]
    assert parsed["error"].endswith("RuntimeError: x")


def test_deploy_backs_up_old_model_and_inference_loads_the_new_one(training_paths, monkeypatch):
    _write_training_checkpoint(training_paths.BEST_CHECKPOINT)
    training_paths.DEPLOYED_MODEL.write_bytes(b"old model")

    backup = training_paths._deploy_best({"accuracy": 94.58, "f1": 94.61})

    assert Path(backup).read_bytes() == b"old model"
    deployed = torch.load(training_paths.DEPLOYED_MODEL, map_location="cpu", weights_only=True)
    assert deployed["architecture"] == "se_resnet18"
    # Mapped to the names the rest of the system uses, in the training index order.
    assert deployed["class_names"] == ["healthy", "northern_leaf_blight", "common_rust", "gray_leaf_spot"]
    assert deployed["test_metrics"]["accuracy"] == 94.58

    monkeypatch.setitem(IMAGE_MODEL, "weights_path", str(training_paths.DEPLOYED_MODEL))
    inference._load_trained_model.cache_clear()
    try:
        result = inference.predict(make_leaf_jpeg_bytes(128))
        assert result["predicted_class"] in deployed["class_names"] + [inference.UNCERTAIN_CLASS]
        heat = inference.disease_heatmap(make_leaf_jpeg_bytes(128))
        assert heat is None or heat.shape == (128, 128)
    finally:
        inference._load_trained_model.cache_clear()


def test_model_ops_work_from_a_process_that_already_loaded_pandas(tmp_path):
    """Regression: in the dashboard process pandas/pyarrow are imported
    first, after which `import torch` fails on Windows (WinError 1114,
    c10.dll). The page must therefore never import torch itself."""
    import subprocess
    import sys
    checkpoint = tmp_path / "best.pth"
    _write_training_checkpoint(checkpoint)
    code = ("import pandas, pyarrow, sys\n"
            "from dashboard import training_page\n"
            f"info = training_page._model_ops('info', {str(checkpoint)!r})\n"
            "assert info['val_accuracy'] == 0.9598, info\n"
            "assert 'torch' not in sys.modules\n")
    result = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).resolve().parent.parent,
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr[-2000:]


def test_is_better_compares_test_then_validation_accuracy():
    from dashboard.model_ops import is_better
    new = {"val_accuracy": 0.95, "test_metrics": {"accuracy": 94.0}}
    assert is_better(new, None)[0]
    assert is_better(new, {"class_names": []})[0]                         # nothing recorded
    assert is_better(new, {"val_accuracy": 0.99, "test_metrics": {"accuracy": 93.0}})[0]
    assert not is_better(new, {"val_accuracy": 0.90, "test_metrics": {"accuracy": 94.0}})[0]  # tie keeps
    assert not is_better(new, {"val_accuracy": 0.96})[0]                  # no test metrics -> val
    assert is_better(new, {"val_accuracy": 0.94})[0]


def _run_train_wrapper(training_paths, tmp_path):
    """Run model_ops' train command the way the page starts it, with a
    stand-in train.py that only prints SAMPLE_LOG."""
    import json
    import subprocess
    import sys
    fake_train = tmp_path / "fake_train.py"
    fake_train.write_text(f"print({SAMPLE_LOG!r})", encoding="utf-8")
    log = tmp_path / "train.log"
    cfg = {"python": sys.executable, "script": str(fake_train), "cwd": str(tmp_path), "resume": False,
           "log": str(log), "best": str(training_paths.BEST_CHECKPOINT),
           "deployed": str(training_paths.DEPLOYED_MODEL), "backup_dir": str(training_paths.BACKUP_DIR)}
    with open(log, "wb") as out:
        subprocess.run([sys.executable, "-u", "-m", "dashboard.model_ops", "train", json.dumps(cfg)],
                       cwd=Path(__file__).resolve().parent.parent, stdout=out, stderr=subprocess.STDOUT,
                       timeout=300)
    return training_paths._parse_log(log.read_text(encoding="utf-8"))


def test_finished_training_auto_deploys_a_better_model(training_paths, tmp_path):
    _write_training_checkpoint(training_paths.BEST_CHECKPOINT)

    parsed = _run_train_wrapper(training_paths, tmp_path)

    assert parsed["auto_deploy"].startswith("deployed the new model"), parsed["auto_deploy"]
    deployed = torch.load(training_paths.DEPLOYED_MODEL, map_location="cpu", weights_only=True)
    assert deployed["test_metrics"]["accuracy"] == 94.58
    assert deployed["deployed_by"] == "auto (after training)"


def test_finished_training_keeps_a_better_deployed_model(training_paths, tmp_path):
    _write_training_checkpoint(training_paths.BEST_CHECKPOINT)
    torch.save({"class_names": ["healthy"], "val_accuracy": 0.99, "test_metrics": {"accuracy": 98.0}},
               training_paths.DEPLOYED_MODEL)

    parsed = _run_train_wrapper(training_paths, tmp_path)

    assert parsed["auto_deploy"].startswith("kept the model in use"), parsed["auto_deploy"]
    kept = torch.load(training_paths.DEPLOYED_MODEL, map_location="cpu", weights_only=True)
    assert kept["test_metrics"]["accuracy"] == 98.0
    assert not training_paths.BACKUP_DIR.exists()


def test_inference_reloads_a_replaced_model_file(training_paths, monkeypatch):
    _write_training_checkpoint(training_paths.BEST_CHECKPOINT)
    training_paths._deploy_best({})
    monkeypatch.setitem(IMAGE_MODEL, "weights_path", str(training_paths.DEPLOYED_MODEL))
    inference._load_trained_model.cache_clear()
    try:
        first = inference._load_trained_model()
        assert inference._load_trained_model() is first          # cached while unchanged
        training_paths._deploy_best({"accuracy": 99.0})            # os.replace, like a real deploy
        assert inference._load_trained_model() is not first        # picked up without a restart
    finally:
        inference._load_trained_model.cache_clear()


def test_restore_puts_a_backup_back_and_keeps_the_replaced_model(training_paths):
    training_paths.BACKUP_DIR.mkdir()
    old = training_paths.BACKUP_DIR / "saved_model_cnn_20260101_000000.pt"
    old.write_bytes(b"older model")
    training_paths.DEPLOYED_MODEL.write_bytes(b"current model")

    training_paths._restore_backup(old)

    assert training_paths.DEPLOYED_MODEL.read_bytes() == b"older model"
    assert b"current model" in [p.read_bytes() for p in training_paths.BACKUP_DIR.glob("*.pt")]


def test_resume_without_a_previous_run_is_refused(training_paths, monkeypatch):
    training_paths.TRAIN_SCRIPT.write_text("")
    monkeypatch.setattr(training_paths, "TRAINING_PYTHON", Path(__file__))  # any existing file
    assert "no previous run" in training_paths._start_training(resume=True)


def test_page_renders_with_last_run_and_deploy_section(training_paths, drive):  # noqa: F811
    _write_training_checkpoint(training_paths.BEST_CHECKPOINT)
    training_paths.LOG_DIR.mkdir(parents=True)
    log = training_paths.LOG_DIR / "train.log"
    log.write_text(SAMPLE_LOG, encoding="utf-8")
    # A finished run: pid 0 never matches a live process with this create_time.
    training_paths._write_run({"pid": 0, "create_time": 0, "mode": "resume", "log": str(log),
                               "started_at": "2026-10-03T18:00:00", "stopped_by_admin": False})
    _seed_admin_and_farmer()

    at = _logged_in("Model Training")

    text = _all_text(at)
    assert "<h1>Model training</h1>" in text
    assert "Last run: Finished" in text
    assert "Validation accuracy: **95.98%**" in text
    assert "Test accuracy: **94.58%**" in text
    assert any(b.label == "Start training" for b in at.button)
    deploy = [b for b in at.button if b.label == "Deploy best model"][0]
    assert deploy.disabled  # until the confirmation box is ticked
