"""
Checkpoint work for the Model training page (dashboard/training_page.py),
run as a separate process:

    python -m dashboard.model_ops info   <checkpoint>
    python -m dashboard.model_ops deploy <best checkpoint> <deployed model> <backup dir> <extra-metadata json>

Why a separate process: the dashboard imports pandas, which loads pyarrow,
and on Windows importing torch AFTER pyarrow fails ("WinError 1114 ...
c10.dll" — the two ship clashing C++ runtime DLLs). This module imports
torch (only inside its functions, so the page can import CLASS_NAME_MAP
from here safely) and nothing that pulls in pandas/pyarrow. Prints one JSON
object on stdout.
"""

import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Training-project class names -> the names the rest of this system uses
# (predictions table, DISEASE_COLOR_MAP, Android app).
CLASS_NAME_MAP = {
    "Healthy": "healthy",
    "Blight": "northern_leaf_blight",
    "Common_Rust": "common_rust",
    "Gray_Leaf_Spot": "gray_leaf_spot",
}


def info(path: str) -> dict:
    """Everything in a checkpoint except the weights."""
    import torch
    ck = torch.load(path, map_location="cpu", weights_only=True)
    return {k: v for k, v in ck.items() if not k.endswith("state_dict")}


def deploy(best: str, deployed: str, backup_dir: str, extra: dict) -> dict:
    """Export the training project's best checkpoint into the format
    ml/inference.py loads, check that it loads and runs, back up the model in
    use, then swap the new file in."""
    import torch
    from ml.se_resnet import SEResNet18

    best, deployed, backup_dir = Path(best), Path(deployed), Path(backup_dir)
    ck = torch.load(best, map_location="cpu", weights_only=True)
    class_names = [CLASS_NAME_MAP.get(c, c.lower()) for c in ck["class_names"]]
    exported = {
        "state_dict": ck["model_state_dict"],
        "architecture": "se_resnet18",
        "class_names": class_names,
        "healthy_class_name": "healthy",
        "img_size": ck.get("img_size", 224),
        "mean": [0.485, 0.456, 0.406],
        "std": [0.229, 0.224, 0.225],
        # traceability only — ml/inference.py ignores these
        "trained_epoch": ck.get("epoch"),
        "val_accuracy": ck.get("val_accuracy"),
        "source_checkpoint": str(best),
        "source_modified_at": datetime.fromtimestamp(best.stat().st_mtime).isoformat(timespec="seconds"),
        "deployed_at": datetime.now().isoformat(timespec="seconds"),
        **extra,
    }

    # Refuse to deploy anything that wouldn't load in ml/inference.py.
    model = SEResNet18(num_classes=len(class_names))
    model.load_state_dict(exported["state_dict"])
    model.eval()
    with torch.no_grad():
        model(torch.zeros(1, 3, exported["img_size"], exported["img_size"]))

    deployed.parent.mkdir(parents=True, exist_ok=True)
    tmp = deployed.with_suffix(".pt.tmp")
    torch.save(exported, tmp)
    backup = ""
    if deployed.exists():
        backup_dir.mkdir(exist_ok=True)
        backup = str(backup_dir / f"{deployed.stem}_{datetime.now():%Y%m%d_%H%M%S}.pt")
        shutil.copy2(deployed, backup)
    os.replace(tmp, deployed)
    return {"backup": backup, "class_names": class_names}


def _test_metrics(log_text: str) -> dict:
    """Test-set results train.py prints at the end (same lines the page's
    _parse_log reads), in percent."""
    out = {}
    for key, label in (("loss", "Test Loss"), ("accuracy", "Test Accuracy"), ("precision", "Precision"),
                       ("recall", "Recall"), ("f1", "F1-Score")):
        if m := re.search(rf"^{label}: ([\d.]+)", log_text, re.M):
            out[key] = float(m.group(1))
    return out


def is_better(new: dict, current: dict | None) -> tuple[bool, str]:
    """Whether a freshly trained model (checkpoint metadata + test_metrics)
    should replace the deployed one. Compares test accuracy when both have
    it, else validation accuracy; a deployed model with neither recorded is
    always replaced. Ties keep the model in use."""
    def test_acc(m):
        return (m.get("test_metrics") or {}).get("accuracy")

    def val_acc(m):  # stored as a fraction in checkpoints
        return None if m.get("val_accuracy") is None else m["val_accuracy"] * 100

    if current is None:
        return True, "no model was deployed"
    for label, get in (("test accuracy", test_acc), ("validation accuracy", val_acc)):
        a, b = get(new), get(current)
        if a is not None and b is not None:
            if a > b:
                return True, f"{label} {a:.2f}% > {b:.2f}% of the model in use"
            return False, f"{label} {a:.2f}% is not better than {b:.2f}% of the model in use"
    return True, "the model in use has no recorded accuracy"


def train_and_deploy(cfg: dict) -> int:
    """Run train.py, then automatically deploy its best checkpoint if it
    beats the model in use. Started detached by the Model training page;
    this process's stdout is the run's log file, which train.py inherits, so
    the page reads both. The last line written is "Auto-deploy: ...".
    Killing this process tree (Stop training) means nothing is deployed."""
    import subprocess
    argv = [cfg["python"], "-u", cfg["script"]] + (["--resume"] if cfg["resume"] else [])
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    code = subprocess.run(argv, cwd=cfg["cwd"], stdin=subprocess.DEVNULL, env=env).returncode
    log_text = Path(cfg["log"]).read_text(encoding="utf-8", errors="replace")
    if code != 0 or "Training completed." not in log_text:
        print("\nAuto-deploy: skipped — training did not finish.", flush=True)
        return code or 1
    try:
        test_metrics = _test_metrics(log_text)
        new = {**info(cfg["best"]), "test_metrics": test_metrics or None}
        current = info(cfg["deployed"]) if Path(cfg["deployed"]).exists() else None
        better, reason = is_better(new, current)
        if not better:
            print(f"\nAuto-deploy: kept the model in use — {reason}.", flush=True)
            return 0
        deploy(cfg["best"], cfg["deployed"], cfg["backup_dir"],
               {"test_metrics": test_metrics or None, "deployed_by": "auto (after training)"})
        print(f"\nAuto-deploy: deployed the new model — {reason}.", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"\nAuto-deploy: failed, the model in use was not changed — {e}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    command, *args = sys.argv[1:]
    if command == "info":
        result = info(args[0])
    elif command == "deploy":
        result = deploy(args[0], args[1], args[2], json.loads(args[3]))
    elif command == "train":
        raise SystemExit(train_and_deploy(json.loads(args[0])))
    else:
        raise SystemExit(f"unknown command {command!r}")
    print(json.dumps(result))
