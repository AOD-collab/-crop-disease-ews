"""
"Model training" page of the ADMIN dashboard (dashboard/app.py).

Training does NOT happen in this project. It runs in the separate
crop_disease_ews training project (its own venv with CUDA torch, its
dataset and train.py) on this same laptop. This page only:

  * starts `train.py` there as a detached background process (so a
    Streamlit rerun, a closed browser tab or a dashboard restart doesn't
    kill a 10-minute GPU run), and remembers which run it started in
    storage/model_training/run.json;
  * shows live progress by reading that run's log file;
  * shows the training project's test results when it finishes;
  * deploys the best checkpoint into ml/trained_model/saved_model_cnn.pt
    in the format ml/inference.py reads (architecture "se_resnet18"),
    after backing up the model in use — automatically when a run finishes,
    but only if it scores better than the model in use (a new run can be
    worse; dashboard/model_ops.py train_and_deploy), or by hand with the
    Deploy button. ml/inference.py reloads the file when it changes, so
    Spark uses the new model without a restart.

Where the training project lives (.env):
    TRAINING_PROJECT_DIR   default C:\\Users\\user\\OneDrive\\Pictures\\crop_disease_ews
    TRAINING_PYTHON        default <TRAINING_PROJECT_DIR>\\crop_classifier_env\\Scripts\\python.exe
"""

import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config.settings import BASE_DIR, IMAGE_MODEL, TRAINING_PROJECT_DIR
from dashboard.theme import metric_row

TRAINING_DIR = TRAINING_PROJECT_DIR
TRAINING_PYTHON = Path(os.getenv("TRAINING_PYTHON",
                                 str(TRAINING_DIR / "crop_classifier_env" / "Scripts" / "python.exe")))
TRAIN_SCRIPT = TRAINING_DIR / "train.py"
BEST_CHECKPOINT = TRAINING_DIR / "checkpoints" / "best_seresnet18_crop_ews.pth"
LAST_CHECKPOINT = TRAINING_DIR / "checkpoints" / "last_seresnet18_crop_ews.pth"
RESULTS_DIR = TRAINING_DIR / "results"

STATE_DIR = BASE_DIR / "storage" / "model_training"
RUN_FILE = STATE_DIR / "run.json"
LOG_DIR = STATE_DIR / "logs"

DEPLOYED_MODEL = Path(IMAGE_MODEL["weights_path"])
BACKUP_DIR = DEPLOYED_MODEL.parent / "backups"

from dashboard.model_ops import (  # noqa: E402 - model_ops imports torch only inside its functions
    CLASS_NAME_MAP, _test_metrics, detached_popen, swap_in)

COLOR_TRAIN = "#2E7D32"
COLOR_VAL = "#E0A800"


# ------------------------------------------------------------------
# Run bookkeeping
# ------------------------------------------------------------------
def _read_run() -> dict | None:
    try:
        return json.loads(RUN_FILE.read_text())
    except (OSError, ValueError):
        return None


def _write_run(run: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    RUN_FILE.write_text(json.dumps(run, indent=2))


def _process(run: dict | None):
    """The live psutil.Process of `run`, or None. Matches on create_time too,
    so a recycled PID belonging to some other program is never mistaken for
    (or killed as) the training run."""
    if not run or run.get("pid", 0) <= 0:
        return None
    import psutil
    try:
        proc = psutil.Process(run["pid"])
        if abs(proc.create_time() - run["create_time"]) > 1 or proc.status() == psutil.STATUS_ZOMBIE:
            return None
        return proc
    except (psutil.Error, KeyError):
        return None


def _start_training(resume: bool) -> str | None:
    """Launch train.py in the training project. Returns an error message,
    or None on success."""
    if _process(_read_run()):
        return "A training run is already in progress."
    if not TRAINING_PYTHON.exists():
        return f"Training Python not found at `{TRAINING_PYTHON}` (set TRAINING_PYTHON in .env)."
    if not TRAIN_SCRIPT.exists():
        return f"`train.py` not found in `{TRAINING_DIR}` (set TRAINING_PROJECT_DIR in .env)."
    if resume and not LAST_CHECKPOINT.exists():
        return "There is no previous run to continue — train from scratch first."

    import psutil
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = datetime.now()
    log_path = LOG_DIR / f"train_{started:%Y%m%d_%H%M%S}.log"
    # dashboard.model_ops runs train.py and, when it finishes, deploys the
    # best checkpoint if it beats the model in use (see train_and_deploy).
    cfg = {"python": str(TRAINING_PYTHON), "script": str(TRAIN_SCRIPT), "cwd": str(TRAINING_DIR),
           "resume": resume, "log": str(log_path), "best": str(BEST_CHECKPOINT),
           "deployed": str(DEPLOYED_MODEL), "backup_dir": str(BACKUP_DIR)}
    proc = detached_popen([sys.executable, "-u", "-m", "dashboard.model_ops", "train", json.dumps(cfg)],
                          log_path, BASE_DIR)
    _write_run({
        "pid": proc.pid,
        "create_time": psutil.Process(proc.pid).create_time(),
        "mode": "resume" if resume else "scratch",
        "started_at": started.isoformat(timespec="seconds"),
        "log": str(log_path),
        "started_by": st.session_state.get("admin_user", {}).get("username"),
        "stopped_by_admin": False,
    })
    return None


def _stop_training(run: dict) -> None:
    proc = _process(run)
    if proc:
        import psutil
        procs = proc.children(recursive=True) + [proc]  # DataLoader workers too
        for p in procs:
            try:
                p.kill()
            except psutil.Error:
                pass
        psutil.wait_procs(procs, timeout=10)
    run["stopped_by_admin"] = True
    _write_run(run)


# ------------------------------------------------------------------
# Log parsing (the plain-text output of train.py)
# ------------------------------------------------------------------
_FLOAT = r"([\d.]+)"


def _read_log(run: dict) -> str:
    try:
        return Path(run["log"]).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _parse_log(text: str) -> dict:
    out = {"epochs": [], "total_epochs": None, "resumed_after": None, "device": None,
           "early_stop": "Early stopping triggered" in text,
           "completed": "Training completed." in text,
           "error": None, "test": {}}
    if m := re.search(r"Resuming after epoch (\d+)", text):
        out["resumed_after"] = int(m.group(1))
    if m := re.search(r"GPU detected:\s*\n(.+)", text):
        out["device"] = m.group(1).strip()
    elif "Training on CPU." in text:
        out["device"] = "CPU (no GPU found — this will be much slower)"
    for block in re.split(r"\nEpoch (?=\d+/\d+)", text)[1:]:
        m = re.match(r"(\d+)/(\d+)", block)
        epoch = {"epoch": int(m.group(1))}
        out["total_epochs"] = int(m.group(2))
        for key, label in (("train_loss", "Train Loss"), ("train_acc", "Train Accuracy"),
                           ("val_loss", "Validation Loss"), ("val_acc", "Validation Accuracy")):
            if v := re.search(rf"{label}: {_FLOAT}", block):
                epoch[key] = float(v.group(1))
        epoch["best"] = "Best model saved" in block
        out["epochs"].append(epoch)
    out["test"].update(_test_metrics(text))
    if m := re.search(r"Traceback \(most recent call last\):.*", text, re.S):
        out["error"] = m.group(0)[-3000:]
    out["auto_deploy"] = m.group(1).strip() if (m := re.search(r"^Auto-deploy: (.+)$", text, re.M)) else None
    return out


def _run_outcome(run: dict, parsed: dict) -> tuple[str, str]:
    """(level, message) for a run that is no longer running."""
    if parsed["completed"]:
        return "ok", "Finished" + (" (stopped early: no improvement for a while)" if parsed["early_stop"] else "")
    if run.get("stopped_by_admin"):
        return "warn", "Stopped by an admin before it finished"
    return "bad", "Failed — see the error below"


# ------------------------------------------------------------------
# Models: what's deployed, what training produced, deploy / restore
# ------------------------------------------------------------------
def _model_ops(*args: str) -> dict:
    """Run dashboard/model_ops.py in its own process — torch can't be
    imported into the dashboard process once pandas/pyarrow are loaded
    (see that module's docstring)."""
    result = subprocess.run([sys.executable, "-m", "dashboard.model_ops", *args], cwd=BASE_DIR,
                            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300)
    if result.returncode != 0:
        raise RuntimeError((result.stderr.strip().splitlines() or ["model_ops failed"])[-1])
    return json.loads(result.stdout.strip().splitlines()[-1])


@st.cache_data(show_spinner=False)
def _checkpoint_info(path: str, mtime: float) -> dict:
    """Everything in a checkpoint except the weights. `mtime` is only part
    of the cache key, so a replaced file is re-read."""
    return _model_ops("info", path)


def _info(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return _checkpoint_info(str(path), path.stat().st_mtime)
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)[:300]}


def _deploy_best(test_metrics: dict) -> str:
    """Export the training project's best checkpoint into the format
    ml/inference.py loads, check that it loads and runs, back up the model in
    use, then swap the new file in (dashboard/model_ops.py deploy). Returns
    the backup path ("" if there was no model to back up)."""
    extra = {"test_metrics": test_metrics or None,
             "deployed_by": st.session_state.get("admin_user", {}).get("username")}
    result = _model_ops("deploy", str(BEST_CHECKPOINT), str(DEPLOYED_MODEL), str(BACKUP_DIR), json.dumps(extra))
    _after_model_swap()
    return result["backup"]


def _restore_backup(backup: Path) -> None:
    tmp = DEPLOYED_MODEL.with_suffix(".pt.tmp")
    shutil.copy2(backup, tmp)
    swap_in(tmp, DEPLOYED_MODEL, BACKUP_DIR)
    _after_model_swap()


def _after_model_swap() -> None:
    _checkpoint_info.clear()
    try:  # only this dashboard process; Spark caches its own copy
        from ml import inference
        inference._load_trained_model.cache_clear()
    except Exception:  # noqa: BLE001
        pass


def _describe_model(info: dict | None) -> list[str]:
    if info is None:
        return ["No model file."]
    if "error" in info:
        return [f"Could not read the file: {info['error']}"]
    lines = [f"Architecture: `{info.get('architecture', 'resnet18')}`",
             "Classes: " + ", ".join(info.get("class_names", []))]
    if info.get("val_accuracy") is not None:
        lines.append(f"Validation accuracy: **{info['val_accuracy'] * 100:.2f}%**"
                     + (f" (epoch {info['trained_epoch']})" if info.get("trained_epoch") else ""))
    if (tm := info.get("test_metrics")) and tm.get("accuracy") is not None:
        lines.append(f"Test accuracy: **{tm['accuracy']:.2f}%**, F1 {tm.get('f1', 0):.2f}%")
    if info.get("deployed_at"):
        lines.append(f"Deployed {info['deployed_at'].replace('T', ' ')}"
                     + (f" by {info['deployed_by']}" if info.get("deployed_by") else ""))
    return lines


# ------------------------------------------------------------------
# UI pieces
# ------------------------------------------------------------------
def _history_chart(epochs: list[dict], metric: str, title: str, suffix: str):
    df = pd.DataFrame(epochs)
    fig = go.Figure()
    for col, name, color in ((f"train_{metric}", "Training", COLOR_TRAIN), (f"val_{metric}", "Validation", COLOR_VAL)):
        if col in df:
            fig.add_scatter(x=df["epoch"], y=df[col], name=name, mode="lines+markers",
                            line=dict(color=color, width=2), marker=dict(size=6),
                            hovertemplate=f"Epoch %{{x}}<br>{name}: %{{y:.2f}}{suffix}<extra></extra>")
    best = df[df.get("best", False) == True] if "best" in df else df.iloc[0:0]  # noqa: E712
    if f"val_{metric}" in df and len(best):
        fig.add_scatter(x=best["epoch"], y=best[f"val_{metric}"], name="Best so far (saved)", mode="markers",
                        marker=dict(color=COLOR_VAL, size=11, symbol="star", line=dict(color="#5B3A29", width=1)),
                        hovertemplate="Epoch %{x}: new best<extra></extra>")
    fig.update_layout(title=title, height=300, margin=dict(l=10, r=10, t=40, b=10),
                      legend=dict(orientation="h", y=-0.25), xaxis_title="Epoch",
                      xaxis=dict(dtick=1 if len(df) <= 25 else None))
    st.plotly_chart(fig, width="stretch")


def _progress_section(run: dict, parsed: dict, running: bool):
    epochs = [e for e in parsed["epochs"] if "val_acc" in e]
    total = parsed["total_epochs"]
    current = parsed["epochs"][-1]["epoch"] if parsed["epochs"] else None
    started = datetime.fromisoformat(run["started_at"])
    mode = "continuing the last run" if run.get("mode") == "resume" else "from scratch"

    if running:
        st.info(f"**Training is running** ({mode}) — started {started:%d/%m/%Y %H:%M}"
                + (f" by {run['started_by']}" if run.get("started_by") else "")
                + (f" on {parsed['device']}" if parsed["device"] else "") + ".", icon="⏳")
        if current and total:
            done = len(epochs) and epochs[-1]["epoch"]
            st.progress(min(done / total, 1.0), text=f"Epoch {current} of {total} (stops early if "
                                                     "validation accuracy stops improving)")
        else:
            st.progress(0.0, text="Loading the dataset…")
    else:
        level, message = _run_outcome(run, parsed)
        {"ok": st.success, "warn": st.warning, "bad": st.error}[level](
            f"**Last run: {message}.** Started {started:%d/%m/%Y %H:%M} ({mode}).")
        if parsed["auto_deploy"]:
            st.info(f"**Auto-deploy:** {parsed['auto_deploy']}", icon="🚀")

    if epochs:
        best = max(epochs, key=lambda e: e["val_acc"])
        metric_row(("Epochs done", f"{epochs[-1]['epoch']}" + (f" / {total}" if total else "")),
                   ("Latest validation accuracy", f"{epochs[-1]['val_acc']:.2f}%"),
                   ("Best validation accuracy", f"{best['val_acc']:.2f}%", f"Epoch {best['epoch']}"),
                   ("Latest training accuracy", f"{epochs[-1].get('train_acc', 0):.2f}%"))
        left, right = st.columns(2)
        with left:
            _history_chart(epochs, "acc", "Accuracy", "%")
        with right:
            _history_chart(epochs, "loss", "Loss", "")
        if parsed["resumed_after"]:
            st.caption(f"Continued after epoch {parsed['resumed_after']}; only this run's epochs are shown.")

    if parsed["error"] and not running:
        st.code(parsed["error"], language=None)
    with st.expander("Training log"):
        text = _read_log(run)
        st.code("\n".join(text.splitlines()[-60:]) or "(empty)", language=None)
        st.caption(f"Full log: `{run['log']}`")


def _start_form():
    st.markdown("#### Start a training run")
    st.caption(f"Runs `train.py` in `{TRAINING_DIR}` on this laptop's GPU — about 10 minutes. "
               "Uses the images in that project's `data` folder. When it finishes, the best model "
               "is deployed **automatically if it scores better** than the model in use (test "
               "accuracy, else validation accuracy); the old one is backed up.")
    from dashboard.labelling_page import pending_photos
    if waiting := len(pending_photos()):
        st.warning(f"**{waiting} farmer photo(s) are not labelled yet** — label them on the "
                   "*Label photos* page first if you want this training to learn from them.", icon="🏷️")
    mode = st.radio(
        "Mode",
        ["scratch", "resume"],
        format_func=lambda m: {"scratch": "Train from scratch (after adding images or changing settings)",
                               "resume": "Continue the last run (only after it was interrupted)"}[m],
        key="train_mode",
    )
    if st.button("Start training", type="primary", icon="▶️"):
        error = _start_training(resume=mode == "resume")
        if error:
            st.error(error)
        else:
            st.rerun()


def _results_section():
    report = RESULTS_DIR / "classification_report.txt"
    if not report.exists():
        return
    st.markdown("#### Test results of the latest training")
    st.caption(f"From `{RESULTS_DIR}`, written "
               f"{datetime.fromtimestamp(report.stat().st_mtime):%d/%m/%Y %H:%M}. "
               "Measured on test images the model never trained on.")
    left, right = st.columns([1, 1])
    with left:
        st.code(report.read_text(encoding="utf-8", errors="replace"), language=None)
    with right:
        cm = RESULTS_DIR / "confusion_matrix.png"
        if cm.exists():
            st.image(str(cm), caption="Rows: true class · columns: predicted class")


def _deploy_section(running: bool, test_metrics: dict):
    st.markdown("#### Deploy")
    deployed = _info(DEPLOYED_MODEL)
    candidate = _info(BEST_CHECKPOINT)
    left, right = st.columns(2)
    with left, st.container(border=True):
        st.markdown("**Model in use now**")
        for line in _describe_model(deployed):
            st.caption(line)
        if deployed and "error" not in deployed and not deployed.get("val_accuracy"):
            st.caption("No accuracy recorded in this file (it predates this page).")
    with right, st.container(border=True):
        st.markdown("**Best model from training**")
        if candidate is None:
            st.caption("None yet — run a training first.")
        elif "error" in candidate:
            st.caption(f"Could not read it: {candidate['error']}")
        else:
            st.caption(f"Validation accuracy: **{candidate['val_accuracy'] * 100:.2f}%** "
                       f"(epoch {candidate.get('epoch')})")
            if test_metrics.get("accuracy") is not None:
                st.caption(f"Test accuracy: **{test_metrics['accuracy']:.2f}%**, "
                           f"F1 {test_metrics.get('f1', 0):.2f}%")
            st.caption("Classes: " + ", ".join(CLASS_NAME_MAP.get(c, c) for c in candidate["class_names"]))
            st.caption(f"Saved {datetime.fromtimestamp(BEST_CHECKPOINT.stat().st_mtime):%d/%m/%Y %H:%M}")

    ok_to_deploy = candidate is not None and "error" not in candidate and not running
    if running:
        st.caption("Deploying is disabled while training runs — the best model can still change.")
    confirm = st.checkbox("I checked the test results and want the system to use this model",
                          disabled=not ok_to_deploy, key="deploy_confirm")
    if st.button("Deploy best model", icon="🚀", type="primary", disabled=not (ok_to_deploy and confirm)):
        try:
            with st.spinner("Exporting, checking and installing the model…"):
                backup = _deploy_best(test_metrics)
            st.session_state["deploy_msg"] = ("ok", backup)
        except Exception as e:  # noqa: BLE001
            st.session_state["deploy_msg"] = ("error", str(e))
        st.rerun()

    if msg := st.session_state.pop("deploy_msg", None):
        if msg[0] == "ok":
            st.success("Model deployed to `ml/trained_model/" + DEPLOYED_MODEL.name + "`."
                       + (f" The previous one was backed up to `{msg[1]}`." if msg[1] else ""))
        else:
            st.error(f"Deploy failed — the model in use was not changed. {msg[1]}")
    if msg or st.session_state.get("_restored"):
        st.session_state.pop("_restored", None)
        st.info("Spark picks up the new model on its next prediction — no restart needed.",
                icon="🔁")

    backups = sorted(BACKUP_DIR.glob("*.pt"), reverse=True) if BACKUP_DIR.exists() else []
    if backups:
        with st.expander(f"Restore a previous model ({len(backups)} backup(s))"):
            choice = st.selectbox("Backup", backups, format_func=lambda p: p.name)
            for line in _describe_model(_info(choice)):
                st.caption(line)
            if st.button("Restore this model", icon="↩️", disabled=running):
                _restore_backup(choice)
                st.session_state["_restored"] = True
                st.rerun()


# ------------------------------------------------------------------
# Page
# ------------------------------------------------------------------
@st.fragment(run_every=5)
def _live_status():
    """Re-runs on its own every 5 s; triggers a full page rerun when a run
    starts or ends so the start form and deploy section follow."""
    run = _read_run()
    running = _process(run) is not None
    if st.session_state.get("_training_running") is not None and st.session_state["_training_running"] != running:
        st.session_state["_training_running"] = running
        st.rerun(scope="app")
    st.session_state["_training_running"] = running
    if run:
        _progress_section(run, _parse_log(_read_log(run)), running)


def render():
    run = _read_run()
    running = _process(run) is not None
    st.session_state["_training_running"] = running

    if not TRAINING_DIR.exists():
        st.error(f"Training project not found at `{TRAINING_DIR}`. Set TRAINING_PROJECT_DIR in `.env`.")
        return

    if running:
        _live_status()
        stop_ok = st.checkbox("Yes, stop this training run", key="stop_confirm")
        if st.button("Stop training", icon="⏹️", disabled=not stop_ok):
            _stop_training(run)
            st.rerun()
    else:
        if run:
            _progress_section(run, _parse_log(_read_log(run)), running=False)
        _start_form()

    st.divider()
    _results_section()
    st.divider()
    test_metrics = _parse_log(_read_log(run))["test"] if run and not running else {}
    _deploy_section(running, test_metrics)
