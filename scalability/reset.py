"""
Complete reset of the scalability test — the dashboard's "Delete test"
button and `python -m scalability.run reset`.

Removes everything that belongs ONLY to the scalability test and returns
the test to its initial state. Each resource is identified by a dedicated
name, not guessed:

  processes  running test processes: command line `-m scalability.run start|prepare`
             (their Spark JVM is a child process and stops with them)
  Kafka      the test's own topic SCALABILITY_KAFKA_TOPIC (its events are
             deleted, the empty topic is kept) and its own consumer groups
             (SCALABILITY_CONSUMER_GROUP*)
  database   scale_test_runs / scale_test_items; legacy rows from before the
             isolation (images.source = SCALE_TEST_SOURCE and their
             predictions / processing events / rejected events / reviews);
             the inactive SCALE_TEST_USERNAME system account
  files      SCALABILITY_REPORTS_DIR/scale-* (staged hard links, previews,
             CSV files, Spark checkpoints), logs/, the status and lock files;
             the legacy staging folder IMAGE_STORAGE_PATH/scalability; the
             local-queue file of the test topic

Never touched: the production Kafka topic and Spark job, farmer photos and
their local copies, Google Drive, the model, training/review data, and any
other folder. Hard links and photos read in place are only directory
entries or never inside the test folder, so originals are never deleted.
Every step is checked again afterwards, so the result says "reset" only
when nothing of the test is left. Running it again is harmless.
"""

import os
import shutil
from pathlib import Path

import psutil

from config import settings

TEST_COMMANDS = ("start", "prepare")
RUN_PREFIX = "scale-"
REPORT_FILES = ("_prepare_status.json", "_lock.json")
REPORT_DIRS = ("logs",)


class UnsafeReset(Exception):
    """A resource could not be proven to belong only to the test."""


def _protected_paths() -> list:
    base = Path(settings.BASE_DIR)
    return [base, base / "storage", base / "data", base / "ml", settings.MAIL_OUTBOX_DIR,
            settings.IMAGE_STORAGE_PATH, settings.DOWNLOADED_PHOTOS_DIR,
            settings.SPARK_CHECKPOINT_ROOT, settings.LOCAL_QUEUE_DIR, settings.REVIEW_DIR,
            settings.LABELLED_TRAIN_DIR, settings.TRAINING_PROJECT_DIR,
            Path(settings.IMAGE_MODEL["weights_path"]).parent]


def reports_dir() -> Path:
    """The test's own folder, refused if it is (or contains) a shared one."""
    root = Path(settings.SCALABILITY_REPORTS_DIR).resolve()
    if root == Path(root.anchor):
        raise UnsafeReset(f"the scalability reports folder is a drive root: {root}")
    for p in _protected_paths():
        p = Path(p).resolve()
        if root == p or p.is_relative_to(root):
            raise UnsafeReset(f"the scalability reports folder {root} is or contains the shared folder {p}")
    return root


def _remove(path: Path, root: Path, failures: list) -> None:
    """Deletes `path` (a directory entry inside `root`) without following
    links: a symlink/junction is removed as a link, a hard link only loses
    this entry."""
    location = path.parent.resolve() / path.name
    if not location.is_relative_to(root) or location == root:
        raise UnsafeReset(f"{location} is not inside the test folder {root}")
    try:
        if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
            os.unlink(path) if not path.is_dir() else os.rmdir(path)
        elif path.is_dir():
            shutil.rmtree(path, onexc=lambda fn, p, e: failures.append(f"{p}: {e}"))
        elif path.exists():
            path.unlink()
    except OSError as e:
        failures.append(f"{path}: {e}")


# --------------------------------------------------------------- steps
def _is_test_process(proc) -> bool:
    try:
        cmd = proc.cmdline()
    except (psutil.Error, OSError):
        return False
    if "scalability.run" not in cmd:
        return False
    rest = cmd[cmd.index("scalability.run") + 1:]
    return bool(rest) and rest[0] in TEST_COMMANDS


def test_processes() -> list:
    me = {os.getpid(), *(p.pid for p in psutil.Process().parents())}
    found = []
    for proc in psutil.process_iter():
        if proc.pid not in me and _is_test_process(proc):
            found.append(proc)
    return found


def stop_test_processes() -> str:
    """Stops running scalability-test processes and their children (the
    test's own Spark JVM). The live Spark job is a different command
    (spark.streaming_job) and is never matched."""
    procs = test_processes()
    targets = []
    for p in procs:
        targets.append(p)
        try:
            targets.extend(p.children(recursive=True))
        except psutil.Error:
            pass
    for p in targets:
        try:
            p.terminate()
        except psutil.Error:
            pass
    _, alive = psutil.wait_procs(targets, timeout=10)
    for p in alive:
        try:
            p.kill()
        except psutil.Error:
            pass
    psutil.wait_procs(alive, timeout=5)
    if test_processes():
        raise RuntimeError("a scalability test process is still running")
    return f"stopped {len(procs)} test process(es)" if procs else "no test was running"


def _test_groups() -> list:
    base = settings.SCALABILITY_CONSUMER_GROUP
    return [base, f"{base}-spark-offsets"]


def _kafka_admin():
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS, "socket.timeout.ms": 5000})


def _watermarks(topic: str, partitions) -> dict:
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                  "group.id": f"{settings.SCALABILITY_CONSUMER_GROUP}-reset", "enable.auto.commit": False})
    try:
        return {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in partitions}
    finally:
        c.close()


def clear_kafka(admin=None, watermarks=_watermarks) -> str:
    """Deletes the events of the test's own topic (the empty topic stays,
    so the Kafka monitor keeps working) and the test's consumer groups.
    Refuses anything that is not the test's own."""
    from confluent_kafka import TopicPartition
    topic = settings.SCALABILITY_KAFKA_TOPIC
    if topic == settings.KAFKA_TOPIC or not topic:
        raise UnsafeReset("the scalability test topic is the production topic")
    groups = _test_groups()
    from kafka.monitor import GROUP_ID as MONITOR_GROUP
    if any(g == MONITOR_GROUP or not g for g in groups):
        raise UnsafeReset("a scalability consumer group is shared")
    admin = admin or _kafka_admin()
    meta = admin.list_topics(timeout=5)
    deleted = 0
    if topic in meta.topics:
        marks = watermarks(topic, list(meta.topics[topic].partitions))
        cut = [TopicPartition(topic, p, high) for p, (low, high) in marks.items() if high > low]
        if cut:
            for future in admin.delete_records(cut).values():
                future.result(timeout=30)
            deleted = sum(high - low for low, high in marks.values())
    existing = {g.group_id for g in admin.list_consumer_groups().result(timeout=10).valid}
    gone = [g for g in groups if g in existing]
    if gone:
        for future in admin.delete_consumer_groups(gone).values():
            future.result(timeout=30)
    queue_file = Path(settings.LOCAL_QUEUE_DIR) / f"{topic}.jsonl"     # the test topic's fallback file
    if queue_file.exists():
        queue_file.unlink()
    return (f"{deleted} test event(s) deleted from '{topic}'"
            + (f", consumer group(s) {', '.join(gone)} removed" if gone else ""))


def clear_database() -> str:
    from storage.database import (SessionLocal, Image, Prediction, ProcessingEvent, RejectedEvent, ImageReview,
                                  User, UserStatus, ScaleTestRun, ScaleTestItem, SCALE_TEST_SOURCE,
                                  SCALE_TEST_USERNAME)
    with SessionLocal() as s:
        items = s.query(ScaleTestItem).delete(synchronize_session=False)
        runs = s.query(ScaleTestRun).delete(synchronize_session=False)
        legacy = [i for (i,) in s.query(Image.image_id).filter(Image.source == SCALE_TEST_SOURCE)]
        for start in range(0, len(legacy), 500):
            chunk = legacy[start:start + 500]
            for model in (Prediction, ProcessingEvent, RejectedEvent, ImageReview):
                s.query(model).filter(model.image_id.in_(chunk)).delete(synchronize_session=False)
            s.query(Image).filter(Image.image_id.in_(chunk)).delete(synchronize_session=False)
        s.commit()
    account = "no test system account"
    with SessionLocal() as s:
        user = s.query(User).filter(User.username == SCALE_TEST_USERNAME,
                                    User.status == UserStatus.INACTIVE).first()
        if user is not None:
            if s.query(Image).filter(Image.user_id == user.user_id).count():
                account = "test system account kept (still owns images)"
            else:
                try:
                    s.delete(user)
                    s.commit()
                    account = "test system account removed"
                except Exception:  # noqa: BLE001 - referenced elsewhere: keep it, it is inactive
                    s.rollback()
                    account = "test system account kept (still referenced)"
    return f"{runs} run(s), {items} measurement(s), {len(legacy)} legacy image row(s) deleted; {account}"


def clear_files() -> str:
    root = reports_dir()
    failures = []
    removed = 0
    if root.exists():
        for entry in root.iterdir():
            if (entry.name.startswith(RUN_PREFIX) or entry.name in REPORT_FILES
                    or entry.name in REPORT_DIRS):
                _remove(entry, root, failures)
                removed += 1
    legacy = Path(settings.IMAGE_STORAGE_PATH) / "scalability"
    if legacy.exists():
        _remove(legacy, Path(settings.IMAGE_STORAGE_PATH).resolve(), failures)
        removed += 1
    if failures:
        raise RuntimeError(f"{len(failures)} file(s) could not be deleted, e.g. {failures[0]}")
    return f"{removed} test folder(s)/file(s) removed"


def leftovers() -> list:
    """What of the test is still there (empty = fully reset)."""
    from storage.database import SessionLocal, Image, ScaleTestRun, ScaleTestItem, SCALE_TEST_SOURCE
    left = []
    with SessionLocal() as s:
        if s.query(ScaleTestRun).count() or s.query(ScaleTestItem).count():
            left.append("test records in the database")
        if s.query(Image).filter(Image.source == SCALE_TEST_SOURCE).count():
            left.append("legacy test image rows")
    root = reports_dir()
    if root.exists() and any(e.name.startswith(RUN_PREFIX) or e.name in REPORT_FILES or e.name in REPORT_DIRS
                             for e in root.iterdir()):
        left.append(f"test files in {root}")
    if (Path(settings.IMAGE_STORAGE_PATH) / "scalability").exists():
        left.append("the legacy test staging folder")
    if test_processes():
        left.append("a running test process")
    return left


STEPS = (("Stop test processes (and their Spark job)", stop_test_processes),
         ("Clear the test's Kafka topic", clear_kafka),
         ("Delete test records", clear_database),
         ("Delete test images and files", clear_files))


def reset_all(steps=STEPS, performed_by: str = None) -> dict:
    """Runs every step even if one fails, then checks what is left, and
    records the outcome in storage_audit_log like the other destructive
    storage actions. {"ok": bool, "steps": [{"step", "ok", "detail"}], "left": [...]}"""
    try:
        reports_dir()
    except UnsafeReset as e:
        result = {"ok": False, "steps": [{"step": "Safety check", "ok": False, "detail": str(e)}], "left": []}
        _audit(result, performed_by)
        return result
    out = []
    for name, step in steps:
        try:
            out.append({"step": name, "ok": True, "detail": step()})
        except Exception as e:  # noqa: BLE001 - report it and go on with the others
            out.append({"step": name, "ok": False, "detail": f"{type(e).__name__}: {e}"})
    try:
        left = leftovers()
    except Exception as e:  # noqa: BLE001
        left = [f"could not check: {e}"]
    result = {"ok": all(s["ok"] for s in out) and not left, "steps": out, "left": left}
    _audit(result, performed_by)
    return result


def _audit(result: dict, performed_by) -> None:
    try:
        from storage.image_cache import audit
        audit("scalability_reset", "done" if result["ok"] else "failed", category="scalability_test",
              target=str(settings.SCALABILITY_REPORTS_DIR), reason="Delete test (complete scalability test reset)",
              detail="; ".join(f"{s['step']}: {'ok' if s['ok'] else 'FAILED'} — {s['detail']}" for s in result["steps"])
              + (f"; still left: {', '.join(result['left'])}" if result["left"] else ""),
              performed_by=performed_by)
    except Exception as e:  # noqa: BLE001 - the reset itself is reported to the caller either way
        print(f"[scalability] could not write the audit record: {e}", flush=True)
