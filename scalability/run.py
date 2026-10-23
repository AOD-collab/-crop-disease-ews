r"""
Scalability test runner (Demo 2). Started by the dashboard's "Scalability
test" page as a background process, or by hand:

    python -m scalability.run preview
    python -m scalability.run prepare --size 100
    python -m scalability.run start --size 100
    python -m scalability.run start --size 100 --iterations 3
    python -m scalability.run start --size 5000 --source-dir D:\photos
    python -m scalability.run preview --source-dir D:\photos
    python -m scalability.run start --replay-of <run_id>
    python -m scalability.run report --run-id <run_id>
    python -m scalability.run cleanup --run-id <run_id>
    python -m scalability.run delete --run-id <run_id>
    python -m scalability.run list
    python -m scalability.run reset      # delete EVERY test (the dashboard's Delete test)

The test is ISOLATED from the live pipeline (scalability/worker.py): it has
its own Kafka topic, its own Spark query (in this process, separate from the
live Spark job), and it
writes only to scale_test_runs / scale_test_items and to
SCALABILITY_REPORTS_DIR/<run_id>/. No Image, Prediction or Drive result is
ever created for a test photo, so nothing of a test can reach a farmer, the
dashboard's farmer pages, Tableau or the labelling folder.

What `start` does:
  1. Lists the Drive collection (scalability/dataset.py), takes the oldest
     `size` valid photos (or the photos of the run being replayed). With
     --source-dir it scans that folder instead and takes the first `size`
     photos by path.
  2. Verifies or downloads each photo's local copy (Drive only).
  3. Stages every ready photo as a hard link in
     SCALABILITY_REPORTS_DIR/<run_id>/images/<item_id>.<ext> plus one
     scale_test_items row (a fresh item id, never a farmer's image_id).
  4. Waits until no farmer photo is being processed (real time first),
     starts the test's own Spark Structured Streaming query on the TEST topic
     (scalability/spark_engine.py; JVM + Kafka start-up before the timer),
     then starts the clock: publishes one event per photo to the TEST topic;
     Spark reads them in micro-batches and processes each photo with the
     live pipeline's own preprocessing + CNN code, SCALABILITY_WORKERS at a
     time. New photos wait while farmer photos are being processed.
     `--engine python` (SCALABILITY_ENGINE) uses a plain Kafka consumer +
     thread pool in this process instead of Spark.
  5. Stops the clock when every photo is COMPLETED or FAILED and writes the
     summary + CSV files.
"""

import argparse
import json
import os
import queue as stdlib_queue
import shutil
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psutil  # noqa: E402

from config import settings  # noqa: E402
from config.settings import IMAGE_STORAGE_PATH  # noqa: E402
from storage import drive_client  # noqa: E402
from storage.database import (  # noqa: E402
    SessionLocal, init_db, Image, Prediction, ProcessingEvent, RejectedEvent,
    ScaleTestRun, ScaleTestItem, SCALE_TEST_SOURCE, now_utc,
)
from scalability import dataset, report, worker  # noqa: E402
from scalability.monitor import ResourceMonitor, gpu_name, spark_job_running  # noqa: E402

REPORTS_DIR = report.REPORTS_DIR
LOCK_FILE = REPORTS_DIR / "_lock.json"
PREPARE_STATUS_FILE = REPORTS_DIR / "_prepare_status.json"
# Where runs from before the isolation (2026-10-10) staged their photos;
# only clean-up still looks here.
LEGACY_STAGING_ROOT = IMAGE_STORAGE_PATH / "scalability"
SCALE_TEST_EMAIL = "scalability-test@system.local"
ACTIVE_STATUSES = ("PREPARING", "RUNNING")
CHUNK = 500


class RunError(Exception):
    pass


def log(msg: str) -> None:
    print(f"[scalability {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# Live console output while a test runs (`start --live`), so the test can be
# watched in a command prompt like the Spark and Kafka windows:
#   photos   (default) Kafka / gate / progress lines + one line per finished photo
#   progress Kafka / gate lines + a progress line every 2 s (for big runs)
#   off      only the start / end lines, as before
# Lines are collected and written in ONE console write at most every 0.5 s,
# from the loop that already does the bookkeeping — never from the photo
# workers — so watching does not slow the measured processing.
LIVE_MODES = ("photos", "progress", "off")
_live_mode = "photos"
PROGRESS_EVERY_S = 2.0


ENGINES = ("spark", "python")
_engine_override = None


def set_engine(name: str) -> None:
    global _engine_override
    if name not in ENGINES:
        raise RunError(f"--engine must be one of {', '.join(ENGINES)}")
    _engine_override = name


def engine() -> str:
    """"spark" (the test's own Spark Structured Streaming query) or
    "python" (Kafka consumer + thread pool), see SCALABILITY_ENGINE."""
    name = _engine_override or settings.SCALABILITY_ENGINE
    if name not in ENGINES:
        raise RunError(f"SCALABILITY_ENGINE must be one of {', '.join(ENGINES)}, not {name!r}")
    return name


def set_live(mode: str) -> None:
    global _live_mode
    if mode not in LIVE_MODES:
        raise RunError(f"--live must be one of {', '.join(LIVE_MODES)}")
    _live_mode = mode


def _emit(lines: list) -> None:
    """Writes the collected live lines at once (one console write)."""
    if lines and _live_mode != "off":
        stamp = datetime.now().strftime("%H:%M:%S")
        sys.stdout.write("".join(f"[scalability {stamp}] {line}\n" for line in lines))
        sys.stdout.flush()


def _photo_line(n: int, total: int, item_id: str, outcome) -> str:
    status, error, timings, prediction = outcome
    timings, prediction = timings or {}, prediction or {}
    head = f"[cnn]      {n:>{len(str(total))}}/{total}  {item_id[:8]}"
    if status != "COMPLETED":
        return f"{head}  FAILED  {(error or '')[:120]}"
    conf = prediction.get("prediction_confidence")
    result = (f"{prediction.get('predicted_class')}"
              + (f" {conf * 100:.2f}%" if isinstance(conf, (int, float)) else "")
              + f"  risk {prediction.get('risk_level')}")
    flag = "  (overlapped farmer processing — excluded)" if timings.get("contended") else ""
    return (f"{head}  preprocess {timings.get('preprocess_s', 0):.2f}s  cnn {timings.get('cnn_s', 0):.2f}s"
            f"  -> {result}{flag}")


def run_dir(run_id: str) -> Path:
    """Everything one run keeps on disk: images/ (its staged photos),
    the before/after pair, and the CSV files."""
    return REPORTS_DIR / run_id


# --------------------------------------------------------------- one job at a time
def _pid_alive(pid) -> bool:
    try:
        return bool(pid) and psutil.pid_exists(int(pid))
    except (ValueError, TypeError):
        return False


def active_job():
    """{"pid", "kind", "run_id", "started_at"} of the job holding the lock,
    or None. A lock left behind by a dead process is ignored."""
    try:
        info = json.loads(LOCK_FILE.read_text())
    except (OSError, ValueError):
        return None
    return info if _pid_alive(info.get("pid")) else None


def _acquire_lock(kind: str, run_id: str = None) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    holder = active_job()
    if holder:
        raise RunError(f"another scalability job is running ({holder['kind']}, pid {holder['pid']})")
    LOCK_FILE.unlink(missing_ok=True)     # stale
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RunError("another scalability job just started")
    with os.fdopen(fd, "w") as f:
        json.dump({"pid": os.getpid(), "kind": kind, "run_id": run_id,
                   "started_at": now_utc().isoformat()}, f)


def _release_lock() -> None:
    try:
        if json.loads(LOCK_FILE.read_text()).get("pid") == os.getpid():
            LOCK_FILE.unlink()
    except (OSError, ValueError):
        pass


def reap_dead_runs() -> int:
    """Runs still PREPARING/RUNNING whose runner process is gone -> ABORTED."""
    n = 0
    with SessionLocal() as s:
        for run in s.query(ScaleTestRun).filter(ScaleTestRun.status.in_(ACTIVE_STATUSES)).all():
            if not _pid_alive(run.pid):
                run.status, run.finished_at = "ABORTED", now_utc()
                run.error_message = "the runner process ended before the run finished"
                n += 1
        s.commit()
    return n


# --------------------------------------------------------------- helpers
def _source(source_dir=None, from_drive=False) -> str:
    """"folder" (a custom folder), "google_drive" (only when asked for
    explicitly) or "local" (the default: the farmer photos already on this
    laptop, no Google Drive)."""
    return "folder" if source_dir else "google_drive" if from_drive else dataset.LOCAL_SOURCE


def _source_label(source_dir, from_drive=False) -> str:
    return {"folder": f"custom folder {source_dir}", "google_drive": "Google Drive",
            "local": f"local photos in {dataset.local_photos_dir()}"}[_source(source_dir, from_drive)]


def _collection(source_dir=None, from_drive=False):
    """The run's photo source (see _source). Only from_drive=True contacts
    Google Drive."""
    if source_dir:
        try:
            return dataset.enumerate_folder(source_dir, staging_root=REPORTS_DIR)
        except FileNotFoundError as e:
            raise RunError(str(e))
    if from_drive:
        return dataset.enumerate_collection()
    return dataset.enumerate_local_photos(staging_root=REPORTS_DIR)


def _benchmark_config(source_dir=None, series=None, from_drive=False) -> dict:
    return {
        **(series or {}),
        "KAFKA_TOPIC": settings.SCALABILITY_KAFKA_TOPIC,
        "KAFKA_CONSUMER_GROUP": settings.SCALABILITY_CONSUMER_GROUP,
        "ENGINE": engine(),
        **({"SPARK_MAX_EVENTS_PER_BATCH": settings.SPARK_MAX_EVENTS_PER_BATCH,
            "SPARK_TRIGGER_SECONDS": settings.SPARK_TRIGGER_SECONDS} if engine() == "spark" else {}),
        "WORKERS": settings.SCALABILITY_WORKERS,
        "ANALYSIS_MAX_PX": settings.ANALYSIS_MAX_PX,
        "MODEL_DEVICE": settings.IMAGE_MODEL.get("device"),
        "MODEL_PATH": Path(settings.IMAGE_MODEL["weights_path"]).name,
        "GPU": gpu_name(),
        "CPU_LOGICAL_CORES": psutil.cpu_count(),
        "RAM_GB": round(psutil.virtual_memory().total / 2**30, 1),
        "LIVE_OUTPUT": _live_mode,
        "SOURCE": _source(source_dir, from_drive),
        "IMAGE_SOURCE": {"folder": f"photos in the custom folder {source_dir}",
                         "google_drive": "verified local copies of the Google Drive originals",
                         "local": f"farmer photos stored on this laptop ({dataset.local_photos_dir()}), "
                                  "no Google Drive"}[_source(source_dir, from_drive)],
        **({"SOURCE_DIR": str(source_dir)} if source_dir else {}),
    }


def _test_queue(consume: bool = True):
    """The test's own Kafka topic + consumer group (tests replace this).
    consume=False: producer only, the Spark engine reads the topic."""
    try:
        return worker.KafkaTestQueue(consume=consume)
    except Exception as e:  # noqa: BLE001
        raise RunError(f"Kafka is not reachable at {settings.KAFKA_BOOTSTRAP_SERVERS} "
                       f"(test topic {settings.SCALABILITY_KAFKA_TOPIC}): {e}")


def _test_stream(run_id: str, ids: list, handle, on_done):
    """The test's own Spark Structured Streaming query on the test topic,
    started and warmed up (tests replace this)."""
    from scalability import spark_engine
    checkpoint = run_dir(run_id) / "spark_checkpoint"
    shutil.rmtree(checkpoint, ignore_errors=True)
    try:
        return spark_engine.SparkTestStream(run_id, ids, handle, on_done, checkpoint).start()
    except Exception as e:  # noqa: BLE001
        raise RunError(f"the scalability test's Spark query could not start: {e}")


def _live_gate():
    """Real-time priority (tests replace this)."""
    return worker.LiveGate()


def _stage(src: Path, dst: Path) -> Path:
    """The path the run reads one photo from: a hard link `dst` in the
    run's own folder (same file, no extra disk space), or, when that is not
    possible (the photo is on another drive), the original `src` itself,
    read in place and never written. Photos are never copied: a 7,000-photo
    folder on C: copied twice filled the A: drive (2026-10-11)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(src, dst)
        return dst
    except OSError:
        return src


def _chunks(seq, n=CHUNK):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _set_phase(run_id: str, phase: str, **fields) -> None:
    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        run.phase = phase
        for k, v in fields.items():
            setattr(run, k, v)
        s.commit()


# --------------------------------------------------------------- preview / prepare
def preview(source_dir=None, from_drive=False) -> dict:
    col = _collection(source_dir, from_drive)
    if not from_drive:
        n = len(col.candidates)
        return {**col.as_dict(), "with_local_copy": n, "missing_local_copy": 0}
    return {**col.as_dict(), **dataset.local_copy_counts(col.candidates)}


def _write_prepare_status(**fields) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = PREPARE_STATUS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"pid": os.getpid(), **fields}, default=str))
    tmp.replace(PREPARE_STATUS_FILE)


def prepare(size: int, workers: int = 8) -> dict:
    """Step 2 on its own: verify/download local copies of the first `size`
    valid photos, without starting a run."""
    _acquire_lock("prepare")
    try:
        _write_prepare_status(phase="Listing the Drive collection", done=0, total=0)
        col = dataset.enumerate_collection()
        chosen = col.candidates[:size]
        last = [0.0]

        def progress(done, total):
            if time.monotonic() - last[0] > 1 or done == total:
                last[0] = time.monotonic()
                _write_prepare_status(phase="Preparing local copies", done=done, total=total)

        prep = dataset.prepare_local_copies(chosen, workers=workers, progress=progress)
        result = {**col.as_dict(), "selected": len(chosen), **prep}
        _write_prepare_status(phase="Finished", done=len(chosen), total=len(chosen), finished=True, result=result)
        log(f"prepare: {json.dumps(result)}")
        return result
    except Exception as e:
        _write_prepare_status(phase="Failed", finished=True, error=str(e))
        raise
    finally:
        _release_lock()


# --------------------------------------------------------------- the run
def start(size: int = None, replay_of: str = None, workers: int = 8, stall_seconds: float = 600,
          run_id: str = None, source_dir: str = None, series: dict = None, _locked: bool = False,
          from_drive: bool = False) -> str:
    """Runs one complete test iteration and returns its run_id (see module
    docstring). `source_dir`: take the photos from this folder instead of
    Google Drive. `series`: {"SERIES_ID", "ITERATION", "ITERATIONS"} when
    this run is one iteration of start_series(), stored in config_json."""
    # Total execution time (end_to_end_seconds) starts when Start test was
    # pressed: the dashboard starts this process at that moment, so a single
    # test or iteration 1 counts from the process start; a later iteration
    # from here (the previous iteration's gallery previews are display only).
    first = not series or series.get("ITERATION", 1) == 1
    started_wall = psutil.Process().create_time() if first else time.time()
    init_db()
    reap_dead_runs()
    if replay_of:
        with SessionLocal() as s:
            old = s.get(ScaleTestRun, replay_of)
            if old is None:
                raise RunError(f"unknown run {replay_of}")
            size = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == replay_of).count()
            # a replay uses the same source as the run it repeats — except that
            # runs from before the local source (no "SOURCE") were Drive runs:
            # they are replayed from the same photos' local copies.
            old_config = json.loads(old.config_json or "{}")
            source_dir = old_config.get("SOURCE_DIR")
            from_drive = old_config.get("SOURCE") == "google_drive"
    if not size or size < 1:
        raise RunError("size must be at least 1")
    if source_dir:
        source_dir = str(Path(source_dir).expanduser().resolve())
        if not Path(source_dir).is_dir():
            raise RunError(f"folder not found: {source_dir}")
    run_id = run_id or f"scale-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"
    if not _locked:
        _acquire_lock("run", run_id)
    monitor = gate = queue = stream = None
    use_spark = engine() == "spark"
    try:
        worker.lower_priority()
        with SessionLocal() as s:
            s.add(ScaleTestRun(run_id=run_id, status="PREPARING",
                               phase={"folder": "Scanning the folder", "google_drive": "Listing the Drive collection",
                                      "local": "Scanning the local photos"}[_source(source_dir, from_drive)],
                               pid=os.getpid(), requested_size=size, replay_of=replay_of, publish_results=False,
                               config_json=json.dumps(_benchmark_config(source_dir, series, from_drive))))
            s.commit()
        log(f"{run_id}: started (size {size}, source {_source_label(source_dir, from_drive)}"
            + (f", replay of {replay_of}" if replay_of else "") + ")")

        # 1. dataset ---------------------------------------------------------
        col = _collection(source_dir, from_drive)
        selection = {**col.as_dict(), "source": _source(source_dir, from_drive), "source_dir": source_dir}
        if replay_of:
            with SessionLocal() as s:
                wanted = s.query(ScaleTestItem.source_drive_file_id, ScaleTestItem.source_image_id).filter(
                    ScaleTestItem.run_id == replay_of).all()
            # same file (Drive id / content hash), else the same farmer photo
            # (image_id): an old Drive run replayed from the local copies
            by_file = {c.drive_file_id: c for c in col.candidates}
            by_image = {c.image_id: c for c in col.candidates}
            chosen = [c for c in (by_file.get(f) or by_image.get(i) for f, i in wanted) if c is not None]
            selection["replay_missing_from_drive"] = len(wanted) - len(chosen)
        else:
            chosen = col.candidates[:size]
        selection.update(requested=size, selected=len(chosen))
        if not chosen:
            raise RunError({"folder": f"no valid photos in the folder {source_dir}",
                            "google_drive": "no valid photos in the Google Drive collection",
                            "local": f"no valid photos in {dataset.local_photos_dir()}"}[
                               _source(source_dir, from_drive)])

        # 2. local copies ----------------------------------------------------
        last = [0.0]

        def progress(done, total):
            if time.monotonic() - last[0] > 2 or done == total:
                last[0] = time.monotonic()
                _set_phase(run_id, f"Preparing local copies: {done} of {total}")
                _emit([f"[prepare]  local copies {done} of {total} ready (before the timer)"])

        prep = dataset.prepare_local_copies(chosen, workers=workers, progress=progress)
        selection.update(prep)
        ready = [c for c in chosen if c.local_path]
        if not ready:
            raise RunError("no photo has a valid local copy")

        # 3. staging: the run's own folder, its own item ids ----------------
        _set_phase(run_id, "Staging test images", selection_json=json.dumps(selection))
        images_dir = run_dir(run_id) / "images"
        paths = {}
        staged = []
        for c in ready:
            item_id = str(uuid.uuid4())
            ext = ".png" if c.mime_type == "image/png" else ".jpg"     # e.g. IMG_1.JPEG -> .jpg
            paths[item_id] = _stage(Path(c.local_path), images_dir / f"{item_id}{ext}")
            staged.append((item_id, c))
        in_place = sum(not path.is_relative_to(images_dir) for path in paths.values())
        if in_place:
            _emit([f"[prepare]  {in_place} photo(s) are on another drive: read in place (read-only), not copied"])
        # where each photo was taken (EXIF GPS is read from the file) — before the timer
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            places = list(pool.map(lambda ic: worker.photo_location(paths[ic[0]], ic[1].location,
                                                                     ic[1].device_gps), staged))
        with SessionLocal() as s:
            for chunk in _chunks(list(zip(staged, places))):
                for (item_id, c), place in chunk:
                    s.add(ScaleTestItem(
                        image_id=item_id, run_id=run_id, source_image_id=c.image_id,
                        source_drive_file_id=c.drive_file_id, source_user_id=c.user_id,
                        local_copy_status=c.local_copy_status, download_seconds=c.download_seconds,
                        size_bytes=c.size, status="STAGED", **place,
                    ))
                s.commit()
        ids = list(paths)

        # 4. wait for real time, then the measured iteration ----------------
        gate = _live_gate().start()
        farmer_wait_s = 0.0             # real-time priority: not test time
        if gate.paused:
            _set_phase(run_id, "Waiting: farmer photos are being processed")
            log(f"{run_id}: waiting until farmer photos are processed")
            waited = time.monotonic()
            gate.wait()
            farmer_wait_s = time.monotonic() - waited
        _set_phase(run_id, "Loading the CNN")
        _emit([f"[cnn]      loading and warming up the model, {len(ids)} photo(s) staged"])
        worker.warm_up()                        # not in the per-photo times (it is in the total)

        def handle(item_id, received_at):
            gate.wait()                         # farmer photos first
            try:
                timings, prediction = worker.process_photo(paths[item_id])
            except Exception as e:  # noqa: BLE001 - this photo only
                return item_id, ("FAILED", f"{type(e).__name__}: {e}"[:2000], {"received_at": received_at}, None)
            t_start, t_end = timings.pop("_t0"), timings.pop("_t1")
            timings["contended"] = gate.overlapped(t_start, t_end)
            return item_id, ("COMPLETED", None, {"received_at": received_at, **timings}, prediction)

        # Finished photos arrive on this queue (a done-callback per photo), so
        # the loop only ever touches photos that just finished. It used to
        # rescan every submitted photo twice every 50 ms and write the phase
        # to the database each time — at 10,000 photos that main-thread work
        # competed with the workers for the GIL inside the measured time.
        finished = stdlib_queue.Queue()
        queue = _test_queue(consume=False) if use_spark else _test_queue()
        if use_spark:
            _set_phase(run_id, "Starting Spark")
            _emit([f"[spark]    starting the test's own Spark Structured Streaming query on topic "
                   f"'{settings.SCALABILITY_KAFKA_TOPIC}' (the live Spark job is not touched)"])
            stream = _test_stream(run_id, ids, handle, finished.put)
            stream.warm_up(queue.send_many)     # Spark's first job on real data is not measured
            _emit([f"[spark]    ready — micro-batches of up to {settings.SPARK_MAX_EVENTS_PER_BATCH} event(s) "
                   f"every {settings.SPARK_TRIGGER_SECONDS:g}s, {settings.SCALABILITY_WORKERS} photo(s) at a time"])
        _set_phase(run_id, "Processing", status="RUNNING")
        # The live Spark job shares the GPU; recorded so a slow iteration can
        # be explained (a 7 s stall on 2026-10-10 happened with it running).
        spark_running = spark_job_running()
        monitor = ResourceMonitor().start()
        t0 = time.monotonic()
        published_at = now_utc()
        with SessionLocal() as s:
            for chunk in _chunks(ids):
                s.query(ScaleTestItem).filter(ScaleTestItem.image_id.in_(chunk)).update(
                    {ScaleTestItem.status: "PUBLISHED", ScaleTestItem.published_at: published_at},
                    synchronize_session=False)
            s.commit()
        events = [{"run_id": run_id, "item_id": i} for i in ids]
        _emit([f"[timer]    started — publishing {len(events)} event(s) to Kafka topic "
               f"'{settings.SCALABILITY_KAFKA_TOPIC}' (the live topic '{settings.KAFKA_TOPIC}' is not touched)"])
        failures = queue.send_many(events)
        done = set()
        if failures:
            _record(run_id, {events[i]["item_id"]: ("FAILED", f"Kafka publish failed: {err}", None, None)
                             for i, err in failures.items()})
            done.update(events[i]["item_id"] for i in failures)
        log(f"{run_id}: published {len(events) - len(failures)} event(s) to {settings.SCALABILITY_KAFKA_TOPIC}")

        submitted = set()
        pending = {}                    # finished, not yet written to the database
        last_change = last_flush = last_phase = last_progress = last_emit = time.monotonic()
        live = []                       # console lines collected until the next write
        received = 0                    # events consumed since the last write
        was_paused = False
        with ThreadPoolExecutor(max_workers=max(1, settings.SCALABILITY_WORKERS)) as pool:
            while len(done) < len(ids):
                if stream is not None:
                    # Spark consumes and processes on its own threads; the
                    # loop only books what it received and finished.
                    stream.raise_if_failed()
                    new = stream.take_received()
                    submitted.update(new)
                    received += len(new)
                    wait_s = 0.05
                elif len(submitted) + len(failures) < len(ids):
                    for event in queue.poll(0.05):
                        item_id = event.get("item_id")
                        if event.get("run_id") != run_id or item_id not in paths or item_id in submitted:
                            continue            # another run's leftover, or a duplicate delivery
                        submitted.add(item_id)
                        received += 1
                        pool.submit(handle, item_id, now_utc()).add_done_callback(
                            lambda f: finished.put(f.result()))
                    wait_s = 0.0
                else:
                    wait_s = 0.25               # everything is in the pool: just wait for results
                try:
                    item_id, outcome = finished.get(timeout=wait_s) if wait_s else finished.get_nowait()
                    pending[item_id] = outcome
                    while True:
                        item_id, outcome = finished.get_nowait()
                        pending[item_id] = outcome
                except stdlib_queue.Empty:
                    pass
                now = time.monotonic()
                if pending:
                    last_change = now
                if pending and (now - last_flush >= 0.5 or len(done) + len(pending) >= len(ids)):
                    _record(run_id, pending)
                    if _live_mode != "off":
                        if received:
                            live.append(f"[spark]    micro-batch read {received} event(s) from Kafka "
                                        f"({len(submitted)} of {len(ids)} received)" if stream is not None else
                                        f"[kafka]    consumed {received} event(s) "
                                        f"({len(submitted)} of {len(ids)} received)")
                            received = 0
                        if _live_mode == "photos":
                            for n, (item_id, outcome) in enumerate(pending.items(), len(done) + 1):
                                live.append(_photo_line(n, len(ids), item_id, outcome))
                    done.update(pending)
                    pending = {}
                    last_flush = now
                if _live_mode != "off":
                    if gate.paused != was_paused:
                        was_paused = gate.paused
                        live.append("[gate]     PAUSED — a farmer photo is being processed by the live "
                                    "pipeline; it goes first" if was_paused else "[gate]     resumed")
                    if now - last_progress >= PROGRESS_EVERY_S:
                        last_progress = now
                        elapsed = now - t0
                        live.append(f"[progress] {len(done)}/{len(ids)} done  "
                                    f"{len(done) / elapsed if elapsed > 0 else 0:.2f} photos/s  "
                                    f"in workers {len(submitted) - len(done) - len(pending)}  "
                                    f"waiting in Kafka {len(ids) - len(submitted) - len(failures)}  "
                                    f"elapsed {elapsed:.2f}s")
                    if live and (now - last_emit >= 0.5 or len(done) >= len(ids)):
                        _emit(live)
                        live = []
                        last_emit = now
                if now - last_phase >= 1.0:
                    last_phase = now
                    if gate.paused:
                        _set_phase(run_id, "Paused: farmer photos are being processed")
                    else:
                        _set_phase(run_id, f"Processing: {len(done)} of {len(ids)} done")
                if gate.paused:
                    last_change = now           # waiting for farmers is not a stall
                elif not pending and now - last_change > stall_seconds:
                    log(f"{run_id}: no progress for {stall_seconds:.2f}s, stopping the wait")
                    break
        t1 = time.monotonic()
        end_to_end = time.time() - started_wall - farmer_wait_s
        _emit(live)
        if stream is not None:
            stream.stop()
            stream = None
        samples = monitor.stop()
        monitor = None
        gate.stop()
        if pending:                     # finished while the loop was stopping on a stall
            _record(run_id, pending)
            done.update(pending)

        # 5. pictures, after the timer: the before/after pair + the gallery --
        comparison = _save_pictures(run_id, ids, paths, series)

        # 6. summary -----------------------------------------------------------
        finalize(run_id, samples=samples, execution_seconds=t1 - t0, end_to_end_seconds=end_to_end,
                 live_overlap=gate.overlapped(t0, t1), paused_seconds=gate.paused_seconds(t0, t1),
                 comparison=comparison, published=len(events) - len(failures), publish_failed=len(failures),
                 status="COMPLETED" if len(done) >= len(ids) else "INCOMPLETE",
                 spark_job_running=spark_running or spark_job_running())
        return run_id
    except Exception as e:
        log(f"{run_id}: FAILED: {e}")
        with SessionLocal() as s:
            run = s.get(ScaleTestRun, run_id)
            if run is not None:
                run.status, run.error_message, run.finished_at = "FAILED", str(e)[:2000], now_utc()
                run.phase = "Failed"
                s.commit()
        raise
    finally:
        if monitor is not None:
            monitor.stop()
        if gate is not None:
            gate.stop()
        if stream is not None:
            stream.stop()
        if queue is not None:
            queue.close()
        shutil.rmtree(run_dir(run_id) / "spark_checkpoint", ignore_errors=True)
        if not _locked:
            _release_lock()


def _record(run_id: str, outcomes: dict) -> None:
    """Stores finished photos' measurements and CNN results (scale_test_items
    only) in ONE bulk UPDATE, instead of a SELECT + UPDATE per photo.
    `outcomes`: {item_id: (status, error, timings, prediction)}, item_ids of
    this run (the caller only ever passes ids it staged for `run_id`)."""
    completed_at = now_utc()
    rows = []
    for item_id, (status, error, timings, prediction) in outcomes.items():
        timings = dict(timings or {})
        received = timings.pop("received_at", None)
        rows.append({
            "image_id": item_id, "status": status, "error_message": error,
            "spark_received_at": received,          # = when the test worker received the event
            "completed_at": completed_at,
            "pipeline_seconds": timings.get("processing_s"),
            "timings_json": json.dumps(timings) if timings else None,
            **(prediction or {}),
        })
    with SessionLocal() as s:
        s.bulk_update_mappings(ScaleTestItem, rows)
        s.commit()


def _save_pictures(run_id: str, ids: list, paths: dict, series: dict = None) -> dict:
    """Everything a run shows as pictures, made AFTER its timer stopped so
    none of it is in the measurement: the before/after pair (first photo
    that completed) and, for the gallery, one small before/after preview per
    photo (scalability/worker.save_previews). Later iterations of a series
    repeat iteration 1's photos, so only iteration 1 saves previews.
    Returns the comparison pair, or {}."""
    out = run_dir(run_id)
    with SessionLocal() as s:
        completed = {i for (i,) in s.query(ScaleTestItem.image_id).filter(
            ScaleTestItem.run_id == run_id, ScaleTestItem.status == "COMPLETED")}
    comparison = {}
    first_ok = next((i for i in ids if i in completed), None)
    if first_ok:
        try:
            from spark.preprocessing import prepare_image
            comparison = worker.save_comparison(paths[first_ok], prepare_image(paths[first_ok].read_bytes()), out)
        except Exception as e:  # noqa: BLE001
            log(f"{run_id}: could not save the before/after pair: {e}")
    if series and series.get("ITERATION", 1) != 1:
        return comparison
    _set_phase(run_id, "Saving preview images")
    failed = []

    def one(item_id):
        try:
            worker.save_previews(paths[item_id], out, item_id)
        except Exception as e:  # noqa: BLE001 - e.g. a photo the CNN could not read either
            failed.append(f"{item_id}: {e}")

    with ThreadPoolExecutor(max_workers=max(1, settings.SCALABILITY_WORKERS)) as pool:
        list(pool.map(one, ids))
    if failed:
        log(f"{run_id}: {len(failed)} preview(s) could not be made, e.g. {failed[0]}")
    return comparison


def start_series(size: int, iterations: int = 1, **kwargs) -> list:
    """Runs the same test `iterations` times and returns the run_ids.
    Iteration 1 selects `size` unique photos; every later iteration is a
    replay of it: the same photos, each staged again under a NEW item id in
    its own run folder, so iterations never share images or measurements.
    The workload is therefore size x iterations photo processings of `size`
    unique photos. Stops at the first iteration that fails."""
    if iterations < 1:
        raise RunError("iterations must be at least 1")
    series_id = f"series-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"
    _acquire_lock("run", series_id)
    try:
        run_ids = []
        for n in range(1, iterations + 1):
            series = {"SERIES_ID": series_id, "ITERATION": n, "ITERATIONS": iterations}
            log(f"{series_id}: iteration {n} of {iterations}")
            if n == 1:
                run_ids.append(start(size, series=series, _locked=True, **kwargs))
            else:
                kwargs.pop("source_dir", None)        # a replay uses iteration 1's own source
                kwargs.pop("from_drive", None)
                run_ids.append(start(replay_of=run_ids[0], series=series, _locked=True, **kwargs))
        return run_ids
    finally:
        _release_lock()


def _load_items(session, run_id):
    return session.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()


def finalize(run_id: str, *, samples: list, execution_seconds: float, live_overlap: bool,
             paused_seconds: float, comparison: dict, published: int, publish_failed: int,
             status: str, spark_job_running: bool = None, end_to_end_seconds: float = None) -> dict:
    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        items = _load_items(s, run_id)
        selection = json.loads(run.selection_json or "{}")
        summary = {
            "run_id": run_id, "status": status, "requested_size": run.requested_size,
            "replay_of": run.replay_of, "source": selection.get("source") or "google_drive",
            "source_dir": selection.get("source_dir"), "selected": selection.get("selected"),
            "published": published, "publish_failed": publish_failed,
            "completed": sum(i.status == "COMPLETED" for i in items),
            "failed": sum(i.status == "FAILED" for i in items),
            "not_finished": sum(i.status in ("STAGED", "PUBLISHED") for i in items),
            # timer: first event published to the test topic -> last photo finished
            "execution_seconds": round(execution_seconds, 3),
            # from pressing Start test (process start; for iteration 2+ the
            # iteration's start) to the last photo finished: photo staging,
            # CNN loading, Spark start, Kafka, preprocessing, CNN, saving;
            # minus any wait for farmer photos before the start
            "end_to_end_seconds": round(end_to_end_seconds, 3) if end_to_end_seconds is not None else None,
            "live_overlap": live_overlap,
            "paused_seconds": round(paused_seconds, 3),
            # the live Spark job was running (sharing the GPU) during this iteration
            "spark_job_running": spark_job_running,
            "comparison": comparison or None,
            "config": json.loads(run.config_json or "{}"),
            **report.summarize_samples(samples),
        }
        run.status, run.phase, run.finished_at = status, "Finished", now_utc()
        run.summary_json = json.dumps(summary, default=str)
        s.commit()
        out = report.write_reports(run, items, samples, summary)
    log(f"{run_id}: {status}. {summary['completed']} ok, {summary['failed']} failed in "
        f"{summary['execution_seconds']}s"
        + (" (live farmer processing overlapped: left out of the averages)" if live_overlap else "")
        + f". Files: {out}")
    return summary


def regenerate_report(run_id: str) -> Path:
    """Rewrites the CSV files from the stored summary + items. Resource
    samples are read back from the old resources.csv when it still exists."""
    import csv
    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        if run is None or not run.summary_json:
            raise RunError(f"run {run_id} has no finished summary")
        items = _load_items(s, run_id)
        samples = []
        path = run_dir(run_id) / "resources.csv"
        if path.exists():
            with open(path, encoding="utf-8") as f:
                samples = list(csv.DictReader(f))
        return report.write_reports(run, items, samples, json.loads(run.summary_json))


# --------------------------------------------------------------- reset
def _is_active(run) -> bool:
    return run.status in ACTIVE_STATUSES and _pid_alive(run.pid)


def cleanup(run_id: str, drive_results: bool = False) -> dict:
    """Removes a run's staged photos (hard links). For runs from before the
    isolation, also the Image / Prediction / ProcessingEvent / RejectedEvent
    rows they created in the production tables and, with drive_results,
    their result folder in Drive. Keeps the measurements, the before/after
    pair and the CSV files. Never touches the Drive originals, the local
    copies, the model, training data or any farmer."""
    reap_dead_runs()
    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        if run is None:
            raise RunError(f"unknown run {run_id}")
        if _is_active(run):
            raise RunError("the run is still in progress")
        ids = [i.image_id for i in _load_items(s, run_id)]
        counts = {"images": 0, "predictions": 0, "processing_events": 0, "rejected_events": 0}
        for chunk in _chunks(ids):
            # legacy runs only: rows that really are this run's test images
            test_ids = [r[0] for r in s.query(Image.image_id).filter(
                Image.image_id.in_(chunk), Image.source == SCALE_TEST_SOURCE).all()]
            if not test_ids:
                continue
            counts["predictions"] += s.query(Prediction).filter(
                Prediction.image_id.in_(test_ids)).delete(synchronize_session=False)
            counts["processing_events"] += s.query(ProcessingEvent).filter(
                ProcessingEvent.image_id.in_(test_ids)).delete(synchronize_session=False)
            counts["rejected_events"] += s.query(RejectedEvent).filter(
                RejectedEvent.image_id.in_(test_ids)).delete(synchronize_session=False)
            counts["images"] += s.query(Image).filter(
                Image.image_id.in_(test_ids)).delete(synchronize_session=False)
        comparison = (json.loads(run.summary_json or "{}").get("comparison") or {}).get("before")
        run.data_cleaned_at = now_utc()
        folder_id = run.drive_results_folder_id
        s.commit()

    for staged, root in ((LEGACY_STAGING_ROOT / run_id, LEGACY_STAGING_ROOT),
                         (run_dir(run_id) / "images", run_dir(run_id))):
        staged = staged.resolve()
        if staged.exists() and staged.is_relative_to(root.resolve()):
            keep = (run_dir(run_id) / comparison).resolve() if comparison else None
            for f in staged.iterdir():           # hard links / copies only; the originals stay
                if f.resolve() != keep:
                    f.unlink()
    counts["staged_files_removed"] = True

    if drive_results and folder_id:
        try:
            resp = drive_client._request("PATCH", f"{drive_client.DRIVE_API}/files/{folder_id}",
                                         params={"fields": "id"}, json={"trashed": True})
            drive_client._check(resp, f"trash results folder of {run_id}")
            counts["drive_results_folder"] = "moved to the central account's trash"
        except Exception as e:  # noqa: BLE001
            counts["drive_results_folder"] = f"FAILED: {e}"
    log(f"{run_id}: cleaned up {counts}")
    return counts


def delete(run_id: str, drive_results: bool = False) -> dict:
    """cleanup() plus the run's measurements and its whole folder."""
    counts = cleanup(run_id, drive_results)
    with SessionLocal() as s:
        s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).delete(synchronize_session=False)
        s.query(ScaleTestRun).filter(ScaleTestRun.run_id == run_id).delete(synchronize_session=False)
        s.commit()
    out = run_dir(run_id).resolve()
    if out.parent == REPORTS_DIR.resolve() and out.exists():
        shutil.rmtree(out)
    log(f"{run_id}: deleted")
    return counts


# --------------------------------------------------------------- CLI
class _Tee:
    """stdout that also goes to a log file — a run shown in a console window
    keeps the log file it would have had when started in the background."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, text):
        for st in self.streams:
            st.write(text)
        return len(text)

    def flush(self):
        for st in self.streams:
            st.flush()


def _tee_to(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    f = open(path, "a", encoding="utf-8", buffering=1)
    sys.stdout = _Tee(sys.stdout, f)
    sys.stderr = _Tee(sys.stderr, f)


def _main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    pv = sub.add_parser("preview", help="count the valid photos in the Drive collection (or a folder)")
    pv.add_argument("--source-dir", help="count the photos in this folder instead of the local photos")
    pv.add_argument("--from-drive", action="store_true", help="count the Google Drive collection instead")
    sp = sub.add_parser("prepare", help="verify/download local copies without running a test")
    sp.add_argument("--size", type=int, required=True)
    sp.add_argument("--workers", type=int, default=8)
    st = sub.add_parser("start", help="run a scalability test")
    st.add_argument("--size", type=int)
    st.add_argument("--iterations", type=int, default=1,
                    help="run the test this many times on the same photos (new item ids each time)")
    st.add_argument("--replay-of", help="reuse the photo selection of this run (new run_id)")
    st.add_argument("--source-dir", help="take the photos from this folder (and its subfolders) "
                                         "instead of the farmer photos stored on this laptop")
    st.add_argument("--from-drive", action="store_true",
                    help="take the photos from the Google Drive collection (downloads missing copies) "
                         "instead of the farmer photos stored on this laptop (the default)")
    st.add_argument("--workers", type=int, default=8, help="parallel downloads during preparation")
    st.add_argument("--stall-minutes", type=float, default=10,
                    help="stop waiting after this long without a single photo finishing")
    st.add_argument("--run-id", help=argparse.SUPPRESS)
    st.add_argument("--engine", choices=ENGINES,
                    help="what reads the test topic and processes the photos: the test's own Spark "
                         "Structured Streaming query (spark) or a plain Kafka consumer (python); "
                         "default SCALABILITY_ENGINE (spark)")
    st.add_argument("--live", choices=LIVE_MODES, default="photos",
                    help="what to show while it runs: one line per photo (default), "
                         "only progress lines (for very large runs), or nothing")
    for c in (sp, st):
        c.add_argument("--log-file", help="also write everything shown here to this file")
        c.add_argument("--keep-open", action="store_true",
                       help="wait for Enter before exiting (a console window opened by the dashboard "
                            "stays readable after the test)")
    for name in ("report", "cleanup", "delete"):
        c = sub.add_parser(name)
        c.add_argument("--run-id", required=True)
        if name != "report":
            c.add_argument("--drive-results", action="store_true",
                           help="also move a legacy run's result folder in Drive to the trash")
    sub.add_parser("list")
    sub.add_parser("reset", help="delete every scalability test and its data (scalability/reset.py)")
    args = p.parse_args(argv)
    for stream in (sys.stdout, sys.stderr):         # "—" in the live lines, any console / log
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    if getattr(args, "keep_open", False) and sys.platform == "win32":
        import ctypes
        ctypes.windll.kernel32.SetConsoleTitleW(f"EWS-ScalabilityTest ({args.cmd})")
    if getattr(args, "live", None):
        set_live(args.live)
    if getattr(args, "engine", None):
        set_engine(args.engine)
    if getattr(args, "log_file", None):
        _tee_to(args.log_file)

    init_db()
    try:
        if args.cmd == "preview":
            print(json.dumps(preview(args.source_dir, args.from_drive), indent=2))
        elif args.cmd == "prepare":
            prepare(args.size, args.workers)
        elif args.cmd == "start":
            if not args.size and not args.replay_of:
                p.error("give --size or --replay-of")
            if (args.source_dir or args.from_drive) and args.replay_of:
                p.error("--replay-of reuses the replayed run's own source; leave out --source-dir/--from-drive")
            if args.source_dir and args.from_drive:
                p.error("choose one source: --source-dir or --from-drive")
            if args.iterations > 1:
                if args.replay_of or args.run_id:
                    p.error("--iterations can't be combined with --replay-of")
                start_series(args.size, args.iterations, workers=args.workers,
                             stall_seconds=args.stall_minutes * 60, source_dir=args.source_dir,
                             from_drive=args.from_drive)
            else:
                start(args.size, replay_of=args.replay_of, workers=args.workers,
                      stall_seconds=args.stall_minutes * 60, run_id=args.run_id, source_dir=args.source_dir,
                      from_drive=args.from_drive)
        elif args.cmd == "report":
            print(regenerate_report(args.run_id))
        elif args.cmd == "cleanup":
            print(json.dumps(cleanup(args.run_id, args.drive_results), indent=2))
        elif args.cmd == "delete":
            print(json.dumps(delete(args.run_id, args.drive_results), indent=2))
        elif args.cmd == "reset":
            from scalability import reset as test_reset
            result = test_reset.reset_all(performed_by="cli")
            for step in result["steps"]:
                print(f"{'ok    ' if step['ok'] else 'FAILED'} {step['step']}: {step['detail']}")
            if result["left"]:
                print(f"still left: {', '.join(result['left'])}")
            print("reset complete" if result["ok"] else "reset NOT complete")
            if not result["ok"]:
                return 1
        elif args.cmd == "list":
            reap_dead_runs()
            with SessionLocal() as s:
                for r in s.query(ScaleTestRun).order_by(ScaleTestRun.started_at).all():
                    sm = json.loads(r.summary_json or "{}")
                    print(f"{r.run_id}  {r.status:<10} size={r.requested_size:<6} "
                          f"ok={sm.get('completed')} failed={sm.get('failed')} "
                          f"execution={sm.get('execution_seconds')}s")
    except RunError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        _wait_for_enter(args)
        return 1
    except Exception:
        import traceback
        traceback.print_exc()
        _wait_for_enter(args)
        return 1
    _wait_for_enter(args)
    return 0


def _wait_for_enter(args) -> None:
    if getattr(args, "keep_open", False):
        try:
            input("\nTest finished — results are on the dashboard's Scalability Test page. "
                  "Press Enter to close this window.")
        except EOFError:
            pass


if __name__ == "__main__":
    sys.exit(_main())
