"""
Scalability test module (scalability/): dataset selection from the Drive
collection or a custom folder, local-copy preparation, and an ISOLATED run:
its own Kafka topic (an in-memory stand-in here), its own worker running the
live pipeline's preprocessing + CNN code, nothing written to the production
tables, the production Kafka topic or Drive results — plus the real-time
priority gate, the two averages and the dashboard page.

Google Drive is the in-memory FakeDrive from test_drive_flow.
"""

import json
import threading
import time
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

import config.settings as settings  # noqa: E402
from storage import drive_client  # noqa: E402
from storage.database import (  # noqa: E402
    SessionLocal, Image, Prediction, ProcessingEvent, RejectedEvent, ScaleTestItem, ScaleTestRun,
)
from scalability import dataset, report, worker, run as runner  # noqa: E402
from spark import analytics  # noqa: E402
from tests.conftest import make_leaf_jpeg_bytes  # noqa: E402
from tests.test_drive_flow import (  # noqa: E402,F401 - fixtures
    drive, fresh_db, share, _scan, RESULTS_FOLDER,
)


class InMemoryTestTopic:
    """Stands in for the test's own Kafka topic + consumer group."""

    def __init__(self, fail_indexes=()):
        self.sent = []
        self._unread = []
        self.fail_indexes = set(fail_indexes)

    def send_many(self, events):
        failed = {}
        for i, e in enumerate(events):
            if i in self.fail_indexes:
                failed[i] = "broker said no"
                continue
            self.sent.append(dict(e))
            self._unread.append(dict(e))
        return failed

    def poll(self, timeout=0.5):
        out, self._unread = self._unread, []
        return out

    def close(self):
        pass


@pytest.fixture(autouse=True)
def python_engine(monkeypatch):
    """These tests drive the runner with the in-memory topic and the plain
    consumer loop; the Spark engine has its own tests
    (tests/test_scalability_spark.py)."""
    monkeypatch.setattr(settings, "SCALABILITY_ENGINE", "python")


@pytest.fixture()
def test_topic(monkeypatch):
    """The test's own topic, no live farmer processing, and proof that the
    production path (production Kafka producer, Spark sink) is never used."""
    topic = InMemoryTestTopic()
    monkeypatch.setattr(runner, "_test_queue", lambda: topic)
    monkeypatch.setattr(runner, "_live_gate", lambda: worker.LiveGate(check=lambda: False, interval=0.05))

    def forbidden(*a, **k):
        raise AssertionError("the scalability test used the production pipeline")
    import kafka.producer
    import spark.streaming_job
    monkeypatch.setattr(kafka.producer.KafkaBackend, "__init__", forbidden)
    monkeypatch.setattr(spark.streaming_job, "_process_one", forbidden)
    return topic


@pytest.fixture()
def collection(drive):
    """Two farmers, five distinct photos, plus files that must be excluded."""
    share("alice@gmail.com")
    share("bob@gmail.com")
    for n in range(3):
        drive.phone_upload("alice@gmail.com", make_leaf_jpeg_bytes(96 + n))
    bob_photos = [make_leaf_jpeg_bytes(120 + n) for n in range(2)]
    for content in bob_photos:
        drive.phone_upload("bob@gmail.com", content)
    drive.phone_upload("bob@gmail.com", bob_photos[0])                         # same photo again
    drive.phone_upload("alice@gmail.com", b"x" * 10, name="holiday.jpg")      # name != image_id
    _scan()                      # the watcher registers the farmers (and their originals)
    return drive


@pytest.fixture()
def downloads(monkeypatch):
    calls = []
    real = drive_client.download_file

    def counting(file_id):
        calls.append(file_id)
        return real(file_id)
    monkeypatch.setattr(drive_client, "download_file", counting)
    return calls


def _production_counts():
    with SessionLocal() as s:
        return (s.query(Image).count(), s.query(Prediction).count(), s.query(ProcessingEvent).count(),
                s.query(RejectedEvent).count())


def _production_queue_lines():
    path = settings.LOCAL_QUEUE_DIR / f"{settings.KAFKA_TOPIC}.jsonl"
    return path.read_text().count("\n") if path.exists() else 0


def test_collection_keeps_valid_photos_once_and_counts_exclusions(collection):
    col = dataset.enumerate_collection()
    assert len(col.candidates) == 5
    assert col.total_files == 7
    assert col.excluded == {"same photo uploaded more than once": 1, "file name does not match its image_id": 1}
    assert len({c.drive_file_id for c in col.candidates}) == 5


def test_prepare_downloads_once_then_verifies(collection, downloads):
    col = dataset.enumerate_collection()
    first = dataset.prepare_local_copies(col.candidates[:3])
    assert (first["downloaded"], first["verified_local"], first["ready"]) == (3, 0, 3)
    again = dataset.prepare_local_copies(dataset.enumerate_collection().candidates[:3])
    assert (again["downloaded"], again["verified_local"]) == (0, 3)
    assert len(downloads) == 3                          # never downloaded twice


def test_corrupt_local_copy_is_downloaded_again(collection, downloads):
    c = dataset.enumerate_collection().candidates[0]
    dataset.prepare_local_copies([c])
    with open(c.local_path, "wb") as f:
        f.write(b"truncated")
    c2 = dataset.enumerate_collection().candidates[0]
    assert dataset.prepare_local_copies([c2])["downloaded"] == 1
    assert len(downloads) == 2


def test_run_is_isolated_from_production(collection, test_topic, downloads):
    farmer_results = [collection.folder_of(e, RESULTS_FOLDER) for e in ("alice@gmail.com", "bob@gmail.com")]
    results_before = sum(len(collection.files_in(f)) for f in farmer_results if f)
    production_before, queue_before = _production_counts(), _production_queue_lines()
    review_before = set(settings.REVIEW_DIR.glob("*")) if settings.REVIEW_DIR.exists() else set()

    run_id = runner.start(4, from_drive=True)

    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()
    summary = json.loads(run.summary_json)
    assert run.status == "COMPLETED" and summary["completed"] == 4 and summary["failed"] == 0
    assert summary["execution_seconds"] > 0 and summary["live_overlap"] is False
    for i in items:
        t = json.loads(i.timings_json)
        assert t["preprocess_s"] > 0 and t["cnn_s"] > 0
        assert t["processing_s"] == pytest.approx(t["preprocess_s"] + t["cnn_s"], abs=2e-4)
        assert t["contended"] is False
    # nothing in the production tables, the production topic, farmers' Drive or the labelling folder
    assert _production_counts() == production_before
    assert _production_queue_lines() == queue_before
    assert sum(len(collection.files_in(f)) for f in farmer_results if f) == results_before
    assert (set(settings.REVIEW_DIR.glob("*")) if settings.REVIEW_DIR.exists() else set()) == review_before
    assert not set(analytics.images_with_predictions()["image_id"]) & {i.image_id for i in items}
    # its own topic, its own ids, its own folder
    assert len(test_topic.sent) == 4
    assert all(e == {"run_id": run_id, "item_id": e["item_id"]} for e in test_topic.sent)
    assert not {i.image_id for i in items} & {i.source_image_id for i in items}
    folder = runner.run_dir(run_id)
    assert len(list((folder / "images").iterdir())) == 4
    pair = summary["comparison"]
    assert (folder / pair["before"]).is_file() and (folder / pair["after"]).is_file()
    for name in ("summary.csv", "items.csv"):
        assert (folder / name).exists()
    assert summary["selected"] == 4 and len(downloads) == 4


def test_after_image_is_the_preprocessing_output_of_the_before_image(photo_folder, test_topic):
    from io import BytesIO
    from PIL import Image as PILImage
    from spark.preprocessing import prepare_image
    from ml.inference import model_input_image
    run_id = runner.start(1, source_dir=str(photo_folder))
    with SessionLocal() as s:
        pair = json.loads(s.get(ScaleTestRun, run_id).summary_json)["comparison"]
    folder = runner.run_dir(run_id)
    expected = model_input_image(prepare_image((folder / pair["before"]).read_bytes()))
    saved = PILImage.open(BytesIO((folder / pair["after"]).read_bytes())).convert("RGB")
    assert saved.size == expected.size and saved.tobytes() == expected.tobytes()


def test_production_spark_job_skips_test_events():
    """A test event in the production topic (from before the isolation) is
    never processed as a farmer photo."""
    from spark.streaming_job import _process_one
    before = _production_counts()
    assert _process_one({"image_id": "abc", "user_id": "u", "image_path": "x.jpg",
                         "uploaded_at": "2026-10-10T00:00:00+00:00", "run_id": "scale-old"}) is None
    assert _production_counts() == before


def test_replay_reuses_local_copies_with_new_ids(collection, test_topic, monkeypatch):
    first = runner.start(3, from_drive=True)

    def no_download(*a, **k):
        raise AssertionError("replay must not download")
    monkeypatch.setattr(drive_client, "download_file", no_download)
    second = runner.start(replay_of=first)

    with SessionLocal() as s:
        a = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == first).all()
        b = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == second).all()
        run = s.get(ScaleTestRun, second)
    assert run.replay_of == first and run.status == "COMPLETED"
    assert {i.source_drive_file_id for i in a} == {i.source_drive_file_id for i in b}
    assert not {i.image_id for i in a} & {i.image_id for i in b}
    assert all(i.local_copy_status == "verified" for i in b)


def test_kafka_publish_failure_is_recorded_and_not_counted(collection, monkeypatch):
    topic = InMemoryTestTopic(fail_indexes={0})
    monkeypatch.setattr(runner, "_test_queue", lambda: topic)
    monkeypatch.setattr(runner, "_live_gate", lambda: worker.LiveGate(check=lambda: False, interval=0.05))
    run_id = runner.start(3, from_drive=True)
    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()
    summary = json.loads(run.summary_json)
    assert summary["publish_failed"] == 1 and summary["completed"] == 2 and summary["failed"] == 1
    assert any("Kafka publish failed" in (i.error_message or "") for i in items)
    avg = report.series_averages([run], items)
    assert avg["average_execution_s"] is None                       # an iteration with a failure
    assert avg["photos_measured"] == 2                              # the two photos that worked


def test_cleanup_and_delete_keep_originals(collection, test_topic):
    run_id = runner.start(2, from_drive=True)
    with SessionLocal() as s:
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()
        pair = json.loads(s.get(ScaleTestRun, run_id).summary_json)["comparison"]
    local_copies = [dataset.downloaded_photo_path(i.source_user_id, i.source_image_id) for i in items]
    folder = runner.run_dir(run_id)

    runner.cleanup(run_id)
    assert [p.name for p in (folder / "images").iterdir()] == [folder.joinpath(pair["before"]).name]
    assert all(p is not None and p.exists() for p in local_copies)          # originals' copies stay
    with SessionLocal() as s:
        assert s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).count() == 2

    runner.delete(run_id)
    with SessionLocal() as s:
        assert s.get(ScaleTestRun, run_id) is None
    assert not folder.exists()
    assert all(p.exists() for p in local_copies)


@pytest.fixture()
def photo_folder(tmp_path):
    """A custom folder: 4 distinct photos (one in a subfolder, one .JPEG),
    plus files that must be left out."""
    root = tmp_path / "my photos"
    (root / "field 2").mkdir(parents=True)
    for n in range(2):
        (root / f"leaf_{n}.jpg").write_bytes(make_leaf_jpeg_bytes(90 + n))
    (root / "field 2" / "IMG_7.JPEG").write_bytes(make_leaf_jpeg_bytes(110))
    from io import BytesIO
    from PIL import Image as PILImage
    png = BytesIO()
    PILImage.open(BytesIO(make_leaf_jpeg_bytes(100))).save(png, format="PNG")
    (root / "field 2" / "scan.png").write_bytes(png.getvalue())
    (root / "copy_of_leaf_0.jpg").write_bytes((root / "leaf_0.jpg").read_bytes())   # same photo again
    (root / "broken.jpg").write_bytes(b"not a photo")
    (root / "empty.jpg").write_bytes(b"")
    (root / "notes.txt").write_text("ignored: not a photo file name")
    return root


def _snapshot(folder):
    return {p: p.read_bytes() for p in folder.rglob("*") if p.is_file()}


def test_folder_keeps_valid_photos_once_and_counts_exclusions(photo_folder):
    col = dataset.enumerate_folder(photo_folder)
    assert len(col.candidates) == 4
    assert col.total_files == 7                       # notes.txt isn't a photo file
    assert col.excluded == {"same photo more than once": 1, "not a JPEG or PNG image": 1, "empty file": 1}
    assert all(c.drive_file_id.startswith("local:") and c.local_path for c in col.candidates)
    assert {c.mime_type for c in col.candidates} == {"image/jpeg", "image/png"}


def test_missing_folder_is_refused(tmp_path):
    with pytest.raises(runner.RunError, match="folder not found"):
        runner.preview(str(tmp_path / "nope"))
    with pytest.raises(runner.RunError, match="folder not found"):
        runner.start(1, source_dir=str(tmp_path / "nope"))


def test_full_run_from_a_custom_folder(photo_folder, test_topic, downloads):
    before = _snapshot(photo_folder)
    run_id = runner.start(10, source_dir=str(photo_folder))

    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()
    summary = json.loads(run.summary_json)
    assert run.status == "COMPLETED"
    assert summary["selected"] == 4 and summary["completed"] == 4 and summary["source"] == "folder"
    assert downloads == []                                         # Google Drive never used
    assert all(i.source_drive_file_id.startswith("local:") for i in items)
    assert {p.suffix for p in (runner.run_dir(run_id) / "images").iterdir()} <= {".jpg", ".png"}
    assert _snapshot(photo_folder) == before                       # nothing moved or changed

    runner.delete(run_id)
    assert _snapshot(photo_folder) == before                       # deleting never touches the folder


def test_photos_on_another_drive_are_read_in_place_not_copied(photo_folder, test_topic, monkeypatch):
    """os.link fails across drives; the test must then read the originals
    where they are instead of filling the disk with copies."""
    import os

    def cross_drive(src, dst):
        raise OSError(17, "The system cannot move the file to a different disk drive")
    monkeypatch.setattr(os, "link", cross_drive)
    before = _snapshot(photo_folder)
    copies = []
    import shutil
    real_copy2 = shutil.copy2
    monkeypatch.setattr(shutil, "copy2", lambda src, dst, **k: copies.append(dst) or real_copy2(src, dst, **k))

    run_id = runner.start(10, source_dir=str(photo_folder))

    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
    summary = json.loads(run.summary_json)
    assert run.status == "COMPLETED" and summary["completed"] == 4
    staged = runner.run_dir(run_id) / "images"
    assert not staged.exists() or not any(staged.iterdir())        # nothing staged, nothing copied
    assert len(copies) == 1                                         # only the before/after picture
    assert (runner.run_dir(run_id) / summary["comparison"]["before"]).is_file()
    runner.delete(run_id)
    assert _snapshot(photo_folder) == before                       # originals untouched


def test_replay_of_a_folder_run_finds_renamed_photos(photo_folder, test_topic):
    first = runner.start(10, source_dir=str(photo_folder))
    (photo_folder / "leaf_1.jpg").rename(photo_folder / "renamed.jpg")
    second = runner.start(replay_of=first)

    with SessionLocal() as s:
        a = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == first).all()
        b = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == second).all()
        run = s.get(ScaleTestRun, second)
    assert run.status == "COMPLETED" and json.loads(run.config_json)["SOURCE_DIR"] == str(photo_folder.resolve())
    assert {i.source_drive_file_id for i in a} == {i.source_drive_file_id for i in b}


def test_iterations_keep_images_and_measurements_apart(photo_folder, test_topic):
    run_ids = runner.start_series(10, 3, source_dir=str(photo_folder))

    with SessionLocal() as s:
        runs = [s.get(ScaleTestRun, r) for r in run_ids]
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id.in_(run_ids)).all()
        s.expunge_all()
    assert len(runs) == 3 and all(r.status == "COMPLETED" for r in runs)
    assert len({json.loads(r.config_json)["SERIES_ID"] for r in runs}) == 1
    assert [json.loads(r.config_json)["ITERATION"] for r in runs] == [1, 2, 3]
    # 4 unique photos x 3 iterations = 12 processings, 12 distinct item ids, 3 separate folders
    assert len(items) == 12 and len({i.image_id for i in items}) == 12
    assert len({i.source_drive_file_id for i in items}) == 4
    for r in run_ids:
        staged = {p.stem for p in (runner.run_dir(r) / "images").iterdir()}
        assert staged == {i.image_id for i in items if i.run_id == r}

    avg = report.series_averages(runs, items)
    durations = [json.loads(r.summary_json)["execution_seconds"] for r in runs]
    per_photo = [json.loads(i.timings_json)["processing_s"] for i in items]
    assert avg["iterations_measured"] == 3
    assert avg["average_execution_s"] == pytest.approx(sum(durations) / 3)
    assert avg["average_processing_s"] == pytest.approx(sum(per_photo) / 12)


def test_live_farmer_processing_goes_first(photo_folder, monkeypatch):
    """While farmer photos are being processed the test does not start; it
    starts once they are done, and the waiting is not measured."""
    topic = InMemoryTestTopic()
    live = {"on": True}
    monkeypatch.setattr(runner, "_test_queue", lambda: topic)
    monkeypatch.setattr(runner, "_live_gate", lambda: worker.LiveGate(check=lambda: live["on"], interval=0.05))
    threading.Timer(0.6, lambda: live.update(on=False)).start()
    started = time.monotonic()
    run_id = runner.start(2, source_dir=str(photo_folder))
    with SessionLocal() as s:
        summary = json.loads(s.get(ScaleTestRun, run_id).summary_json)
    assert time.monotonic() - started >= 0.6
    assert summary["completed"] == 2 and summary["live_overlap"] is False


def test_live_gate_pauses_and_marks_overlapping_measurements():
    live = {"on": False}
    gate = worker.LiveGate(check=lambda: live["on"], interval=0.02).start()
    try:
        t0 = time.monotonic()
        live["on"] = True
        time.sleep(0.1)
        assert gate.paused and not gate.wait(timeout=0.05)          # no new photo may start
        live["on"] = False
        assert gate.wait(timeout=1)
        t1 = time.monotonic()
        assert gate.overlapped(t0, t1) and gate.paused_seconds(t0, t1) > 0
        time.sleep(0.05)
        assert not gate.overlapped(time.monotonic(), time.monotonic() + 0.01)
    finally:
        gate.stop()


def test_live_processing_active_ignores_test_and_stuck_photos(collection):
    """Only a farmer photo INGESTED/PROCESSING recently counts as live."""
    from datetime import timedelta
    from storage.database import ProcessingStatus, now_utc
    with SessionLocal() as s:
        s.query(Image).update({Image.status: ProcessingStatus.COMPLETED})
        s.commit()
    assert worker.live_processing_active() is False
    with SessionLocal() as s:
        img = s.query(Image).first()
        img.status, img.ingested_at = ProcessingStatus.INGESTED, now_utc() - timedelta(hours=2)
        s.commit()
        assert worker.live_processing_active() is False                 # stuck, not active
        img.ingested_at = now_utc()
        s.commit()
    assert worker.live_processing_active() is True


def _run(run_id, status="COMPLETED", **summary):
    return SimpleNamespace(run_id=run_id, status=status, summary_json=json.dumps(summary))


def _item(run_id, processing_s, status="COMPLETED", contended=False):
    return SimpleNamespace(run_id=run_id, status=status,
                           timings_json=json.dumps({"processing_s": processing_s, "contended": contended}))


def test_total_execution_time_counts_from_pressing_start_for_every_iteration():
    ok = [_run("a", execution_seconds=10.0, end_to_end_seconds=40.0, failed=0),
          _run("b", execution_seconds=12.0, end_to_end_seconds=30.0, failed=0)]
    assert report.series_averages(ok, [])["total_end_to_end_s"] == pytest.approx(70.0)
    one_bad = ok + [_run("c", execution_seconds=9.0, end_to_end_seconds=20.0, failed=1)]
    assert report.series_averages(one_bad, [])["total_end_to_end_s"] is None    # every iteration must count
    old = [_run("d", execution_seconds=10.0, failed=0)]                           # before 11 October 2026
    assert report.series_averages(old, [])["total_end_to_end_s"] is None


def test_end_to_end_includes_setup_before_the_photo_timer(photo_folder, test_topic):
    run_ids = runner.start_series(10, 2, source_dir=str(photo_folder))
    with SessionLocal() as s:
        summaries = [json.loads(s.get(ScaleTestRun, r).summary_json) for r in run_ids]
    for sm in summaries:
        assert sm["end_to_end_seconds"] >= sm["execution_seconds"] > 0


def test_averages_leave_out_invalid_measurements():
    runs = [_run("a", execution_seconds=10.0, failed=0), _run("b", execution_seconds=20.0, failed=0),
            _run("c", execution_seconds=99.0, failed=0, live_overlap=True),     # farmers were processed
            _run("d", execution_seconds=50.0, failed=1),                         # a photo failed
            _run("e", status="INCOMPLETE", execution_seconds=600.0, failed=0)]   # never finished
    items = [_item("a", 0.2), _item("b", 0.4), _item("c", 0.3), _item("c", 5.0, contended=True),
             _item("d", 0.6), _item("d", None, status="FAILED"), _item("e", 9.0)]
    avg = report.series_averages(runs, items)
    assert avg["average_execution_s"] == pytest.approx(15.0)                 # (10 + 20) / 2
    assert avg["average_processing_s"] == pytest.approx((0.2 + 0.4 + 0.3 + 0.6) / 4)
    assert avg["total_execution_s"] == pytest.approx(30.0)                   # 10 + 20
    assert avg["total_processing_s"] == pytest.approx(0.2 + 0.4 + 0.3 + 0.6)

    nothing = report.series_averages([_run("x", execution_seconds=5.0, failed=0, live_overlap=True)],
                                     [_item("x", 1.0, contended=True)])
    assert nothing["average_execution_s"] is None and nothing["average_processing_s"] is None
    assert nothing["total_execution_s"] is None and nothing["total_processing_s"] is None


def test_page_checks_a_custom_folder(photo_folder, drive):  # noqa: F811
    from tests.test_dashboard_users import _logged_in, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    at = _logged_in("Scalability Test")
    at.radio(key="scale_source").set_value("Custom folder").run()
    assert [b for b in at.button if b.label == "Check the folder"][0].disabled      # no path yet
    at.text_input(key="scale_source_dir").set_value(f'"{photo_folder}"').run()      # pasted with quotes
    [b for b in at.button if b.label == "Check the folder"][0].click().run()

    assert not at.exception
    assert at.selectbox(key="scale_size").value == 4                                # all 4 valid photos
    assert "Prepare local copies" not in [b.label for b in at.button]              # Drive-only step


def test_page_shows_averages_disease_data_and_every_photo(photo_folder, test_topic, drive):  # noqa: F811
    from tests.test_dashboard_users import _logged_in, _all_text, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    runner.start_series(10, 2, source_dir=str(photo_folder))
    at = _logged_in("Scalability Test")

    assert not at.exception
    assert [m.label for m in at.metric] == ["Total Execution Time", "Average Processing Time"]
    assert at.metric[0].value.endswith(" seconds") and at.metric[1].value.endswith(" seconds/image")
    assert [h.value for h in at.subheader if h.value in ("Disease data", "All test photos")] == \
        ["Disease data", "All test photos"]
    # the gallery: every photo of iteration 1 (4 unique photos), before + after each
    captions = [c for el in at.get("image") for c in el.captions if c]    # the app logo has none
    assert captions == ["Before", "After"] * 4
    text = _all_text(at)
    assert "Unknown location" in text                  # custom-folder photos carry no municipality


def test_run_keeps_cnn_result_and_place_for_the_disease_charts(drive, test_topic):  # noqa: F811
    """The test's own CNN result, municipality and GPS are stored per test
    photo (scale_test_items only) — still nothing in the production tables."""
    share("carol@gmail.com")
    with_gps, _ = drive.phone_upload("carol@gmail.com", make_leaf_jpeg_bytes(100), location="  baucau ",
                                     extra_props={"gps_lat": "-8.47", "gps_lon": "126.45"})
    without_gps, _ = drive.phone_upload("carol@gmail.com", make_leaf_jpeg_bytes(101), location="Dili")
    _scan()
    before = _production_counts()
    run_id = runner.start(2, from_drive=True)

    with SessionLocal() as s:
        items = {i.source_image_id: i for i in s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id)}
    assert {i.status for i in items.values()} == {"COMPLETED"}
    for i in items.values():
        assert i.predicted_class and i.risk_level in ("LOW", "MEDIUM", "HIGH")
        assert 0 <= i.prediction_confidence <= 1 and i.is_uncertain in (True, False)
    assert items[with_gps].location == "Baucau"
    assert (items[with_gps].gps_latitude, items[with_gps].gps_longitude) == (-8.47, 126.45)
    assert items[without_gps].location == "Dili" and items[without_gps].gps_latitude is None
    assert _production_counts() == before
    frame = report.items_frame(list(items.values()))
    assert set(frame["location"]) == {"Baucau", "Dili"}


def test_previews_are_saved_once_per_test_after_the_timer(photo_folder, test_topic):
    run_ids = runner.start_series(10, 2, source_dir=str(photo_folder))
    with SessionLocal() as s:
        first = [i.image_id for i in s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_ids[0])]
    for item_id in first:
        assert all(p.is_file() for p in worker.preview_paths(runner.run_dir(run_ids[0]), item_id))
    assert not (runner.run_dir(run_ids[1]) / "previews").exists()     # iteration 2 repeats the same photos
    runner.cleanup(run_ids[0])                                         # clean-up keeps the gallery
    assert all(p.is_file() for p in worker.preview_paths(runner.run_dir(run_ids[0]), first[0]))


def test_results_are_written_in_batches_not_per_photo(photo_folder, test_topic, monkeypatch):
    calls = []
    real = runner._record
    monkeypatch.setattr(runner, "_record", lambda run_id, outcomes: (calls.append(len(outcomes)),
                                                                     real(run_id, outcomes)))
    runner.start(10, source_dir=str(photo_folder))
    assert sum(calls) == 4 and len(calls) < 4 + 1


def test_resource_monitor_always_has_a_real_sample():
    """psutil.cpu_percent is per thread: the monitor primes it in its own
    sampling thread (it used to report 0.0 for the first second) and takes a
    final sample on stop, so a very short run still has one."""
    from scalability.monitor import ResourceMonitor
    samples = ResourceMonitor(interval=5).start().stop()
    assert len(samples) == 1 and samples[0]["cpu_percent"] is not None


def test_page_says_so_when_nothing_valid_was_measured(photo_folder, drive, monkeypatch):  # noqa: F811
    from tests.test_dashboard_users import _logged_in, _all_text, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    topic = InMemoryTestTopic(fail_indexes={0, 1, 2, 3})      # nothing gets through
    monkeypatch.setattr(runner, "_test_queue", lambda: topic)
    monkeypatch.setattr(runner, "_live_gate", lambda: worker.LiveGate(check=lambda: False, interval=0.05))
    runner.start(10, source_dir=str(photo_folder))
    at = _logged_in("Scalability Test")

    assert not at.exception
    assert not at.metric
    assert "no valid timing measurement" in _all_text(at)


def test_overview_queue_shows_local_copies_and_no_test_photos(photo_folder, collection, test_topic, monkeypatch):
    """The Overview "Analysis Queue" lists farmer uploads only and shows the
    saved local copy instead of downloading the original from Drive."""
    from tests.test_dashboard_users import _logged_in, _all_text, _seed_admin_and_farmer
    from tests.test_drive_flow import _event_for, _run_spark
    farmer = dataset.enumerate_collection().candidates[0]
    _run_spark(_event_for(farmer.image_id))            # processed -> local copy saved
    assert dataset.downloaded_photo_path(farmer.user_id, farmer.image_id) is not None
    run_id = runner.start(10, source_dir=str(photo_folder))
    _seed_admin_and_farmer()

    def no_download(*a, **k):
        raise AssertionError("the dashboard downloaded an original that has a local copy")
    monkeypatch.setattr(drive_client, "download_file", no_download)

    at = _logged_in("Overview")
    text = _all_text(at)
    with SessionLocal() as s:
        test_ids = [i.image_id for i in s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id)]
    assert f"#{farmer.image_id[:8]}" in text
    assert not any(f"#{t[:8]}" in text for t in test_ids)


def test_only_one_job_at_a_time(collection, test_topic):
    runner._acquire_lock("run", "other")
    try:
        with pytest.raises(runner.RunError):
            runner.start(1, from_drive=True)
        with pytest.raises(runner.RunError):
            runner.start_series(1, 2)
    finally:
        runner._release_lock()


def test_test_topic_can_never_be_the_production_topic(monkeypatch):
    monkeypatch.setattr(settings, "SCALABILITY_KAFKA_TOPIC", settings.KAFKA_TOPIC)
    with pytest.raises(ValueError, match="production Kafka topic"):
        worker.KafkaTestQueue()


# --------------------------------------------------- live console output (--live)
@pytest.fixture()
def live_mode():
    yield runner.set_live
    runner.set_live("photos")


def test_live_console_shows_kafka_and_every_photo(photo_folder, test_topic, capsys, live_mode):
    live_mode("photos")
    run_id = runner.start(10, source_dir=str(photo_folder))
    out = capsys.readouterr().out
    assert "[timer]    started — publishing 4 event(s)" in out
    assert f"'{settings.KAFKA_TOPIC}' is not touched" in out
    assert "[kafka]    consumed" in out and "(4 of 4 received)" in out
    photo_lines = [line for line in out.splitlines() if "[cnn]" in line and "/4  " in line]
    assert len(photo_lines) == 4 and all("preprocess" in line and "cnn " in line for line in photo_lines)
    with SessionLocal() as s:
        cfg = json.loads(s.get(ScaleTestRun, run_id).config_json)
    assert cfg["LIVE_OUTPUT"] == "photos"


def test_live_progress_mode_has_no_photo_lines(photo_folder, test_topic, capsys, live_mode, monkeypatch):
    live_mode("progress")
    monkeypatch.setattr(runner, "PROGRESS_EVERY_S", 0.0)          # every loop turn
    runner.start(10, source_dir=str(photo_folder))
    out = capsys.readouterr().out
    assert "[progress]" in out and "photos/s" in out
    assert not [line for line in out.splitlines() if "[cnn]" in line and "preprocess" in line]


def test_live_off_keeps_the_old_quiet_output(photo_folder, test_topic, capsys, live_mode):
    live_mode("off")
    runner.start(10, source_dir=str(photo_folder))
    out = capsys.readouterr().out
    assert "COMPLETED. 4 ok" in out
    assert "[kafka]" not in out and "[progress]" not in out and "[cnn]" not in out


def test_live_mode_is_validated():
    with pytest.raises(runner.RunError):
        runner.set_live("loud")


def test_photo_line_formats_results_failures_and_overlap():
    ok = ("COMPLETED", None, {"preprocess_s": 0.1234, "cnn_s": 0.05, "contended": True},
          {"predicted_class": "common_rust", "prediction_confidence": 0.973, "risk_level": "HIGH"})
    line = runner._photo_line(7, 120, "abcdef0123456789", ok)
    assert "  7/120  abcdef01" in line and "preprocess 0.12s" in line and "common_rust 97.30%" in line
    assert "risk HIGH" in line and "excluded" in line
    bad = runner._photo_line(1, 2, "deadbeef00", ("FAILED", "OSError: broken file", {}, None))
    assert "FAILED  OSError: broken file" in bad


def test_cli_log_file_gets_a_copy_of_the_console(tmp_path, monkeypatch, capsys):
    import sys as _sys
    log = tmp_path / "logs" / "run.log"
    monkeypatch.setattr(_sys, "stdout", _sys.stdout)                # restored after _tee_to replaces it
    monkeypatch.setattr(_sys, "stderr", _sys.stderr)
    assert runner._main(["list", ]) == 0                            # no --log-file on list: unchanged
    runner._tee_to(str(log))
    runner.log("hello from the test")
    _sys.stdout.flush()
    assert "hello from the test" in log.read_text(encoding="utf-8")


def test_dashboard_launch_in_a_console_window_or_hidden(monkeypatch, tmp_path):
    import subprocess
    from dashboard import scalability_page as page
    calls = []
    monkeypatch.setattr(page, "LOG_DIR", tmp_path)
    monkeypatch.setattr(page.subprocess, "Popen", lambda argv, **kw: calls.append((argv, kw)))
    monkeypatch.setattr(page.sys, "platform", "win32")

    log = page._launch(["start", "--size", "5"], console=True)
    argv, kw = calls[-1]
    assert argv[-3:] == ["--log-file", log, "--keep-open"]
    assert kw["creationflags"] & subprocess.CREATE_NEW_CONSOLE
    assert "stdout" not in kw                                      # the window shows it; --log-file keeps a copy

    page._launch(["start", "--size", "5"], console=False)
    argv, kw = calls[-1]
    assert "--keep-open" not in argv and kw["creationflags"] & subprocess.CREATE_NO_WINDOW


# --------------------------------------------------- default source: local photos, no Google Drive
@pytest.fixture()
def local_photos_dir(tmp_path, monkeypatch):
    import api.image_storage
    path = tmp_path / "downloaded_photos"
    monkeypatch.setattr(settings, "DOWNLOADED_PHOTOS_DIR", path)
    monkeypatch.setattr(api.image_storage, "DOWNLOADED_PHOTOS_DIR", path)
    return path


def _forbid_drive(monkeypatch):
    def forbidden(*a, **k):
        raise AssertionError("the scalability test contacted Google Drive")
    for name in ("list_subfolders", "list_files_in_folders", "download_file", "get_file_metadata",
                 "images_folder_id"):
        monkeypatch.setattr(drive_client, name, forbidden)


def test_default_source_is_local_photos_without_google_drive(collection, test_topic, local_photos_dir,
                                                             monkeypatch):
    drive_run = runner.start(5, from_drive=True)          # leaves the local copies Spark would keep
    _forbid_drive(monkeypatch)
    run_id = runner.start(5)
    with SessionLocal() as s:
        run = s.get(ScaleTestRun, run_id)
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()
        drive_items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == drive_run).all()
        farmer = {i.image_id: i for i in s.query(Image)}
    summary, config = json.loads(run.summary_json), json.loads(run.config_json)
    assert run.status == "COMPLETED" and summary["completed"] == 5 and summary["source"] == "local"
    assert config["SOURCE"] == "local" and "no Google Drive" in config["IMAGE_SOURCE"]
    assert {i.source_image_id for i in items} == {i.source_image_id for i in drive_items}
    for i in items:
        assert i.source_drive_file_id.startswith("local:") and i.local_copy_status == "local copy"
        assert i.source_user_id == farmer[i.source_image_id].user_id
        assert i.location == farmer[i.source_image_id].location
    # oldest upload first, like the Drive collection
    order = [i.source_image_id for i in sorted(items, key=lambda i: i.source_image_id)]
    assert sorted(order, key=lambda i: farmer[i].uploaded_at) == [
        c.image_id for c in runner.dataset.enumerate_local_photos().candidates]


def test_old_drive_run_is_replayed_from_local_copies(collection, test_topic, local_photos_dir, monkeypatch):
    first = runner.start(3, from_drive=True)
    with SessionLocal() as s:                               # as recorded before the local source existed
        run = s.get(ScaleTestRun, first)
        cfg = json.loads(run.config_json)
        cfg.pop("SOURCE", None)
        run.config_json = json.dumps(cfg)
        s.commit()
    _forbid_drive(monkeypatch)
    second = runner.start(replay_of=first)
    with SessionLocal() as s:
        a = {i.source_image_id for i in s.query(ScaleTestItem).filter(ScaleTestItem.run_id == first)}
        b = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == second).all()
        summary = json.loads(s.get(ScaleTestRun, second).summary_json)
    assert summary["status"] == "COMPLETED" and summary["source"] == "local"
    assert {i.source_image_id for i in b} == a and summary["completed"] == 3


def test_local_photos_without_a_database_row_still_count(local_photos_dir, test_topic, monkeypatch):
    from api.image_storage import store_downloaded_photo
    _forbid_drive(monkeypatch)
    store_downloaded_photo("someuser", "orphan-photo-1", make_leaf_jpeg_bytes(90))
    store_downloaded_photo("someuser", "orphan-photo-2", make_leaf_jpeg_bytes(91))
    preview = runner.preview()
    assert preview["valid"] == 2 and preview["missing_local_copy"] == 0
    run_id = runner.start(2)
    with SessionLocal() as s:
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id == run_id).all()
    assert {i.source_image_id for i in items} == {"orphan-photo-1", "orphan-photo-2"}
    assert all(i.status == "COMPLETED" and i.location is None for i in items)


def test_no_local_photos_is_a_clear_error(local_photos_dir, test_topic, monkeypatch):
    _forbid_drive(monkeypatch)
    with pytest.raises(runner.RunError, match="no valid photos in"):
        runner.start(5)


def test_page_offers_local_photos_not_google_drive(local_photos_dir, drive, monkeypatch):  # noqa: F811
    from api.image_storage import store_downloaded_photo
    _forbid_drive(monkeypatch)
    store_downloaded_photo("someuser", "photo-1", make_leaf_jpeg_bytes(90))
    from tests.test_dashboard_users import _logged_in, _seed_admin_and_farmer
    _seed_admin_and_farmer()
    at = _logged_in("Scalability Test")
    radio = at.radio(key="scale_source")
    assert radio.value == "Local photos on this laptop" and "Google Drive" not in radio.options
    [b for b in at.button if b.label == "Check the local photos"][0].click().run()
    assert not at.exception, at.exception
    assert "Prepare local copies" not in [b.label for b in at.button]
    assert [b for b in at.button if b.label == "Start test"]
