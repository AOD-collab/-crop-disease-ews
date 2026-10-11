# Scalability test (Demo 2)

Measures how fast the system's image processing — **Kafka → Apache Spark
Structured Streaming → the live pipeline's own preprocessing
(`spark/preprocessing.prepare_image`) and CNN (`ml/inference.predict`)** —
handles a large batch of real photos, **isolated from the real-time pipeline**
(Demo 1) so that a test never touches farmer submissions, predictions or
results.

**Engine (since 11 October 2026): the test's own Spark query.** The test topic
is read by a Spark Structured Streaming query that belongs to the test
(`scalability/spark_engine.py`, started inside the test process) — the same
shape as the live job: micro-batches of up to `SPARK_MAX_EVENTS_PER_BATCH`
events every `SPARK_TRIGGER_SECONDS`, parse + filter as Spark SQL, then
`foreachBatch` runs preprocessing + CNN `SCALABILITY_WORKERS` photos at a time,
and a micro-batch commits only when all its photos are done. It is a separate
query with its own checkpoint (deleted after each run); the live Spark job
(`spark/streaming_job.py`) is not changed and never sees the test topic. The
old plain Kafka consumer is still there: `--engine python` (or
`SCALABILITY_ENGINE=python`). Each run records its engine in `config_json`
(`ENGINE`).

**Photo source on the dashboard (since 11 October 2026): a folder you select.**
The page has a **Select folder** button that opens the Windows folder window;
there is no other choice there. On the command line, by default a test reads the farmer photos already stored locally in
`storage/downloaded_photos/<user_id>/<image_id>.jpg` (the copies Spark keeps of
every upload) and takes each photo's municipality and phone GPS from its
database row. Google Drive is never contacted. Alternatives: `--source-dir` (any
folder) or, only when asked for explicitly, `--from-drive` (the old Drive
collection, which downloads missing copies). Earlier Drive runs are replayed from
the same photos' local copies.

## How it is isolated

```
Google Drive images/<email>/ ─(list + verify)─► storage/downloaded_photos/<user>/<image_id>.jpg
   or a custom folder on this computer            (verified local copies)
                                                         │ hard link (copy across drives)
                                                         ▼
                              storage/scalability_reports/<run_id>/images/<item_id>.jpg
                                                         │
 scalability/run.py ── {run_id, item_id} events ──► Kafka topic maize-disease-images-scalability-test
                                                         │ the TEST's own Spark Structured Streaming query
                                                         │ (scalability/spark_engine.py, own checkpoint)
                                                         ▼
                      foreachBatch, SCALABILITY_WORKERS at a time (scalability/worker.process_photo):
                      prepare_image → CNN  (same code as the live Spark job)
                                                         │
                      scale_test_runs / scale_test_items (timings only), CSV files per run
```

| Shared with the live pipeline? | |
|---|---|
| Kafka topic | **No** — own topic (`SCALABILITY_KAFKA_TOPIC`); a guard refuses the production topic |
| Spark streaming job | **No** — the test runs its own Spark query (own SparkSession in the test process, own checkpoint); the live job reads only the live topic and skips any event with a `run_id` |
| `images`, `predictions`, `processing_events` | **No** — a test creates no row there; its items have fresh ids, never a farmer's image_id |
| Google Drive results, labelling folder, Tableau, dashboard farmer pages | **No** |
| CNN code and checkpoint | **Same code, own copy** — loaded into the runner process (extra GPU memory while it runs), warmed up before the timer |
| CPU, RAM, GPU of this laptop | **Yes** — see "Real-time priority" |
| PostgreSQL server | Yes, for the test's own two tables only |

### Real-time priority

The two workloads still share one laptop, so a test can slow live
processing. To give farmers priority:

* While any farmer photo is `INGESTED` or `PROCESSING` (ingested in the last
  `SCALABILITY_LIVE_WINDOW_MINUTES`, default 10), the test waits before it
  starts and starts no new photo during a run (`scalability/worker.LiveGate`).
  The page says the test is paused.
* The runner process runs at below-normal OS priority. GPU time cannot be
  prioritised this way.
* Photos measured while farmer photos were being processed, and iterations
  that overlapped such a period, are marked and **left out of the averages**.

## Running it

Dashboard → **Scalability test**:
1. **Select folder** (the Windows folder window opens) → *Check the folder*
2. choose the number of **unique photos** and the number of **iterations**
3. *Start test* (needs the Kafka broker)

The results show the **latest test** only:

* **Image comparison** — one photo of the test: *before preprocessing* (the
  staged original) next to *after preprocessing* (the bytes `prepare_image`
  produced for it during the run, through the model's own Resize +
  CenterCrop — the 224 × 224 picture the CNN received). Saved by the run
  itself, outside the timed stages. No disease names, labels, confidences or
  annotations.
* **Total Execution Time (seconds)** — from pressing *Start test* until
  the system has finished the last photo: preparing the photos, loading the
  CNN, starting the test's Spark job, Kafka, preprocessing, CNN and saving
  the results (`end_to_end_seconds`, counted from the runner process's
  start; for iteration 2 and later from that iteration's start). Time spent
  waiting before the start because farmer photos were being processed is
  not counted, and the gallery previews made after the last photo are not
  included. With several iterations their times are added. Shown only when
  every iteration finished with every photo processed and no overlap with
  farmer processing ("Not available" otherwise, and for tests from before
  11 October 2026). The photo-only timer — first event to the test topic →
  last photo (`execution_seconds`) — is still in `summary.csv`.
* **Average Processing Time (seconds/image)** = mean, over every validly
  measured photo of the test's finished iterations, of `time.perf_counter`
  around `prepare_image` + CNN inference only — no queue waiting, file
  reading, Google Drive or database time. Photos are processed several at a
  time, so this is not execution time ÷ photos.

No valid measurement → the page says so instead of showing a number.
Calculation: `scalability/report.series_averages`. Every other measurement
is in `storage/scalability_reports/<run_id>/`: `summary.csv`, `items.csv`
(per photo: `preprocess_s`, `cnn_s`, `processing_s`, `contended`),
`resources.csv` (CPU/RAM/GPU every second). All times are in seconds.

**Iterations and workload.** Iteration 1 picks N unique photos; iterations
2…K replay it (same photos, new item ids, their own run folder), so the
workload is N × K photo processings of N unique photos. Iterations never
share images or measurements.

From PowerShell in `A:\final\crop_disease_image_system`:

```powershell
python -m scalability.run preview                          # the photos stored on this laptop
python -m scalability.run start --size 100                 # one iteration, local photos (default)
python -m scalability.run start --size 100 --iterations 3  # same 100 photos, 3 timed iterations
python -m scalability.run start --size 5000 --source-dir D:\maize_photos
python -m scalability.run start --size 100 --from-drive    # only if you want the Google Drive collection
python -m scalability.run list
python -m scalability.run report --run-id <run_id>         # rewrite the CSV files
```

### Watching it live in a command prompt

Like the Spark and Kafka windows of `run_all.py`, a test can be watched as it runs.
From the dashboard, leave **Show the test live in a command prompt window** ticked: Start test
opens an `EWS-ScalabilityTest` window (it stays open until you press Enter). From a command prompt:

```
python -m scalability.run start --size 100 --iterations 3
python -m scalability.run start --size 10000 --live progress     # big runs: progress lines only
```

```
[scalability 17:40:48] [timer]    started — publishing 12 event(s) to Kafka topic 'maize-disease-images-scalability-test' (the live topic 'maize-disease-images' is not touched)
[scalability 17:40:47] [spark]    ready — micro-batches of up to 100 event(s) every 1s, 8 photo(s) at a time
[scalability 17:40:49] [spark]    micro-batch read 12 event(s) from Kafka (12 of 12 received)
[scalability 17:40:49] [cnn]       1/12  7c92a3ad  preprocess 0.323s  cnn 0.240s  -> northern_leaf_blight 84%  risk HIGH
[scalability 17:42:51] [progress] 24/60 done  11.55 photos/s  in workers 28  waiting in Kafka 0  elapsed 2.1s
[scalability 17:40:51] scale-20261010-174030-cf35: COMPLETED. 12 ok, 0 failed in 1.266s.
```

`--live photos` (default) shows a line per photo, `progress` only the Kafka/progress/pause lines,
`off` only the start and end. Watching changes nothing about the isolation above (own topic, own
consumer, pauses for farmer photos) and does not slow the measurement: the lines are written by the
bookkeeping loop at most every 0.5 s, never by the photo workers (60 photos: 3.53 s / 3.52 s with
`photos` vs 3.98 s / 3.69 s with `off`). The run's `config_json` records which mode was used
(`LIVE_OUTPUT`). `--log-file` keeps a copy of the window's output.

**Why the EWS-Spark window stays quiet during a test:** that window is the LIVE
Spark job, and the test never goes through it (its events are on their own topic
and read by the test's own Spark query, in the test window) — that is the
isolation. The **EWS-KafkaMonitor** window does show the test's Kafka traffic, as
one summary line per second:

```
[kafka 18:33:49] TEST topic 'maize-disease-images-scalability-test': +60 event(s) (offsets 60-119; 60 for scale-20261010-183344-72dc); 120 since start — read by the scalability test's own Spark query, not the live Spark job
```

### Photos from a custom folder

Any folder on this computer (dashboard: **Select folder**).
Every `.jpg`/`.jpeg`/`.png` in it and its subfolders is checked with the
upload rules; the same content under two names counts once; files are only
read. They are hard-linked into the run folder (no extra disk space). If the
folder is on another drive, where a hard link is impossible, the photos are
**read in place and never copied** (since 11 October 2026: copying a
6,977-photo folder from `C:` twice filled the 24 GB `A:` drive). Only the run's
one before-picture and the small gallery previews are written to `A:`.

## Results on this laptop (10 October 2026)

| Run | Photos | Execution time | Notes |
|---|---|---|---|
| 20 photos × 2 iterations | 40 | 8.7 s, 1.4 s | iteration 1 included loading the CNN; the model is now warmed up before the timer |
| 6,977 photos from a local folder | 6,977 | 937.8 s | 0 failures |

During the 20 × 2 run, the production tables (177 images, 54 predictions)
and the production topic's offset (524) were unchanged; the test topic
received the 40 events.

## Clean-up and replay

| Action | Removes | Keeps |
|---|---|---|
| Image management → Storage → staged photos of finished tests | the run's staged copies | measurements, CSV files, the before/after pair |
| **Delete test** (page) / `reset` | **every** scalability test — see below | everything that is not the test's |
| `delete --run-id X` | one run's measurements and its whole folder | Drive originals, local copies |
| `cleanup --run-id X` | staged copies (and, for runs from before 10 October, their old rows in the production tables) | measurements |
| **Replay** (`start --replay-of X`) | nothing | a new run with exactly the same photos |

The factory reset never deletes the scalability measurements.

### Delete test = complete reset (`scalability/reset.py`)

The page's **Delete test** button (tick "Yes, delete all scalability test data"
first) and `python -m scalability.run reset` return the scalability test to
its initial state. Every resource is found by a name that belongs only to
the test:

| Step | Removes | Identified by |
|---|---|---|
| Stop test processes | a running test and its own Spark JVM (child process) | command line `-m scalability.run start` / `prepare` |
| Kafka | the events of the test topic (the empty topic stays) and the test's consumer groups | `SCALABILITY_KAFKA_TOPIC`, `SCALABILITY_CONSUMER_GROUP*`; the production topic is refused |
| Database | `scale_test_runs`, `scale_test_items`, legacy test rows, the inactive test system account | the test tables; `images.source = 'scalability_test'` |
| Files | `scale-*` run folders (hard links, previews, CSV, Spark checkpoints), `logs/`, `_prepare_status.json`, `_lock.json`, the legacy `storage/images/scalability/` | the test folder `SCALABILITY_REPORTS_DIR` (refused if it is or contains a shared folder) |

**Never touched:** the live Kafka topic and the live Spark job, farmer
photos and `storage/downloaded_photos`, Google Drive, the model, the
training and review folders, `inference_benchmarks/`, and your own photo
folders. A hard link is only a directory entry, and photos read in place are
never inside the test folder, so originals cannot be deleted.

Every step runs even if another one fails. Afterwards the reset checks what
is left: the page shows **"Status: Ready — 0 test images, progress 0%"**
only when nothing of the test remains. Otherwise it shows which step failed
(for example a file that is open in another program). Every reset is
recorded in `storage_audit_log` (`action = scalability_reset`). Running it
again, or with no test at all, is harmless.

**Not removed (cannot be attributed to the test alone):** Spark's temporary
`spark-*` folders in `%TEMP%` (Spark removes its own on a normal stop, and the
live job uses the same folder), and gallery images your browser has cached.

Verified on 11 October 2026 against a running 1,000-photo Spark test, with
the live system running (Spark job, Drive watcher, Kafka monitor, dashboard).
The test runner and its Spark JVM were stopped, 1,001 test events were
deleted, and the run folder and rows were removed. The live Spark job and
its JVM kept running, the production topic stayed at 558 events, and the
images / predictions / users / image_reviews counts were unchanged.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| "Kafka is not reachable" | `docker compose up -d kafka` (the test never uses the local file-queue fallback) |
| page says the test is paused | farmer photos are being processed; it continues when they are done |
| "no valid timing measurement" | every iteration had a failure, did not finish, or overlapped live processing; run it again when the system is idle |
| "another scalability job is running" | one job at a time; a dead runner's lock clears itself |
| status INCOMPLETE | no photo finished for `--stall-minutes`; check the runner's log in `storage/scalability_reports/logs/` |
| run shows ABORTED | the runner process ended (closed window, reboot); start it again |
