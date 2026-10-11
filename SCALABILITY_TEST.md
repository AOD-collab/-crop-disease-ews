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

**Photo source (since 10 October 2026): the photos on this laptop, not Google
Drive.** By default a test reads the farmer photos already stored locally in
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
1. Photo source *Local photos on this laptop* (default) or *Custom folder* → *Check*
2. choose the number of **unique photos** and the number of **iterations**
3. *Start test* (needs the Kafka broker)

The results show the **latest test** only:

* **Image comparison** — one photo of the test: *before preprocessing* (the
  staged original) next to *after preprocessing* (the bytes `prepare_image`
  produced for it during the run, through the model's own Resize +
  CenterCrop — the 224 × 224 picture the CNN received). Saved by the run
  itself, outside the timed stages. No disease names, labels, confidences or
  annotations.
* **Average Execution Time (seconds)** = sum of the valid iterations'
  execution times ÷ number of valid iterations. One iteration runs
  (`time.monotonic`) from right before the first event is published to the
  test topic until the last photo is finished: test topic, reading each
  staged file, preprocessing, CNN and recording the measurement. Finding,
  downloading and staging photos, and loading the CNN, happen before the
  timer starts, and so does starting the test's Spark query (JVM, Kafka
  connection, one warm-up event through a first micro-batch), so the timer covers Spark's
  micro-batch scheduling but not its start-up. A *valid* iteration finished with every photo processed, no
  failure and no overlap with live farmer processing.
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

Any folder on this computer (dashboard: *Photo source → Custom folder*).
Every `.jpg`/`.jpeg`/`.png` in it and its subfolders is checked with the
upload rules; the same content under two names counts once; files are only
read. They are hard-linked into the run folder — **copied in full if the
folder is on another drive** (a 6,977-photo run from `C:` took 8.8 GB under
`A:\…\storage\scalability_reports`). Remove those copies afterwards with
Image management → Storage → "Staged photos of finished scalability tests"
(the measurements and the before/after pair are kept).

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
| **Delete test** (page) / `delete --run-id X` | the run's measurements and its whole folder | Drive originals, local copies |
| `cleanup --run-id X` | staged copies (and, for runs from before 10 October, their old rows in the production tables) | measurements |
| **Replay** (`start --replay-of X`) | nothing | a new run with exactly the same photos |

The factory reset never deletes the scalability measurements.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| "Kafka is not reachable" | `docker compose up -d kafka` (the test never uses the local file-queue fallback) |
| page says the test is paused | farmer photos are being processed; it continues when they are done |
| "no valid timing measurement" | every iteration had a failure, did not finish, or overlapped live processing; run it again when the system is idle |
| "another scalability job is running" | one job at a time; a dead runner's lock clears itself |
| status INCOMPLETE | no photo finished for `--stall-minutes`; check the runner's log in `storage/scalability_reports/logs/` |
| run shows ABORTED | the runner process ended (closed window, reboot); start it again |
