# CNN inference optimisation — measurements (2026-10-09)

Hardware: NVIDIA GeForce RTX 3050 6GB Laptop GPU, 16 logical CPU cores, 15.2 GB RAM, PyTorch 2.13 + CUDA 13.0.
Model: `ml/trained_model/saved_model_cnn.pt` (SE-ResNet-18, 4 classes, 224×224, ImageNet mean/std).
Data: the 572 valid photos of the Google Drive collection, read from their verified local copies and
put through the exact Spark preprocessing (`spark/preprocessing.prepare_image`). Every number below
was measured. Raw output is in `storage/scalability_reports/inference_benchmarks/20261009-211118/` and in
the scalability runs listed at the end.

## 1. What was slow (assessment)

| Finding | Evidence |
|---|---|
| The model already ran on the GPU in `eval()` mode, was loaded once per process (cached, hot-reloaded on deploy) and used `torch.no_grad()`. | `ml/inference.py` before the change |
| A single batch-of-1 forward pass takes ~19 ms. That is mostly kernel-launch overhead, not GPU arithmetic. | CNN-only, batch 1: 53.6 img/s |
| **Inside Spark, 8 photo threads called the model at the same time** and waited for each other (GPU queue + Python GIL). The same forward took 128 ms instead of 18 ms. | 8 threads vs 1 thread, same photos |
| PyTorch's CPU thread pool (one thread per core) ran **in each of the 8 photo threads** → CPU oversubscription. | CPU 83–93% with no extra throughput |
| `disease_heatmap()` decoded and transformed every photo a second time. | code |
| **The real ceiling of the whole pipeline is CPU work before the CNN**: `prepare_image` (decode the phone photo, make it upright, ≤1280 px, re-encode) is ~136 ms per photo and manages only ~30 photos/s with 8 threads. It must stay unchanged, because changing it changes the pixels the model sees. | spark-like test with `prepare_image` included: ~21 img/s in every CNN configuration |

## 2. What changed

| File | Change |
|---|---|
| `ml/inference.py` | `torch.inference_mode()`. One shared `_forward()` for every path (FP32, or FP16 autocast with the unchanged weights, softmax always in FP32). Optional **GPU batcher**: Spark's photo threads hand their prepared tensor to one worker thread that runs one batched forward pass, and each photo gets its own row back. **Out-of-memory handling**: the batch is split in halves, later batches are kept smaller, and a single photo that still doesn't fit raises the error. The prepared input is reused by `disease_heatmap()` (same bytes → same tensor). PyTorch CPU threads are set once on GPU. `predict()` also returns `inference` = batch size, precision and timings. |
| `config/settings.py` | `MODEL_BATCH_SIZE` (1), `MODEL_BATCH_WAIT_MS` (5), `MODEL_PRECISION` (fp32), `MODEL_PIN_MEMORY` (false), `MODEL_TORCH_THREADS` (1) |
| `spark/preprocessing.py` | records the batch size, precision, input time and forward time each photo actually used (`timings`) |
| `scalability/report.py`, `dashboard/scalability_page.py` | the run report and comparison table show CNN precision, batch size, input/forward split |
| `scripts/benchmark_inference.py` (new) | this benchmark |
| `tests/test_inference_speed.py` (new) | mapping under concurrency, FP16 vs FP32, CPU fallback, out-of-memory split, heatmap input reuse |

Unchanged: the model, its weights, the class list, the transforms (Resize 224 → CenterCrop 224 → ToTensor →
Normalize), `prepare_image`, the confidence threshold and risk rules, the Kafka event format, the Spark job
structure, PostgreSQL records, Drive behaviour and the Android app. There is one CNN implementation, used by
both the live flow and the scalability test.

## 3. Results

Startup (excluded from all timings): model load 4.59 s, warm-up 1.94 s.
Preprocessing, one thread: `prepare_image` 135.8 ms/photo; decode + model transforms 22.8 ms/photo.

### CNN only: forward pass + copy to/from GPU + softmax, CUDA-synchronised, 572 photos, median of 3

| Precision | Batch | Images/s | ms/image | Peak GPU MB | Class / risk / uncertain changes vs original | Max confidence diff |
|---|---|---|---|---|---|---|
| original (fp32, no_grad) | 1 | 53.6 | 18.7 | 62 | – | – |
| fp32 | 1 | 50.9 | 19.6 | 62 | 0 / 0 / 0 | 0.0 |
| fp32 | 8 | 99.2 | 10.1 | 105 | 0 / 0 / 0 | 0.0010 |
| fp32 | 16 | 104.4 | 9.6 | 160 | 0 / 0 / 0 | 0.0008 |
| fp32 | 32 | 108.1 | 9.3 | 266 | 0 / 0 / 0 | 0.0009 |
| fp16 | 1 | 45.0 | 22.2 | 62 | 0 / 0 / 0 | 0.0021 |
| fp16 | 8 | 159.3 | 6.3 | 83 | 0 / 0 / 0 | 0.0018 |
| **fp16** | **16** | **174.2** | **5.7** | 116 | 0 / 0 / 0 | 0.0020 |
| fp16 | 32 | 166.9 | 6.0 | 190 | 0 / 0 / 0 | 0.0018 |

Pinned memory at batch 16 / fp16: 174.6 img/s pinned vs 175.9 not pinned → no benefit, so it's off by default.

### CNN stage under Spark-like load: 8 threads calling `predict()` at once, 572 photos

| Configuration | PyTorch CPU threads | Images/s | CPU mean % | Class changes |
|---|---|---|---|---|
| original (each thread on its own, batch 1, fp32) | 8 | 49.7 | 92.9 | – |
| original | 1 | 49.5 | 41.9 | 0 |
| batcher 8, fp32 | 1 | 58.6 | 44.7 | 0 |
| batcher 8, fp16 | 1 | 78.5 | 25.9 | 0 |
| batcher 16, fp16 | 1 | 78.7 | 28.1 | 0 |
| batcher 32, fp16 | 1 | 80.2 | 24.7 | 0 |

With Spark's own `prepare_image` added to each thread (300 photos), every configuration landed at
20–23 img/s. A TorchScript-traced copy of the model was also tried (max probability difference 7.6e-5,
0 class changes), with no consistent gain, so it was not adopted.

### Full pipeline: Kafka → Spark → CNN → PostgreSQL, same 572 photos

The first run after a Spark restart is consistently slower (cold), so both runs are shown.

| Configuration | Cold run img/s | Warm run img/s | CPU mean % (warm) | Peak GPU MB | Stored predictions vs original code |
|---|---|---|---|---|---|
| original code (before this change) | – | 10.97 / 11.53 | 85 / 82 | 417 | – |
| new code, batch 1, fp32, PyTorch threads 8 (its default) | 9.33 / 8.83 | 10.80 | 83 | 415 | – |
| new code, batch 8, fp32, threads 1 | 9.31 | – | 50 (cold) | 311 | 0 class / 0 risk changes, conf ≤ 0.001 |
| new code, batch 8, fp16, threads 1 | 9.39 / 9.19 | 11.54 | 45 | 257 | 0 class / 0 risk changes, conf ≤ 0.002 |
| **new defaults: batch 1, fp32, threads 1** | 9.25 | **11.80** | **45** | 417 | **identical** (conf diff 0.000) |

Inside Spark the CNN step stayed ~0.18 s per photo in every configuration: ~0.06 s input decode/transform
and ~0.11 s waiting for the forward pass, even with batching. The photo threads are held up by CPU work and
the GIL, not by the GPU (GPU utilisation 16–28%).

### Accuracy on labelled photos

`crop_disease_ews/data/corn_maize`: 103 photos (30 northern leaf blight, 19 common rust, 6 gray leaf spot,
48 healthy). FP32: accuracy 0.8641, macro F1 0.7628. FP16: accuracy 0.8641, macro F1 0.7628,
0 predictions different. **These photos were part of the training data, so this is not a held-out
accuracy figure.** It only shows that FP16 did not change any result on them. No held-out labelled set
exists in the project.

## 4. Recommendation

* **Live system (defaults, active now):** batch 1, FP32, `MODEL_TORCH_THREADS=1`. Predictions are
  identical to before. CPU use roughly halves (82–85% → 45%) at the same throughput, which leaves room for
  the dashboard, PostgreSQL and the Drive watcher on the same laptop.
* **CNN-heavy batch work / benchmarking the CNN itself:** `MODEL_BATCH_SIZE=16`, `MODEL_PRECISION=fp16`.
  That is 3.2× the original CNN-only throughput (174 vs 54 img/s), 116 MB GPU memory, with no class, risk or
  uncertainty change on 572 + 103 photos (confidence within 0.002). In the full Spark pipeline it gave no
  measurable gain over the defaults (11.54 vs 11.80 img/s), so FP16 is **not** switched on by default.
  Inside Spark, batches rarely exceed the 8 photo threads, so 8 and 16 behave the same.
* **What would make the full pipeline faster** is the CPU side. Run the preprocessing outside the
  Python GIL, e.g. Spark executors or a process pool per photo. That is an architecture change to the Spark
  job, so it is not done here. TensorRT / Torch-TensorRT could speed up the forward pass further, but the
  forward pass is no longer what photos wait on, so it is an optional later step.
* **10,000 images:** the collection holds 572 valid photos. 1,000 and 10,000 could not be measured
  without inventing data (see SCALABILITY_TEST.md).

## 5. Commands (PowerShell, project folder)

```powershell
python -m scalability.run prepare --size 572           # verified local copies (once)
python scripts\benchmark_inference.py                  # all configurations, 572 photos
python scripts\benchmark_inference.py --images 100 --repeats 3
python scripts\benchmark_inference.py --labelled-dir "C:\Users\user\OneDrive\Pictures\crop_disease_ews\data\corn_maize"
python -m pytest tests\test_inference_speed.py -q
```

A full-pipeline run with other inference settings needs the Spark job restarted with them, e.g. in its window:

```powershell
$env:MODEL_BATCH_SIZE = "16"; $env:MODEL_PRECISION = "fp16"; python -m spark.streaming_job
python -m scalability.run start --replay-of <run_id>    # same photos as an earlier run
```

or put the variables in `.env` and restart with `python run_all.py --stop` then `python run_all.py`. Do one
throw-away run after each restart: the first run is cold.

## 6. Troubleshooting

| Problem | What to do |
|---|---|
| Log says "MODEL_DEVICE=cuda but PyTorch sees no CUDA GPU" | this Python has the CPU build of torch: `pip install --force-reinstall --no-deps torch==2.13.0 torchvision==0.28.0 --index-url https://download.pytorch.org/whl/cu130`. Check with `python -c "import torch; print(torch.cuda.is_available())"` |
| `CUDA out of memory` | the batcher halves the batch automatically and logs "batches are now at most N". Lower `MODEL_BATCH_SIZE`, close other GPU programs (training, games), check `nvidia-smi` |
| `OSError: [WinError 1114]` loading torch | torch must be imported before pandas/pyarrow. The Spark job and the benchmark already do this |
| FP16 gives an error for an operation | set `MODEL_PRECISION=fp32` (the fallback). Grad-CAM always runs in FP32 |
| `CUBLAS_STATUS_ALLOC_FAILED` / driver errors | update the NVIDIA driver, reboot, run `nvidia-smi`; as a stop-gap `MODEL_DEVICE=cpu` |
| Benchmark numbers jump between runs | close other GPU/CPU-heavy programs, keep the laptop plugged in, and ignore the first run after a restart |
