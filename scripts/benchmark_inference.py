"""
CNN inference benchmark (ml/inference.py) on this laptop's real hardware.

Uses the verified local copies of the Google Drive photo collection
(storage/downloaded_photos, prepared by `python -m scalability.run prepare`),
each put through the exact Spark preprocessing for the CNN (spark/
preprocessing.model_input), then measures:

  1. startup     model load + warm-up, reported apart from everything else
  2. cnn-only    forward passes on already-prepared tensors, batch sizes
                 1/8/16/32 x FP32/FP16 (CUDA-synchronised, model load and
                 warm-up excluded), peak GPU memory
  3. spark-like  N threads calling predict() at once, as the Spark job's
                 photo threads do: the original one-by-one path vs the shared
                 GPU batcher (MODEL_BATCH_SIZE), with CPU/GPU sampling
  4. agreement   every configuration against the ORIGINAL code path
                 (FP32, batch of 1, torch.no_grad, no pinned memory): class,
                 uncertain flag, risk level, confidence differences
  5. accuracy    optional, on a labelled folder (--labelled-dir with one
                 sub-folder per class)

Usage (PowerShell, from the project folder):
    python scripts\\benchmark_inference.py
    python scripts\\benchmark_inference.py --images 100 --repeats 3
    python scripts\\benchmark_inference.py --labelled-dir "C:\\Users\\user\\OneDrive\\Pictures\\crop_disease_ews\\data\\corn_maize"

Writes storage/scalability_reports/inference_benchmarks/<timestamp>/
results.json + report.md.
"""

import argparse
import json
import statistics
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import torch  # noqa: E402  - before pandas/pyarrow (WinError 1114 on Windows)

from config.settings import (  # noqa: E402
    DOWNLOADED_PHOTOS_DIR, IMAGE_MODEL, SCALABILITY_REPORTS_DIR,
)
from ml import inference  # noqa: E402
from spark.preprocessing import model_input  # noqa: E402
from ml.risk import compute_risk  # noqa: E402

FOLDER_ALIASES = {"blight": "northern_leaf_blight", "northern_leaf_blight": "northern_leaf_blight",
                  "common_rust": "common_rust", "gray_leaf_spot": "gray_leaf_spot", "healthy": "healthy"}


def sync():
    if torch.cuda.is_available():
        torch.cuda.synchronize()


def load_photos(n: int) -> list:
    files = sorted(p for p in Path(DOWNLOADED_PHOTOS_DIR).rglob("*") if p.suffix.lower() in (".jpg", ".png"))
    return files[:n]


def legacy_forward(loaded, tensor):
    """The ORIGINAL predict() forward pass, kept here verbatim as the
    baseline: one photo, FP32, torch.no_grad, plain .to(device)."""
    with torch.no_grad():
        logits = loaded["model"](tensor.unsqueeze(0).to(loaded["device"]))
        return torch.softmax(logits, dim=1)[0].cpu()


def summarize_result(loaded, probs) -> dict:
    r = inference._result_from_probs(loaded, probs)
    score, level = compute_risk(r, is_uncertain=r["is_uncertain"])
    return {"cls": r["predicted_class"], "unc": r["is_uncertain"], "conf": float(probs.max()),
            "lesion": r["lesion_score"], "risk": level, "risk_score": score}


def agreement(base: list, other: list) -> dict:
    n = len(base)
    conf = [abs(a["conf"] - b["conf"]) for a, b in zip(base, other)]
    lesion = [abs(a["lesion"] - b["lesion"]) for a, b in zip(base, other)]
    return {
        "class_agreement": round(sum(a["cls"] == b["cls"] for a, b in zip(base, other)) / n, 5),
        "class_changes": sum(a["cls"] != b["cls"] for a, b in zip(base, other)),
        "uncertain_flag_changes": sum(a["unc"] != b["unc"] for a, b in zip(base, other)),
        "risk_level_changes": sum(a["risk"] != b["risk"] for a, b in zip(base, other)),
        "conf_abs_diff_max": round(max(conf), 6), "conf_abs_diff_mean": round(statistics.fmean(conf), 7),
        "lesion_abs_diff_max": round(max(lesion), 6),
    }


def _reset_peak():
    if torch.cuda.is_available():
        torch.cuda.reset_peak_memory_stats()


def _peak_mb():
    """Peak GPU memory since the last _reset_peak(), in MB (None on a CPU)."""
    return round(torch.cuda.max_memory_allocated() / 2**20, 1) if torch.cuda.is_available() else None


def _forward_all(loaded, tensors, batch_size, precision, pin=None) -> list:
    """Class probabilities for every tensor, `batch_size` at a time."""
    probs = []
    for i in range(0, len(tensors), batch_size):
        probs.extend(inference._forward(loaded, torch.stack(tensors[i:i + batch_size]), precision, pin))
    return probs


def cnn_only(loaded, tensors, batch_size, precision, repeats, pin=True):
    """Steady-state forward-pass throughput over all tensors."""
    times = []
    for r in range(repeats):
        _reset_peak()
        sync()
        t = time.perf_counter()
        probs = _forward_all(loaded, tensors, batch_size, precision, pin)
        sync()
        times.append(time.perf_counter() - t)
    best = statistics.median(times)
    return probs, {"seconds_median": round(best, 4), "seconds_all": [round(x, 4) for x in times],
                   "images_per_s": round(len(tensors) / best, 1),
                   "ms_per_image": round(1000 * best / len(tensors), 3),
                   "peak_gpu_mem_mb": _peak_mb()}


def spark_like(prepped, threads, batch_size, precision, legacy=False):
    """`threads` workers each run predict() on their photos, like the Spark
    job's photo threads; returns (results in input order, stats)."""
    from scalability.monitor import ResourceMonitor, summarize
    IMAGE_MODEL["batch_size"], IMAGE_MODEL["precision"] = batch_size, precision
    loaded = inference._load_trained_model()
    loaded["batcher"] = None
    if legacy:
        def one(x):
            tensor = inference._as_input(loaded, x)
            t = time.perf_counter()
            p = legacy_forward(loaded, tensor)
            return p, time.perf_counter() - t, 1
    else:
        def one(x):
            t = time.perf_counter()
            p, size = inference._probabilities(loaded, x)
            return p, time.perf_counter() - t, size
    for x in prepped[:threads * 2]:          # warm the path (and the batcher thread)
        one(x)
    _reset_peak()
    mon = ResourceMonitor(interval=0.5).start()
    t = time.perf_counter()
    with ThreadPoolExecutor(threads) as pool:
        out = list(pool.map(one, prepped))
    wall = time.perf_counter() - t
    res = summarize(mon.stop())
    return [o[0] for o in out], {
        "wall_seconds": round(wall, 3), "images_per_s": round(len(prepped) / wall, 1),
        "cnn_call_ms_mean": round(1000 * statistics.fmean(o[1] for o in out), 2),
        "batch_mean": round(statistics.fmean(o[2] for o in out), 2), "batch_max": max(o[2] for o in out),
        "peak_gpu_mem_mb": _peak_mb(),
        "gpu_util_mean": res.get("gpu_util_percent_mean"), "gpu_util_peak": res.get("gpu_util_percent_peak"),
        "cpu_mean": res.get("cpu_percent_mean"), "cpu_peak": res.get("cpu_percent_peak"),
        "ram_peak_mb": res.get("memory_used_mb_peak"), "gpu_mem_used_peak_mb": res.get("gpu_memory_used_mb_peak"),
    }


def accuracy(loaded, folder: Path, precisions, batch_size) -> dict:
    items = []
    for sub in sorted(p for p in folder.iterdir() if p.is_dir()):
        cls = FOLDER_ALIASES.get(sub.name.lower())
        if cls is None or cls not in loaded["class_names"]:
            continue
        items += [(f, cls) for f in sorted(sub.iterdir()) if f.suffix.lower() in (".jpg", ".jpeg", ".png")]
    tensors = [inference._as_input(loaded, model_input(f.read_bytes())) for f, _ in items]
    out = {"images": len(items), "per_class_images": dict(Counter(c for _, c in items))}
    for prec in precisions:
        probs = _forward_all(loaded, tensors, batch_size, prec)
        pred = [loaded["class_names"][int(p.argmax())] for p in probs]      # raw top class
        truth = [c for _, c in items]
        per = {}
        for c in loaded["class_names"]:
            tp = sum(p == c and t == c for p, t in zip(pred, truth))
            fp = sum(p == c and t != c for p, t in zip(pred, truth))
            fn = sum(p != c and t == c for p, t in zip(pred, truth))
            prec_c = tp / (tp + fp) if tp + fp else 0.0
            rec_c = tp / (tp + fn) if tp + fn else 0.0
            per[c] = {"precision": round(prec_c, 4), "recall": round(rec_c, 4),
                      "f1": round(2 * prec_c * rec_c / (prec_c + rec_c), 4) if prec_c + rec_c else 0.0}
        present = [c for c in per if c in truth]
        out[prec] = {"accuracy": round(sum(p == t for p, t in zip(pred, truth)) / len(truth), 4),
                     "macro_f1": round(statistics.fmean(per[c]["f1"] for c in present), 4),
                     "per_class": per, "predictions": pred}
    if len(precisions) == 2:
        a, b = (out[p]["predictions"] for p in precisions)
        out["fp32_vs_fp16_prediction_changes"] = sum(x != y for x, y in zip(a, b))
    for p in precisions:
        out[p].pop("predictions")
    return out


def markdown(r: dict) -> str:
    L = [f"# CNN inference benchmark {r['started']}", "",
         f"Hardware: {r['device']}, torch {r['torch']}, CUDA {r['cuda']}. Model: `{r['model']}`. "
         f"{r['images']} real photos from the Drive collection (local copies), Spark preprocessing applied.", "",
         "## Startup (excluded from the timings below)", "",
         f"- model load: {r['startup']['model_load_s']} s, warm-up: {r['startup']['warmup_s']} s", "",
         "## Preprocessing (CPU, one thread, per photo)", "",
         f"- model_input (Spark: prepare_image, resize, centre crop, normalise): "
         f"{r['preprocess']['model_input_ms']} ms",
         f"- check + wrap as a tensor (ml/inference): {r['preprocess']['to_tensor_ms']} ms", "",
         "## CNN only (forward + copy to/from GPU + softmax, CUDA-synchronised)", "",
         "| Precision | Batch | Images/s | ms/image | Peak GPU MB | Class changes vs original | Max confidence diff |",
         "|---|---|---|---|---|---|---|"]
    o = r["cnn_only"]["original"]
    L.append(f"| original (fp32, no_grad) | 1 | {o['images_per_s']} | {o['ms_per_image']} | {o['peak_gpu_mem_mb']} | – | – |")
    for row in r["cnn_only"]["configs"]:
        L.append(f"| {row['precision']} | {row['batch_size']} | {row['images_per_s']} | {row['ms_per_image']} | "
                 f"{row['peak_gpu_mem_mb']} | {row['agreement']['class_changes']} | {row['agreement']['conf_abs_diff_max']} |")
    if r["cnn_only"].get("pinned_vs_not"):
        p = r["cnn_only"]["pinned_vs_not"]
        L += ["", f"Pinned memory at batch {p['batch_size']} ({p['precision']}): {p['pinned_images_per_s']} img/s "
                  f"pinned vs {p['unpinned_images_per_s']} img/s not pinned."]
    L += ["", f"## Spark-like: {r['threads']} photo threads calling predict() at once", "",
          "| Configuration | Images/s | predict() call ms (mean) | Batch mean/max | GPU util mean % | CPU mean % | Peak GPU MB | Class changes | Risk changes |",
          "|---|---|---|---|---|---|---|---|---|"]
    for row in r["spark_like"]:
        a = row.get("agreement") or {}
        L.append(f"| {row['name']} | {row['images_per_s']} | {row['cnn_call_ms_mean']} | {row['batch_mean']}/{row['batch_max']} | "
                 f"{row['gpu_util_mean']} | {row['cpu_mean']} | {row['peak_gpu_mem_mb']} | "
                 f"{a.get('class_changes', '–')} | {a.get('risk_level_changes', '–')} |")
    if r.get("accuracy"):
        a = r["accuracy"]
        L += ["", f"## Accuracy on labelled photos ({a['images']} images: {a['per_class_images']})", "",
              "**These folders were part of the model's training data, so this is not a held-out accuracy "
              "estimate. It only shows whether FP16 changes results compared with FP32.**", ""]
        for p in ("fp32", "fp16"):
            if p in a:
                L.append(f"- {p}: accuracy {a[p]['accuracy']}, macro F1 {a[p]['macro_f1']}")
        if "fp32_vs_fp16_prediction_changes" in a:
            L.append(f"- predictions that differ between FP32 and FP16: {a['fp32_vs_fp16_prediction_changes']}")
    return "\n".join(L) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--images", type=int, default=572)
    ap.add_argument("--batch-sizes", type=int, nargs="+", default=[1, 8, 16, 32])
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--threads", type=int, default=8, help="Spark-like photo threads (SPARK_PARALLEL_PHOTOS)")
    ap.add_argument("--labelled-dir", type=Path)
    ap.add_argument("--torch-threads", type=int, nargs="+", default=[0, 1],
                    help="PyTorch CPU threads for the spark-like runs (0 = PyTorch default)")
    args = ap.parse_args(argv)
    default_threads = torch.get_num_threads()

    started = datetime.now()
    t = time.perf_counter()
    loaded = inference._load_trained_model()
    load_s = time.perf_counter() - t
    precisions = ["fp32", "fp16"] if loaded["device"].type == "cuda" else ["fp32"]

    files = load_photos(args.images)
    if not files:
        sys.exit("No local copies found - run: python -m scalability.run prepare --size 100")
    t = time.perf_counter()
    prepped = [model_input(f.read_bytes()) for f in files]
    prep_ms = 1000 * (time.perf_counter() - t) / len(files)
    t = time.perf_counter()
    tensors = [inference._as_input(loaded, x) for x in prepped]
    tensor_ms = 1000 * (time.perf_counter() - t) / len(files)

    t = time.perf_counter()
    for prec in precisions:              # warm-up: cuDNN algorithm choice, CUDA context, autocast
        for bs in args.batch_sizes:
            inference._forward(loaded, torch.stack(tensors[:bs]), prec)
    for x in tensors[:5]:
        legacy_forward(loaded, x)
    sync()
    warm_s = time.perf_counter() - t

    print(f"{len(files)} photos, model_input {prep_ms:.1f} ms, to-tensor {tensor_ms:.1f} ms per photo")
    # 2. original + cnn-only grid
    _reset_peak()
    times = []
    for _ in range(args.repeats):
        sync()
        t = time.perf_counter()
        base_probs = [legacy_forward(loaded, x) for x in tensors]
        sync()
        times.append(time.perf_counter() - t)
    tb = statistics.median(times)
    original = {"seconds_median": round(tb, 4), "images_per_s": round(len(tensors) / tb, 1),
                "ms_per_image": round(1000 * tb / len(tensors), 3),
                "peak_gpu_mem_mb": _peak_mb()}
    base = [summarize_result(loaded, p) for p in base_probs]
    print(f"original: {original}")
    configs = []
    for prec in precisions:
        for bs in args.batch_sizes:
            probs, stats = cnn_only(loaded, tensors, bs, prec, args.repeats)
            stats.update(precision=prec, batch_size=bs,
                         agreement=agreement(base, [summarize_result(loaded, p) for p in probs]))
            configs.append(stats)
            print(f"cnn-only {prec} bs={bs}: {stats['images_per_s']} img/s, peak {stats['peak_gpu_mem_mb']} MB, "
                  f"class changes {stats['agreement']['class_changes']}")
    best = max(configs, key=lambda c: c["images_per_s"])
    _, pinned = cnn_only(loaded, tensors, best["batch_size"], best["precision"], args.repeats, pin=True)
    _, unpinned = cnn_only(loaded, tensors, best["batch_size"], best["precision"], args.repeats, pin=False)

    # 3. spark-like
    saved = dict(IMAGE_MODEL)
    spark_rows = []
    try:
        runs = []
        for tt in args.torch_threads:
            label = f"torch threads {tt or default_threads}"
            runs += [(f"original (each thread, bs 1, fp32), {label}", 1, "fp32", True, tt)]
            runs += [(f"batcher bs {bs} {prec}, {label}", bs, prec, False, tt)
                     for prec in precisions for bs in args.batch_sizes if bs > 1]
        for name, bs, prec, legacy, tt in runs:
            torch.set_num_threads(tt or default_threads)
            probs, stats = spark_like(prepped, args.threads, bs, prec, legacy)
            stats.update(name=name, batch_size=bs, precision=prec, torch_threads=tt or default_threads,
                         agreement=agreement(base, [summarize_result(loaded, p) for p in probs]))
            spark_rows.append(stats)
            print(f"spark-like {name}: {stats['images_per_s']} img/s, batch mean {stats['batch_mean']}, "
                  f"GPU {stats['gpu_util_mean']}%, class changes {stats['agreement']['class_changes']}")
    finally:
        IMAGE_MODEL.clear()
        IMAGE_MODEL.update(saved)
        loaded["batcher"] = None
        torch.set_num_threads(default_threads)

    result = {
        "started": started.isoformat(timespec="seconds"), "images": len(files), "threads": args.threads,
        "device": inference._device_name(torch, loaded["device"]), "torch": torch.__version__,
        "cuda": torch.version.cuda, "model": Path(IMAGE_MODEL["weights_path"]).name,
        "startup": {"model_load_s": round(load_s, 3), "warmup_s": round(warm_s, 3)},
        "preprocess": {"model_input_ms": round(prep_ms, 2), "to_tensor_ms": round(tensor_ms, 2)},
        "cnn_only": {"original": original, "configs": configs,
                     "pinned_vs_not": {"batch_size": best["batch_size"], "precision": best["precision"],
                                       "pinned_images_per_s": pinned["images_per_s"],
                                       "unpinned_images_per_s": unpinned["images_per_s"]}},
        "spark_like": spark_rows,
    }
    if args.labelled_dir:
        result["accuracy"] = accuracy(loaded, args.labelled_dir, precisions, 16)
        print(f"accuracy: { {p: result['accuracy'][p]['accuracy'] for p in precisions} }")

    out = SCALABILITY_REPORTS_DIR / "inference_benchmarks" / started.strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    (out / "report.md").write_text(markdown(result), encoding="utf-8")
    print(f"\nReport: {out / 'report.md'}")


if __name__ == "__main__":
    main()
