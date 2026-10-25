"""
Resource sampling during a scalability run's measured stage: whole-machine
CPU and memory (psutil), the Spark job's own memory (its Python driver plus
the JVM it starts), and the NVIDIA GPU's utilisation and memory (nvidia-smi,
when present). One sample per `interval` seconds, all real readings.
"""

import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone

import psutil

FIELDS = ["t_seconds", "timestamp_utc", "cpu_percent", "memory_used_mb", "memory_percent",
          "spark_rss_mb", "gpu_util_percent", "gpu_memory_used_mb", "gpu_memory_total_mb"]


def spark_processes() -> list:
    """The running spark.streaming_job Python process(es) and their child
    processes (the JVM)."""
    found = []
    for p in psutil.process_iter(["cmdline"]):
        try:
            cmd = " ".join(p.info["cmdline"] or [])
        except (psutil.Error, TypeError):
            continue
        if "spark.streaming_job" in cmd or "streaming_job.py" in cmd:
            found.append(p)
            try:
                found.extend(p.children(recursive=True))
            except psutil.Error:
                pass
    return found


def spark_job_running() -> bool:
    return bool(spark_processes())


def _nvidia_smi(*args, parse=lambda out: out):
    """parse(nvidia-smi's stripped output), or None without nvidia-smi or on any error."""
    exe = shutil.which("nvidia-smi")
    if not exe:
        return None
    try:
        return parse(subprocess.run([exe, *args], capture_output=True, text=True, timeout=5,
                                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)).stdout.strip())
    except Exception:  # noqa: BLE001
        return None


def gpu_reading():
    """(util %, memory used MB, memory total MB) of GPU 0, or None."""
    return _nvidia_smi("--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits",
                       parse=lambda out: tuple(float(x) for x in out.splitlines()[0].split(",")))


def gpu_name():
    return _nvidia_smi("--query-gpu=name", "--format=csv,noheader", parse=lambda out: out or None)


class ResourceMonitor:
    """Samples in a background thread between start() and stop()."""

    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.samples = []
        self._stop = threading.Event()
        self._thread = None
        self._t0 = None

    def start(self):
        self._t0 = time.monotonic()
        primed = threading.Event()
        self._thread = threading.Thread(target=self._loop, args=(primed,), name="resource-monitor", daemon=True)
        self._thread.start()
        primed.wait(5)
        return self

    def stop(self):
        """Stops sampling and takes one last sample, so even a run shorter
        than `interval` has a real reading."""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        return self.samples

    def _loop(self, primed):
        # psutil.cpu_percent(None) measures since the previous call IN THE
        # SAME THREAD (psutil 7): priming it in start()'s thread left the first
        # sample here always 0.0 — which made every run's mean CPU too low,
        # and a run under 2 s report 0.
        psutil.cpu_percent(None)
        primed.set()
        self._procs, self._procs_checked = [], 0.0
        while not self._stop.wait(self.interval):
            self._sample()
        self._sample()

    def _sample(self):
        now = time.monotonic()
        if now - self._procs_checked > 15:      # the JVM can start/stop; re-find every 15 s
            self._procs, self._procs_checked = spark_processes(), now
        rss = 0
        for p in self._procs:
            try:
                rss += p.memory_info().rss
            except psutil.Error:
                pass
        mem = psutil.virtual_memory()
        gpu = gpu_reading()
        self.samples.append({
            "t_seconds": round(now - self._t0, 2),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "cpu_percent": psutil.cpu_percent(None),
            "memory_used_mb": round((mem.total - mem.available) / 2**20, 1),
            "memory_percent": mem.percent,
            "spark_rss_mb": round(rss / 2**20, 1) if self._procs else None,
            **dict(zip(("gpu_util_percent", "gpu_memory_used_mb", "gpu_memory_total_mb"), gpu or (None,) * 3)),
        })


def summarize(samples: list) -> dict:
    def stat(key):
        vals = [s[key] for s in samples if s.get(key) is not None]
        if not vals:
            return None, None
        return round(sum(vals) / len(vals), 1), round(max(vals), 1)

    out = {"resource_samples": len(samples)}
    for key in ("cpu_percent", "memory_used_mb", "memory_percent", "spark_rss_mb",
                "gpu_util_percent", "gpu_memory_used_mb"):
        out[f"{key}_mean"], out[f"{key}_peak"] = stat(key)
    return out
