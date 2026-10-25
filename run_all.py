"""
Starts the whole image system, each part in its own window: Kafka (Docker),
the Spark streaming job, Spark analytics, the dashboard and the Drive watcher.
Settings come from .env.

    python run_all.py                  # start everything
    python run_all.py --no-kafka       # a broker is already running
    python run_all.py --no-spark       # skip Spark (and its analytics)
    python run_all.py --no-analytics   # skip only the Spark analytics
    python run_all.py --no-drive       # skip the Drive watcher
    python run_all.py --reset          # factory reset first (keeps users)
    python run_all.py --stop           # stop everything it started
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from shutil import which

ROOT = Path(__file__).resolve().parent
PID_FILE = ROOT / ".run_all.pids.json"
IS_WINDOWS = sys.platform == "win32"


# ---------------------------------------------------------------------------
# process helpers
# ---------------------------------------------------------------------------
def start_service(name: str, argv: list[str]) -> dict:
    """Starts `argv` in its own console window; returns {name, pid}."""
    if IS_WINDOWS:
        # a titled window that stays open after a crash; taskkill /T later stops the child too
        inner = " ".join(argv)
        proc = subprocess.Popen(
            f'cmd /k "title {name} && {inner}"',
            cwd=ROOT,
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
    else:
        proc = subprocess.Popen(argv, cwd=ROOT, start_new_session=True)
    print(f"  started {name:<14} pid {proc.pid}")
    return {"name": name, "pid": proc.pid}


def pid_alive(pid: int) -> bool:
    if IS_WINDOWS:
        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True,
        ).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def stop_pid(pid: int) -> None:
    if IS_WINDOWS:
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                       capture_output=True)
    else:
        import signal
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except OSError:
            pass


def read_pid_file() -> dict | None:
    """The saved {kafka, services} record, or None if missing or damaged."""
    try:
        saved = json.loads(PID_FILE.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None
    return saved if isinstance(saved, dict) else None


def find_service_windows() -> list[dict]:
    """Windows: the "EWS-<name>" windows, found by title (when the pid file is unreadable)."""
    if not IS_WINDOWS:
        return []
    out = subprocess.run(
        ["powershell", "-NoProfile", "-Command",
         "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe'\" | "
         "Where-Object CommandLine -match 'title EWS-' | "
         "ForEach-Object { \"$($_.ProcessId) $($_.CommandLine)\" }"],
        capture_output=True, text=True,
    ).stdout
    found = []
    for line in out.splitlines():
        pid, _, cmdline = line.strip().partition(" ")
        if pid.isdigit() and "title EWS-" in cmdline:
            name = cmdline.split("title ", 1)[1].split()[0].strip('"&')
            found.append({"name": name, "pid": int(pid)})
    return found


def spark_streaming_alive() -> bool:
    """Is the Spark job's Python process really running? (The saved pid is
    its cmd window, which stays alive after a crash.)"""
    from storage.reset import spark_job_running
    running = spark_job_running()
    return True if running is None else running  # can't tell — don't cry wolf


def count_stuck_images() -> dict | None:
    """{"waiting", "undelivered"} photos older than 2 minutes, or None if the
    database can't be read."""
    try:
        from storage.database import SessionLocal
        from storage.requeue_stuck import count_owed
        with SessionLocal() as s:
            return count_owed(s)
    except Exception:
        return None


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def wait_for_port(port: int, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if port_in_use(port):
            return True
        time.sleep(2)
    return False


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--no-kafka", action="store_true", help="don't start a Kafka broker (one is already running elsewhere)")
    p.add_argument("--no-spark", action="store_true", help="don't start the Spark streaming job (or the Spark analytics)")
    p.add_argument("--no-analytics", action="store_true",
                   help="don't start the Spark analytics service (EWS-SparkAnalytics, the dashboard's figures)")
    p.add_argument("--no-dashboard", action="store_true", help="don't start the Streamlit dashboard")
    p.add_argument("--no-mail", action="store_true", help=argparse.SUPPRESS)  # old flag: accepted, does nothing
    p.add_argument("--no-drive", action="store_true", help="don't start the Google Drive -> Kafka watcher")
    p.add_argument("--no-kafka-monitor", action="store_true",
                   help="don't open the read-only EWS-KafkaMonitor window (live view of the Kafka topic)")
    p.add_argument("--reset", action="store_true", help="factory-reset pipeline data first (keeps users/admins)")
    p.add_argument("--stop", action="store_true", help="stop everything a previous run started")
    args = p.parse_args()

    # ---- stop mode -------------------------------------------------------
    if args.stop:
        if not PID_FILE.exists():
            print("Nothing to stop - no .run_all.pids.json found.")
            return 0
        saved = read_pid_file()
        if saved is None:
            print(".run_all.pids.json is empty or damaged - looking for the EWS-* service windows instead.")
            saved = {"kafka": False, "services": find_service_windows()}
        for s in saved.get("services", []):
            if pid_alive(s["pid"]):
                stop_pid(s["pid"])
                print(f"stopped {s['name']} (pid {s['pid']})")
            else:
                print(f"{s['name']} (pid {s['pid']}) was not running")
        if saved.get("kafka"):
            print("stopping Kafka broker (docker compose down)...")
            subprocess.run(["docker", "compose", "down"], cwd=ROOT, capture_output=True)
        PID_FILE.unlink()
        print("All stopped.")
        return 0

    # ---- guard against a double start ----------------------------------
    if PID_FILE.exists():
        prev = read_pid_file() or {"services": find_service_windows()}
        alive = [s for s in prev.get("services", []) if pid_alive(s["pid"])]
        if alive:
            names = ", ".join(s["name"] for s in alive)
            print(f"Some components are already running ({names}). Run 'python run_all.py --stop' first.")
            return 1
        PID_FILE.unlink()  # stale file, all dead

    # ---- dependency sanity check --------------------------------------
    if subprocess.run([sys.executable, "-c", "import streamlit, sqlalchemy"]).returncode != 0:
        print("Python dependencies are missing. Run:  pip install -r requirements.txt")
        return 1

    # ---- 1. Kafka broker -------------------------------------------
    started_kafka = False
    if not args.no_kafka:
        if port_in_use(9092):
            print("Kafka already reachable on localhost:9092 - not starting another broker.")
        elif not which("docker"):
            print("Kafka: 'docker' not on PATH - skipping. Start a broker yourself, or pass --no-kafka.")
        else:
            print("Starting Kafka broker (docker compose up -d kafka)...")
            if subprocess.run(["docker", "compose", "up", "-d", "kafka"], cwd=ROOT).returncode != 0:
                print("docker compose failed (is Docker running?) - continuing without a broker.")
            else:
                print("Waiting for Kafka on localhost:9092...")
                started_kafka = wait_for_port(9092, 60)
                print("Kafka is up." if started_kafka
                      else "Kafka did not come up within 60s; continuing anyway.")
    elif not args.no_spark:
        print("Note: --no-kafka set but Spark is on. The Spark job needs a broker at")
        print("      KAFKA_BOOTSTRAP_SERVERS or it will error out.")

    # ---- 1b. PostgreSQL (only when DATABASE_URL uses it) --------------
    from config.settings import DATABASE_URL
    use_pg = DATABASE_URL.startswith("postgresql")
    if use_pg:
        if port_in_use(5432):
            print("PostgreSQL already reachable on localhost:5432.")
        elif not which("docker"):
            print("PostgreSQL: 'docker' not on PATH - start PostgreSQL yourself on localhost:5432.")
        else:
            print("Starting PostgreSQL (docker compose up -d postgres)...")
            if subprocess.run(["docker", "compose", "up", "-d", "postgres"], cwd=ROOT).returncode != 0:
                print("Could not start PostgreSQL - check Docker and your internet "
                      "(first time: `docker pull postgres:16`).")
                return 1
            print("Waiting for PostgreSQL on localhost:5432...")
            if not wait_for_port(5432, 60):
                print("PostgreSQL did not come up within 60s - aborting.")
                return 1
            time.sleep(3)  # the port opens a moment before it accepts logins
            print("PostgreSQL is up.")
    else:
        print("Database: SQLite file (storage/app.db) - no database server needed.")

    # ---- 2. DB init (+ optional reset), inline ----------------------
    print("Initializing database (python -m storage.init_db)...")
    if subprocess.run([sys.executable, "-m", "storage.init_db"], cwd=ROOT).returncode != 0:
        print("init_db failed - aborting.")
        return 1
    if args.reset:
        print("Factory-resetting pipeline data (keeps users/admins)...")
        # also empty the Kafka topic, or Spark replays every old message
        reset_cmd = [sys.executable, "-m", "storage.reset", "--yes"]
        if not args.no_kafka:
            reset_cmd.append("--kafka")
        if subprocess.run(reset_cmd, cwd=ROOT).returncode != 0:
            print("reset failed - aborting.")
            return 1

    # ---- 3. launch the long-running components ---------------------
    print("\nLaunching components:")
    services: list[dict] = []

    if not args.no_spark:
        services.append(start_service("EWS-Spark", ["python", "-m", "spark.streaming_job"]))

    if not (args.no_spark or args.no_analytics):
        if use_pg:
            services.append(start_service("EWS-SparkAnalytics", ["python", "-m", "spark.batch_analytics", "--watch"]))
        else:
            print("  skipped Spark analytics - it reads the database over JDBC, which needs PostgreSQL "
                  "(DATABASE_URL); the dashboard's charts stay empty on SQLite")

    if not args.no_kafka_monitor:
        services.append(start_service("EWS-KafkaMonitor", ["python", "-m", "kafka.monitor"]))

    if not args.no_dashboard:
        if port_in_use(8501):
            print("  skipped Dashboard  - port 8501 already in use (something already running there?)")
        else:
            services.append(start_service("EWS-Dashboard",
                                          ["python", "-m", "streamlit", "run", "dashboard/app.py"]))

    if not args.no_drive:
        from storage import drive_client
        if drive_client.is_configured():
            services.append(start_service("EWS-DriveWatcher", ["python", "-m", "kafka.drive_watcher"]))
        else:
            print("  skipped Drive watcher - Google Drive not set up yet "
                  "(run: python scripts/drive_authorize.py, see README 'Google Drive setup')")

    if not services:
        print("Nothing was started.")
        return 0

    PID_FILE.write_text(json.dumps({"kafka": started_kafka, "services": services}, indent=2))

    # ---- 3b. verify the Spark consumer actually came up ----------------
    # a dead consumer leaves uploads stuck at INGESTED, so say so now
    if not args.no_spark:
        print("\nWaiting for the Spark streaming job to come up...")
        alive = False
        for _ in range(15):  # ~30s; first run also resolves the kafka package
            time.sleep(2)
            if spark_streaming_alive():
                alive = True
                break
        if alive:
            print("  Spark streaming job is running.")
        else:
            print("  WARNING: no live 'python -m spark.streaming_job' process detected.")
            print("           Check the EWS-Spark console window for a traceback.")
            print("           While it's down, uploads will pile up at status INGESTED.")

    # ---- 3c. flag anything already stuck ingested-but-not-processed ---
    stuck = count_stuck_images()
    if stuck and (stuck["waiting"] or stuck["undelivered"]):
        print(f"\n{stuck['waiting']} image(s) are waiting but not processed, and "
              f"{stuck['undelivered']} finished result(s) never reached Google Drive. To clear them:")
        print("  python -m storage.requeue_stuck --all            # re-publish to Kafka (Spark must be up)")
        print("  python -m storage.requeue_stuck --all --inline    # or process them now, in-process")
        print("  python -m storage.requeue_stuck --from-drive      # Drive photos Spark never registered")

    # ---- 4. report ------------------------------------------------
    print("\nUp. Open:")
    if not args.no_dashboard:
        print("  Dashboard  http://localhost:8501")
    print("  Android    talks only to Google Drive - no ngrok or port forwarding needed")

    print("\nStop everything with:  python run_all.py --stop")
    return 0


if __name__ == "__main__":
    sys.exit(main())
