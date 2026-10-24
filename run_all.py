"""
One-shot launcher for the Crop Disease Early Warning System (image system).

Initializes the database, then starts these long-running components, each in
its own console window:
    * Kafka broker        docker compose up -d             localhost:9092
    * Spark streaming job  python -m spark.streaming_job
    * Spark analytics      python -m spark.batch_analytics --watch  (the dashboard's figures; PostgreSQL)
    * Admin dashboard      streamlit run dashboard/app.py   http://localhost:8501
    * Drive watcher        python -m kafka.drive_watcher    (Google Drive /images -> Kafka)

The Android app talks only to Google Drive; the Drive watcher is what
brings its uploads into Kafka. It is skipped (with a message) until
`python scripts/drive_authorize.py` has been run once — see README "Google Drive setup".

There is no server of ours for the phone to reach, so no public address,
ngrok or port forwarding is needed.

Config (BOOTSTRAP_ADMIN_PASSWORD, PYSPARK_PYTHON, ...) is read from the
.env file in this folder by config/settings.py — edit that, not this.

Examples
--------
    python run_all.py                     # DB init + Kafka + Spark + dashboard + Drive watcher
    python run_all.py --no-kafka          # skip starting a broker (one already running / started elsewhere)
    python run_all.py --no-spark          # skip the Spark streaming job (and the Spark analytics)
    python run_all.py --no-analytics      # skip only the Spark analytics service
    python run_all.py --no-drive          # skip the Google Drive watcher
    python run_all.py --reset             # factory-reset pipeline data + Kafka messages first (keeps users/admins)
    python run_all.py --stop              # stop everything this script started
"""

import argparse
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PID_FILE = ROOT / ".run_all.pids.json"
IS_WINDOWS = sys.platform == "win32"


# ---------------------------------------------------------------------------
# process helpers
# ---------------------------------------------------------------------------
def start_service(name: str, argv: list[str]) -> dict:
    """Launch `argv` in its own console window and return {name, pid}."""
    if IS_WINDOWS:
        # `cmd /k "title <name> && <argv...>"` gives each service a labelled
        # window that stays open after the process exits (so a crash is
        # readable). The recorded pid is cmd.exe; `taskkill /T` on it later
        # takes the python child with it.
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
        import os
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def stop_pid(pid: int) -> None:
    if IS_WINDOWS:
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                       capture_output=True)
    else:
        import os
        import signal
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except OSError:
            pass


def read_pid_file() -> dict | None:
    """The saved {kafka, services} record, or None when the file is empty
    or damaged (e.g. a start that was interrupted, or a hand edit)."""
    try:
        saved = json.loads(PID_FILE.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None
    return saved if isinstance(saved, dict) else None


def find_service_windows() -> list[dict]:
    """Windows only: the "EWS-<name>" cmd windows start_service() opens,
    found by their title, for when the pid file can't be read."""
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
    """Best-effort: is a real `python -m spark.streaming_job` process running?

    start_service() launches Spark inside a `cmd /k` wrapper so a crash
    stays readable, which means the recorded pid is cmd.exe — it stays
    "alive" even after the Python streaming job under it has died. This
    looks for the actual child so run_all.py can warn when the consumer
    side of the pipeline is down (the exact condition that leaves uploads
    stuck at INGESTED). Same check as the factory reset's
    (storage/reset.spark_job_running, PowerShell CIM on Windows: `wmic`,
    used here before, is gone from current Windows 11, so this always
    answered "running").
    """
    from storage.reset import spark_job_running
    running = spark_job_running()
    return True if running is None else running  # can't tell — don't cry wolf


def count_stuck_images() -> dict | None:
    """{"waiting": photos the pipeline still owes a result (INGESTED /
    PROCESSING), "undelivered": COMPLETED Drive photos whose result
    never reached Drive} — both older than 2 minutes, i.e. what
    storage.requeue_stuck re-sends. Returns None if the DB can't be read for
    any reason (don't let a reporting nicety break the launcher)."""
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


def which(cmd: str) -> bool:
    from shutil import which as _which
    return _which(cmd) is not None


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

    # ---- 1b. PostgreSQL — only when .env's DATABASE_URL uses it ------
    # (With the default SQLite file there's nothing to start or download.)
    from config.settings import DATABASE_URL
    if DATABASE_URL.startswith("postgresql"):
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
        # Also empty the Kafka topic: the reset clears Spark's checkpoints and
        # Spark reads from "earliest", so otherwise it replays every old message.
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
        from config.settings import DATABASE_URL
        if DATABASE_URL.startswith("postgresql"):
            # keeps the dashboard's analytics tables current (spark/batch_analytics.py)
            services.append(start_service("EWS-SparkAnalytics", ["python", "-m", "spark.batch_analytics", "--watch"]))
        else:
            print("  skipped Spark analytics - it reads the database over JDBC, which needs PostgreSQL "
                  "(DATABASE_URL); the dashboard's charts stay empty on SQLite")

    if not args.no_kafka_monitor:
        # read-only window: each farmer-photo event as it reaches Kafka (kafka/monitor.py)
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
    # A dead consumer is what silently leaves uploads stuck at INGESTED,
    # so surface it here instead of letting it be discovered hours later.
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
