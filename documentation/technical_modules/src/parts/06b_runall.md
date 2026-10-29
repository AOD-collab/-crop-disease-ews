
## D. Esplikasaun Kódigu-ba-Kódigu

### D.1 `run_all.py` — hahú no hapara sistema

#### D.1.1 Docstring, import, konstante

**1. Kódigu orijinál**

{{code run_all.py 1-27}}

**2. Objetivu**

Esplika opsaun liña komando no define pasta raiz, arkivu PID (`.run_all.pids.json`) no se sistema mak Windows.

**3. Fundamentu Python**

`Path(__file__).resolve().parent` — pasta arkivu ne'e; `sys.platform == "win32"`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: hahú sistema imajen tomak, kada parte iha janela rasik: Kafka (Docker), Spark streaming, Spark analítika, dashboard no Drive watcher; konfigurasaun husi `.env`.
- **Liña 5** — Liña mamuk docstring.
- **Liña 6** — `python run_all.py` — hahú hotu.
- **Liña 7** — `--no-kafka` — broker halai ona.
- **Liña 8** — `--no-spark` — salta Spark (no analítika).
- **Liña 9** — `--no-analytics` — salta analítika de'it.
- **Liña 10** — `--no-drive` — salta watcher.
- **Liña 11** — `--reset` — factory reset uluk (mantein utilizadór).
- **Liña 12** — `--stop` — hapara buat hotu ne'ebé nia hahú.
- **Liña 13** — Remata docstring.
- **Liña 15** — `import argparse`.
- **Liña 16** — `import json`.
- **Liña 17** — `import os`.
- **Liña 18** — `import socket`.
- **Liña 19** — `import subprocess`.
- **Liña 20** — `import sys`.
- **Liña 21** — `import time`.
- **Liña 22** — `from pathlib import Path`.
- **Liña 23** — `from shutil import which`.
- **Liña 25** — `ROOT = Path(__file__).resolve().parent`.
- **Liña 26** — `PID_FILE = ROOT / ".run_all.pids.json"`.
- **Liña 27** — `IS_WINDOWS = sys.platform == "win32"`.

**5. Walkthrough ezekusaun**

Iha laptop Windows: `IS_WINDOWS = True`, `PID_FILE = A:\final\crop_disease_image_system\.run_all.pids.json`.

**6. Tipu dadus**

`ROOT`, `PID_FILE`: `Path`; `IS_WINDOWS`: `bool`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

Pontu tama sistema tomak.

**9. Checkpoint ba principiante**

**P:** Saida mak arkivu `.run_all.pids.json`?
**R:** Lista prosesu ne'ebé `run_all.py` hahú (naran + PID), atu `--stop` bele hapara sira.

#### D.1.2 `start_service`, `pid_alive`, `stop_pid`

**1. Kódigu orijinál**

{{code run_all.py 30-72}}

**2. Objetivu**

Hahú komando iha janela konsola rasik ho títulu; kontrola se PID moris; hapara prosesu (no oan sira).

**3. Fundamentu Python**

`subprocess.Popen(..., creationflags=subprocess.CREATE_NEW_CONSOLE)`; `start_new_session=True` (Linux); `os.killpg(os.getpgid(pid), signal.SIGTERM)` — hapara grupu prosesu.

**4. Esplikasaun liña-ba-liña**

- **Liña 30–32** — Komentáriu seksaun helper prosesu.
- **Liña 33** — `def start_service(name: str, argv: list[str]) -> dict:`
- **Liña 34** — Docstring: hahú `argv` iha janela konsola rasik; fó `{name, pid}`.
- **Liña 35** — `if IS_WINDOWS:`
- **Liña 36** — Komentáriu: janela ho títulu ne'ebé hela loke depois crash; `taskkill /T` depois hapara oan mós.
- **Liña 37** — `inner = " ".join(argv)`.
- **Liña 38–42** — `proc = subprocess.Popen(f'cmd /k "title {name} && {inner}"', cwd=ROOT, creationflags=subprocess.CREATE_NEW_CONSOLE)` — `cmd /k` mantein janela loke.
- **Liña 43** — `else:`
- **Liña 44** — `proc = subprocess.Popen(argv, cwd=ROOT, start_new_session=True)`.
- **Liña 45** — `print(f"  started {name:<14} pid {proc.pid}")`.
- **Liña 46** — `return {"name": name, "pid": proc.pid}`.
- **Liña 49** — `def pid_alive(pid: int) -> bool:`
- **Liña 50** — `if IS_WINDOWS:`
- **Liña 51–54** — `out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True).stdout`.
- **Liña 55** — `return str(pid) in out`.
- **Liña 56** — `try:`
- **Liña 57** — `os.kill(pid, 0)` — sinál 0: kontrola de'it.
- **Liña 58** — `return True`.
- **Liña 59** — `except OSError:`
- **Liña 60** — `return False`.
- **Liña 63** — `def stop_pid(pid: int) -> None:`
- **Liña 64** — `if IS_WINDOWS:`
- **Liña 65–66** — `subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True)` — `/T` árvore, `/F` forsa.
- **Liña 67** — `else:`
- **Liña 68** — `import signal`.
- **Liña 69** — `try:`
- **Liña 70** — `os.killpg(os.getpgid(pid), signal.SIGTERM)`.
- **Liña 71** — `except OSError:`
- **Liña 72** — `pass`.

**5. Walkthrough ezekusaun**

`start_service("EWS-Spark", ["python", "-m", "spark.streaming_job"])` → janela foun naran "EWS-Spark" → `{"name": "EWS-Spark", "pid": 12345}` (*ezemplu PID*).

**6. Tipu dadus**

`start_service -> dict`; `pid_alive -> bool`; `stop_pid -> None`; `proc`: `subprocess.Popen`.

**7. Ligasaun OOP**

`subprocess.Popen` mak klase biblioteka; `proc.pid` atributu objetu.

**8. Impaktu integrasaun**

`main` hahú serbisu 5 ho `start_service`; `--stop` uza `pid_alive` + `stop_pid`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `cmd /k` no la'ós `cmd /c`?
**R:** `/k` mantein janela loke depois komando remata (ka monu) — ema bele lee erru.

#### D.1.3 PID file, janela, saúde Spark, fotografia hein, port

**1. Kódigu orijinál**

{{code run_all.py 75-136}}

**2. Objetivu**

Lee arkivu PID (seguru); buka janela "EWS-*" tuir títulu (Windows) bainhira arkivu PID aat; kontrola prosesu Spark loloos; konta fotografia hein/la entrega; kontrola no hein port.

**3. Fundamentu Python**

`json.loads(...read_text(encoding="utf-8-sig"))` (aseita BOM); PowerShell husi Python; `str.partition(" ")`; `with socket.socket(...) as s:`.

**4. Esplikasaun liña-ba-liña**

- **Liña 75** — `def read_pid_file() -> dict | None:`
- **Liña 76** — Docstring: rekord `{kafka, services}`, ka `None` se la iha/aat.
- **Liña 77** — `try:`
- **Liña 78** — `saved = json.loads(PID_FILE.read_text(encoding="utf-8-sig"))`.
- **Liña 79** — `except (OSError, ValueError):`
- **Liña 80** — `return None`.
- **Liña 81** — `return saved if isinstance(saved, dict) else None`.
- **Liña 84** — `def find_service_windows() -> list[dict]:`
- **Liña 85** — Docstring: Windows: janela "EWS-<naran>", buka tuir títulu (bainhira arkivu PID la bele lee).
- **Liña 86** — `if not IS_WINDOWS:`
- **Liña 87** — `return []`.
- **Liña 88–94** — `out = subprocess.run(["powershell", "-NoProfile", "-Command", "Get-CimInstance Win32_Process -Filter \"Name='cmd.exe'\" | Where-Object CommandLine -match 'title EWS-' | ForEach-Object {...}"], capture_output=True, text=True).stdout`.
- **Liña 95** — `found = []`.
- **Liña 96** — `for line in out.splitlines():`
- **Liña 97** — `pid, _, cmdline = line.strip().partition(" ")`.
- **Liña 98** — `if pid.isdigit() and "title EWS-" in cmdline:`
- **Liña 99** — `name = cmdline.split("title ", 1)[1].split()[0].strip('"&')`.
- **Liña 100** — `found.append({"name": name, "pid": int(pid)})`.
- **Liña 101** — `return found`.
- **Liña 104** — `def spark_streaming_alive() -> bool:`
- **Liña 105–106** — Docstring: prosesu Python job Spark halai duni? (PID rai mak janela cmd, ne'ebé moris depois crash.)
- **Liña 107** — `from storage.reset import spark_job_running`.
- **Liña 108** — `running = spark_job_running()`.
- **Liña 109** — `return True if running is None else running` — komentáriu: la bele hatene — la alarme falsu.
- **Liña 112** — `def count_stuck_images() -> dict | None:`
- **Liña 113–114** — Docstring: `{"waiting", "undelivered"}` fotografia tuan liu minutu 2, ka `None` se BD la bele lee.
- **Liña 115** — `try:`
- **Liña 116** — `from storage.database import SessionLocal`.
- **Liña 117** — `from storage.requeue_stuck import count_owed`.
- **Liña 118** — `with SessionLocal() as s:`
- **Liña 119** — `return count_owed(s)`.
- **Liña 120** — `except Exception:`
- **Liña 121** — `return None`.
- **Liña 124** — `def port_in_use(port: int) -> bool:`
- **Liña 125** — `with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:`
- **Liña 126** — `s.settimeout(0.5)`.
- **Liña 127** — `return s.connect_ex(("127.0.0.1", port)) == 0`.
- **Liña 130** — `def wait_for_port(port: int, timeout: int) -> bool:`
- **Liña 131** — `deadline = time.time() + timeout`.
- **Liña 132** — `while time.time() < deadline:`
- **Liña 133** — `if port_in_use(port):`
- **Liña 134** — `return True`.
- **Liña 135** — `time.sleep(2)`.
- **Liña 136** — `return False`.

**5. Walkthrough ezekusaun**

`wait_for_port(9092, 60)` → kontrola kada 2 s → Kafka loke iha 12 s → `True`. `count_stuck_images()` → `{"waiting": 0, "undelivered": 2}` (*ezemplu*).

**6. Tipu dadus**

`read_pid_file -> dict | None`; `find_service_windows -> list[dict]`; `spark_streaming_alive -> bool`; `count_stuck_images -> dict | None`; `port_in_use`, `wait_for_port -> bool`.

**7. Ligasaun OOP**

`socket.socket` mak context manager.

**8. Impaktu integrasaun**

Funsaun saúde (`spark_streaming_alive`, `count_stuck_images`) uza `storage.reset` no `storage.requeue_stuck`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `spark_streaming_alive` la uza `pid_alive` ho PID janela?
**R:** PID rai mak janela `cmd` ne'ebé hela loke maske Spark monu; presiza kontrola prosesu Python loloos.

#### D.1.4 `main` — argumentu, modu `--stop`, guarda hahú dala rua

**1. Kódigu orijinál**

{{code run_all.py 139-188}}

**2. Objetivu**

Lee opsaun; se `--stop`, hapara serbisu hotu (no Kafka se nia hahú) no hamoos arkivu PID; se lae, la husik hahú dala rua bainhira serbisu sira halai hela.

**3. Fundamentu Python**

`argparse` ho `help=argparse.SUPPRESS` (opsaun subar); `a or b` iha `prev = read_pid_file() or {...}`.

**4. Esplikasaun liña-ba-liña**

- **Liña 139–141** — Komentáriu seksaun main.
- **Liña 142** — `def main() -> int:`
- **Liña 143–144** — `p = argparse.ArgumentParser(description=__doc__, formatter_class=...)`.
- **Liña 145** — `--no-kafka`.
- **Liña 146** — `--no-spark`.
- **Liña 147–148** — `--no-analytics`.
- **Liña 149** — `--no-dashboard`.
- **Liña 150** — `--no-mail` — komentáriu: opsaun tuan; aseita, la halo buat ida.
- **Liña 151** — `--no-drive`.
- **Liña 152–153** — `--no-kafka-monitor`.
- **Liña 154** — `--reset`.
- **Liña 155** — `--stop`.
- **Liña 156** — `args = p.parse_args()`.
- **Liña 158** — Komentáriu: modu stop.
- **Liña 159** — `if args.stop:`
- **Liña 160** — `if not PID_FILE.exists():`
- **Liña 161** — `print("Nothing to stop - no .run_all.pids.json found.")`.
- **Liña 162** — `return 0`.
- **Liña 163** — `saved = read_pid_file()`.
- **Liña 164** — `if saved is None:`
- **Liña 165** — `print(...)` — arkivu aat; buka janela EWS-*.
- **Liña 166** — `saved = {"kafka": False, "services": find_service_windows()}`.
- **Liña 167** — `for s in saved.get("services", []):`
- **Liña 168** — `if pid_alive(s["pid"]):`
- **Liña 169** — `stop_pid(s["pid"])`.
- **Liña 170** — `print(f"stopped {s['name']} (pid {s['pid']})")`.
- **Liña 171** — `else:`
- **Liña 172** — `print(f"{s['name']} (pid {s['pid']}) was not running")`.
- **Liña 173** — `if saved.get("kafka"):` — Kafka hahú husi `run_all`:
- **Liña 174** — `print("stopping Kafka broker (docker compose down)...")`.
- **Liña 175** — `subprocess.run(["docker", "compose", "down"], cwd=ROOT, capture_output=True)`.
- **Liña 176** — `PID_FILE.unlink()`.
- **Liña 177** — `print("All stopped.")`.
- **Liña 178** — `return 0`.
- **Liña 180** — Komentáriu: guarda kontra hahú dala rua.
- **Liña 181** — `if PID_FILE.exists():`
- **Liña 182** — `prev = read_pid_file() or {"services": find_service_windows()}`.
- **Liña 183** — `alive = [s for s in prev.get("services", []) if pid_alive(s["pid"])]`.
- **Liña 184** — `if alive:`
- **Liña 185** — `names = ", ".join(s["name"] for s in alive)`.
- **Liña 186** — `print(f"Some components are already running ({names}). Run 'python run_all.py --stop' first.")`.
- **Liña 187** — `return 1`.
- **Liña 188** — `PID_FILE.unlink()` — komentáriu: arkivu tuan, hotu mate.

**5. Walkthrough ezekusaun**

`python run_all.py --stop` → hapara EWS-Spark, EWS-SparkAnalytics... → `docker compose down` (se `kafka: true`) → "All stopped.". `python run_all.py` dala rua → "Some components are already running (...)".

**6. Tipu dadus**

`args`: `argparse.Namespace`; `saved`, `prev`: `dict`; `alive`: `list[dict]`; return `int`.

**7. Ligasaun OOP**

`ArgumentParser`/`Namespace` objetu biblioteka.

**8. Impaktu integrasaun**

`docker compose down` hapara Kafka **no** PostgreSQL (volume mantein dadus).

**9. Checkpoint ba principiante**

**P:** Tanba sá guarda hahú dala rua importante?
**R:** Spark rua lee topic hanesan no hakerek BD hanesan — bele prosesa fotografia dala rua no harii konflitu.

#### D.1.5 Dependénsia, Kafka, PostgreSQL, `init_db`, reset

**1. Kódigu orijinál**

{{code run_all.py 190-251}}

**2. Objetivu**

Kontrola pakote Python; hahú Kafka (se port 9092 la loke no Docker iha) no hein; hahú PostgreSQL (se `DATABASE_URL` PostgreSQL) no hein; halai `storage.init_db`; opsionál factory reset (inklui hamamuk topic Kafka).

**3. Fundamentu Python**

`subprocess.run([sys.executable, "-c", "import streamlit, sqlalchemy"]).returncode` — kontrola import iha prosesu separadu.

**4. Esplikasaun liña-ba-liña**

- **Liña 190** — Komentáriu: kontrola dependénsia.
- **Liña 191** — `if subprocess.run([sys.executable, "-c", "import streamlit, sqlalchemy"]).returncode != 0:`
- **Liña 192** — `print("Python dependencies are missing. Run:  pip install -r requirements.txt")`.
- **Liña 193** — `return 1`.
- **Liña 195** — Komentáriu: 1. broker Kafka.
- **Liña 196** — `started_kafka = False`.
- **Liña 197** — `if not args.no_kafka:`
- **Liña 198** — `if port_in_use(9092):`
- **Liña 199** — `print("Kafka already reachable on localhost:9092 - not starting another broker.")`.
- **Liña 200** — `elif not which("docker"):`
- **Liña 201** — `print(...)` — `docker` la iha PATH; salta.
- **Liña 202** — `else:`
- **Liña 203** — `print("Starting Kafka broker (docker compose up -d kafka)...")`.
- **Liña 204** — `if subprocess.run(["docker", "compose", "up", "-d", "kafka"], cwd=ROOT).returncode != 0:`
- **Liña 205** — `print(...)` — compose falla; kontinua la ho broker.
- **Liña 206** — `else:`
- **Liña 207** — `print("Waiting for Kafka on localhost:9092...")`.
- **Liña 208** — `started_kafka = wait_for_port(9092, 60)`.
- **Liña 209–210** — `print("Kafka is up." if started_kafka else "Kafka did not come up within 60s; continuing anyway.")`.
- **Liña 211** — `elif not args.no_spark:`
- **Liña 212–213** — `print(...)` — `--no-kafka` maibé Spark liga: Spark presiza broker.
- **Liña 215** — Komentáriu: 1b. PostgreSQL (só bainhira `DATABASE_URL` uza).
- **Liña 216** — `from config.settings import DATABASE_URL`.
- **Liña 217** — `use_pg = DATABASE_URL.startswith("postgresql")`.
- **Liña 218** — `if use_pg:`
- **Liña 219** — `if port_in_use(5432):`
- **Liña 220** — `print("PostgreSQL already reachable on localhost:5432.")`.
- **Liña 221** — `elif not which("docker"):`
- **Liña 222** — `print(...)`.
- **Liña 223** — `else:`
- **Liña 224** — `print("Starting PostgreSQL (docker compose up -d postgres)...")`.
- **Liña 225** — `if subprocess.run(["docker", "compose", "up", "-d", "postgres"], cwd=ROOT).returncode != 0:`
- **Liña 226–227** — `print(...)` — la bele hahú; kontrola Docker no internet (dala uluk `docker pull postgres:16`).
- **Liña 228** — `return 1`.
- **Liña 229** — `print("Waiting for PostgreSQL on localhost:5432...")`.
- **Liña 230** — `if not wait_for_port(5432, 60):`
- **Liña 231** — `print("PostgreSQL did not come up within 60s - aborting.")`.
- **Liña 232** — `return 1`.
- **Liña 233** — `time.sleep(3)` — komentáriu: port loke momentu molok aseita login.
- **Liña 234** — `print("PostgreSQL is up.")`.
- **Liña 235** — `else:`
- **Liña 236** — `print("Database: SQLite file (storage/app.db) - no database server needed.")`.
- **Liña 238** — Komentáriu: 2. init BD (+ reset opsionál).
- **Liña 239** — `print("Initializing database (python -m storage.init_db)...")`.
- **Liña 240** — `if subprocess.run([sys.executable, "-m", "storage.init_db"], cwd=ROOT).returncode != 0:`
- **Liña 241** — `print("init_db failed - aborting.")`.
- **Liña 242** — `return 1`.
- **Liña 243** — `if args.reset:`
- **Liña 244** — `print("Factory-resetting pipeline data (keeps users/admins)...")`.
- **Liña 245** — Komentáriu: hamamuk mós topic Kafka, se lae Spark replay mensajen tuan hotu.
- **Liña 246** — `reset_cmd = [sys.executable, "-m", "storage.reset", "--yes"]`.
- **Liña 247** — `if not args.no_kafka:`
- **Liña 248** — `reset_cmd.append("--kafka")`.
- **Liña 249** — `if subprocess.run(reset_cmd, cwd=ROOT).returncode != 0:`
- **Liña 250** — `print("reset failed - aborting.")`.
- **Liña 251** — `return 1`.

**5. Walkthrough ezekusaun**

Docker Desktop mate: `docker compose up -d kafka` falla → "continuing without a broker"; `docker compose up -d postgres` falla → `return 1` (aborta). Docker moris: Kafka ~10 s, PostgreSQL ~5 s + 3 s.

**6. Tipu dadus**

`started_kafka`, `use_pg`: `bool`; `reset_cmd`: `list[str]`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

Orden importante: BD tenke iha molok `init_db`; `init_db` tenke halai molok Spark/dashboard.

**9. Checkpoint ba principiante**

**P:** Tanba sá Kafka falla la aborta, maibé PostgreSQL falla aborta?
**R:** La ho Kafka, produtór uza fila lokál (fallback) no dashboard funsiona; la ho base de dadus, komponente hotu la funsiona.

#### D.1.6 Lansa komponente no rai PID

**1. Kódigu orijinál**

{{code run_all.py 253-289}}

**2. Objetivu**

Hahú serbisu long-running tuir opsaun: EWS-Spark, EWS-SparkAnalytics (PostgreSQL de'it), EWS-KafkaMonitor, EWS-Dashboard (se port 8501 livre), EWS-DriveWatcher (se Drive konfigura). Rai lista PID.

**3. Fundamentu Python**

`services: list[dict] = []` (annotation variavel); `not (a or b)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 253** — Komentáriu: 3. lansa komponente long-running.
- **Liña 254** — `print("\nLaunching components:")`.
- **Liña 255** — `services: list[dict] = []`.
- **Liña 257** — `if not args.no_spark:`
- **Liña 258** — `services.append(start_service("EWS-Spark", ["python", "-m", "spark.streaming_job"]))`.
- **Liña 260** — `if not (args.no_spark or args.no_analytics):`
- **Liña 261** — `if use_pg:`
- **Liña 262** — `services.append(start_service("EWS-SparkAnalytics", ["python", "-m", "spark.batch_analytics", "--watch"]))`.
- **Liña 263** — `else:`
- **Liña 264–265** — `print(...)` — salta analítika: lee BD ho JDBC (presiza PostgreSQL); gráfiku dashboard mamuk iha SQLite.
- **Liña 267** — `if not args.no_kafka_monitor:`
- **Liña 268** — `services.append(start_service("EWS-KafkaMonitor", ["python", "-m", "kafka.monitor"]))`.
- **Liña 270** — `if not args.no_dashboard:`
- **Liña 271** — `if port_in_use(8501):`
- **Liña 272** — `print(...)` — port 8501 uza ona.
- **Liña 273** — `else:`
- **Liña 274–275** — `services.append(start_service("EWS-Dashboard", ["python", "-m", "streamlit", "run", "dashboard/app.py"]))`.
- **Liña 277** — `if not args.no_drive:`
- **Liña 278** — `from storage import drive_client`.
- **Liña 279** — `if drive_client.is_configured():` — token + ID pasta iha.
- **Liña 280** — `services.append(start_service("EWS-DriveWatcher", ["python", "-m", "kafka.drive_watcher"]))`.
- **Liña 281** — `else:`
- **Liña 282–283** — `print(...)` — salta watcher; halai `scripts/drive_authorize.py`.
- **Liña 285** — `if not services:`
- **Liña 286** — `print("Nothing was started.")`.
- **Liña 287** — `return 0`.
- **Liña 289** — `PID_FILE.write_text(json.dumps({"kafka": started_kafka, "services": services}, indent=2))`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* `.run_all.pids.json`:

```json
{"kafka": true, "services": [
  {"name": "EWS-Spark", "pid": 12345}, {"name": "EWS-SparkAnalytics", "pid": 12346},
  {"name": "EWS-KafkaMonitor", "pid": 12347}, {"name": "EWS-Dashboard", "pid": 12348},
  {"name": "EWS-DriveWatcher", "pid": 12349}]}
```

**6. Tipu dadus**

`services`: `list[dict[str, str|int]]`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

Prosesu 5 ne'e mak komponente Modulu 01 (Spark, analítika), 02 (monitor, watcher), 03 (watcher Drive), dashboard.

**9. Checkpoint ba principiante**

**P:** Tanba sá analítika Spark la hahú ho SQLite?
**R:** Analítika lee/hakerek BD ho JDBC; projetu la iha driver JDBC SQLite (Modulu 01 D.4.2).

#### D.1.7 Kontrola Spark, fotografia hein, relatóriu

**1. Kódigu orijinál**

{{code run_all.py 291-328}}

**2. Objetivu**

Hein to'o ~30 s atu kontrola katak job Spark moris (aviszu se lae); hatudu fotografia hein/la entrega ho komando atu limpa; hatudu enderesu dashboard.

**3. Fundamentu Python**

`for _ in range(15)` + `break`; `if stuck and (a or b)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 291** — Komentáriu: 3b. kontrola konsumidór Spark moris duni.
- **Liña 292** — Komentáriu: konsumidór mate husik upload hela `INGESTED`, entaun hatete agora.
- **Liña 293** — `if not args.no_spark:`
- **Liña 294** — `print("\nWaiting for the Spark streaming job to come up...")`.
- **Liña 295** — `alive = False`.
- **Liña 296** — `for _ in range(15):` — komentáriu: ~30 s; dala uluk mós rezolve pakote Kafka.
- **Liña 297** — `time.sleep(2)`.
- **Liña 298** — `if spark_streaming_alive():`
- **Liña 299** — `alive = True`.
- **Liña 300** — `break`.
- **Liña 301** — `if alive:`
- **Liña 302** — `print("  Spark streaming job is running.")`.
- **Liña 303** — `else:`
- **Liña 304** — `print("  WARNING: no live 'python -m spark.streaming_job' process detected.")`.
- **Liña 305** — `print("           Check the EWS-Spark console window for a traceback.")`.
- **Liña 306** — `print("           While it's down, uploads will pile up at status INGESTED.")`.
- **Liña 308** — Komentáriu: 3c. hatudu buat ne'ebé hela ingested-maibé-la-prosesa.
- **Liña 309** — `stuck = count_stuck_images()`.
- **Liña 310** — `if stuck and (stuck["waiting"] or stuck["undelivered"]):`
- **Liña 311–312** — `print(...)` — N imajen hein la prosesa no M rezultadu la to'o Drive; atu limpa:
- **Liña 313** — `python -m storage.requeue_stuck --all` — publika fali (Spark tenke moris).
- **Liña 314** — `--all --inline` — prosesa agora iha prosesu.
- **Liña 315** — `--from-drive` — fotografia Drive ne'ebé Spark seidauk rejista.
- **Liña 317** — Komentáriu: 4. relatóriu.
- **Liña 318** — `print("\nUp. Open:")`.
- **Liña 319** — `if not args.no_dashboard:`
- **Liña 320** — `print("  Dashboard  http://localhost:8501")`.
- **Liña 321** — `print("  Android    talks only to Google Drive - no ngrok or port forwarding needed")`.
- **Liña 323** — `print("\nStop everything with:  python run_all.py --stop")`.
- **Liña 324** — `return 0`.
- **Liña 327** — `if __name__ == "__main__":`
- **Liña 328** — `sys.exit(main())`.

**5. Walkthrough ezekusaun**

Hahú normál → "Spark streaming job is running." → "Up. Open: Dashboard http://localhost:8501". Java la iha → Spark monu → "WARNING: no live ... process detected."

**6. Tipu dadus**

`alive`: `bool`; `stuck`: `dict | None`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

`requeue_stuck` (haree Modulu 01 `_auto_resend`) mós halai automátiku iha Spark kada 300 s.

**9. Checkpoint ba principiante**

**P:** Tanba sá Android "la presiza ngrok"?
**R:** Telemóvel ko'alia de'it ho Google Drive; laptop la presiza IP publiku.
