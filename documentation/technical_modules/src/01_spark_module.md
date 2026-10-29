# 01 — Modulu Apache Spark & Streaming

*Sistema Crop Disease Early Warning System (EWS) — dokumentasaun téknika ba modulu Spark, hakerek ba estudante Enjeñaria Informátika ne'ebé foin hahú aprende Python. Fonte úniku: kódigu iha repozitóriu (branch `main`, commit `302818f`). Kódigu hotu iha dokumentu ne'e mak kópia automátiku husi arkivu loloos, ho númeru liña — la iha kódigu ne'ebé hakerek fali ka hamenus.*

{{coverage spark/session.py 1-30}}
{{coverage spark/streaming_job.py 1-360}}
{{coverage spark/preprocessing.py 1-233}}
{{coverage spark/batch_analytics.py 1-307}}
{{coverage spark/live_log.py 1-81}}
{{coverage spark/analytics_reader.py 1-102}}

> **Oinsá lee dokumentu ne'e.** Kada bloku kódigu iha seksaun D tuir pasu 9 ne'ebé hanesan: (1) Kódigu orijinál, (2) Objetivu, (3) Fundamentu Python, (4) Esplikasaun liña-ba-liña, (5) Walkthrough ezekusaun, (6) Tipu dadus, (7) Ligasaun OOP, (8) Impaktu integrasaun, (9) Checkpoint ba principiante. Se ita foin hahú Python, lee uluk seksaun **A.3 Fundamentu Python** — konseitu hotu ne'ebé kódigu Spark uza esplika iha ne'ebá ho ezemplu ki'ik.
>
> **Etiketa ne'ebé uza:** *Implementadu* = kódigu halo duni; *Inferidu* = ita dedús husi kódigu maibé kódigu la hakerek esplísitu; *Teoria* = esplikasaun jerál, la'ós kódigu projetu; *Ezemplu ilustrativu* = valór ne'ebé ita hili atu esplika, la'ós dadus loloos husi sistema.

## A. Introdusaun no Objetivu

### A.1 Papél Spark iha sistema

Sistema EWS ajuda agrikultór sira iha Timor-Leste atu deteta moras iha tahan batar (maize). Agrikultór foti fotografia ho telemóvel, aplikasaun Android haruka fotografia ba Google Drive, no sistema iha laptop analiza fotografia no haruka rezultadu fali ba Drive.

Iha sistema ne'e, kada komponente iha nia serbisu rasik:

| Komponente | Nia serbisu |
|---|---|
| Google Drive | fatin ne'ebé agrikultór haruka fotografia no simu rezultadu |
| `kafka/drive_watcher.py` + Kafka | hatene katak fotografia foun to'o ona, no lori **eventu** (mensajen JSON ki'ik) ba Spark |
| **Spark (modulu ne'e)** | simu eventu, kontrola (validasaun), foti fotografia, lee EXIF, **prepara imajen (preprocessing)**, rai imajen ne'ebé prosesa ona, no kalkula **analítika** hotu ba dashboard |
| CNN (`ml/inference.py`) | **klasifika** imajen (healthy, blight, gray_leaf_spot, ...) |
| `ml/risk.py` | kalkula nivel risku (LOW/MEDIUM/HIGH) |
| `storage/results.py` | rai rezultadu iha PostgreSQL no hakerek rezultadu ba Drive agrikultór |

Spark iha serbisu prinsipál **rua**:

1. **Streaming (tempu-real)** — `spark/streaming_job.py`. Spark Structured Streaming lee eventu husi Kafka topic. Eventu ida = fotografia ida. Spark halo *parsing* JSON → validasaun ho Spark SQL → iha `foreachBatch` nia prosesa kada fotografia: rejista → kontrola → foti imajen → EXIF → prepara imajen → CNN → risku → rai rezultadu → haruka ba Drive.
2. **Analítika (batch)** — `spark/batch_analytics.py`. Spark lee tabela PostgreSQL liuhusi **JDBC**, kalkula númeru hotu ne'ebé dashboard hatudu (total fotografia, distribuisaun moras, risku tuir fatin, tendénsia loron-loron), no hakerek fali tabela `analytics_*` liuhusi JDBC.

### A.2 Problema ne'ebé modulu ne'e rezolve

- **Fotografia barak to'o iha tempu hanesan.** Spark foti eventu iha *micro-batch* (grupu ki'ik) kada segundu 1 (`SPARK_TRIGGER_SECONDS = 1`), maximu eventu 100 kada batch (`SPARK_MAX_EVENTS_PER_BATCH = 100`), no prosesa to'o fotografia 8 iha paralelu (`SPARK_PARALLEL_PHOTOS = 8`). Valór sira ne'e mak *default* iha `config/settings.py`.
- **Eventu ida bele aat.** Spark la soe eventu aat: nia hatama razaun iha koluna `rejected_reason` no depois rai fotografia ne'e ho status `FAILED`, entaun la iha fotografia ida mak lakon.
- **Sistema bele monu.** Spark rai *checkpoint* (fatin ne'ebé Spark hakerek to'o ne'ebé nia lee ona). Se laptop mate, Spark hahú fali husi checkpoint — at-least-once delivery.
- **Dashboard presiza númeru loos.** Spark kalkula analítika hotu, no dashboard só lee tabela ne'ebé Spark hakerek (`spark/analytics_reader.py`).

### A.3 Fundamentu Python (aprende uluk molok lee kódigu)

Seksaun ne'e hanorin konseitu Python hotu ne'ebé kódigu Spark uza. Ezemplu sira iha ne'e mak **Ezemplu ilustrativu** — la'ós kódigu husi projetu.

**1. Variavel, valór no atribuisaun (assignment).** Variavel mak naran ida ne'ebé hatudu ba valór ida. Sinál `=` signifika "rai valór iha naran ne'e", la'ós "hanesan".

```python
total = 100          # int (númeru moris)
naran = "batar"      # str (testu)
konfiansa = 0.97     # float (númeru ho vírgula)
loos = True          # bool (True ka False)
la_iha = None        # None = "la iha valór"
```

**2. Tipu dadus prinsipál.**

| Tipu | Ezemplu | Signifika |
|---|---|---|
| `int` | `8` | númeru moris |
| `float` | `0.97` | númeru ho parte desimál |
| `str` | `"HIGH"` | testu |
| `bool` | `True` | loos/sala |
| `None` | `None` | la iha valór |
| `bytes` | `b"\xff\xd8"` | dadus binária (ezemplu: arkivu imajen JPEG) |
| `list` | `[1, 2, 3]` | lista ho orden, bele muda |
| `tuple` | `(0.8, "HIGH")` | lista ho orden, **labele** muda |
| `dict` | `{"image_id": "abc"}` | pár *chave → valór* |
| `set` | `{"a", "b"}` | grupu la ho orden, la iha repetisaun |

**3. String no f-string.** String ne'ebé hahú ho `f` bele tau variavel iha laran ho `{ }`. `!r` hatudu valór ho aspas (repr). `:.2f` hatudu númeru ho desimál 2.

```python
segundu = 1.23456
print(f"total {segundu:.2f}s")   # -> total 1.23s
```

**4. Lista, dicionáriu no `.get()`.** `d["chave"]` foti valór maibé fó erru `KeyError` se chave la iha; `d.get("chave")` fó `None` (ka valór *default*) se la iha.

```python
event = {"image_id": "abc", "location": None}
event["image_id"]          # "abc"
event.get("user_id")       # None (la fó erru)
event.get("x", "default")  # "default"
```

**5. Kondisaun (`if / elif / else`).** Python hili dalan ida tuir kondisaun. Indentasaun (espasu iha liña nia oin) hatudu liña sira ne'ebé pertense ba bloku ida.

**6. Loop (`for`) no compreensaun (comprehension).** `for x in lista:` repete ba kada elementu. Compreensaun mak forma badak atu harii lista ka dict:

```python
quadradu = [n * n for n in [1, 2, 3]]          # [1, 4, 9]
d = {k: 0 for k in ("a", "b")}                 # {"a": 0, "b": 0}
```

**7. Funsaun (`def`), parámetru, argumentu no `return`.** Funsaun mak bloku kódigu ho naran. Parámetru mak naran iha definisaun; argumentu mak valór ne'ebé ita haruka bainhira bolu. `return` fó rezultadu fali. Parámetru bele iha valór *default* (`console_progress: bool = True`). `*args` simu argumentu barak tuir orden; `**kwargs` simu argumentu ho naran barak.

```python
def soma(a, b=1):
    return a + b
soma(2)      # 3
soma(2, 5)   # 7
```

**8. Type hints.** `def f(event: dict) -> dict:` signifika "ita hein `event` mak dict no funsaun fó fali dict". Python **la kontrola** ida ne'e iha tempu ezekusaun — ne'e só informasaun ba ema no ferramenta sira.

**9. Import, modulu no pakote.** Arkivu `.py` ida = modulu. Pasta ho arkivu `.py` = pakote (ezemplu `spark/`). `import os` foti modulu tomak; `from x import y` foti naran ida husi modulu. Import bele mós iha funsaun nia laran (*lazy import*) atu modulu todan (ezemplu `pyspark`, `torch`) karrega só bainhira presiza.

**10. Exception (`try / except / finally / raise`).** Exception mak erru ne'ebé para kódigu. `try:` koko halo buat ruma; `except TipuErru as e:` kaer erru; `finally:` halai beibeik (iha erru ka lae); `raise` hamosu erru.

```python
try:
    x = int("abc")
except ValueError as e:
    print("la'ós númeru:", e)
finally:
    print("remata")
```

**11. Klase, objetu, `self` no `__init__`.** Klase mak planta (molde); objetu mak buat ida ne'ebé harii husi molde ne'e. `__init__` mak konstrutór — halai bainhira objetu foun moris. `self` mak objetu rasik.

```python
class Kontadór:
    def __init__(self, naran):
        self.naran = naran    # atributu instánsia
        self.valór = 0
    def aumenta(self):        # métodu
        self.valór += 1
k = Kontadór("fotografia")    # harii objetu
k.aumenta()                   # k.valór = 1
```

Klase ida bele **herda** husi klase seluk: `class InvalidEvent(Exception): pass` signifika `InvalidEvent` mak tipu `Exception` foun.

**12. Métodu espesiál `__call__`.** Se klase iha `__call__`, ita bele "bolu" objetu hanesan funsaun: `tabela()`.

**13. Decorator.** `@lru_cache(maxsize=4096)` iha leten `def` mak *decorator* — funsaun ida ne'ebé falun funsaun seluk atu aumenta hahalok. `lru_cache` rai rezultadu ba argumentu ida-idak, entaun bolu dala rua ho argumentu hanesan la halai fali kódigu.

**14. Context manager (`with`).** `with SessionLocal() as session:` loke rekursu (konesaun base de dadus) no taka automátiku bainhira bloku remata, maske iha erru. `with _LOCK:` foti *lock* no fó fali automátiku.

**15. Funsaun iha funsaun nia laran (closure) no `nonlocal`.** Funsaun bele define iha funsaun seluk nia laran no uza variavel husi funsaun ne'ebé falun nia. `nonlocal clock` husu Python atu muda variavel `clock` husi funsaun liur, la'ós harii variavel lokál foun.

**16. Operadór walrus `:=`.** `if sent := resend_owed(...):` atribui valór ba `sent` no kontrola nia iha tempu hanesan.

**17. Thread no `ThreadPoolExecutor`.** Thread mak "liña serbisu" ida ne'ebé halai iha tempu hanesan ho seluk iha programa ida nia laran. `ThreadPoolExecutor(max_workers=16)` mak grupu thread 16 ne'ebé prontu. `pool.submit(f, x)` fó serbisu ida no fó fali **Future** (promesa ida katak rezultadu sei mai); `future.result()` hein to'o rezultadu prontu. `pool.map(f, lista)` halai `f` ba kada elementu iha paralelu.

**18. Unpacking.** `a, b = (1, 2)` fahe tuple ba variavel rua. `{**d1, **d2}` kombina dict rua. `**stamps` iha chamada haruka pár hotu husi dict hanesan argumentu ho naran.

**19. Konseitu Spark báziku (Teoria).** *DataFrame* mak tabela distribuida (liña no koluna). *Lazy evaluation*: Spark la halai kálkulu bainhira ita hakerek `filter`/`select` — nia só halai bainhira presiza rezultadu (ezemplu `collect()`, `write`). *Column expression* (`col("x") == ""`) mak deskrisaun kálkulu, la'ós valór. *Structured Streaming* trata dadus ne'ebé mai beibeik hanesan tabela ne'ebé sempre aumenta liña.

## B. Arkivu Fonte Loloos

| Arkivu | Liña | Klase / funsaun prinsipál | Serbisu |
|---|---|---|---|
| `spark/session.py` | 30 | `kafka_session`, `KAFKA_PACKAGE` | Harii `SparkSession` ho konektór Kafka; workaround Hadoop iha Windows |
| `spark/streaming_job.py` | 360 | `EVENT_SCHEMA`, `parse_events`, `process_event`, `process_and_persist`, `foreach_batch_sink`, `_process_one`, `_upload_result`, `_warm_up_model`, `_auto_resend`, `run` | Job streaming: Kafka → parsing → validasaun → prosesa fotografia → rai → Drive |
| `spark/preprocessing.py` | 233 | `InvalidEvent`, `clean_event`, `_clean_drive_event`, `extract_metadata`, `model_input`, `prepare_image`, `store_processed_image`, `save_for_review`, `save_downloaded_photo`, `scale_test_run_for` | Spark nia serbisu ba fotografia ida: kontrola eventu, EXIF, preprocessing imajen |
| `spark/batch_analytics.py` | 307 | `jdbc_url_and_properties`, `read_inputs`, `photos_with_latest_prediction`, `compute`, `compute_scale_test`, `write_tables`, `refresh`, `watch` | Analítika batch ho JDBC: lee PostgreSQL, agregasaun, hakerek tabela `analytics_*` |
| `spark/live_log.py` | 81 | `say`, `farmer_label`, `photo_line`, `batch_started`, `photo_done`, `result_delivered`, `batch_committed` | Liña tempu-real iha janela konsola EWS-Spark |
| `spark/analytics_reader.py` | 102 | `SparkTable`, `images_with_predictions`, `kpi_summary` | Dashboard lee tabela ne'ebé Spark kalkula |

**Dependénsia esternu:** `pyspark` (Spark 3.5), pakote Maven `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` (konektór Kafka) no `org.postgresql:postgresql:42.7.3` (driver JDBC), `Pillow` (PIL, imajen), `torchvision` (transforms ba tensor), `SQLAlchemy` (ORM ba PostgreSQL), `pandas` (dashboard).

**Dependénsia internu:** `config/settings.py` (konfigurasaun), `kafka/config.py` (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`), `storage/*` (base de dadus, Drive, rezultadu), `ml/*` (CNN, risku, desenhu).

## C. Arkitetura no Fluxu Ezekusaun

### C.1 Input, prosesu, output

| | Streaming (`streaming_job.py`) | Analítika (`batch_analytics.py`) |
|---|---|---|
| **Input** | Mensajen JSON iha Kafka topic (`KAFKA_TOPIC`) — referénsia ba fotografia (la'ós bytes imajen) | Tabela PostgreSQL `images`, `predictions`, `users`, `scale_test_items` (liuhusi JDBC) |
| **Prosesu** | `from_json` → regra validasaun Spark SQL → `foreachBatch` → `_process_one` kada fotografia (paralelu) | Window function (prediksaun foun liu), `groupBy` + `agg`, kontajen kondisionál |
| **Output** | Liña `predictions` + status `images` iha PostgreSQL (ORM SQLAlchemy); imajen PNG iha `PROCESSED_IMAGES_DIR`; rezultadu JSON + imajen iha Drive | Tabela `analytics_*` no `batch_location_daily_summary` (JDBC append ba `_staging`, depois troka iha transasaun ida) |

### C.2 Diagrama fluxu streaming

```mermaid
flowchart TD
    A["Kafka topic<br/>(mensajen JSON)"] --> B["spark.readStream.format('kafka')<br/>run()"]
    B --> C["parse_events()<br/>from_json + EVENT_SCHEMA<br/>rejected_reason"]
    C --> D["writeStream.foreachBatch<br/>trigger 1 s, checkpoint"]
    D --> E["foreach_batch_sink()<br/>dropDuplicates(image_id)<br/>collect()"]
    E --> F["_process_one() x N<br/>ThreadPoolExecutor (8)"]
    F --> G{"replay_state?"}
    G -- completed --> S["skip (None)"]
    G -- None / result_missing --> H["mark_processing()"]
    H --> I["process_and_persist()"]
    I --> J["process_event():<br/>clean_event → bytes → duplikadu<br/>→ EXIF → prepare_image<br/>→ model_input → CNN → risku"]
    J --> K["persist_result()<br/>COMPLETED / FAILED"]
    K --> L["_UPLOADS.submit(_upload_result)<br/>rezultadu ba Drive"]
    L --> M["batch_committed()<br/>Spark hakerek checkpoint"]
```

### C.3 Diagrama fluxu analítika

```mermaid
flowchart TD
    A["watch(): kada 30 s<br/>data_signature()"] --> B{"dadus muda<br/>ka liu 600 s?"}
    B -- lae --> A
    B -- sin --> C["refresh(spark)"]
    C --> D["read_inputs():<br/>spark.read.jdbc x 4"]
    D --> E["photos_with_latest_prediction()<br/>Window row_number"]
    E --> F["compute() + compute_scale_test()<br/>groupBy / agg"]
    F --> G["write_tables():<br/>df.write.mode('append').jdbc<br/>ba naran_staging"]
    G --> H["transasaun ida:<br/>DELETE + INSERT SELECT<br/>+ DROP staging"]
    H --> I["dashboard: analytics_reader.SparkTable"]
```

### C.4 Trasu konkretu: eventu ida husi Kafka to'o PostgreSQL

Valór sira iha tabela ne'e mak **Ezemplu ilustrativu** (identifikadór no email la'ós dadus loloos — dokumentu ne'e la hatudu dadus privadu agrikultór). Naran kampu, funsaun no regra mak loloos husi kódigu.

| Pasu | Funsaun (arkivu) | Dadus antes | Dadus depois |
|---|---|---|---|
| 1 | `build_drive_event` (`kafka/drive_watcher.py`) | metadata Drive | dict `{"kind": "drive_upload", "image_id": "a1b2c3d4", "image_path": "gdrive:1AbC...", "owner_email": "agrikultor@example.com", "uploaded_at": "2026-10-20T01:15:00Z", "location": "  aileu   vila ", ...}` |
| 2 | Produtór Kafka | dict | bytes JSON iha topic (kolumna `value`) |
| 3 | `parse_events` | `value` = bytes | liña ho kolumna `kind`, `image_id`, ..., `location = "Aileu Vila"`, `rejected_reason = null`, `raw_value = null` |
| 4 | `foreach_batch_sink` | DataFrame batch | lista Python `[{"kind": "drive_upload", "image_id": "a1b2c3d4", ...}]` |
| 5 | `_process_one` → `register_upload` | dict | liña `images` foun (`outcome = "new"`) |
| 6 | `replay_state` | `"a1b2c3d4"` | `None` (seidauk prosesa) |
| 7 | `mark_processing` | — | `images.status = PROCESSING` |
| 8 | `clean_event` | dict | dict validadu (`uploaded_at` ISO, `location` limpu, `user_id` iha) |
| 9 | `load_image_bytes` | `gdrive:1AbC...` | `bytes` JPEG (ezemplu 2.4 MB) |
| 10 | `extract_metadata` | bytes orijinál | `camera_make`, `gps_latitude`... iha liña `images` |
| 11 | `prepare_image` | JPEG 4000×3000 | JPEG maximu 1280 px (`ANALYSIS_MAX_PX`), vertikál, la iha EXIF |
| 12 | `model_input` | JPEG 1280×960 | tensor `torch.Tensor` forma `[3, S, S]` normalizadu (S = `img_size` husi modelu) |
| 13 | `run_model` (`ml/inference.predict`) | tensor | dict `{"predicted_class": "blight", "predicted_class_confidence": 0.97, "lesion_score": 0.97, ...}` |
| 14 | `compute_risk` | features | `(0.97, "HIGH")` |
| 15 | `persist_result` | result dict | liña `predictions` + `images.status = COMPLETED` |
| 16 | `_upload_result` | result | arkivu rezultadu iha Drive agrikultór |

### C.5 Diagrama sekuénsia

```mermaid
sequenceDiagram
    participant K as Kafka
    participant S as Spark (run)
    participant B as foreach_batch_sink
    participant P as _process_one
    participant E as process_event
    participant C as CNN (ml.inference)
    participant D as PostgreSQL
    participant G as Google Drive
    K->>S: mensajen JSON (micro-batch)
    S->>S: parse_events (from_json)
    S->>B: batch_df, batch_id
    B->>P: event dict (paralelu)
    P->>D: register_upload / replay_state / mark_processing
    P->>E: process_and_persist
    E->>G: load_image_bytes (download)
    E->>D: extract_metadata (EXIF)
    E->>C: predict(model_input)
    C-->>E: features
    E-->>P: result
    P->>D: persist_result COMPLETED
    P->>G: _upload_result (thread background)
    B->>S: return (Spark hakerek checkpoint)
```

## D. Esplikasaun Kódigu-ba-Kódigu

Seksaun ne'e esplika kada liña kódigu Spark. Bloku ida-idak tuir pasu 9. Númeru liña iha bullet **Liña N** hanesan ho númeru iha bloku kódigu.

### D.1 `spark/session.py` — harii SparkSession ho Kafka

#### D.1.1 Docstring, workaround Hadoop Windows no `KAFKA_PACKAGE`

**1. Kódigu orijinál**

{{code spark/session.py 1-14}}

**2. Objetivu**

Bloku ne'e prepara ambiente molok Spark hahú. Iha Windows, Spark (liuhusi Hadoop) presiza programa `winutils.exe`; kódigu ne'e hatudu fatin programa ne'e. Nia mós define naran pakote konektór Kafka ne'ebé Spark tenke download.

**3. Fundamentu Python**

- **Docstring** (`"""..."""` iha arkivu nia leten): testu esplikasaun ba modulu. Python rai nia iha `__doc__`, la halai nia.
- **`import os`**: modulu padraun ba sistema operativu. `os.name` fó `"nt"` iha Windows; `os.environ` mak dict ho variavel ambiente (environment variables).
- **Variavel ho `_` iha oin** (`_hadoop_bin`): konvensaun Python ba "privadu/internu" — Python la blokeia asesu, ne'e só sinál ba programadór.
- **Kódigu iha nível modulu** (la iha funsaun nia laran) halai **dala ida** bainhira modulu import dahuluk.
- **String raw** `r"C:\hadoop"`: `r` signifika `\` la'ós karakter espesiál.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring modulu: sesaun Spark ne'ebé lee Kafka, uza husi job tempu-real no teste eskala. Tenke import **molok** `pyspark` tanba iha Windows nia hatudu `winutils` uluk.
- **Liña 5** — `import os`: foti modulu `os` atu lee no muda variavel ambiente.
- **Liña 7** — Komentáriu: esplika katak bloku tuirmai ba Windows de'it.
- **Liña 8** — `if os.name == "nt" and "HADOOP_HOME" not in os.environ:` — kondisaun rua tenke loos: komputadór mak Windows (`"nt"`) **no** variavel `HADOOP_HOME` seidauk iha. Se utilizadór define ona `HADOOP_HOME`, kódigu la muda nia.
- **Liña 9** — `os.environ["HADOOP_HOME"] = r"C:\hadoop"` — define fatin Hadoop. Efeitu sekundáriu: muda ambiente ba prosesu Python tomak no prosesu oan sira (inklui JVM Spark).
- **Liña 10** — `_hadoop_bin = r"C:\hadoop\bin"` — rai dalan pasta `bin` iha variavel `str`.
- **Liña 11** — `if _hadoop_bin not in os.environ.get("PATH", ""):` — `.get("PATH", "")` fó `""` se `PATH` la iha (la fó erru). Kontrola katak pasta seidauk iha `PATH`.
- **Liña 12** — `os.environ["PATH"] += os.pathsep + _hadoop_bin` — aumenta pasta ba `PATH`. `os.pathsep` mak `;` iha Windows. `+=` signifika `PATH = PATH + ...`.
- **Liña 14** — `KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"` — konstante (naran ho letra boot = konvensaun ba valór ne'ebé la muda). Koordenada Maven: grupu `org.apache.spark`, artefaktu `spark-sql-kafka-0-10_2.12` (Scala 2.12), versaun `3.5.0`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* iha laptop Windows ne'ebé `HADOOP_HOME` la iha, no `PATH = "C:\Windows;C:\Python312"`. Depois import `spark.session`: `os.environ["HADOOP_HOME"] = "C:\hadoop"` no `PATH = "C:\Windows;C:\Python312;C:\hadoop\bin"`. Iha Linux (`os.name == "posix"`) kondisaun liña 8 sala, entaun la iha buat ida muda.

**6. Tipu dadus**

| Naran | Tipu | Esplísitu / inferidu |
|---|---|---|
| `os.name` | `str` | inferidu (biblioteka padraun) |
| `os.environ` | `os._Environ` (hanesan `dict[str, str]`) | inferidu |
| `_hadoop_bin` | `str` | inferidu husi literal |
| `KAFKA_PACKAGE` | `str` | inferidu husi literal |

**7. Ligasaun OOP**

La iha klase iha bloku ne'e. `os.environ` mak objetu ida (instánsia husi `os._Environ`) ne'ebé hahalok hanesan dict — ezemplu ki'ik husi *polimorfizmu* (objetu ne'ebé la'ós `dict` maibé suporta operasaun dict). Bloku ne'e rasik la define klase.

**8. Impaktu integrasaun**

`spark/streaming_job.py` liña 23 import `kafka_session` husi modulu ne'e **molok** `pyspark` (komentáriu iha ne'ebá hatete "before pyspark"). Ho ida ne'e, bainhira Spark hahú JVM, `HADOOP_HOME` iha ona. `KAFKA_PACKAGE` uza iha `kafka_session` (liña 23).

**9. Checkpoint ba principiante**

**P:** Tanba sá kódigu kontrola `"HADOOP_HOME" not in os.environ` molok define?
**R:** Atu la hakerek liu valór ne'ebé utilizadór define ona. Kódigu só fó valór *default* bainhira seidauk iha.

#### D.1.2 Funsaun `kafka_session`

**1. Kódigu orijinál**

{{code spark/session.py 17-30}}

**2. Objetivu**

Harii (ka foti fali) `SparkSession` — pontu tama ba funsionalidade Spark hotu — ho konektór Kafka no konfigurasaun checkpoint ne'ebé estável iha Windows.

**3. Fundamentu Python**

- **Parámetru ho type hint no default**: `app_name: str` (tenke fó), `console_progress: bool = True` (opsionál).
- **Import iha funsaun nia laran** (`from pyspark.sql import SparkSession`): `pyspark` karrega só bainhira funsaun ne'e halai.
- **Method chaining**: `a.b().c().d()` — kada métodu fó fali objetu ne'ebé bele bolu métodu tuirmai. Parénteze `( ... )` permite hakerek chain iha liña barak.
- **Padraun Builder (Teoria)**: objetu ida ne'ebé ita konfigura pasu-ba-pasu, depois bolu `getOrCreate()` atu harii objetu final.

**4. Esplikasaun liña-ba-liña**

- **Liña 17** — `def kafka_session(app_name: str, console_progress: bool = True):` — define funsaun ho parámetru rua. La iha type hint ba `return`.
- **Liña 18** — Docstring: SparkSession ho konektór Kafka (download dala uluk).
- **Liña 19** — Import `SparkSession` husi `pyspark.sql`.
- **Liña 20** — `builder = (` — hahú espresaun ne'ebé sei rai iha `builder`.
- **Liña 21** — `SparkSession.builder` — atributu klase ne'ebé fó objetu `Builder`.
- **Liña 22** — `.appName(app_name)` — naran aplikasaun (hatudu iha Spark UI no log).
- **Liña 23** — `.config("spark.jars.packages", KAFKA_PACKAGE)` — husu Spark atu download pakote Maven Kafka (dala uluk de'it; depois uza cache `~/.ivy2`).
- **Liña 24** — Komentáriu: checkpoint manager default monu ho aleatóriu iha disku lokál Windows.
- **Liña 25–26** — `.config("spark.sql.streaming.checkpointFileManagerClass", "...FileSystemBasedCheckpointFileManager")` — troka klase ne'ebé hakerek checkpoint ba versaun ne'ebé uza operasaun sistema arkivu simples (estável liu iha Windows).
- **Liña 27** — `)` — remata espresaun; `builder` agora mak objetu `Builder` konfiguradu.
- **Liña 28** — `if not console_progress:` — se ema husu atu la hatudu barra progresu.
- **Liña 29** — `builder = builder.config("spark.ui.showConsoleProgress", "false")` — hamate barra progresu iha konsola (atu liña `live_log` la kahur).
- **Liña 30** — `return builder.getOrCreate()` — se SparkSession iha ona iha prosesu ne'e, fó fali ida ne'ebé iha; se lae, harii foun.

**5. Walkthrough ezekusaun**

`streaming_job.build_spark()` bolu `kafka_session("ImageSystem-CropDiseaseEWS")`. `console_progress` = `True` (default), entaun liña 29 la halai. Rezultadu: objetu `SparkSession` ho naran aplikasaun `ImageSystem-CropDiseaseEWS`, konektór Kafka 3.5.0 no checkpoint manager `FileSystemBasedCheckpointFileManager`. Bolu dala rua iha prosesu hanesan fó fali objetu **hanesan** (`getOrCreate`).

**6. Tipu dadus**

| Naran | Tipu | Esplísitu / inferidu |
|---|---|---|
| `app_name` | `str` | esplísitu (type hint) |
| `console_progress` | `bool` | esplísitu |
| `builder` | `pyspark.sql.SparkSession.Builder` | inferidu |
| valór `return` | `pyspark.sql.SparkSession` | inferidu (la iha hint) |
| Exception | erru JVM/Py4J se Java la iha ka download falla | inferidu |

**7. Ligasaun OOP**

`SparkSession.builder` mak padraun **Builder** husi biblioteka PySpark (la'ós projetu). `getOrCreate()` hahalok hanesan **Singleton** kada prosesu: SparkSession ida de'it. Projetu ne'e **uza** padraun sira ne'e; nia la define klase rasik iha arkivu ne'e.

**8. Impaktu integrasaun**

Bolu husi `streaming_job.build_spark()` (liña 80) no husi teste eskalabilidade. SparkSession ne'ebé fó fali uza iha `run()` atu `readStream` husi Kafka.

**9. Checkpoint ba principiante**

**P:** Saida mak diferensa entre `SparkSession.builder` no `builder.getOrCreate()`?
**R:** `builder` mak objetu konfigurasaun (seidauk iha sesaun); `getOrCreate()` mak harii sesaun loloos (ka fó fali ida ne'ebé iha ona).

### D.2 `spark/streaming_job.py` — job Structured Streaming

#### D.2.1 Docstring modulu

**1. Kódigu orijinál**

{{code spark/streaming_job.py 1-16}}

**2. Objetivu**

Esplika iha liña badak saida mak job ne'e halo ba kada eventu fotografia, no oinsá halai nia.

**3. Fundamentu Python**

Docstring modulu la halai; nia mak dokumentasaun. Komando `python -m spark.streaming_job` halai modulu ne'e hanesan programa (`-m` = "halai modulu husi pakote").

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Job Spark Structured Streaming (manuál DIT seksaun 22); ba kada eventu fotografia husi Kafka:
- **Liña 5** — rejista upload → `storage/uploads`.
- **Liña 6–7** — kontrola, foti, preprocess no rai imajen → `spark/preprocessing`.
- **Liña 8** — klasifika → `ml/inference` (CNN).
- **Liña 9** — nivel risku → `ml/risk`.
- **Liña 10** — rai + haruka rezultadu ba Drive → `storage/results`.
- **Liña 12–13** — Checkpoint fó entrega *at-least-once*; eventu ne'ebé repete (replay) seguru tanba `persist_result` troka prediksaun antigu.
- **Liña 15** — Oinsá halai: `python -m spark.streaming_job` (presiza Kafka iha `KAFKA_BOOTSTRAP_SERVERS`).
- **Liña 16** — Remata docstring.

**5. Walkthrough ezekusaun**

Python lee docstring no rai iha `spark.streaming_job.__doc__`. La iha output.

**6. Tipu dadus**

`__doc__`: `str` (inferidu).

**7. Ligasaun OOP**

Not implemented in the current codebase. (Docstring la iha klase.)

**8. Impaktu integrasaun**

Lista ne'e hatudu orden komponente sira: `storage/uploads` → `spark/preprocessing` → `ml/inference` → `ml/risk` → `storage/results`. Orden ne'e hanesan ho kódigu iha `_process_one` no `process_event`.

**9. Checkpoint ba principiante**

**P:** Saida mak "at-least-once delivery"?
**R:** Kada eventu sei prosesa **dala ida ka liu** — la lakon, maibé bele repete. Tanba ne'e kódigu tenke seguru bainhira eventu repete (idempotent).

#### D.2.2 Import no `CHECKPOINT_DIR`

**1. Kódigu orijinál**

{{code spark/streaming_job.py 17-50}}

**2. Objetivu**

Foti biblioteka padraun, funsaun Spark SQL, konfigurasaun no funsaun husi modulu projetu seluk ne'ebé job presiza. Define pasta checkpoint.

**3. Fundamentu Python**

- `from x import (a, b, c)` — parénteze permite import naran barak iha liña barak.
- `import ... as nome` — fó naran seluk: `predict as run_model` signifika iha arkivu ne'e funsaun `predict` naran `run_model`.
- Orden import importante: liña 23 import `spark.session` **molok** `pyspark` (haree D.1.1).
- Operadór `/` iha `Path`: `Path("a") / "b"` harii dalan `a/b`.

**4. Esplikasaun liña-ba-liña**

- **Liña 17** — `import threading` — atu harii thread `_auto_resend` (liña 355).
- **Liña 18** — `import time` — `perf_counter`, `monotonic`, `sleep` ba medida tempu no hein.
- **Liña 19** — `ThreadPoolExecutor` — grupu thread ba download, prosesa paralelu no upload.
- **Liña 20** — `datetime, timezone` — tempu ho fuso UTC (`datetime.now(timezone.utc)`).
- **Liña 21** — `Path` — dalan arkivu orientadu ba objetu.
- **Liña 23** — `kafka_session` husi `spark.session`; komentáriu: tenke molok pyspark (workaround Hadoop Windows).
- **Liña 24–26** — Funsaun Spark SQL: `coalesce` (valór dahuluk ne'ebé la null), `col` (koluna), `concat` (kombina string), `current_timestamp`, `from_json` (parse JSON), `initcap` (Letra Boot Iha Oin), `lit` (valór literal), `regexp_replace`, `to_timestamp`, `trim`, `when` (kondisaun).
- **Liña 27** — `StructType, StructField, StringType` — atu define eskema JSON.
- **Liña 29** — `KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC` husi `kafka/config.py`.
- **Liña 30–33** — Konfigurasaun Spark husi `config/settings.py`: pasta checkpoint, eventu maximu kada batch, segundu trigger, fotografia paralelu, worker upload Drive, no parámetru re-send automátiku.
- **Liña 34–37** — Funsaun husi `spark/preprocessing.py`: `InvalidEvent`, `clean_event`, `extract_metadata`, `prepare_image`, `model_input`, `store_processed_image`, `save_for_review`, `save_downloaded_photo`, `scale_test_run_for`.
- **Liña 38** — Husi `ml/inference`: `predict` (naran `run_model`), `model_version` (naran `get_model_version`), `disease_heatmap`.
- **Liña 39** — `compute_risk` husi `ml/risk`.
- **Liña 40** — `render_processed_image`, `heatmap_on_photo` husi `ml/render` (desenha imajen ba agrikultór).
- **Liña 41** — `ProcessingStatus` (enum status) no `init_db` husi `storage/database`.
- **Liña 42** — `check_duplicate_by_hash`, `resend_duplicates_of` husi `storage/duplicates`.
- **Liña 43** — `is_drive_path`, `load_image_bytes`, `downloaded_photo_path` husi `storage/image_storage`.
- **Liña 44–46** — Husi `storage/results`: `FEATURE_FIELDS`, `persist_result`, `mark_processing`, `replay_state`, `publish_result_to_drive_safely`.
- **Liña 47** — `register_upload` husi `storage/uploads`.
- **Liña 48** — Modulu `live_log` (liña konsola tempu-real).
- **Liña 50** — `CHECKPOINT_DIR = str(SPARK_CHECKPOINT_ROOT / KAFKA_TOPIC)` — pasta checkpoint = pasta raiz checkpoint + naran topic, konverte ba `str`. Topic kada ida iha nia checkpoint rasik.

**5. Walkthrough ezekusaun**

Ho valór *default* `SPARK_CHECKPOINT_ROOT = <projetu>/storage/spark_checkpoints` (`config/settings.py` liña 29), `CHECKPOINT_DIR` sai `<projetu>/storage/spark_checkpoints/<KAFKA_TOPIC>`. Import `ml.inference` iha ne'e la karrega modelu CNN — modelu karrega iha `_warm_up_model` (D.2.11).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `SPARK_CHECKPOINT_ROOT` | `pathlib.Path` (inferidu husi `config/settings.py`) |
| `KAFKA_TOPIC` | `str` |
| `CHECKPOINT_DIR` | `str` (esplísitu ho `str(...)`) |
| `SPARK_TRIGGER_SECONDS` | `float` |
| `SPARK_MAX_EVENTS_PER_BATCH`, `SPARK_PARALLEL_PHOTOS` | `int` |

**7. Ligasaun OOP**

`ProcessingStatus` mak klase **enum** (haree Modulu 04). `InvalidEvent` mak klase exception (D.3.2). `Path` mak klase biblioteka padraun — operadór `/` funsiona tanba klase `Path` define métodu espesiál `__truediv__` (polimorfizmu operadór).

**8. Impaktu integrasaun**

Lista import ne'e hatudu katak Spark job mak **koordenadór**: nia la halo klasifikasaun ka hakerek base de dadus rasik; nia bolu funsaun husi `ml/` no `storage/`.

**9. Checkpoint ba principiante**

**P:** Iha arkivu ne'e, se ita hakerek `run_model(x)`, funsaun saida mak halai?
**R:** `ml.inference.predict`, tanba liña 38 import ho `predict as run_model`.

#### D.2.3 `EVENT_SCHEMA`, `DRIVE_UPLOAD` no `build_spark`

**1. Kódigu orijinál**

{{code spark/streaming_job.py 52-80}}

**2. Objetivu**

Define forma (eskema) mensajen JSON husi Kafka atu Spark bele konverte testu JSON ba koluna ho naran. Kampu hotu mak `StringType` — validasaun tipu halo depois.

**3. Fundamentu Python**

- Lista Python `[ ... ]` ho objetu `StructField(...)` barak; vírgula iha elementu ikus permite.
- Chamada funsaun bele simu objetu ne'ebé harii iha argumentu nia laran: `StructType([StructField(...), ...])`.
- `def f(): return x` — funsaun badak ne'ebé la simu parámetru.

**4. Esplikasaun liña-ba-liña**

- **Liña 52** — `EVENT_SCHEMA = StructType([` — hahú eskema: tipu "struct" (rekord ho kampu barak).
- **Liña 53** — Komentáriu: `"drive_upload"` = fotografia Drive foun; `"submit"` = fotografia ne'ebé iha ona iha base de dadus, haruka fali.
- **Liña 54** — Kampu `kind` (tipu eventu), `StringType()`.
- **Liña 55** — `image_id` — identifikadór úniku fotografia.
- **Liña 56** — `user_id` — só iha eventu `submit` (komentáriu).
- **Liña 57** — `location` — fatin ne'ebé agrikultór hakerek.
- **Liña 58** — `image_path` — dalan fotografia (`gdrive:<id>` ka dalan lokál).
- **Liña 59** — `drive_file_id` — ID arkivu iha Google Drive.
- **Liña 60** — `captured_at` — tempu foti fotografia (testu ISO-8601).
- **Liña 61** — `uploaded_at` — tempu upload.
- **Liña 62** — Komentáriu: kampu tuirmai só iha `drive_upload` (metadata Drive).
- **Liña 63** — `file_name` — naran arkivu iha Drive.
- **Liña 64** — `mime_type` — ezemplu `image/jpeg`.
- **Liña 65** — `size_bytes` — tamañu (testu).
- **Liña 66** — `owner_email` — email na'in arkivu (agrikultór).
- **Liña 67** — `owner_name` — naran na'in.
- **Liña 68** — `folder_email` — email pasta agrikultór.
- **Liña 69** — `original_filename` — naran arkivu orijinál iha telemóvel.
- **Liña 70** — `content_hash` — SHA-256 husi Drive (ba deteta duplikadu).
- **Liña 71** — `device_gps_lat` — latitude husi telemóvel.
- **Liña 72** — `device_gps_lon` — longitude husi telemóvel.
- **Liña 73** — `run_id` — só eventu teste eskalabilidade antigu; job salta nia (komentáriu).
- **Liña 74** — `])` — taka lista no `StructType`.
- **Liña 76** — `DRIVE_UPLOAD = "drive_upload"` — konstante atu la hakerek string ne'e repete (evita sala ortografia).
- **Liña 79** — `def build_spark():` — funsaun ki'ik atu harii SparkSession job ne'e.
- **Liña 80** — `return kafka_session("ImageSystem-CropDiseaseEWS")` — bolu D.1.2 ho naran aplikasaun.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* JSON `{"kind":"drive_upload","image_id":"a1b2c3d4","size_bytes":"2457600","extra":"x"}` ho `from_json(..., EVENT_SCHEMA)` sai liña ho `kind="drive_upload"`, `image_id="a1b2c3d4"`, `size_bytes="2457600"` no kampu seluk hotu `null`. Kampu `extra` la iha eskema, entaun Spark soe nia. Se testu la'ós JSON válidu, `from_json` fó `null` ba struct tomak.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `EVENT_SCHEMA` | `pyspark.sql.types.StructType` (19 kampu, hotu `StringType`) |
| `DRIVE_UPLOAD` | `str` |
| `build_spark()` return | `SparkSession` (inferidu) |

**7. Ligasaun OOP**

`StructType`, `StructField`, `StringType` mak klase PySpark; kódigu harii **objetu** husi sira (instánsia). Ne'e **komposisaun** iha biblioteka: `StructType` iha lista objetu `StructField`. Projetu la define klase foun iha ne'e.

**8. Impaktu integrasaun**

Naran kampu tenke hanesan ho chave iha `build_drive_event` (`kafka/drive_watcher.py`) no `build_upload_event` (`storage/pipeline_submit.py`) — haree Modulu 02. `EVENT_SCHEMA` uza iha `parse_events` (liña 87).

**9. Checkpoint ba principiante**

**P:** Tanba sá `size_bytes` mak `StringType` maske nia númeru?
**R:** Produtór haruka valór hotu hanesan testu ka `None` (`build_drive_event` hakerek `str(meta["size"])`). Eskema simples (hotu string) evita erru parsing; konversaun tipu halo depois bainhira presiza.

#### D.2.4 Funsaun `parse_events` — parsing JSON no validasaun Spark SQL

**1. Kódigu orijinál**

{{code spark/streaming_job.py 83-112}}

**2. Objetivu**

Konverte mensajen Kafka (bytes) ba liña ho koluna, limpa `location`, no kalkula `rejected_reason` (razaun rejeita) ho regra Spark SQL. Eventu aat **la soe** — nia hetan razaun no sei rai hanesan `FAILED`.

**3. Fundamentu Python**

- **Funsaun iha funsaun nia laran** (`def missing(field)`) — funsaun ajuda ne'ebé só uza iha `parse_events`.
- **Operadór `|`, `&`, `~` iha koluna Spark** — la'ós `or/and/not` Python. Spark sobrekarga (overload) operadór sira ne'e atu harii espresaun: `|` = OR, `&` = AND, `~` = NOT.
- **Loop `for` ne'ebé harii espresaun**: kada volta aumenta kondisaun ida ba cadeia `when`.
- **`when(lit(False), lit(None))`** — truk atu hahú cadeia `when` ne'ebé nunka loos; depois `.when(...)` aumenta kondisaun. Kondisaun dahuluk ne'ebé loos manán.
- **f-string iha loop**: `f"Missing required field: {field}"`.

**4. Esplikasaun liña-ba-liña**

- **Liña 83** — `def parse_events(raw):` — simu `raw`, DataFrame streaming husi Kafka (koluna `key`, `value`, `topic`, `partition`, `offset`, `timestamp`...).
- **Liña 84–85** — Docstring: Kafka JSON → liña ho tipu, validadu ho Spark SQL; eventu aat la soe, hetan `rejected_reason` no rai `FAILED` depois.
- **Liña 86** — `parsed = raw.select(` — hili koluna foun.
- **Liña 87** — `from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")` — `value` mak bytes; `.cast("string")` konverte ba testu; `from_json` parse ho eskema; rezultadu struct naran `data`.
- **Liña 88** — `col("value").cast("string").alias("raw_value")` — rai mós testu orijinál iha `raw_value` (atu rai eventu rejeitadu).
- **Liña 89** — `).select("data.*", "raw_value")` — "loke" struct: kampu hotu husi `data` sai koluna rasik, plus `raw_value`.
- **Liña 91** — `def missing(field):` — funsaun ajuda.
- **Liña 92** — `return col(field).isNull() | (col(field) == "")` — espresaun: koluna null **ka** string mamuk.
- **Liña 94** — `is_upload = coalesce(col("kind"), lit("")) == DRIVE_UPLOAD` — `coalesce` troka null ho `""` atu komparasaun la fó null; espresaun loos se eventu mak `drive_upload`.
- **Liña 95** — `rejected_reason = when(lit(False), lit(None))` — hahú cadeia ne'ebé nunka loos (valór null).
- **Liña 96** — `for field in ("image_id", "image_path", "uploaded_at"):` — kampu obrigatóriu ba eventu hotu.
- **Liña 97** — `rejected_reason = rejected_reason.when(missing(field), lit(f"Missing required field: {field}"))` — se kampu falta, razaun mak "Missing required field: <naran>".
- **Liña 98** — `rejected_reason = (rejected_reason` — kontinua cadeia.
- **Liña 99** — `.when(is_upload & missing("owner_email"), ...)` — eventu `drive_upload` tenke iha `owner_email`.
- **Liña 100** — `.when(~is_upload & missing("user_id"), ...))` — eventu seluk (`submit`) tenke iha `user_id`.
- **Liña 101** — `rejected_reason = rejected_reason.when(` — kondisaun ikus:
- **Liña 102** — `to_timestamp(col("captured_at")) > current_timestamp(),` — tempu foti fotografia iha futuru.
- **Liña 103** — `concat(lit("captured_at is in the future: "), col("captured_at")),` — razaun ho valór.
- **Liña 104** — `)` — taka.
- **Liña 106** — `return (` — fó fali DataFrame foun.
- **Liña 107** — `parsed` — hahú husi DataFrame parsed.
- **Liña 108** — `.withColumn("location", initcap(regexp_replace(trim(col("location")), r"\s+", " ")))` — limpeza: `trim` hasai espasu iha rohan, `regexp_replace` troka espasu barak ba ida, `initcap` halo Letra Boot iha liafuan kada ida.
- **Liña 109** — `.withColumn("rejected_reason", rejected_reason)` — aumenta koluna razaun (null se eventu di'ak).
- **Liña 110** — Komentáriu: rai testu orijinál só ba eventu rejeitadu.
- **Liña 111** — `.withColumn("raw_value", when(col("rejected_reason").isNotNull(), col("raw_value")))` — `when` la ho `otherwise` fó null; entaun eventu di'ak la lori `raw_value` (poupa memória).
- **Liña 112** — `)` — remata.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* — eventu tolu iha batch ida:

| JSON `value` | `location` depois | `rejected_reason` |
|---|---|---|
| `{"kind":"drive_upload","image_id":"a1","image_path":"gdrive:X","uploaded_at":"2026-10-20T01:15:00Z","owner_email":"a@example.com","location":"  aileu   vila "}` | `"Aileu Vila"` | `null` |
| `{"kind":"submit","image_id":"b2","image_path":"p.jpg","uploaded_at":"2026-10-20T01:00:00Z"}` | `null` | `"Missing required field: user_id"` |
| `{"kind":"drive_upload","image_path":"gdrive:Y","uploaded_at":"..."}` | `null` | `"Missing required field: image_id"` |

Kondisaun dahuluk ne'ebé loos manán: eventu terseiru falta `image_id` no `owner_email`, maibé razaun mak `image_id` tanba nia mai uluk iha cadeia.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `raw` | `pyspark.sql.DataFrame` (streaming) — la iha hint, inferidu |
| `parsed` | `DataFrame` ho 19 koluna eskema + `raw_value` |
| `missing(field)` | parámetru `str`, return `Column` |
| `is_upload`, `rejected_reason` | `pyspark.sql.Column` (espresaun, la'ós valór) |
| return | `DataFrame` ho koluna `rejected_reason` no `raw_value` |
| Exception | la iha; JSON aat fó null, la'ós erru |

**7. Ligasaun OOP**

Objetu `Column` husi PySpark sobrekarga operadór `|`, `&`, `~`, `==`, `>` (métodu espesiál `__or__`, `__and__`, `__invert__`, `__eq__`, `__gt__`). Ne'e **polimorfizmu operadór** iha biblioteka. Projetu uza nia; projetu la define klase iha bloku ne'e.

**8. Impaktu integrasaun**

`run()` (liña 344) bolu `parse_events(raw)`. DataFrame ne'ebé fó fali ba `writeStream.foreachBatch(foreach_batch_sink)`. `rejected_reason` uza iha `_process_one` (liña 266) no `process_and_persist` (liña 208–209): eventu rejeitadu sai `FAILED` ho razaun. Regra limpeza `location` hanesan ho `normalize_location` (preprocessing liña 37–42) no `_location` (batch_analytics liña 85–89).

**9. Checkpoint ba principiante**

**P:** Tanba sá kódigu uza `|` no la'ós `or`?
**R:** `col(field).isNull()` la'ós `True/False` Python — nia mak espresaun Spark (`Column`). `or` Python la bele kombina espresaun; `|` mak operadór ne'ebé `Column` sobrekarga atu harii espresaun OR.

#### D.2.5 `_DOWNLOADS` no hahú `process_event` (validasaun no download paralelu)

**1. Kódigu orijinál**

{{code spark/streaming_job.py 115-143}}

**2. Objetivu**

Hahú prosesa fotografia ida: prepara medida tempu kada pasu, hahú download fotografia husi Drive **iha tempu hanesan** ho validasaun eventu (atu poupa tempu), no se validasaun falla, kansela download.

**3. Fundamentu Python**

- **Closure + `nonlocal`**: `lap` muda variavel `clock` husi funsaun `process_event`.
- **`dict` vazia `{}`** rai tempu kada pasu.
- **Future**: `_DOWNLOADS.submit(...)` fó objetu `Future`; `download.cancel()` koko kansela se seidauk hahú.
- **`raise` mesak** iha `except`: hamosu fali erru hanesan (re-raise) depois halo limpeza.
- **`or ""`**: `event.get("image_path") or ""` fó `""` se valór mak `None` ka mamuk.

**4. Esplikasaun liña-ba-liña**

- **Liña 115** — `_DOWNLOADS = ThreadPoolExecutor(max_workers=16, thread_name_prefix="drive-download")` — grupu thread 16 ba download Drive, harii dala ida bainhira modulu import. Naran thread hahú ho `drive-download`.
- **Liña 118** — `def process_event(event: dict) -> dict:` — simu eventu (dict), fó fali rezultadu (dict).
- **Liña 119–120** — Docstring: Spark preprocess, CNN klasifika, depois risku. Fó rezultadu atu rai; hamosu erru se falla (**nunka** rezultadu falsu).
- **Liña 121** — `timings = {}` — segundu kada pasu, ba relatóriu eskalabilidade.
- **Liña 122** — `clock = time.perf_counter()` — relójiu presizaun aas (segundu, `float`).
- **Liña 124** — `def lap(name):` — funsaun ki'ik: rai tempu desde `lap` anterior.
- **Liña 125** — `nonlocal clock` — uza `clock` husi `process_event`.
- **Liña 126** — `now = time.perf_counter()` — tempu agora.
- **Liña 127** — `timings[name] = round(now - clock, 4)` — rai diferensa ho desimál 4.
- **Liña 128** — `clock = now` — reset relójiu ba pasu tuirmai.
- **Liña 130** — Komentáriu: hahú download Drive bainhira kontrola eventu; uza kópia lokál se iha.
- **Liña 131** — `download = None` — seidauk iha download.
- **Liña 132** — `local_copy = None` — seidauk iha kópia lokál.
- **Liña 133** — `if is_drive_path(event.get("image_path") or ""):` — dalan hahú ho `gdrive:`?
- **Liña 134** — `local_copy = downloaded_photo_path(event.get("user_id"), event.get("image_id"))` — buka kópia ne'ebé download ona (`storage/downloaded_photos/`); `None` se la iha.
- **Liña 135** — `if local_copy is None:` — se la iha kópia lokál:
- **Liña 136** — `download = _DOWNLOADS.submit(load_image_bytes, event["image_path"])` — hahú download iha thread background. Kódigu kontinua kedas, la hein.
- **Liña 137** — `try:` — koko validasaun.
- **Liña 138** — `event = clean_event(event)` — validasaun no limpeza (D.3.3). Bele hamosu `InvalidEvent`.
- **Liña 139** — `except Exception:` — se erru saida de'it:
- **Liña 140** — `if download is not None:` — se download hahú ona:
- **Liña 141** — `download.cancel()` — koko kansela (se thread seidauk hahú; se hahú ona, nia remata maibé rezultadu la uza).
- **Liña 142** — `raise` — hamosu fali erru orijinál ba funsaun ne'ebé bolu.
- **Liña 143** — `lap("validate_s")` — rai tempu validasaun.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* eventu `image_path = "gdrive:1AbC"`, la iha kópia lokál. Liña 136 hahú download (thread `drive-download_0`). Paralelu, `clean_event` konsulta base de dadus (~0.05 s). Se validasaun OK, `timings = {"validate_s": 0.0512}`. Download kontinua iha background no sei uza iha liña 150. Se `clean_event` hamosu `InvalidEvent("Unknown user_id: ...")`, download kansela no erru sa'e ba `process_and_persist`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `event` | `dict` (esplísitu) — chave `str`, valór `str` ka `None` |
| `timings` | `dict[str, float]` (inferidu) |
| `clock`, `now` | `float` |
| `download` | `concurrent.futures.Future` ka `None` |
| `local_copy` | `pathlib.Path` ka `None` |
| Exception | `InvalidEvent` ka erru seluk husi `clean_event` (re-raise) |

**7. Ligasaun OOP**

`_DOWNLOADS` mak **objetu** `ThreadPoolExecutor` ne'ebé modulu na'in (komposisaun nível modulu): moris bainhira modulu import no moris to'o prosesu remata. `Future` mak objetu ne'ebé reprezenta rezultadu futuru.

**8. Impaktu integrasaun**

`process_and_persist` (liña 210) bolu `process_event`. Funsaun ne'e bolu `downloaded_photo_path` no `load_image_bytes` (`storage/image_storage.py`, haree Modulu 03), no `clean_event` (D.3.3).

**9. Checkpoint ba principiante**

**P:** Tanba sá download hahú **molok** validasaun?
**R:** Atu poupa tempu: download Drive neineik, validasaun mós presiza tempu. Halo rua iha paralelu. Se validasaun falla, download kansela.

#### D.2.6 `process_event` — foti bytes, duplikadu, EXIF, preprocessing no CNN

**1. Kódigu orijinál**

{{code spark/streaming_job.py 144-169}}

**2. Objetivu**

Foti bytes fotografia (husi kópia lokál, download ka disku), kontrola duplikadu, lee EXIF, **prepara imajen** no harii tensor ba CNN, rai imajen ne'ebé prosesa, no halai CNN.

**3. Fundamentu Python**

- **`if / elif / else`** ho dalan tolu ba fonte bytes.
- **Dict comprehension** iha `timings.update({...})`.
- **`.get(k) or {}`**: se `inference` la iha ka `None`, uza dict mamuk.

**4. Esplikasaun liña-ba-liña**

- **Liña 144** — `scale_run = scale_test_run_for(event)` — se eventu mak husi teste eskalabilidade, fó objetu `ScaleTestRun`; se lae, `None` (D.3.7).
- **Liña 146** — `prediction_started_at = datetime.now(timezone.utc)` — tempu hahú analiza (UTC).
- **Liña 147** — `if local_copy is not None:` — iha kópia lokál:
- **Liña 148** — `original_bytes = local_copy.read_bytes()` — lee arkivu tomak hanesan `bytes`.
- **Liña 149** — `elif download is not None:` — se download hahú:
- **Liña 150** — `original_bytes = download.result()` — hein to'o download remata; se download falla, erru sa'e iha ne'e.
- **Liña 151** — `save_downloaded_photo(event["user_id"], event["image_id"], original_bytes)` — rai kópia lokál (nunka halo diagnóstiku falla, D.3.8).
- **Liña 152** — `else:` — fotografia lokál (la'ós Drive):
- **Liña 153** — `original_bytes = Path(event["image_path"]).read_bytes()` — lee husi disku.
- **Liña 154** — `lap("read_s")` — tempu lee/download.
- **Liña 155** — `if scale_run is None:` — la'ós teste (teste uza fotografia hanesan dala barak ho intensaun):
- **Liña 156** — `check_duplicate_by_hash(event, original_bytes)` — se fotografia hanesan ho ida ne'ebé iha ona, hamosu erru `DuplicateImage` (haree Modulu 04/06).
- **Liña 157** — `extract_metadata(event, original_bytes)` — lee EXIF husi **orijinál** (kámara, GPS, tempu foti) no rai iha liña `images` (D.3.5).
- **Liña 158** — `lap("metadata_s")`.
- **Liña 159** — `image_bytes = prepare_image(original_bytes)` — imajen vertikál, maximu 1280 px, JPEG la ho EXIF (D.3.7).
- **Liña 160** — `cnn_input = model_input(original_bytes, image_bytes)` — tensor normalizadu ba CNN (D.3.6).
- **Liña 161** — `if scale_run is None:` — la'ós teste:
- **Liña 162** — `store_processed_image(event["image_id"], original_bytes, image_bytes)` — rai kuadradu ne'ebé CNN haree hanesan PNG (D.3.8).
- **Liña 163** — `lap("preprocess_s")`.
- **Liña 164** — `features = run_model(cnn_input)` — CNN klasifika; fó dict ho klase, konfiansa, feature (Modulu 05).
- **Liña 165** — `lap("cnn_s")`.
- **Liña 166** — `used = features.get("inference") or {}` — informasaun teknika kona-ba inferénsia (batch, presizaun, tempu).
- **Liña 167** — `timings.update({f"cnn_{k}": used.get(k) for k in ("batch_size", "precision", "input_s", "forward_s")})` — aumenta 4 valór ho prefiksu `cnn_`.
- **Liña 168** — `if scale_run is None:`
- **Liña 169** — `save_for_review(event["image_id"], image_bytes)` — kopia fotografia (la ho EXIF/GPS) ba pasta revizaun ba pájina "Label photos" (D.3.8).

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fotografia 4000×3000 JPEG (2.4 MB) husi Drive. `download.result()` fó `bytes` 2,457,600. `extract_metadata` hakerek `camera_make="samsung"`. `prepare_image` fó JPEG 1280×960. `model_input` fó tensor `[3, S, S]`. `run_model` fó `{"predicted_class": "blight", "predicted_class_confidence": 0.97, "lesion_score": 0.97, "is_uncertain": False, "inference": {"batch_size": 1, ...}}`. `timings` sai `{"validate_s": 0.05, "read_s": 1.2, "metadata_s": 0.03, "preprocess_s": 0.15, "cnn_s": 0.08, "cnn_batch_size": 1, ...}`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `scale_run` | `ScaleTestRun` (objetu ORM) ka `None` |
| `prediction_started_at` | `datetime` (aware, UTC) |
| `original_bytes`, `image_bytes` | `bytes` |
| `cnn_input` | `torch.Tensor` (float32, forma `[3, S, S]`) — inferidu husi `transforms.ToTensor` |
| `features` | `dict` |
| `used` | `dict` |
| Exception | erru download, `DuplicateImage`, `ModelNotAvailable` — sa'e ba `process_and_persist` |

**7. Ligasaun OOP**

`local_copy` mak objetu `Path`; `.read_bytes()` mak métodu objetu. `scale_run` mak objetu ORM (instánsia klase `ScaleTestRun`, Modulu 04). Funsaun ne'e kombina objetu barak maibé la define klase.

**8. Impaktu integrasaun**

Ne'e mak fatin ne'ebé komponente sira hasoru: Spark (preprocessing) → CNN (`ml/inference`). Rezultadu `features` uza iha D.2.7 ba risku no rezultadu final.

**9. Checkpoint ba principiante**

**P:** Tanba sá `extract_metadata` uza `original_bytes` no la'ós `image_bytes`?
**R:** `prepare_image` hasai EXIF. Informasaun kámara, GPS no tempu foti só iha fotografia orijinál.

#### D.2.7 `process_event` — risku, dict rezultadu no imajen ba agrikultór

**1. Kódigu orijinál**

{{code spark/streaming_job.py 171-200}}

**2. Objetivu**

Kalkula risku, harii dict rezultadu ne'ebé sei rai iha base de dadus, no (ba fotografia Drive) desenha imajen ho *heatmap* ba agrikultór. Erru desenhu nunka halo diagnóstiku falla.

**3. Fundamentu Python**

- **Tuple unpacking**: `risk_score, risk_level = compute_risk(...)`.
- **Dict literal ho `**`**: `**{k: features.get(k) for k in FEATURE_FIELDS}` hatama pár barak husi dict comprehension ba dict `result`.
- **Espresaun kondisionál** (ternary): `A if kondisaun else B`.
- **`try/except` lokál** atu erru ki'ik la estraga rezultadu prinsipál.

**4. Esplikasaun liña-ba-liña**

- **Liña 171** — `is_uncertain = features.get("is_uncertain", False)` — CNN la seguru? Default `False`.
- **Liña 172** — `risk_score, risk_level = compute_risk(features, is_uncertain=is_uncertain)` — `ml/risk.py`: score = `lesion_score` (probabilidade moras) entre 0–1; nivel `HIGH/MEDIUM/LOW` tuir `RISK_THRESHOLDS`; uncertain nunka `HIGH`.
- **Liña 174** — `predicted_at = datetime.now(timezone.utc)` — tempu remata prediksaun.
- **Liña 176** — `result = {` — hahú dict rezultadu.
- **Liña 177** — `"image_id": event["image_id"],`
- **Liña 178** — `"predicted_class": features.get("predicted_class"),` — klase CNN.
- **Liña 179** — `"prediction_confidence": features.get("predicted_class_confidence"),` — konfiansa (0–1).
- **Liña 180** — `**{k: features.get(k) for k in FEATURE_FIELDS},` — aumenta `lesion_score`, `discoloration_pct`, `texture_irregularity`, `leaf_area_affected_pct`.
- **Liña 181** — `"risk_score": risk_score,`
- **Liña 182** — `"risk_level": risk_level,`
- **Liña 183** — `"is_uncertain": is_uncertain,`
- **Liña 184** — `"model_version": get_model_version(),` — versaun modelu ne'ebé uza (rastreabilidade).
- **Liña 185** — `"predicted_at": predicted_at,`
- **Liña 186** — `"prediction_started_at": prediction_started_at,`
- **Liña 187** — `"timings": timings,` — tempu kada pasu.
- **Liña 188** — `}` — remata dict.
- **Liña 189** — `lap("risk_s")`.
- **Liña 190–191** — `wants_processed_image = (scale_run.publish_results if scale_run is not None else is_drive_path(event["image_path"]))` — ba teste: tuir konfigurasaun run; ba fotografia normál: só fotografia Drive presiza imajen.
- **Liña 192** — `if wants_processed_image:`
- **Liña 193** — Komentáriu: tahan ho marka ba agrikultór; erru desenhu nunka halo diagnóstiku falla.
- **Liña 194** — `try:`
- **Liña 195** — `heatmap = heatmap_on_photo(disease_heatmap(cnn_input), image_bytes) if not is_uncertain else None` — se CNN seguru, kalkula mapa (Grad-CAM) fatin ne'ebé moras no tau iha fotografia; se uncertain, la iha heatmap.
- **Liña 196** — `result["processed_image_jpeg"] = render_processed_image(image_bytes, result, heatmap)` — desenha imajen final (JPEG bytes) ho testu rezultadu.
- **Liña 197** — `except Exception as e:` — erru saida de'it iha desenhu:
- **Liña 198** — `print(f"[streaming_job] Could not draw processed image for {event['image_id']}: {e}")` — hakerek mensajen; rezultadu kontinua la ho imajen.
- **Liña 199** — `lap("render_s")`.
- **Liña 200** — `return result` — fó fali dict rezultadu.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `features = {"predicted_class": "blight", "predicted_class_confidence": 0.97, "lesion_score": 0.97, "is_uncertain": False, ...}`. `compute_risk` → `score = round(min(max(0.97, 0), 1), 3) = 0.97`; nivel = `"HIGH"` tanba `0.97 >= RISK_THRESHOLDS["high"]` (default `0.6`; `"medium"` default `0.3`, `config/settings.py` liña 99–102). `result = {"image_id": "a1b2c3d4", "predicted_class": "blight", "prediction_confidence": 0.97, "lesion_score": 0.97, ..., "risk_score": 0.97, "risk_level": "HIGH", "is_uncertain": False, "model_version": "...", "processed_image_jpeg": b"\xff\xd8..."}`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `is_uncertain` | `bool` |
| `risk_score` | `float` (0–1, desimál 3) |
| `risk_level` | `str` (`"LOW"`, `"MEDIUM"`, `"HIGH"`) |
| `result` | `dict[str, object]` |
| `wants_processed_image` | `bool` |
| `heatmap` | objetu imajen husi `ml/render` ka `None` (inferidu) |
| return | `dict` (esplísitu `-> dict`) |

**7. Ligasaun OOP**

`scale_run.publish_results` mak asesu **atributu** husi objetu ORM `ScaleTestRun`. Seluk mak funsaun no dict — la iha klase foun.

**8. Impaktu integrasaun**

`result` fila ba `process_and_persist` → `persist_result` (rai iha `predictions`), no `processed_image_jpeg` ba `_upload_result` (Drive). Chave `timings` uza iha `live_log.photo_line`.

**9. Checkpoint ba principiante**

**P:** Se `render_processed_image` hamosu erru, fotografia sai `FAILED`?
**R:** Lae. Erru kaer iha `except` liña 197; rezultadu fila la ho `processed_image_jpeg`, no fotografia kontinua `COMPLETED`.

#### D.2.8 Funsaun `process_and_persist`

**1. Kódigu orijinál**

{{code spark/streaming_job.py 203-220}}

**2. Objetivu**

Halai `process_event` no rai rezultadu ho status `COMPLETED` ka `FAILED`. Funsaun ne'e **nunka** hamosu erru — nia fó fali tuple `(result, status, error)`.

**3. Fundamentu Python**

- **`**stamps`** iha definisaun: simu argumentu ho naran barak (ezemplu `kafka_received_at=...`) iha dict `stamps`; iha chamada `persist_result(..., **stamps)` haruka fali hotu.
- **`isinstance(e, InvalidEvent)`**: kontrola tipu objetu erru.
- **`try` iha `except` nia laran**: atu erru segundu la estraga funsaun.
- **Return tuple**: `return result, status, None` fó valór tolu.

**4. Esplikasaun liña-ba-liña**

- **Liña 203** — `def process_and_persist(event: dict, rejected_reason: str = None, **stamps) -> tuple:`
- **Liña 204–205** — Docstring: `process_event`, depois rai `COMPLETED` ka `FAILED`. Nunka hamosu erru. Fó `(result, status, error)`.
- **Liña 206** — `result = None` — default se prosesa falla.
- **Liña 207** — `try:`
- **Liña 208** — `if rejected_reason:` — eventu rejeitadu ona iha `parse_events` ka `register_upload`:
- **Liña 209** — `raise InvalidEvent(rejected_reason)` — hamosu erru atu ba dalan `FAILED`.
- **Liña 210** — `result = process_event(event)` — prosesa fotografia.
- **Liña 211** — `persist_result(event, result, ProcessingStatus.COMPLETED, **stamps)` — rai prediksaun + status `COMPLETED` (Modulu 04).
- **Liña 212** — `return result, ProcessingStatus.COMPLETED, None`.
- **Liña 213** — `except Exception as e:` — erru saida de'it.
- **Liña 214–215** — `print(...)` — mensajen "Invalid event, skipping" ba `InvalidEvent`, "Processing failed for <id>" ba erru seluk.
- **Liña 216** — `try:`
- **Liña 217** — `persist_result(event, None, ProcessingStatus.FAILED, error_message=str(e), **stamps)` — rai status `FAILED` ho mensajen erru.
- **Liña 218** — `except Exception as db_error:` — se base de dadus mós falla; komentáriu: fotografia hela `PROCESSING`, `requeue_stuck` sei koko fali.
- **Liña 219** — `print(...)` — "Could not record the failure".
- **Liña 220** — `return result, ProcessingStatus.FAILED, str(e)`.

**5. Walkthrough ezekusaun**

- Eventu di'ak → `(result_dict, ProcessingStatus.COMPLETED, None)`.
- Eventu ho `rejected_reason = "Missing required field: user_id"` → liña 209 hamosu `InvalidEvent`; print "Invalid event, skipping: Missing required field: user_id"; `persist_result(... FAILED ...)`; return `(None, ProcessingStatus.FAILED, "Missing required field: user_id")`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `event` | `dict` |
| `rejected_reason` | `str` ka `None` |
| `stamps` | `dict[str, datetime]` (`kafka_received_at`, `spark_started_at`) |
| return | `tuple` — `(dict ka None, ProcessingStatus, str ka None)` |
| Exception | nunka sai husi funsaun (hotu kaer) |

**7. Ligasaun OOP**

`InvalidEvent` mak klase ne'ebé **herda** husi `Exception` (D.3.2). `isinstance(e, InvalidEvent)` hatudu **polimorfizmu** iha exception: `except Exception` kaer erru hotu, depois kódigu hatene tipu espesífiku. `ProcessingStatus.COMPLETED` mak membru enum.

**8. Impaktu integrasaun**

Bolu husi `_process_one` (liña 295). `persist_result` iha `storage/results.py` rai iha tabela `predictions`, `images`, `processing_events` (Modulu 04). Tuple ne'ebé fó fali uza ba `live_log.photo_done` no upload Drive.

**9. Checkpoint ba principiante**

**P:** Tanba sá iha `try` segundu iha `except` nia laran?
**R:** Se base de dadus mós la funsiona, `persist_result` FAILED bele hamosu erru. `try` segundu kaer nia atu funsaun nunka monu — fotografia hela `PROCESSING` no `requeue_stuck` koko fali depois.

#### D.2.9 `foreach_batch_sink`, `_UPLOADS` no `_upload_result`

**1. Kódigu orijinál**

{{code spark/streaming_job.py 223-256}}

**2. Objetivu**

`foreach_batch_sink` mak funsaun ne'ebé Spark bolu ba kada micro-batch. Nia hasai duplikadu, konverte liña Spark ba dict Python, prosesa fotografia (paralelu), hein upload Drive hotu, no hakerek sumáriu. Bainhira funsaun ne'e remata, Spark hakerek checkpoint (batch "commit").

**3. Fundamentu Python**

- **List comprehension**: `[row.asDict() for row in deduped.collect()]`.
- **`with ThreadPoolExecutor(...) as pool:`** — pool taka automátiku depois bloku.
- **`zip(a, b)`** — kombina lista rua elementu-ba-elementu.
- **`list.count(x)`** — konta dala hira `x` iha lista.
- **Walrus `:=`** — `if resent := resend_duplicates_of(failed):`.
- **`min(a, b)`, `max(1, x)`** — valór ki'ik/boot liu.

**4. Esplikasaun liña-ba-liña**

- **Liña 223** — `def foreach_batch_sink(batch_df, batch_id):` — Spark fó DataFrame batch no númeru batch.
- **Liña 224** — `if batch_df.isEmpty():` — kontrola iha JVM; komentáriu: `.rdd` sei hahú worker Python (monu iha Windows).
- **Liña 225** — `return` — batch mamuk, la iha serbisu.
- **Liña 226** — Komentáriu: hasai duplikadu tuir `image_id`; liña la iha `image_id` mantein, kada ida rejeita mesak.
- **Liña 227** — `named = col("image_id").isNotNull() & (col("image_id") != "")` — espresaun: iha `image_id`.
- **Liña 228** — `deduped = batch_df.filter(named).dropDuplicates(["image_id"]).unionByName(batch_df.filter(~named))` — liña ho `image_id`: hela ida kada `image_id`; liña la ho `image_id`: hotu; kombina tuir naran koluna.
- **Liña 229** — `events = [row.asDict() for row in deduped.collect()]` — `collect()` lori liña hotu husi Spark ba Python (lista `Row`); `asDict()` konverte ba dict.
- **Liña 230** — `started = time.monotonic()` — relójiu ne'ebé nunka fila ba kotuk.
- **Liña 231** — `live_log.batch_started(batch_id, len(events))` — liña konsola "batch N: X photo event(s) received from Kafka".
- **Liña 232** — `if len(events) <= 1 or SPARK_PARALLEL_PHOTOS <= 1:` — eventu ida de'it ka paralelu hamate:
- **Liña 233** — `uploads = [_process_one(event) for event in events]` — prosesa ida-ida.
- **Liña 234** — `else:`
- **Liña 235** — `with ThreadPoolExecutor(max_workers=min(SPARK_PARALLEL_PHOTOS, len(events))) as pool:` — pool ho thread = minimu entre 8 no númeru eventu.
- **Liña 236** — `uploads = list(pool.map(_process_one, events))` — prosesa paralelu; `list(...)` hein hotu remata no rai rezultadu tuir orden eventu.
- **Liña 237** — Komentáriu: commit batch só depois upload Drive hotu koko ona, atu crash halo replay.
- **Liña 238** — `statuses = [upload.result() if upload is not None else None for upload in uploads]` — hein kada `Future` upload; eventu ne'ebé salta (`None`) fó `None`.
- **Liña 239** — `failed = [e.get("image_id") for e, s in zip(events, statuses) if s == ProcessingStatus.FAILED and e.get("image_id")]` — lista `image_id` ne'ebé falla.
- **Liña 240** — `if resent := resend_duplicates_of(failed):` — fotografia ne'ebé rejeita tanba duplikadu ba ida ne'ebé falla, haruka fali atu hetan rezultadu rasik.
- **Liña 241** — `live_log.say(...)` — hakerek kuantu haruka fali.
- **Liña 242–244** — `live_log.batch_committed(batch_id, ok=..., failed=..., skipped=..., seconds=...)` — sumáriu: COMPLETED, FAILED, salta, no segundu.
- **Liña 247** — `_UPLOADS = ThreadPoolExecutor(max_workers=max(1, SPARK_DRIVE_UPLOAD_WORKERS), thread_name_prefix="drive-upload")` — pool upload (default 16), minimu 1.
- **Liña 250** — `def _upload_result(event, status, error, processed_image):`
- **Liña 251** — Docstring: haruka rezultadu ida ba Drive; nunka hamosu erru; fó status fotografia.
- **Liña 252** — `started = time.monotonic()`.
- **Liña 253** — `file_id = publish_result_to_drive_safely(event, status, error, processed_image=processed_image)` — hakerek rezultadu ba Drive; `None` se falla (Modulu 03).
- **Liña 254** — `if is_drive_path(event.get("image_path") or ""):` — só fotografia Drive:
- **Liña 255** — `live_log.result_delivered(event, file_id is not None, time.monotonic() - started)` — liña "result delivered" ka "NOT delivered".
- **Liña 256** — `return status` — status fotografia (ba konta iha liña 242–244).

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* batch 42 ho eventu 3, rua ho `image_id="a1"` (repete) no ida `"b2"`. `dropDuplicates` → eventu 2. Konsola: `[spark 09:15:02] [kafka]  batch 42: 2 photo event(s) received from Kafka`. Pool ho thread 2 prosesa `a1` no `b2`. Kada `_process_one` fó `Future` upload. `statuses = [COMPLETED, COMPLETED]`. Konsola: `[spark]  batch 42 committed: 2 ok, 0 failed, 0 skipped in 3.41s`. Spark hakerek checkpoint offset batch 42.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `batch_df` | `pyspark.sql.DataFrame` (estátiku, la'ós streaming) |
| `batch_id` | `int` |
| `events` | `list[dict]` |
| `uploads` | `list[Future ka None]` |
| `statuses` | `list[ProcessingStatus ka None]` |
| `failed` | `list[str]` |
| `resent` | `int` |
| `_upload_result` return | `ProcessingStatus` |

**7. Ligasaun OOP**

`row.asDict()` mak métodu objetu `Row` PySpark. `_UPLOADS` mak objetu pool ne'ebé modulu na'in. `Future` sira hatudu **komposisaun** temporária: lista `uploads` kaer objetu `Future` to'o `result()`.

**8. Impaktu integrasaun**

`run()` liña 348 rejista funsaun ne'e ho `.foreachBatch(foreach_batch_sink)`. Ne'e mak **sink** — fatin ne'ebé dadus streaming sai husi Spark. Rezultadu la hakerek ho Spark JDBC iha ne'e: `persist_result` uza SQLAlchemy (ORM) ba PostgreSQL. Spark hakerek checkpoint depois funsaun remata.

**9. Checkpoint ba principiante**

**P:** Tanba sá kódigu hein `upload.result()` molok remata funsaun?
**R:** Bainhira funsaun remata, Spark marka batch hanesan "commit" (la lee fali). Se laptop mate molok upload remata, rezultadu la to'o Drive. Hein uluk garante katak crash halo batch repete (replay).

#### D.2.10 Funsaun `_process_one`

**1. Kódigu orijinál**

{{code spark/streaming_job.py 259-299}}

**2. Objetivu**

Prosesa fotografia ida husi batch: salta eventu teste antigu, rejista upload Drive foun, kontrola replay (prosesa ona?), marka `PROCESSING`, halai `process_and_persist`, hakerek liña konsola, no haruka upload Drive ba thread background.

**3. Fundamentu Python**

- **`dict.pop(chave, default)`**: hasai chave husi dict no fó nia valór.
- **`all(...)` ho generator**: `all(event.get(f) for f in (...))` loos se kampu hotu iha valór.
- **Tuple assignment**: `outcome, rejected_reason = "new", str(e)`.
- **`return None` sedu** (early return) atu salta.

**4. Esplikasaun liña-ba-liña**

- **Liña 259** — `def _process_one(event: dict):`
- **Liña 260–261** — Docstring: fotografia ida; upload Drive iha background; fó `Future` ka `None` (salta). Nunka hamosu erru.
- **Liña 262** — `if event.get("run_id"):` — eventu teste eskalabilidade antigu:
- **Liña 263–264** — `print(...)` — hakerek katak salta.
- **Liña 265** — `return None`.
- **Liña 266** — `rejected_reason = event.pop("rejected_reason", None)` — hasai razaun husi dict (atu la ba base de dadus hanesan kampu).
- **Liña 267** — `raw_value = event.pop("raw_value", None)` — hasai testu orijinál.
- **Liña 268** — `if rejected_reason and not event.get("image_id"):` — rejeitadu no la iha `image_id`:
- **Liña 269** — `event["raw_value"] = raw_value` — tau fali testu orijinál atu `persist_result` rai iha `rejected_events`.
- **Liña 270–271** — `if (event.get("kind") == DRIVE_UPLOAD and all(event.get(f) for f in ("image_id", "image_path", "owner_email"))):` — eventu Drive ho kampu importante hotu:
- **Liña 272** — Komentáriu: upload ne'ebé rejeita sei hetan liña no falla iha okos ho razaun.
- **Liña 273** — `try:`
- **Liña 274** — `outcome = register_upload(event)` — rejista iha `images` (`storage/uploads.py`); fó `"new"`, `"duplicate"` ka `"conflict"`; mós hatama `user_id`.
- **Liña 275** — `except InvalidEvent as e:` — upload rejeitadu (ezemplu email la rejista):
- **Liña 276** — `outcome, rejected_reason = "new", str(e)` — kontinua hanesan foun maibé ho razaun rejeita.
- **Liña 277** — `if outcome != "new":`
- **Liña 278–279** — `why = ("already registered" if outcome == "duplicate" else "its image_id belongs to another Drive file (see rejected_events)")`.
- **Liña 280–281** — `print(...)` — hakerek razaun salta.
- **Liña 282** — `return None`.
- **Liña 283** — `state = replay_state(event.get("image_id"))` — `"completed"`, `"result_missing"` ka `None`.
- **Liña 284** — `if state == "completed":`
- **Liña 285** — `print(...)` — "already COMPLETED, skipping replayed event".
- **Liña 286** — `return None`.
- **Liña 287** — `if state == "result_missing":` — COMPLETED maibé rezultadu seidauk iha Drive:
- **Liña 288** — `print(...)` — "processing again".
- **Liña 289** — `kafka_received_at = datetime.now(timezone.utc)` — tempu Spark simu eventu.
- **Liña 290** — `photo_started = time.monotonic()`.
- **Liña 291** — `image_id = event.get("image_id")`.
- **Liña 292** — `if image_id:`
- **Liña 293** — `mark_processing(image_id)` — status `PROCESSING` (dashboard hatudu "iha prosesu").
- **Liña 294** — `spark_started_at = datetime.now(timezone.utc)`.
- **Liña 295–296** — `result, status, error = process_and_persist(event, rejected_reason, kafka_received_at=..., spark_started_at=...)` — D.2.8.
- **Liña 297** — `live_log.photo_done(event, status.value, result, error, time.monotonic() - photo_started)` — liña rezultadu iha konsola.
- **Liña 298** — `processed = result.get("processed_image_jpeg") if result else None` — imajen ba agrikultór (se iha).
- **Liña 299** — `return _UPLOADS.submit(_upload_result, event, status, error, processed)` — upload Drive iha background; fó `Future`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* eventu `{"kind": "drive_upload", "image_id": "a1b2c3d4", "image_path": "gdrive:1AbC", "owner_email": "agrikultor@example.com", "rejected_reason": None, "raw_value": None, ...}`. Liña 266–267 hasai rua. `register_upload` → `"new"`. `replay_state` → `None`. `mark_processing("a1b2c3d4")`. `process_and_persist` → `(result, COMPLETED, None)`. Konsola: `[photo]  a1b2c3d4  agrikultor@example.com  Aileu Vila  -> blight 97.00%  risk HIGH  (download 1.20s  preprocess 0.15s  cnn 0.08s  render 0.30s  total 2.10s)`. Return `Future`.

Eventu hanesan mai fali (replay depois crash) → `replay_state` = `"completed"` → return `None` (la prosesa dala rua).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `event` | `dict` |
| `rejected_reason`, `raw_value` | `str` ka `None` |
| `outcome` | `str` |
| `state` | `str` ka `None` |
| `kafka_received_at`, `spark_started_at` | `datetime` |
| `photo_started` | `float` |
| return | `Future` ka `None` (la iha hint) |

**7. Ligasaun OOP**

`status.value` — `ProcessingStatus` mak enum; `.value` fó testu (`"COMPLETED"`). Funsaun ne'e mak **koordenadór** (Teoria: *Facade*/orquestrasaun) — nia bolu funsaun barak husi modulu seluk, maibé ne'e prinsípiu dezeñu, la'ós klase.

**8. Impaktu integrasaun**

Bolu husi `foreach_batch_sink` (liña 233/236). Nia bolu `register_upload` (`storage/uploads`), `replay_state` no `mark_processing` (`storage/results`), `process_and_persist`, `live_log.photo_done`, no `_upload_result` iha `_UPLOADS`.

**9. Checkpoint ba principiante**

**P:** Saida mak akontese ba eventu ne'ebé repete depois fotografia COMPLETED no rezultadu iha Drive ona?
**R:** `replay_state` fó `"completed"`, funsaun print "already COMPLETED" no fó `None` — la prosesa fali. Ne'e halo job *idempotent*.

#### D.2.11 `_warm_up_model` no `_auto_resend`

**1. Kódigu orijinál**

{{code spark/streaming_job.py 302-325}}

**2. Objetivu**

`_warm_up_model` karrega CNN no halai fotografia falsu ida atu agrikultór dahuluk la hein. `_auto_resend` mak thread background ne'ebé kada `SPARK_AUTO_RESEND_SECONDS` (default 300 s) haruka fali fotografia ne'ebé seidauk hetan rezultadu.

**3. Fundamentu Python**

- **`-> None`**: funsaun la fó fali valór.
- **`BytesIO`**: arkivu iha memória (la'ós disku).
- **`while True:`** — loop infinitu (para só bainhira prosesu remata).
- **`time.sleep(s)`** — hein segundu `s`.

**4. Esplikasaun liña-ba-liña**

- **Liña 302** — `def _warm_up_model() -> None:`
- **Liña 303–304** — Docstring: karrega CNN no halai fotografia falsu; mós import torch molok pyarrow (WinError 1114 iha Windows).
- **Liña 305** — `from io import BytesIO`.
- **Liña 306** — `from PIL import Image as PILImage`.
- **Liña 307** — `from ml import inference`.
- **Liña 308** — `from spark.preprocessing import model_input`.
- **Liña 309** — `buf = BytesIO()` — arkivu memória mamuk.
- **Liña 310** — `PILImage.new("RGB", (256, 256), (60, 140, 50)).save(buf, format="JPEG")` — harii imajen 256×256 kór matak (R=60, G=140, B=50), rai hanesan JPEG iha `buf`.
- **Liña 311** — `x = model_input(buf.getvalue())` — tensor husi imajen falsu.
- **Liña 312** — `inference.predict(x)` — CNN halai dala uluk (karrega modelu ba memória/GPU).
- **Liña 313** — `inference.disease_heatmap(x)` — prepara mós Grad-CAM.
- **Liña 316** — `def _auto_resend() -> None:`
- **Liña 317** — Docstring: thread background; haruka fali fotografia ne'ebé seidauk hetan rezultadu.
- **Liña 318** — `from storage.requeue_stuck import resend_owed`.
- **Liña 319** — `while True:`
- **Liña 320** — `time.sleep(SPARK_AUTO_RESEND_SECONDS)` — hein (default 300 s).
- **Liña 321** — `try:`
- **Liña 322** — `if sent := resend_owed(SPARK_AUTO_RESEND_AFTER_SECONDS, SPARK_AUTO_RESEND_MAX):` — haruka fali fotografia ne'ebé hein liu 600 s, maximu 50; `sent` = kuantu.
- **Liña 323** — `live_log.say(...)` — hakerek kuantu haruka fali.
- **Liña 324** — `except Exception as e:` — komentáriu: koko fali iha volta tuirmai.
- **Liña 325** — `print(...)` — "Automatic re-send failed, trying again later".

**5. Walkthrough ezekusaun**

Bainhira `run()` hahú, `_warm_up_model` halai ~ segundu balun (karrega modelu). Depois, se Kafka hetan fotografia, CNN prontu ona. `_auto_resend`: minutu 5 depois hahú, nia buka fotografia `INGESTED`/`PROCESSING` ne'ebé hein liu minutu 10; *ezemplu ilustrativu*: hetan 2 → konsola `[resend] 2 photo(s) still owed a result sent through the pipeline again`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `buf` | `io.BytesIO` |
| `x` | `torch.Tensor` |
| `sent` | `int` |
| return (rua) | `None` |

**7. Ligasaun OOP**

`PILImage.new(...)` fó objetu `Image` Pillow; `.save(...)` mak nia métodu. `buf` mak objetu `BytesIO` ne'ebé hahalok hanesan arkivu (polimorfizmu "file-like object": `save` la hatene diferensa entre arkivu disku no memória).

**8. Impaktu integrasaun**

`run()` bolu `_warm_up_model()` (liña 331) no hahú `_auto_resend` iha thread (liña 355). `resend_owed` (`storage/requeue_stuck.py`) haruka eventu fali ba Kafka — fotografia tama fali iha `foreach_batch_sink`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `_auto_resend` iha `try/except` iha loop nia laran, la'ós iha liur?
**R:** Atu erru ida (ezemplu base de dadus mate momentu ida) la para loop. Thread kontinua no koko fali volta tuirmai.

#### D.2.12 Funsaun `run` — hahú query streaming

**1. Kódigu orijinál**

{{code spark/streaming_job.py 328-360}}

**2. Objetivu**

Pontu tama job: prepara pasta checkpoint, base de dadus no CNN, harii SparkSession, define **source** Kafka, parsing, **sink** `foreachBatch`, trigger no checkpoint, hahú query no hein ba nafatin.

**3. Fundamentu Python**

- **`if __name__ == "__main__":`** — `__name__` mak `"__main__"` só bainhira arkivu halai direta (`python -m spark.streaming_job`), la'ós bainhira import.
- **`threading.Thread(target=f, daemon=True).start()`** — thread *daemon* la hein bainhira programa prinsipál remata.
- **`mkdir(parents=True, exist_ok=True)`** — harii pasta no pasta inan; la fó erru se iha ona.

**4. Esplikasaun liña-ba-liña**

- **Liña 328** — `def run():`
- **Liña 329** — `Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)` — komentáriu: Spark nia harii lazy falla iha Windows.
- **Liña 330** — `init_db()` — harii tabela se seidauk iha (Modulu 04).
- **Liña 331** — `_warm_up_model()` — D.2.11.
- **Liña 333** — `spark = build_spark()` — D.2.3.
- **Liña 334** — `spark.sparkContext.setLogLevel("WARN")` — hatudu só aviszu no erru (la'ós INFO barak).
- **Liña 336** — `raw = (` — source streaming:
- **Liña 337** — `spark.readStream.format("kafka")` — lee husi Kafka.
- **Liña 338** — `.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)` — enderesu broker.
- **Liña 339** — `.option("subscribe", KAFKA_TOPIC)` — topic.
- **Liña 340** — `.option("startingOffsets", "earliest")` — se checkpoint la iha, hahú husi mensajen dahuluk iha topic (la lakon fotografia). Se checkpoint iha, Spark kontinua husi checkpoint.
- **Liña 341** — `.option("maxOffsetsPerTrigger", SPARK_MAX_EVENTS_PER_BATCH)` — maximu mensajen kada batch (100).
- **Liña 342** — `.load()` — fó DataFrame streaming.
- **Liña 343** — `)`.
- **Liña 344** — `parsed = parse_events(raw)` — D.2.4.
- **Liña 346** — `query = (`
- **Liña 347** — `parsed.writeStream` — hahú definisaun output.
- **Liña 348** — `.foreachBatch(foreach_batch_sink)` — kada micro-batch bolu D.2.9.
- **Liña 349** — `.outputMode("append")` — liña foun de'it.
- **Liña 350** — `.trigger(processingTime=f"{SPARK_TRIGGER_SECONDS} seconds")` — batch kada 1 s (default): `"1.0 seconds"`.
- **Liña 351** — `.option("checkpointLocation", CHECKPOINT_DIR)` — fatin checkpoint (offset Kafka ne'ebé remata ona).
- **Liña 352** — `.start()` — hahú query; fó objetu `StreamingQuery`.
- **Liña 353** — `)`.
- **Liña 354** — `if SPARK_AUTO_RESEND_SECONDS > 0:` — re-send automátiku la hamate:
- **Liña 355** — `threading.Thread(target=_auto_resend, name="auto-resend", daemon=True).start()`.
- **Liña 356** — `query.awaitTermination()` — hein to'o query para (ka erru).
- **Liña 359** — `if __name__ == "__main__":`
- **Liña 360** — `run()`.

**5. Walkthrough ezekusaun**

`python -m spark.streaming_job` → `run()`: harii `storage/spark_checkpoints/<topic>` → `init_db` → warm-up CNN → SparkSession → Kafka source ho `startingOffsets=earliest`, `maxOffsetsPerTrigger=100` → trigger `"1.0 seconds"` → `start()`. Kada segundu, Spark kontrola Kafka; se iha mensajen foun, nia bolu `foreach_batch_sink(batch_df, batch_id)`. Programa hein iha liña 356 to'o Ctrl+C.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `spark` | `SparkSession` |
| `raw`, `parsed` | `DataFrame` (streaming) |
| `query` | `pyspark.sql.streaming.StreamingQuery` |
| return | `None` (la iha `return`) |
| Exception | `StreamingQueryException` husi `awaitTermination` se query monu (inferidu) |

**7. Ligasaun OOP**

`readStream` no `writeStream` mak padraun **Builder** PySpark (konfigura ho `.option` depois `.load()`/`.start()`). `query` mak objetu ne'ebé kontrola query moris. `foreachBatch` simu **funsaun** hanesan argumentu (funsaun mak objetu iha Python — *first-class function*), ne'e hanesan padraun *callback/Strategy*.

**8. Impaktu integrasaun**

`run_all.py` hahú prosesu ne'e (Modulu 06). Source = Kafka (Modulu 02); sink = PostgreSQL + Drive liuhusi `storage/results` (Modulu 03, 04).

**9. Checkpoint ba principiante**

**P:** Se ita hamate job no hahú fali, Spark lee fali mensajen hotu husi dahuluk?
**R:** Lae. `startingOffsets=earliest` só uza bainhira checkpoint la iha. Ho checkpoint, Spark kontinua husi offset ikus ne'ebé commit ona.

### D.3 `spark/preprocessing.py` — Spark nia serbisu ba fotografia ida

#### D.3.1 Docstring, import no `REQUIRED_EVENT_FIELDS`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 1-26}}

**2. Objetivu**

Foti biblioteka imajen (Pillow), konfigurasaun, funsaun armazenamentu, cliente Drive, espesifikasaun input modelu no modelu base de dadus. Define lista kampu obrigatóriu ba eventu.

**3. Fundamentu Python**

- **Lista** `["image_id", ...]` — sekuénsia ho orden.
- **Import naran privadu**: `from kafka.drive_watcher import _parse_time` — naran ho `_` mak konvensaun "internu", maibé Python permite import. Ne'e reuza funsaun parsing tempu ne'ebé hanesan ho watcher (DRY: *Don't Repeat Yourself*).
- **`from storage import drive_client`** — import modulu tomak (uza hanesan `drive_client.get_file_metadata`).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: Spark nia serbisu ba fotografia ida — kontrola eventu, foti fotografia, lee EXIF, preprocess ba CNN no rai imajen ne'ebé prosesa.
- **Liña 6** — `import re` — espresaun regulár (regex).
- **Liña 7** — `datetime, timezone` — tempu UTC.
- **Liña 8** — `BytesIO` — arkivu iha memória.
- **Liña 9** — `Path` — dalan arkivu.
- **Liña 11** — `PILImage, ImageOps` husi Pillow — loke/rai imajen; `ImageOps.exif_transpose` roda imajen tuir EXIF.
- **Liña 13–16** — Konfigurasaun: `IMAGE_STORAGE_PATH` (pasta fotografia lokál), `REVIEW_DIR` (pasta revizaun), `SAVE_PHOTOS_FOR_REVIEW`, `PROCESSED_IMAGES_DIR`, `ANALYSIS_MAX_PX` (1280), `SAVE_DOWNLOADED_PHOTOS`, `SPARK_RECHECK_DRIVE_FILE` (default `False`).
- **Liña 17** — Funsaun dalan imajen: `resolve_image_path`, `is_drive_path`, `drive_file_id_from_path`, `store_downloaded_photo`.
- **Liña 18** — `extract_image_metadata` (lee EXIF) no `GPS_COLUMNS` (`gps_latitude`, `gps_longitude`, `gps_altitude_m`).
- **Liña 19** — `as_aware_utc` — tau fuso UTC ba datetime ne'ebé la iha fuso.
- **Liña 20** — `_parse_time` — testu ISO → datetime (ka `None`).
- **Liña 21** — Modulu `drive_client` (API Google Drive, Modulu 03).
- **Liña 22** — `input_spec` — fó `img_size`, `mean`, `std` husi checkpoint modelu.
- **Liña 23** — Modelu ORM: `SessionLocal` (fábrika sesaun), `Image`, `User`, `ScaleTestItem`, `ScaleTestRun`.
- **Liña 26** — `REQUIRED_EVENT_FIELDS = ["image_id", "user_id", "image_path", "uploaded_at"]` — kampu ne'ebé eventu tenke iha iha nível Python.

**5. Walkthrough ezekusaun**

Bainhira `spark.streaming_job` import modulu ne'e, Python halai liña 1–26 dala ida. `REQUIRED_EVENT_FIELDS` sai lista ho string 4.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `REQUIRED_EVENT_FIELDS` | `list[str]` |
| `ANALYSIS_MAX_PX` | `int` |
| `IMAGE_STORAGE_PATH`, `REVIEW_DIR`, `PROCESSED_IMAGES_DIR` | `Path` |
| `SAVE_*`, `SPARK_RECHECK_DRIVE_FILE` | `bool` |

**7. Ligasaun OOP**

`Image`, `User`, `ScaleTestItem`, `ScaleTestRun` mak **klase ORM** (SQLAlchemy) — kada klase reprezenta tabela ida; kada objetu reprezenta liña ida (Modulu 04). `SessionLocal` mak fábrika (Teoria: padraun *Factory*) ne'ebé harii objetu `Session`.

**8. Impaktu integrasaun**

Modulu ne'e liga Spark ho `storage/` (base de dadus, Drive), `kafka/` (parsing tempu) no `ml/` (espesifikasaun input CNN).

**9. Checkpoint ba principiante**

**P:** Diferensa saida entre `REQUIRED_EVENT_FIELDS` iha ne'e no kampu obrigatóriu iha `parse_events`?
**R:** `parse_events` (Spark SQL) obriga `image_id`, `image_path`, `uploaded_at` + `owner_email` (Drive) ka `user_id` (submit). Iha Python, depois `register_upload` hatama `user_id`, `clean_event` obriga kampu 4 ne'e — inklui `user_id` ba eventu hotu.

#### D.3.2 `_parse_timestamp`, `normalize_location` no klase `InvalidEvent`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 29-46}}

**2. Objetivu**

Funsaun ajuda rua ba limpeza dadus (tempu no fatin) no klase erru espesiál ba eventu aat.

**3. Fundamentu Python**

- **Herança (inheritance)**: `class InvalidEvent(Exception): pass` — klase foun ne'ebé **herda** hotu husi `Exception`. `pass` = "la iha buat ida tan". Ita bele `raise InvalidEvent("...")` no `except InvalidEvent`.
- **`{value!r}`** iha f-string — hatudu valór ho aspas.
- **`str.strip()`, `str.title()`** — hasai espasu rohan; Letra Boot iha liafuan kada ida.
- **`re.sub(padraun, troka, testu)`** — troka testu ne'ebé tuir regex. `\s+` = espasu ida ka liu.
- **Return sedu** `return None`.

**4. Esplikasaun liña-ba-liña**

- **Liña 29** — `def _parse_timestamp(value, field_name: str):` — privadu (`_`).
- **Liña 30** — Docstring: testu ISO-8601 → datetime aware; hamosu `InvalidEvent` se la bele parse.
- **Liña 31** — `dt = _parse_time(value)` — `None` se valór mamuk ka la válidu.
- **Liña 32** — `if dt is None and value:` — parse falla **maske** iha valór (testu aat):
- **Liña 33** — `raise InvalidEvent(f"Invalid {field_name}: not a valid ISO-8601 timestamp ({value!r})")`.
- **Liña 34** — `return as_aware_utc(dt)` — datetime ho fuso UTC (ka `None` se valór mamuk).
- **Liña 37** — `def normalize_location(location) -> str:` — publiku; hint `-> str` maibé bele fó `None`.
- **Liña 38** — Docstring: regra hanesan `parse_events` — trim, espasu ida, Title Case.
- **Liña 39** — `if not location:` — `None` ka `""`:
- **Liña 40** — `return None`.
- **Liña 41** — `collapsed = re.sub(r"\s+", " ", str(location).strip())` — konverte ba str, hasai espasu rohan, troka espasu barak ba ida.
- **Liña 42** — `return collapsed.title() if collapsed else None` — Title Case; se mamuk depois limpeza, `None`.
- **Liña 45** — `class InvalidEvent(Exception):` — klase erru eventu aat.
- **Liña 46** — `pass` — la aumenta métodu ka atributu.

**5. Walkthrough ezekusaun**

- `_parse_timestamp("2026-10-20T01:15:00Z", "uploaded_at")` → `datetime(2026, 10, 20, 1, 15, tzinfo=UTC)`.
- `_parse_timestamp("ontem", "captured_at")` → `InvalidEvent("Invalid captured_at: not a valid ISO-8601 timestamp ('ontem')")`.
- `_parse_timestamp(None, "captured_at")` → `None`.
- `normalize_location("  aileu   vila ")` → `"Aileu Vila"`; `normalize_location("   ")` → `None`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception |
|---|---|---|---|
| `_parse_timestamp` | `value` (`str`/`None`, la iha hint), `field_name: str` | `datetime` ka `None` | `InvalidEvent` |
| `normalize_location` | `location` (objetu saida de'it) | `str` ka `None` (hint hatete `str`) | la iha |
| `InvalidEvent` | mensajen `str` (husi `Exception.__init__`) | — | — |

**7. Ligasaun OOP**

**Herança implementadu**: `InvalidEvent` → `Exception` → `BaseException` → `object`. Klase la iha `__init__` rasik; nia uza `__init__` husi `Exception` (herda). Sub-klase seluk iha projetu: `UploadRejected(InvalidEvent)` iha `storage/uploads.py` no `DuplicateImage(InvalidEvent)` iha `storage/duplicates.py` — ne'e **hierarkia herança** loloos. Tanba ne'e `except InvalidEvent` iha `_process_one` mós kaer `UploadRejected` (**polimorfizmu** liuhusi herança).

**8. Impaktu integrasaun**

`InvalidEvent` uza iha `streaming_job` (liña 209, 244, 275). `normalize_location` uza iha `clean_event` (liña 61) no sira seluk ne'ebé presiza regra hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá `InvalidEvent` la iha kódigu iha laran (só `pass`), maibé útil?
**R:** Nia **tipu** mak informasaun. Kódigu bele kaer `except InvalidEvent` atu trata eventu aat diferente husi erru seluk (ezemplu, print "Invalid event, skipping").

#### D.3.3 Funsaun `clean_event` — validasaun no limpeza eventu

**1. Kódigu orijinál**

{{code spark/preprocessing.py 49-87}}

**2. Objetivu**

Validasaun nível Python ba eventu ida: kampu obrigatóriu, tempu la iha futuru, `location` limpu, `user_id` iha base de dadus, no dalan imajen seguru (fotografia Drive → `_clean_drive_event`; fotografia lokál → tenke iha laran `IMAGE_STORAGE_PATH` no naran arkivu iha `image_id`).

**3. Fundamentu Python**

- **Dict comprehension** `{field: f(field) for field in (...)}`.
- **`dict.items()`** — fó pár `(chave, valór)` atu loop.
- **`dict.update({...})`** — muda/aumenta chave barak.
- **`raise ... from`** la uza iha ne'e; `raise InvalidEvent(...)` iha `except` troka erru ba tipu ne'ebé job hatene.
- **`Path.relative_to(base)`** — hamosu `ValueError` se dalan la iha laran `base` (proteje kontra *path traversal*).

**4. Esplikasaun liña-ba-liña**

- **Liña 49** — `def clean_event(event: dict) -> dict:`
- **Liña 50** — Docstring: validasaun no limpeza; hamosu `InvalidEvent` se eventu aat.
- **Liña 51** — `for key in REQUIRED_EVENT_FIELDS:`
- **Liña 52** — `if not event.get(key):` — la iha ka mamuk:
- **Liña 53** — `raise InvalidEvent(f"Missing required field: {key}")`.
- **Liña 55** — `now = datetime.now(timezone.utc)`.
- **Liña 56** — `times = {field: _parse_timestamp(event.get(field), field) for field in ("uploaded_at", "captured_at")}` — parse tempu rua.
- **Liña 57** — `for field, value in times.items():`
- **Liña 58** — `if value is not None and value > now:` — tempu iha futuru:
- **Liña 59** — `raise InvalidEvent(f"{field} is in the future: {value.isoformat()}")`.
- **Liña 60** — `event.update({field: value.isoformat() if value else None for field, value in times.items()})` — hakerek fali tempu iha formatu ISO padraun (UTC).
- **Liña 61** — `event["location"] = normalize_location(event.get("location"))`.
- **Liña 63** — `with SessionLocal() as session:` — loke sesaun base de dadus.
- **Liña 64** — `if session.get(User, event["user_id"]) is None:` — buka utilizadór tuir chave primária.
- **Liña 65** — `raise InvalidEvent(f"Unknown user_id: {event['user_id']}")`.
- **Liña 67** — `if is_drive_path(event["image_path"]):` — fotografia Drive:
- **Liña 68** — `return _clean_drive_event(event)` — D.3.4.
- **Liña 70** — `try:`
- **Liña 71** — `resolved = resolve_image_path(event["image_path"])` — konverte dalan rai ba dalan loloos iha disku.
- **Liña 72** — `except FileNotFoundError:`
- **Liña 73** — `raise InvalidEvent(f"Image file not found on disk: {event['image_path']}")`.
- **Liña 75** — `try:`
- **Liña 76** — `resolved.resolve().relative_to(Path(IMAGE_STORAGE_PATH).resolve())` — dalan absolutu tenke iha pasta armazenamentu.
- **Liña 77** — `except ValueError:`
- **Liña 78** — `raise InvalidEvent(f"Resolved image path escapes IMAGE_STORAGE_PATH: {resolved}")` — seguransa.
- **Liña 80** — `if event["image_id"] not in resolved.name:` — naran arkivu tenke iha `image_id`:
- **Liña 81–84** — `raise InvalidEvent(...)` — "image_id ... is not reflected in the resolved file name ...".
- **Liña 86** — `event["image_path"] = str(resolved)` — rai dalan loloos.
- **Liña 87** — `return event`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `{"image_id": "a1", "user_id": "u-9", "image_path": "gdrive:1AbC", "uploaded_at": "2026-10-20T01:15:00Z", "captured_at": None, "location": " aileu "}`. Kampu OK; `uploaded_at` → `"2026-10-20T01:15:00+00:00"`; `location` → `"Aileu"`; `User u-9` iha; dalan Drive → `_clean_drive_event`.

Se `captured_at = "2030-01-01T00:00:00Z"` → `InvalidEvent("captured_at is in the future: 2030-01-01T00:00:00+00:00")`. Se `image_path = "../../etc/passwd"` (dalan lokál) → `InvalidEvent("Resolved image path escapes IMAGE_STORAGE_PATH: ...")` ka "not found".

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `event` | `dict` (muda iha fatin — efeitu sekundáriu: dict orijinál mós muda) |
| `now` | `datetime` |
| `times` | `dict[str, datetime ka None]` |
| `session` | `sqlalchemy.orm.Session` |
| `resolved` | `Path` |
| return | `dict` |
| Exception | `InvalidEvent` |

**7. Ligasaun OOP**

`session.get(User, id)` — métodu objetu `Session` ne'ebé fó objetu `User` (ka `None`). `with SessionLocal() as session` uza protokolu **context manager** (`__enter__`/`__exit__`) husi klase `Session`. **Enkapsulamentu**: detalle SQL subar iha ORM; kódigu ne'e la hakerek SQL.

**8. Impaktu integrasaun**

Bolu husi `process_event` (streaming_job liña 138). Konsulta tabela `users` (Modulu 04). Ba Drive, kontinua iha `_clean_drive_event`.

**9. Checkpoint ba principiante**

**P:** Tanba sá kódigu kontrola `relative_to(IMAGE_STORAGE_PATH)`?
**R:** Seguransa: eventu ida labele obriga sistema lee arkivu iha liur pasta fotografia (ezemplu arkivu sistema). Se dalan "fakar" sai husi pasta, eventu rejeita.

#### D.3.4 `_clean_drive_event` no `_registered_from_drive`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 90-116}}

**2. Objetivu**

Ba fotografia Drive: `drive_file_id` tenke hanesan ho dalan; se fotografia rejista ona husi watcher ho dadus hanesan (no `SPARK_RECHECK_DRIVE_FILE = False`), la husu Drive fali (lalais liu). Se lae, husu Drive: arkivu tenke iha, la iha lixu, no iha etiketa `image_id` hanesan.

**3. Fundamentu Python**

- **Kaer exception barak**: `except (A, B) as e:`.
- **`(x or {}).get(k)`** — se `x` mak `None`, uza dict mamuk.
- **Espresaun `bool` naruk** ho `and` iha parénteze ba liña barak.

**4. Esplikasaun liña-ba-liña**

- **Liña 90** — `def _clean_drive_event(event: dict) -> dict:`
- **Liña 91** — Docstring: arkivu Drive tenke iha, la iha lixu, no lori `image_id` ne'e.
- **Liña 92** — `file_id = drive_file_id_from_path(event["image_path"])` — `"gdrive:1AbC"` → `"1AbC"`.
- **Liña 93** — `if event.get("drive_file_id") and event["drive_file_id"] != file_id:` — kampu la hanesan:
- **Liña 94** — `raise InvalidEvent("drive_file_id does not match image_path")`.
- **Liña 95** — `event["drive_file_id"] = file_id`.
- **Liña 96** — `if not SPARK_RECHECK_DRIVE_FILE and _registered_from_drive(event):`
- **Liña 97** — `return event` — komentáriu: kontrola ona iha rejistu; husu Drive fali neineik.
- **Liña 98** — `try:`
- **Liña 99** — `meta = drive_client.get_file_metadata(file_id)` — chamada API Drive.
- **Liña 100** — `except (drive_client.DriveError, drive_client.DriveNotConfigured) as e:`
- **Liña 101** — `raise InvalidEvent(f"Image file not found in Google Drive: {file_id} ({e})")`.
- **Liña 102** — `if meta.get("trashed"):` — arkivu iha lixu:
- **Liña 103** — `raise InvalidEvent(f"Image file was deleted from Google Drive: {file_id}")`.
- **Liña 104** — `if (meta.get("appProperties") or {}).get("image_id") != event["image_id"]:` — etiketa `image_id` ne'ebé Android tau la hanesan:
- **Liña 105–107** — `raise InvalidEvent(...)` — "Google Drive file ... is not tagged with image_id ...".
- **Liña 108** — `return event`.
- **Liña 111** — `def _registered_from_drive(event: dict) -> bool:`
- **Liña 112** — Docstring: loos bainhira arkivu Drive ne'e rejista ona ba utilizadór ne'e.
- **Liña 113** — `with SessionLocal() as session:`
- **Liña 114** — `image = session.get(Image, event["image_id"])`.
- **Liña 115–116** — `return (image is not None and image.source == "android_drive_upload" and image.image_path == event["image_path"] and image.user_id == event["user_id"])` — kondisaun 4 hotu tenke loos.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* eventu Drive foun, `register_upload` hakerek ona liña `images` ho `source="android_drive_upload"`, `image_path="gdrive:1AbC"`, `user_id="u-9"`. `_registered_from_drive` → `True`; `SPARK_RECHECK_DRIVE_FILE=False` → return kedas (la iha chamada API). Ba eventu `submit` antigu ho dalan seluk, kódigu husu Drive; se `meta = {"trashed": True}` → `InvalidEvent("Image file was deleted from Google Drive: 1AbC")`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `file_id` | `str` |
| `meta` | `dict` (resposta API Drive) |
| `image` | `Image` (ORM) ka `None` |
| `_registered_from_drive` return | `bool` |
| Exception | `InvalidEvent` |

**7. Ligasaun OOP**

`image.source`, `image.image_path`, `image.user_id` — asesu **atributu** objetu ORM. `drive_client.DriveError` mak klase exception iha `storage/drive_client.py` (Modulu 03).

**8. Impaktu integrasaun**

Bolu husi `clean_event` (liña 68). Uza `drive_client.get_file_metadata` (Modulu 03) no tabela `images` (Modulu 04). Etiketa `appProperties.image_id` hakerek husi aplikasaun Android bainhira upload (Modulu 03).

**9. Checkpoint ba principiante**

**P:** Tanba sá kódigu kontrola etiketa `image_id` iha Drive?
**R:** Atu garante katak arkivu Drive ne'e duni fotografia ne'ebé eventu ko'alia ba — evita analiza arkivu sala.

#### D.3.5 Funsaun `extract_metadata` — EXIF ba base de dadus

**1. Kódigu orijinál**

{{code spark/preprocessing.py 119-141}}

**2. Objetivu**

Lee EXIF husi fotografia **orijinál** (kámara, tamañu, GPS, tempu foti) no rai iha liña `images`. Regra GPS: GPS EXIF manán; se la iha, mantein posisaun telemóvel (`gps_source = "device"`).

**3. Fundamentu Python**

- **Tuple konstante** `_METADATA_COLUMNS = (...)`.
- **`setattr(obj, naran, valór)`** — muda atributu ho naran iha variavel (ezemplu `setattr(image, "camera_make", "samsung")` = `image.camera_make = "samsung"`).
- **Espresaun kondisionál iha argumentu**: `meta[name] if exif_gps else None`.

**4. Esplikasaun liña-ba-liña**

- **Liña 119** — `_METADATA_COLUMNS = ("camera_make", "camera_model", "image_width", "image_height")`.
- **Liña 122** — `def extract_metadata(event: dict, image_bytes: bytes) -> dict:`
- **Liña 123–124** — Docstring: rai EXIF fotografia ORIJINÁL iha nia liña; GPS EXIF manán; la iha nia, posisaun telemóvel mantein.
- **Liña 125** — `meta = extract_image_metadata(image_bytes)` — dict ho chave hotu (valór `None` se la iha).
- **Liña 126** — `with SessionLocal() as session:`
- **Liña 127** — `image = session.get(Image, event["image_id"])`.
- **Liña 128** — `if image is not None:`
- **Liña 129** — `for name in _METADATA_COLUMNS:`
- **Liña 130** — `setattr(image, name, meta[name])` — kámara no tamañu.
- **Liña 131** — `exif_gps = meta["gps_latitude"] is not None and meta["gps_longitude"] is not None` — EXIF iha GPS kompletu?
- **Liña 132** — `if exif_gps or image.gps_source != "device":` — muda GPS se EXIF iha, ka se GPS ida agora la'ós husi telemóvel:
- **Liña 133** — `for name in GPS_COLUMNS:`
- **Liña 134** — `setattr(image, name, meta[name] if exif_gps else None)` — tau GPS EXIF, ka hamamuk.
- **Liña 135** — `image.gps_source = "exif" if exif_gps else None`.
- **Liña 136** — `if image.captured_at is None and meta["captured_at"] is not None:` — tempu foti seidauk iha maibé EXIF iha:
- **Liña 137** — `image.captured_at = meta["captured_at"]`.
- **Liña 138** — `image.captured_at_source = meta["captured_at_source"]`.
- **Liña 139** — `event["captured_at"] = meta["captured_at"].isoformat()` — muda mós eventu.
- **Liña 140** — `session.commit()` — rai mudansa iha base de dadus.
- **Liña 141** — `return meta`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* EXIF fó `{"camera_make": "samsung", "camera_model": "SM-A145F", "image_width": 4000, "image_height": 3000, "gps_latitude": None, "gps_longitude": None, ...}`. Liña `images` iha `gps_source = "device"` (husi telemóvel). `exif_gps = False`; kondisaun liña 132 sala → GPS telemóvel mantein. Kámara no tamañu rai.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `image_bytes` | `bytes` |
| `meta` | `dict` |
| `exif_gps` | `bool` |
| return | `dict` |
| Efeitu sekundáriu | UPDATE liña `images`; muda `event["captured_at"]` |

**7. Ligasaun OOP**

`setattr` muda atributu objetu ORM `Image`; SQLAlchemy deteta mudansa no hakerek `UPDATE` bainhira `commit()` (padraun *Unit of Work* iha ORM). Projetu la define klase iha ne'e.

**8. Impaktu integrasaun**

Bolu husi `process_event` (streaming_job liña 157). Koluna ne'ebé hakerek (`camera_make`, `gps_latitude`, `captured_at`...) hatudu iha dashboard no Tableau (`analytics_reader.images_with_predictions`).

**9. Checkpoint ba principiante**

**P:** Se fotografia la iha EXIF GPS maibé telemóvel haruka GPS, saida mak rai?
**R:** GPS telemóvel mantein (`gps_source` hela `"device"`), tanba kondisaun liña 132 sala.

#### D.3.6 `model_input`, `model_input_preview` no `_prepared_rgb` — imajen ba tensor

**1. Kódigu orijinál**

{{code spark/preprocessing.py 144-170}}

**2. Objetivu**

Konverte imajen ba **tensor** ne'ebé CNN simu: hadi'ak sisi badak ba `S` piksel, koto kuadradu `S×S` iha klaran, konverte ba tensor, no normaliza ho `mean`/`std` husi checkpoint modelu. Docstring hatete centre crop di'ak liu duke "squeeze" (95.67% vs 94.74%).

**3. Fundamentu Python**

- **`transforms.Compose([...])`** — kombina transformasaun barak ba objetu ida ne'ebé bele bolu hanesan funsaun.
- **Bolu objetu** `to_model(img)` — objetu `Compose` iha `__call__`.
- **Parámetru opsionál** `prepared_bytes: bytes = None`.
- **Chamada kadeia** `transforms.CenterCrop(size)(transforms.Resize(size)(img))` — harii objetu, bolu kedas.

**4. Esplikasaun liña-ba-liña**

- **Liña 144** — `def model_input(original_bytes: bytes, prepared_bytes: bytes = None):`
- **Liña 145–146** — Docstring: tensor input CNN — sisi badak ba S, koto centre S×S, normaliza; centre crop di'ak liu squeeze (95.67% vs 94.74%).
- **Liña 147** — `from torchvision import transforms` — import lazy (torchvision todan).
- **Liña 148** — `spec = input_spec()` — dict husi checkpoint modelu.
- **Liña 149** — `size = spec["img_size"]`.
- **Liña 150** — `img = _prepared_rgb(original_bytes, prepared_bytes)` — imajen PIL RGB.
- **Liña 151** — `to_model = transforms.Compose([`
- **Liña 152** — `transforms.Resize(size),` — sisi badak liu sai `size` piksel; proporsaun mantein.
- **Liña 153** — `transforms.CenterCrop(size),` — koto kuadradu `size×size` iha klaran.
- **Liña 154** — `transforms.ToTensor(),` — PIL (H×W×3, 0–255) → tensor `[3, H, W]` float 0.0–1.0.
- **Liña 155** — `transforms.Normalize(mean=spec["mean"], std=spec["std"]),` — kada kanál: $x' = (x - \mu_c)/\sigma_c$.
- **Liña 156** — `])`.
- **Liña 157** — `return to_model(img)` — tensor final.
- **Liña 160** — `def model_input_preview(original_bytes: bytes, prepared_bytes: bytes = None):`
- **Liña 161** — Docstring: kuadradu ne'ebé CNN haree, hanesan imajen (model_input molok normaliza).
- **Liña 162** — `from torchvision import transforms`.
- **Liña 163** — `size = input_spec()["img_size"]`.
- **Liña 164** — `img = _prepared_rgb(original_bytes, prepared_bytes)`.
- **Liña 165** — `return transforms.CenterCrop(size)(transforms.Resize(size)(img))` — Resize depois CenterCrop; fó imajen PIL (la'ós tensor).
- **Liña 168** — `def _prepared_rgb(original_bytes, prepared_bytes=None):`
- **Liña 169** — `data = prepared_bytes if prepared_bytes is not None else prepare_image(original_bytes)` — uza imajen prepara ona se iha; se lae, prepara agora.
- **Liña 170** — `return ImageOps.exif_transpose(PILImage.open(BytesIO(data)).convert("RGB"))` — loke bytes, konverte RGB, roda tuir EXIF (seguru maske EXIF la iha ona).

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* ho `size = 224` no `mean = [0.485, 0.456, 0.406]`, `std = [0.229, 0.224, 0.225]` (valór ImageNet tipiku — valór loloos mai husi checkpoint, haree Modulu 05):

1. Imajen 1280×960 → `Resize(224)` → 299×224 (sisi badak 224).
2. `CenterCrop(224)` → 224×224.
3. `ToTensor` → piksel kanál vermelhu 153 → $153/255 = 0.6$.
4. `Normalize` → $(0.6 - 0.485)/0.229 \approx 0.502$.
5. Rezultadu: tensor `[3, 224, 224]`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return |
|---|---|---|
| `model_input` | `original_bytes: bytes`, `prepared_bytes: bytes ka None` | `torch.Tensor` float32 `[3, S, S]` (inferidu) |
| `model_input_preview` | hanesan | `PIL.Image.Image` S×S |
| `_prepared_rgb` | bytes ka `None`, bytes ka `None` | `PIL.Image.Image` RGB |
| `spec` | `dict` ho `img_size` (`int`), `mean`/`std` (`list[float]`) | — |

**7. Ligasaun OOP**

`transforms.Resize`, `CenterCrop`, `ToTensor`, `Normalize`, `Compose` mak **klase** torchvision; kada ida iha `__call__`, entaun objetu bele bolu hanesan funsaun. `Compose` kaer lista objetu transformasaun (**komposisaun**) no bolu sira tuir orden. Ne'e mós padraun *Pipeline* — maibé implementa iha biblioteka, la'ós projetu.

**8. Impaktu integrasaun**

`process_event` bolu `model_input` (liña 160) no fó tensor ba `ml.inference.predict` (Modulu 05). `model_input_preview` uza iha `store_processed_image` (liña 230). `input_spec()` garante katak preprocessing hanesan ho treinu modelu.

**9. Checkpoint ba principiante**

**P:** Tanba sá `img_size`, `mean`, `std` mai husi `input_spec()` no la hakerek fixu iha kódigu?
**R:** Tanba valór sira ne'e tenke hanesan ho valór ne'ebé uza bainhira treina modelu. Rai iha checkpoint modelu garante katak modelu foun ho valór seluk sei funsiona kedas.

#### D.3.7 `prepare_image` no `scale_test_run_for`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 173-198}}

**2. Objetivu**

`prepare_image`: kópia imajen ne'ebé vertikál (roda tuir EXIF), maximu `ANALYSIS_MAX_PX` (1280) piksel, re-encode JPEG kualidade 95 la ho EXIF. Pasu hotu depois hahú husi kópia ne'e. `scale_test_run_for`: hatene se eventu pertense ba teste eskalabilidade.

**3. Fundamentu Python**

- **`except Exception: return image_bytes`** — se imajen la bele lee, fó fali orijinál (la monu).
- **`# noqa: BLE001`** — komentáriu ba ferramenta *linter* (la husu aviszu "except luan").
- **Espresaun kondisionál naruk** ho `if ... else None`.
- **`session.expunge(obj)`** — hasai objetu husi sesaun atu bele uza depois sesaun taka.

**4. Esplikasaun liña-ba-liña**

- **Liña 173** — `def prepare_image(image_bytes: bytes) -> bytes:`
- **Liña 174–175** — Docstring: vertikál, maximu `ANALYSIS_MAX_PX`, re-encode la ho EXIF; imajen la bele lee fó fali la muda.
- **Liña 176** — `try:`
- **Liña 177** — `img = PILImage.open(BytesIO(image_bytes))` — loke (lazy — seidauk descodifika tomak).
- **Liña 178** — `img.draft("RGB", (ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))` — husu descodificadór JPEG atu descodifika iha eskala ki'ik (lalais liu, poupa memória).
- **Liña 179** — `img = ImageOps.exif_transpose(img).convert("RGB")` — roda tuir orientasaun EXIF; konverte RGB.
- **Liña 180** — `except Exception:` — imajen aat.
- **Liña 181** — `return image_bytes` — fó fali orijinál (CNN/erru depois sei trata).
- **Liña 182** — `img.thumbnail((ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))` — hamenus to'o sisi boot ≤ 1280, proporsaun mantein; la hadi'ak imajen ki'ik.
- **Liña 183** — `buf = BytesIO()`.
- **Liña 184** — `img.save(buf, format="JPEG", quality=95)` — rai JPEG (EXIF la hakerek).
- **Liña 185** — `return buf.getvalue()` — bytes JPEG foun.
- **Liña 188** — `def scale_test_run_for(event: dict):`
- **Liña 189** — Docstring: run teste ne'ebé eventu pertense, ka `None` (kontrola iha base de dadus).
- **Liña 190** — `run_id = event.get("run_id")`.
- **Liña 191** — `if not run_id:`
- **Liña 192** — `return None` — eventu normál.
- **Liña 193** — `with SessionLocal() as session:`
- **Liña 194** — `item = session.get(ScaleTestItem, event.get("image_id")) if event.get("image_id") else None`.
- **Liña 195** — `run = session.get(ScaleTestRun, run_id) if item is not None and item.run_id == run_id else None` — item tenke pertense ba run ne'e.
- **Liña 196** — `if run is not None:`
- **Liña 197** — `session.expunge(run)` — objetu bele uza depois sesaun taka.
- **Liña 198** — `return run`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* JPEG 4000×3000 ho EXIF `Orientation=6` (roda 90°). `draft` descodifika ~1000×750 ka 2000×1500 (eskala JPEG 1/2, 1/4...). `exif_transpose` roda → vertikál. `thumbnail((1280,1280))` → ezemplu 960×1280. Rai JPEG q=95 → ~300 KB. Ba `scale_test_run_for`: iha streaming normál `run_id` la iha → `None` (no `_process_one` salta eventu ho `run_id` molok to'o iha ne'e).

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception |
|---|---|---|---|
| `prepare_image` | `image_bytes: bytes` | `bytes` | la iha (hotu kaer) |
| `scale_test_run_for` | `event: dict` | `ScaleTestRun` ka `None` | erru base de dadus (la kaer) |

**7. Ligasaun OOP**

`img` mak objetu `PIL.Image.Image`; `draft`, `thumbnail`, `save`, `convert` mak métodu. `thumbnail` muda objetu **iha fatin** (la fó objetu foun) — ne'e diferente husi `convert` ne'ebé fó objetu foun.

**8. Impaktu integrasaun**

`prepare_image` bolu husi `process_event` (liña 159) no `_prepared_rgb`. Kópia ne'e uza ba CNN, imajen revizaun, imajen prosesa, no desenhu rezultadu. `scale_test_run_for` bolu husi `process_event` (liña 144).

**9. Checkpoint ba principiante**

**P:** Tanba sá `prepare_image` hasai EXIF?
**R:** EXIF iha GPS (dadus privadu) no orientasaun. Imajen ne'ebé roda ona no la ho EXIF evita roda dala rua no la vaza GPS ba imajen revizaun.

#### D.3.8 `save_downloaded_photo`, `save_for_review` no `store_processed_image`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 201-233}}

**2. Objetivu**

Funsaun tolu ne'ebé rai kópia imajen iha disku. Sira hotu **nunka** halo diagnóstiku falla: erru só print. Rua uza padraun "hakerek ba `.tmp`, depois `replace`" atu arkivu nunka sai la kompletu.

**3. Fundamentu Python**

- **`-> None`** — la fó valór.
- **`Path.replace(destinu)`** — troka naran arkivu (atómiku iha sistema arkivu hanesan): leitór sei haree arkivu tuan ka foun, nunka sorin-balun.
- **f-string ba naran arkivu**: `f"{image_id}.jpg.tmp"`.

**4. Esplikasaun liña-ba-liña**

- **Liña 201** — `def save_downloaded_photo(user_id: str, image_id: str, original_bytes: bytes) -> None:`
- **Liña 202** — Docstring: rai kópia lokál fotografia Drive; nunka halo diagnóstiku falla.
- **Liña 203** — `if not SAVE_DOWNLOADED_PHOTOS:` — konfigurasaun hamate:
- **Liña 204** — `return`.
- **Liña 205** — `try:`
- **Liña 206** — `store_downloaded_photo(user_id, image_id, original_bytes)` — rai iha `storage/downloaded_photos/` (Modulu 03).
- **Liña 207** — `except Exception as e:`
- **Liña 208** — `print(f"[preprocessing] Could not save downloaded photo {image_id}: {e}")`.
- **Liña 211** — `def save_for_review(image_id: str, image_bytes: bytes) -> None:`
- **Liña 212** — Docstring: kopia fotografia (la ho EXIF, entaun la ho GPS) ba `REVIEW_DIR` ba pájina Label photos.
- **Liña 213** — `if not SAVE_PHOTOS_FOR_REVIEW:`
- **Liña 214** — `return`.
- **Liña 215** — `try:`
- **Liña 216** — `img = _prepared_rgb(None, image_bytes)` — imajen PIL husi bytes prepara ona.
- **Liña 217** — `REVIEW_DIR.mkdir(parents=True, exist_ok=True)`.
- **Liña 218** — `tmp = REVIEW_DIR / f"{image_id}.jpg.tmp"`.
- **Liña 219** — `img.save(tmp, format="JPEG", quality=95)`.
- **Liña 220** — `tmp.replace(REVIEW_DIR / f"{image_id}.jpg")` — troka naran atómiku.
- **Liña 221** — `except Exception as e:`
- **Liña 222** — `print(f"[preprocessing] Could not save {image_id} for labelling: {e}")`.
- **Liña 225** — `def store_processed_image(image_id: str, original_bytes: bytes, prepared_bytes: bytes) -> None:`
- **Liña 226** — Docstring: rai kuadradu ne'ebé CNN haree hanesan `PROCESSED_IMAGES_DIR/<image_id>.png`.
- **Liña 227** — `try:`
- **Liña 228** — `PROCESSED_IMAGES_DIR.mkdir(parents=True, exist_ok=True)`.
- **Liña 229** — `tmp = PROCESSED_IMAGES_DIR / f"{image_id}.png.tmp"`.
- **Liña 230** — `model_input_preview(original_bytes, prepared_bytes).save(tmp, format="PNG")` — PNG (la lakon kualidade).
- **Liña 231** — `tmp.replace(PROCESSED_IMAGES_DIR / f"{image_id}.png")`.
- **Liña 232** — `except Exception as e:`
- **Liña 233** — `print(f"[preprocessing] Could not store the processed image of {image_id}: {e}")`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `image_id = "a1b2c3d4"`. `store_processed_image` hakerek `storage/processed_images/a1b2c3d4.png.tmp`, depois troka naran ba `a1b2c3d4.png` (224×224 se `img_size=224`). `save_for_review` hakerek `<TRAINING_PROJECT_DIR>/data/review/a1b2c3d4.jpg`. Se disku nakonu, print "Could not store ..." no fotografia kontinua.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Efeitu sekundáriu |
|---|---|---|---|
| `save_downloaded_photo` | `str`, `str`, `bytes` | `None` | arkivu iha `storage/downloaded_photos/` |
| `save_for_review` | `str`, `bytes` | `None` | `REVIEW_DIR/<id>.jpg` |
| `store_processed_image` | `str`, `bytes`, `bytes` | `None` | `PROCESSED_IMAGES_DIR/<id>.png` |

**7. Ligasaun OOP**

`tmp` mak objetu `Path`; `.replace()` mak métodu. Not implemented in the current codebase: klase ba armazenamentu imajen (funsaun simples de'it).

**8. Impaktu integrasaun**

Bolu husi `process_event` (liña 151, 162, 169). `REVIEW_DIR` uza husi pájina dashboard "Label photos" (ema hili klase loloos → dadus treinu foun, Modulu 05). `PROCESSED_IMAGES_DIR` uza husi dashboard atu hatudu imajen ne'ebé CNN haree.

**9. Checkpoint ba principiante**

**P:** Tanba sá hakerek ba `.tmp` uluk, depois `replace`?
**R:** Se programa ka dashboard lee arkivu momentu hakerek, nia bele haree arkivu la kompletu. `replace` troka naran iha pasu ida — arkivu final sempre kompletu.

### D.4 `spark/batch_analytics.py` — analítika batch ho JDBC

**Fundamentu Spark ba seksaun ne'e (Teoria).** *JDBC* (Java Database Connectivity) mak padraun Java atu ko'alia ho base de dadus. Spark uza driver JDBC PostgreSQL atu **lee** tabela ba DataFrame (`spark.read.jdbc`) no **hakerek** DataFrame ba tabela (`df.write.mode(...).jdbc`). *Agregasaun* = kombina liña barak ba valór ida (konta, soma, média). `groupBy(...).agg(...)` = fahe liña ba grupu, depois agrega kada grupu. *Window function* = kalkula valór ba kada liña tuir grupu liña sira (ezemplu "númeru 1 iha grupu").

#### D.4.1 Docstring, import no konstante

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 1-21}}

**2. Objetivu**

Esplika job batch: kalkula númeru hotu ba dashboard husi PostgreSQL no hakerek tabela analítika. Define konstante ba fatin la iha naran, fonte teste, no regex atu fahe `DATABASE_URL`.

**3. Fundamentu Python**

- **`re.compile(padraun)`** — prepara regex dala ida atu uza dala barak.
- **Grupu regex** `( ... )` kaer parte testu; `(?: ... )?` = grupu la kaer no opsionál.
- **`import sys`** — `sys.exit(kódigu)` remata programa ho kódigu.
- **`argparse`** — lee argumentu liña komando (`--watch`).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–5** — Docstring: job batch Spark; kalkula figura dashboard husi PostgreSQL (images, predictions, users), hakerek tabela analítika. Imajen ida konta dala ida ho prediksaun foun liu; imajen teste la tama; loron = loron Timor-Leste. Presiza PostgreSQL (Spark lee ho JDBC).
- **Liña 7** — Oinsá halai: `python -m spark.batch_analytics [--watch]`.
- **Liña 8** — Remata docstring.
- **Liña 10** — `import argparse`.
- **Liña 11** — `import re`.
- **Liña 12** — `import sys`.
- **Liña 13** — `import time`.
- **Liña 15–17** — Husi `config.settings`: `DATABASE_URL`, `LOCAL_TZ_OFFSET_HOURS` (fuso Timor), `ANALYTICS_CHECK_SECONDS` (30), `ANALYTICS_MAX_AGE_SECONDS` (600).
- **Liña 19** — `UNKNOWN_LOCATION = "Unknown location"` — naran ba fatin mamuk.
- **Liña 20** — `SCALE_TEST_SOURCE = "scalability_test"` — komentáriu: hanesan `storage/database.SCALE_TEST_SOURCE`.
- **Liña 21** — `_POSTGRES_URL_RE = re.compile(r"postgresql\+psycopg2://(?:([^:]+):([^@]*)@)?([^/]+)/(.+)")` — grupu 4: utilizadór, password, host:port, naran base de dadus.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* (password la loos): `postgresql+psycopg2://ews:***@localhost:5432/crop_ews` → grupu `("ews", "***", "localhost:5432", "crop_ews")`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `UNKNOWN_LOCATION`, `SCALE_TEST_SOURCE` | `str` |
| `_POSTGRES_URL_RE` | `re.Pattern` |
| `LOCAL_TZ_OFFSET_HOURS` | `int` |
| `ANALYTICS_*_SECONDS` | `float` |

**7. Ligasaun OOP**

`_POSTGRES_URL_RE` mak objetu `re.Pattern` ho métodu `.match()`. Arkivu ne'e la define klase.

**8. Impaktu integrasaun**

`run_all.py` hahú `python -m spark.batch_analytics --watch` (Modulu 06). Tabela ne'ebé job hakerek dashboard lee iha `spark/analytics_reader.py`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `SCALE_TEST_SOURCE` importante?
**R:** Atu imajen teste eskalabilidade (`source = 'scalability_test'`) la tama iha estatístika agrikultór.

#### D.4.2 `jdbc_url_and_properties` no `build_spark`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 24-56}}

**2. Objetivu**

Konverte `DATABASE_URL` (formatu SQLAlchemy) ba URL JDBC no dict propriedade (driver, utilizadór, password) ne'ebé Spark presiza. Harii SparkSession ho driver JDBC PostgreSQL.

**3. Fundamentu Python**

- **`match.groups()`** — fó tuple ho grupu regex hotu; unpacking ba variavel 4.
- **`raise RuntimeError(...)`** ho string naruk iha parénteze (string adjasente kombina automátiku).
- **Return tuple** `return jdbc_url, properties`.

**4. Esplikasaun liña-ba-liña**

- **Liña 24** — `def jdbc_url_and_properties() -> tuple:`
- **Liña 25** — Docstring: `DATABASE_URL` → (URL JDBC, propriedade); hamosu erru se la'ós PostgreSQL.
- **Liña 26** — `match = _POSTGRES_URL_RE.match(DATABASE_URL)`.
- **Liña 27** — `if not match:` — la'ós PostgreSQL (ezemplu SQLite):
- **Liña 28–33** — `raise RuntimeError(...)` — mensajen: job presiza PostgreSQL; leitór JDBC Spark presiza driver JDBC loloos; projetu la lori jar SQLite JDBC.
- **Liña 34** — `user, password, host_port, dbname = match.groups()`.
- **Liña 35** — `jdbc_url = f"jdbc:postgresql://{host_port}/{dbname}"` — formatu URL JDBC.
- **Liña 36** — `properties = {"driver": "org.postgresql.Driver"}` — klase driver Java.
- **Liña 37** — `if user:`
- **Liña 38** — `properties["user"] = user`.
- **Liña 39** — `if password is not None:` — (password mamuk `""` mós aseita).
- **Liña 40** — `properties["password"] = password`.
- **Liña 41** — `return jdbc_url, properties`.
- **Liña 44** — `def build_spark():`
- **Liña 45** — `from pyspark.sql import SparkSession`.
- **Liña 46** — `spark = (`
- **Liña 47** — `SparkSession.builder`.
- **Liña 48** — `.appName("ImageSystem-CropDiseaseEWS-Analytics")`.
- **Liña 49** — `.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")` — download driver JDBC PostgreSQL 42.7.3.
- **Liña 50** — Komentáriu: loron foti iha Timor-Leste Time esplísitu (`_local_day`).
- **Liña 51** — `.config("spark.sql.session.timeZone", "UTC")` — Spark trata tempu hotu iha UTC.
- **Liña 52** — `.config("spark.ui.showConsoleProgress", "false")`.
- **Liña 53** — `.getOrCreate()`.
- **Liña 54** — `)`.
- **Liña 55** — `spark.sparkContext.setLogLevel("WARN")`.
- **Liña 56** — `return spark`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `DATABASE_URL = "postgresql+psycopg2://ews:***@localhost:5432/crop_ews"` → `("jdbc:postgresql://localhost:5432/crop_ews", {"driver": "org.postgresql.Driver", "user": "ews", "password": "***"})`. Ho `DATABASE_URL = "sqlite:///ews.db"` → `RuntimeError`.

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `jdbc_url_and_properties()` | `tuple[str, dict[str, str]]` | `RuntimeError` |
| `build_spark()` | `SparkSession` | erru JVM |

**7. Ligasaun OOP**

Builder SparkSession (haree D.1.2). Not implemented in the current codebase: klase ba konfigurasaun JDBC — dict simples mak uza.

**8. Impaktu integrasaun**

Rezultadu uza iha `read_inputs` no `write_tables`. Password mai husi variavel ambiente/`config/settings.py` — dokumentu ne'e la hatudu valór loloos.

**9. Checkpoint ba principiante**

**P:** Tanba sá URL SQLAlchemy (`postgresql+psycopg2://`) la bele uza direta iha Spark?
**R:** Spark uza Java JDBC, ne'ebé presiza formatu `jdbc:postgresql://host:port/db` no driver Java `org.postgresql.Driver`.

#### D.4.3 Query input, `read_inputs`, `_local_day` no `_location`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 59-89}}

**2. Objetivu**

Define query SQL ne'ebé Spark lee (koluna presiza de'it — **nunka** email ka hash password), lee tabela 4 ho JDBC, no funsaun ajuda rua: loron Timor-Leste no limpeza fatin.

**3. Fundamentu Python**

- **String multi-liña** `"""..."""` no **f-string** multi-liña.
- **Generator iha `tuple(...)`**: `tuple(f(x) for x in (...))` harii tuple husi rezultadu kada elementu.
- **Import lokál** iha funsaun Spark.

**4. Esplikasaun liña-ba-liña**

- **Liña 59** — Komentáriu seksaun "inputs".
- **Liña 60** — Komentáriu: koluna presiza de'it (nunka email ka hash password) — privasidade.
- **Liña 61–63** — `_IMAGES_SQL` — subquery husi `images`: `image_id`, `user_id`, `location`, `status` (cast ba `VARCHAR(32)` tanba enum PostgreSQL), tempu 4; `WHERE source <> 'scalability_test'`; alias `i`.
- **Liña 64–67** — `_PREDICTIONS_SQL` — `prediction_id`, `image_id`, `predicted_class`, `risk_score`, `risk_level` (cast `VARCHAR(16)`), `predicted_at`, `processing_latency_ms`; alias `p`.
- **Liña 68** — `_USERS_SQL` — `user_id`, `is_deleted` husi `users`.
- **Liña 69–70** — `_SCALE_TEST_ITEMS_SQL` — `run_id`, `status`, `predicted_class`, `risk_level`, `location` husi `scale_test_items`.
- **Liña 73** — `def read_inputs(spark, jdbc_url, properties) -> tuple:`
- **Liña 74–75** — `return tuple(spark.read.jdbc(jdbc_url, sql, properties=properties) for sql in (...))` — DataFrame 4 tuir orden: images, predictions, users, scale_test_items. JDBC simu subquery `(SELECT ...) AS x` hanesan "tabela".
- **Liña 78** — Komentáriu seksaun "computation".
- **Liña 79** — `def _local_day(column):`
- **Liña 80** — Docstring: loron kalendáriu Timor-Leste husi koluna tempu.
- **Liña 81** — Import `col`, `expr`, `to_date`.
- **Liña 82** — `return to_date(col(column) + expr(f"INTERVAL {int(LOCAL_TZ_OFFSET_HOURS)} HOURS"))` — tempu UTC + oras fuso (Timor = +9) → data.
- **Liña 85** — `def _location():`
- **Liña 86** — Docstring: fatin limpa hanesan streaming; mamuk → `UNKNOWN_LOCATION`.
- **Liña 87** — Import funsaun Spark.
- **Liña 88** — `loc = initcap(regexp_replace(trim(coalesce(col("location"), lit(""))), r"\s+", " "))` — null → `""`, trim, espasu ida, Title Case.
- **Liña 89** — `return when(loc == "", lit(UNKNOWN_LOCATION)).otherwise(loc)`.

**5. Walkthrough ezekusaun**

- `_local_day("predicted_at")` ba `2026-10-20 16:30:00 UTC` → `16:30 + 9 h = 2026-10-21 01:30` → data `2026-10-21` (loron Timor, la'ós loron UTC).
- `_location()` ba `"  aileu  "` → `"Aileu"`; ba `null` → `"Unknown location"`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `_IMAGES_SQL` ... | `str` |
| `read_inputs` return | `tuple[DataFrame, DataFrame, DataFrame, DataFrame]` |
| `_local_day(column: str)` | return `Column` (tipu `date`) |
| `_location()` | return `Column` (`string`) |

**7. Ligasaun OOP**

`spark.read` fó objetu `DataFrameReader`; `.jdbc(...)` mak nia métodu. `when(...).otherwise(...)` — chaining métodu objetu `Column`. Projetu la define klase.

**8. Impaktu integrasaun**

Lee husi tabela PostgreSQL ne'ebé streaming job hakerek (Modulu 04). Regra fatin hanesan ho `parse_events` liña 108, entaun "Aileu" iha streaming no analítika hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá `_local_day` aumenta oras 9?
**R:** Tempu rai iha UTC. Fotografia ne'ebé foti 01:30 dadeer iha Timor mak 16:30 loron uluk iha UTC. Aumenta fuso hatudu loron loos ba agrikultór Timor.

#### D.4.4 `photos_with_latest_prediction` no funsaun ajuda agregasaun

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 92-127}}

**2. Objetivu**

Kombina imajen ho nia prediksaun **foun liu** (imajen ida = liña ida), no funsaun ajuda ki'ik ne'ebé harii agregasaun: konta kondisionál, konta tuir grupu, risku aas tuir grupu, no aumenta `generated_at`.

**3. Fundamentu Python**

- **`*keys`** — funsaun simu koluna grupu barak: `_count_photos(df, "location", "day")` → `keys = ("location", "day")`; `groupBy(*keys)` haruka hotu fali.
- **`from pyspark.sql import functions as F`** — naran badak `F`.

**4. Esplikasaun liña-ba-liña**

- **Liña 92** — `def photos_with_latest_prediction(images, predictions):`
- **Liña 93** — Docstring: liña ida kada imajen ho prediksaun foun liu (null se la iha).
- **Liña 94** — `from pyspark.sql import Window`.
- **Liña 95** — `from pyspark.sql.functions import col, row_number`.
- **Liña 96–97** — `newest = Window.partitionBy("image_id").orderBy(col("predicted_at").desc_nulls_last(), col("prediction_id").desc())` — janela: grupu tuir `image_id`, orden foun → tuan (null iha ikus), empate → `prediction_id` boot liu.
- **Liña 98** — `latest = (predictions.withColumn("_rn", row_number().over(newest))` — númeru 1, 2, 3... iha kada grupu.
- **Liña 99** — `.filter(col("_rn") == 1).drop("_rn", "prediction_id"))` — hela númeru 1 de'it; soe koluna ajuda.
- **Liña 100** — `return images.join(latest, "image_id", "left")` — *left join*: imajen hotu mantein; imajen la ho prediksaun hetan null.
- **Liña 103** — `def _count_if(cond):`
- **Liña 104** — Docstring: númeru liña ne'ebé `cond` loos.
- **Liña 105** — `from pyspark.sql import functions as F`.
- **Liña 106** — `return F.sum(F.when(cond, 1).otherwise(0)).cast("long")` — 1 se loos, 0 se lae, soma.
- **Liña 109** — `def _count_photos(df, *keys):`
- **Liña 110** — Docstring: liña kada `keys`, iha koluna `photos`.
- **Liña 111** — Import `F`.
- **Liña 112** — `return df.groupBy(*keys).agg(F.count("*").alias("photos"))`.
- **Liña 115** — `def _high_risk_by(df, *keys):`
- **Liña 116** — Docstring: kada `keys`: total analiza, high (risku HIGH) no high_pct.
- **Liña 117** — Import `F`.
- **Liña 118** — `return (df.filter(F.col("risk_level").isNotNull()).groupBy(*keys)` — imajen ho risku de'it.
- **Liña 119** — `.agg(_count_if(F.col("risk_level") == "HIGH").alias("high"), F.count("*").alias("total"))`.
- **Liña 120** — `.withColumn("high_pct", F.bround(F.col("high") / F.col("total") * 100, 1)))` — persentajen ho desimál 1 (*banker's rounding* `bround`).
- **Liña 123** — `def _stamped(tables: dict) -> dict:`
- **Liña 124** — Docstring: aumenta koluna `generated_at`.
- **Liña 125** — Import `F`.
- **Liña 126** — `now = F.current_timestamp()`.
- **Liña 127** — `return {name: df.withColumn("generated_at", now) for name, df in tables.items()}`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* — prediksaun ba imajen `a1`:

| prediction_id | predicted_at | predicted_class | `_rn` |
|---|---|---|---|
| 7 | 2026-10-20 02:00 | blight | 1 |
| 3 | 2026-10-19 08:00 | healthy | 2 |

Hela liña `_rn = 1` (blight). `_high_risk_by(located, "location")` ba Aileu ho risku 10 (HIGH 3) → `high=3, total=10, high_pct=30.0`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return |
|---|---|---|
| `photos_with_latest_prediction` | DataFrame rua | DataFrame |
| `_count_if` | `Column` | `Column` (`long`) |
| `_count_photos` | DataFrame, `*keys: str` | DataFrame (`keys` + `photos`) |
| `_high_risk_by` | DataFrame, `*keys` | DataFrame (`keys`, `high`, `total`, `high_pct`) |
| `_stamped` | `dict[str, DataFrame]` | `dict[str, DataFrame]` |

**7. Ligasaun OOP**

`Window` mak klase PySpark; `partitionBy(...).orderBy(...)` fó objetu `WindowSpec`. Funsaun ajuda sira hatudu **abstrasaun** (prinsípiu dezeñu): kódigu `compute` uza naran klaru (`_count_if`) no la repete espresaun naruk. Ne'e funsaun, la'ós klase.

**8. Impaktu integrasaun**

`refresh` bolu `photos_with_latest_prediction` (liña 228). Regra "prediksaun foun liu" hanesan ho `LATEST_PREDICTION_CTE` iha `storage/database.py` (dashboard), entaun númeru Spark no lista dashboard hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá *left* join no la'ós join normál?
**R:** Atu imajen ne'ebé seidauk iha prediksaun (ezemplu `FAILED` ka iha prosesu) mós konta iha total imajen.

#### D.4.5 `compute` — figura prinsipál dashboard

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 130-167}}

**2. Objetivu**

Kalkula distribuisaun moras, moras ne'ebé deteta barak liu, no tabela sumáriu ho figura prinsipál hotu (total, healthy, diseased, uncertain, risku aas, submisaun 24h, latênsia média...).

**3. Fundamentu Python**

- **Tuple assignment** `is_healthy, is_uncertain = cls == "healthy", cls == "uncertain"`.
- **Funsaun lokál** `avg_seconds(later, earlier)`.
- **Argumentu barak** iha `agg(...)` — kada ida koluna rezultadu ida.

**4. Esplikasaun liña-ba-liña**

- **Liña 130** — `def compute(photos, users) -> dict:`
- **Liña 131** — Docstring: `{naran tabela: DataFrame}` ba tabela analítika agrikultór hotu.
- **Liña 132** — Import `F`.
- **Liña 134** — `cls = F.lower(F.col("predicted_class"))` — klase letra ki'ik.
- **Liña 135** — `has_class = F.col("predicted_class").isNotNull()`.
- **Liña 136** — `is_healthy, is_uncertain = cls == "healthy", cls == "uncertain"`.
- **Liña 138** — `def avg_seconds(later, earlier):`
- **Liña 139** — `diff = F.col(later).cast("double") - F.col(earlier).cast("double")` — timestamp → segundu (double), subtrai.
- **Liña 140** — `return F.bround(F.avg(diff), 1)` — média, desimál 1.
- **Liña 142** — `distribution = _count_photos(photos.filter(has_class), "predicted_class")` — fotografia kada klase.
- **Liña 143** — `most_detected = (distribution.filter(F.lower(F.col("predicted_class")) != "healthy")` — moras de'it.
- **Liña 144** — `.orderBy(F.col("photos").desc(), F.col("predicted_class"))` — barak liu uluk; empate → alfabétiku.
- **Liña 145** — `.limit(1).select(F.col("predicted_class").alias("most_detected_disease")))`.
- **Liña 147** — `recent = F.col("uploaded_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")` — upload iha oras 24 ikus.
- **Liña 148** — `totals = photos.agg(` — agrega DataFrame tomak (liña ida).
- **Liña 149** — `F.count("*").alias("total_images"),`
- **Liña 150** — `_count_if(has_class & is_healthy).alias("total_healthy"),`
- **Liña 151** — `_count_if(has_class & ~is_healthy & ~is_uncertain).alias("total_diseased"),`
- **Liña 152** — `_count_if(has_class & is_uncertain).alias("total_uncertain"),`
- **Liña 153** — `_count_if(~has_class).alias("total_unclassified"),`
- **Liña 154** — `_count_if(F.col("risk_level") == "HIGH").alias("total_high_risk"),`
- **Liña 155** — `_count_if(F.col("predicted_at").isNotNull()).alias("total_processed"),`
- **Liña 156** — `_count_if(F.col("status") == "FAILED").alias("total_failed"),`
- **Liña 157** — `_count_if(recent).alias("submissions_24h"),`
- **Liña 158** — `F.countDistinct(F.when(recent, F.col("user_id"))).alias("active_farmers_24h"),` — agrikultór diferente iha 24h.
- **Liña 159** — `F.bround(F.avg("processing_latency_ms"), 1).alias("avg_processing_latency_ms"),`
- **Liña 160** — `avg_seconds("uploaded_at", "captured_at").alias("avg_capture_to_upload_seconds"),`
- **Liña 161** — `avg_seconds("ingested_at", "uploaded_at").alias("avg_upload_to_ingestion_seconds"),`
- **Liña 162** — `avg_seconds("predicted_at", "uploaded_at").alias("avg_end_to_end_latency_seconds"),`
- **Liña 163** — `)`.
- **Liña 164** — `active_users = users.agg(_count_if(~F.coalesce(F.col("is_deleted"), F.lit(False))).alias("total_users"))` — utilizadór la hamoos.
- **Liña 165** — `summary = (totals.crossJoin(active_users)` — kombina liña ida ho liña ida.
- **Liña 166** — `.join(most_detected, how="left")` — komentáriu: la iha liña → null.
- **Liña 167** — `.withColumn("summary_id", F.lit(1)))` — chave fixu 1.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fotografia 10: healthy 4, blight 3, gray_leaf_spot 2, uncertain 1; HIGH 3. `distribution` = `[(healthy,4),(blight,3),(gray_leaf_spot,2),(uncertain,1)]`. `most_detected` = `blight` (la konta healthy; 3 > 2 > 1). `totals`: `total_images=10, total_healthy=4, total_diseased=5, total_uncertain=1, total_unclassified=0, total_high_risk=3`.

Nota: `most_detected` la hasai `"uncertain"` — se uncertain barak liu moras hotu, nia bele sai `most_detected_disease`. Ne'e hahalok kódigu atuál (Implementadu).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `photos`, `users` | DataFrame |
| `cls`, `has_class`, `is_healthy`, `is_uncertain`, `recent` | `Column` |
| `distribution`, `most_detected`, `totals`, `active_users`, `summary` | DataFrame |
| `summary` | liña ida, koluna 17: 14 husi `totals` + `total_users` + `most_detected_disease` + `summary_id` (`generated_at` aumenta iha `_stamped`) |

**7. Ligasaun OOP**

Hotu mak objetu DataFrame/Column PySpark ho métodu chaining. Projetu la define klase.

**8. Impaktu integrasaun**

`summary` sai tabela `analytics_summary` → `analytics_reader.kpi_summary()` → kartaun KPI iha dashboard.

**9. Checkpoint ba principiante**

**P:** Tanba sá `crossJoin` seguru iha ne'e?
**R:** `totals` no `active_users` iha liña **ida** kada ida, entaun crossJoin fó liña ida de'it.

#### D.4.6 `compute` (parte 2) no `compute_scale_test`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 169-201}}

**2. Objetivu**

Kalkula figura tuir fatin no loron (tendénsia), tabela sumáriu loron-loron kada fatin, no fó fali dict ho tabela 7. `compute_scale_test` kalkula tabela 3 ba pájina teste eskalabilidade kada `run_id`.

**3. Fundamentu Python**

- **Dict literal** ho chave = naran tabela, valór = DataFrame.
- **Bolu funsaun iha valór dict** (`_count_photos(...)`).

**4. Esplikasaun liña-ba-liña**

- **Liña 169** — `located = photos.withColumn("location", _location())` — fatin limpu.
- **Liña 170** — `classified = (located.filter(F.col("predicted_at").isNotNull() & has_class)` — fotografia klasifika ona.
- **Liña 171** — `.withColumn("day", _local_day("predicted_at")))` — loron Timor.
- **Liña 172** — `location_daily = (located.withColumn("summary_date", _local_day("uploaded_at"))` — loron upload.
- **Liña 173** — `.groupBy("location", "summary_date")`.
- **Liña 174** — `.agg(F.count("*").alias("total_images"),`
- **Liña 175** — `_count_if(F.col("predicted_class") == "healthy").alias("total_healthy"),`
- **Liña 176–177** — `_count_if(has_class & (F.col("predicted_class") != "healthy") & (F.col("predicted_class") != "uncertain")).alias("total_diseased"),`
- **Liña 178** — `F.avg("risk_score").alias("avg_risk_score"),`
- **Liña 179** — `_count_if(F.col("risk_level") == "HIGH").alias("high_risk_count")))`.
- **Liña 181** — `return _stamped({` — tabela hotu ho `generated_at`:
- **Liña 182** — `"analytics_summary": summary,`
- **Liña 183** — `"analytics_disease_distribution": distribution,`
- **Liña 184** — `"analytics_risk_distribution": _count_photos(photos.filter(F.col("risk_level").isNotNull()), "risk_level"),` — kuantu LOW/MEDIUM/HIGH.
- **Liña 185** — `"analytics_high_risk_by_location": _high_risk_by(located, "location"),`
- **Liña 186** — `"analytics_disease_trend": _count_photos(classified, "location", "day", "predicted_class"),`
- **Liña 187** — `"analytics_disease_daily": _count_photos(classified, "day", "predicted_class"),`
- **Liña 188** — `"batch_location_daily_summary": location_daily,`
- **Liña 189** — `})`.
- **Liña 192** — `def compute_scale_test(items) -> dict:`
- **Liña 193** — Docstring: tabela ba pájina teste, kada `run_id`.
- **Liña 194** — Import `F`.
- **Liña 195** — `done = (items.filter((F.col("status") == "COMPLETED") & F.col("predicted_class").isNotNull())` — item remata ho klase.
- **Liña 196** — `.withColumn("location", _location()))`.
- **Liña 197** — `return _stamped({`
- **Liña 198** — `"analytics_scale_test_disease_by_location": _count_photos(done, "run_id", "location", "predicted_class"),`
- **Liña 199** — `"analytics_scale_test_disease_distribution": _count_photos(done, "run_id", "predicted_class"),`
- **Liña 200** — `"analytics_scale_test_high_risk_by_location": _high_risk_by(done, "run_id", "location"),`
- **Liña 201** — `})`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fotografia 3 iha Aileu, upload 2026-10-21 (loron Timor): healthy 1, blight 2 (risk 0.97, 0.85, healthy 0.05). `batch_location_daily_summary` → `("Aileu", 2026-10-21, total_images=3, total_healthy=1, total_diseased=2, avg_risk_score≈0.623, high_risk_count=2)`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `located`, `classified`, `location_daily`, `done` | DataFrame |
| `compute` return | `dict[str, DataFrame]` — chave 7 |
| `compute_scale_test` return | `dict[str, DataFrame]` — chave 3 |

**7. Ligasaun OOP**

Not implemented in the current codebase: klase ba tabela analítika iha Spark — dict naran → DataFrame mak uza.

**8. Impaktu integrasaun**

`refresh` kombina dict rua (`{**a, **b}`) no haruka ba `write_tables`. Naran chave tenke hanesan ho naran tabela iha `storage/schema.sql`/`storage/database.py` (Modulu 04), tanba `write_tables` uza `CREATE TABLE ... (LIKE naran)`.

**9. Checkpoint ba principiante**

**P:** Diferensa entre `analytics_disease_trend` no `analytics_disease_daily`?
**R:** `trend` grupu tuir fatin + loron + klase; `daily` grupu tuir loron + klase de'it (fatin hotu hamutuk).

#### D.4.7 `write_tables`, `refresh` no `run` — JDBC write sink

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 204-247}}

**2. Objetivu**

Hakerek DataFrame hotu ba PostgreSQL. Mekanizmu: (1) harii tabela `<naran>_staging` mamuk ho estrutura hanesan; (2) Spark hakerek ho JDBC `mode("append")` ba staging; (3) iha **transasaun ida**, hamoos tabela loloos, kopia husi staging, no soe staging. Dashboard nunka lee tabela mamuk.

**3. Fundamentu Python**

- **`with engine.begin() as conn:`** — transasaun SQLAlchemy: `COMMIT` se bloku remata di'ak, `ROLLBACK` se erru.
- **`try/finally`** — `photos.unpersist()` halai beibeik.
- **`spark or build_spark()`** — se `spark` mak `None`, harii foun.
- **`owns_spark`** — hatene se funsaun ne'e mak harii sesaun (no tenke taka).

**4. Esplikasaun liña-ba-liña**

- **Liña 204** — Komentáriu seksaun "output".
- **Liña 205** — `def write_tables(tables: dict, jdbc_url, properties) -> None:`
- **Liña 206–207** — Docstring: hakerek ba kópia `_staging`, depois troka hotu iha transasaun ida, atu dashboard nunka lee tabela mamuk.
- **Liña 208** — `from sqlalchemy import text`.
- **Liña 209** — `from storage.database import engine`.
- **Liña 210** — `with engine.begin() as conn:` — transasaun 1.
- **Liña 211** — `for name in tables:`
- **Liña 212** — `conn.execute(text(f"DROP TABLE IF EXISTS {name}_staging"))` — hamoos staging tuan.
- **Liña 213** — `conn.execute(text(f"CREATE TABLE {name}_staging (LIKE {name})"))` — kopia estrutura (koluna + tipu).
- **Liña 214** — `for name, df in tables.items():`
- **Liña 215** — `df.write.mode("append").jdbc(jdbc_url, f"{name}_staging", properties=properties)` — **Spark JDBC write**: hatama liña DataFrame ba staging (`INSERT` batch husi partisaun kada ida).
- **Liña 216** — `with engine.begin() as conn:` — transasaun 2 (atómiku).
- **Liña 217** — `for name in tables:`
- **Liña 218** — `conn.execute(text(f"DELETE FROM {name}"))`.
- **Liña 219** — `conn.execute(text(f"INSERT INTO {name} SELECT * FROM {name}_staging"))`.
- **Liña 220** — `conn.execute(text(f"DROP TABLE {name}_staging"))`.
- **Liña 223** — `def refresh(spark) -> float:`
- **Liña 224** — Docstring: rekalkula tomak dala ida; fó segundu.
- **Liña 225** — `started = time.monotonic()`.
- **Liña 226** — `jdbc_url, properties = jdbc_url_and_properties()`.
- **Liña 227** — `images, predictions, users, scale_test_items = read_inputs(spark, jdbc_url, properties)`.
- **Liña 228** — `photos = photos_with_latest_prediction(images, predictions).cache()` — `cache()` rai rezultadu iha memória tanba tabela barak uza nia.
- **Liña 229** — `try:`
- **Liña 230** — `write_tables({**compute(photos, users), **compute_scale_test(scale_test_items)}, jdbc_url, properties)` — kombina tabela 10 no hakerek.
- **Liña 231** — `finally:`
- **Liña 232** — `photos.unpersist()` — liberta memória cache.
- **Liña 233** — `return time.monotonic() - started`.
- **Liña 236** — `def run(spark=None) -> float:`
- **Liña 237** — Docstring: kalkula dala ida.
- **Liña 238** — `from storage.database import init_db`.
- **Liña 239** — `jdbc_url_and_properties()` — komentáriu: falla lalais ba SQLite molok hahú JVM.
- **Liña 240** — `init_db()` — komentáriu: tabela analítika iha ho nia tipu.
- **Liña 241** — `owns_spark = spark is None`.
- **Liña 242** — `spark = spark or build_spark()`.
- **Liña 243** — `try:`
- **Liña 244** — `return refresh(spark)`.
- **Liña 245** — `finally:`
- **Liña 246** — `if owns_spark:`
- **Liña 247** — `spark.stop()` — taka sesaun ne'ebé funsaun harii.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* ba tabela `analytics_risk_distribution`: (1) `DROP TABLE IF EXISTS analytics_risk_distribution_staging`; `CREATE TABLE analytics_risk_distribution_staging (LIKE analytics_risk_distribution)`. (2) Spark JDBC hatama `[(LOW, 5, t), (MEDIUM, 2, t), (HIGH, 3, t)]`. (3) Transasaun: `DELETE FROM analytics_risk_distribution` → `INSERT ... SELECT * FROM ..._staging` → `DROP TABLE ..._staging`. Dashboard ne'ebé lee durante pasu 3 haree dadus tuan (transasaun seidauk commit) ka foun — nunka mamuk.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception / efeitu |
|---|---|---|---|
| `write_tables` | `dict`, `str`, `dict` | `None` | DDL + DML iha PostgreSQL; erru SQL/JDBC sa'e |
| `refresh` | `SparkSession` | `float` (segundu) | erru sa'e (watch kaer) |
| `run` | `SparkSession ka None` | `float` | `RuntimeError` (la'ós PostgreSQL) |

**7. Ligasaun OOP**

`df.write` fó objetu `DataFrameWriter`; `.mode("append").jdbc(...)` mak builder/chaining. `engine` mak objetu SQLAlchemy `Engine` (partilla husi `storage/database.py`). `conn` mak objetu `Connection` iha context manager transasaun.

**8. Impaktu integrasaun**

Ne'e mak **mekanizmu loloos Spark hakerek ba PostgreSQL**: JDBC `mode("append")` ba tabela staging, depois SQLAlchemy troka. Streaming job (D.2) **la** uza Spark JDBC — nia uza SQLAlchemy liuhusi `persist_result`. Spark JDBC `mode("overwrite")` Not implemented in the current codebase (kódigu uza staging + transasaun atu evita tabela mamuk).

**9. Checkpoint ba principiante**

**P:** Tanba sá la hakerek direta ho `mode("overwrite")` ba tabela loloos?
**R:** `overwrite` hamoos tabela uluk, depois hakerek — durante tempu ne'e dashboard bele haree tabela mamuk. Staging + transasaun troka hotu dala ida.

#### D.4.8 `data_signature` no `watch`

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 250-293}}

**2. Objetivu**

`data_signature` kalkula "impresaun digitál" badak husi dadus (kontajen no tempu máximu). `watch` kontrola impresaun ne'e kada 30 s; se muda, ka liu 600 s desde kálkulu ikus ("24h ikus" muda ho tempu), nia halai `refresh`.

**3. Fundamentu Python**

- **`tuple(a) + tuple(b) + (x, y)`** — kombina tuple.
- **Komparasaun tuple** `signature != last_signature`.
- **`except KeyboardInterrupt:`** — Ctrl+C.
- **`flush=True`** iha `print` — hakerek kedas ba konsola.
- **`time.strftime('%H:%M:%S')`** — formatu oras.

**4. Esplikasaun liña-ba-liña**

- **Liña 250** — Komentáriu seksaun "watch".
- **Liña 251** — `def data_signature() -> tuple:`
- **Liña 252** — Docstring: impresaun barata; muda bainhira buat ruma ne'ebé figura uza muda.
- **Liña 253** — `from sqlalchemy import func` — funsaun SQL (`count`, `max`).
- **Liña 254** — Import `SessionLocal`, `Image`, `Prediction`, `User`, `ScaleTestItem`.
- **Liña 255** — `with SessionLocal() as s:`
- **Liña 256–257** — `images = s.query(func.count(Image.image_id), func.max(Image.created_at), func.max(Image.ingested_at), func.max(Image.processed_at)).one()` — liña ida ho valór 4.
- **Liña 258** — `predictions = s.query(func.count(Prediction.prediction_id), func.max(Prediction.predicted_at)).one()`.
- **Liña 259** — `users = s.query(func.count(User.user_id)).scalar()` — valór ida.
- **Liña 260** — `active_users = s.query(func.count(User.user_id)).filter(User.is_deleted.is_(False)).scalar()`.
- **Liña 261** — `scale_test = s.query(func.count(ScaleTestItem.image_id), func.max(ScaleTestItem.completed_at)).one()`.
- **Liña 262–263** — `scale_test_done = (s.query(func.count(ScaleTestItem.image_id)).filter(ScaleTestItem.status == "COMPLETED").scalar())`.
- **Liña 264** — `return tuple(images) + tuple(predictions) + (users, active_users) + tuple(scale_test) + (scale_test_done,)` — tuple ho valór 11.
- **Liña 267** — `def watch() -> int:`
- **Liña 268–269** — Docstring: rekalkula bainhira dadus muda, no minimu kada `ANALYTICS_MAX_AGE_SECONDS`.
- **Liña 270** — `from storage.database import init_db`.
- **Liña 271** — `jdbc_url_and_properties()` — falla lalais se la'ós PostgreSQL.
- **Liña 272** — `init_db()`.
- **Liña 273** — `spark = build_spark()` — sesaun ida ba loop tomak.
- **Liña 274–275** — `print(...)` — "Watching the database every 30s; recomputing ... when it changes".
- **Liña 276** — `last_signature, last_run = None, 0.0`.
- **Liña 277** — `try:`
- **Liña 278** — `while True:`
- **Liña 279** — `try:`
- **Liña 280** — `signature = data_signature()`.
- **Liña 281** — `stale = time.monotonic() - last_run >= ANALYTICS_MAX_AGE_SECONDS` — tuan liu 600 s?
- **Liña 282** — `if signature != last_signature or stale:`
- **Liña 283** — `seconds = refresh(spark)`.
- **Liña 284** — `last_signature, last_run = signature, time.monotonic()`.
- **Liña 285–286** — `print(...)` — "HH:MM:SS analytics recomputed (N photos) in X.Xs".
- **Liña 287** — `except Exception as e:` — komentáriu: base de dadus ka Spark problema; koko fali.
- **Liña 288** — `print(f"[spark-analytics] recompute failed, retrying: {e}", flush=True)`.
- **Liña 289** — `time.sleep(ANALYTICS_CHECK_SECONDS)` — hein 30 s.
- **Liña 290** — `except KeyboardInterrupt:`
- **Liña 291** — `return 0`.
- **Liña 292** — `finally:`
- **Liña 293** — `spark.stop()`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* volta 1: `last_signature = None` → refresh → `[spark-analytics] 09:00:00 analytics recomputed (120 photos) in 8.4s`. Volta 2 (30 s depois): la iha fotografia foun, `stale = False` → la halo buat ida. Fotografia foun mai → `count(Image)` muda 120 → 121 → refresh. Se PostgreSQL mate → "recompute failed, retrying" no koko fali 30 s depois.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `data_signature` return | `tuple` ho elementu 11 (`int`, `datetime ka None`) |
| `signature`, `last_signature` | `tuple ka None` |
| `last_run` | `float` |
| `stale` | `bool` |
| `watch` return | `int` (0) |

**7. Ligasaun OOP**

`s.query(...)` — métodu objetu `Session`; `Image.image_id` mak atributu **klase** ORM (koluna), la'ós atributu instánsia. Not implemented in the current codebase: padraun *Observer* (notifikasaun base de dadus) — kódigu uza *polling* (husu kada 30 s).

**8. Impaktu integrasaun**

`run_all.py` hahú `--watch`. Kada streaming job hakerek prediksaun foun, impresaun muda no analítika atualiza iha ~30 s.

**9. Checkpoint ba principiante**

**P:** Tanba sá iha `stale` maske dadus la muda?
**R:** Figura "submissions_24h" no "active_farmers_24h" muda ho tempu: fotografia ne'ebé tama oras 25 liubá tenke sai husi figura maske la iha dadus foun.

#### D.4.9 `main` — liña komando

**1. Kódigu orijinál**

{{code spark/batch_analytics.py 296-307}}

**2. Objetivu**

Lee argumentu liña komando: `--watch` → `watch()`; se lae, kalkula dala ida no hakerek tempu.

**3. Fundamentu Python**

- **`argparse.ArgumentParser`** — objetu ne'ebé define no lee argumentu.
- **`action="store_true"`** — argumentu `--watch` sai `True` se iha.
- **`argv=None`** — se `None`, lee `sys.argv` (permite teste haruka argumentu).
- **`sys.exit(main())`** — kódigu saída programa.

**4. Esplikasaun liña-ba-liña**

- **Liña 296** — `def main(argv=None) -> int:`
- **Liña 297** — `p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)` — ajuda (`--help`) hatudu docstring modulu.
- **Liña 298** — `p.add_argument("--watch", action="store_true", help="keep the analytics tables up to date")`.
- **Liña 299** — `args = p.parse_args(argv)`.
- **Liña 300** — `if args.watch:`
- **Liña 301** — `return watch()`.
- **Liña 302** — `print(f"[spark-analytics] analytics recomputed in {run():.1f}s")` — halai dala ida.
- **Liña 303** — `return 0`.
- **Liña 306** — `if __name__ == "__main__":`
- **Liña 307** — `sys.exit(main())`.

**5. Walkthrough ezekusaun**

`python -m spark.batch_analytics` → `args.watch = False` → `run()` → print "analytics recomputed in 9.2s" (*ezemplu ilustrativu*) → saída 0. `python -m spark.batch_analytics --watch` → loop `watch()`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `p` | `argparse.ArgumentParser` |
| `args` | `argparse.Namespace` (`args.watch: bool`) |
| return | `int` |

**7. Ligasaun OOP**

`ArgumentParser` mak klase biblioteka padraun; `p` mak objetu; `add_argument` no `parse_args` mak métodu. `args` mak objetu `Namespace` ho atributu dinámiku.

**8. Impaktu integrasaun**

`run_all.py` hahú prosesu ho `--watch` (Modulu 06).

**9. Checkpoint ba principiante**

**P:** Saida mak `args.watch` se ita la hakerek `--watch`?
**R:** `False`, tanba `action="store_true"` fó `False` hanesan default.

### D.5 `spark/live_log.py` — liña tempu-real iha konsola

#### D.5.1 Docstring, `ENABLED`, `_LOCK` no `say`

**1. Kódigu orijinál**

{{code spark/live_log.py 1-26}}

**2. Objetivu**

Hakerek liña badak ho oras iha janela konsola EWS-Spark. Variavel ambiente `SPARK_LIVE_LOG=false` hamate. *Lock* garante katak thread barak la hakerek liña kahur malu.

**3. Fundamentu Python**

- **`threading.Lock()`** — objetu ne'ebé thread ida de'it bele kaer iha tempu ida.
- **`with _LOCK:`** — kaer lock, hakerek, fó fali automátiku.
- **Espresaun `not in (tuple)`**: kontrola valór la iha lista.
- **`str.strip().lower()`** — hasai espasu, letra ki'ik.
- **`except (A, B): pass`** — ignora erru rua ne'e.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–5** — Docstring: liña tempu-real iha konsola EWS-Spark — batch simu, rezultadu kada fotografia, rezultadu to'o Drive, batch commit. `SPARK_LIVE_LOG=false` hamate.
- **Liña 7** — `import os`.
- **Liña 8** — `import sys` — `sys.stdout`.
- **Liña 9** — `import threading`.
- **Liña 10** — `from datetime import datetime`.
- **Liña 11** — `from functools import lru_cache`.
- **Liña 13** — `ENABLED = os.getenv("SPARK_LIVE_LOG", "true").strip().lower() not in ("0", "false", "no", "off")` — `True` exetu valór 4 ne'e.
- **Liña 14** — `_LOCK = threading.Lock()`.
- **Liña 17** — `def say(line: str) -> None:`
- **Liña 18** — `if not ENABLED:`
- **Liña 19** — `return`.
- **Liña 20** — `text = f"[spark {datetime.now().strftime('%H:%M:%S')}] {line}\n"` — oras lokál laptop.
- **Liña 21** — `with _LOCK:`
- **Liña 22** — `try:`
- **Liña 23** — `sys.stdout.write(text)`.
- **Liña 24** — `sys.stdout.flush()` — hatudu kedas.
- **Liña 25** — `except (UnicodeEncodeError, OSError):` — konsola Windows la bele hatudu karakter ka konsola taka.
- **Liña 26** — `pass` — log nunka halo pipeline monu.

**5. Walkthrough ezekusaun**

`say("[kafka]  batch 42: 2 photo event(s) received from Kafka")` iha oras 09:15:02 → `[spark 09:15:02] [kafka]  batch 42: 2 photo event(s) received from Kafka`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `ENABLED` | `bool` |
| `_LOCK` | `threading.Lock` |
| `line`, `text` | `str` |
| `say` return | `None` |

**7. Ligasaun OOP**

`_LOCK` mak objetu nível modulu ne'ebé thread hotu partilla. `sys.stdout` mak objetu arkivu. Not implemented in the current codebase: klase logger rasik (funsaun ho estadu modulu mak uza).

**8. Impaktu integrasaun**

Funsaun hotu iha arkivu ne'e bolu `say`. `streaming_job` bolu `say` direta (liña 241, 323).

**9. Checkpoint ba principiante**

**P:** Saida mak bele akontese la ho `_LOCK`?
**R:** Thread rua bele hakerek iha tempu hanesan no letra husi liña rua kahur malu iha konsola.

#### D.5.2 `farmer_label` no `batch_started`

**1. Kódigu orijinál**

{{code spark/live_log.py 29-44}}

**2. Objetivu**

Hatudu email agrikultór (ka naran utilizadór) iha liña konsola; rai rezultadu iha cache atu la husu base de dadus dala barak. Hakerek liña "batch simu".

**3. Fundamentu Python**

- **Decorator `@lru_cache(maxsize=4096)`** — rai to'o rezultadu 4096; chamada ho `user_id` hanesan fó rezultadu husi cache.
- **Slicing** `user_id[:8]` — karakter 8 dahuluk.
- **`a or b`** — `a` se loos, se lae `b`.

**4. Esplikasaun liña-ba-liña**

- **Liña 29** — `@lru_cache(maxsize=4096)`.
- **Liña 30** — `def farmer_label(user_id: str) -> str:`
- **Liña 31** — Docstring: Gmail agrikultór, ba hatudu.
- **Liña 32** — `if not user_id:`
- **Liña 33** — `return "?"`.
- **Liña 34** — `try:`
- **Liña 35** — `from storage.database import SessionLocal, User` — import lazy.
- **Liña 36** — `with SessionLocal() as s:`
- **Liña 37** — `user = s.get(User, user_id)`.
- **Liña 38** — `return (user.email or user.username) if user else user_id[:8]`.
- **Liña 39** — `except Exception:` — komentáriu: só ba hatudu.
- **Liña 40** — `return user_id[:8]`.
- **Liña 43** — `def batch_started(batch_id, n_events: int) -> None:`
- **Liña 44** — `say(f"[kafka]  batch {batch_id}: {n_events} photo event(s) received from Kafka")`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `farmer_label("u-9f3a7c21-...")` dala uluk → konsulta → `"agrikultor@example.com"`; dala rua → cache (la konsulta). Se base de dadus mate → `"u-9f3a7c"`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return |
|---|---|---|
| `farmer_label` | `user_id: str` | `str` |
| `batch_started` | `batch_id` (`int`), `n_events: int` | `None` |

**7. Ligasaun OOP**

`lru_cache` falun funsaun ho objetu ne'ebé iha métodu `cache_info()`/`cache_clear()` — decorator fó fali **objetu** ne'ebé bele bolu. `user.email` mak atributu objetu ORM.

**8. Impaktu integrasaun**

`photo_line` no `result_delivered` uza `farmer_label`. `foreach_batch_sink` bolu `batch_started` (streaming_job liña 231).

**9. Checkpoint ba principiante**

**P:** Tanba sá cache importante iha ne'e?
**R:** Batch ida bele iha fotografia barak husi agrikultór hanesan; cache evita konsulta base de dadus ba kada liña.

#### D.5.3 `photo_line`, `photo_done`, `result_delivered`, `batch_committed`

**1. Kódigu orijinál**

{{code spark/live_log.py 47-81}}

**2. Objetivu**

Formata liña rezultadu fotografia (klase, konfiansa, risku, tempu kada pasu), liña entrega Drive, no liña commit batch.

**3. Fundamentu Python**

- **`isinstance(x, (int, float))`** — kontrola tipu barak.
- **`" ".join(gerador)`** — kombina string ho separadór.
- **Generator ho kondisaun**: `(... for key, label in pares if ...)`.
- **Formatu** `{conf * 100:.2f}%`, `{(error or '')[:160]}`.

**4. Esplikasaun liña-ba-liña**

- **Liña 47–48** — `def photo_line(event: dict, status: str, result: dict = None, error: str = None, seconds: float = None) -> str:`
- **Liña 49–50** — `head = (f"{(event.get('image_id') or '?')[:8]}  {farmer_label(event.get('user_id'))}  " f"{event.get('location') or 'no location'}")` — ID badak, agrikultór, fatin.
- **Liña 51** — `if status != "COMPLETED":`
- **Liña 52** — `return f"[photo]  {head}  FAILED: {(error or '')[:160]}"` — erru maximu karakter 160.
- **Liña 53** — `t = (result or {}).get("timings") or {}`.
- **Liña 54** — `conf = result.get("prediction_confidence")`.
- **Liña 55** — `verdict = (f"{result.get('predicted_class')}"`
- **Liña 56** — `+ (f" {conf * 100:.2f}%" if isinstance(conf, (int, float)) else "")` — persentajen se númeru.
- **Liña 57** — `+ f"  risk {result.get('risk_level')}")`.
- **Liña 58** — `stages = "  ".join(f"{label} {t[key]:.2f}s" for key, label in`
- **Liña 59** — `(("read_s", "download"), ("preprocess_s", "preprocess"), ("cnn_s", "cnn"),`
- **Liña 60** — `("render_s", "render")) if isinstance(t.get(key), (int, float)))` — pasu ne'ebé iha tempu de'it.
- **Liña 61** — `total = f"  total {seconds:.2f}s" if seconds is not None else ""`.
- **Liña 62** — `return f"[photo]  {head}  -> {verdict}  ({stages}{total})"`.
- **Liña 65** — `def photo_done(...) -> None:`
- **Liña 66** — `say(photo_line(event, status, result, error, seconds))`.
- **Liña 69** — `def result_delivered(event: dict, delivered: bool, seconds: float) -> None:`
- **Liña 70** — `image_id = (event.get("image_id") or "?")[:8]`.
- **Liña 71** — `if delivered:`
- **Liña 72–73** — `say(...)` — "result + processed image delivered to <agrikultór>'s Drive (X.XXs)".
- **Liña 74** — `else:`
- **Liña 75–76** — `say(...)` — "result NOT delivered to Drive — `python -m storage.requeue_stuck` re-sends it".
- **Liña 79** — `def batch_committed(batch_id, ok: int, failed: int, skipped: int, seconds: float) -> None:`
- **Liña 80–81** — `say(...)` — "batch N committed: X ok, Y failed, Z skipped in S.SSs".

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `photo_line({"image_id": "a1b2c3d4e5", "user_id": "u-9", "location": "Aileu"}, "COMPLETED", {"predicted_class": "blight", "prediction_confidence": 0.9712, "risk_level": "HIGH", "timings": {"read_s": 1.2, "preprocess_s": 0.15, "cnn_s": 0.08}}, None, 2.1)` → `[photo]  a1b2c3d4  agrikultor@example.com  Aileu  -> blight 97.12%  risk HIGH  (download 1.20s  preprocess 0.15s  cnn 0.08s  total 2.10s)`.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `photo_line` | `str` |
| `photo_done`, `result_delivered`, `batch_committed` | `None` |
| `t` | `dict` |
| `conf` | `float ka None` |

**7. Ligasaun OOP**

Not implemented in the current codebase: klase formatadór. Funsaun simples ne'ebé fahe serbisu: `photo_line` formata (fó `str`), `photo_done` hakerek — ne'e prinsípiu *separation of concerns*.

**8. Impaktu integrasaun**

`_process_one` bolu `photo_done` (streaming_job liña 297); `_upload_result` bolu `result_delivered` (liña 255); `foreach_batch_sink` bolu `batch_committed` (liña 242). Janela `EWS-KafkaMonitor` (Modulu 02) mós hatudu liña tempu-real.

**9. Checkpoint ba principiante**

**P:** Tanba sá `photo_line` fó `str` no la hakerek direta?
**R:** Atu formatu bele reuza ka teste la ho hakerek ba konsola; `photo_done` mak hakerek.

### D.6 `spark/analytics_reader.py` — dashboard lee tabela Spark

#### D.6.1 Docstring, import no `_read_sql`

**1. Kódigu orijinál**

{{code spark/analytics_reader.py 1-14}}

**2. Objetivu**

Modulu ne'e mak "leitór": dashboard lee tabela ne'ebé Spark kalkula, plus lista fotografia nível-liña. `_read_sql` mak funsaun ajuda ba query pandas.

**3. Fundamentu Python**

- **`import pandas as pd`** — naran badak `pd`.
- **`params: dict = None`** — parámetru SQL ho naran (seguru kontra SQL injection).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: lee tabela analítika ne'ebé Spark kalkula (`spark/batch_analytics.py`) ba dashboard, plus lista fotografia nível-liña.
- **Liña 6** — `import pandas as pd`.
- **Liña 7** — `from sqlalchemy import text`.
- **Liña 9** — `engine`, `LATEST_PREDICTION_CTE` husi `storage.database`.
- **Liña 10** — `series_to_local` husi `config.localtime` — UTC → Timor.
- **Liña 13** — `def _read_sql(query: str, params: dict = None) -> pd.DataFrame:`
- **Liña 14** — `return pd.read_sql(text(query), engine, params=params)`.

**5. Walkthrough ezekusaun**

`_read_sql("SELECT * FROM analytics_summary")` → `pd.DataFrame` ho liña ida.

**6. Tipu dadus**

`_read_sql`: `query: str`, `params: dict ka None` → `pandas.DataFrame`.

**7. Ligasaun OOP**

`engine` mak objetu SQLAlchemy partilla. Funsaun simples.

**8. Impaktu integrasaun**

Dashboard (Streamlit) import modulu ne'e. Spark la lee iha ne'e — pandas lee rezultadu Spark.

**9. Checkpoint ba principiante**

**P:** Pandas DataFrame no Spark DataFrame hanesan?
**R:** Lae. Pandas DataFrame iha memória komputadór ida; Spark DataFrame distribuidu no lazy. Dashboard uza pandas tanba tabela analítika ki'ik.

#### D.6.2 Klase `SparkTable`

**1. Kódigu orijinál**

{{code spark/analytics_reader.py 17-41}}

**2. Objetivu**

Klase ida ba leitór tabela analítika ida. Objetu hatene naran tabela, koluna, orden, renomeia no koluna data. Bolu objetu (`tabela()`) fó DataFrame pandas; se tabela seidauk iha, fó DataFrame mamuk (dashboard la monu).

**3. Fundamentu Python**

- **`class SparkTable:`** — klase rasik projetu.
- **`__init__(self, ...)`** — konstrutór; rai parámetru hanesan atributu instánsia (`self.name`...).
- **Atribuisaun múltipla** `self.name, self.cols, self.order_by = name, cols, order_by`.
- **`rename or {}`** — default dict mamuk (evita default mutável iha definisaun).
- **`__call__(self, run_id=None)`** — objetu bele bolu hanesan funsaun.
- **`dates=()`** — default tuple mamuk.

**4. Esplikasaun liña-ba-liña**

- **Liña 17** — `class SparkTable:`
- **Liña 18–19** — Docstring: leitór ba tabela analítika ida; frame mamuk se tabela seidauk iha; tabela teste eskala simu `run_id`.
- **Liña 21** — `def __init__(self, name: str, cols: list = None, order_by: str = None, rename: dict = None, dates=()):`
- **Liña 22** — `self.name, self.cols, self.order_by = name, cols, order_by` — atributu instánsia 3.
- **Liña 23** — `self.rename, self.dates = rename or {}, dates`.
- **Liña 25** — `def __call__(self, run_id: str = None) -> pd.DataFrame:`
- **Liña 26** — `query, params = f"SELECT * FROM {self.name}", None`.
- **Liña 27** — `if run_id is not None:`
- **Liña 28** — `query, params = query + " WHERE run_id = :run_id", {"run_id": run_id}` — parámetru ho naran (`:run_id`), la tau valór direta iha SQL.
- **Liña 29** — `if self.order_by:`
- **Liña 30** — `query += f" ORDER BY {self.order_by}"`.
- **Liña 31** — `try:`
- **Liña 32** — `df = _read_sql(query, params).rename(columns=self.rename)`.
- **Liña 33** — `except Exception:` — komentáriu: tabela la iha.
- **Liña 34** — `df = pd.DataFrame()`.
- **Liña 35** — `if self.cols is None:`
- **Liña 36** — `return df` — koluna hotu.
- **Liña 37** — `if df.empty:`
- **Liña 38** — `return pd.DataFrame(columns=self.cols)` — mamuk maibé ho koluna (gráfiku la monu).
- **Liña 39** — `for col in self.dates:`
- **Liña 40** — `df[col] = pd.to_datetime(df[col])`.
- **Liña 41** — `return df[self.cols].reset_index(drop=True)` — koluna tuir orden; índise 0..n.

**5. Walkthrough ezekusaun**

`disease_distribution = SparkTable("analytics_disease_distribution", ["predicted_class", "count"], "photos DESC, predicted_class", rename={"photos": "count"})`. Bolu `disease_distribution()` → SQL `SELECT * FROM analytics_disease_distribution ORDER BY photos DESC, predicted_class` → renomeia `photos` → `count` → fó koluna `["predicted_class", "count"]`. *Ezemplu ilustrativu* rezultadu: `[("healthy", 4), ("blight", 3)]`.

**6. Tipu dadus**

| Atributu / parámetru | Tipu |
|---|---|
| `self.name` | `str` |
| `self.cols` | `list[str] ka None` |
| `self.order_by` | `str ka None` |
| `self.rename` | `dict[str, str]` |
| `self.dates` | `tuple` / sekuénsia `str` |
| `__call__(run_id)` | `str ka None` → `pd.DataFrame` |

**7. Ligasaun OOP**

- **Klase no objetu**: `SparkTable` mak klase; `disease_distribution`, `risk_distribution`... (D.6.4) mak **objetu** (instánsia) ne'ebé harii iha nível modulu.
- **`__init__` no `self`**: konstrutór rai konfigurasaun iha atributu instánsia; `self` mak objetu ida-idak — `disease_distribution.name` no `risk_distribution.name` diferente.
- **Atributu instánsia**: `name`, `cols`, `order_by`, `rename`, `dates`. Atributu klase: la iha.
- **Enkapsulamentu**: SQL, renomeia no tratamentu erru subar iha laran; dashboard só bolu `tabela()`.
- **Polimorfizmu/protokolu**: `__call__` halo objetu "callable" — dashboard uza objetu hanesan funsaun (`disease_distribution()`) no funsaun normál (`kpi_summary()`) ho forma hanesan.
- **Herança**: Not implemented in the current codebase (`SparkTable` herda `object` de'it).

**8. Impaktu integrasaun**

Objetu sira iha D.6.4 uza husi pájina dashboard. Naran tabela tenke hanesan ho chave iha `compute`/`compute_scale_test` (D.4.6).

**9. Checkpoint ba principiante**

**P:** Saida mak `self` iha `self.name`?
**R:** Objetu ne'ebé ita bolu. Bainhira `disease_distribution()` halai, `self` = objetu `disease_distribution`, entaun `self.name = "analytics_disease_distribution"`.

#### D.6.3 `images_with_predictions` no `kpi_summary`

**1. Kódigu orijinál**

{{code spark/analytics_reader.py 44-80}}

**2. Objetivu**

`images_with_predictions`: lista fotografia nível-liña ho prediksaun foun liu (la ho teste), tempu konverte ba Timor. `kpi_summary`: figura prinsipál husi `analytics_summary` hanesan dict, ka `None` molok Spark halai dala uluk.

**3. Fundamentu Python**

- **Type hint `dict | None`** — Python 3.10+: dict ka None.
- **`df.iloc[0]`** — liña dahuluk.
- **`pd.isna(x)`** — kontrola valór "mamuk" pandas (NaN/None).
- **`int(...)`, `float(...)`** — konverte tipu numpy ba tipu Python.

**4. Esplikasaun liña-ba-liña**

- **Liña 44** — `def images_with_predictions() -> pd.DataFrame:`
- **Liña 45** — Docstring: liña ida kada imajen agrikultór ho prediksaun foun liu (imajen teste la tama).
- **Liña 46** — `query = f"""`
- **Liña 47** — `{LATEST_PREDICTION_CTE}` — CTE `latest` ho `ROW_NUMBER()` (Modulu 04).
- **Liña 48** — `SELECT`
- **Liña 49** — koluna imajen: `image_id`, `location`, `user_id`, `source`, `status`,
- **Liña 50** — tempu: `captured_at`, `uploaded_at`, `ingested_at`, `processed_at`,
- **Liña 51** — `gps_latitude`, `gps_longitude`, `camera_make`, `camera_model`,
- **Liña 52** — prediksaun: `predicted_class`, `prediction_confidence`,
- **Liña 53** — feature: `lesion_score`, `discoloration_pct`, `texture_irregularity`,
- **Liña 54** — `leaf_area_affected_pct`, `risk_score`, `risk_level`,
- **Liña 55** — `model_version`, `predicted_at`, `processing_latency_ms`
- **Liña 56** — `FROM images i`
- **Liña 57** — `LEFT JOIN latest p ON p.image_id = i.image_id AND p.rn = 1` — prediksaun númeru 1.
- **Liña 58** — `WHERE i.source <> 'scalability_test'`.
- **Liña 59** — `"""`.
- **Liña 60** — `df = _read_sql(query)`.
- **Liña 61** — `for col in [...]:` — koluna tempu 5.
- **Liña 62** — `if col in df.columns:`
- **Liña 63** — Komentáriu: rai UTC, hatudu Timor-Leste Time.
- **Liña 64** — `df[col] = series_to_local(df[col])`.
- **Liña 65** — `return df`.
- **Liña 68** — `def kpi_summary() -> dict | None:`
- **Liña 69** — Docstring: figura prinsipál, ka `None` molok Spark halai dala uluk.
- **Liña 70** — `summary = SparkTable("analytics_summary")()` — harii objetu no bolu kedas (koluna hotu).
- **Liña 71** — `if summary.empty:`
- **Liña 72** — `return None`.
- **Liña 73** — `row = summary.iloc[0]`.
- **Liña 74–75** — `keys = (...)` — chave 9: `total_users`, `total_images`, `total_healthy`, `total_diseased`, `total_uncertain`, `total_unclassified`, `total_high_risk`, `submissions_24h`, `active_farmers_24h`.
- **Liña 76** — `out = {k: int(row[k]) for k in keys}`.
- **Liña 77** — `out["most_detected_disease"] = None if pd.isna(row["most_detected_disease"]) else row["most_detected_disease"]`.
- **Liña 78** — `latency = row["avg_processing_latency_ms"]`.
- **Liña 79** — `out["avg_processing_latency_ms"] = None if pd.isna(latency) else float(latency)`.
- **Liña 80** — `return out`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `analytics_summary` ho `total_images=10, total_healthy=4, most_detected_disease="blight", avg_processing_latency_ms=2100.5` → `kpi_summary()` = `{"total_users": 3, "total_images": 10, "total_healthy": 4, ..., "most_detected_disease": "blight", "avg_processing_latency_ms": 2100.5}`. Molok Spark halai: tabela mamuk → `None`.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `images_with_predictions` | `pd.DataFrame` (koluna 24) |
| `kpi_summary` | `dict[str, int ka str ka float ka None]` ka `None` |

**7. Ligasaun OOP**

`SparkTable("analytics_summary")()` — harii **objetu temporáriu** no bolu nia `__call__` kedas. `row` mak objetu `pd.Series`.

**8. Impaktu integrasaun**

Dashboard bolu `kpi_summary()` ba kartaun KPI no `images_with_predictions()` ba tabela/mapa fotografia. `images_with_predictions` lee direta husi `images`/`predictions` (la'ós husi Spark), maibé uza regra "prediksaun foun liu" hanesan Spark.

**9. Checkpoint ba principiante**

**P:** Tanba sá `int(row[k])` presiza?
**R:** Pandas fó tipu numpy (`numpy.int64`). Konverte ba `int` Python halo dict simples ba dashboard no JSON.

#### D.6.4 Objetu `SparkTable` ba kada tabela

**1. Kódigu orijinál**

{{code spark/analytics_reader.py 83-102}}

**2. Objetivu**

Harii objetu `SparkTable` ida ba kada tabela analítika ne'ebé dashboard uza, ho koluna, orden no renomeia.

**3. Fundamentu Python**

- **Harii objetu ho argumentu tuir pozisaun no ho naran** (`rename=...`, `dates=...`).
- **Tuple ho elementu ida** `("day",)` — vírgula obrigatóriu.

**4. Esplikasaun liña-ba-liña**

- **Liña 83–84** — `disease_distribution` — tabela `analytics_disease_distribution`, koluna `predicted_class`, `count`; orden `photos DESC, predicted_class`; `photos` → `count`.
- **Liña 85–86** — `risk_distribution` — `analytics_risk_distribution`, koluna `risk_level`, `count`.
- **Liña 87–88** — `high_risk_locations` — `analytics_high_risk_by_location`, koluna `location`, `high`, `total`, `high_pct`; orden `high DESC, high_pct DESC, location`.
- **Liña 89–90** — `disease_trend_by_location` — `analytics_disease_trend`, koluna `location`, `day`, `predicted_class`, `photos`; `day` konverte data.
- **Liña 91–92** — `disease_daily` — `analytics_disease_daily`, koluna `day`, `predicted_class`, `photos`.
- **Liña 94** — Komentáriu: run teste eskala ida (haruka `run_id`).
- **Liña 95–97** — `scale_test_disease_by_location` — orden `SUM(photos) OVER (PARTITION BY location) DESC, ...` (fatin ho fotografia barak liu uluk).
- **Liña 98–99** — `scale_test_disease_distribution` — koluna `predicted_class`, `photos`.
- **Liña 100–102** — `scale_test_high_risk_by_location` — koluna `location`, `high`, `total`, `high_pct`.

**5. Walkthrough ezekusaun**

`scale_test_disease_distribution("run-123")` (*ezemplu ilustrativu* `run_id`) → `SELECT * FROM analytics_scale_test_disease_distribution WHERE run_id = :run_id ORDER BY photos DESC, predicted_class` ho `{"run_id": "run-123"}`.

**6. Tipu dadus**

Objetu 8 hotu: tipu `SparkTable`.

**7. Ligasaun OOP**

Klase ida, **objetu barak** ho konfigurasaun diferente — ezemplu klaru reuza klase. Objetu sira moris bainhira modulu import no moris to'o prosesu remata.

**8. Impaktu integrasaun**

Pájina dashboard (Overview, Disease Analytics, Scalability test) bolu objetu sira ne'e. Naran tabela tenke hanesan ho `compute`/`compute_scale_test` (D.4.6).

**9. Checkpoint ba principiante**

**P:** Objetu hira mak harii husi klase `SparkTable` iha bloku ne'e?
**R:** Ualu (8): 5 ba agrikultór, 3 ba teste eskalabilidade.

## E. Analize OOP (Object-Oriented Programming)

Seksaun ne'e hatudu OOP ne'ebé **loloos iha** kódigu Spark. Bainhira konseitu la iha, ita hakerek "Not implemented in the current codebase." no esplika teoria iha **Nota aprendizajen Python** — teoria ne'e la'ós implementasaun.

### E.1 Klase ne'ebé projetu define iha modulu Spark

| Klase | Arkivu | Herda husi | `__init__` | Atributu | Métodu | Uza iha |
|---|---|---|---|---|---|---|
| `InvalidEvent` | `spark/preprocessing.py` liña 45–46 | `Exception` | herda husi `Exception` | mensajen (herda, `e.args`) | la iha rasik | `raise` iha `clean_event`, `_clean_drive_event`, `_parse_timestamp`, `process_and_persist`; `except` iha `_process_one` |
| `SparkTable` | `spark/analytics_reader.py` liña 17–41 | `object` | sin (liña 21–23) | `name`, `cols`, `order_by`, `rename`, `dates` | `__call__` | objetu 8 iha liña 83–102, plus `kpi_summary` |

Klase seluk hotu ne'ebé kódigu Spark uza (`SparkSession`, `DataFrame`, `Column`, `Window`, `StructType`, `ThreadPoolExecutor`, `Future`, `Path`, `PIL.Image`, `Session` SQLAlchemy, klase ORM `Image`/`User`/`ScaleTestRun`...) mai husi biblioteka ka modulu projetu seluk (Modulu 04).

### E.2 Objetu: oinsá harii no uza

- **`SparkTable`**: harii iha nível modulu — `disease_distribution = SparkTable("analytics_disease_distribution", [...], ...)`. Python bolu `SparkTable.__init__(objetu_foun, "analytics_disease_distribution", ...)`; `self` = objetu foun. Bainhira dashboard hakerek `disease_distribution()`, Python bolu `SparkTable.__call__(disease_distribution)`.
- **`InvalidEvent`**: harii bainhira `raise InvalidEvent("Missing required field: user_id")`. Objetu erru sa'e liuhusi chamada funsaun to'o `except` ida kaer nia (`process_and_persist` liña 213).
- **Objetu nível modulu (komposisaun)**: `_DOWNLOADS` no `_UPLOADS` (`ThreadPoolExecutor`) iha `streaming_job.py`; `_LOCK` (`threading.Lock`) iha `live_log.py`. Modulu "na'in" objetu sira ne'e: moris bainhira import, moris to'o prosesu remata.
- **Objetu temporáriu**: `ThreadPoolExecutor` iha `foreach_batch_sink` liña 235 — `with` harii no taka nia iha batch ida nia laran.

### E.3 Trasu chamada métodu entre objetu

```mermaid
flowchart TD
    A["SparkSession (kafka_session)"] -->|readStream ... load| B["DataFrame raw"]
    B -->|parse_events| C["DataFrame parsed"]
    C -->|writeStream.foreachBatch.start| D["StreamingQuery"]
    D -->|kada batch| E["foreach_batch_sink(batch_df, batch_id)"]
    E -->|pool.map| F["_process_one(event)"]
    F -->|_UPLOADS.submit| G["Future"]
    F --> H["process_and_persist"]
    H -->|raise / except| I["InvalidEvent"]
    J["dashboard"] -->|"disease_distribution()"| K["SparkTable.__call__"]
    K --> L["_read_sql → pandas.DataFrame"]
```

### E.4 Prinsípiu OOP — evidénsia husi kódigu

| Konseitu | Iha modulu Spark? | Evidénsia / esplikasaun |
|---|---|---|
| **Klase no objetu** | Sin | `SparkTable` (objetu 8), `InvalidEvent` |
| **Konstrutór `__init__`** | Sin | `SparkTable.__init__` rai parámetru 5 iha atributu instánsia |
| **`self`** | Sin | `self.name`, `self.cols` iha `SparkTable` |
| **Atributu instánsia** | Sin | `SparkTable.name/cols/order_by/rename/dates` |
| **Atributu klase** | Not implemented in the current codebase. | La iha variavel define iha `class` nia laran fora husi métodu |
| **Métodu instánsia** | Sin | `SparkTable.__call__` |
| **Métodu espesiál (dunder)** | Sin | `__init__`, `__call__` |
| **Herança** | Sin | `InvalidEvent(Exception)`; iha modulu seluk `UploadRejected(InvalidEvent)`, `DuplicateImage(InvalidEvent)` |
| **Polimorfizmu** | Sin (liuhusi herança no protokolu) | `except InvalidEvent` kaer mós sub-klase; `except Exception` kaer hotu no `isinstance(e, InvalidEvent)` hili mensajen; `SparkTable` objetu "callable" hanesan funsaun; `Column` biblioteka sobrekarga `|`, `&`, `~` |
| **Enkapsulamentu** | Parsiál | `SparkTable` subar SQL no tratamentu erru; naran ho `_` (`_process_one`, `_DOWNLOADS`, `_LOCK`) mak konvensaun "internu" — Python la blokeia asesu |
| **Abstrasaun** | Parsiál (prinsípiu dezeñu) | Funsaun `_count_if`, `_count_photos`, `_high_risk_by` subar detalle agregasaun; `process_event` subar pasu 10 iha naran ida. Klase abstrata (`abc.ABC`): Not implemented in the current codebase. |
| **Komposisaun** | Sin (nível modulu) | `streaming_job` na'in `_DOWNLOADS`, `_UPLOADS`; `live_log` na'in `_LOCK`; `Compose` torchvision kaer lista transformasaun (biblioteka) |
| **Padraun Builder** | Uza husi biblioteka | `SparkSession.builder...getOrCreate()`, `readStream...load()`, `writeStream...start()` |
| **Padraun Singleton** | Uza husi biblioteka | `getOrCreate()` fó SparkSession ida kada prosesu |
| **Callback / Strategy** | Sin | `foreachBatch(foreach_batch_sink)` — funsaun projetu haruka hanesan argumentu |
| **Thread pool (Concurrency pattern)** | Sin | `ThreadPoolExecutor` ba download, prosesa paralelu, upload |
| **Interface / Protocol formál** | Not implemented in the current codebase. | La iha `typing.Protocol` ka klase abstrata |
| **Dataclass** | Not implemented in the current codebase. | Eventu no rezultadu mak `dict` |

**Nota aprendizajen Python — atributu klase (Teoria, la'ós implementasaun):**

```python
class Ezemplu:
    kontadór = 0          # atributu klase: objetu hotu partilla
    def __init__(self):
        self.naran = "x"  # atributu instánsia: kada objetu iha rasik
```

**Nota aprendizajen Python — klase abstrata (Teoria):** `from abc import ABC, abstractmethod`; klase ho `@abstractmethod` labele harii objetu direta — sub-klase tenke implementa métodu. Projetu Spark la uza.

**Nota aprendizajen Python — dataclass (Teoria):** `@dataclass class Event: image_id: str; location: str = None` harii `__init__` automátiku. Kódigu atuál uza `dict` ba eventu tanba Spark `Row.asDict()` fó dict direta.

### E.5 Distinsaun: karakterístika Python formál vs prinsípiu dezeñu

- **Formál (Python)**: `class`, herança `(Exception)`, `__init__`, `__call__`, decorator `@lru_cache`, context manager `with`.
- **Prinsípiu dezeñu (la'ós sintaxe Python)**: separasaun responsabilidade (Kafka = eventu, Spark = preprocessing/analítika, CNN = klasifikasaun), idempotênsia (`replay_state`), abstrasaun liuhusi funsaun ajuda. Prinsípiu sira ne'e iha, maibé sira la presiza klase.

## F. Integrasaun ho Komponente Seluk

| Komponente | Direksaun | Saida mak troka | Kódigu |
|---|---|---|---|
| Kafka (Modulu 02) | Kafka → Spark | Mensajen JSON (`kind`, `image_id`, `image_path`, ...) | `run()` `readStream.format("kafka")`; `parse_events` |
| `storage/uploads.py` | Spark → BD | Rejistu upload Drive foun | `_process_one` → `register_upload` |
| `storage/results.py` | Spark ↔ BD | Kontrola replay, status PROCESSING, rai COMPLETED/FAILED | `replay_state`, `mark_processing`, `persist_result` |
| Google Drive (Modulu 03) | Drive → Spark | Bytes fotografia | `load_image_bytes` iha `_DOWNLOADS` |
| Google Drive (Modulu 03) | Spark → Drive | JSON rezultadu + imajen ho heatmap | `_upload_result` → `publish_result_to_drive_safely` |
| PostgreSQL (Modulu 04) | Spark (batch) ↔ BD | JDBC read 4 tabela; JDBC append ba `_staging`; transasaun troka | `read_inputs`, `write_tables` |
| CNN (Modulu 05) | Spark → CNN → Spark | Tensor `[3,S,S]` → dict feature | `model_input` → `run_model` |
| `ml/risk.py` (Modulu 05) | Spark → risku | `lesion_score` → `(score, level)` | `compute_risk` |
| Dashboard | BD → dashboard | Tabela `analytics_*` | `analytics_reader.SparkTable` |
| `run_all.py` (Modulu 06) | hahú prosesu | `python -m spark.streaming_job`, `python -m spark.batch_analytics --watch` | — |

**Fluxu dadus ida-ne'ebé kompletu** (haree C.4): Drive watcher → Kafka → `parse_events` → `foreach_batch_sink` → `_process_one` → `process_event` (Drive download, EXIF, `prepare_image`, `model_input`, CNN, risku) → `persist_result` (PostgreSQL) → `_upload_result` (Drive) → `batch_analytics.watch` hetan mudansa → `write_tables` → dashboard.

## G. Tratamentu Erru no Konfiabilidade

| Mekanizmu | Oinsá funsiona | Kódigu |
|---|---|---|
| Validasaun eskema | `from_json` ho `EVENT_SCHEMA`; JSON aat fó null, la monu | `parse_events` liña 87 |
| Validasaun regra Spark SQL | `rejected_reason` ba kampu falta, `owner_email`/`user_id`, `captured_at` futuru | `parse_events` liña 95–104 |
| Validasaun Python | Kampu, tempu ISO, futuru, `user_id` iha BD, dalan seguru, etiketa Drive | `clean_event`, `_clean_drive_event` |
| Eventu aat la lakon | Rai `FAILED` ho razaun; `raw_value` ba eventu la iha `image_id` | `process_and_persist`, `_process_one` liña 268–269 |
| At-least-once + idempotênsia | Checkpoint Spark; `replay_state` salta COMPLETED; `persist_result` troka prediksaun | `run()` liña 351; `_process_one` liña 283–288 |
| Duplikadu iha batch | `dropDuplicates(["image_id"])` | `foreach_batch_sink` liña 228 |
| Duplikadu konteúdu | `check_duplicate_by_hash` | `process_event` liña 156 |
| Commit depois upload | Hein `Future` upload hotu molok remata batch | `foreach_batch_sink` liña 238 |
| Re-send automátiku | Thread kada 300 s haruka fali fotografia ne'ebé hein > 600 s (maximu 50) | `_auto_resend` |
| Erru BD bainhira rai FAILED | Kaer; fotografia hela PROCESSING; `requeue_stuck` koko fali | `process_and_persist` liña 218–219 |
| Erru desenhu imajen | Kaer; diagnóstiku kontinua | `process_event` liña 197–198 |
| Erru rai kópia disku | Kaer; print de'it | `save_*`, `store_processed_image` |
| Analítika falla | Kaer; koko fali kada 30 s | `watch` liña 287–288 |
| Dashboard la iha tabela | DataFrame mamuk | `SparkTable.__call__` liña 33–34 |
| Leitór lee tabela mamuk | Staging + transasaun | `write_tables` |
| Log la monu | `UnicodeEncodeError`, `OSError` ignora | `say` |

**Limitasaun (Implementadu / Inferidu):**

- Streaming job la uza **Spark JDBC** ba rezultadu; nia uza SQLAlchemy kada fotografia. Spark JDBC write streaming: Not implemented in the current codebase.
- Prosesa fotografia akontese iha **driver** Spark (Python, `collect()` + thread), la'ós distribuidu iha executor (komentáriu liña 224: worker Python monu iha Windows). Ne'e limita eskala ba komputadór ida.
- *Exactly-once*: Not implemented in the current codebase — sistema uza at-least-once + idempotênsia.
- Dead-letter topic Kafka separadu: Not implemented in the current codebase — eventu rejeitadu rai iha tabela (`rejected_events`, `FAILED`).
- `analytics_disease_distribution`/`most_detected` konta mós klase `"uncertain"` hanesan klase (`most_detected` só hasai `"healthy"`).
- `watch` uza *polling* (30 s), la'ós notifikasaun BD.

## H. Sumáriu no Pergunta Akadémiku

### H.1 Sumáriu

Modulu Spark iha parte rua. **Streaming** (`streaming_job.py` + `preprocessing.py`): Spark Structured Streaming lee Kafka kada segundu, parse JSON ho `EVENT_SCHEMA`, valida ho Spark SQL (`rejected_reason`), depois iha `foreachBatch` prosesa fotografia to'o 8 iha paralelu — validasaun Python, download, EXIF, `prepare_image` (vertikál, ≤1280 px, la ho EXIF), `model_input` (Resize → CenterCrop → ToTensor → Normalize), CNN, risku, rai iha PostgreSQL (SQLAlchemy) no haruka ba Drive. Checkpoint + `replay_state` fó at-least-once seguru. **Analítika** (`batch_analytics.py`): Spark lee PostgreSQL ho JDBC, kalkula prediksaun foun liu ho Window, agregasaun `groupBy/agg` ba tabela 10, no hakerek ho JDBC `append` ba staging + transasaun troka. Dashboard lee ho `SparkTable`.

### H.2 Pergunta no resposta (nível profesór)

**P1. Spark hakerek rezultadu CNN ba PostgreSQL ho JDBC?**
R: Lae. Streaming job rai rezultadu ho SQLAlchemy (`persist_result`, `storage/results.py`) iha `foreach_batch_sink`. Spark JDBC uza de'it iha `batch_analytics.py`: `spark.read.jdbc` (lee) no `df.write.mode("append").jdbc(..._staging)` (hakerek).

**P2. Oinsá sistema garante katak eventu ne'ebé repete depois crash la halo prediksaun rua?**
R: (1) `dropDuplicates(["image_id"])` iha batch; (2) `replay_state` fó `"completed"` → `_process_one` salta; (3) se COMPLETED maibé rezultadu seidauk iha Drive (`"result_missing"`), prosesa fali no `persist_result` troka prediksaun tuan.

**P3. Tanba sá `foreach_batch_sink` hein Future upload hotu?**
R: Spark commit checkpoint bainhira funsaun remata. Hein garante katak se prosesu monu molok upload, batch sei repete (komentáriu liña 237).

**P4. Eventu JSON aat (la'ós JSON) akontese saida?**
R: `from_json` fó null ba kampu hotu → `rejected_reason = "Missing required field: image_id"` → `_process_one` rai `raw_value` → `process_and_persist` hamosu `InvalidEvent` → `persist_result(..., FAILED)`.

**P5. Formula normalizasaun iha `model_input`?**
R: Ba kada kanál $c$: $x'_c = \dfrac{x_c - \mu_c}{\sigma_c}$, ne'ebé $x_c \in [0,1]$ husi `ToTensor` ($x = \text{piksel}/255$), $\mu_c$ = `spec["mean"][c]`, $\sigma_c$ = `spec["std"][c]` husi checkpoint modelu.

**P6. Tanba sá `prepare_image` uza `draft` molok `thumbnail`?**
R: `draft` husu descodificadór JPEG atu descodifika iha rezolusaun ki'ik (lalais no poupa memória); `thumbnail` depois garante sisi boot ≤ `ANALYSIS_MAX_PX`.

**P7. `startingOffsets = "earliest"` la halo fotografia hotu prosesa fali bainhira restart?**
R: Lae. Opsaun ne'e uza só bainhira checkpoint la iha. Ho checkpoint, Spark kontinua husi offset ikus. Maske eventu tuan mai fali, `replay_state` salta COMPLETED.

**P8. Saida mak risku "race" iha `foreach_batch_sink` ho thread 8?**
R: Thread barak bolu BD no Drive iha tempu hanesan. Kódigu proteje: kada thread uza `SessionLocal()` rasik; `live_log` uza `_LOCK`; duplikadu `image_id` hasai molok paralelu. `check_duplicate_by_hash` uza lock rasik (`_DUPLICATE_LOCK` iha `storage/duplicates.py`).

**P9. Oinsá analítika garante "imajen ida konta dala ida"?**
R: `photos_with_latest_prediction` uza `row_number()` iha Window `partitionBy("image_id")` no hela `_rn == 1`, depois *left join* ho `images`.

**P10. Tanba sá `_local_day` presiza maske `spark.sql.session.timeZone = "UTC"`?**
R: Sesaun UTC halo kálkulu konsistente; `_local_day` aumenta `LOCAL_TZ_OFFSET_HOURS` esplísitu atu loron sai loron Timor-Leste, la depende ba fuso komputadór.

**P11. Se `SPARK_PARALLEL_PHOTOS = 1`, saida muda?**
R: Liña 232 loos → fotografia prosesa ida-ida iha thread prinsipál (`[_process_one(e) for e in events]`). Upload Drive kontinua iha `_UPLOADS` background.

**P12. Iha ne'ebé OOP herança loloos aparese iha modulu ne'e?**
R: `class InvalidEvent(Exception)`. Sub-klase `UploadRejected` no `DuplicateImage` (modulu `storage/`) herda husi `InvalidEvent`, entaun `except InvalidEvent` iha `_process_one` kaer sira hotu — polimorfizmu liuhusi herança.
