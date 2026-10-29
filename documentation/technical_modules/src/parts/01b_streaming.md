
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
