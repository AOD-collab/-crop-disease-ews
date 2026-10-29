
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
