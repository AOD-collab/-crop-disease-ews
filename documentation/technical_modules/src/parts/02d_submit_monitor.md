
### D.4 `storage/pipeline_submit.py` — produtór "submit"

#### D.4.1 `build_upload_event`, `UPLOAD_REJECTED_PREFIX`, `NotResubmittable`, `is_upload_rejected`

**1. Kódigu orijinál**

{{code storage/pipeline_submit.py 1-36}}

**2. Objetivu**

Harii eventu `submit` ba fotografia ne'ebé iha ona iha base de dadus (Reprocess, Retry, requeue_stuck). Eventu lori referénsia, nunka bytes. Define regra: upload ne'ebé rejeita iha rejistu labele haruka fali.

**3. Fundamentu Python**

- **Parámetru la ho type hint** (`image`) — objetu ORM `Image` (Inferidu husi atributu).
- **`.isoformat()`** — `datetime` → testu ISO.
- **Herança** `class NotResubmittable(ValueError)`.
- **`(x or "").startswith(prefix)`** — evita erru bainhira `None`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: haruka fali fotografia ne'ebé iha ona iha base de dadus (Reprocess / Retry, requeue_stuck): eventu `"submit"` ba Kafka, liña INGESTED.
- **Liña 6** — `ProcessingStatus`, `now_utc`.
- **Liña 7** — `is_drive_path`, `drive_file_id_from_path`.
- **Liña 8** — `KAFKA_TOPIC`.
- **Liña 9** — `get_backend`.
- **Liña 12** — `def build_upload_event(image) -> dict:`
- **Liña 13** — Docstring: eventu Kafka ba liña `Image` (referénsia, nunka bytes).
- **Liña 14** — `return {`
- **Liña 15** — `"kind": "submit",`
- **Liña 16** — `"image_id": image.image_id,`
- **Liña 17** — `"user_id": image.user_id,` — iha ona (diferente husi `drive_upload`).
- **Liña 18** — `"location": image.location,`
- **Liña 19** — `"image_path": image.image_path,`
- **Liña 20** — `"drive_file_id": drive_file_id_from_path(image.image_path) if is_drive_path(image.image_path) else None,`
- **Liña 21** — `"captured_at": image.captured_at.isoformat() if image.captured_at else None,`
- **Liña 22** — `"uploaded_at": image.uploaded_at.isoformat() if image.uploaded_at else None,`
- **Liña 23** — `}`.
- **Liña 26** — Komentáriu: prefiksu `error_message` ba upload rejeita iha rejistu.
- **Liña 27** — `UPLOAD_REJECTED_PREFIX = "Upload rejected: "`.
- **Liña 30** — `class NotResubmittable(ValueError):`
- **Liña 31** — Docstring: upload rejeitadu; labele analiza.
- **Liña 34** — `def is_upload_rejected(image) -> bool:`
- **Liña 35–36** — `return (image.status == ProcessingStatus.FAILED and (image.error_message or "").startswith(UPLOAD_REJECTED_PREFIX))`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `Image(image_id="a1", user_id="u-9", location="Aileu", image_path="gdrive:1AbC", captured_at=None, uploaded_at=datetime(2026,10,20,1,15,tzinfo=UTC))` → `{"kind": "submit", "image_id": "a1", "user_id": "u-9", "location": "Aileu", "image_path": "gdrive:1AbC", "drive_file_id": "1AbC", "captured_at": None, "uploaded_at": "2026-10-20T01:15:00+00:00"}` (kampu 8).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `image` | `storage.database.Image` (inferidu) |
| `build_upload_event` return | `dict` (kampu 8) |
| `UPLOAD_REJECTED_PREFIX` | `str` |
| `is_upload_rejected` return | `bool` |

**7. Ligasaun OOP**

Lee **atributu** objetu ORM (`image.image_id`...). `NotResubmittable` **herda** `ValueError` — erru ho naran klaru ba dashboard.

**8. Impaktu integrasaun**

Payload `submit` iha kampu menus duke `drive_upload`; Spark `parse_events` obriga `user_id` ba eventu la'ós `drive_upload` (Modulu 01 D.2.4).

**9. Checkpoint ba principiante**

**P:** Tanba sá eventu `submit` la iha `owner_email`?
**R:** Fotografia rejista ona; `user_id` iha kedas. Spark la presiza buka utilizadór husi email.

#### D.4.2 `submit_image_for_processing` no `submit_all`

**1. Kódigu orijinál**

{{code storage/pipeline_submit.py 39-70}}

**2. Objetivu**

Marka imajen `INGESTED` (commit uluk), depois publika eventu. Se publika falla, marka `FAILED` no hamosu fali erru. Rejeita upload ne'ebé rejeita ona. `submit_all` haruka barak; erru ida só log.

**3. Fundamentu Python**

- **`backend = backend or get_backend(...)`** — injesaun dependénsia opsionál (teste bele haruka backend falsu).
- **`raise` mesak** re-raise depois rai status.

**4. Esplikasaun liña-ba-liña**

- **Liña 39** — `def submit_image_for_processing(session, image, backend=None) -> None:`
- **Liña 40–41** — Docstring: marka INGESTED, depois publika; erru publika → FAILED no re-raise; rejeita upload rejeitadu.
- **Liña 42** — `if is_upload_rejected(image):`
- **Liña 43** — `raise NotResubmittable(f"{image.error_message} — this upload can't be processed")`.
- **Liña 44** — `backend = backend or get_backend(KAFKA_TOPIC)`.
- **Liña 45** — Komentáriu: commit INGESTED molok publika, atu rezultadu Spark labele troka.
- **Liña 46** — `if image.status == ProcessingStatus.FAILED:`
- **Liña 47** — `image.status = ProcessingStatus.INGESTED`.
- **Liña 48** — `image.error_message = None`.
- **Liña 49** — `image.ingested_at = now_utc()`.
- **Liña 50** — `session.commit()`.
- **Liña 51** — `try:`
- **Liña 52** — `backend.send(build_upload_event(image))` — **publika**.
- **Liña 53** — `except Exception as e:`
- **Liña 54** — `image.status = ProcessingStatus.FAILED`.
- **Liña 55** — `image.error_message = f"Kafka publish failed: {e}"`.
- **Liña 56** — `session.commit()`.
- **Liña 57** — `raise`.
- **Liña 60** — `def submit_all(session, images, backend=None, log_prefix="[pipeline_submit]") -> int:`
- **Liña 61** — Docstring: haruka kada imajen; falla só log; fó kuantu haruka.
- **Liña 62** — `sent = 0`.
- **Liña 63** — `for image in images:`
- **Liña 64** — `try:`
- **Liña 65** — `backend = backend or get_backend(KAFKA_TOPIC)` — harii dala ida.
- **Liña 66** — `submit_image_for_processing(session, image, backend)`.
- **Liña 67** — `sent += 1`.
- **Liña 68** — `except Exception as e:` — komentáriu: rai iha liña; seluk kontinua.
- **Liña 69** — `print(f"{log_prefix} Could not re-send {image.image_id}: {e}")`.
- **Liña 70** — `return sent`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* admin klik "Retry" ba imajen `FAILED` → status `INGESTED`, `error_message = None`, commit → `ResilientBackend.send` → Kafka. Se `KafkaBackend` (fallback hamate) no broker mate → `FAILED`, `"Kafka publish failed: ..."`, erru sa'e ba dashboard.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception |
|---|---|---|---|
| `submit_image_for_processing` | `session` (SQLAlchemy `Session`), `image` (`Image`), `backend` | `None` | `NotResubmittable`, erru backend |
| `submit_all` | `session`, `images` (iterável), `backend`, `log_prefix: str` | `int` | la iha |

**7. Ligasaun OOP**

Muda atributu objetu ORM (`image.status = ...`) no `session.commit()` hakerek. `backend` — duck typing (`send`). **Injesaun dependénsia** (prinsípiu dezeñu): `backend` bele haruka husi liur.

**8. Impaktu integrasaun**

Bolu husi dashboard (Reprocess/Retry/Process, Modulu 06) no `storage/requeue_stuck.py` (`resend_owed` iha `_auto_resend` Spark, Modulu 01).

**9. Checkpoint ba principiante**

**P:** Tanba sá commit `INGESTED` **molok** publika?
**R:** Se publika uluk, Spark bele prosesa lalais no hakerek `COMPLETED`; depois commit `INGESTED` sei troka rezultadu Spark. Commit uluk evita ida ne'e (komentáriu liña 45).

### D.5 `kafka/monitor.py` — konsumidór só-lee

#### D.5.1 Docstring, import, `GROUP_ID` no `_say`

**1. Kódigu orijinál**

{{code kafka/monitor.py 1-24}}

**2. Objetivu**

Janela EWS-KafkaMonitor: liña ida kada eventu fotografia agrikultór, no sumáriu kada segundu ba topic teste eskalabilidade. Só-lee: nunka commit offset.

**3. Fundamentu Python**

`Counter` (A.3.8). `print(..., flush=True)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: janela EWS-KafkaMonitor; liña ida kada eventu fotografia; sumáriu kada segundu ba topic teste. Só-lee: nunka commit offset.
- **Liña 5** — Uza: `python -m kafka.monitor [--from-start] [--live-only]`.
- **Liña 6** — Remata docstring.
- **Liña 8** — `import argparse`.
- **Liña 9** — `import json`.
- **Liña 10** — `import sys`.
- **Liña 11** — `import time`.
- **Liña 12** — `from collections import Counter`.
- **Liña 13** — `from datetime import datetime`.
- **Liña 15** — Konfigurasaun teste: `SCALABILITY_KAFKA_TOPIC` (default `"maize-disease-images-scalability-test"`), `SCALABILITY_CONSUMER_GROUP` (`"scalability-test"`), `SCALABILITY_ENGINE` (`"spark"`).
- **Liña 16** — `format_local` — tempu Timor ("28/09/2026 14:05 TLT").
- **Liña 17** — `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `broker_reachable`.
- **Liña 19** — `GROUP_ID = "ews-kafka-monitor"` — komentáriu: Spark ka teste nunka uza.
- **Liña 20** — `TEST_SUMMARY_EVERY_S = 1.0`.
- **Liña 23** — `def _say(line: str) -> None:`
- **Liña 24** — `print(f"[kafka {datetime.now().strftime('%H:%M:%S')}] {line}", flush=True)`.

**5. Walkthrough ezekusaun**

`_say("hello")` iha 09:15:01 → `[kafka 09:15:01] hello`.

**6. Tipu dadus**

`GROUP_ID`: `str`; `TEST_SUMMARY_EVERY_S`: `float`; `_say` → `None`.

**7. Ligasaun OOP**

Not implemented in the current codebase (bloku ne'e).

**8. Impaktu integrasaun**

`run_all.py` loke janela monitor (Modulu 06). Topic hanesan Spark.

**9. Checkpoint ba principiante**

**P:** Tanba sá `GROUP_ID` diferente husi Spark importante?
**R:** Atu monitor nunka afeta pozisaun lee konsumidór seluk.

#### D.5.2 `describe` — deserializa no formata eventu

**1. Kódigu orijinál**

{{code kafka/monitor.py 27-40}}

**2. Objetivu**

Konverte bytes mensajen ida (JSON) ba liña konsola ne'ebé ema komprende.

**3. Fundamentu Python**

`json.loads(bytes)` aseita bytes UTF-8 direta. `len(value or b'')` — tamañu, `b''` se `None`.

**4. Esplikasaun liña-ba-liña**

- **Liña 27** — `def describe(value: bytes, partition: int, offset: int) -> str:`
- **Liña 28** — Docstring: liña konsola ba rekord Kafka ida.
- **Liña 29** — `try:`
- **Liña 30** — `event = json.loads(value)` — **deserializasaun**.
- **Liña 31** — `except (TypeError, ValueError):` — `None` ka la'ós JSON.
- **Liña 32** — `return f"p{partition} offset {offset}: unreadable message ({len(value or b'')} bytes)"`.
- **Liña 33** — `from spark.live_log import farmer_label`.
- **Liña 34** — `uploaded = format_local(event.get("uploaded_at")) or event.get("uploaded_at") or ""`.
- **Liña 35** — `source = "Drive file" if event.get("drive_file_id") else "local file"`.
- **Liña 36** — `farmer = event.get("owner_email") or farmer_label(event.get("user_id"))`.
- **Liña 37** — `kind = "new photo" if event.get("kind") == "drive_upload" else "photo sent again"`.
- **Liña 38–40** — `return (...)` — "p<partition> offset <n>: <kind> <id8> from <agrikultór> (<fatin>), uploaded <tempu>, <fonte> -> waiting for Spark".

**5. Walkthrough ezekusaun**

Haree C.5 pasu 7. Bytes `b"abc"` → `"p0 offset 12: unreadable message (3 bytes)"`.

**6. Tipu dadus**

| Parámetru | Tipu |
|---|---|
| `value` | `bytes` |
| `partition`, `offset` | `int` |
| return | `str` |

**7. Ligasaun OOP**

Not implemented in the current codebase. (Funsaun.)

**8. Impaktu integrasaun**

`run` bolu ba kada mensajen topic prinsipál.

**9. Checkpoint ba principiante**

**P:** Diferensa saida entre deserializasaun iha ne'e no iha Spark?
**R:** Monitor uza `json.loads` (Python, mensajen ida); Spark uza `from_json` ho `EVENT_SCHEMA` (koluna ho tipu). Formatu JSON UTF-8 hanesan.

#### D.5.3 Klase `TestTopicSummary`

**1. Kódigu orijinál**

{{code kafka/monitor.py 43-79}}

**2. Objetivu**

Halibur eventu topic teste entre liña sumáriu rua: kuantu kada `run_id`, intervalu offset, total, offset foun liu. `line()` fó testu sumáriu no reset kontadór.

**3. Fundamentu Python**

- **Atribuisaun kadeia** `self.first = self.last = None`.
- **`min`/`max` ho kondisaun** primeira vez.
- **Type hint `str | None`**.
- **`Counter.items()`**, **`sum(...values())`**, **`.clear()`**.

**4. Esplikasaun liña-ba-liña**

- **Liña 43** — `class TestTopicSummary:`
- **Liña 44** — Docstring: halibur eventu topic eskala entre liña sumáriu rua.
- **Liña 46** — `def __init__(self):`
- **Liña 47** — `self.runs = Counter()` — komentáriu: run_id → eventu desde liña ikus.
- **Liña 48** — `self.first = self.last = None` — intervalu offset.
- **Liña 49** — `self.total = 0`.
- **Liña 50** — `self.end_offset = None` — offset foun liu (+1).
- **Liña 52** — `def add(self, value: bytes, offset: int) -> None:`
- **Liña 53** — `try:`
- **Liña 54** — `run_id = json.loads(value).get("run_id") or "?"`.
- **Liña 55** — `except (TypeError, ValueError):`
- **Liña 56** — `run_id = "unreadable"`.
- **Liña 57** — `self.runs[run_id] += 1`.
- **Liña 58** — `self.first = offset if self.first is None else min(self.first, offset)`.
- **Liña 59** — `self.last = offset if self.last is None else max(self.last, offset)`.
- **Liña 60** — `self.total += 1`.
- **Liña 61** — `self.end_offset = max(self.end_offset or 0, offset + 1)`.
- **Liña 63** — `def line(self, consumed_to=None, engine: str = SCALABILITY_ENGINE) -> str | None:`
- **Liña 64** — Docstring: sumáriu desde chamada ikus, ka `None`.
- **Liña 65** — `if not self.runs:`
- **Liña 66** — `return None`.
- **Liña 67** — `runs = ", ".join(f"{n} for {run}" for run, n in self.runs.items())`.
- **Liña 68–69** — `text = ...` — "TEST topic '<topic>': +N event(s) (offsets a-b; ...); total since start".
- **Liña 70** — `if engine == "spark":`
- **Liña 71** — `text += " — read by the scalability test's own Spark query, not the live Spark job"`.
- **Liña 72** — `else:`
- **Liña 73** — `if consumed_to is not None and self.end_offset is not None:`
- **Liña 74–75** — `text += ...` — "test worker committed up to offset X (Y behind ...)".
- **Liña 76** — `text += " — processed by the scalability test's worker, not Spark"`.
- **Liña 77** — `self.runs.clear()`.
- **Liña 78** — `self.first = self.last = None`.
- **Liña 79** — `return text`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `t = TestTopicSummary()`; `t.add(b'{"run_id":"r1"}', 100)`; `t.add(b'{"run_id":"r1"}', 101)` → `t.line()` (engine `"spark"`) = `"TEST topic 'maize-disease-images-scalability-test': +2 event(s) (offsets 100-101; 2 for r1); 2 since start — read by the scalability test's own Spark query, not the live Spark job"`. Chamada tuirmai → `None`.

**6. Tipu dadus**

| Atributu | Tipu |
|---|---|
| `runs` | `Counter[str]` |
| `first`, `last`, `end_offset` | `int ka None` |
| `total` | `int` |
| `line()` return | `str ka None` |

**7. Ligasaun OOP**

- **Klase + `__init__`** ho atributu instánsia 5.
- **Enkapsulamentu estadu**: kontadór no métodu `add`/`line` hamutuk iha objetu ida.
- **Komposisaun**: objetu na'in `Counter`.
- Objetu ida harii iha `run` (`tests = TestTopicSummary()`, liña 161).

**8. Impaktu integrasaun**

`run` bolu `tests.add` ba mensajen topic teste no `tests.line(...)` kada segundu.

**9. Checkpoint ba principiante**

**P:** Tanba sá `line()` hamamuk `runs` maibé la hamamuk `total`?
**R:** `runs` ba liña ida-idak (desde liña ikus); `total` mak kontajen desde monitor hahú.

#### D.5.4 `test_consumer_position` no klase `Assigner`

**1. Kódigu orijinál**

{{code kafka/monitor.py 82-127}}

**2. Objetivu**

`test_consumer_position` lee offset ne'ebé consumer group teste commit ona. `Assigner` lee topic ho **assign direta** partition (la iha group, la iha rebalance); topic ne'ebé harii depois lee husi inísiu.

**3. Fundamentu Python**

- **Set ho tuple** `(topic, partition)`.
- **`any(...)`** — loos se elementu ida loos.
- **Atributu bool** `first_check`.

**4. Esplikasaun liña-ba-liña**

- **Liña 82** — `def test_consumer_position(admin) -> int | None:`
- **Liña 83** — Docstring: offset commit consumer teste (soma), ka `None`.
- **Liña 84** — `try:`
- **Liña 85** — `from confluent_kafka import ConsumerGroupTopicPartitions`.
- **Liña 86** — `futures = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(SCALABILITY_CONSUMER_GROUP)])`.
- **Liña 87** — `result = futures[SCALABILITY_CONSUMER_GROUP].result(timeout=2)`.
- **Liña 88–89** — `offsets = [tp.offset for tp in result.topic_partitions if tp.topic == SCALABILITY_KAFKA_TOPIC and tp.offset >= 0]`.
- **Liña 90** — `return sum(offsets) if offsets else None` — soma partition hotu.
- **Liña 91** — `except Exception:` — komentáriu: só ba hatudu.
- **Liña 92** — `return None`.
- **Liña 95** — `class Assigner:`
- **Liña 96–97** — Docstring: lee topic ho assign direta (la iha group, la iha rebalance); topic harii depois lee husi inísiu.
- **Liña 99** — `def __init__(self, consumer, topics, from_start: bool):`
- **Liña 100** — `self.consumer, self.topics, self.from_start = consumer, topics, from_start`.
- **Liña 101** — `self.assigned = set()`.
- **Liña 102** — `self.first_check = True`.
- **Liña 104** — `def refresh(self) -> list:`
- **Liña 105** — Docstring: assign partition foun desde chamada ikus; fó sira.
- **Liña 106** — `from confluent_kafka import TopicPartition, OFFSET_BEGINNING, OFFSET_END`.
- **Liña 107** — `try:`
- **Liña 108** — `meta = self.consumer.list_topics(timeout=2)`.
- **Liña 109** — `except Exception:` — komentáriu: koko fali tempu seluk.
- **Liña 110** — `return []`.
- **Liña 111** — `new = []`.
- **Liña 112** — `for topic in self.topics:`
- **Liña 113** — `info = meta.topics.get(topic)`.
- **Liña 114** — `if info is None or info.error is not None:` — topic seidauk iha.
- **Liña 115** — `continue`.
- **Liña 116** — `start = OFFSET_BEGINNING if (self.from_start or not self.first_check) else OFFSET_END` — dala uluk: rohan (eventu foun de'it) exetu `--from-start`; topic foun depois: inísiu.
- **Liña 117** — `for partition in info.partitions:` — **partition hotu** topic.
- **Liña 118** — `if (topic, partition) not in self.assigned:`
- **Liña 119** — `self.assigned.add((topic, partition))`.
- **Liña 120** — `new.append(TopicPartition(topic, partition, start))`.
- **Liña 121** — `self.first_check = False`.
- **Liña 122** — `if new:`
- **Liña 123** — `self.consumer.incremental_assign(new)` — assign direta (la'ós `subscribe`).
- **Liña 124** — `return new`.
- **Liña 126** — `def missing(self) -> list:`
- **Liña 127** — `return [t for t in self.topics if not any(a[0] == t for a in self.assigned)]` — topic la iha partition assign.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `topics = ["maize-disease-images", "maize-disease-images-scalability-test"]`; topic teste seidauk iha. `refresh()` dahuluk → assign `("maize-disease-images", 0)` iha `OFFSET_END`; `missing()` = `["...-scalability-test"]`. Depois teste harii topic → `refresh()` → assign partition 0 iha `OFFSET_BEGINNING`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `admin` | `confluent_kafka.admin.AdminClient` |
| `test_consumer_position` return | `int ka None` |
| `Assigner.consumer` | `confluent_kafka.Consumer` |
| `Assigner.topics` | `list[str]` |
| `Assigner.assigned` | `set[tuple[str, int]]` |
| `refresh()` return | `list[TopicPartition]` |

**7. Ligasaun OOP**

- **Komposisaun/agregasaun**: `Assigner` kaer referénsia ba objetu `Consumer` ne'ebé `run` harii (la'ós na'in — `run` mak taka nia).
- **Enkapsulamentu**: lójika assign (set `assigned`, `first_check`) iha objetu.
- **Métodu**: `refresh`, `missing`.

**8. Impaktu integrasaun**

Assign direta signifika monitor **la tama** iha consumer group ruma, entaun Spark no teste la haree rebalance.

**9. Checkpoint ba principiante**

**P:** Diferensa entre `subscribe` no `assign`?
**R:** `subscribe` tama consumer group no broker fahe partition (rebalance). `assign` konsumidór hili partition rasik — la iha group koordenasaun.

#### D.5.5 `consumer_config` no hahú `run`

**1. Kódigu orijinál**

{{code kafka/monitor.py 130-162}}

**2. Objetivu**

Konfigurasaun konsumidór (la auto-commit), no inísiu `run`: naran janela, hein broker, harii `Consumer`, `Assigner`, `AdminClient`, no hakerek mensajen hahú.

**3. Fundamentu Python**

- **`ctypes.windll.kernel32.SetConsoleTitleW`** — bolu API Windows.
- **Lista kondisionál** `[A] + ([] if x else [B])`.
- **Atribuisaun kadeia** `a = b = c = time.monotonic()`.

**4. Esplikasaun liña-ba-liña**

- **Liña 130** — `def consumer_config(from_start: bool) -> dict:`
- **Liña 131** — `return {`
- **Liña 132** — `"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,`
- **Liña 133** — `"group.id": GROUP_ID,` — confluent-kafka obriga; la uza ba commit.
- **Liña 134** — `"enable.auto.commit": False,` — **nunka commit**.
- **Liña 135** — `"auto.offset.reset": "earliest" if from_start else "latest",`
- **Liña 136** — Komentáriu: haree topic foun iha segundu balun.
- **Liña 137** — `"topic.metadata.refresh.interval.ms": 5000,`
- **Liña 138** — `}`.
- **Liña 141** — `def run(from_start: bool = False, live_only: bool = False) -> int:`
- **Liña 142** — `from confluent_kafka import Consumer, KafkaError`.
- **Liña 143** — `from confluent_kafka.admin import AdminClient`.
- **Liña 144** — `if sys.platform == "win32":`
- **Liña 145** — `import ctypes`.
- **Liña 146** — `ctypes.windll.kernel32.SetConsoleTitleW("EWS-KafkaMonitor")` — naran janela.
- **Liña 147** — `while not broker_reachable():`
- **Liña 148** — `_say(...)` — "Kafka broker not reachable ...; retrying in 5 s".
- **Liña 149** — `time.sleep(5)`.
- **Liña 150** — `topics = [KAFKA_TOPIC] + ([] if live_only else [SCALABILITY_KAFKA_TOPIC])`.
- **Liña 151** — `consumer = Consumer(consumer_config(from_start))`.
- **Liña 152** — `assigner = Assigner(consumer, topics, from_start)`.
- **Liña 153** — `admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})`.
- **Liña 154–156** — `_say(...)` — "Watching '<topic>' (farmer photos -> Spark) and '<teste>' ... Read-only: no consumer group, no offsets committed."
- **Liña 157** — `assigner.refresh()`.
- **Liña 158** — `if assigner.missing():`
- **Liña 159** — `_say(...)` — "(not created yet: ... — picked up as soon as it exists)".
- **Liña 160** — `seen = 0`.
- **Liña 161** — `tests = TestTopicSummary()`.
- **Liña 162** — `last_quiet = last_summary = last_refresh = time.monotonic()`.

**5. Walkthrough ezekusaun**

`python -m kafka.monitor` → `from_start=False` → `auto.offset.reset="latest"` → janela naran "EWS-KafkaMonitor" → se Docker mate, print kada 5 s → broker moris → assign partition → mensajen "Watching ...".

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `consumer_config` return | `dict[str, object]` |
| `topics` | `list[str]` |
| `consumer` | `confluent_kafka.Consumer` |
| `assigner` | `Assigner` |
| `admin` | `AdminClient` |
| `tests` | `TestTopicSummary` |

**7. Ligasaun OOP**

`run` harii objetu 4 (`Consumer`, `Assigner`, `AdminClient`, `TestTopicSummary`) no liga sira: `Assigner` simu `consumer` (**komposisaun ho injesaun**).

**8. Impaktu integrasaun**

Konfigurasaun `enable.auto.commit=False` + assign direta = monitor nunka muda offset Spark (Spark uza checkpoint rasik) ka teste.

**9. Checkpoint ba principiante**

**P:** Tanba sá monitor hein `broker_reachable()` molok harii `Consumer`?
**R:** Atu hatudu mensajen klaru kada 5 s duke erru librdkafka barak bainhira Docker seidauk hahú.

#### D.5.6 `run` loop, `main`

**1. Kódigu orijinál**

{{code kafka/monitor.py 163-209}}

**2. Objetivu**

Loop `poll`: refresh topic foun kada 3 s, sumáriu teste kada 1 s, mensajen "nonok" kada minutu, trata erru Kafka, no hakerek liña ba kada eventu. `main` lee argumentu no prepara konsola UTF-8.

**3. Fundamentu Python**

- **`try/except KeyboardInterrupt/finally`** — `consumer.close()` halai beibeik.
- **`stream.reconfigure(encoding="utf-8", errors="replace")`** — muda encoding konsola.
- **`for stream in (sys.stdout, sys.stderr)`**.

**4. Esplikasaun liña-ba-liña**

- **Liña 163** — `try:`
- **Liña 164** — `while True:`
- **Liña 165** — `msg = consumer.poll(0.25)` — hein to'o 0.25 s ba mensajen.
- **Liña 166** — `now = time.monotonic()`.
- **Liña 167** — `if now - last_refresh >= 3 and assigner.missing():`
- **Liña 168** — `last_refresh = now`.
- **Liña 169** — `for tp in assigner.refresh():`
- **Liña 170** — `_say(...)` — "now watching '<topic>' partition <n> (it was just created)".
- **Liña 171** — `if now - last_summary >= TEST_SUMMARY_EVERY_S:`
- **Liña 172** — `last_summary = now`.
- **Liña 173** — `if tests.runs:`
- **Liña 174** — `_say(tests.line(test_consumer_position(admin)))`.
- **Liña 175** — `if msg is None:` — la iha mensajen:
- **Liña 176** — `if now - last_quiet > 60:`
- **Liña 177** — `_say(...)` — "… no new events in the last minute (X farmer, Y test seen since start)".
- **Liña 178** — `last_quiet = now`.
- **Liña 179** — `continue`.
- **Liña 180** — `if msg.error():`
- **Liña 181** — `code = msg.error().code()`.
- **Liña 182** — `if code not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):` — erru normál ignora.
- **Liña 183** — `_say(f"Kafka error: {msg.error()}")`.
- **Liña 184** — `continue`.
- **Liña 185** — `last_quiet = now`.
- **Liña 186** — `if msg.topic() == SCALABILITY_KAFKA_TOPIC:`
- **Liña 187** — `tests.add(msg.value(), msg.offset())`.
- **Liña 188** — `continue`.
- **Liña 189** — `seen += 1`.
- **Liña 190** — `_say(describe(msg.value(), msg.partition(), msg.offset()))`.
- **Liña 191** — `except KeyboardInterrupt:`
- **Liña 192** — `return 0`.
- **Liña 193** — `finally:`
- **Liña 194** — `consumer.close()`.
- **Liña 197** — `def main() -> int:`
- **Liña 198** — `p = argparse.ArgumentParser(...)`.
- **Liña 199** — `--from-start` — hatudu mós eventu iha topic ona.
- **Liña 200** — `--live-only` — topic tempu-real de'it.
- **Liña 201** — `args = p.parse_args()`.
- **Liña 202** — `for stream in (sys.stdout, sys.stderr):` — komentáriu: karakter "—" iha konsola ka log.
- **Liña 203** — `if hasattr(stream, "reconfigure"):`
- **Liña 204** — `stream.reconfigure(encoding="utf-8", errors="replace")`.
- **Liña 205** — `return run(args.from_start, args.live_only)`.
- **Liña 208** — `if __name__ == "__main__":`
- **Liña 209** — `sys.exit(main())`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* mensajen offset 57 topic prinsipál → `seen = 1` → liña C.5 pasu 7. Minutu ida la iha mensajen → "… no new events in the last minute (1 farmer, 0 test seen since start)".

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `msg` | `confluent_kafka.Message ka None` |
| `code` | `int` |
| `seen` | `int` |
| `run`, `main` return | `int` |

**7. Ligasaun OOP**

Métodu objetu `Message`: `error()`, `topic()`, `value()`, `partition()`, `offset()`. `hasattr(stream, "reconfigure")` — duck typing.

**8. Impaktu integrasaun**

Monitor la afeta pipeline; nia ajuda ema haree eventu to'o Kafka molok Spark prosesa (komplementu ba konsola EWS-Spark, Modulu 01 D.5).

**9. Checkpoint ba principiante**

**P:** Tanba sá `consumer.close()` iha `finally`?
**R:** Atu konesaun Kafka taka di'ak maske ema hamate ho Ctrl+C ka erru.

### D.6 `docker-compose.yml` — broker Kafka

#### D.6.1 Komentáriu uzu

**1. Kódigu orijinál**

{{code docker-compose.yml 1-17}}

**2. Objetivu**

Esplika arkivu: infraestrutura lokál ba demo — Kafka no PostgreSQL — no oinsá uza.

**3. Fundamentu Python**

La'ós Python. YAML: `#` = komentáriu. Arkivu YAML deskreve konfigurasaun ho indentasaun (hanesan Python).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Infraestrutura lokál: Kafka (`run_all.py` bolu `docker compose up -d` no hein broker iha `localhost:9092` — uluk arkivu ne'e la iha, entaun pasu ne'e falla nonok) no PostgreSQL (manuál DIT 5.3).
- **Liña 5** — Liña mamuk komentáriu.
- **Liña 6–8** — SQLite + fila Kafka lokál mak default ba dezenvolvimentu; arkivu compose ne'e presiza ba demo ho infraestrutura loloos.
- **Liña 9** — Liña mamuk komentáriu.
- **Liña 10–12** — Uza: `docker compose up -d`, depois `python -m storage.init_db`.
- **Liña 13** — Liña mamuk komentáriu.
- **Liña 14–15** — Hatudu aplikasaun ba Postgres no hamate fallback Kafka atu broker mate sai "loud".
- **Liña 16** — Ezemplu `DATABASE_URL` (password troka ho `***` iha dokumentu ne'e).
- **Liña 17** — `KAFKA_USE_LOCAL_FALLBACK=false`.

**5. Walkthrough ezekusaun**

`docker compose up -d` → hahú container `crop-disease-ews-kafka` no Postgres.

**6. Tipu dadus**

Komentáriu de'it.

**7. Ligasaun OOP**

Not implemented in the current codebase. (Konfigurasaun.)

**8. Impaktu integrasaun**

`run_all.py` (Modulu 06) bolu `docker compose up -d`.

**9. Checkpoint ba principiante**

**P:** Tanba sá rekomenda `KAFKA_USE_LOCAL_FALLBACK=false` ba demo?
**R:** Atu Kafka mate hatudu erru kedas; ho fallback, eventu hela iha fila lokál no Spark la simu buat ida (nonok).

#### D.6.2 Serbisu `kafka`

**1. Kódigu orijinál**

{{code docker-compose.yml 18-50}}

**2. Objetivu**

Define container Kafka 3.7.0 ho modu KRaft nó ida (la iha Zookeeper), port só iha localhost, no volume atu dadus topic no offset la lakon.

**3. Fundamentu Python**

La'ós Python. YAML: `chave: valór`; lista ho `-`.

**4. Esplikasaun liña-ba-liña**

- **Liña 18** — `services:`.
- **Liña 19** — `kafka:` — naran serbisu.
- **Liña 20** — `image: apache/kafka:3.7.0`.
- **Liña 21** — `container_name: crop-disease-ews-kafka`.
- **Liña 22** — `restart: unless-stopped` — hahú fali automátiku.
- **Liña 23** — `ports:`.
- **Liña 24–29** — Komentáriu: liga ba localhost de'it, hanesan PostgreSQL — Spark rejista fotografia foun ba konta Google ne'ebé eventu temi, entaun ema seluk iha rede hanesan labele publika ba topic. Endereso loopback rua: `localhost` rezolve ba `::1` uluk iha Windows; ho IPv4 de'it cliente lakon ~2 s kada konesaun.
- **Liña 30** — `"127.0.0.1:9092:9092"` — IPv4.
- **Liña 31** — `"[::1]:9092:9092"` — IPv6.
- **Liña 32** — `environment:`.
- **Liña 33** — Komentáriu: KRaft nó ida (la presiza container Zookeeper).
- **Liña 34** — `KAFKA_NODE_ID: 1`.
- **Liña 35** — `KAFKA_PROCESS_ROLES: broker,controller` — nó ida halo papél rua.
- **Liña 36** — `KAFKA_LISTENERS` — `PLAINTEXT://:29092` (interior Docker), `CONTROLLER://:29093`, `PLAINTEXT_HOST://:9092` (husi host).
- **Liña 37–38** — Komentáriu: `PLAINTEXT_HOST` anunsia `localhost:9092`, buat ne'ebé `KAFKA_BOOTSTRAP_SERVERS` hein.
- **Liña 39** — `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092`.
- **Liña 40** — `KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER`.
- **Liña 41** — `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` — listener hotu `PLAINTEXT` (la iha TLS/SASL).
- **Liña 42** — `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093`.
- **Liña 43** — `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` — topic offset interna ho kópia ida.
- **Liña 44** — `CLUSTER_ID` — identifikadór cluster KRaft (la'ós segredu).
- **Liña 45–46** — Komentáriu: log Kafka iha volume ho naran atu dadus topic no offset konsumidór moris liu `docker compose down`.
- **Liña 47** — `KAFKA_LOG_DIRS: /var/lib/kafka/data`.
- **Liña 48** — `user: root` — permisaun hakerek volume.
- **Liña 49** — `volumes:`.
- **Liña 50** — `- kafka_data:/var/lib/kafka/data`.

**5. Walkthrough ezekusaun**

Produtór iha laptop konekta `localhost:9092` → broker fó metadata ho listener anunsia `localhost:9092` → produce. Container seluk (se iha) uza `kafka:29092`.

**6. Tipu dadus**

YAML: string, númeru, lista.

**7. Ligasaun OOP**

Not implemented in the current codebase. (Konfigurasaun.)

**8. Impaktu integrasaun**

Endereso `localhost:9092` hanesan default `KAFKA_BOOTSTRAP_SERVERS`. Volume `kafka_data` garante katak mensajen ne'ebé seidauk prosesa moris liu restart. Konfigurasaun partition la iha (default broker).

**9. Checkpoint ba principiante**

**P:** Tanba sá port liga ba `127.0.0.1` no la'ós `0.0.0.0`?
**R:** Seguransa: komputadór seluk iha rede labele publika eventu falsu ba topic (Spark konfia `owner_email` iha eventu).
