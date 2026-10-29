
## D. Esplikasaun Kódigu-ba-Kódigu

### D.1 `kafka/config.py`

#### D.1.1 Re-esporta konfigurasaun

**1. Kódigu orijinál**

{{code kafka/config.py 1-8}}

**2. Objetivu**

Fó fatin ida ba konfigurasaun Kafka: modulu seluk import husi `kafka.config` (la'ós direta husi `config.settings`).

**3. Fundamentu Python**

- **Re-export**: import naran no la uza iha arkivu, atu modulu seluk bele `from kafka.config import KAFKA_TOPIC`.
- **`# noqa: F401`**: husu linter atu la hatete "import la uza".

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — Docstring: konfigurasaun Kafka, husi `config.settings`.
- **Liña 3** — `from config.settings import (` — komentáriu: re-esporta.
- **Liña 4** — `KAFKA_BOOTSTRAP_SERVERS` — enderesu broker (default `"localhost:9092"`).
- **Liña 5** — `KAFKA_TOPIC` — topic (default `"maize-disease-images"`).
- **Liña 6** — `KAFKA_USE_LOCAL_FALLBACK` — uza fila lokál bainhira Kafka mate (default `True`).
- **Liña 7** — `LOCAL_QUEUE_DIR` — pasta fila lokál.
- **Liña 8** — `)`.

**5. Walkthrough ezekusaun**

`from kafka.config import KAFKA_TOPIC` → `"maize-disease-images"` (se variavel ambiente `KAFKA_TOPIC` la define).

**6. Tipu dadus**

`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`: `str`; `KAFKA_USE_LOCAL_FALLBACK`: `bool`; `LOCAL_QUEUE_DIR`: `Path`.

**7. Ligasaun OOP**

Not implemented in the current codebase. (Konstante modulu de'it.)

**8. Impaktu integrasaun**

Import husi `producer.py`, `monitor.py`, `pipeline_submit.py`, `spark/streaming_job.py`.

**9. Checkpoint ba principiante**

**P:** Tanba sá la import direta husi `config.settings`?
**R:** Atu modulu Kafka iha "pontu tama" ida; se fatin konfigurasaun muda, só arkivu ne'e mak muda.

#### D.1.2 `bootstrap_address` no `broker_reachable`

**1. Kódigu orijinál**

{{code kafka/config.py 11-28}}

**2. Objetivu**

Hetan host no port husi broker dahuluk iha `KAFKA_BOOTSTRAP_SERVERS`, no kontrola lalais (TCP) se broker moris.

**3. Fundamentu Python**

- **`split(",")[0]`** — elementu dahuluk; **`split("://", 1)[-1]`** — fahe dala ida, foti parte ikus.
- **`rpartition(":")`** — fahe iha `:` ikus (funsiona ba IPv6 `[::1]:9092`).
- **`_`** — variavel ne'ebé ita la uza.
- **`with socket.create_connection(...)`** — taka konesaun automátiku.

**4. Esplikasaun liña-ba-liña**

- **Liña 11** — `def bootstrap_address() -> tuple:`
- **Liña 12** — Docstring: (host, port) broker dahuluk.
- **Liña 13** — `first = KAFKA_BOOTSTRAP_SERVERS.split(",")[0].strip()` — broker dahuluk, la ho espasu.
- **Liña 14** — `first = first.split("://", 1)[-1]` — hasai prefiksu protokolu (komentáriu `PLAINTEXT://host:port`).
- **Liña 15** — `host, _, port = first.rpartition(":")`.
- **Liña 16** — `if not host:` — komentáriu: la iha port.
- **Liña 17** — `return first or "localhost", 9092` — port default 9092.
- **Liña 18** — `return host.strip("[]"), int(port)` — hasai `[]` husi IPv6; port `int`.
- **Liña 21** — `def broker_reachable(timeout: float = 1.0) -> bool:`
- **Liña 22** — `import socket`.
- **Liña 23** — `host, port = bootstrap_address()`.
- **Liña 24** — `try:`
- **Liña 25** — `with socket.create_connection((host, port), timeout=timeout):` — koko konesaun TCP.
- **Liña 26** — `return True`.
- **Liña 27** — `except OSError:` — konesaun falla/timeout.
- **Liña 28** — `return False`.

**5. Walkthrough ezekusaun**

| `KAFKA_BOOTSTRAP_SERVERS` | `bootstrap_address()` |
|---|---|
| `"localhost:9092"` | `("localhost", 9092)` |
| `"PLAINTEXT://a:9093,b:9094"` | `("a", 9093)` |
| `"[::1]:9092"` | `("::1", 9092)` |
| `"kafka"` | `("kafka", 9092)` |

`broker_reachable()` ho Docker mate → `False` iha ~1 s.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception |
|---|---|---|---|
| `bootstrap_address` | — | `tuple[str, int]` | `ValueError` se port la'ós númeru |
| `broker_reachable` | `timeout: float` | `bool` | la iha (OSError kaer) |

**7. Ligasaun OOP**

Objetu `socket` (husi `create_connection`) mak context manager. Not implemented in the current codebase: klase konfigurasaun.

**8. Impaktu integrasaun**

`monitor.run()` liña 147 hein to'o `broker_reachable()` loos. Mós uza husi `dashboard/app.py` liña 389–390 (status Kafka iha dashboard) no `dashboard/scalability_page.py` liña 148.

**9. Checkpoint ba principiante**

**P:** Tanba sá uza `rpartition` no la'ós `partition`?
**R:** Enderesu IPv6 iha `:` barak (`[::1]:9092`). `rpartition` fahe iha `:` **ikus**, ne'ebé sempre antes port.

### D.2 `kafka/producer.py` — serializasaun no haruka

#### D.2.1 Docstring, import no `_APPEND_LOCK`

**1. Kódigu orijinál**

{{code kafka/producer.py 1-18}}

**2. Objetivu**

Esplika modulu (haruka eventu ba Kafka; fila arkivu lokál bainhira broker mate), import, harii pasta fila, no lock thread ba hakerek arkivu.

**3. Fundamentu Python**

Kódigu nível modulu (`LOCAL_QUEUE_DIR.mkdir(...)`) halai bainhira import. `threading.Lock()` haree Modulu 01 A.3.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–4** — Docstring: haruka eventu upload ba Kafka (confluent-kafka; kafka-python sei kahur ho pakote `kafka/` projetu). Bainhira broker mate, eventu hein iha fila arkivu lokál no haruka bainhira nia fila.
- **Liña 5** — Remata docstring.
- **Liña 7** — `import json` — serializasaun.
- **Liña 8** — `import os` — `os.name`, `os.replace`, `os.utime`.
- **Liña 9** — `import threading`.
- **Liña 10** — `import time` — `monotonic`, `time`.
- **Liña 11** — `import uuid` — naran arkivu úniku.
- **Liña 13** — Konfigurasaun husi `kafka.config`.
- **Liña 15** — `LOCAL_QUEUE_DIR.mkdir(parents=True, exist_ok=True)` — pasta fila iha.
- **Liña 17** — Komentáriu: append ida de'it iha tempu ida iha prosesu ne'e (thread barak bele haruka).
- **Liña 18** — `_APPEND_LOCK = threading.Lock()`.

**5. Walkthrough ezekusaun**

Import dahuluk → harii `storage/local_queue/` se seidauk iha.

**6. Tipu dadus**

`_APPEND_LOCK`: `threading.Lock`.

**7. Ligasaun OOP**

Objetu lock nível modulu (komposisaun modulu).

**8. Impaktu integrasaun**

Watcher no `pipeline_submit` import `get_backend` husi modulu ne'e.

**9. Checkpoint ba principiante**

**P:** Tanba sá projetu la uza pakote `kafka-python`?
**R:** Pakote ne'e naran `kafka` — hanesan ho pasta projetu `kafka/`. `import kafka` sei kahur; `confluent_kafka` iha naran seluk.

#### D.2.2 `_append` no `_parse_lines`

**1. Kódigu orijinál**

{{code kafka/producer.py 21-48}}

**2. Objetivu**

`_append` hakerek testu iha rohan arkivu fila ho lock thread **no** lock sistema operativu (prosesu rua labele kahur liña). `_parse_lines` lee liña JSON; fahe ba mensajen válidu no liña aat.

**3. Fundamentu Python**

- **`with A, B:`** — context manager rua iha `with` ida.
- **`if os.name == "nt": ... else: ...`** — kódigu diferente ba Windows/Linux.
- **`f.seek(0)`** — muda pozisaun arkivu ba inísiu.
- **Return tuple** `return messages, bad`.

**4. Esplikasaun liña-ba-liña**

- **Liña 21** — `def _append(path, text: str) -> None:`
- **Liña 22** — Docstring: append ho lock OS, atu prosesu rua la kahur liña.
- **Liña 23** — `with _APPEND_LOCK, open(path, "a") as f:` — lock thread + loke arkivu append.
- **Liña 24** — `if os.name == "nt":` — Windows:
- **Liña 25** — `import msvcrt`.
- **Liña 26** — `f.seek(0)` — lock byte dahuluk (fatin hanesan ba prosesu hotu).
- **Liña 27** — `msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)` — lock byte 1; komentáriu: hein to'o ~10 s, depois hamosu erru.
- **Liña 28** — `f.write(text)` — modu `"a"` hakerek sempre iha rohan.
- **Liña 29** — `f.flush()` — haruka ba disku.
- **Liña 30** — `f.seek(0)`.
- **Liña 31** — `msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)` — loke lock.
- **Liña 32** — `else:` — Linux/macOS:
- **Liña 33** — `import fcntl`.
- **Liña 34** — `fcntl.flock(f.fileno(), fcntl.LOCK_EX)` — lock eskluzivu; komentáriu: loke bainhira arkivu taka.
- **Liña 35** — `f.write(text)`.
- **Liña 38** — `def _parse_lines(lines):`
- **Liña 39** — Docstring: (messages, bad_lines) husi fila JSON-lines.
- **Liña 40** — `messages, bad = [], []`.
- **Liña 41** — `for line in lines:`
- **Liña 42** — `if not line.strip():` — liña mamuk:
- **Liña 43** — `continue` — salta.
- **Liña 44** — `try:`
- **Liña 45** — `messages.append(json.loads(line))` — **deserializasaun** JSON.
- **Liña 46** — `except ValueError:` — JSON aat.
- **Liña 47** — `bad.append(line)`.
- **Liña 48** — `return messages, bad`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* arkivu ho liña `{"image_id":"a1"}\n`, `\n`, `{"image_id":` (corte) → `_parse_lines` → `([{"image_id": "a1"}], ['{"image_id":'])`.

**6. Tipu dadus**

| Funsaun | Parámetru | Return | Exception |
|---|---|---|---|
| `_append` | `path` (`Path`), `text: str` | `None` | `OSError` se lock la hetan (Windows ~10 s) |
| `_parse_lines` | iterável `str` (arkivu ka lista) | `tuple[list[dict], list[str]]` | la iha |

**7. Ligasaun OOP**

Objetu arkivu `f` iha métodu `write`, `flush`, `seek`, `fileno`. `_parse_lines` simu **iterável saida de'it** (arkivu ka lista) — duck typing.

**8. Impaktu integrasaun**

`LocalFileBackend.send` no `drain` (liña ba `.corrupt`) bolu `_append`. `drain` bolu `_parse_lines`.

**9. Checkpoint ba principiante**

**P:** Tanba sá presiza lock rua (`_APPEND_LOCK` no `msvcrt`/`fcntl`)?
**R:** `_APPEND_LOCK` proteje thread iha prosesu hanesan; lock OS proteje prosesu diferente (ezemplu watcher no dashboard) ne'ebé hakerek arkivu hanesan.

#### D.2.3 Klase `KafkaBackend` — konstrutór no `send`

**1. Kódigu orijinál**

{{code kafka/producer.py 51-63}}

**2. Objetivu**

Objetu ne'ebé haruka eventu ba Kafka loloos. Konstrutór harii `Producer` confluent-kafka no kontrola lalais katak broker moris. `send` haruka eventu ida no hamosu erru se Kafka la konfirma.

**3. Fundamentu Python**

- **Klase ho `__init__`**: `KafkaBackend("maize-disease-images")` → Python bolu `__init__(self, "maize-disease-images")`.
- **Atributu "privadu"** `self._producer` (konvensaun `_`).
- **Walrus** `if failed := self.send_many(...)`.
- **Dict vazia mak `False`** iha `if`.

**4. Esplikasaun liña-ba-liña**

- **Liña 51** — `class KafkaBackend:` — klase (herda `object`).
- **Liña 52** — `def __init__(self, topic: str):`
- **Liña 53** — `from confluent_kafka import Producer` — import lazy.
- **Liña 54** — `self.topic = topic` — atributu instánsia publiku.
- **Liña 55** — `self._producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})` — objetu produtór (konfigurasaun mínimu; serializasaun halo manual iha `send_many`).
- **Liña 56** — Komentáriu: falla lalais bainhira broker mate.
- **Liña 57** — `self._producer.list_topics(timeout=3)` — husu metadata; hamosu `KafkaException` se broker la responde iha 3 s.
- **Liña 59** — `def send(self, message: dict):`
- **Liña 60** — Komentáriu: hamosu erru se Kafka la konfirma, atu chamadór hatene.
- **Liña 61** — `from confluent_kafka import KafkaException`.
- **Liña 62** — `if failed := self.send_many([message], timeout=10):` — haruka lista ho mensajen ida; `failed` dict `{0: "erru"}` se falla.
- **Liña 63** — `raise KafkaException(f"Kafka delivery failed: {failed[0]} — broker unreachable?")`.

**5. Walkthrough ezekusaun**

`kb = KafkaBackend("maize-disease-images")` ho broker moris → objetu ho `kb.topic` no `kb._producer`. Broker mate → `list_topics` hamosu erru iha `__init__` → objetu la harii (ResilientBackend kaer). `kb.send({"image_id": "a1"})` → `send_many` fó `{}` → return `None`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `self.topic` | `str` |
| `self._producer` | `confluent_kafka.Producer` |
| `send(message: dict)` | return `None`; exception `KafkaException` |

**7. Ligasaun OOP**

- **Klase + objetu**: `KafkaBackend` mak klase; `ResilientBackend._kafka` kaer objetu.
- **`__init__`/`self`**: rai `topic` no `_producer` iha objetu.
- **Enkapsulamentu**: `_producer` "internu"; kódigu liur uza `send`/`send_many` de'it.
- **Komposisaun**: objetu `KafkaBackend` **na'in** objetu `Producer`.
- **Polimorfizmu (duck typing)**: `KafkaBackend`, `LocalFileBackend`, `ResilientBackend` hotu iha métodu `send(message)` — chamadór (`publish_drive_file`, `submit_image_for_processing`) la hatene klase ida ne'ebé. La iha klase baze komún (herança Not implemented in the current codebase).

**8. Impaktu integrasaun**

`get_backend` fó objetu ne'e direta se `KAFKA_USE_LOCAL_FALLBACK=False` (docker-compose rekomenda ida ne'e ba demo atu falla "loud").

**9. Checkpoint ba principiante**

**P:** Tanba sá konstrutór bolu `list_topics(timeout=3)`?
**R:** Atu hatene kedas se broker mate (falla lalais), duke hein bainhira haruka mensajen dahuluk.

#### D.2.4 `KafkaBackend.send_many` — serializasaun, produce, flush no ack

**1. Kódigu orijinál**

{{code kafka/producer.py 65-91}}

**2. Objetivu**

Haruka mensajen barak ho `flush` ida. Kada mensajen: dict → JSON → bytes UTF-8 → `produce` ho callback ne'ebé rai ack ka erru. Depois `flush`, mensajen ne'ebé la ack no la erru konta hanesan falla (timeout). Fó dict `{índise: erru}`.

**3. Fundamentu Python**

- **Closure factory** `_callback(index)` (haree A.3.6) — kada mensajen iha callback ne'ebé hanoin nia índise.
- **`set()`** ba ack; **`dict`** ba falla.
- **`while True: try ... break except BufferError: ...`** — koko fali to'o susesu.
- **`enumerate`**, **`range(len(...))`**.

**4. Esplikasaun liña-ba-liña**

- **Liña 65** — `def send_many(self, messages: list, timeout: float = 120) -> dict:`
- **Liña 66** — Docstring: haruka eventu barak ho flush ida; fó `{index: error}` ba sira ne'ebé la to'o.
- **Liña 67** — `failed = {}`.
- **Liña 68** — `acked = set()`.
- **Liña 70** — `def _callback(index):` — fábrika callback.
- **Liña 71** — `def _on_delivery(err, _msg):` — librdkafka bolu bainhira broker responde.
- **Liña 72** — `if err is not None:`
- **Liña 73** — `failed[index] = str(err)`.
- **Liña 74** — `else:`
- **Liña 75** — `acked.add(index)` — broker konfirma.
- **Liña 76** — `return _on_delivery`.
- **Liña 78** — `for index, message in enumerate(messages):`
- **Liña 79** — `payload = json.dumps(message).encode("utf-8")` — **serializasaun**: dict → str JSON → bytes.
- **Liña 80** — `while True:`
- **Liña 81** — `try:`
- **Liña 82** — `self._producer.produce(self.topic, payload, callback=_callback(index))` — tau mensajen iha buffer haruka (asinkrónu); la iha key.
- **Liña 83** — `break`.
- **Liña 84** — `except BufferError:` — komentáriu: buffer nakonu, husik nia mamuk.
- **Liña 85** — `self._producer.poll(0.5)` — prosesa evento/callback 0.5 s (buffer mamuk).
- **Liña 86** — `self._producer.poll(0)` — prosesa callback ne'ebé prontu, la hein.
- **Liña 87** — `self._producer.flush(timeout=timeout)` — hein to'o mensajen hotu haruka no callback halai (ka timeout).
- **Liña 88** — `for index in range(len(messages)):`
- **Liña 89** — `if index not in acked and index not in failed:` — la iha resposta:
- **Liña 90** — `failed[index] = f"not acknowledged within {timeout:.0f}s"`.
- **Liña 91** — `return failed`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `send_many([e0, e1], timeout=10)`. `produce` e0 ho callback(0), e1 ho callback(1). `flush(10)`: broker ack e0 → `acked={0}`; e1 erru `"Broker: Message size too large"` → `failed={1: "..."}`. Return `{1: "KafkaError{...}"}`. Se broker mate durante flush → la iha callback → `failed={0: "not acknowledged within 10s", 1: "..."}`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `messages` | `list[dict]` |
| `timeout` | `float` |
| `failed` | `dict[int, str]` |
| `acked` | `set[int]` |
| `payload` | `bytes` |
| return | `dict[int, str]` |

**7. Ligasaun OOP**

Métodu instánsia uza `self._producer` no `self.topic` (estadu objetu). Callback mak funsaun (objetu first-class) ne'ebé haruka ba objetu `Producer` — padraun **callback/observer** husi biblioteka.

**8. Impaktu integrasaun**

Bytes `payload` mak koluna `value` ne'ebé Spark lee (`col("value").cast("string")` iha `parse_events`, Modulu 01) — serializasaun iha ne'e no deserializasaun iha Spark tenke uza JSON UTF-8 hanesan.

**9. Checkpoint ba principiante**

**P:** Tanba sá `produce` la sufisiente atu hatene mensajen to'o?
**R:** `produce` só tau mensajen iha buffer lokál. Konfirmasaun mai husi callback bainhira broker responde; `flush` hein callback hotu.

#### D.2.5 `LocalFileBackend` no `ResilientBackend` (konstrutór, `_connect`)

**1. Kódigu orijinál**

{{code kafka/producer.py 94-130}}

**2. Objetivu**

`LocalFileBackend` hakerek eventu iha arkivu `<topic>.jsonl`. `ResilientBackend` uza Kafka bainhira moris; bainhira mate, uza fila lokál, no koko konekta fali kada 30 s.

**3. Fundamentu Python**

- **Atributu klase** `RETRY_SECONDS = 30`.
- **`self._local = LocalFileBackend(topic)`** — objetu iha objetu (komposisaun).
- **Return sedu** iha `_connect`.

**4. Esplikasaun liña-ba-liña**

- **Liña 94** — `class LocalFileBackend:`
- **Liña 95** — Docstring: fila arkivu lokál uza bainhira Kafka mate.
- **Liña 97** — `def __init__(self, topic: str):`
- **Liña 98** — `self.path = LOCAL_QUEUE_DIR / f"{topic}.jsonl"`.
- **Liña 100** — `def send(self, message: dict):`
- **Liña 101** — `_append(self.path, json.dumps(message) + "\n")` — serializa liña JSON ida.
- **Liña 104** — `class ResilientBackend:`
- **Liña 105–106** — Docstring: Kafka bainhira moris, fila lokál bainhira mate (kontrola kada `send`); Kafka fila → fila haruka uluk.
- **Liña 108** — `RETRY_SECONDS = 30` — atributu klase.
- **Liña 110** — `def __init__(self, topic: str):`
- **Liña 111** — `self.topic = topic`.
- **Liña 112** — `self._local = LocalFileBackend(topic)`.
- **Liña 113** — `self._kafka = None` — seidauk konekta.
- **Liña 114** — `self._next_attempt = 0.0` — bele koko kedas.
- **Liña 115** — `self._connect()` — koko konekta iha konstrutór.
- **Liña 117** — `def _connect(self):`
- **Liña 118** — `if self._kafka is not None:` — konekta ona:
- **Liña 119** — `return self._kafka`.
- **Liña 120** — `if time.monotonic() < self._next_attempt:` — seidauk tempu koko fali:
- **Liña 121** — `return None`.
- **Liña 122** — `try:`
- **Liña 123** — `self._kafka = KafkaBackend(self.topic)`.
- **Liña 124** — `print(...)` — "Connected to Kafka at ...".
- **Liña 125** — `except Exception as e:`
- **Liña 126** — `self._next_attempt = time.monotonic() + self.RETRY_SECONDS`.
- **Liña 127–129** — `print(...)` — Kafka la iha; uza fila lokál; koko fali 30 s; Spark **la** lee fila ne'e — hahú broker no backlog haruka automátiku.
- **Liña 130** — `return self._kafka`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* Docker mate. `ResilientBackend("maize-disease-images")` → `_connect` → `KafkaBackend` falla → `_next_attempt = agora + 30` → print aviszu. `send` iha 10 s depois → `_connect` fó `None` (seidauk 30 s) → `LocalFileBackend.send` → liña iha `storage/local_queue/maize-disease-images.jsonl`.

**6. Tipu dadus**

| Atributu | Tipu |
|---|---|
| `LocalFileBackend.path` | `Path` |
| `ResilientBackend.RETRY_SECONDS` (klase) | `int` |
| `.topic` | `str` |
| `._local` | `LocalFileBackend` |
| `._kafka` | `KafkaBackend ka None` |
| `._next_attempt` | `float` |
| `_connect()` return | `KafkaBackend ka None` |

**7. Ligasaun OOP**

- **Komposisaun**: `ResilientBackend` na'in `LocalFileBackend` (sempre) no `KafkaBackend` (bainhira konekta). Objetu interior harii iha `__init__`/`_connect` no moris hamutuk ho objetu liur.
- **Atributu klase vs instánsia**: `RETRY_SECONDS` partilla; `_kafka`, `_next_attempt` kada objetu.
- **Padraun dezeñu**: hanesan **Proxy/Fallback** — objetu ida ho interface `send` hanesan ne'ebé hili objetu interior tuir estadu. Sira la herda klase komún.

**8. Impaktu integrasaun**

Watcher no `pipeline_submit` uza `ResilientBackend` (default). Spark la lee fila lokál — fila tenke haruka ba Kafka (`drain`).

**9. Checkpoint ba principiante**

**P:** Saida mak `self.RETRY_SECONDS` lee?
**R:** Atributu klase `ResilientBackend.RETRY_SECONDS` (30), tanba objetu la iha atributu instánsia ho naran ne'e.

#### D.2.6 `ResilientBackend._lost_kafka` no `send`

**1. Kódigu orijinál**

{{code kafka/producer.py 132-147}}

**2. Objetivu**

`send` haruka eventu: se Kafka moris, haruka fila tuan uluk (`drain`) atu mantein orden, depois eventu foun; se haruka falla, marka Kafka lakon no rai iha fila lokál.

**3. Fundamentu Python**

- **`is`** — komparasaun identidade objetu (objetu hanesan, la'ós só valór hanesan).
- **Return sedu** bainhira susesu.

**4. Esplikasaun liña-ba-liña**

- **Liña 132** — `def _lost_kafka(self, e: Exception):`
- **Liña 133** — `print(...)` — "Kafka send failed ...; falling back to local file queue."
- **Liña 134** — `self._kafka = None`.
- **Liña 135** — `self._next_attempt = time.monotonic() + self.RETRY_SECONDS`.
- **Liña 137** — `def send(self, message: dict):`
- **Liña 138** — `kafka = self._connect()`.
- **Liña 139** — `if kafka is not None:`
- **Liña 140** — `self.drain()` — komentáriu: eventu tuan iha fila ba uluk.
- **Liña 141** — `if self._kafka is kafka:` — `drain` la lakon Kafka (se lakon, `_kafka` sai `None`).
- **Liña 142** — `try:`
- **Liña 143** — `kafka.send(message)`.
- **Liña 144** — `return`.
- **Liña 145** — `except Exception as e:`
- **Liña 146** — `self._lost_kafka(e)`.
- **Liña 147** — `self._local.send(message)` — fallback.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fila iha eventu 3; Kafka moris fali. `send(e4)` → `drain` haruka e1–e3 → `_kafka` hanesan → `kafka.send(e4)` → return. Orden iha topic: e1, e2, e3, e4. Se `kafka.send(e4)` falla → `_lost_kafka` → e4 ba fila.

**6. Tipu dadus**

`send(message: dict)` → `None`; la hamosu erru (erru Kafka kaer; erru `_append` bele sa'e).

**7. Ligasaun OOP**

Objetu bolu nia métodu rasik (`self._connect()`, `self.drain()`, `self._lost_kafka(e)`) no métodu objetu interior (`kafka.send`, `self._local.send`) — **delegasaun**.

**8. Impaktu integrasaun**

`publish_drive_file` (`backend.send(event)`) no `submit_image_for_processing` bolu métodu ne'e.

**9. Checkpoint ba principiante**

**P:** Tanba sá `drain()` molok haruka eventu foun?
**R:** Atu eventu tuan to'o Spark uluk — orden kronolójiku mantein.

#### D.2.7 `ORPHAN_CLAIM_SECONDS` no `_claim_files`

**1. Kódigu orijinál**

{{code kafka/producer.py 149-173}}

**2. Objetivu**

"Kaer" (claim) arkivu fila atu haruka: troka naran arkivu ba `.draining-<uuid>`. Troka naran mak **lock entre prosesu** — prosesu ida de'it bele troka naran arkivu ida. Mós kaer arkivu `.draining-*` tuan liu 600 s (prosesu ne'ebé mate husik).

**3. Fundamentu Python**

- **`path.parent.glob(padraun)`** — buka arkivu tuir padraun.
- **`sorted(...)`** — orden.
- **`old.stat().st_mtime`** — tempu muda ikus (segundu desde 1970).
- **`os.utime(dst)`** — muda tempu arkivu ba agora.

**4. Esplikasaun liña-ba-liña**

- **Liña 149** — Komentáriu: arkivu kaer tuan liu ne'e husik husi prosesu mate.
- **Liña 150** — `ORPHAN_CLAIM_SECONDS = 600` — atributu klase.
- **Liña 152** — `def _claim_files(self) -> list:`
- **Liña 153–154** — Docstring: kaer arkivu fila (no kaer órfaun) ho troka naran; troka naran mak lock entre prosesu.
- **Liña 155** — `path = self._local.path`.
- **Liña 156** — `sources = [path] if path.exists() else []`.
- **Liña 157** — `cutoff = time.time() - self.ORPHAN_CLAIM_SECONDS`.
- **Liña 158** — `for old in sorted(path.parent.glob(f"{path.name}.draining-*")):`
- **Liña 159** — `try:`
- **Liña 160** — `if old.stat().st_mtime < cutoff:` — tuan liu 600 s:
- **Liña 161** — `sources.append(old)`.
- **Liña 162** — `except OSError:` — arkivu lakon (prosesu seluk kaer).
- **Liña 163** — `continue`.
- **Liña 164** — `claimed = []`.
- **Liña 165** — `for src in sources:`
- **Liña 166** — `dst = path.with_name(f"{path.name}.draining-{uuid.uuid4().hex}")`.
- **Liña 167** — `try:`
- **Liña 168** — `os.replace(src, dst)` — troka naran atómiku.
- **Liña 169** — `os.utime(dst)` — komentáriu: tempu kaer.
- **Liña 170** — `except OSError:` — prosesu seluk kaer uluk.
- **Liña 171** — `continue`.
- **Liña 172** — `claimed.append(dst)`.
- **Liña 173** — `return claimed`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `maize-disease-images.jsonl` iha → troka ba `maize-disease-images.jsonl.draining-3f2a...` → `claimed = [Path(...draining-3f2a...)]`. Watcher no dashboard koko kaer iha tempu hanesan → ida de'it susesu `os.replace`; seluk hetan `OSError` no salta.

**6. Tipu dadus**

`ORPHAN_CLAIM_SECONDS`: `int` (atributu klase); `sources`, `claimed`: `list[Path]`; `cutoff`: `float`.

**7. Ligasaun OOP**

Atributu klase segundu (`ORPHAN_CLAIM_SECONDS`). Métodu "privadu" (`_claim_files`) — enkapsulamentu ho konvensaun.

**8. Impaktu integrasaun**

Bolu de'it husi `drain` (liña 181).

**9. Checkpoint ba principiante**

**P:** Tanba sá troka naran funsiona hanesan lock?
**R:** Arkivu ida bele troka naran dala ida de'it — prosesu dahuluk susesu; prosesu segundu la hetan arkivu orijinál no hetan erru.

#### D.2.8 `ResilientBackend.drain` no `get_backend`

**1. Kódigu orijinál**

{{code kafka/producer.py 175-221}}

**2. Objetivu**

Haruka fila lokál ba Kafka. Liña aat ba arkivu `.corrupt`. Se Kafka falla iha klaran, eventu ne'ebé seidauk haruka fila fali ba fila lokál (la lakon). `get_backend` hili `ResilientBackend` ka `KafkaBackend` tuir konfigurasaun.

**3. Fundamentu Python**

- **Slicing** `messages[done:]`, `claimed[i + 1:]`.
- **`break`** — sai husi loop `for`.
- **`path.unlink()`** — hamoos arkivu.
- **Espresaun kondisionál ba klase**: `(A if x else B)(topic)` — hili klase, depois harii objetu.

**4. Esplikasaun liña-ba-liña**

- **Liña 175** — `def drain(self) -> int:`
- **Liña 176–177** — Docstring: haruka fila lokál ba Kafka; fó kuantu; liña la bele lee ba `<queue>.corrupt`.
- **Liña 178** — `kafka = self._connect()`.
- **Liña 179** — `if kafka is None:`
- **Liña 180** — `return 0`.
- **Liña 181** — `claimed = self._claim_files()`.
- **Liña 182** — `sent = 0`.
- **Liña 183** — `for i, path in enumerate(claimed):`
- **Liña 184** — `try:`
- **Liña 185** — `with open(path) as f:`
- **Liña 186** — `messages, bad = _parse_lines(f)` — deserializa.
- **Liña 187** — `except OSError as e:`
- **Liña 188** — `print(...)` — "Could not read ...".
- **Liña 189** — `continue`.
- **Liña 190** — `if bad:`
- **Liña 191–192** — `_append(self._local.path.with_suffix(".corrupt"), "".join(line if line.endswith("\n") else line + "\n" for line in bad))` — rai liña aat (ho `\n`).
- **Liña 193–194** — `print(...)` — kuantu liña aat muda ba `.corrupt`.
- **Liña 195** — `done = 0`.
- **Liña 196** — `try:`
- **Liña 197** — `for m in messages:`
- **Liña 198** — `kafka.send(m)`.
- **Liña 199** — `done += 1`.
- **Liña 200** — `except Exception as e:`
- **Liña 201** — Komentáriu: tau fali buat hotu ne'ebé seidauk haruka iha fila.
- **Liña 202** — `for m in messages[done:]:`
- **Liña 203** — `self._local.send(m)`.
- **Liña 204** — `for rest in claimed[i + 1:]:` — arkivu kaer seluk ne'ebé seidauk loke:
- **Liña 205** — `with open(rest) as f:`
- **Liña 206** — `for m in _parse_lines(f)[0]:`
- **Liña 207** — `self._local.send(m)`.
- **Liña 208** — `rest.unlink()`.
- **Liña 209** — `path.unlink()`.
- **Liña 210** — `sent += done`.
- **Liña 211** — `self._lost_kafka(e)`.
- **Liña 212** — `break`.
- **Liña 213** — `path.unlink()` — arkivu haruka hotu, hamoos.
- **Liña 214** — `sent += done`.
- **Liña 215** — `if sent:`
- **Liña 216** — `print(...)` — "Forwarded N queued event(s) ... to Kafka."
- **Liña 217** — `return sent`.
- **Liña 220** — `def get_backend(topic: str):`
- **Liña 221** — `return (ResilientBackend if KAFKA_USE_LOCAL_FALLBACK else KafkaBackend)(topic)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fila ho eventu 5 + liña aat 1. `drain` → kaer arkivu → `messages` 5, `bad` 1 → liña aat ba `maize-disease-images.corrupt` → haruka e1, e2, e3; e4 falla → e4, e5 fila fali ba `maize-disease-images.jsonl` → `sent = 3` → `_lost_kafka` → return 3.

**6. Tipu dadus**

| Funsaun | Return | Exception |
|---|---|---|
| `drain` | `int` | la iha (erru Kafka kaer; erru OS iha `rest` bele sa'e) |
| `get_backend(topic)` | `ResilientBackend ka KafkaBackend` | `KafkaBackend` hamosu erru se broker mate |

**7. Ligasaun OOP**

`get_backend` mak **Factory function** (Teoria: padraun Factory): hili klase tuir konfigurasaun no harii objetu. Klase mak objetu iha Python, entaun espresaun kondisionál bele fó klase.

**8. Impaktu integrasaun**

Watcher `main` bolu `backend.drain()` kada volta (liña 228–229); `send` bolu `drain` molok eventu foun.

**9. Checkpoint ba principiante**

**P:** Se Kafka mate iha klaran `drain`, eventu ne'ebé seidauk haruka lakon?
**R:** Lae. Liña 202–208 tau fali eventu hotu ne'ebé seidauk haruka (no arkivu kaer seluk) ba fila lokál.
