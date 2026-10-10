# Dokumentasaun — Crop Disease Early Warning System

> **Nota kona-ba tradusaun:** Dokumentu ida-ne'e hakerek uza liafuan Tetun ba
> titulu no deskrisaun badak, no Ingles ba komandu tékniku (curl, kódigu,
> naran field/parâmetru) — nune'e naran tékniku la sai sala. Favór husu ema
> ida ne'ebé fluente iha Tetun atu lee fila-fali antes submete.
> Atualiza ikus: 10 Outubru 2026.

## 1. Introdusaun

*Crop Disease Early Warning System* mak sistema ida ba detekta moras
batar husi litratu. Agrikultór (USER) uza aplikasaun Android atu foti
litratu husi tahan batar; aplikasaun haruka litratu ba nia pasta privadu iha
**Google Drive**. Iha laptop, *Drive watcher* hetan litratu foun no publika
event ba Kafka; Spark prosesa litratu ho modelu **CNN** (la iha tan modelu
heuristic, "gate" ka hasai fundu), kalkula nível risku (LOW/MEDIUM/HIGH) no
haruka rezultadu fila-fali ba Google Drive, ne'ebé aplikasaun hatudu:

**Android → Google Drive → Kafka → Spark → Google Drive → Android.**

Komponente sira:
- **Android app** — login ho konta Google, upload litratu, istória.
- **Google Drive** — rai litratu orijinál (`images/<email>/`) no rezultadu
  (`results/<email>/`).
- **Drive watcher** (`kafka/drive_watcher.py`) — Drive → Kafka.
- **Kafka** — transporta event ba Spark.
- **Spark Structured Streaming** (`spark/streaming_job.py`) — limpa dadus,
  halo inferénsia CNN, kalkula risku, grava rezultadu.
- **Spark batch job** (`spark/batch_analytics.py`) — agregasaun loroloron
  kada munisípiu, liu husi `spark.read.jdbc`.
- **Baze dadus** — PostgreSQL (Docker) ba demo; SQLite ba teste deit.
- **Dashboard** (`dashboard/app.py`, Streamlit) — painel admin.
- **Tableau** — lee view `v_tableau_images` ho konta read-only.
- **API** (`api/server.py`) — sei iha, maibé aplikasaun la uza ona.

## 2. Prepara ambiente (Setup)

```bash
pip install -r requirements.txt
copy .env.example .env        # depois edita .env: JWT_SECRET, etc.
python -m storage.init_db     # kria tabela sira + admin bootstrap
```

Admin bootstrap default: username `admin`, password husi
`BOOTSTRAP_ADMIN_PASSWORD` iha `.env` (defini valor forte antes submete).

### 2.1 PostgreSQL ba demo (manuál seksaun 5.3)

```bash
docker compose up -d
```

Iha `.env`:

```
DATABASE_URL=postgresql+psycopg2://ews_user:ews_password@localhost:5432/crop_disease
KAFKA_USE_LOCAL_FALLBACK=false
```

## 3. Halo sistema funsiona (`run_all.py`)

```bash
python run_all.py
```

Komandu ida-ne'e inisializa baze dadus, hahú Kafka no PostgreSQL (Docker) se
seidauk funsiona, depois hahú **3 prosesu Python**, kada ida iha nia janela:
Spark streaming job, dashboard (http://localhost:8501) no Drive watcher.

```bash
python run_all.py --stop            # para hotu-hotu ne'ebé run_all.py hahú
python run_all.py --no-kafka        # se broker Kafka funsiona tiha ona iha fatin seluk
python run_all.py --api             # hahú mós REST API
```

## 4. Modu Prosesamentu: AUTOMATIC vs MANUAL

- **AUTOMATIC** — Drive watcher publika litratu foun ba Kafka kedas.
- **MANUAL** — litratu grava ho status `PENDING_REVIEW`; admin tenke klik
  **Process** (pájina *Pending review*) ka muda ba AUTOMATIC (ne'ebé libera
  litratu hotu ne'ebé hein).

Muda modu iha *sidebar* dashboard nian. Bainhira litratu ida libera, nia
status sai `INGESTED` **antes** event publika. Tanba ne'e Spark la prosesa
event ida ne'ebé hetan litratu sei `PENDING_REVIEW` — event tuan ida (ezemplu
Spark lee fila-fali topic hafoin *factory reset* ne'ebé la hamoos Kafka)
labele liu modu MANUAL.

Se `system_settings` seidauk iha valor, default mak AUTOMATIC.

## 5. API — Login

```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"<password>"}'
```

## 6. API — Upload litratu

Field ba litratu mak **`photo`**:

```bash
curl -X POST http://localhost:8000/upload-image \
  -H "Authorization: Bearer <token>" \
  -F "photo=@leaf.jpg" \
  -F "location=Dili"
```

## 7. Istória rasik (agrikultór)

```bash
curl http://localhost:8000/my/history?limit=20 \
  -H "Authorization: Bearer <token>"
```

## 8. Rekuperasaun Admin

```bash
curl -X PUT http://localhost:8000/admin/users/{user_id} \
  -H "Authorization: Bearer <admin_token>" \
  -H "Content-Type: application/json" \
  -d '{"status": "ACTIVE"}'
```

Sistema **la iha** funsaun "forgot password" ba admin.

### 8.1 Litratu hela iha INGESTED (Spark la prosesa)

```bash
python -m storage.requeue_stuck --dry-run
python -m storage.requeue_stuck --all
python -m storage.requeue_stuck --all --inline
```

## 9. Kafka — instalasaun package

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark/streaming_job.py
spark-submit --packages org.postgresql:postgresql:42.7.3 spark/batch_analytics.py
```

`--packages` download husi Maven Central dala uluk deit — presiza internet
iha dala uluk.

## 10. Teste eskalabilidade

Dashboard → **Scalability test** (detallu: `SCALABILITY_TEST.md`). Teste ida-ne'e
**izoladu** husi pipeline real-time: nia iha nia Kafka topic rasik
(`maize-disease-images-scalability-test`), nia *consumer group* rasik no nia
prosesu rasik; uza kódigu hanesan (`prepare_image` + CNN), maibé la kria
`images`/`predictions` no la haruka rezultadu ba Drive. Bainhira litratu
agrikultór sira iha prosesu, teste para uluk (real-time iha prioridade).

Pájina hatudu de'it:
- litratu ida *antes* no *depois* preprosesamentu (la iha naran moras);
- **Average Execution Time** (segundu) — tempu iterasaun ida, husi event
  dahuluk to'o litratu ikus;
- **Average Processing Time** (segundu/litratu) — preprosesamentu + CNN de'it.

```bash
python -m scalability.run start --size 100 --iterations 3
python -m scalability.run start --size 5000 --source-dir D:\maize_photos
```

## 11. Fluxu Upload

1. Agrikultór foti litratu (kamera ka galeria), hili munisípiu.
2. Aplikasaun upload ba Drive `images/<email>/`.
3. Drive watcher rejista litratu (SHA-256 husi Drive, oras upload husi Google).
4. AUTOMATIC: publika ba Kafka, status `INGESTED`. MANUAL: `PENDING_REVIEW`.
5. Spark limpa dadus (`filter`, `to_timestamp`, `initcap(trim(location))`,
   rejeita `captured_at` iha futuru), lee litratu (kópia lokál ka Drive),
   lee EXIF (GPS, oras, kámara), `prepare_image`, CNN, risku.
6. Rezultadu grava iha PostgreSQL no haruka ba Drive `results/<email>/`.
7. Aplikasaun hatudu litratu orijinál no litratu ho marka moras.

```
Android --> Google Drive --> Drive watcher --> Kafka --> Spark (CNN + risku)
                                                           |
                                    PostgreSQL <-----------+-----------> Drive results --> Android
                                        |
                              Dashboard admin / Tableau
```

## 12. Jestaun litratu (Image management)

Pájina **Image management** iha dashboard:
- **Kontajen** — rejistu iha baze dadus (status, ho predisaun, hein label)
  no file iha laptop (kópia lokál, espasu, kópia lakon/la loos, file órfaun).
- **Galeria** — litratu foun liu iha leten; filtru ba ID, agrikultór,
  munisípiu, data, status, moras, risku, label no kópia lokál.
- **Detallu** — litratu orijinál no litratu prosesadu lado-ba-lado;
  **predisaun modelu** no **label ne'ebé admin konfirma** hatudu ketak.
- **Storage** — espasu kada kategoria, rekonsiliasaun file ho baze dadus,
  *preview* (la hamoos buat ida) no hamoos ho konfirmasaun admin; kada
  asaun grava iha `storage_audit_log`. Litratu ba label/treinu no litratu
  ne'ebé sei prosesa la hamoos.

Kópia lokál iha `storage/downloaded_photos/` verifika ho SHA-256; se lakon ka
la loos, sistema download fali husi Drive no verifika antes rai.

## 13. Factory reset

*System settings* → *Factory reset* hatudu uluk saida mak sei hamoos no saida
mak sei rai. Nia hamoos `images`, `predictions`, `processing_events`,
`rejected_events`, checkpoint Spark no fila Kafka lokál; rai users, label,
audit log, sukat eskalabilidade, kópia lokál, litratu review no treinu.
**Tenke para Spark job no Drive watcher uluk** — sistema la husik reset
bainhira sira sei funsiona.

## 14. Unidade tempu

Naran ne'ebé remata ho `_ms` = milisegundu; `_s` / `_seconds` = segundu.
Pájina *Performance* hatudu tempu hotu iha **segundu**
(`processing_latency_ms` fahe ho 1000).

## 15. Teste (pytest)

```bash
pytest -q
```

Suite kompletu inklui teste PySpark (local mode), modelu CNN real, fluxu
Drive (Drive falsu iha memória), jestaun litratu no teste eskalabilidade.
