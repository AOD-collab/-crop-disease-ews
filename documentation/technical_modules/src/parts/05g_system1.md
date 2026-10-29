
### D.8 `dashboard/model_ops.py` — deploy modelu ba sistema

#### D.8.1 Docstring, import, `CLASS_NAME_MAP`, `info`

**1. Kódigu orijinál**

{{code dashboard/model_ops.py 1-30}}

**2. Objetivu**

Serbisu checkpoint ba pájina "Model training", halai hanesan prosesu rasik (torch labele import depois pandas/pyarrow iha Windows). Mapa naran klase treinu → sistema. `info` fó metadata checkpoint (la ho peze).

**3. Fundamentu Python**

Dict comprehension ho filtru: `{k: v for k, v in ck.items() if not k.endswith("state_dict")}`. `torch.load(..., weights_only=True)` — karrega seguru (la halai kódigu arbitráriu).

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: serbisu checkpoint ba pájina Model training, prosesu rasik (torch labele import depois pandas/pyarrow iha Windows); print JSON.
- **Liña 4** — Liña mamuk docstring.
- **Liña 5** — Uza: `python -m dashboard.model_ops info <checkpoint>`.
- **Liña 6** — Uza: `python -m dashboard.model_ops deploy <best> <deployed> <backup dir> <extra json>`.
- **Liña 7** — Remata docstring.
- **Liña 9** — `import json`.
- **Liña 10** — `import os`.
- **Liña 11** — `import re`.
- **Liña 12** — `import shutil`.
- **Liña 13** — `import sys`.
- **Liña 14** — `from datetime import datetime`.
- **Liña 15** — `from pathlib import Path`.
- **Liña 17** — Komentáriu: naran klase projetu treinu → naran sistema.
- **Liña 18** — `CLASS_NAME_MAP = {`
- **Liña 19** — `"Healthy": "healthy",`
- **Liña 20** — `"Blight": "northern_leaf_blight",`
- **Liña 21** — `"Common_Rust": "common_rust",`
- **Liña 22** — `"Gray_Leaf_Spot": "gray_leaf_spot",`
- **Liña 23** — `}`.
- **Liña 26** — `def info(path: str) -> dict:`
- **Liña 27** — Docstring: buat hotu iha checkpoint exetu peze.
- **Liña 28** — `import torch`.
- **Liña 29** — `ck = torch.load(path, map_location="cpu", weights_only=True)`.
- **Liña 30** — `return {k: v for k, v in ck.items() if not k.endswith("state_dict")}`.

**5. Walkthrough ezekusaun**

`info("checkpoints/best_seresnet18_crop_ews.pth")` → `{"class_names": ["Healthy", ...], "img_size": 224, "val_accuracy": 0.96, "epoch": 15}` (*ezemplu ilustrativu* ba valór).

**6. Tipu dadus**

`CLASS_NAME_MAP`: `dict[str, str]`; `info(str) -> dict`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

Naran klase sistema (`northern_leaf_blight`...) mak naran ne'ebé rai iha `predictions.predicted_class` no hatudu iha dashboard/Android.

**9. Checkpoint ba principiante**

**P:** Tanba sá `weights_only=True`?
**R:** `torch.load` normál bele halai kódigu Python iha arkivu (risku seguransa). `weights_only=True` karrega tensor/tipu simples de'it.

#### D.8.2 `deploy` no `swap_in`

**1. Kódigu orijinál**

{{code dashboard/model_ops.py 33-80}}

**2. Objetivu**

Exporta checkpoint best ba formatu sistema (`state_dict`, `architecture="se_resnet18"`, naran klase sistema, `img_size`, `mean`, `std`, metadata), **kontrola** katak modelu karrega no halai (input zero), rai ba `.tmp`, depois backup modelu atuál no troka.

**3. Fundamentu Python**

`{**extra}` kombina dict; `torch.zeros(1, 3, S, S)`; `shutil.copy2` (kopia ho metadata); `os.replace` (atómiku); f-string ho formatu data `f"{datetime.now():%Y%m%d_%H%M%S}"`.

**4. Esplikasaun liña-ba-liña**

- **Liña 33** — `def deploy(best: str, deployed: str, backup_dir: str, extra: dict) -> dict:`
- **Liña 34–35** — Docstring: exporta checkpoint best, kontrola nia karrega no halai, backup modelu atuál, depois troka.
- **Liña 36** — `import torch`.
- **Liña 37** — `from ml.se_resnet import SEResNet18`.
- **Liña 39** — `best, deployed, backup_dir = Path(best), Path(deployed), Path(backup_dir)`.
- **Liña 40** — `ck = torch.load(best, map_location="cpu", weights_only=True)`.
- **Liña 41** — `class_names = [CLASS_NAME_MAP.get(c, c.lower()) for c in ck["class_names"]]`.
- **Liña 42** — `exported = {`
- **Liña 43** — `"state_dict": ck["model_state_dict"],` — chave muda (`model_state_dict` → `state_dict`).
- **Liña 44** — `"architecture": "se_resnet18",`
- **Liña 45** — `"class_names": class_names,`
- **Liña 46** — `"healthy_class_name": "healthy",`
- **Liña 47** — `"img_size": ck.get("img_size", 224),`
- **Liña 48** — `"mean": [0.485, 0.456, 0.406],`
- **Liña 49** — `"std": [0.229, 0.224, 0.225],`
- **Liña 50** — Komentáriu: ba rastreabilidade de'it.
- **Liña 51** — `"trained_epoch": ck.get("epoch"),`
- **Liña 52** — `"val_accuracy": ck.get("val_accuracy"),`
- **Liña 53** — `"source_checkpoint": str(best),`
- **Liña 54** — `"source_modified_at": datetime.fromtimestamp(best.stat().st_mtime).isoformat(timespec="seconds"),`
- **Liña 55** — `"deployed_at": datetime.now().isoformat(timespec="seconds"),`
- **Liña 56** — `**extra,` — ezemplu `test_metrics`, `deployed_by`.
- **Liña 57** — `}`.
- **Liña 59** — Komentáriu: rejeita buat ne'ebé `ml/inference.py` la bele karrega.
- **Liña 60** — `model = SEResNet18(num_classes=len(class_names))`.
- **Liña 61** — `model.load_state_dict(exported["state_dict"])` — erru se naran kamada la hanesan.
- **Liña 62** — `model.eval()`.
- **Liña 63** — `with torch.no_grad():`
- **Liña 64** — `model(torch.zeros(1, 3, exported["img_size"], exported["img_size"]))` — teste forward.
- **Liña 66** — `deployed.parent.mkdir(parents=True, exist_ok=True)`.
- **Liña 67** — `tmp = deployed.with_suffix(".pt.tmp")`.
- **Liña 68** — `torch.save(exported, tmp)`.
- **Liña 69** — `return {"backup": swap_in(tmp, deployed, backup_dir), "class_names": class_names}`.
- **Liña 72** — `def swap_in(tmp: Path, deployed: Path, backup_dir: Path) -> str:`
- **Liña 73** — Docstring: backup modelu atuál, depois muda `tmp` ba fatin; fó dalan backup.
- **Liña 74** — `backup = ""`.
- **Liña 75** — `if deployed.exists():`
- **Liña 76** — `backup_dir.mkdir(exist_ok=True)`.
- **Liña 77** — `backup = str(backup_dir / f"{deployed.stem}_{datetime.now():%Y%m%d_%H%M%S}.pt")`.
- **Liña 78** — `shutil.copy2(deployed, backup)`.
- **Liña 79** — `os.replace(tmp, deployed)` — atómiku; inferénsia haree arkivu foun (hot-reload, D.12.1).
- **Liña 80** — `return backup`.

**5. Walkthrough ezekusaun**

`deploy("checkpoints/best_...pth", "ml/trained_model/saved_model_cnn.pt", "ml/trained_model/backups", {"deployed_by": "auto"})` → kontrola OK → backup `saved_model_cnn_20261029_221500.pt` (*ezemplu*) → troka → `{"backup": "...", "class_names": ["healthy", "northern_leaf_blight", "common_rust", "gray_leaf_spot"]}`.

**6. Tipu dadus**

`exported`: `dict`; `model`: `SEResNet18` (sistema); `deploy -> dict`; `swap_in -> str`.

**7. Ligasaun OOP**

Harii objetu `SEResNet18` (`ml/se_resnet.py`) atu **valida** — kontratu entre klase treinu no klase sistema (naran atributu hanesan).

**8. Impaktu integrasaun**

Arkivu `saved_model_cnn.pt` = `IMAGE_MODEL["weights_path"]` default (D.9). `ml/inference._load_trained_model` deteta mudansa (mtime/size/inode) no karrega fali.

**9. Checkpoint ba principiante**

**P:** Tanba sá teste `model(torch.zeros(...))` molok troka?
**R:** Atu garante modelu foun karrega no halai; se la, deploy falla molok troka modelu ne'ebé funsiona.

#### D.8.3 `utf8_env`, `detached_popen`, `_test_metrics`, `is_better`

**1. Kódigu orijinál**

{{code dashboard/model_ops.py 83-131}}

**2. Objetivu**

Ambiente UTF-8 ba prosesu oan; hahú prosesu background separadu husi dashboard; lee métrika teste husi log `train.py`; deside se modelu foun di'ak liu (akurásia teste, ka validasaun).

**3. Fundamentu Python**

`subprocess.Popen(...)`; regex `rf"^{label}: ([\d.]+)"` ho `re.M`; walrus iha `if`; funsaun lokál; type hint `tuple[bool, str]`, `dict | None`.

**4. Esplikasaun liña-ba-liña**

- **Liña 83** — `def utf8_env() -> dict:`
- **Liña 84–86** — Docstring: ambiente ho stdio UTF-8: `train.py` print "✓"; la ho ida ne'e Python Windows hakerek stdout redireksiona iha cp1252 no monu.
- **Liña 87** — `return {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}`.
- **Liña 90** — `def detached_popen(argv: list, log_path, cwd):`
- **Liña 91–92** — Docstring: hahú `argv` iha background, separadu husi dashboard (grupu prosesu rasik, la ho janela), stdout/stderr iha `log_path`.
- **Liña 93** — `import subprocess`.
- **Liña 94–95** — `kwargs = ({"creationflags": CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW} if sys.platform == "win32" else {"start_new_session": True})`.
- **Liña 96** — `with open(log_path, "wb") as log:`
- **Liña 97–98** — `return subprocess.Popen(argv, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=utf8_env(), **kwargs)`.
- **Liña 101** — `def _test_metrics(log_text: str) -> dict:`
- **Liña 102–103** — Docstring: rezultadu teste ne'ebé `train.py` print iha rohan (liña hanesan `_parse_log` pájina lee), iha persentajen.
- **Liña 104** — `out = {}`.
- **Liña 105–106** — `for key, label in (("loss", "Test Loss"), ("accuracy", "Test Accuracy"), ("precision", "Precision"), ("recall", "Recall"), ("f1", "F1-Score")):`
- **Liña 107** — `if m := re.search(rf"^{label}: ([\d.]+)", log_text, re.M):`
- **Liña 108** — `out[key] = float(m.group(1))`.
- **Liña 109** — `return out`.
- **Liña 112** — `def is_better(new: dict, current: dict | None) -> tuple[bool, str]:`
- **Liña 113–116** — Docstring: se modelu foun tenke troka modelu deploy; kompara akurásia teste se rua iha, se lae validasaun; modelu deploy la ho rua sempre troka; empate mantein modelu atuál.
- **Liña 117** — `def test_acc(m):`
- **Liña 118** — `return (m.get("test_metrics") or {}).get("accuracy")`.
- **Liña 120** — `def val_acc(m):` — komentáriu: rai hanesan fraksaun iha checkpoint.
- **Liña 121** — `return None if m.get("val_accuracy") is None else m["val_accuracy"] * 100`.
- **Liña 123** — `if current is None:`
- **Liña 124** — `return True, "no model was deployed"`.
- **Liña 125** — `for label, get in (("test accuracy", test_acc), ("validation accuracy", val_acc)):`
- **Liña 126** — `a, b = get(new), get(current)`.
- **Liña 127** — `if a is not None and b is not None:`
- **Liña 128** — `if a > b:` — aas liu (la'ós hanesan).
- **Liña 129** — `return True, f"{label} {a:.2f}% > {b:.2f}% of the model in use"`.
- **Liña 130** — `return False, f"{label} {a:.2f}% is not better than {b:.2f}% of the model in use"`.
- **Liña 131** — `return True, "the model in use has no recorded accuracy"`.

**5. Walkthrough ezekusaun**

Log ho "Test Accuracy: 95.20%" → `_test_metrics` → `{"accuracy": 95.2, ...}`. `is_better({"test_metrics": {"accuracy": 95.2}}, {"test_metrics": {"accuracy": 94.58}})` → `(True, "test accuracy 95.20% > 94.58% of the model in use")` (*ezemplu ilustrativu*).

**6. Tipu dadus**

`utf8_env -> dict[str, str]`; `detached_popen -> subprocess.Popen`; `_test_metrics -> dict[str, float]`; `is_better -> tuple[bool, str]`.

**7. Ligasaun OOP**

`subprocess.Popen` — objetu prosesu. Funsaun `test_acc`/`val_acc` haruka hanesan valór iha tuple (first-class function — padraun *Strategy* simples).

**8. Impaktu integrasaun**

Pájina dashboard "Model training" bolu `detached_popen` ba `python -m dashboard.model_ops train <json>`.

**9. Checkpoint ba principiante**

**P:** Se akurásia hanesan (empate), modelu foun troka?
**R:** Lae — `a > b` tenke loos; empate mantein modelu atuál.

#### D.8.4 `train_and_deploy` no `__main__`

**1. Kódigu orijinál**

{{code dashboard/model_ops.py 134-181}}

**2. Objetivu**

Halai `train.py` (prosesu oan), lee log; se treinu remata, lee métrika teste, kompara ho modelu atuál, no deploy automátiku se di'ak liu. Bloku `__main__` lee komando (`info`, `deploy`, `train`) no print JSON.

**3. Fundamentu Python**

`subprocess.run(...).returncode`; `command, *args = sys.argv[1:]`; `raise SystemExit(kódigu)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 134** — `def train_and_deploy(cfg: dict) -> int:`
- **Liña 135–139** — Docstring: halai `train.py`, depois deploy automátiku checkpoint best se manán modelu atuál; pájina hahú ida ne'e separadu; stdout prosesu ne'e mak log; liña ikus "Auto-deploy: ..."; oho prosesu (Stop training) = la deploy.
- **Liña 140** — `import subprocess`.
- **Liña 141** — `argv = [cfg["python"], "-u", cfg["script"]] + (["--resume"] if cfg["resume"] else [])`.
- **Liña 142** — `code = subprocess.run(argv, cwd=cfg["cwd"], stdin=subprocess.DEVNULL, env=utf8_env()).returncode`.
- **Liña 143** — `log_text = Path(cfg["log"]).read_text(encoding="utf-8", errors="replace")`.
- **Liña 144** — `if code != 0 or "Training completed." not in log_text:`
- **Liña 145** — `print("\nAuto-deploy: skipped — training did not finish.", flush=True)`.
- **Liña 146** — `return code or 1`.
- **Liña 147** — `try:`
- **Liña 148** — `test_metrics = _test_metrics(log_text)`.
- **Liña 149** — `new = {**info(cfg["best"]), "test_metrics": test_metrics or None}`.
- **Liña 150** — `current = info(cfg["deployed"]) if Path(cfg["deployed"]).exists() else None`.
- **Liña 151** — `better, reason = is_better(new, current)`.
- **Liña 152** — `if not better:`
- **Liña 153** — `print(f"\nAuto-deploy: kept the model in use — {reason}.", flush=True)`.
- **Liña 154** — `return 0`.
- **Liña 155–156** — `deploy(cfg["best"], cfg["deployed"], cfg["backup_dir"], {"test_metrics": test_metrics or None, "deployed_by": "auto (after training)"})`.
- **Liña 157** — `print(f"\nAuto-deploy: deployed the new model — {reason}.", flush=True)`.
- **Liña 158** — `except Exception as e:`
- **Liña 159** — `print(f"\nAuto-deploy: failed, the model in use was not changed — {e}", flush=True)`.
- **Liña 160** — `return 1`.
- **Liña 161** — `return 0`.
- **Liña 164** — `if __name__ == "__main__":`
- **Liña 165–168** — Komentáriu: log no leitór JSON pájina hein UTF-8 ("—" iha liña Auto-deploy); la ho ida ne'e stdout redireksiona Windows mak cp1252.
- **Liña 169** — `for _stream in (sys.stdout, sys.stderr):`
- **Liña 170** — `if hasattr(_stream, "reconfigure"):`
- **Liña 171** — `_stream.reconfigure(encoding="utf-8")`.
- **Liña 172** — `command, *args = sys.argv[1:]`.
- **Liña 173** — `if command == "info":`
- **Liña 174** — `result = info(args[0])`.
- **Liña 175** — `elif command == "deploy":`
- **Liña 176** — `result = deploy(args[0], args[1], args[2], json.loads(args[3]))`.
- **Liña 177** — `elif command == "train":`
- **Liña 178** — `raise SystemExit(train_and_deploy(json.loads(args[0])))`.
- **Liña 179** — `else:`
- **Liña 180** — `raise SystemExit(f"unknown command {command!r}")`.
- **Liña 181** — `print(json.dumps(result))`.

**5. Walkthrough ezekusaun**

Admin klik "Train" → `python -m dashboard.model_ops train '{"python": ..., "script": "train.py", ...}'` → treinu ~oras → "Training completed." → akurásia teste 95.2 > 94.58 → deploy → "Auto-deploy: deployed the new model — test accuracy 95.20% > 94.58% ..." (*ezemplu*). Spark foti modelu foun iha fotografia tuirmai.

**6. Tipu dadus**

`cfg`: `dict` (`python`, `script`, `cwd`, `resume`, `log`, `best`, `deployed`, `backup_dir`); return `int`.

**7. Ligasaun OOP**

Not implemented in the current codebase (funsaun).

**8. Impaktu integrasaun**

Liga projetu treinu (`train.py`) ho sistema (`ml/trained_model/saved_model_cnn.pt`). Fotografia label husi agrikultór → treinu foun → modelu di'ak liu → deploy automátiku.

**9. Checkpoint ba principiante**

**P:** Saida mak akontese se admin hapara treinu iha klaran?
**R:** `train.py` la print "Training completed." → "Auto-deploy: skipped" → modelu atuál la muda.

### D.9 `config/settings.py` — konfigurasaun modelu

#### D.9.1 `IMAGE_MODEL`, `RISK_THRESHOLDS`, `ANALYSIS_MAX_PX`

**1. Kódigu orijinál**

{{code config/settings.py 50-67}}

{{code config/settings.py 96-102}}

{{code config/settings.py 126-127}}

**2. Objetivu**

Konfigurasaun modelu: dalan peze, limite konfiansa (0.5), device, batch GPU, presizaun, pin memory, thread; limite risku (0.6 / 0.3); tamañu maximu imajen analiza (1280).

**3. Fundamentu Python**

Dict ho valór husi `os.getenv(...)` konverte tipu (`float`, `int`, `.lower() == "true"`).

**4. Esplikasaun liña-ba-liña**

- **Liña 50–52** — Komentáriu seksaun modelu ML.
- **Liña 53** — `IMAGE_MODEL = {`
- **Liña 54** — Komentáriu: CNN treina halo análize hotu; la iha modelu = prosesa falla.
- **Liña 55** — `"weights_path"` — `MODEL_PATH` ka `ml/trained_model/saved_model_cnn.pt`.
- **Liña 56** — `"confidence_threshold"` — `MODEL_CONFIDENCE_THRESHOLD`, default `0.5`.
- **Liña 57** — Komentáriu: `"auto"` = GPU se PyTorch haree, se lae CPU.
- **Liña 58** — `"device"` — default `"auto"`.
- **Liña 59** — Komentáriu: velosidade: batch fotografia iha pasajen GPU ida (>1), fp16 iha GPU, memória pinned.
- **Liña 60** — `"batch_size"` — default `1` (batch hamate).
- **Liña 61** — `"batch_wait_ms"` — default `5`.
- **Liña 62** — `"precision"` — default `"fp32"`.
- **Liña 63** — `"pin_memory"` — default `false`; komentáriu: mede ona: la iha ganha.
- **Liña 64** — Komentáriu: thread CPU kada prosesu bainhira modelu iha GPU.
- **Liña 65** — `"torch_threads"` — default `1`.
- **Liña 66** — Komentáriu: klase moras mai husi arkivu modelu, nunka husi ne'e.
- **Liña 67** — `}`.
- **Liña 96–98** — Komentáriu seksaun limite risku (`ml/risk.py`).
- **Liña 99** — `RISK_THRESHOLDS = {`
- **Liña 100** — `"high": float(os.getenv("RISK_THRESHOLD_HIGH", "0.6")),`
- **Liña 101** — `"medium": float(os.getenv("RISK_THRESHOLD_MEDIUM", "0.3")),`
- **Liña 102** — `}`.
- **Liña 126** — Komentáriu: sisi naruk kópia ne'ebé Spark analiza.
- **Liña 127** — `ANALYSIS_MAX_PX = int(os.getenv("ANALYSIS_MAX_PX", "1280"))`.

**5. Walkthrough ezekusaun**

La iha variavel ambiente: limite uncertain 0.5, HIGH ≥ 0.6, MEDIUM ≥ 0.3, CPU/GPU automátiku, batch 1, FP32.

**6. Tipu dadus**

`IMAGE_MODEL`: `dict[str, object]`; `RISK_THRESHOLDS`: `dict[str, float]`; `ANALYSIS_MAX_PX`: `int`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

`ml/inference.py` lee `IMAGE_MODEL`; `ml/risk.py` lee `RISK_THRESHOLDS`; `spark/preprocessing.prepare_image` lee `ANALYSIS_MAX_PX`.

**9. Checkpoint ba principiante**

**P:** Tanba sá klase moras la define iha konfigurasaun?
**R:** Klase rai iha arkivu modelu (`class_names`); modelu foun ho klase foun funsiona kedas la ho muda konfigurasaun (komentáriu liña 66).

### D.10 `spark/preprocessing.py` — preprocessing ba CNN

#### D.10.1 `model_input`, `model_input_preview`, `_prepared_rgb`, `prepare_image`

**1. Kódigu orijinál**

{{code spark/preprocessing.py 144-185}}

**2. Objetivu**

Spark mak na'in preprocessing: imajen vertikál ≤1280 px la ho EXIF (`prepare_image`), depois tensor CNN: `Resize(S)` (sisi badak) → `CenterCrop(S)` → `ToTensor` → `Normalize(mean, std)` ho `S`, `mean`, `std` husi checkpoint (`input_spec`). Docstring: centre crop 95.67% vs squeeze 94.74%.

**3. Fundamentu Python**

`transforms.Compose([...])` (torchvision); import lazy; objetu "callable".

**4. Esplikasaun liña-ba-liña**

- **Liña 144** — `def model_input(original_bytes: bytes, prepared_bytes: bytes = None):`
- **Liña 145–146** — Docstring: tensor input CNN: hadi'ak sisi badak ba S, koto centre S×S, normaliza; centre crop di'ak liu squeeze (95.67% vs 94.74%).
- **Liña 147** — `from torchvision import transforms`.
- **Liña 148** — `spec = input_spec()` — husi checkpoint deploy.
- **Liña 149** — `size = spec["img_size"]` — 224.
- **Liña 150** — `img = _prepared_rgb(original_bytes, prepared_bytes)`.
- **Liña 151** — `to_model = transforms.Compose([`
- **Liña 152** — `transforms.Resize(size),` — sisi badak = 224 (proporsaun mantein).
- **Liña 153** — `transforms.CenterCrop(size),` — 224×224 klaran.
- **Liña 154** — `transforms.ToTensor(),` — `/255`, `[3, H, W]`.
- **Liña 155** — `transforms.Normalize(mean=spec["mean"], std=spec["std"]),` — formula A.4.1.
- **Liña 156** — `])`.
- **Liña 157** — `return to_model(img)`.
- **Liña 160** — `def model_input_preview(original_bytes: bytes, prepared_bytes: bytes = None):`
- **Liña 161** — Docstring: kuadradu ne'ebé CNN haree, hanesan imajen (molok normaliza).
- **Liña 162** — `from torchvision import transforms`.
- **Liña 163** — `size = input_spec()["img_size"]`.
- **Liña 164** — `img = _prepared_rgb(original_bytes, prepared_bytes)`.
- **Liña 165** — `return transforms.CenterCrop(size)(transforms.Resize(size)(img))`.
- **Liña 168** — `def _prepared_rgb(original_bytes, prepared_bytes=None):`
- **Liña 169** — `data = prepared_bytes if prepared_bytes is not None else prepare_image(original_bytes)`.
- **Liña 170** — `return ImageOps.exif_transpose(PILImage.open(BytesIO(data)).convert("RGB"))`.
- **Liña 173** — `def prepare_image(image_bytes: bytes) -> bytes:`
- **Liña 174–175** — Docstring: vertikál, maximu `ANALYSIS_MAX_PX`, re-encode la ho EXIF; pasu hotu hahú husi kópia ne'e; imajen la bele lee fó fali la muda.
- **Liña 176** — `try:`
- **Liña 177** — `img = PILImage.open(BytesIO(image_bytes))`.
- **Liña 178** — `img.draft("RGB", (ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))`.
- **Liña 179** — `img = ImageOps.exif_transpose(img).convert("RGB")`.
- **Liña 180** — `except Exception:`
- **Liña 181** — `return image_bytes`.
- **Liña 182** — `img.thumbnail((ANALYSIS_MAX_PX, ANALYSIS_MAX_PX))`.
- **Liña 183** — `buf = BytesIO()`.
- **Liña 184** — `img.save(buf, format="JPEG", quality=95)`.
- **Liña 185** — `return buf.getvalue()`.

**5. Walkthrough ezekusaun**

JPEG 4000×3000 (EXIF orientation 6) → `prepare_image` → 960×1280 vertikál → `Resize(224)` → 224×299 → `CenterCrop(224)` → 224×224 → `ToTensor` → `Normalize` → `[3, 224, 224]`. Kompara treinu validasaun: `A.Resize(224, 224)` → 224×224 squeeze (folha "nakdedok").

**6. Tipu dadus**

`model_input -> torch.Tensor [3, S, S]`; `model_input_preview -> PIL.Image`; `_prepared_rgb -> PIL.Image`; `prepare_image -> bytes`.

**7. Ligasaun OOP**

Komposisaun objetu torchvision (`Compose` kaer transformasaun 4).

**8. Impaktu integrasaun**

Bolu husi `spark/streaming_job.process_event` (Modulu 01); output ba `ml/inference.predict`. `_as_input` (D.12.3) kontrola forma.

**9. Checkpoint ba principiante**

**P:** Tanba sá preprocessing iha Spark no la'ós iha `ml/inference.py`?
**R:** Dezeñu responsabilidade: Spark = preprocessing + rai imajen; CNN = klasifika de'it (docstring `ml/inference.py`: "No preprocessing here"). Spark mós rai kuadradu ne'ebé CNN haree (`store_processed_image`).

### D.11 `ml/se_resnet.py` — SE-ResNet-18 (sistema)

#### D.11.1 `SEBlock` (sistema)

**1. Kódigu orijinál**

{{code ml/se_resnet.py 1-26}}

**2. Objetivu**

Versaun badak `SEBlock` hanesan treinu — naran atributu (`squeeze`, `excitation`) tenke hanesan atu peze karrega.

**3. Fundamentu Python**

Type hint `channels: int`; chaining `self.excitation(self.squeeze(x).view(b, c)).view(b, c, 1, 1)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1–3** — Docstring: SE-ResNet18, modelu ne'ebé projetu treinu treina; naran kamada tenke hanesan `model.py` treinu atu peze rai karrega.
- **Liña 4** — Remata docstring.
- **Liña 6** — `import torch`.
- **Liña 7** — `import torch.nn as nn`.
- **Liña 8** — `from torchvision import models`.
- **Liña 11** — `class SEBlock(nn.Module):`
- **Liña 12** — `def __init__(self, channels: int, reduction: int = 16):`
- **Liña 13** — `super().__init__()`.
- **Liña 14** — `reduced = max(channels // reduction, 1)`.
- **Liña 15** — `self.squeeze = nn.AdaptiveAvgPool2d(1)`.
- **Liña 16** — `self.excitation = nn.Sequential(`
- **Liña 17** — `nn.Linear(channels, reduced, bias=False),` — índise 0 iha Sequential.
- **Liña 18** — `nn.ReLU(inplace=True),` — 1.
- **Liña 19** — `nn.Linear(reduced, channels, bias=False),` — 2.
- **Liña 20** — `nn.Sigmoid(),` — 3.
- **Liña 21** — `)`.
- **Liña 23** — `def forward(self, x):`
- **Liña 24** — `b, c, _, _ = x.size()`.
- **Liña 25** — `y = self.excitation(self.squeeze(x).view(b, c)).view(b, c, 1, 1)`.
- **Liña 26** — `return x * y`.

**5. Walkthrough ezekusaun**

Hanesan D.4.1; chave `state_dict` ezemplu `layer1.0.se.excitation.0.weight` hanesan iha treinu no sistema.

**6. Tipu dadus**

Hanesan D.4.1.

**7. Ligasaun OOP**

Herança `nn.Module`; komposisaun; override `forward`. **Duplikasaun intensionál** ho treinu (projetu rua separadu) — kontratu mak naran atributu.

**8. Impaktu integrasaun**

`SEResNet18._inject_se` (sistema) harii.

**9. Checkpoint ba principiante**

**P:** Tanba sá orden kamada iha `Sequential` importante?
**R:** Chave `state_dict` uza índise (`excitation.0`, `excitation.2`); orden diferente = peze la karrega.

#### D.11.2 `SEResNet18` (sistema)

**1. Kódigu orijinál**

{{code ml/se_resnet.py 29-60}}

**2. Objetivu**

SE-ResNet-18 sistema: la ho peze ImageNet (peze mai husi checkpoint), SE injeta iha layer 4, `forward` badak.

**3. Fundamentu Python**

`@staticmethod`; atribuisaun múltipla `self.a, self.b = x, y`; funsaun aninhada.

**4. Esplikasaun liña-ba-liña**

- **Liña 29** — `class SEResNet18(nn.Module):`
- **Liña 30** — `def __init__(self, num_classes: int):`
- **Liña 31** — `super().__init__()`.
- **Liña 32** — `base = models.resnet18(weights=None)`.
- **Liña 33** — `self.conv1, self.bn1, self.relu, self.maxpool = base.conv1, base.bn1, base.relu, base.maxpool`.
- **Liña 34** — `self.layer1 = self._inject_se(base.layer1)`.
- **Liña 35** — `self.layer2 = self._inject_se(base.layer2)`.
- **Liña 36** — `self.layer3 = self._inject_se(base.layer3)`.
- **Liña 37** — `self.layer4 = self._inject_se(base.layer4)`.
- **Liña 38** — `self.avgpool = base.avgpool`.
- **Liña 39** — `self.fc = nn.Linear(base.fc.in_features, num_classes)`.
- **Liña 41** — `@staticmethod`.
- **Liña 42** — `def _inject_se(layer):`
- **Liña 43** — `for block in layer:`
- **Liña 44** — `block.se = SEBlock(block.conv2.out_channels)`.
- **Liña 46** — `def forward_with_se(x, block=block):`
- **Liña 47** — `identity = x`.
- **Liña 48** — `out = block.relu(block.bn1(block.conv1(x)))`.
- **Liña 49** — `out = block.se(block.bn2(block.conv2(out)))`.
- **Liña 50** — `if block.downsample is not None:`
- **Liña 51** — `identity = block.downsample(x)`.
- **Liña 52** — `return block.relu(out + identity)`.
- **Liña 54** — `block.forward = forward_with_se`.
- **Liña 55** — `return layer` — (treinu fó `nn.Sequential(*blocks)` foun; rezultadu hanesan).
- **Liña 57** — `def forward(self, x):`
- **Liña 58** — `x = self.maxpool(self.relu(self.bn1(self.conv1(x))))`.
- **Liña 59** — `x = self.layer4(self.layer3(self.layer2(self.layer1(x))))`.
- **Liña 60** — `return self.fc(torch.flatten(self.avgpool(x), 1))`.

**5. Walkthrough ezekusaun**

`SEResNet18(num_classes=4)` → `load_state_dict(checkpoint["state_dict"])` → modelu hanesan treinu.

**6. Tipu dadus**

`num_classes`: `int`; `forward -> Tensor [B, num_classes]`.

**7. Ligasaun OOP**

`@staticmethod` — métodu la uza `self` (diferente husi treinu ne'ebé uza métodu instánsia). Komposisaun, monkey patching hanesan D.4.3.

**8. Impaktu integrasaun**

`ml/inference._load_checkpoint` (architecture `se_resnet18`) no `model_ops.deploy` harii.

**9. Checkpoint ba principiante**

**P:** Tanba sá `weights=None` iha sistema?
**R:** Peze tomak mai husi checkpoint treina; peze ImageNet la presiza (no evita download iha laptop sistema).
