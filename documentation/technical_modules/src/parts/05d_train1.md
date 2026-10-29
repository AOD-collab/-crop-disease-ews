
### D.5 `train.py` — treinu no avaliasaun

#### D.5.1 Import no konfigurasaun (hiperparámetru)

**1. Kódigu orijinál**

{{code @train/train.py 1-81}}

**2. Objetivu**

Import biblioteka treinu/métrika no define hiperparámetru: `SEED=42`, `IMG_SIZE=224`, `BATCH_SIZE=32`, `EPOCHS=25`, `LEARNING_RATE=3e-4`, `WEIGHT_DECAY=1e-2`, `NUM_WORKERS=4`, `PATIENCE=10`; pasta dataset, revizaun, augmented, checkpoint, rezultadu.

**3. Fundamentu Python**

Notasaun sientífika `3e-4` = 0.0003. Konstante MAIÚSKULU.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import csv`.
- **Liña 2** — `import os`.
- **Liña 3** — `import random`.
- **Liña 4** — `import sys` — `sys.argv` ba `--resume`.
- **Liña 6** — `import numpy as np`.
- **Liña 7** — `import matplotlib.pyplot as plt` — gráfiku.
- **Liña 9** — `import torch`.
- **Liña 10** — `import torch.nn as nn`.
- **Liña 12** — `from torch.utils.data import DataLoader`.
- **Liña 13** — `from torch.utils.data import WeightedRandomSampler`.
- **Liña 15** — `from sklearn.model_selection import train_test_split`.
- **Liña 17–24** — Métrika sklearn: `accuracy_score`, `precision_score`, `recall_score`, `f1_score`, `classification_report`, `confusion_matrix`.
- **Liña 26–31** — Husi `dataset`: `find_images`, `PlantDiseaseDataset`, `get_transforms`, `CLASS_NAMES`.
- **Liña 33** — `from model import SEResNet18`.
- **Liña 36–38** — Komentáriu seksaun konfigurasaun.
- **Liña 40** — `SEED = 42`.
- **Liña 42** — `IMG_SIZE = 224`.
- **Liña 44** — `BATCH_SIZE = 32`.
- **Liña 46** — `EPOCHS = 25`.
- **Liña 48** — `LEARNING_RATE = 3e-4`.
- **Liña 50** — `WEIGHT_DECAY = 1e-2`.
- **Liña 52** — `NUM_WORKERS = 4` — prosesu paralelu lee imajen.
- **Liña 54** — `PATIENCE = 10` — early stopping.
- **Liña 56–57** — Komentáriu: PlantVillage "color" de'it; "grayscale" no "segmented" mak kópia folha hanesan no sei vaza entre split.
- **Liña 58** — `DATASET_DIRS = [`
- **Liña 59** — `"data/plantdoc",`
- **Liña 60** — `"data/plantvillage/plantvillage dataset/color",`
- **Liña 61** — Komentáriu: só imajen ne'ebé la iha ona iha PlantDoc/PlantVillage.
- **Liña 62** — `"data/corn_maize"`
- **Liña 63** — `]`.
- **Liña 65–67** — Komentáriu: fotografia agrikultór ne'ebé admin label iha dashboard (`data/train/<class>/`); split treinu de'it, nunka val/test, atu akurásia teste kompara entre run.
- **Liña 68** — `REVIEWED_DIR = "data/train"`.
- **Liña 70** — Komentáriu: kópia augmented husi `augment.py` (split treinu de'it).
- **Liña 71** — `AUGMENTED_DIR = "data/augmented"`.
- **Liña 73–76** — `AUGMENTED_MANIFEST = os.path.join(AUGMENTED_DIR, "manifest.csv")`.
- **Liña 78** — `CHECKPOINT_DIR = "checkpoints"`.
- **Liña 80** — `RESULTS_DIR = "results"`.

**5. Walkthrough ezekusaun**

Import `train` husi `augment.py`/`gradcam.py` halai de'it liña 1–81 no definisaun funsaun (la treina, tanba `main()` iha `__main__`).

**6. Tipu dadus**

Hiperparámetru `int`/`float`; `DATASET_DIRS`: `list[str]`; pasta: `str`.

**7. Ligasaun OOP**

Not implemented in the current codebase (bloku ne'e).

**8. Impaktu integrasaun**

`REVIEWED_DIR` = `config.settings.LABELLED_TRAIN_DIR` (sistema hakerek fotografia label ba ne'e). `IMG_SIZE` rai iha checkpoint → `input_spec` sistema.

**9. Checkpoint ba principiante**

**P:** Saida mak `PATIENCE = 10`?
**R:** Se akurásia validasaun la di'ak liu durante epoch 10 tuir malu, treinu para (early stopping).

#### D.5.2 `set_seed`, `get_device`, `split_data`

**1. Kódigu orijinál**

{{code @train/train.py 83-156}}

**2. Objetivu**

Seed ba reprodutibilidade; hili GPU/CPU; fahe dadus 70/15/15 estratifikadu.

**3. Fundamentu Python**

`train_test_split(..., stratify=labels)` — proporsaun klase hanesan iha split hotu. Return tuple 6.

**4. Esplikasaun liña-ba-liña**

- **Liña 83–85** — Komentáriu seksaun seed.
- **Liña 87** — `def set_seed(seed):`
- **Liña 89** — `random.seed(seed)`.
- **Liña 91** — `np.random.seed(seed)`.
- **Liña 93** — `torch.manual_seed(seed)`.
- **Liña 95** — `torch.cuda.manual_seed_all(seed)`.
- **Liña 98–100** — Komentáriu seksaun device.
- **Liña 102** — `def get_device():`
- **Liña 104** — `if torch.cuda.is_available():`
- **Liña 106** — `device = torch.device("cuda")`.
- **Liña 108** — `print("\nGPU detected:")`.
- **Liña 109–111** — `print(torch.cuda.get_device_name(0))`.
- **Liña 113** — `else:`
- **Liña 115** — `device = torch.device("cpu")`.
- **Liña 117** — `print("\nCUDA GPU not detected.")`.
- **Liña 118** — `print("Training on CPU.")`.
- **Liña 120** — `return device`.
- **Liña 123–125** — Komentáriu seksaun split.
- **Liña 127** — `def split_data(image_paths, labels):`
- **Liña 129–130** — Komentáriu: 70/15/15 estratifikadu; `augment.py` uza funsaun hanesan atu kópia mai husi treinu de'it.
- **Liña 132–140** — `train_paths, temp_paths, train_labels, temp_labels = train_test_split(image_paths, labels, test_size=0.30, random_state=SEED, stratify=labels)` — 70% treinu, 30% temp.
- **Liña 142–150** — `val_paths, test_paths, val_labels, test_labels = train_test_split(temp_paths, temp_labels, test_size=0.50, random_state=SEED, stratify=temp_labels)` — temp fahe 15% / 15%.
- **Liña 152–156** — `return (train_paths, train_labels, val_paths, val_labels, test_paths, test_labels)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* imajen 4300 → treinu ~3010, validasaun ~645, teste ~645. (Relatóriu loloos: teste 646.)

**6. Tipu dadus**

`get_device() -> torch.device`; `split_data(list, ndarray) -> tuple` (lista dalan + array label × 3).

**7. Ligasaun OOP**

`torch.device` mak objetu. Not implemented in the current codebase: klase konfigurasaun treinu.

**8. Impaktu integrasaun**

`augment.py` no `gradcam.py` import `split_data` — split hanesan iha fatin hotu (seed 42).

**9. Checkpoint ba principiante**

**P:** Tanba sá `stratify`?
**R:** Atu kada split iha proporsaun klase hanesan (ezemplu Gray_Leaf_Spot uitoan iha treinu, validasaun no teste hotu).

#### D.5.3 `load_augmented`

**1. Kódigu orijinál**

{{code @train/train.py 159-213}}

**2. Objetivu**

Lee `manifest.csv` no aumenta kópia augmented ba treinu — só se imajen fonte iha split treinu, klase válidu, no arkivu iha. Protesaun kontra data leakage.

**3. Fundamentu Python**

Set comprehension; `csv.DictReader`; kondisaun `or` naruk iha parénteze.

**4. Esplikasaun liña-ba-liña**

- **Liña 159–161** — Komentáriu seksaun.
- **Liña 163** — `def load_augmented(train_paths):`
- **Liña 165** — `if not os.path.exists(AUGMENTED_MANIFEST):`
- **Liña 167–171** — `print(...)` — la iha dadus augmented; halai `augment.py`.
- **Liña 173** — `return [], []`.
- **Liña 175–178** — `train_set = {os.path.normpath(p) for p in train_paths}`.
- **Liña 180** — `aug_paths = []`.
- **Liña 182** — `aug_labels = []`.
- **Liña 184** — `skipped = 0`.
- **Liña 186** — `with open(AUGMENTED_MANIFEST, newline="") as file:`
- **Liña 188** — `for row in csv.DictReader(file):`
- **Liña 190** — Komentáriu: nunka husik kópia imajen val/test tama treinu.
- **Liña 191–196** — `if (normpath(row["source_path"]) not in train_set or row["class_name"] not in CLASS_NAMES or not os.path.exists(row["aug_path"])):`
- **Liña 198** — `skipped += 1`.
- **Liña 200** — `continue`.
- **Liña 202** — `aug_paths.append(row["aug_path"])`.
- **Liña 204–206** — `aug_labels.append(CLASS_NAMES.index(row["class_name"]))`.
- **Liña 208–211** — `print(f"\nAugmented copies added to training: {len(aug_paths)} (skipped {skipped})")`.
- **Liña 213** — `return aug_paths, aug_labels`.

**5. Walkthrough ezekusaun**

Manifest liña 3500; 20 ho fonte ne'ebé agora iha validasaun (split muda) → `skipped = 20`.

**6. Tipu dadus**

Return `tuple[list[str], list[int]]`; `train_set`: `set[str]`.

**7. Ligasaun OOP**

`csv.DictReader` — objetu iteravel.

**8. Impaktu integrasaun**

`main` bolu depois split.

**9. Checkpoint ba principiante**

**P:** Saida mak "data leakage"?
**R:** Bainhira informasaun husi dadus teste tama iha treinu (ezemplu kópia imajen teste) — akurásia teste sai aas falsu.

#### D.5.4 `create_weighted_sampler`

**1. Kódigu orijinál**

{{code @train/train.py 216-266}}

**2. Objetivu**

Sampler ne'ebé foti imajen klase ki'ik dala barak liu: peze kada imajen = $1/n_k$ (formula A.4.9).

**3. Fundamentu Python**

`np.bincount(labels, minlength=K)` — konta kada label; `np.zeros(K, dtype=np.float64)`; list comprehension; `torch.DoubleTensor`.

**4. Esplikasaun liña-ba-liña**

- **Liña 216–218** — Komentáriu seksaun sampler balansu.
- **Liña 220** — `def create_weighted_sampler(labels):`
- **Liña 222** — `labels = np.array(labels)`.
- **Liña 224–227** — `class_counts = np.bincount(labels, minlength=len(CLASS_NAMES))`.
- **Liña 229** — `print("\nClass distribution:")`.
- **Liña 231** — `for i, class_name in enumerate(CLASS_NAMES):`
- **Liña 233–236** — `print(f"{class_name}: {class_counts[i]}")`.
- **Liña 238–241** — `class_weights = np.zeros(len(CLASS_NAMES), dtype=np.float64)`.
- **Liña 243** — `for i in range(len(CLASS_NAMES)):`
- **Liña 245** — `if class_counts[i] > 0:` — evita divizaun ho zero.
- **Liña 247–249** — `class_weights[i] = (1.0 / class_counts[i])`.
- **Liña 251–256** — `sample_weights = np.array([class_weights[label] for label in labels])`.
- **Liña 258–264** — `sampler = WeightedRandomSampler(weights=torch.DoubleTensor(sample_weights), num_samples=len(sample_weights), replacement=True)` — foti ho reposisaun.
- **Liña 266** — `return sampler`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* klase `[1000, 1000, 1000, 400]` → peze `[0.001, 0.001, 0.001, 0.0025]` → klase ikus foti ~2.5× liu kada imajen → kada epoch klase hotu aparese ~hanesan.

**6. Tipu dadus**

`class_counts`: `ndarray[int]`; `class_weights`, `sample_weights`: `ndarray[float64]`; return `WeightedRandomSampler`.

**7. Ligasaun OOP**

Harii objetu `WeightedRandomSampler` (klase PyTorch) ne'ebé `DataLoader` uza.

**8. Impaktu integrasaun**

`main` haruka ba `DataLoader(..., sampler=train_sampler)`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `replacement=True`?
**R:** Atu imajen klase ki'ik bele foti dala barak iha epoch ida (sein ida ne'e, la bele balansu).

#### D.5.5 `train_one_epoch`

**1. Kódigu orijinál**

{{code @train/train.py 269-356}}

**2. Objetivu**

Treina epoch ida: kada batch → forward → loss (cross-entropy) → backward ho AMP scaler → optimizer step; akumula loss no prediksaun; fó loss média no akurásia.

**3. Fundamentu Python**

- **`model.train()`** — modu treinu (dropout/batchnorm aprende).
- **`optimizer.zero_grad(set_to_none=True)`** — hamoos gradiente.
- **`with torch.amp.autocast(...)`** — presizaun mista (FP16) iha GPU.
- **`scaler.scale(loss).backward()`** — backpropagation ho eskala (evita underflow FP16).
- **`loss.item()`** — tensor → `float`.
- **`torch.argmax(outputs, dim=1)`** — klase ho score boot liu.
- **`.detach().cpu().numpy()`** — tensor GPU → array NumPy.

**4. Esplikasaun liña-ba-liña**

- **Liña 269–271** — Komentáriu seksaun treina epoch ida.
- **Liña 273–280** — `def train_one_epoch(model, dataloader, optimizer, criterion, scaler, device):`
- **Liña 282** — `model.train()`.
- **Liña 284** — `running_loss = 0.0`.
- **Liña 286** — `predictions = []`.
- **Liña 288** — `targets = []`.
- **Liña 290** — `for images, labels in dataloader:` — batch `[32, 3, 224, 224]`.
- **Liña 292–295** — `images = images.to(device, non_blocking=True)`.
- **Liña 297–300** — `labels = labels.to(device, non_blocking=True)`.
- **Liña 302–304** — `optimizer.zero_grad(set_to_none=True)`.
- **Liña 306–309** — `with torch.amp.autocast(device_type=device.type, enabled=device.type == "cuda"):`
- **Liña 311** — `outputs = model(images)` — logits `[32, 4]`.
- **Liña 313–316** — `loss = criterion(outputs, labels)` — formula A.4.6.
- **Liña 318** — `scaler.scale(loss).backward()` — kalkula gradiente.
- **Liña 320** — `scaler.step(optimizer)` — atualiza peze (AdamW).
- **Liña 322** — `scaler.update()`.
- **Liña 324–327** — `running_loss += (loss.item() * images.size(0))` — loss total (média × imajen).
- **Liña 329–332** — `preds = torch.argmax(outputs, dim=1)`.
- **Liña 334–338** — `predictions.extend(preds.detach().cpu().numpy())`.
- **Liña 340–344** — `targets.extend(labels.detach().cpu().numpy())`.
- **Liña 346–349** — `epoch_loss = (running_loss / len(dataloader.dataset))`.
- **Liña 351–354** — `epoch_accuracy = accuracy_score(targets, predictions)`.
- **Liña 356** — `return epoch_loss, epoch_accuracy`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* batch ida: label loos `[1, 0, 2, ...]`, logits → softmax → probabilidade klase loos média 0.7 → loss ≈ $-\log 0.7 = 0.357$; gradiente → peze muda uitoan. Epoch remata: `(0.31, 0.89)`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `model` | `SEResNet18` |
| `dataloader` | `DataLoader` |
| `optimizer` | `torch.optim.AdamW` |
| `criterion` | `nn.CrossEntropyLoss` |
| `scaler` | `torch.amp.GradScaler` |
| `device` | `torch.device` |
| return | `tuple[float, float]` |

**7. Ligasaun OOP**

Funsaun ne'e kolabora ho objetu 5 (modelu, loader, optimizer, criterion, scaler) — sira hotu objetu PyTorch ho métodu (`model.train()`, `optimizer.zero_grad()`, `scaler.step()`). `criterion(outputs, labels)` — objetu "callable".

**8. Impaktu integrasaun**

`main` bolu kada epoch.

**9. Checkpoint ba principiante**

**P:** Tanba sá `optimizer.zero_grad()` kada batch?
**R:** PyTorch soma gradiente automátiku; la hamoos, gradiente batch anterior sei kahur.

#### D.5.6 `evaluate`

**1. Kódigu orijinál**

{{code @train/train.py 359-432}}

**2. Objetivu**

Avalia modelu (validasaun ka teste) la ho treina: loss média, akurásia, label loos no prediksaun.

**3. Fundamentu Python**

`model.eval()` — modu avaliasaun (batchnorm uza statístika rai); `with torch.no_grad():` — la kalkula gradiente.

**4. Esplikasaun liña-ba-liña**

- **Liña 359–361** — Komentáriu seksaun validasaun.
- **Liña 363–368** — `def evaluate(model, dataloader, criterion, device):`
- **Liña 370** — `model.eval()`.
- **Liña 372** — `running_loss = 0.0`.
- **Liña 374** — `predictions = []`.
- **Liña 376** — `targets = []`.
- **Liña 378** — `with torch.no_grad():`
- **Liña 380** — `for images, labels in dataloader:`
- **Liña 382–385** — `images = images.to(device, non_blocking=True)`.
- **Liña 387–390** — `labels = labels.to(device, non_blocking=True)`.
- **Liña 392–395** — `with torch.amp.autocast(device_type=device.type, enabled=device.type == "cuda"):`
- **Liña 397** — `outputs = model(images)`.
- **Liña 399–402** — `loss = criterion(outputs, labels)`.
- **Liña 404–407** — `running_loss += (loss.item() * images.size(0))`.
- **Liña 409–412** — `preds = torch.argmax(outputs, dim=1)`.
- **Liña 414–416** — `predictions.extend(preds.cpu().numpy())`.
- **Liña 418–420** — `targets.extend(labels.cpu().numpy())`.
- **Liña 422–425** — `loss = (running_loss / len(dataloader.dataset))`.
- **Liña 427–430** — `accuracy = accuracy_score(targets, predictions)`.
- **Liña 432** — `return loss, accuracy, targets, predictions`.

**5. Walkthrough ezekusaun**

Validasaun 645 imajen → `(0.15, 0.96, [...], [...])` (*ezemplu ilustrativu*).

**6. Tipu dadus**

Return `tuple[float, float, list[int], list[int]]`.

**7. Ligasaun OOP**

Hanesan D.5.5.

**8. Impaktu integrasaun**

`main` bolu ba validasaun kada epoch no ba teste ikus.

**9. Checkpoint ba principiante**

**P:** Diferensa `model.train()` no `model.eval()`?
**R:** `train()` halo BatchNorm atualiza statístika no Dropout ativu; `eval()` uza statístika rai no hamate Dropout — rezultadu estável.

#### D.5.7 `plot_history`

**1. Kódigu orijinál**

{{code @train/train.py 435-523}}

**2. Objetivu**

Rai gráfiku loss no akurásia (treinu vs validasaun) kada epoch: `loss_curve.png`, `accuracy_curve.png`.

**3. Fundamentu Python**

Matplotlib: `plt.figure()`, `plt.plot(x, y, label=...)`, `plt.legend()`, `plt.savefig()`, `plt.close()`.

**4. Esplikasaun liña-ba-liña**

- **Liña 435–437** — Komentáriu seksaun.
- **Liña 439–442** — `def plot_history(history, output_dir):`
- **Liña 444–447** — `epochs = range(1, len(history["train_loss"]) + 1)`.
- **Liña 449–451** — Komentáriu loss.
- **Liña 453** — `plt.figure()`.
- **Liña 455–459** — `plt.plot(epochs, history["train_loss"], label="Train Loss")`.
- **Liña 461–465** — `plt.plot(epochs, history["val_loss"], label="Validation Loss")`.
- **Liña 467** — `plt.xlabel("Epoch")`.
- **Liña 469** — `plt.ylabel("Loss")`.
- **Liña 471** — `plt.title("Training and Validation Loss")`.
- **Liña 473** — `plt.legend()`.
- **Liña 475** — `plt.tight_layout()`.
- **Liña 477–482** — `plt.savefig(os.path.join(output_dir, "loss_curve.png"))`.
- **Liña 484** — `plt.close()`.
- **Liña 486–488** — Komentáriu akurásia.
- **Liña 490** — `plt.figure()`.
- **Liña 492–496** — `plt.plot(epochs, history["train_accuracy"], label="Train Accuracy")`.
- **Liña 498–502** — `plt.plot(epochs, history["val_accuracy"], label="Validation Accuracy")`.
- **Liña 504** — `plt.xlabel("Epoch")`.
- **Liña 506** — `plt.ylabel("Accuracy")`.
- **Liña 508–510** — `plt.title("Training and Validation Accuracy")`.
- **Liña 512** — `plt.legend()`.
- **Liña 514** — `plt.tight_layout()`.
- **Liña 516–521** — `plt.savefig(os.path.join(output_dir, "accuracy_curve.png"))`.
- **Liña 523** — `plt.close()`.

**5. Walkthrough ezekusaun**

Epoch 25 → gráfiku 2 iha `results/`. Se val loss sa'e maibé train loss tun → overfitting.

**6. Tipu dadus**

`history`: `dict[str, list[float]]`; `output_dir`: `str`; return `None`.

**7. Ligasaun OOP**

Matplotlib API "state machine" (`plt.*`) — la uza objetu esplísitu.

**8. Impaktu integrasaun**

Gráfiku hatudu iha relatóriu/dashboard "Model training".

**9. Checkpoint ba principiante**

**P:** Oinsá gráfiku ajuda hatene overfitting?
**R:** Bainhira loss treinu kontinua tun maibé loss validasaun sa'e, modelu "hanoin" dadus treinu duke aprende jenerál.

#### D.5.8 `save_confusion_matrix`

**1. Kódigu orijinál**

{{code @train/train.py 526-590}}

**2. Objetivu**

Kalkula no rai matriz konfuzaun (liña = klase loos, koluna = prediksaun) ho númeru iha kada kaixa.

**3. Fundamentu Python**

`confusion_matrix(targets, predictions, labels=range(K))`; `cm.shape`; loop aninhadu `for i ... for j ...`.

**4. Esplikasaun liña-ba-liña**

- **Liña 526–528** — Komentáriu seksaun.
- **Liña 530–534** — `def save_confusion_matrix(targets, predictions, output_dir):`
- **Liña 536–540** — `cm = confusion_matrix(targets, predictions, labels=range(len(CLASS_NAMES)))`.
- **Liña 542–544** — `plt.figure(figsize=(8, 6))`.
- **Liña 546** — `plt.imshow(cm)`.
- **Liña 548** — `plt.title("Confusion Matrix")`.
- **Liña 550** — `plt.colorbar()`.
- **Liña 552–557** — `plt.xticks(range(len(CLASS_NAMES)), CLASS_NAMES, rotation=45, ha="right")`.
- **Liña 559–562** — `plt.yticks(range(len(CLASS_NAMES)), CLASS_NAMES)`.
- **Liña 564** — `plt.xlabel("Predicted")`.
- **Liña 566** — `plt.ylabel("Actual")`.
- **Liña 568** — Komentáriu: hakerek valór.
- **Liña 569** — `for i in range(cm.shape[0]):`
- **Liña 571** — `for j in range(cm.shape[1]):`
- **Liña 573–579** — `plt.text(j, i, str(cm[i, j]), ha="center", va="center")`.
- **Liña 581** — `plt.tight_layout()`.
- **Liña 583–588** — `plt.savefig(os.path.join(output_dir, "confusion_matrix.png"))`.
- **Liña 590** — `plt.close()`.

**5. Walkthrough ezekusaun**

Husi relatóriu loloos: Gray_Leaf_Spot support 86, recall 0.86 → ~74 loos, ~12 sala (Inferidu; konfuzaun ho Blight posível — numeru loloos iha `confusion_matrix.png`).

**6. Tipu dadus**

`cm`: `ndarray[int]` `[4, 4]`; return `None`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

`main` bolu depois teste.

**9. Checkpoint ba principiante**

**P:** Saida mak diagonál matriz konfuzaun?
**R:** Prediksaun loos (klase loos = klase prediksaun). Fora diagonál = erru.
