
#### D.5.9 `main` — seed, pasta, buka imajen, split, augmented, revizaun

**1. Kódigu orijinál**

{{code @train/train.py 593-692}}

**2. Objetivu**

Prepara treinu: seed, pasta, device, buka imajen (erru se zero), split, aumenta kópia augmented no fotografia agrikultór label (treinu de'it), hatudu tamañu split.

**3. Fundamentu Python**

Unpacking tuple 6 ho parénteze; `np.concatenate([a, b])`; `list(a) + b`.

**4. Esplikasaun liña-ba-liña**

- **Liña 593–595** — Komentáriu seksaun main.
- **Liña 597** — `def main():`
- **Liña 599** — `set_seed(SEED)`.
- **Liña 601–604** — `os.makedirs(CHECKPOINT_DIR, exist_ok=True)`.
- **Liña 606–609** — `os.makedirs(RESULTS_DIR, exist_ok=True)`.
- **Liña 611** — `device = get_device()`.
- **Liña 613–615** — Komentáriu: buka imajen.
- **Liña 617** — `print("\n--- Searching datasets ---")`.
- **Liña 619–621** — `image_paths, labels = find_images(DATASET_DIRS)`.
- **Liña 623** — `if len(image_paths) == 0:`
- **Liña 625–629** — `raise RuntimeError("\nNo usable images found.\nCheck your dataset directories and CLASS_ALIASES in dataset.py.")`.
- **Liña 631** — `labels = np.array(labels)`.
- **Liña 633–635** — Komentáriu: split.
- **Liña 637–641** — `(train_paths, train_labels, val_paths, val_labels, test_paths, test_labels) = split_data(image_paths, labels)`.
- **Liña 643–645** — Komentáriu: aumenta kópia offline (treinu de'it).
- **Liña 647–649** — `aug_paths, aug_labels = load_augmented(train_paths)`.
- **Liña 651** — `train_paths = list(train_paths) + aug_paths`.
- **Liña 653–656** — `train_labels = np.concatenate([train_labels, np.array(aug_labels, dtype=train_labels.dtype)])`.
- **Liña 658–660** — Komentáriu: aumenta fotografia agrikultór revizaun admin (treinu de'it).
- **Liña 662** — `if os.path.exists(REVIEWED_DIR):`
- **Liña 664–666** — `reviewed_paths, reviewed_labels = find_images([REVIEWED_DIR])`.
- **Liña 668** — `train_paths = list(train_paths) + reviewed_paths`.
- **Liña 670–673** — `train_labels = np.concatenate([train_labels, np.array(reviewed_labels, dtype=train_labels.dtype)])`.
- **Liña 675–678** — `print(f"Reviewed farmer photos added: {len(reviewed_paths)}")`.
- **Liña 680** — `print("\nDataset split:")`.
- **Liña 682–684** — `print(f"Training:   {len(train_paths)}")`.
- **Liña 686–688** — `print(f"Validation: {len(val_paths)}")`.
- **Liña 690–692** — `print(f"Testing:    {len(test_paths)}")`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* imajen 4300 → treinu 3010 + augmented 3500 + revizaun 40 = 6550; validasaun 645; teste 646 (loloos).

**6. Tipu dadus**

`image_paths`: `list[str]`; `labels`, `train_labels`: `ndarray`; `device`: `torch.device`.

**7. Ligasaun OOP**

Not implemented in the current codebase (bloku ne'e).

**8. Impaktu integrasaun**

Fotografia revizaun mai husi pájina "Label photos" sistema (`LABELLED_TRAIN_DIR`) — siklu feedback agrikultór → modelu.

**9. Checkpoint ba principiante**

**P:** Tanba sá fotografia revizaun la tama validasaun/teste?
**R:** Atu akurásia teste kompara entre run: teste hela hanesan (komentáriu liña 65–67).

#### D.5.10 Dataset objetu no DataLoader

**1. Kódigu orijinál**

{{code @train/train.py 694-762}}

**2. Objetivu**

Harii objetu `PlantDiseaseDataset` 3 (transformasaun treinu vs validasaun/teste), sampler balansu, no `DataLoader` 3.

**3. Fundamentu Python**

Argumentu ho naran; `pin_memory=device.type == "cuda"` (bool husi komparasaun); `persistent_workers=NUM_WORKERS > 0`.

**4. Esplikasaun liña-ba-liña**

- **Liña 694–696** — Komentáriu: objetu dataset.
- **Liña 698–705** — `train_dataset = PlantDiseaseDataset(train_paths, train_labels, transform=get_transforms(IMG_SIZE, is_train=True))`.
- **Liña 707–714** — `val_dataset = PlantDiseaseDataset(val_paths, val_labels, transform=get_transforms(IMG_SIZE, is_train=False))`.
- **Liña 716–723** — `test_dataset = PlantDiseaseDataset(test_paths, test_labels, transform=get_transforms(IMG_SIZE, is_train=False))`.
- **Liña 725–727** — Komentáriu: sampler balansu.
- **Liña 729–731** — `train_sampler = create_weighted_sampler(train_labels)`.
- **Liña 733–735** — Komentáriu: DataLoader.
- **Liña 737–744** — `train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, sampler=train_sampler, num_workers=NUM_WORKERS, pin_memory=..., persistent_workers=...)`.
- **Liña 746–753** — `val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False, ...)`.
- **Liña 755–762** — `test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE, shuffle=False, ...)`.

**5. Walkthrough ezekusaun**

`for images, labels in train_loader:` → kada iterasaun batch `[32, 3, 224, 224]` (prosesu 4 lee imajen paralelu).

**6. Tipu dadus**

`*_dataset`: `PlantDiseaseDataset`; `train_sampler`: `WeightedRandomSampler`; `*_loader`: `DataLoader`.

**7. Ligasaun OOP**

Harii **objetu** husi klase projetu (`PlantDiseaseDataset`) no biblioteka (`DataLoader`). `DataLoader` **kaer** dataset no sampler (komposisaun/agregasaun).

**8. Impaktu integrasaun**

Loader 3 uza iha loop treinu (D.5.13) no teste (D.5.14).

**9. Checkpoint ba principiante**

**P:** Tanba sá `shuffle=False` ba validasaun no teste?
**R:** Orden la importa ba avaliasaun; orden fixu halo rezultadu reprodutivel.

#### D.5.11 Modelu, loss, optimizer, scheduler, scaler, istória

**1. Kódigu orijinál**

{{code @train/train.py 764-839}}

**2. Objetivu**

Harii modelu (ImageNet pretrained) iha device, loss cross-entropy, optimizer AdamW, scheduler cosine, scaler AMP, dict istória, no dalan checkpoint (best + last).

**3. Fundamentu Python**

`model.parameters()` — parámetru hotu ne'ebé bele aprende; dict ho lista mamuk.

**4. Esplikasaun liña-ba-liña**

- **Liña 764–766** — Komentáriu: modelu.
- **Liña 768** — `print("\n--- Creating SE-ResNet18 ---")`.
- **Liña 770–773** — `model = SEResNet18(num_classes=len(CLASS_NAMES), pretrained=True)`.
- **Liña 775** — `model = model.to(device)`.
- **Liña 777–779** — Komentáriu: loss.
- **Liña 781** — `criterion = nn.CrossEntropyLoss()` — softmax + log-likelihood negativu (A.4.6).
- **Liña 783–785** — Komentáriu: optimizer.
- **Liña 787–791** — `optimizer = torch.optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)` (A.4.7).
- **Liña 793–795** — Komentáriu: scheduler.
- **Liña 797–800** — `scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=EPOCHS)` (A.4.8).
- **Liña 802–804** — Komentáriu: AMP scaler.
- **Liña 806–809** — `scaler = torch.amp.GradScaler("cuda", enabled=device.type == "cuda")` — CPU: hamate.
- **Liña 811–813** — Komentáriu: istória treinu.
- **Liña 815** — `history = {`
- **Liña 817** — `"train_loss": [],`
- **Liña 818** — `"val_loss": [],`
- **Liña 820** — `"train_accuracy": [],`
- **Liña 821** — `"val_accuracy": []`
- **Liña 822** — `}`.
- **Liña 824** — `best_val_accuracy = 0.0`.
- **Liña 826** — `epochs_without_improvement = 0`.
- **Liña 828–831** — `best_model_path = os.path.join(CHECKPOINT_DIR, "best_seresnet18_crop_ews.pth")`.
- **Liña 833** — Komentáriu: estadu treinu tomak, rai kada epoch ba `--resume`.
- **Liña 834–837** — `last_model_path = os.path.join(CHECKPOINT_DIR, "last_seresnet18_crop_ews.pth")`.
- **Liña 839** — `start_epoch = 0`.

**5. Walkthrough ezekusaun**

Learning rate tuir cosine: epoch 0 → 3e-4; epoch 12.5 → 1.5e-4; epoch 25 → ~0.

**6. Tipu dadus**

`model`: `SEResNet18`; `criterion`: `CrossEntropyLoss`; `optimizer`: `AdamW`; `scheduler`: `CosineAnnealingLR`; `scaler`: `GradScaler`; `history`: `dict[str, list[float]]`.

**7. Ligasaun OOP**

**Komposisaun objetu**: `optimizer` kaer referénsia ba parámetru modelu; `scheduler` kaer `optimizer` — objetu kolabora (scheduler muda `lr` optimizer).

**8. Impaktu integrasaun**

Checkpoint `best_...pth` mak input ba `model_ops.deploy`.

**9. Checkpoint ba principiante**

**P:** Tanba sá rai checkpoint rua (best no last)?
**R:** `best` = modelu di'ak liu (ba deploy). `last` = estadu tomak epoch ikus (ba kontinua treinu ho `--resume`).

#### D.5.12 Resume (`--resume`)

**1. Kódigu orijinál**

{{code @train/train.py 841-895}}

**2. Objetivu**

Se `--resume`, karrega estadu tomak husi `last_...pth` (modelu, optimizer, scheduler, istória, best, epoch). Se só iha `best_...pth` tuan, karrega peze no avansa scheduler. Hatudu inísiu treinu.

**3. Fundamentu Python**

`"--resume" in sys.argv`; `torch.load(path, map_location=device)`; `model.load_state_dict(...)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 841–843** — Komentáriu: resume (`python train.py --resume`).
- **Liña 845** — `if "--resume" in sys.argv:`
- **Liña 847** — `if os.path.exists(last_model_path):`
- **Liña 849–852** — `state = torch.load(last_model_path, map_location=device)`.
- **Liña 854** — `model.load_state_dict(state["model_state_dict"])`.
- **Liña 855** — `optimizer.load_state_dict(state["optimizer_state_dict"])`.
- **Liña 856** — `scheduler.load_state_dict(state["scheduler_state_dict"])`.
- **Liña 858** — `history = state["history"]`.
- **Liña 859** — `best_val_accuracy = state["best_val_accuracy"]`.
- **Liña 860–862** — `epochs_without_improvement = state["epochs_without_improvement"]`.
- **Liña 863** — `start_epoch = state["epoch"]`.
- **Liña 865** — `elif os.path.exists(best_model_path):`
- **Liña 867–869** — Komentáriu: checkpoint tuan iha peze de'it: hahú fali husi epoch di'ak liu ho optimizer foun no scheduler avansa.
- **Liña 870–873** — `state = torch.load(best_model_path, map_location=device)`.
- **Liña 875** — `model.load_state_dict(state["model_state_dict"])`.
- **Liña 877** — `best_val_accuracy = state["val_accuracy"]`.
- **Liña 878** — `start_epoch = state["epoch"]`.
- **Liña 880** — `for _ in range(start_epoch):`
- **Liña 881** — `scheduler.step()`.
- **Liña 883** — `else:`
- **Liña 885** — `print("\nNo checkpoint to resume from.")`.
- **Liña 887–891** — `print(f"\nResuming after epoch {start_epoch} (best val accuracy {best_val_accuracy * 100:.2f}%)")`.
- **Liña 893** — `print("\n======================================")`.
- **Liña 894** — `print("STARTING TRAINING")`.
- **Liña 895** — `print("======================================")`.

**5. Walkthrough ezekusaun**

Treinu para iha epoch 12 (laptop mate) → `python train.py --resume` → karrega estadu → loop hahú husi epoch 13.

**6. Tipu dadus**

`state`: `dict`; `start_epoch`: `int`.

**7. Ligasaun OOP**

`load_state_dict` mak métodu objetu (modelu, optimizer, scheduler) — **polimorfizmu**: objetu diferente iha métodu ho naran hanesan.

**8. Impaktu integrasaun**

`model_ops.train_and_deploy` haruka `--resume` se konfigurasaun husu (`cfg["resume"]`).

**9. Checkpoint ba principiante**

**P:** Tanba sá avansa `scheduler.step()` iha checkpoint tuan?
**R:** Atu learning rate hanesan ho epoch ne'ebé treinu para (cosine depende ba epoch).

#### D.5.13 Loop treinu, rai modelu di'ak liu, early stopping

**1. Kódigu orijinál**

{{code @train/train.py 897-1020}}

**2. Objetivu**

Kada epoch: treina, avalia validasaun, scheduler step, rai istória, print métrika, rai `best` se val accuracy aas liu, rai `last` beibeik, para se la di'ak liu durante `PATIENCE` epoch.

**3. Fundamentu Python**

`for epoch in range(start_epoch, EPOCHS)`; `torch.save(dict, path)`; `break`.

**4. Esplikasaun liña-ba-liña**

- **Liña 897–899** — Komentáriu: loop treinu.
- **Liña 901** — `for epoch in range(start_epoch, EPOCHS):`
- **Liña 903–906** — `print(f"\nEpoch {epoch + 1}/{EPOCHS}")`.
- **Liña 908–915** — `train_loss, train_accuracy = train_one_epoch(model, train_loader, optimizer, criterion, scaler, device)`.
- **Liña 917–922** — `val_loss, val_accuracy, _, _ = evaluate(model, val_loader, criterion, device)`.
- **Liña 924** — `scheduler.step()`.
- **Liña 926–928** — `history["train_loss"].append(train_loss)`.
- **Liña 930–932** — `history["val_loss"].append(val_loss)`.
- **Liña 934–936** — `history["train_accuracy"].append(train_accuracy)`.
- **Liña 938–940** — `history["val_accuracy"].append(val_accuracy)`.
- **Liña 942–945** — `print(f"Train Loss: {train_loss:.4f}")`.
- **Liña 947–950** — `print(f"Train Accuracy: {train_accuracy * 100:.2f}%")`.
- **Liña 952–955** — `print(f"Validation Loss: {val_loss:.4f}")`.
- **Liña 957–960** — `print(f"Validation Accuracy: {val_accuracy * 100:.2f}%")`.
- **Liña 962–964** — Komentáriu: rai modelu di'ak liu.
- **Liña 966** — `if val_accuracy > best_val_accuracy:`
- **Liña 968** — `best_val_accuracy = val_accuracy`.
- **Liña 970** — `epochs_without_improvement = 0`.
- **Liña 972–985** — `torch.save({"model_state_dict": model.state_dict(), "class_names": CLASS_NAMES, "img_size": IMG_SIZE, "val_accuracy": val_accuracy, "epoch": epoch + 1}, best_model_path)` — checkpoint ho metadata.
- **Liña 987–989** — `print("✓ Best model saved.")`.
- **Liña 991** — `else:`
- **Liña 993** — `epochs_without_improvement += 1`.
- **Liña 995–1008** — `torch.save({"model_state_dict": ..., "optimizer_state_dict": ..., "scheduler_state_dict": ..., "history": history, "best_val_accuracy": ..., "epochs_without_improvement": ..., "epoch": epoch + 1}, last_model_path)`.
- **Liña 1010–1012** — Komentáriu: early stopping.
- **Liña 1014** — `if epochs_without_improvement >= PATIENCE:`
- **Liña 1016–1018** — `print("\nEarly stopping triggered.")`.
- **Liña 1020** — `break`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* epoch 1 val 0.85 → best; epoch 2 0.90 → best; ... epoch 15 0.9598 → best; epoch 16–25 la di'ak liu → epoch 25 (EPOCHS) remata (ka early stop se PATIENCE to'o molok).

**6. Tipu dadus**

`train_loss`, `val_loss`: `float`; `train_accuracy`, `val_accuracy`: `float` (0–1); checkpoint: `dict`.

**7. Ligasaun OOP**

`model.state_dict()` — métodu ne'ebé fó dict parámetru (naran atributu → tensor). Naran sira ne'e (`layer1.0.se.excitation.0.weight`...) tenke hanesan iha `ml/se_resnet.py`.

**8. Impaktu integrasaun**

Linha print ("Train Loss: ...", "Validation Accuracy: ...") lee husi pájina dashboard "Model training" (log).

**9. Checkpoint ba principiante**

**P:** Bainhira mak `best_seresnet18_crop_ews.pth` atualiza?
**R:** De'it bainhira akurásia validasaun epoch ne'e **aas liu** best anterior.

#### D.5.14 Karrega best, teste final, métrika

**1. Kódigu orijinál**

{{code @train/train.py 1022-1115}}

**2. Objetivu**

Rai gráfiku istória, karrega modelu best, avalia iha split teste, kalkula precision/recall/F1 *weighted* (formula A.4.10), print.

**3. Fundamentu Python**

`precision_score(targets, predictions, average="weighted", zero_division=0)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1022–1024** — Komentáriu: gráfiku istória.
- **Liña 1026–1029** — `plot_history(history, RESULTS_DIR)`.
- **Liña 1031–1033** — Komentáriu: karrega modelu best.
- **Liña 1035–1037** — `print("\nLoading best model...")`.
- **Liña 1039–1042** — `checkpoint = torch.load(best_model_path, map_location=device)`.
- **Liña 1044–1046** — `model.load_state_dict(checkpoint["model_state_dict"])`.
- **Liña 1048–1050** — Komentáriu: teste final.
- **Liña 1052–1054** — `print("\n======================================")`.
- **Liña 1056–1058** — `print("FINAL TEST RESULTS")`.
- **Liña 1060–1062** — `print("======================================")`.
- **Liña 1064–1069** — `test_loss, test_accuracy, targets, predictions = evaluate(model, test_loader, criterion, device)`.
- **Liña 1071–1076** — `precision = precision_score(targets, predictions, average="weighted", zero_division=0)`.
- **Liña 1078–1083** — `recall = recall_score(...)`.
- **Liña 1085–1090** — `f1 = f1_score(...)`.
- **Liña 1092–1095** — `print(f"\nTest Loss: {test_loss:.4f}")`.
- **Liña 1097–1100** — `print(f"Test Accuracy: {test_accuracy * 100:.2f}%")`.
- **Liña 1102–1105** — `print(f"Precision: {precision * 100:.2f}%")`.
- **Liña 1107–1110** — `print(f"Recall: {recall * 100:.2f}%")`.
- **Liña 1112–1115** — `print(f"F1-Score: {f1 * 100:.2f}%")`.

**5. Walkthrough ezekusaun**

Loloos (relatóriu 2026-10-03): akurásia teste 0.95, weighted precision/recall/F1 0.95 (arredonda 2 desimál). Kálkulu F1 Gray_Leaf_Spot: $2 \cdot 0.86 \cdot 0.86 / (0.86 + 0.86) = 0.86$.

**6. Tipu dadus**

`test_loss`, `test_accuracy`, `precision`, `recall`, `f1`: `float`; `targets`, `predictions`: `list[int]`.

**7. Ligasaun OOP**

Not implemented in the current codebase (funsaun sklearn).

**8. Impaktu integrasaun**

`model_ops._test_metrics` lee liña "Test Accuracy: ...", "Precision: ..." husi log atu deside auto-deploy.

**9. Checkpoint ba principiante**

**P:** Tanba sá avalia ho modelu **best** no la'ós modelu epoch ikus?
**R:** Modelu ikus bele overfit; best mak ida ne'ebé di'ak liu iha validasaun — rezultadu teste representa modelu ne'ebé sei deploy.

#### D.5.15 Classification report, matriz konfuzaun, remata

**1. Kódigu orijinál**

{{code @train/train.py 1117-1167}}

**2. Objetivu**

Print no rai `classification_report.txt` (precision/recall/F1/support kada klase), rai matriz konfuzaun, print liña remata ("Training completed.").

**3. Fundamentu Python**

`with open(path, "w") as file: file.write(report)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1117–1119** — Komentáriu: classification report.
- **Liña 1121** — `print("\nClassification Report:")`.
- **Liña 1123–1129** — `report = classification_report(targets, predictions, labels=range(len(CLASS_NAMES)), target_names=CLASS_NAMES, zero_division=0)`.
- **Liña 1131** — `print(report)`.
- **Liña 1133–1139** — `with open(os.path.join(RESULTS_DIR, "classification_report.txt"), "w") as file:`
- **Liña 1141** — `file.write(report)`.
- **Liña 1143–1145** — Komentáriu: matriz konfuzaun.
- **Liña 1147–1151** — `save_confusion_matrix(targets, predictions, RESULTS_DIR)`.
- **Liña 1153–1155** — `print("\nTraining completed.")` — `model_ops` buka testu ne'e.
- **Liña 1157–1159** — `print(f"Best model: {best_model_path}")`.
- **Liña 1161–1163** — `print(f"Results: {RESULTS_DIR}")`.
- **Liña 1166** — `if __name__ == "__main__":`
- **Liña 1167** — `main()`.

**5. Walkthrough ezekusaun**

Haree tabela B (relatóriu loloos).

**6. Tipu dadus**

`report`: `str`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

`model_ops.train_and_deploy` kontrola `"Training completed." in log_text` molok deploy.

**9. Checkpoint ba principiante**

**P:** Saida mak "support" iha relatóriu?
**R:** Númeru imajen teste kada klase (ezemplu Gray_Leaf_Spot 86).
