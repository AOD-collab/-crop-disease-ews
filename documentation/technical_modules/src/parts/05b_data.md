
## D. Esplikasaun Kódigu-ba-Kódigu

### D.1 Download dadus

#### D.1.1 `download.py` — konfigurasaun no PlantDoc

**1. Kódigu orijinál**

{{code @train/download.py 1-33}}

**2. Objetivu**

Download dataset PlantDoc husi Kaggle (`kagglehub`) no kopia ba `data/plantdoc`.

**3. Fundamentu Python**

`os.path.join` harii dalan; `os.makedirs(..., exist_ok=True)`; `shutil.copytree(src, dst, dirs_exist_ok=True)` kopia pasta tomak.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import os`.
- **Liña 2** — `import shutil` — kopia pasta.
- **Liña 3** — `import kagglehub` — download dataset Kaggle.
- **Liña 6** — `DATA_DIR = "data"`.
- **Liña 8** — `PLANTDOC_DEST = os.path.join(DATA_DIR, "plantdoc")`.
- **Liña 9** — `PLANTVILLAGE_DEST = os.path.join(DATA_DIR, "plantvillage")`.
- **Liña 12** — `def download_datasets():`
- **Liña 14** — `os.makedirs(DATA_DIR, exist_ok=True)`.
- **Liña 16–18** — Komentáriu seksaun PlantDoc.
- **Liña 19** — `print("\n--- Downloading PlantDoc ---")`.
- **Liña 21–23** — `plantdoc_path = kagglehub.dataset_download("abdulhasibuddin/plant-doc-dataset")` — download (cache) no fó dalan.
- **Liña 25** — `print(f"PlantDoc downloaded to: {plantdoc_path}")`.
- **Liña 27** — `if not os.path.exists(PLANTDOC_DEST):` — se seidauk kopia:
- **Liña 28–32** — `shutil.copytree(plantdoc_path, PLANTDOC_DEST, dirs_exist_ok=True)`.

**5. Walkthrough ezekusaun**

Dala uluk: download → `~/.cache/kagglehub/...` → kopia ba `data/plantdoc`. Dala rua: `data/plantdoc` iha → la kopia.

**6. Tipu dadus**

`DATA_DIR`, `PLANTDOC_DEST`, `PLANTVILLAGE_DEST`, `plantdoc_path`: `str`.

**7. Ligasaun OOP**

Not implemented in the current codebase. (Script funsaun.)

**8. Impaktu integrasaun**

`train.DATASET_DIRS` uza `data/plantdoc`.

**9. Checkpoint ba principiante**

**P:** Tanba sá kontrola `os.path.exists(PLANTDOC_DEST)`?
**R:** Atu la kopia dataset dala rua (neineik no la presiza).

#### D.1.2 `download.py` — PlantVillage no `__main__`

**1. Kódigu orijinál**

{{code @train/download.py 34-64}}

**2. Objetivu**

Download PlantVillage, kopia, hatudu sumáriu; bloku `__main__` kaer erru.

**3. Fundamentu Python**

`try/except Exception as e` iha `__main__`.

**4. Esplikasaun liña-ba-liña**

- **Liña 34–36** — Komentáriu seksaun PlantVillage.
- **Liña 37** — `print("\n--- Downloading PlantVillage ---")`.
- **Liña 39–41** — `plantvillage_path = kagglehub.dataset_download("emware/plantvillage-diseased-dataset")`.
- **Liña 43** — `print(...)`.
- **Liña 45** — `if not os.path.exists(PLANTVILLAGE_DEST):`
- **Liña 46–50** — `shutil.copytree(...)`.
- **Liña 52** — `print("\nDatasets retrieved successfully!")`.
- **Liña 54** — `print(f"PlantDoc:     {PLANTDOC_DEST}")`.
- **Liña 55** — `print(f"PlantVillage: {PLANTVILLAGE_DEST}")`.
- **Liña 58** — `if __name__ == "__main__":`
- **Liña 59** — `try:`
- **Liña 60** — `download_datasets()`.
- **Liña 62** — `except Exception as e:`
- **Liña 63** — `print("\nError downloading datasets:")`.
- **Liña 64** — `print(e)`.

**5. Walkthrough ezekusaun**

`python download.py` → pasta rua iha `data/`. Internet la iha → "Error downloading datasets:" + erru.

**6. Tipu dadus**

`plantvillage_path`: `str`; `e`: `Exception`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

`train.DATASET_DIRS` uza `"data/plantvillage/plantvillage dataset/color"` de'it (komentáriu train.py liña 56–57: grayscale/segmented mak kópia folha hanesan — sei vaza entre split).

**9. Checkpoint ba principiante**

**P:** Tanba sá treinu la uza pasta `segmented` PlantVillage?
**R:** Imajen `segmented` mak folha hanesan ho `color` (fundu hasai). Se kópia ida iha treinu no ida iha teste, akurásia teste sei aas falsu (data leakage). (`gradcam.py` uza `segmented` só hanesan máskara.)

#### D.1.3 `Downloading.py`

**1. Kódigu orijinál**

{{code @train/Downloading.py 1-6}}

**2. Objetivu**

Download dataset "corn-or-maize-leaf-disease-dataset" (versaun foun liu) no hatudu dalan.

**3. Fundamentu Python**

Script nível modulu (la iha funsaun).

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import kagglehub`.
- **Liña 3** — Komentáriu: download versaun foun liu.
- **Liña 4** — `path = kagglehub.dataset_download("smaranjitghose/corn-or-maize-leaf-disease-dataset")`.
- **Liña 6** — `print("Path to dataset files:", path)`.

**5. Walkthrough ezekusaun**

Fó dalan cache. Kódigu la kopia automátiku; `train.DATASET_DIRS` hatudu `data/corn_maize` (komentáriu: imajen ne'ebé la iha ona iha PlantDoc/PlantVillage) — Inferidu: kopia/filtra ho liman.

**6. Tipu dadus**

`path`: `str`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

`data/corn_maize` mós uza iha `augment.FIELD_HEALTHY_DIR` (fotografia kampu healthy).

**9. Checkpoint ba principiante**

**P:** Script ne'e kopia dadus ba `data/`?
**R:** Lae; nia só download no print dalan.

### D.2 `dataset.py` — imajen, label no Albumentations

#### D.2.1 Import, `IMAGE_EXTENSIONS`, `CLASS_NAMES`, `normalize_label`

**1. Kódigu orijinál**

{{code @train/dataset.py 1-44}}

**2. Objetivu**

Import biblioteka; define estensaun imajen aseita, naran klase 4 (orden = índise label), no funsaun normaliza naran pasta.

**3. Fundamentu Python**

`str.lower()`, `str.replace()`; tuple `IMAGE_EXTENSIONS` (bele uza iha `str.endswith(tuple)`).

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import os`.
- **Liña 2** — `import cv2` — OpenCV.
- **Liña 3** — `import numpy as np`.
- **Liña 5** — `from torch.utils.data import Dataset`.
- **Liña 7** — `import albumentations as A`.
- **Liña 8** — `from albumentations.pytorch import ToTensorV2` — array → tensor PyTorch.
- **Liña 11–13** — Komentáriu seksaun konfigurasaun.
- **Liña 15** — `IMAGE_EXTENSIONS = (`
- **Liña 16** — `".jpg",`
- **Liña 17** — `".jpeg",`
- **Liña 18** — `".png",`
- **Liña 19** — `".bmp",`
- **Liña 20** — `".webp"`
- **Liña 21** — `)`.
- **Liña 24** — Komentáriu: muda tuir klase projetu.
- **Liña 25** — `CLASS_NAMES = [`
- **Liña 26** — `"Healthy",` — índise 0.
- **Liña 27** — `"Blight",` — 1.
- **Liña 28** — `"Common_Rust",` — 2.
- **Liña 29** — `"Gray_Leaf_Spot"` — 3.
- **Liña 30** — Komentáriu: `"Pest_Damage"` — tau fali bainhira iha imajen peste.
- **Liña 31** — `]`.
- **Liña 34–36** — Komentáriu seksaun normalizasaun label.
- **Liña 38** — `def normalize_label(label):`
- **Liña 40** — `label = label.lower()`.
- **Liña 41** — `label = label.replace("-", "_")`.
- **Liña 42** — `label = label.replace(" ", "_")`.
- **Liña 44** — `return label`.

**5. Walkthrough ezekusaun**

`normalize_label("Northern Leaf-Blight")` → `"northern_leaf_blight"`.

**6. Tipu dadus**

`IMAGE_EXTENSIONS`: `tuple[str, ...]`; `CLASS_NAMES`: `list[str]`; `normalize_label(str) -> str`.

**7. Ligasaun OOP**

Not implemented in the current codebase (bloku ne'e).

**8. Impaktu integrasaun**

`CLASS_NAMES` rai iha checkpoint (`"class_names"`); `model_ops.CLASS_NAME_MAP` konverte ba naran sistema.

**9. Checkpoint ba principiante**

**P:** Tanba sá orden `CLASS_NAMES` importante?
**R:** Label mak índise iha lista (`Healthy` = 0...). Modelu fó probabilidade tuir orden ne'e; muda orden = interpretasaun sala.

#### D.2.2 `CLASS_ALIASES` no `find_images`

**1. Kódigu orijinál**

{{code @train/dataset.py 47-123}}

**2. Objetivu**

Mapa naran pasta husi dataset diferente ba klase 4. `find_images` la'o iha pasta hotu (`os.walk`), foti imajen ne'ebé naran pasta inan iha `CLASS_ALIASES` no klase iha `CLASS_NAMES`, no fó lista dalan + label.

**3. Fundamentu Python**

- **`os.walk(dir)`** — fó `(root, dirs, files)` ba kada pasta (rekursivu).
- **`continue`** — salta.
- **`list.index(x)`** — pozisaun.

**4. Esplikasaun liña-ba-liña**

- **Liña 47–49** — Komentáriu seksaun aliases.
- **Liña 51** — `CLASS_ALIASES = {`
- **Liña 53** — `"healthy": "Healthy",`
- **Liña 55** — `"blight": "Blight",`
- **Liña 56** — `"northern_leaf_blight": "Blight",`
- **Liña 57** — `"northern_leaf_blight_maize": "Blight",`
- **Liña 59** — `"common_rust": "Common_Rust",`
- **Liña 60** — `"common_rust_maize": "Common_Rust",`
- **Liña 61** — `"rust": "Common_Rust",`
- **Liña 63** — `"gray_leaf_spot": "Gray_Leaf_Spot",`
- **Liña 64** — `"grey_leaf_spot": "Gray_Leaf_Spot",`
- **Liña 66** — `"pest_damage": "Pest_Damage",`
- **Liña 67** — `"pest": "Pest_Damage",`
- **Liña 68** — `"insect_damage": "Pest_Damage"` — klase ne'e la iha `CLASS_NAMES`, entaun salta.
- **Liña 69** — `}`.
- **Liña 72–74** — Komentáriu seksaun buka imajen.
- **Liña 76** — `def find_images(dataset_directories):`
- **Liña 78** — `image_paths = []`.
- **Liña 79** — `labels = []`.
- **Liña 81** — `for dataset_dir in dataset_directories:`
- **Liña 83** — `if not os.path.exists(dataset_dir):`
- **Liña 85–88** — `print(f"WARNING: Dataset directory does not exist: {dataset_dir}")`.
- **Liña 90** — `continue`.
- **Liña 92** — `print(f"\nScanning: {dataset_dir}")`.
- **Liña 94** — `for root, _, files in os.walk(dataset_dir):`
- **Liña 96** — `for file in files:`
- **Liña 98** — `if not file.lower().endswith(IMAGE_EXTENSIONS):`
- **Liña 99** — `continue`.
- **Liña 101** — `image_path = os.path.join(root, file)`.
- **Liña 103** — Komentáriu: uza pasta inan hanesan label.
- **Liña 104** — `parent_folder = os.path.basename(root)`.
- **Liña 106** — `normalized = normalize_label(parent_folder)`.
- **Liña 108** — `if normalized not in CLASS_ALIASES:`
- **Liña 109** — `continue`.
- **Liña 111** — `class_name = CLASS_ALIASES[normalized]`.
- **Liña 113** — `if class_name not in CLASS_NAMES:`
- **Liña 114** — `continue`.
- **Liña 116** — `label = CLASS_NAMES.index(class_name)`.
- **Liña 118** — `image_paths.append(image_path)`.
- **Liña 119** — `labels.append(label)`.
- **Liña 121** — `print(f"\nFound {len(image_paths)} usable images.")`.
- **Liña 123** — `return image_paths, labels`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `data/plantdoc/train/Corn rust leaf/x.jpg` → pasta `"Corn rust leaf"` → `"corn_rust_leaf"` → la iha aliases → salta. `data/corn_maize/Common_Rust/y.jpg` → `"common_rust"` → `"Common_Rust"` → label 2.

**6. Tipu dadus**

`CLASS_ALIASES`: `dict[str, str]`; `find_images(list[str]) -> tuple[list[str], list[int]]`.

**7. Ligasaun OOP**

Not implemented in the current codebase.

**8. Impaktu integrasaun**

`train.main`, `augment.main`, `gradcam.main` bolu `find_images(DATASET_DIRS)`; `train.main` mós bolu ho `["data/train"]` (label admin).

**9. Checkpoint ba principiante**

**P:** Saida mak label ba imajen iha pasta `Grey_Leaf_Spot`?
**R:** `normalize_label` → `"grey_leaf_spot"` → alias `"Gray_Leaf_Spot"` → índise 3.

#### D.2.3 `get_transforms` — augmentation treinu (Albumentations)

**1. Kódigu orijinál**

{{code @train/dataset.py 126-178}}

**2. Objetivu**

Pipeline augmentation **online** ba treinu (kada epoch imajen hetan transformasaun aleatóriu foun): koto/hadi'ak aleatóriu, flip, rotasaun, shift/scale/rotate, kór, sombra, sharpen.

**3. Fundamentu Python**

`A.Compose([...])` — lista transformasaun; kada ida ho `p` (probabilidade aplika).

**4. Esplikasaun liña-ba-liña**

- **Liña 126–128** — Komentáriu seksaun augmentation.
- **Liña 130** — `def get_transforms(img_size=224, is_train=True):`
- **Liña 132** — `if is_train:`
- **Liña 134** — `return A.Compose([`
- **Liña 136** — Komentáriu: augmentation espasiál.
- **Liña 137–141** — `A.RandomResizedCrop(size=(img_size, img_size), scale=(0.7, 1.0), p=1.0)` — koto área 70–100% no hadi'ak ba 224×224 (sempre).
- **Liña 143** — `A.HorizontalFlip(p=0.5)`.
- **Liña 145** — `A.VerticalFlip(p=0.5)`.
- **Liña 147** — `A.RandomRotate90(p=0.5)` — 0/90/180/270°.
- **Liña 149–154** — `A.ShiftScaleRotate(shift_limit=0.1, scale_limit=0.1, rotate_limit=30, p=0.5)` — muda ±10%, eskala ±10%, roda ±30°.
- **Liña 156** — Komentáriu: kondisaun lus.
- **Liña 157–163** — `A.ColorJitter(brightness=0.3, contrast=0.3, saturation=0.3, hue=0.15, p=0.7)`.
- **Liña 165** — Komentáriu: sombra iha liur.
- **Liña 166–171** — `A.RandomShadow(num_shadows_limit=(1, 3), shadow_dimension=5, shadow_intensity_range=(0.3, 0.7), p=0.5)`.
- **Liña 173** — Komentáriu: sharpen.
- **Liña 174–178** — `A.Sharpen(alpha=(0.2, 0.5), lightness=(0.5, 1.0), p=0.3)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* imajen 256×256 → koto 200×200 → 224×224 → flip horizontal (50%) → roda 90° (50%) → kór (70%) → sombra (50%)... Kada epoch imajen hanesan sai diferente — modelu aprende variasaun.

**6. Tipu dadus**

`img_size`: `int`; `is_train`: `bool`; return `A.Compose`.

**7. Ligasaun OOP**

Kada `A.X(...)` mak **objetu** klase transformasaun Albumentations; `A.Compose` kaer lista objetu (**komposisaun**, padraun *Pipeline*). Objetu `Compose` bele bolu (`transform(image=img)`) — iha `__call__`.

**8. Impaktu integrasaun**

`PlantDiseaseDataset.__getitem__` aplika; `train.main` harii ho `is_train=True` ba treinu.

**9. Checkpoint ba principiante**

**P:** Diferensa augmentation **online** (ne'e) no **offline** (`augment.py`)?
**R:** Online: transformasaun aleatóriu foun kada vez imajen lee (la rai). Offline: kópia rai iha disku dala ida (aumenta dataset).

#### D.2.4 `get_transforms` — blur/noise, normalize, validasaun

**1. Kódigu orijinál**

{{code @train/dataset.py 180-222}}

**2. Objetivu**

Termina pipeline treinu (blur/ruídu, normalizasaun ImageNet, tensor) no define pipeline validasaun/teste (Resize + Normalize + tensor, la aleatóriu).

**3. Fundamentu Python**

`A.OneOf([...], p=0.4)` — hili transformasaun **ida** husi lista (ho probabilidade 40%).

**4. Esplikasaun liña-ba-liña**

- **Liña 180** — Komentáriu: blur/ruídu.
- **Liña 181** — `A.OneOf([`
- **Liña 182–184** — `A.GaussianBlur(blur_limit=(3, 7)),`
- **Liña 186–188** — `A.MotionBlur(blur_limit=(3, 7)),` — blur movimentu (liman nakdoko).
- **Liña 190–192** — `A.GaussNoise(std_range=(0.01, 0.05))` — ruídu.
- **Liña 194** — `], p=0.4),`
- **Liña 196** — Komentáriu: normalizasaun ImageNet.
- **Liña 197–200** — `A.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])` — formula A.4.1 (Albumentations divide ho 255 uluk).
- **Liña 202** — `ToTensorV2()` — `[H, W, 3]` → tensor `[3, H, W]`.
- **Liña 204** — `])`.
- **Liña 206** — `else:` — validasaun/teste.
- **Liña 208** — `return A.Compose([`
- **Liña 210–213** — `A.Resize(height=img_size, width=img_size)` — hadi'ak direta ba 224×224 (proporsaun la mantein).
- **Liña 215–218** — `A.Normalize(mean=[...], std=[...])`.
- **Liña 220** — `ToTensorV2()`.
- **Liña 222** — `])`.

**5. Walkthrough ezekusaun**

Imajen validasaun 256×192 → `Resize` 224×224 (squeeze) → normaliza → tensor `[3, 224, 224]`. Piksel R=153 → $(153/255 - 0.485)/0.229 \approx 0.502$.

**6. Tipu dadus**

Return `A.Compose`; output `transform(image=img)["image"]` = `torch.Tensor` float32 `[3, 224, 224]`.

**7. Ligasaun OOP**

`OneOf` mós objetu ne'ebé kaer objetu seluk (komposisaun aninhada).

**8. Impaktu integrasaun**

`predict.DiseasePredictor` no `gradcam.main` uza `is_train=False`. Sistema (Spark) la uza Albumentations — uza torchvision `Resize + CenterCrop` (D.10.1).

**9. Checkpoint ba principiante**

**P:** Tanba sá validasaun la uza augmentation aleatóriu?
**R:** Avaliasaun tenke konsistente: imajen hanesan tenke fó rezultadu hanesan kada epoch atu kompara modelu.

#### D.2.5 Klase `PlantDiseaseDataset`

**1. Kódigu orijinál**

{{code @train/dataset.py 225-273}}

**2. Objetivu**

Dataset PyTorch: rai lista dalan + label + transformasaun; `__getitem__` lee imajen, konverte BGR → RGB, aplika transformasaun, fó `(tensor, label)`.

**3. Fundamentu Python**

- **Herança** `class PlantDiseaseDataset(Dataset)`.
- **`__len__`** no **`__getitem__`** — protokolu sekuénsia.

**4. Esplikasaun liña-ba-liña**

- **Liña 225–227** — Komentáriu seksaun dataset PyTorch.
- **Liña 229** — `class PlantDiseaseDataset(Dataset):`
- **Liña 231–236** — `def __init__(self, image_paths, labels, transform=None):`
- **Liña 238** — `self.image_paths = image_paths`.
- **Liña 239** — `self.labels = labels`.
- **Liña 240** — `self.transform = transform`.
- **Liña 242** — `def __len__(self):`
- **Liña 244** — `return len(self.image_paths)`.
- **Liña 246** — `def __getitem__(self, idx):`
- **Liña 248** — `image_path = self.image_paths[idx]`.
- **Liña 250** — `image = cv2.imread(image_path)` — BGR `uint8` ka `None`.
- **Liña 252** — `if image is None:`
- **Liña 254–256** — `raise RuntimeError(f"Could not read image: {image_path}")`.
- **Liña 258–261** — `image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)`.
- **Liña 263** — `label = self.labels[idx]`.
- **Liña 265** — `if self.transform:`
- **Liña 267–269** — `augmented = self.transform(image=image)`.
- **Liña 271** — `image = augmented["image"]`.
- **Liña 273** — `return image, label`.

**5. Walkthrough ezekusaun**

`ds = PlantDiseaseDataset(["a.jpg"], [1], get_transforms(224, False))`; `len(ds)` → 1; `ds[0]` → `(tensor[3,224,224], 1)`. `DataLoader` bolu `ds[i]` ba 32 índise → batch `[32, 3, 224, 224]` + label `[32]`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `self.image_paths` | `list[str]` |
| `self.labels` | `numpy.ndarray` ka `list[int]` |
| `self.transform` | `A.Compose` ka `None` |
| `__getitem__` return | `tuple[torch.Tensor, int]` |

**7. Ligasaun OOP**

- **Herança implementadu**: `PlantDiseaseDataset(Dataset)`.
- **Polimorfizmu (override)**: `__len__`, `__getitem__` substitui métodu abstratu `Dataset` — `DataLoader` uza objetu la hatene klase espesífiku.
- **`__init__` / `self`**: atributu instánsia 3.
- **Komposisaun**: objetu kaer objetu `transform`.
- **Enkapsulamentu**: lójika lee/transforma subar iha `__getitem__`.

**8. Impaktu integrasaun**

`train.main` harii objetu 3 (train/val/test); `DataLoader` uza.

**9. Checkpoint ba principiante**

**P:** Tanba sá `cv2.cvtColor(..., BGR2RGB)` presiza?
**R:** OpenCV lee kór ho orden BGR; modelu ImageNet (no normalizasaun `mean/std`) hein RGB.
