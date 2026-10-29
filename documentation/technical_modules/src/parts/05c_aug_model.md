
### D.3 `augment.py` — augmentation offline

#### D.3.1 Import no konfigurasaun

**1. Kódigu orijinál**

{{code @train/augment.py 1-42}}

**2. Objetivu**

Import biblioteka no reuza konfigurasaun husi `train.py` (`DATASET_DIRS`, pasta augmented, `SEED`, `split_data`) atu kópia augmentation mai **só** husi split treinu. Define kuantidade kópia no regra ba healthy kampu no grayscale.

**3. Fundamentu Python**

`import csv`, `hashlib` (MD5 ba naran úniku). Import husi script seluk (`from train import ...`).

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import csv` — hakerek manifest.
- **Liña 2** — `import hashlib`.
- **Liña 3** — `import os`.
- **Liña 4** — `import shutil`.
- **Liña 6** — `import cv2`.
- **Liña 7** — `import numpy as np`.
- **Liña 9** — `import albumentations as A`.
- **Liña 11–14** — `from dataset import (find_images, CLASS_NAMES)`.
- **Liña 16–22** — `from train import (DATASET_DIRS, AUGMENTED_DIR, AUGMENTED_MANIFEST, SEED, split_data)`.
- **Liña 25–27** — Komentáriu seksaun konfigurasaun.
- **Liña 29** — Komentáriu: kópia kada imajen treinu (1 = dataset dobru).
- **Liña 30** — `COPIES_PER_IMAGE = 1`.
- **Liña 32–33** — Komentáriu: fotografia kampu batar healthy uitoan, entaun kada ida hetan rotasaun/flip 8 iha kór plus kópia grayscale ekstra.
- **Liña 34** — `FIELD_HEALTHY_DIR = os.path.normpath("data/corn_maize")`.
- **Liña 35** — `FIELD_HEALTHY_GRAY_COPIES = 2`.
- **Liña 37–38** — Komentáriu: kópia regulár sai grayscale ho probabilidade ne'e, atu grayscale la'ós sinál Healthy de'it (kópia healthy kampu 2/10 gray).
- **Liña 39** — `GRAYSCALE_PROBABILITY = 0.15`.
- **Liña 41** — `JPEG_QUALITY = 95`.

**5. Walkthrough ezekusaun**

Imajen treinu 3000 → kópia ~3000 (1 kada ida) + kópia ekstra ba healthy kampu (10 kada ida: 8 rotasaun + 2 gray).

**6. Tipu dadus**

`COPIES_PER_IMAGE`, `FIELD_HEALTHY_GRAY_COPIES`, `JPEG_QUALITY`: `int`; `GRAYSCALE_PROBABILITY`: `float`; `FIELD_HEALTHY_DIR`: `str`.

**7. Ligasaun OOP**

Not implemented in the current codebase (bloku ne'e).

**8. Impaktu integrasaun**

Reuza `split_data` husi `train.py` garante split hanesan (SEED 42).

**9. Checkpoint ba principiante**

**P:** Tanba sá kópia grayscale mós ba klase seluk?
**R:** Se só healthy iha grayscale, modelu bele aprende "grayscale = healthy" (atalhu sala). Grayscale 15% ba hotu evita ida ne'e.

#### D.3.2 `OFFLINE_TRANSFORM`, `rng`, `to_grayscale`, `dihedral`

**1. Kódigu orijinál**

{{code @train/augment.py 44-107}}

**2. Objetivu**

Pipeline Albumentations ba kópia offline (mantein tamañu orijinál), jeradór aleatóriu ho seed, konversaun grayscale, no transformasaun "dihedral" 8 (rotasaun 90° × flip).

**3. Fundamentu Python**

`np.random.default_rng(SEED)` — jeradór aleatóriu reprodutivel; `np.rot90`, `np.fliplr`; `np.ascontiguousarray` (memória kontínua ba OpenCV); `%` (resto).

**4. Esplikasaun liña-ba-liña**

- **Liña 44–46** — Komentáriu seksaun augmentation offline.
- **Liña 48–49** — Komentáriu: mantein tamañu orijinál; resize/crop no normaliza halo online iha `train.py`.
- **Liña 50** — `OFFLINE_TRANSFORM = A.Compose([`
- **Liña 52** — `A.HorizontalFlip(p=0.5),`
- **Liña 54** — `A.VerticalFlip(p=0.5),`
- **Liña 56** — `A.RandomRotate90(p=0.5),`
- **Liña 58–64** — `A.Affine(translate_percent=(-0.1, 0.1), scale=(0.9, 1.1), rotate=(-30, 30), border_mode=cv2.BORDER_REFLECT_101, p=0.7)` — rohan prenxe ho reflesaun.
- **Liña 66–72** — `A.ColorJitter(brightness=0.3, contrast=0.3, saturation=0.3, hue=0.05, p=0.8)`.
- **Liña 74–79** — `A.RandomShadow(num_shadows_limit=(1, 3), shadow_dimension=5, shadow_intensity_range=(0.3, 0.7), p=0.4)`.
- **Liña 81** — `A.OneOf([`
- **Liña 82** — `A.GaussianBlur(blur_limit=(3, 5)),`
- **Liña 83** — `A.MotionBlur(blur_limit=(3, 5)),`
- **Liña 84** — `A.GaussNoise(std_range=(0.01, 0.04))`
- **Liña 85** — `], p=0.3)`.
- **Liña 87** — `], seed=SEED)` — Compose ho seed (reprodutivel).
- **Liña 89** — `rng = np.random.default_rng(SEED)`.
- **Liña 92** — `def to_grayscale(image):`
- **Liña 94** — `gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)`.
- **Liña 96** — `return cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)` — kanál 3 fali (formatu hanesan).
- **Liña 99** — `def dihedral(image, k):`
- **Liña 101** — Komentáriu: k = 0..7: rotasaun 90° haat, kada ida ho no la ho flip.
- **Liña 102** — `image = np.rot90(image, k % 4)`.
- **Liña 104** — `if k >= 4:`
- **Liña 105** — `image = np.fliplr(image)`.
- **Liña 107** — `return np.ascontiguousarray(image)`.

**5. Walkthrough ezekusaun**

`dihedral(img, 5)` → roda 90° × (5 % 4 = 1) → flip esquerda-direita. `to_grayscale(img)` → `[H, W, 3]` ho kanál 3 hanesan.

**6. Tipu dadus**

`OFFLINE_TRANSFORM`: `A.Compose`; `rng`: `numpy.random.Generator`; `to_grayscale(ndarray) -> ndarray`; `dihedral(ndarray, int) -> ndarray`.

**7. Ligasaun OOP**

Komposisaun objetu Albumentations (haree D.2.3).

**8. Impaktu integrasaun**

`make_copies` uza funsaun sira ne'e.

**9. Checkpoint ba principiante**

**P:** Tanba sá `OFFLINE_TRANSFORM` la iha `Resize`/`Normalize`?
**R:** Kópia rai iha disku ho tamañu orijinál; `train.py` halo resize/crop no normaliza online (komentáriu liña 48–49).

#### D.3.3 `make_copies`

**1. Kódigu orijinál**

{{code @train/augment.py 110-149}}

**2. Objetivu**

Harii lista `(sufiksu, imajen)`: ba healthy kampu → rotasaun/flip 8 + grayscale 2; ba seluk → `COPIES_PER_IMAGE` kópia, ho grayscale ho probabilidade 15%.

**3. Fundamentu Python**

List comprehension ho f-string sufiksu; `rng.random()` (0–1); `+=` iha string.

**4. Esplikasaun liña-ba-liña**

- **Liña 110** — `def make_copies(image, source_path, class_name):`
- **Liña 112** — Komentáriu: fó lista `(suffix, augmented image)`.
- **Liña 113–116** — `is_field_healthy = (class_name == "Healthy" and os.path.normpath(source_path).startswith(FIELD_HEALTHY_DIR))`.
- **Liña 118** — `if is_field_healthy:`
- **Liña 120–123** — `copies = [(f"rot{k}", OFFLINE_TRANSFORM(image=dihedral(image, k))["image"]) for k in range(8)]`.
- **Liña 125** — `for g in range(FIELD_HEALTHY_GRAY_COPIES):`
- **Liña 127** — `augmented = OFFLINE_TRANSFORM(image=image)["image"]`.
- **Liña 129** — `copies.append((f"gray{g + 1}", to_grayscale(augmented)))`.
- **Liña 131** — `return copies` — kópia 10.
- **Liña 133** — `copies = []`.
- **Liña 135** — `for copy in range(COPIES_PER_IMAGE):`
- **Liña 137** — `augmented = OFFLINE_TRANSFORM(image=image)["image"]`.
- **Liña 139** — `suffix = f"aug{copy + 1}"`.
- **Liña 141** — `if rng.random() < GRAYSCALE_PROBABILITY:`
- **Liña 143** — `augmented = to_grayscale(augmented)`.
- **Liña 145** — `suffix += "_gray"`.
- **Liña 147** — `copies.append((suffix, augmented))`.
- **Liña 149** — `return copies`.

**5. Walkthrough ezekusaun**

Healthy kampu → `["rot0", ..., "rot7", "gray1", "gray2"]`. Blight → `["aug1"]` (85%) ka `["aug1_gray"]` (15%).

**6. Tipu dadus**

Return `list[tuple[str, numpy.ndarray]]`.

**7. Ligasaun OOP**

Bolu objetu `OFFLINE_TRANSFORM(image=...)` — objetu "callable".

**8. Impaktu integrasaun**

`main` bolu kada imajen treinu.

**9. Checkpoint ba principiante**

**P:** Kópia hira ba fotografia healthy kampu ida?
**R:** 10: rotasaun/flip 8 + grayscale 2.

#### D.3.4 `main` — hakerek kópia no manifest

**1. Kódigu orijinál**

{{code @train/augment.py 152-247}}

**2. Objetivu**

Buka imajen, split (hanesan `train.py`), hamoos pasta augmented tuan, harii kópia kada imajen treinu, rai JPEG, no hakerek `manifest.csv` (dalan kópia, dalan fonte, klase).

**3. Fundamentu Python**

`train_paths, train_labels, *_ = split_data(...)`; `csv.DictWriter`; `hashlib.md5(...).hexdigest()[:8]`; `(i + 1) % 500 == 0` (progresu).

**4. Esplikasaun liña-ba-liña**

- **Liña 152–154** — Komentáriu seksaun main.
- **Liña 156** — `def main():`
- **Liña 158** — `image_paths, labels = find_images(DATASET_DIRS)`.
- **Liña 160** — `labels = np.array(labels)`.
- **Liña 162–165** — `train_paths, train_labels, *_ = split_data(image_paths, labels)` — split treinu de'it.
- **Liña 167** — Komentáriu: hahú moos atu kópia tuan husi split tuan la hela.
- **Liña 168** — `if os.path.exists(AUGMENTED_DIR):`
- **Liña 170** — `shutil.rmtree(AUGMENTED_DIR)`.
- **Liña 172** — `for class_name in CLASS_NAMES:`
- **Liña 174–177** — `os.makedirs(os.path.join(AUGMENTED_DIR, class_name), exist_ok=True)`.
- **Liña 179** — `print(f"\nAugmenting {len(train_paths)} training images...")`.
- **Liña 181** — `rows = []`.
- **Liña 183–185** — `for i, (source_path, label) in enumerate(zip(train_paths, train_labels)):`
- **Liña 187** — `image = cv2.imread(source_path)`.
- **Liña 189** — `if image is None:`
- **Liña 191** — `print(f"WARNING: could not read {source_path}")`.
- **Liña 193** — `continue`.
- **Liña 195** — `class_name = CLASS_NAMES[label]`.
- **Liña 197** — Komentáriu: hash dalan fonte mantein naran úniku entre dataset.
- **Liña 198–200** — `stem = os.path.splitext(os.path.basename(source_path))[0]`.
- **Liña 202–204** — `tag = hashlib.md5(source_path.encode()).hexdigest()[:8]`.
- **Liña 206** — `for suffix, augmented in make_copies(image, source_path, class_name):`
- **Liña 208–212** — `aug_path = os.path.join(AUGMENTED_DIR, class_name, f"{stem}_{tag}_{suffix}.jpg")`.
- **Liña 214–218** — `cv2.imwrite(aug_path, augmented, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])`.
- **Liña 220–224** — `rows.append({"aug_path": ..., "source_path": ..., "class_name": ...})`.
- **Liña 226** — `if (i + 1) % 500 == 0:`
- **Liña 228** — `print(f"  {i + 1}/{len(train_paths)}")`.
- **Liña 230** — `with open(AUGMENTED_MANIFEST, "w", newline="") as file:`
- **Liña 232–235** — `writer = csv.DictWriter(file, fieldnames=["aug_path", "source_path", "class_name"])`.
- **Liña 237** — `writer.writeheader()`.
- **Liña 239** — `writer.writerows(rows)`.
- **Liña 241** — `print(f"\nWrote {len(rows)} augmented images to {AUGMENTED_DIR}")`.
- **Liña 243** — `print(f"Manifest: {AUGMENTED_MANIFEST}")`.
- **Liña 246** — `if __name__ == "__main__":`
- **Liña 247** — `main()`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `data/plantdoc/train/Blight/abc.jpg` → `data/augmented/Blight/abc_3f2a9c1d_aug1.jpg` + liña manifest `{aug_path, source_path, "Blight"}`.

**6. Tipu dadus**

`rows`: `list[dict[str, str]]`; `tag`: `str` (karakter 8).

**7. Ligasaun OOP**

`csv.DictWriter` mak objetu ho métodu `writeheader`, `writerows`.

**8. Impaktu integrasaun**

`train.load_augmented` lee manifest no aseita só kópia ho `source_path` iha split treinu.

**9. Checkpoint ba principiante**

**P:** Tanba sá rai `source_path` iha manifest?
**R:** Atu `train.py` kontrola katak kópia ida mai husi imajen treinu (la'ós validasaun/teste) — evita data leakage.

### D.4 `model.py` — SE-ResNet-18 (treinu)

#### D.4.1 Klase `SEBlock`

**1. Kódigu orijinál**

{{code @train/model.py 1-66}}

**2. Objetivu**

Bloku Squeeze-and-Excitation: kalkula peze atensaun ba kada kanál (0–1) no multiplika mapa feature — modelu aprende kanál ida ne'ebé importante (formula A.4.3).

**3. Fundamentu Python**

`class SEBlock(nn.Module)`; `super().__init__()`; `nn.Sequential(...)` (kamada tuir malu); `x.size()`; `y.view(...)` (muda forma); `//` (divizaun inteiru).

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import torch`.
- **Liña 2** — `import torch.nn as nn`.
- **Liña 3** — `import torchvision.models as models`.
- **Liña 6–8** — Komentáriu seksaun SE.
- **Liña 10** — `class SEBlock(nn.Module):`
- **Liña 12–16** — `def __init__(self, channels, reduction=16):`
- **Liña 18** — `super().__init__()` — inisializa `nn.Module`.
- **Liña 20–23** — `reduced_channels = max(channels // reduction, 1)` — ezemplu 64 // 16 = 4.
- **Liña 25** — `self.squeeze = nn.AdaptiveAvgPool2d(1)` — média espasiál → `[B, C, 1, 1]`.
- **Liña 27** — `self.excitation = nn.Sequential(`
- **Liña 29–33** — `nn.Linear(channels, reduced_channels, bias=False),` — $W_1$.
- **Liña 35** — `nn.ReLU(inplace=True),` — $\delta$.
- **Liña 37–41** — `nn.Linear(reduced_channels, channels, bias=False),` — $W_2$.
- **Liña 43** — `nn.Sigmoid()` — $\sigma$ → 0–1.
- **Liña 44** — `)`.
- **Liña 46** — `def forward(self, x):`
- **Liña 48** — `batch_size, channels, _, _ = x.size()`.
- **Liña 50** — `y = self.squeeze(x)`.
- **Liña 52–55** — `y = y.view(batch_size, channels)` — `[B, C]`.
- **Liña 57** — `y = self.excitation(y)` — peze `[B, C]`.
- **Liña 59–64** — `y = y.view(batch_size, channels, 1, 1)`.
- **Liña 66** — `return x * y` — multiplika kada kanál (broadcast).

**5. Walkthrough ezekusaun**

$x$ forma `[32, 64, 56, 56]` → squeeze `[32, 64, 1, 1]` → `[32, 64]` → Linear 64→4 → ReLU → Linear 4→64 → Sigmoid → `[32, 64, 1, 1]` → `x * y` `[32, 64, 56, 56]`.

**6. Tipu dadus**

`channels`, `reduction`, `reduced_channels`: `int`; `x`, `y`: `torch.Tensor`; `forward` return `torch.Tensor` (forma hanesan `x`).

**7. Ligasaun OOP**

- **Herança**: `SEBlock(nn.Module)`.
- **`super().__init__()`** obrigatóriu atu `nn.Module` rejista sub-kamada.
- **Komposisaun**: objetu na'in `squeeze` (`AdaptiveAvgPool2d`) no `excitation` (`Sequential` ho Linear/ReLU/Linear/Sigmoid).
- **Polimorfizmu (override)**: `forward` substitui métodu `nn.Module`; `block(x)` bolu `forward` liuhusi `__call__` herda.

**8. Impaktu integrasaun**

`SEResNet18._inject_se` harii `SEBlock` ba kada bloku residuál.

**9. Checkpoint ba principiante**

**P:** Tanba sá `Sigmoid` iha rohan?
**R:** Atu peze kada kanál entre 0 no 1 — 0 = hamate kanál, 1 = mantein tomak.

#### D.4.2 `SEResNet18.__init__`

**1. Kódigu orijinál**

{{code @train/model.py 69-123}}

**2. Objetivu**

Harii SE-ResNet-18: foti ResNet-18 torchvision (ho peze ImageNet se `pretrained=True`), reuza kamada, injeta SE iha layer 4, no troka kamada final ba klase `num_classes`.

**3. Fundamentu Python**

`models.ResNet18_Weights.DEFAULT` — peze treina ona; `base_resnet.fc.in_features` — 512.

**4. Esplikasaun liña-ba-liña**

- **Liña 69–71** — Komentáriu seksaun SE-RESNET18.
- **Liña 73** — `class SEResNet18(nn.Module):`
- **Liña 75–79** — `def __init__(self, num_classes, pretrained=True):`
- **Liña 81** — `super().__init__()`.
- **Liña 83** — `if pretrained:`
- **Liña 85–87** — `weights = (models.ResNet18_Weights.DEFAULT)` — *transfer learning* ImageNet.
- **Liña 89** — `else:`
- **Liña 91** — `weights = None`.
- **Liña 93–95** — `base_resnet = models.resnet18(weights=weights)`.
- **Liña 97** — `self.conv1 = base_resnet.conv1` — 7×7, 64.
- **Liña 98** — `self.bn1 = base_resnet.bn1`.
- **Liña 99** — `self.relu = base_resnet.relu`.
- **Liña 100** — `self.maxpool = base_resnet.maxpool`.
- **Liña 102–104** — `self.layer1 = self._inject_se(base_resnet.layer1)`.
- **Liña 106–108** — `self.layer2 = self._inject_se(base_resnet.layer2)`.
- **Liña 110–112** — `self.layer3 = self._inject_se(base_resnet.layer3)`.
- **Liña 114–116** — `self.layer4 = self._inject_se(base_resnet.layer4)`.
- **Liña 118** — `self.avgpool = base_resnet.avgpool`.
- **Liña 120–123** — `self.fc = nn.Linear(base_resnet.fc.in_features, num_classes)` — 512 → 4 (foun, aleatóriu).

**5. Walkthrough ezekusaun**

`SEResNet18(num_classes=4, pretrained=True)` → download peze ImageNet (dala uluk) → modelu ho kamada final 512→4.

**6. Tipu dadus**

`num_classes`: `int`; `pretrained`: `bool`; `weights`: `ResNet18_Weights ka None`; atributu sira: `nn.Module`.

**7. Ligasaun OOP**

- **Komposisaun**: `SEResNet18` na'in kamada hotu (conv1, layer1–4, fc). Naran atributu (`conv1`, `layer1`...) mak chave iha `state_dict` — tenke hanesan ho `ml/se_resnet.py` atu peze karrega.
- **Reuza objetu** husi `base_resnet` (agregasaun ba sub-módulu).

**8. Impaktu integrasaun**

`train.main` harii ho `pretrained=True`; `predict`/`gradcam` ho `pretrained=False` (karrega checkpoint).

**9. Checkpoint ba principiante**

**P:** Saida mak *transfer learning* iha ne'e?
**R:** Hahú ho peze ne'ebé aprende ona husi ImageNet (imajen miliaun), depois treina fali ba moras batar — presiza dadus uitoan liu no aprende lalais.

#### D.4.3 `_inject_se` no `forward`

**1. Kódigu orijinál**

{{code @train/model.py 125-204}}

**2. Objetivu**

`_inject_se`: ba kada bloku residuál, aumenta `block.se` no troka `block.forward` ho versaun ne'ebé aplika SE molok soma shortcut (formula A.4.4). `forward`: kálkulu modelu tomak.

**3. Fundamentu Python**

- **Funsaun iha loop ho default `block=block`** — kaer bloku agora (la'ós ikus).
- **Troka métodu objetu** (`block.forward = forward_with_se`).
- **`nn.Sequential(*lista)`** — unpack lista ba argumentu.
- **`torch.flatten(x, 1)`** — `[B, 512, 1, 1]` → `[B, 512]`.
- **`out += identity`** — soma residuál.

**4. Esplikasaun liña-ba-liña**

- **Liña 125–127** — Komentáriu: aumenta SE ba bloku residuál hotu.
- **Liña 129** — `def _inject_se(self, layer):`
- **Liña 131** — `modified_blocks = []`.
- **Liña 133** — `for block in layer:` — `BasicBlock` 2 kada layer.
- **Liña 135** — `channels = block.conv2.out_channels`.
- **Liña 137–138** — Komentáriu: atribui hanesan sub-módulu atu `.to(device)`, `parameters()` no `state_dict()` inklui.
- **Liña 139–142** — `block.se = SEBlock(channels=channels, reduction=16)`.
- **Liña 144–147** — `def forward_with_se(x, block=block):`
- **Liña 149** — `identity = x`.
- **Liña 151** — `out = block.conv1(x)`.
- **Liña 152** — `out = block.bn1(out)`.
- **Liña 153** — `out = block.relu(out)`.
- **Liña 155** — `out = block.conv2(out)`.
- **Liña 156** — `out = block.bn2(out)`.
- **Liña 158** — Komentáriu: atensaun kanál.
- **Liña 159** — `out = block.se(out)`.
- **Liña 161** — `if block.downsample is not None:` — dimensaun muda:
- **Liña 163** — `identity = block.downsample(x)`.
- **Liña 165** — `out += identity` — koneksaun residuál.
- **Liña 167** — `out = block.relu(out)`.
- **Liña 169** — `return out`.
- **Liña 171** — `block.forward = forward_with_se`.
- **Liña 173** — `modified_blocks.append(block)`.
- **Liña 175–177** — `return nn.Sequential(*modified_blocks)`.
- **Liña 179–181** — Komentáriu seksaun forward.
- **Liña 183** — `def forward(self, x):`
- **Liña 185** — `x = self.conv1(x)`.
- **Liña 186** — `x = self.bn1(x)`.
- **Liña 187** — `x = self.relu(x)`.
- **Liña 188** — `x = self.maxpool(x)`.
- **Liña 190** — `x = self.layer1(x)`.
- **Liña 191** — `x = self.layer2(x)`.
- **Liña 192** — `x = self.layer3(x)`.
- **Liña 193** — `x = self.layer4(x)`.
- **Liña 195** — `x = self.avgpool(x)`.
- **Liña 197–200** — `x = torch.flatten(x, 1)`.
- **Liña 202** — `x = self.fc(x)` — logits `[B, 4]`.
- **Liña 204** — `return x`.

**5. Walkthrough ezekusaun**

Forma: `[1, 3, 224, 224]` → conv1 `[1, 64, 112, 112]` → maxpool `[1, 64, 56, 56]` → layer1 `[1, 64, 56, 56]` → layer2 `[1, 128, 28, 28]` → layer3 `[1, 256, 14, 14]` → layer4 `[1, 512, 7, 7]` → avgpool `[1, 512, 1, 1]` → flatten `[1, 512]` → fc `[1, 4]`. (Forma ResNet-18 padraun — *Teoria*.)

**6. Tipu dadus**

`layer`: `nn.Sequential`; `block`: `torchvision BasicBlock`; `forward` return `torch.Tensor` `[B, num_classes]` (logits).

**7. Ligasaun OOP**

- **Monkey patching**: `block.forward = forward_with_se` troka hahalok **objetu ida** (la'ós klase `BasicBlock`) — polimorfizmu dinámiku Python.
- **Closure** ho default argumentu: kada funsaun kaer nia `block`.
- **Komposisaun**: `block.se` sai sub-módulu bloku.

**8. Impaktu integrasaun**

`ml/se_resnet.py` repete lójika ne'e (versaun badak) ho naran atributu hanesan; `state_dict` husi treinu karrega iha sistema.

**9. Checkpoint ba principiante**

**P:** Tanba sá `def forward_with_se(x, block=block)` no la'ós `def forward_with_se(x)`?
**R:** La ho `block=block`, funsaun hotu sei uza `block` ikus husi loop (late binding). Default argumentu kaer valór kada volta.
