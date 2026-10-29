
### D.6 `predict.py` — prediksaun imajen ida (projetu treinu)

#### D.6.1 Konfigurasaun no `DiseasePredictor.__init__`

**1. Kódigu orijinál**

{{code @train/predict.py 1-92}}

**2. Objetivu**

Klase ne'ebé karrega checkpoint, harii SE-ResNet-18 la ho peze ImageNet, karrega peze treina, prepara transformasaun validasaun, no rai limite konfiansa 0.60.

**3. Fundamentu Python**

Klase ho `__init__` ho parámetru default; `checkpoint.get("class_names", CLASS_NAMES)`; `torch.nn.functional as F`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import cv2`.
- **Liña 2** — `import torch`.
- **Liña 3** — `import torch.nn.functional as F`.
- **Liña 5–8** — `from dataset import (get_transforms, CLASS_NAMES)`.
- **Liña 10** — `from model import SEResNet18`.
- **Liña 13–15** — Komentáriu seksaun konfigurasaun.
- **Liña 17–20** — `MODEL_PATH = ("checkpoints/" "best_seresnet18_crop_ews.pth")` — string rua kombina.
- **Liña 22** — `CONFIDENCE_THRESHOLD = 0.60`.
- **Liña 24** — `IMG_SIZE = 224`.
- **Liña 27–29** — Komentáriu seksaun predictor.
- **Liña 31** — `class DiseasePredictor:`
- **Liña 33–37** — `def __init__(self, model_path=MODEL_PATH, confidence_threshold=CONFIDENCE_THRESHOLD):`
- **Liña 39–43** — `self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")`.
- **Liña 45–47** — `self.confidence_threshold = (confidence_threshold)`.
- **Liña 49–51** — Komentáriu: karrega checkpoint.
- **Liña 53–56** — `checkpoint = torch.load(model_path, map_location=self.device)`.
- **Liña 58–61** — `class_names = checkpoint.get("class_names", CLASS_NAMES)`.
- **Liña 63** — `self.class_names = class_names`.
- **Liña 65–67** — Komentáriu: harii modelu.
- **Liña 69–72** — `self.model = SEResNet18(num_classes=len(self.class_names), pretrained=False)`.
- **Liña 74–76** — `self.model.load_state_dict(checkpoint["model_state_dict"])`.
- **Liña 78–80** — `self.model.to(self.device)`.
- **Liña 82** — `self.model.eval()`.
- **Liña 84–86** — Komentáriu: transformasaun.
- **Liña 88–91** — `self.transform = get_transforms(img_size=IMG_SIZE, is_train=False)`.

**5. Walkthrough ezekusaun**

`p = DiseasePredictor()` → karrega `checkpoints/best_seresnet18_crop_ews.pth` → `p.class_names = ["Healthy", "Blight", "Common_Rust", "Gray_Leaf_Spot"]`.

**6. Tipu dadus**

| Atributu | Tipu |
|---|---|
| `self.device` | `torch.device` |
| `self.confidence_threshold` | `float` |
| `self.class_names` | `list[str]` |
| `self.model` | `SEResNet18` |
| `self.transform` | `A.Compose` |

**7. Ligasaun OOP**

- **Klase + `__init__`**: atributu instánsia 5.
- **Komposisaun**: objetu `DiseasePredictor` na'in objetu `SEResNet18` no `Compose` (moris hamutuk).
- **Enkapsulamentu**: chamadór só bolu `predict(path)`.

**8. Impaktu integrasaun**

Ba teste iha projetu treinu de'it. Sistema uza `ml/inference.py` (la'ós klase ne'e).

**9. Checkpoint ba principiante**

**P:** Tanba sá `pretrained=False` iha ne'e?
**R:** Peze ImageNet la presiza — peze treina hotu karrega husi checkpoint iha liña 74–76.

#### D.6.2 `DiseasePredictor.predict` no `__main__`

**1. Kódigu orijinál**

{{code @train/predict.py 93-240}}

**2. Objetivu**

Lee imajen, preprocess, inferénsia (softmax), top-3, klase di'ak liu, aplika limite konfiansa ("Uncertain" se < 0.60), fó dict.

**3. Fundamentu Python**

`tensor.unsqueeze(0)` — `[3,224,224]` → `[1,3,224,224]`; `torch.topk(probs, k, dim=1)`; `.item()`; `zip`.

**4. Esplikasaun liña-ba-liña**

- **Liña 93–95** — Komentáriu seksaun predict.
- **Liña 97** — `def predict(self, image_path):`
- **Liña 99–101** — `image = cv2.imread(image_path)`.
- **Liña 103** — `if image is None:`
- **Liña 105–108** — `raise RuntimeError(f"Could not read image: {image_path}")`.
- **Liña 110–113** — `image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)`.
- **Liña 115–117** — Komentáriu: preprocess.
- **Liña 119–121** — `augmented = self.transform(image=image)`.
- **Liña 123–125** — `tensor = augmented["image"].unsqueeze(0)`.
- **Liña 127–129** — `tensor = tensor.to(self.device)`.
- **Liña 131–133** — Komentáriu: inferénsia.
- **Liña 135** — `with torch.no_grad():`
- **Liña 137–139** — `outputs = self.model(tensor)`.
- **Liña 141–144** — `probabilities = F.softmax(outputs, dim=1)` (A.4.5).
- **Liña 146–148** — Komentáriu: prediksaun top.
- **Liña 150–159** — `confidence_values, indices = (torch.topk(probabilities, k=min(3, len(self.class_names)), dim=1))`.
- **Liña 161** — `top_predictions = []`.
- **Liña 163–166** — `for confidence, index in zip(confidence_values[0], indices[0]):`
- **Liña 168–179** — `top_predictions.append({"disease": self.class_names[index.item()], "confidence": round(confidence.item() * 100, 2)})`.
- **Liña 181–183** — Komentáriu: prediksaun di'ak liu.
- **Liña 185–187** — `confidence = confidence_values[0, 0].item()`.
- **Liña 189–191** — `predicted_index = indices[0, 0].item()`.
- **Liña 193–195** — `predicted_class = self.class_names[predicted_index]`.
- **Liña 197–199** — Komentáriu: limite konfiansa.
- **Liña 201** — `if confidence < self.confidence_threshold:`
- **Liña 203** — `final_prediction = "Uncertain"`.
- **Liña 205** — `else:`
- **Liña 207** — `final_prediction = predicted_class`.
- **Liña 209** — `return {`
- **Liña 211** — `"disease": final_prediction,`
- **Liña 213** — `"raw_prediction": predicted_class,`
- **Liña 215–218** — `"confidence": round(confidence * 100, 2),`
- **Liña 220–221** — `"top_predictions": top_predictions`
- **Liña 222** — `}`.
- **Liña 225–227** — Komentáriu seksaun teste.
- **Liña 229** — `if __name__ == "__main__":`
- **Liña 231** — `predictor = DiseasePredictor()`.
- **Liña 233** — `image_path = "test_image.jpg"`.
- **Liña 235–237** — `result = predictor.predict(image_path)`.
- **Liña 239** — `print("\nPrediction:")`.
- **Liña 240** — `print(result)`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* probabilidade `[0.02, 0.55, 0.40, 0.03]` → top-3 Blight 55%, Common_Rust 40%, Gray 3% → 0.55 < 0.60 → `{"disease": "Uncertain", "raw_prediction": "Blight", "confidence": 55.0, ...}`. (Iha sistema ho limite 0.5, imajen hanesan sai `northern_leaf_blight`.)

**6. Tipu dadus**

`image_path`: `str`; `tensor`: `Tensor [1,3,224,224]`; `probabilities`: `Tensor [1,4]`; return `dict` (`disease: str`, `raw_prediction: str`, `confidence: float` (%), `top_predictions: list[dict]`).

**7. Ligasaun OOP**

Métodu instánsia uza estadu objetu (`self.model`, `self.transform`, `self.class_names`, `self.confidence_threshold`).

**8. Impaktu integrasaun**

Lójika "uncertain" hanesan ho sistema (`_result_from_probs`), maibé limite diferente (0.60 vs 0.5) no output konfiansa iha persentajen.

**9. Checkpoint ba principiante**

**P:** Tanba sá `unsqueeze(0)`?
**R:** Modelu hein batch `[B, 3, H, W]`. Imajen ida mak `[3, H, W]`; `unsqueeze(0)` aumenta dimensaun batch B = 1.

### D.7 `gradcam.py` — esplika modelu (projetu treinu)

#### D.7.1 Konfigurasaun no klase `GradCAM`

**1. Kódigu orijinál**

{{code @train/gradcam.py 1-100}}

**2. Objetivu**

Klase Grad-CAM ho *hook*: rai ativasaun `layer4` (forward) no gradiente (backward); bolu objetu → mapa calor ba klase prediksaun (formula A.4.11).

**3. Fundamentu Python**

- **Hook PyTorch**: `register_forward_hook(f)` — PyTorch bolu `f(module, inputs, output)` kada forward; `tensor.register_hook(g)` — bolu `g(grad)` bainhira backward.
- **`__call__`** — objetu bele bolu.
- **`matplotlib.use("Agg")`** — hakerek gráfiku ba arkivu la ho janela.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `import csv`.
- **Liña 2** — `import os`.
- **Liña 3** — `import random`.
- **Liña 5** — `import cv2`.
- **Liña 6** — `import numpy as np`.
- **Liña 8** — `import matplotlib`.
- **Liña 9** — `matplotlib.use("Agg")`.
- **Liña 10** — `import matplotlib.pyplot as plt`.
- **Liña 12** — `import torch`.
- **Liña 13** — `import torch.nn.functional as F`.
- **Liña 15–19** — `from dataset import (find_images, get_transforms, CLASS_NAMES)`.
- **Liña 21** — `from model import SEResNet18`.
- **Liña 23–29** — `from train import (DATASET_DIRS, IMG_SIZE, RESULTS_DIR, SEED, split_data)`.
- **Liña 32–34** — Komentáriu seksaun konfigurasaun.
- **Liña 36** — `MODEL_PATH = "checkpoints/best_seresnet18_crop_ews.pth"`.
- **Liña 38** — `OUTPUT_DIR = os.path.join(RESULTS_DIR, "gradcam")`.
- **Liña 40** — `SAMPLES_PER_CLASS = 8`.
- **Liña 42–43** — Komentáriu: PlantVillage iha kópia fundu-hasai ba kada fotografia "color"; fó máskara folha ezatu.
- **Liña 44** — `PV_COLOR = os.path.normpath("plantvillage dataset/color")`.
- **Liña 45** — `PV_SEGMENTED = os.path.normpath("plantvillage dataset/segmented")`.
- **Liña 48–50** — Komentáriu seksaun Grad-CAM.
- **Liña 52** — `class GradCAM:`
- **Liña 54** — `def __init__(self, model, target_layer):`
- **Liña 56** — `self.model = model`.
- **Liña 58** — `self.activations = None`.
- **Liña 59** — `self.gradients = None`.
- **Liña 61** — `target_layer.register_forward_hook(self._save)` — métodu objetu hanesan callback.
- **Liña 63** — `def _save(self, module, inputs, output):`
- **Liña 65** — `self.activations = output.detach()` — $A^k$.
- **Liña 67** — Komentáriu: prediksaun `no_grad` mós liu iha ne'e.
- **Liña 68** — `if output.requires_grad:`
- **Liña 69** — `output.register_hook(self._save_gradient)`.
- **Liña 71** — `def _save_gradient(self, grad):`
- **Liña 73** — `self.gradients = grad.detach()` — $\partial y^c / \partial A^k$.
- **Liña 75** — `def __call__(self, tensor):`
- **Liña 77** — `self.model.zero_grad()`.
- **Liña 79** — `outputs = self.model(tensor)` — hook rai ativasaun.
- **Liña 81** — `probabilities = F.softmax(outputs, dim=1)[0]`.
- **Liña 83** — `class_index = probabilities.argmax().item()`.
- **Liña 85** — `outputs[0, class_index].backward()` — gradiente ba score klase.
- **Liña 87** — Komentáriu: peze kanál = gradiente média kada kanál.
- **Liña 88** — `weights = self.gradients.mean(dim=(2, 3), keepdim=True)` — $\alpha_k^c$.
- **Liña 90** — `cam = F.relu((weights * self.activations).sum(dim=1))[0]` — $L^c$.
- **Liña 92** — `cam = cam / (cam.max() + 1e-8)` — normaliza 0–1.
- **Liña 94–98** — `cam = cv2.resize(cam.cpu().numpy(), (IMG_SIZE, IMG_SIZE), interpolation=cv2.INTER_LINEAR)` — 7×7 → 224×224.
- **Liña 100** — `return cam, class_index, probabilities.detach().cpu().numpy()`.

**5. Walkthrough ezekusaun**

`gc = GradCAM(model, model.layer4)`; `cam, idx, p = gc(tensor)` → `cam` `224×224` (0–1), `idx = 1` (Blight), `p = [0.01, 0.97, ...]`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `self.activations`, `self.gradients` | `Tensor [1, 512, 7, 7]` ka `None` |
| `__call__` return | `tuple[ndarray [224,224], int, ndarray [4]]` |

**7. Ligasaun OOP**

- **Klase + estadu**: `activations`/`gradients` rai entre hook no `__call__`.
- **Observer/hook pattern**: objetu rejista métodu (`self._save`) iha kamada PyTorch; PyTorch "notifika" bainhira forward/backward.
- **`__call__`**: objetu hahalok hanesan funsaun.
- Nota: versaun sistema (`disease_heatmap`) **la uza hook** (thread-safe) — D.12.6.

**8. Impaktu integrasaun**

`main` (D.7.4) uza ba imajen teste hotu.

**9. Checkpoint ba principiante**

**P:** Tanba sá `ReLU` iha formula Grad-CAM?
**R:** Atu hatudu de'it área ne'ebé **aumenta** score klase (influénsia pozitivu); valór negativu sai 0.

#### D.7.2 Helper: fonte, lee, máskara folha, hasai folha

**1. Kódigu orijinál**

{{code @train/gradcam.py 103-201}}

**2. Objetivu**

Funsaun ajuda: hatene fonte (lab/field), lee RGB, hetan máskara folha husi imajen PlantVillage `segmented` (indexa naran arkivu), no harii imajen "fundu de'it" (folha hamoos ho kór median fundu).

**3. Fundamentu Python**

Dict globál ba cache (`EXACT_INDEX`, `SHORT_INDEX`); `dict.setdefault(k, []).append(v)`; operasaun morfolójika OpenCV (`morphologyEx`, `dilate`); indexasaun boolean NumPy (`image[grown == 0]`).

**4. Esplikasaun liña-ba-liña**

- **Liña 103–105** — Komentáriu seksaun helper.
- **Liña 107** — `def source_of(path):`
- **Liña 109** — `parts = os.path.normpath(path).split(os.sep)`.
- **Liña 111** — `if "plantvillage" in parts:`
- **Liña 112** — `return "lab"`.
- **Liña 114** — `return "field"`.
- **Liña 117** — `def load_rgb(path):`
- **Liña 119** — `image = cv2.imread(path)`.
- **Liña 121** — `if image is None:`
- **Liña 122** — `raise RuntimeError(f"Could not read image: {path}")`.
- **Liña 124** — `return cv2.cvtColor(image, cv2.COLOR_BGR2RGB)`.
- **Liña 127** — `def mask_keys(path):`
- **Liña 129–131** — Komentáriu: naran segmented `"<name>_final_masked.jpg"`, dalaruma ho prefiksu `"<uuid>___"` no letra diferente; fó (chave ezatu, chave la ho prefiksu).
- **Liña 132** — `stem = os.path.splitext(os.path.basename(path))[0].lower()`.
- **Liña 134** — `stem = stem.replace("_final_masked", "")`.
- **Liña 136** — `folder = os.path.basename(os.path.dirname(path))`.
- **Liña 138** — `return (folder, stem), (folder, stem.split("___")[-1])`.
- **Liña 141** — `EXACT_INDEX = {}`.
- **Liña 142** — `SHORT_INDEX = {}`.
- **Liña 145** — `def load_leaf_mask(color_path):`
- **Liña 147** — `if not EXACT_INDEX:` — indexa dala ida.
- **Liña 149–151** — `root = os.path.dirname(os.path.dirname(os.path.normpath(color_path))).replace(PV_COLOR, PV_SEGMENTED)`.
- **Liña 153** — `for folder, _, files in os.walk(root):`
- **Liña 154** — `for file in files:`
- **Liña 155** — `path = os.path.join(folder, file)`.
- **Liña 156** — `exact, short = mask_keys(path)`.
- **Liña 157** — `EXACT_INDEX[exact] = path`.
- **Liña 158** — `SHORT_INDEX.setdefault(short, []).append(path)`.
- **Liña 160** — `exact, short = mask_keys(color_path)`.
- **Liña 162** — `segmented_path = EXACT_INDEX.get(exact)`.
- **Liña 164** — Komentáriu: uza naran la ho prefiksu só se úniku.
- **Liña 165** — `if segmented_path is None and len(SHORT_INDEX.get(short, [])) == 1:`
- **Liña 166** — `segmented_path = SHORT_INDEX[short][0]`.
- **Liña 168** — `if segmented_path is None:`
- **Liña 169** — `return None, None`.
- **Liña 171** — `segmented = cv2.imread(segmented_path)`.
- **Liña 173** — `if segmented is None:`
- **Liña 174** — `return None, None`.
- **Liña 176** — `mask = (segmented.max(axis=2) > 25).astype(np.uint8)` — piksel la metan = folha.
- **Liña 178** — Komentáriu: hasai pontu JPEG iha fundu metan.
- **Liña 179–183** — `mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, np.ones((5, 5), np.uint8))`.
- **Liña 185** — `return mask, cv2.cvtColor(segmented, cv2.COLOR_BGR2RGB)`.
- **Liña 188** — `def remove_leaf(image, mask):`
- **Liña 190–191** — Komentáriu: imajen fundu de'it: folha (+ margen) pinta ho kór median fundu fotografia.
- **Liña 192** — `grown = cv2.dilate(mask, np.ones((15, 15), np.uint8))` — máskara boot uitoan.
- **Liña 194** — `background = image[grown == 0]` — piksel fundu.
- **Liña 196** — `result = image.copy()`.
- **Liña 198** — `if len(background) > 0:`
- **Liña 199** — `result[grown == 1] = np.median(background, axis=0)`.
- **Liña 201** — `return result`.

**5. Walkthrough ezekusaun**

Imajen PlantVillage `color/Corn_(maize)___healthy/abc.JPG` → buka `segmented/.../abc_final_masked.jpg` → máskara → `remove_leaf` → imajen ho fundu de'it.

**6. Tipu dadus**

`source_of -> str`; `load_rgb -> ndarray`; `mask_keys -> tuple[tuple, tuple]`; `load_leaf_mask -> tuple[ndarray|None, ndarray|None]`; `remove_leaf -> ndarray`.

**7. Ligasaun OOP**

Not implemented in the current codebase (funsaun + cache globál).

**8. Impaktu integrasaun**

`main` uza atu teste se modelu haree folha ka fundu (teste "leaf-only / background-only").

**9. Checkpoint ba principiante**

**P:** Tanba sá teste "background only"?
**R:** Se modelu fó klase loos ho konfiansa aas maske folha la iha, signifika nia aprende husi fundu (atalhu) — la'ós husi moras.

#### D.7.3 `overlay`, `save_grid`, `save_ablation_grid`, `mean`

**1. Kódigu orijinál**

{{code @train/gradcam.py 204-305}}

**2. Objetivu**

Kombina imajen + heatmap (kór JET), rai grade figura (orijinál iha leten, Grad-CAM iha okos; títulu verde se loos, vermelhu se sala), grade ablasaun (orijinál / folha de'it / fundu de'it), no média seguru.

**3. Fundamentu Python**

`cv2.applyColorMap`; `np.uint8(0.55 * a + 0.45 * b)`; `plt.subplots(..., squeeze=False)`; `divmod(i, columns)`; `axes.flat`.

**4. Esplikasaun liña-ba-liña**

- **Liña 204** — `def overlay(image, cam):`
- **Liña 206** — `image = cv2.resize(image, (IMG_SIZE, IMG_SIZE))`.
- **Liña 208–211** — `heatmap = cv2.applyColorMap(np.uint8(255 * cam), cv2.COLORMAP_JET)`.
- **Liña 213** — `heatmap = cv2.cvtColor(heatmap, cv2.COLOR_BGR2RGB)`.
- **Liña 215** — `return np.uint8(0.55 * image + 0.45 * heatmap)` — kombina 55/45.
- **Liña 218** — `def save_grid(rows, title, path, columns=6):`
- **Liña 220** — Komentáriu: kada item: orijinál (leten) iha Grad-CAM (okos).
- **Liña 221** — `if not rows:`
- **Liña 222** — `return`.
- **Liña 224** — `grid_rows = int(np.ceil(len(rows) / columns))`.
- **Liña 226–231** — `figure, axes = plt.subplots(grid_rows * 2, columns, figsize=(columns * 2.4, grid_rows * 5.0), squeeze=False)`.
- **Liña 233** — `for axis in axes.flat:`
- **Liña 234** — `axis.axis("off")`.
- **Liña 236** — `for i, row in enumerate(rows):`
- **Liña 238** — `r, c = divmod(i, columns)`.
- **Liña 240** — `top = axes[r * 2, c]`.
- **Liña 241** — `bottom = axes[r * 2 + 1, c]`.
- **Liña 243** — `top.imshow(cv2.resize(row["image"], (IMG_SIZE, IMG_SIZE)))`.
- **Liña 244–247** — `top.set_title(f"true: {row['true']}\n[{row['source']}]", fontsize=8)`.
- **Liña 249** — `bottom.imshow(overlay(row["image"], row["cam"]))`.
- **Liña 251** — `color = "green" if row["true"] == row["pred"] else "red"`.
- **Liña 253–257** — `bottom.set_title(f"pred: {row['pred']} {row['confidence']:.0f}%", fontsize=8, color=color)`.
- **Liña 259** — `figure.suptitle(title)`.
- **Liña 261** — `figure.tight_layout()`.
- **Liña 263** — `figure.savefig(path, dpi=110)`.
- **Liña 265** — `plt.close(figure)`.
- **Liña 268** — `def save_ablation_grid(rows, path):`
- **Liña 270** — `if not rows:`
- **Liña 271** — `return`.
- **Liña 273–278** — `figure, axes = plt.subplots(len(rows), 3, figsize=(7.5, len(rows) * 2.6), squeeze=False)`.
- **Liña 280** — `for axis_row, row in zip(axes, rows):`
- **Liña 282** — `panels = [`
- **Liña 283** — `(row["image"], f"original\n{row['pred']} {row['confidence']:.0f}%"),`
- **Liña 284** — `(row["leaf_only_image"], f"leaf only\n..."),`
- **Liña 285** — `(row["bg_only_image"], f"background only\n..."),`
- **Liña 286** — `]`.
- **Liña 288** — `for axis, (image, text) in zip(axis_row, panels):`
- **Liña 290** — `axis.imshow(cv2.resize(image, (IMG_SIZE, IMG_SIZE)))`.
- **Liña 291** — `axis.set_title(text, fontsize=8)`.
- **Liña 292** — `axis.axis("off")`.
- **Liña 294** — `figure.suptitle("Lab images: leaf vs background test")`.
- **Liña 296** — `figure.tight_layout()`.
- **Liña 298** — `figure.savefig(path, dpi=110)`.
- **Liña 300** — `plt.close(figure)`.
- **Liña 303** — `def mean(values):`
- **Liña 305** — `return float(np.mean(values)) if values else float("nan")` — lista mamuk → NaN.

**5. Walkthrough ezekusaun**

`save_grid(8 imajen, "Grad-CAM: Blight", ".../gradcam_Blight.png", columns=4)` → grade liña 4 × koluna 4 (imajen 8 × 2 painél).

**6. Tipu dadus**

`overlay -> ndarray uint8`; `save_grid`, `save_ablation_grid -> None`; `mean -> float`.

**7. Ligasaun OOP**

Matplotlib API objetu: `figure`, `axes` (objetu `Figure`/`Axes` ho métodu `imshow`, `set_title`, `savefig`).

**8. Impaktu integrasaun**

Figura iha `results/gradcam/` ba relatóriu.

**9. Checkpoint ba principiante**

**P:** Saida mak títulu vermelhu iha grade?
**R:** Prediksaun sala (`true != pred`).

#### D.7.4 `main` — Grad-CAM iha split teste

**1. Kódigu orijinál**

{{code @train/gradcam.py 308-406}}

**2. Objetivu**

Karrega modelu iha CPU, harii `GradCAM(model, model.layer4)`, split teste hanesan `train.py`, no ba kada imajen teste: Grad-CAM, prediksaun, no (ba imajen lab ho máskara) % calor iha folha, % área folha, prediksaun folha-de'it no fundu-de'it.

**3. Fundamentu Python**

Funsaun lokál `to_tensor`, `classify`; `*_, test_paths, test_labels = split_data(...)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 308–310** — Komentáriu seksaun main.
- **Liña 312** — `def main():`
- **Liña 314** — `os.makedirs(OUTPUT_DIR, exist_ok=True)`.
- **Liña 316** — `device = torch.device("cpu")`.
- **Liña 318** — `checkpoint = torch.load(MODEL_PATH, map_location=device)`.
- **Liña 320** — `class_names = checkpoint.get("class_names", CLASS_NAMES)`.
- **Liña 322** — `model = SEResNet18(num_classes=len(class_names), pretrained=False)`.
- **Liña 324** — `model.load_state_dict(checkpoint["model_state_dict"])`.
- **Liña 326** — `model.eval()`.
- **Liña 328** — `grad_cam = GradCAM(model, model.layer4)`.
- **Liña 330** — `transform = get_transforms(img_size=IMG_SIZE, is_train=False)`.
- **Liña 332** — `def to_tensor(image):`
- **Liña 333** — `return transform(image=image)["image"].unsqueeze(0)`.
- **Liña 335** — `def classify(image):`
- **Liña 336** — `with torch.no_grad():`
- **Liña 337** — `probabilities = F.softmax(model(to_tensor(image)), dim=1)[0]`.
- **Liña 338** — `index = probabilities.argmax().item()`.
- **Liña 339** — `return class_names[index], probabilities[index].item() * 100`.
- **Liña 341–343** — Komentáriu: split teste hanesan `train.py`.
- **Liña 345** — `image_paths, labels = find_images(DATASET_DIRS)`.
- **Liña 347–350** — `*_, test_paths, test_labels = split_data(image_paths, np.array(labels))`.
- **Liña 352** — `print(f"\nRunning Grad-CAM on {len(test_paths)} test images...")`.
- **Liña 354** — `rows = []`.
- **Liña 356** — `for i, (path, label) in enumerate(zip(test_paths, test_labels)):`
- **Liña 358** — `image = load_rgb(path)`.
- **Liña 360** — `cam, pred_index, probabilities = grad_cam(to_tensor(image))`.
- **Liña 362–374** — `row = {"path", "source", "true", "pred", "confidence", "image", "cam", "heat_on_leaf": None, "leaf_area": None, "leaf_only_pred": None, "bg_only_pred": None}`.
- **Liña 376** — `if row["source"] == "lab":`
- **Liña 378** — `mask, segmented = load_leaf_mask(path)`.
- **Liña 380** — `if mask is not None:`
- **Liña 382–386** — `small_mask = cv2.resize(mask, (IMG_SIZE, IMG_SIZE), interpolation=cv2.INTER_NEAREST)`.
- **Liña 388–390** — `row["heat_on_leaf"] = float((cam * small_mask).sum() / (cam.sum() + 1e-8)) * 100` — % calor iha folha.
- **Liña 392** — `row["leaf_area"] = float(small_mask.mean()) * 100` — % imajen ne'ebé folha.
- **Liña 394** — `background_only = remove_leaf(image, mask)`.
- **Liña 396** — `row["leaf_only_image"] = segmented`.
- **Liña 397** — `row["bg_only_image"] = background_only`.
- **Liña 399** — `row["leaf_only_pred"], row["leaf_only_conf"] = classify(segmented)`.
- **Liña 400** — `row["bg_only_pred"], row["bg_only_conf"] = classify(background_only)`.
- **Liña 402** — `rows.append(row)`.
- **Liña 404** — `if (i + 1) % 100 == 0:`
- **Liña 405** — `print(f"  {i + 1}/{len(test_paths)}")`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* imajen lab Blight: `heat_on_leaf = 82%`, `leaf_area = 45%` → calor iha folha liu área folha → modelu haree folha (di'ak). `bg_only_pred = "Healthy"` → fundu la fó Blight (di'ak).

**6. Tipu dadus**

`rows`: `list[dict]`; `cam`: `ndarray [224,224]`; `small_mask`: `ndarray uint8`.

**7. Ligasaun OOP**

Uza objetu `grad_cam` (klase `GradCAM`) hanesan funsaun (`grad_cam(tensor)` → `__call__`).

**8. Impaktu integrasaun**

Rezultadu ba figura/relatóriu (D.7.5).

**9. Checkpoint ba principiante**

**P:** Saida mak `heat_on_leaf` signifika?
**R:** Persentajen mapa Grad-CAM ne'ebé monu iha folha. Se boot liu `leaf_area`, modelu fokus ba folha.

#### D.7.5 Figura, CSV no relatóriu Grad-CAM

**1. Kódigu orijinál**

{{code @train/gradcam.py 407-560}}

**2. Objetivu**

Rai grade kada klase (sample 8), grade imajen sala, grade ablasaun (2 kada klase), CSV kada imajen, no relatóriu testu: (1) akurásia teste tuir fonte (lab/field), (2) fokus Grad-CAM iha imajen lab, (3) teste folha-de'it/fundu-de'it.

**3. Fundamentu Python**

`random.Random(SEED)` — jeradór lokál; `rng.sample(lista, k)`; `csv.DictWriter(..., extrasaction="ignore")`; formatu alinhamentu `f"{x:<16}"`, `f"{x:>18}"`; `sorted(..., key=lambda x: -x[1])`; `"\n".join(lines)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 407–409** — Komentáriu: figura.
- **Liña 411** — `rng = random.Random(SEED)`.
- **Liña 413** — `for class_name in class_names:`
- **Liña 415** — `class_rows = [r for r in rows if r["true"] == class_name]`.
- **Liña 417–420** — `sample = rng.sample(class_rows, min(SAMPLES_PER_CLASS, len(class_rows)))`.
- **Liña 422–427** — `save_grid(sample, f"Grad-CAM: {class_name}", os.path.join(OUTPUT_DIR, f"gradcam_{class_name}.png"), columns=4)`.
- **Liña 429** — `wrong = [r for r in rows if r["true"] != r["pred"]]`.
- **Liña 431–435** — `save_grid(wrong, f"Grad-CAM: misclassified test images ({len(wrong)})", os.path.join(OUTPUT_DIR, "gradcam_misclassified.png"))`.
- **Liña 437** — `ablation_rows = []`.
- **Liña 439** — `for class_name in class_names:`
- **Liña 441–444** — `tested = [r for r in rows if r["true"] == class_name and r["bg_only_pred"]]`.
- **Liña 446** — `ablation_rows += rng.sample(tested, min(2, len(tested)))`.
- **Liña 448–451** — `save_ablation_grid(ablation_rows, os.path.join(OUTPUT_DIR, "leaf_vs_background.png"))`.
- **Liña 453–455** — Komentáriu: CSV kada imajen.
- **Liña 457–461** — `fields = ["path", "source", "true", "pred", "confidence", "heat_on_leaf", "leaf_area", "leaf_only_pred", "bg_only_pred"]`.
- **Liña 463–467** — `with open(os.path.join(OUTPUT_DIR, "gradcam_per_image.csv"), "w", newline="") as file:`
- **Liña 469** — `writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")` — ignora chave seluk (imajen, cam).
- **Liña 471** — `writer.writeheader()`.
- **Liña 473** — `writer.writerows(rows)`.
- **Liña 475–477** — Komentáriu: relatóriu.
- **Liña 479** — `lines = []`.
- **Liña 481** — `lines.append("1. TEST ACCURACY BY SOURCE")`.
- **Liña 482** — `lines.append(f"{'class':<16}{'lab':>18}{'field':>18}")`.
- **Liña 484** — `for class_name in class_names:`
- **Liña 486** — `cells = []`.
- **Liña 488** — `for source in ("lab", "field"):`
- **Liña 490–493** — `group = [r for r in rows if r["true"] == class_name and r["source"] == source]`.
- **Liña 495** — `if group:`
- **Liña 496** — `correct = sum(r["pred"] == class_name for r in group)` — `True` = 1.
- **Liña 497** — `cells.append(f"{correct}/{len(group)} ({100 * correct / len(group):.1f}%)")`.
- **Liña 498** — `else:`
- **Liña 499** — `cells.append("no images")`.
- **Liña 501** — `lines.append(f"{class_name:<16}{cells[0]:>18}{cells[1]:>18}")`.
- **Liña 503** — `lab_rows = [r for r in rows if r["heat_on_leaf"] is not None]`.
- **Liña 505** — `lines.append("")`.
- **Liña 506** — `lines.append("2. GRAD-CAM FOCUS ON LAB IMAGES")`.
- **Liña 507** — `lines.append("   (share of heatmap on the leaf vs share of image that is leaf;")`.
- **Liña 508** — `lines.append("    heat well above area = model looks at the leaf)")`.
- **Liña 509** — `lines.append(f"{'class':<16}{'n':>5}{'heat on leaf':>15}{'leaf area':>12}")`.
- **Liña 511** — `for class_name in class_names:`
- **Liña 513** — `group = [r for r in lab_rows if r["true"] == class_name]`.
- **Liña 515** — `if group:`
- **Liña 516–520** — `lines.append(f"{class_name:<16}{len(group):>5}{mean([...heat_on_leaf...]):>14.1f}%{mean([...leaf_area...]):>11.1f}%")`.
- **Liña 522** — `lines.append("")`.
- **Liña 523** — `lines.append("3. LEAF-ONLY / BACKGROUND-ONLY TEST (lab images)")`.
- **Liña 524** — `lines.append("   leaf only: background blacked out -> should stay correct")`.
- **Liña 525** — `lines.append("   background only: leaf erased -> should NOT give the true class confidently")`.
- **Liña 526** — `lines.append(f"{'class':<16}{'leaf-only acc':>15}   background-only predictions")`.
- **Liña 528** — `for class_name in class_names:`
- **Liña 530** — `group = [r for r in lab_rows if r["true"] == class_name]`.
- **Liña 532** — `if not group:`
- **Liña 533** — `continue`.
- **Liña 535** — `leaf_acc = 100 * mean([r["leaf_only_pred"] == class_name for r in group])`.
- **Liña 537** — `counts = {}`.
- **Liña 539** — `for r in group:`
- **Liña 540** — `counts[r["bg_only_pred"]] = counts.get(r["bg_only_pred"], 0) + 1`.
- **Liña 542–545** — `spread = ", ".join(f"{name} {count}" for name, count in sorted(counts.items(), key=lambda x: -x[1]))`.
- **Liña 547** — `lines.append(f"{class_name:<16}{leaf_acc:>14.1f}%   {spread}")`.
- **Liña 549** — `report = "\n".join(lines)`.
- **Liña 551** — `print("\n" + report)`.
- **Liña 553** — `with open(os.path.join(OUTPUT_DIR, "gradcam_report.txt"), "w") as file:`
- **Liña 554** — `file.write(report + "\n")`.
- **Liña 556** — `print(f"\nSaved figures and report to {OUTPUT_DIR}")`.
- **Liña 559** — `if __name__ == "__main__":`
- **Liña 560** — `main()`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu* liña relatóriu: `Blight          85/90 (94.4%)     84/91 (92.3%)`. (Valór loloos iha `results/gradcam/gradcam_report.txt` — la reprodús iha ne'e.)

**6. Tipu dadus**

`rng`: `random.Random`; `lines`: `list[str]`; `counts`: `dict[str, int]`; `report`: `str`.

**7. Ligasaun OOP**

`random.Random(SEED)` — objetu jeradór lokál (la muda jeradór globál).

**8. Impaktu integrasaun**

Evidénsia ba relatóriu akadémiku: modelu haree folha, la'ós fundu.

**9. Checkpoint ba principiante**

**P:** Tanba sá relatóriu fahe akurásia tuir "lab" no "field"?
**R:** Fotografia laboratóriu (fundu uniforme) fasil liu; fotografia kampu reprezenta situasaun loloos agrikultór. Fahe hatudu se modelu jeneraliza.
