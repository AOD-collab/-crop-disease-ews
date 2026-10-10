"""
The processed (marked-up) image the farmer sees in the app: the photo with
the trained CNN's Grad-CAM (ml.inference.disease_heatmap) drawn on it, plus
a banner with the predicted class, confidence and risk level. Drawing only:
nothing here decides anything about the photo.
"""

from io import BytesIO
from PIL import Image, ImageDraw, ImageFont, ImageOps
import numpy as np

PROCESSED_MAX_PX = 600           # smaller file in Drive (~30-50 KB)
LESION_RGB = (220, 20, 60)       # red: where the model saw the disease
HEAT_FLOOR = 0.35                # Grad-CAM values below this aren't tinted


def _banner_font(size: int):
    try:
        return ImageFont.load_default(size=size)   # Pillow >= 10.1
    except TypeError:
        return ImageFont.load_default()


def render_processed_image(image_bytes: bytes, prediction: dict, heatmap=None) -> bytes:
    """The photo with `heatmap` (the model's Grad-CAM, None when it
    predicted healthy: then nothing is red) tinted red, under a banner with
    class, confidence and risk. Returns JPEG bytes (at most
    PROCESSED_MAX_PX on the longer side, original aspect ratio, quality 75 —
    about 30-50 KB, so it adds little to Drive storage)."""
    img = ImageOps.exif_transpose(Image.open(BytesIO(image_bytes)).convert("RGB"))
    img.thumbnail((PROCESSED_MAX_PX, PROCESSED_MAX_PX))
    out = np.asarray(img).astype(np.float32)

    if heatmap is not None:
        heat = np.asarray(Image.fromarray(np.asarray(heatmap, dtype=np.float32))
                          .resize(img.size, Image.BILINEAR))
        strength = np.clip((heat - HEAT_FLOOR) / (1 - HEAT_FLOOR), 0, 1)
        a = (0.65 * strength)[..., None]
        out = (1 - a) * out + a * np.array(LESION_RGB)
    marked = Image.fromarray(out.clip(0, 255).astype(np.uint8))

    # banner: class · confidence · risk, plus a colour legend
    w, h = marked.size
    font_size = max(14, w // 28)
    font = _banner_font(font_size)
    banner_h = int(font_size * 3.2)
    canvas = Image.new("RGB", (w, h + banner_h), (30, 30, 30))
    canvas.paste(marked, (0, banner_h))
    draw = ImageDraw.Draw(canvas)

    cls = (prediction.get("predicted_class") or "unknown").replace("_", " ")
    conf = prediction.get("prediction_confidence")
    risk = prediction.get("risk_level") or "UNKNOWN"
    line1 = f"{cls}" + (f"  {conf * 100:.0f}%" if isinstance(conf, (int, float)) else "") + f"   Risk: {risk}"
    pad = font_size // 2
    draw.text((pad, pad), line1, fill=(255, 255, 255), font=font)

    if heatmap is not None:
        y2 = pad + int(font_size * 1.4)
        box = font_size
        draw.rectangle([pad, y2, pad + box, y2 + box], fill=LESION_RGB)
        draw.text((pad + box + pad, y2), "disease area (model)", fill=(230, 230, 230), font=font)

    buf = BytesIO()
    canvas.save(buf, format="JPEG", quality=75)
    return buf.getvalue()
