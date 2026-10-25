"""
Replace the Android app's old leaf logo with your own logo — everywhere —
in one command (no Android Studio clicking needed).

    python scripts\\set_app_logo.py A:\\final\\app_logo.jpg
    python scripts\\set_app_logo.py A:\\final\\app_logo.jpg --background FFFFFF

What it does:
  1. Backs up every file it will change to backup\\logo_<date-time>\\
  2. App icon (phone home screen): creates the launcher icon in all sizes
     (mipmap-mdpi ... mipmap-xxxhdpi, round version, Android 8+ adaptive
     icon) from your logo on a background colour.
  3. Login screen: shows your logo instead of the leaf badge.
  4. Home screen: removes the leaf icon from the "Scan" button (a button
     icon is painted one colour, so a picture can't be shown there).
  5. Deletes the old leaf logo files.

--background : colour behind the logo, as 6 hex digits (FFFFFF = white).
               If left out, it is taken from the corners of your logo, so a
               JPG's own background blends in with no visible box.
--size       : how much of the icon's safe area the logo fills (0.5-1.0,
               default 0.9). Lower it if the logo looks cut off.
--no-trim    : keep the empty border around the logo (by default it is cut
               away, like Android Studio's "Trim: Yes").

Running it again with another logo is fine: it simply replaces the logo.
Afterwards: Build > Clean Project, Build > Make Project, uninstall the app
from the phone, then Run.
"""
import argparse
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageOps, ImageStat

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "android_app" / "app" / "src" / "main" / "res"
DENSITIES = {"mdpi": 1.0, "hdpi": 1.5, "xhdpi": 2.0, "xxhdpi": 3.0, "xxxhdpi": 4.0}

OLD_FILES = [  # the old leaf logo, no longer used after this script
    "drawable/ic_leaf.xml",
    "drawable/bg_logo_badge.xml",
    "drawable/ic_launcher_foreground.xml",
    "drawable/ic_launcher_background.xml",
    "mipmap/ic_launcher.xml",
    "mipmap/ic_launcher_round.xml",
]
CHANGED_FILES = [
    "layout/activity_login.xml",
    "layout/activity_home.xml",
    "mipmap-anydpi-v26/ic_launcher.xml",
    "mipmap-anydpi-v26/ic_launcher_round.xml",
    "values/ic_launcher_background.xml",
    "drawable/app_logo.png",
] + [f"mipmap-{d}/{n}" for d in DENSITIES for n in
     ("ic_launcher.png", "ic_launcher_round.png", "ic_launcher_foreground.png")]

NEW_LOGIN_LOGO = '''<ImageView
            android:layout_width="96dp"
            android:layout_height="96dp"
            android:layout_marginTop="24dp"
            android:layout_marginBottom="16dp"
            android:scaleType="fitCenter"
            android:src="@drawable/app_logo"
            android:contentDescription="@string/app_name" />'''

ADAPTIVE_XML = '''<?xml version="1.0" encoding="utf-8"?>
<adaptive-icon xmlns:android="http://schemas.android.com/apk/res/android">
    <background android:drawable="@color/ic_launcher_background" />
    <foreground android:drawable="@mipmap/ic_launcher_foreground" />
</adaptive-icon>
'''


def parse_color(text):
    text = text.strip().lstrip("#")
    if not re.fullmatch(r"[0-9a-fA-F]{6}", text):
        raise SystemExit(f"--background must be 6 hex digits like FFFFFF, not {text!r}")
    return tuple(int(text[i:i + 2], 16) for i in (0, 2, 4))


def corner_color(img):
    """Average colour of the 4 corners — a JPG logo's background."""
    rgb = img.convert("RGB")
    w, h = rgb.size
    k = max(1, min(w, h) // 20)
    means = [ImageStat.Stat(rgb.crop((x0, y0, x0 + k, y0 + k))).mean
             for x0, y0 in ((0, 0), (w - k, 0), (0, h - k), (w - k, h - k))]
    return tuple(round(sum(m[i] for m in means) / 4) for i in range(3))


def trim(img, bg):
    """Cuts away empty border around the logo: pixels (almost) the same as
    the background colour, or fully transparent. Like Android Studio's
    "Trim: Yes"."""
    rgb = img.convert("RGB")
    diff = ImageChops.difference(rgb, Image.new("RGB", rgb.size, bg)).convert("L")
    diff = diff.point(lambda v: 255 if v > 24 else 0)            # ignore JPG noise
    alpha = img.getchannel("A").point(lambda v: 255 if v > 16 else 0)
    box = ImageChops.multiply(diff, alpha).getbbox()
    if not box:
        return img
    pad = max(2, round(max(box[2] - box[0], box[3] - box[1]) * 0.02))
    box = (max(0, box[0] - pad), max(0, box[1] - pad),
           min(img.width, box[2] + pad), min(img.height, box[3] + pad))
    return img.crop(box)


def fit(logo, box):
    """Logo scaled to fit inside a box x box square, aspect ratio kept."""
    img = logo.copy()
    img.thumbnail((box, box), Image.LANCZOS)
    return img


def centered(canvas, img):
    canvas.paste(img, ((canvas.width - img.width) // 2, (canvas.height - img.height) // 2),
                 img if img.mode == "RGBA" else None)
    return canvas


def circle_mask(size):
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    return mask


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("logo", help="your logo file (JPG or PNG)")
    p.add_argument("--background", help="background colour, e.g. FFFFFF (default: from the logo's corners)")
    p.add_argument("--size", type=float, default=0.9, help="logo size inside the safe area, 0.5-1.0 (default 0.9)")
    p.add_argument("--no-trim", action="store_true", help="keep the empty border around the logo")
    args = p.parse_args()

    logo_path = Path(args.logo)
    if not logo_path.exists():
        raise SystemExit(f"Logo file not found: {logo_path}")
    if not RES.is_dir():
        raise SystemExit(f"Android res folder not found: {RES}\n(run this from the project folder)")
    size = min(1.0, max(0.5, args.size))

    logo = ImageOps.exif_transpose(Image.open(logo_path)).convert("RGBA")
    bg = parse_color(args.background) if args.background else corner_color(logo)
    bg_hex = "%02X%02X%02X" % bg
    print(f"Logo: {logo_path.name} ({logo.width}x{logo.height}), background #{bg_hex}")
    if not args.no_trim:
        trimmed = trim(logo, bg)
        if trimmed.size != logo.size:
            print(f"   empty border trimmed: {logo.width}x{logo.height} -> {trimmed.width}x{trimmed.height}")
        logo = trimmed

    # ---- 1. backup ---------------------------------------------------------
    backup = ROOT / "backup" / f"logo_{datetime.now():%Y%m%d_%H%M%S}"
    n = 2
    while backup.exists():  # never overwrite an earlier backup
        backup = backup.with_name(f"logo_{datetime.now():%Y%m%d_%H%M%S}_{n}")
        n += 1
    for rel in OLD_FILES + CHANGED_FILES:
        src = RES / rel
        if src.exists():
            (backup / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, backup / rel)
    print(f"1. Backup of the current files: {backup}")

    # ---- 2. launcher icon, all sizes -------------------------------------
    for d, scale in DENSITIES.items():
        folder = RES / f"mipmap-{d}"
        folder.mkdir(exist_ok=True)
        # Android 8+ adaptive icon foreground: 108dp canvas, logo inside the
        # 66dp safe zone (the part never cut off by circle/square masks).
        canvas = round(108 * scale)
        fg = Image.new("RGBA", (canvas, canvas), (0, 0, 0, 0))
        centered(fg, fit(logo, round(66 * scale * size)))
        fg.save(folder / "ic_launcher_foreground.png")
        # Older phones: a finished 48dp icon, square and round.
        px = round(48 * scale)
        legacy = Image.new("RGBA", (px, px), bg + (255,))
        centered(legacy, fit(logo, round(px * 0.72 * size)))
        legacy.save(folder / "ic_launcher.png")
        round_icon = Image.new("RGBA", (px, px), (0, 0, 0, 0))
        round_icon.paste(legacy, (0, 0), circle_mask(px))
        round_icon.save(folder / "ic_launcher_round.png")
    (RES / "mipmap-anydpi-v26").mkdir(exist_ok=True)
    for name in ("ic_launcher.xml", "ic_launcher_round.xml"):
        (RES / "mipmap-anydpi-v26" / name).write_text(ADAPTIVE_XML, encoding="utf-8")
    (RES / "values" / "ic_launcher_background.xml").write_text(
        '<?xml version="1.0" encoding="utf-8"?>\n<resources>\n'
        f'    <color name="ic_launcher_background">#{bg_hex}</color>\n</resources>\n',
        encoding="utf-8")
    print("2. App icon created (mdpi ... xxxhdpi, round, adaptive)")

    # ---- 3. login screen ---------------------------------------------------
    screen_logo = fit(logo, 512)
    screen_logo.save(RES / "drawable" / "app_logo.png")
    login = RES / "layout" / "activity_login.xml"
    text = login.read_text(encoding="utf-8")
    old_badge = re.compile(r"<FrameLayout(?:(?!<FrameLayout).)*?@drawable/ic_leaf.*?</FrameLayout>", re.S)
    if old_badge.search(text):
        text = old_badge.sub(NEW_LOGIN_LOGO, text, count=1)
        login.write_text(text, encoding="utf-8")
        print("3. Login screen: leaf replaced by your logo")
    elif "@drawable/app_logo" in text:
        print("3. Login screen: already shows your logo (updated the picture)")
    else:
        print("3. Login screen: WARNING - could not find the leaf block; not changed")

    # ---- 4. home screen button --------------------------------------------
    home = RES / "layout" / "activity_home.xml"
    text = home.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    icon_attrs = ('app:icon="@drawable/ic_leaf"', "app:iconGravity=", "app:iconTint=", "app:iconSize=")
    if any('app:icon="@drawable/ic_leaf"' in line for line in lines):
        kept = [line for line in lines if not line.strip().startswith(icon_attrs)]
        home.write_text("".join(kept), encoding="utf-8")
        print("4. Home screen: leaf removed from the Scan button")
    else:
        print("4. Home screen: no leaf on the Scan button (nothing to do)")

    # ---- 5. delete the old logo files -------------------------------------
    removed = [rel for rel in OLD_FILES if (RES / rel).exists()]
    for rel in removed:
        (RES / rel).unlink()
    if (RES / "mipmap").is_dir() and not any((RES / "mipmap").iterdir()):
        try:
            (RES / "mipmap").rmdir()
        except OSError:
            # Windows refuses while another program (e.g. Android Studio)
            # has the folder open. An empty folder is harmless for the build.
            print("   (the now-empty res\\mipmap folder could not be removed - harmless, you may delete it later)")
    print("5. Old logo files deleted: " + (", ".join(removed) if removed else "none left"))

    # ---- safety check: nothing still points at a deleted file ---------------
    old_names = ("ic_leaf", "bg_logo_badge", "drawable/ic_launcher_foreground", "drawable/ic_launcher_background")
    leftovers = [f"{f.relative_to(RES.parent)} still uses {name}"
                 for f in list(RES.rglob("*.xml")) + list((RES.parent / "java").rglob("*.kt"))
                 if (body := f.read_text(encoding="utf-8", errors="ignore"))
                 for name in old_names if name in body]
    if leftovers:
        print("\nWARNING - these still refer to the old logo (the build will fail until fixed):")
        for x in leftovers:
            print("  " + x)
        return 1
    print("\nDone. In Android Studio: Build > Clean Project, then Build > Make Project.")
    print("On the phone: uninstall the app, then press Run (phones cache old icons).")
    print(f"To undo: copy the files from {backup} back into {RES}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
