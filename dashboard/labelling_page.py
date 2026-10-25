"""
"Label photos" page: the admin labels analysed farmer photos (REVIEW_DIR);
each moves to LABELLED_TRAIN_DIR/<class>/ for the next training. Decisions
go to labels_log.csv and image_reviews. A farmer's result never changes.
"""

import csv
from datetime import datetime
from pathlib import Path

import streamlit as st

import config.settings as settings
from storage import reviews
from storage.database import SessionLocal, Prediction

# training folder name -> button label
CLASSES = {
    "healthy": "Healthy",
    "common_rust": "Common rust",
    "gray_leaf_spot": "Gray leaf spot",
    "northern_leaf_blight": "Northern leaf blight",
}
PAGE_SIZE = 10


def pending_photos() -> list[Path]:
    """Photos waiting in REVIEW_DIR, oldest first."""
    if not settings.REVIEW_DIR.exists():
        return []
    return sorted(settings.REVIEW_DIR.glob("*.jpg"), key=lambda p: p.stat().st_mtime)


def labelled_counts() -> dict[str, int]:
    return {c: len(list((settings.LABELLED_TRAIN_DIR / c).glob("*.jpg"))) for c in CLASSES}   # no folder -> 0


def _log(photo: Path, decision: str, ai_class: str | None, admin: str | None) -> None:
    log = settings.LABELLED_TRAIN_DIR / "labels_log.csv"
    log.parent.mkdir(parents=True, exist_ok=True)
    new = not log.exists()
    with open(log, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new:
            writer.writerow(["image_id", "label", "ai_prediction", "labelled_by", "labelled_at"])
        writer.writerow([photo.stem, decision, ai_class or "", admin or "",
                         datetime.now().isoformat(timespec="seconds")])


def label_photo(photo: Path, class_name: str, ai_class: str | None = None, admin: str | None = None) -> Path:
    """Move a reviewed photo into the training folder of `class_name`."""
    if class_name not in CLASSES:
        raise ValueError(f"unknown class {class_name!r}")
    target_dir = settings.LABELLED_TRAIN_DIR / class_name
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / photo.name
    photo.replace(target)
    _log(photo, class_name, ai_class, admin)
    reviews.record(photo.stem, "labelled", class_name, ai_class, admin)
    return target


def discard_photo(photo: Path, ai_class: str | None = None, admin: str | None = None) -> None:
    """Drop a photo that shouldn't be trained on (blurry, not maize, unsure)."""
    photo.unlink(missing_ok=True)
    _log(photo, "discarded", ai_class, admin)
    reviews.record(photo.stem, "discarded", None, ai_class, admin)


def discard_all(admin: str | None = None) -> int:
    """Discards every photo waiting in REVIEW_DIR; returns how many."""
    photos = pending_photos()
    classes = {p.image_id: p.predicted_class for p in _ai_predictions([p.stem for p in photos]).values()}
    for photo in photos:
        discard_photo(photo, classes.get(photo.stem), admin)
    return len(photos)


def _ai_predictions(image_ids: list[str]) -> dict[str, Prediction]:
    with SessionLocal() as session:
        rows = session.query(Prediction).filter(Prediction.image_id.in_(image_ids)).all()
        session.expunge_all()
        return {p.image_id: p for p in rows}


def render():
    admin = st.session_state.get("admin_user", {}).get("username")
    photos = pending_photos()
    counts = labelled_counts()

    cols = st.columns(len(CLASSES) + 1)
    cols[0].metric("Waiting to be labelled", len(photos))
    for col, (c, label) in zip(cols[1:], CLASSES.items()):
        col.metric(f"In training: {label}", counts[c])
    st.caption(f"Photos are in `{settings.REVIEW_DIR}`. Labelled photos go to "
               f"`{settings.LABELLED_TRAIN_DIR}\\<class>` and are used by the next training run "
               "(Model training page). Farmers never see these labels.")

    if msg := st.session_state.pop("label_msg", None):
        st.success(msg)
    if not photos:
        st.info("No photos waiting. Every photo a farmer uploads is added here after it is analysed.")
        return

    with st.expander(f"Delete all {len(photos)} waiting photos", icon="🗑️"):
        st.caption("Removes every photo waiting to be labelled. Photos already labelled for training, "
                   "the farmers' results and their photos in Google Drive are not affected.")
        confirm = st.checkbox("Yes, delete all waiting photos", key="discard_all_confirm")
        if st.button("Delete all", type="primary", disabled=not confirm):
            n = discard_all(admin)
            st.session_state["label_msg"] = f"Deleted {n} waiting photo(s)."
            st.rerun()

    st.divider()
    page_photos = photos[:PAGE_SIZE]
    predictions = _ai_predictions([p.stem for p in page_photos])
    for photo in page_photos:
        pred = predictions.get(photo.stem)
        ai_class = pred.predicted_class if pred else None
        with st.container(border=True):
            left, right = st.columns([1, 2])
            with left:
                st.image(str(photo), width=260)
            with right:
                st.markdown(f"`{photo.stem}`")
                if pred:
                    conf = f" ({pred.prediction_confidence * 100:.2f}%)" if pred.prediction_confidence else ""
                    st.caption(f"AI prediction: {CLASSES.get(ai_class, ai_class)}{conf}")
                st.markdown("**What disease is on this leaf?**")
                buttons = st.columns(len(CLASSES))
                for b, (c, label) in zip(buttons, CLASSES.items()):
                    if b.button(label, key=f"label_{c}_{photo.stem}", width="stretch"):
                        label_photo(photo, c, ai_class, admin)
                        st.session_state["label_msg"] = f"Labelled `{photo.stem[:8]}…` as {label}."
                        st.rerun()
                if st.button("Discard (blurry, not maize, not sure)", key=f"discard_{photo.stem}", icon="🗑️"):
                    discard_photo(photo, ai_class, admin)
                    st.session_state["label_msg"] = f"Discarded `{photo.stem[:8]}…`."
                    st.rerun()
    if len(photos) > PAGE_SIZE:
        st.caption(f"Showing the {PAGE_SIZE} oldest of {len(photos)}; the next ones appear as you label these.")
