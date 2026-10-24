"""
"Image management" page of the ADMIN dashboard (dashboard/app.py; the whole
dashboard is behind the ADMIN login).

PostgreSQL is the catalog (storage/image_catalog.py: the view
v_admin_image_catalog), Google Drive the repository of originals and
results, local storage a managed cache (storage/image_cache.py). The page
shows database record counts and file counts side by side, a filterable
gallery (newest upload first), one image's details with its original and
processed image side by side, and storage management: usage per category,
reconciliation of the cached files against the catalog, a dry-run clean-up
preview and a confirmed, audited clean-up.

No filesystem path above a category folder, Drive link or credential is
shown; images reach the browser through Streamlit only after this
server-side code has read them.
"""

import pandas as pd
import streamlit as st

from config.localtime import format_local, now_local
from storage import image_cache, image_catalog, reviews
from storage.database import SessionLocal, Image, ProcessingStatus
from storage.pipeline_submit import UPLOAD_REJECTED_PREFIX
from storage.image_catalog import CatalogFilters, NO_PREDICTION, NO_RISK

PAGE_SIZE = 12
CACHE_FILTERS = {"Any": None, "Cached, verified": {"valid", "unverified"}, "Not cached": {"missing"},
                 "Cached copy does not match": {"invalid"}}
REVIEW_FILTERS = {"Any": None, **{v: k for k, v in image_cache.REVIEW_LABELS.items()}}
_FILTER_KEYS = ("im_id", "im_farmer", "im_location", "im_dates", "im_status", "im_class", "im_risk",
                "im_review", "im_cache")


def _when(dt) -> str:
    return format_local(dt) if dt is not None and not pd.isna(dt) else "—"


def _pct(v) -> str:
    return f"{v * 100:.2f}%" if v is not None and not pd.isna(v) else "—"


def _admin() -> str | None:
    return (st.session_state.get("admin_user") or {}).get("username")


def _cache(row) -> image_cache.CacheCheck:
    return image_cache.check_cached(row["user_id"], row["image_id"], row.get("content_hash"))


# --------------------------------------------------------------- summary
def _summary():
    db = image_catalog.summary()
    records = image_cache.reconcile()
    verdicts = pd.Series([r.verdict for r in records]).value_counts().to_dict() if records else {}
    usage = image_cache.usage()
    cached = next(u["files"] for u in usage if u["category"] == "original_cache")
    total_bytes = sum(u["bytes"] for u in usage)

    st.markdown("**Database records** (farmer images in PostgreSQL)")
    c = st.columns(6)
    c[0].metric("Farmer images", db["total"])
    c[1].metric("Completed", db["completed"])
    c[2].metric("Processing", db["processing"], help="Ingested or being processed.")
    c[3].metric("Failed", db["failed"])
    c[4].metric("With a prediction", db["with_prediction"])
    c[5].metric("Awaiting labels", len(image_cache.review_ids()),
                help="Photos waiting on the Label photos page (files in the review folder).")
    st.markdown("**Files on this laptop**")
    c = st.columns(4)
    c[0].metric("Cached original photos", cached, help="Files in the original-photo cache.")
    c[1].metric("Local storage used", f"{total_bytes / 1e6:,.2f} MB",
                help="All image categories below; a hard-linked file is counted once.")
    c[2].metric("Missing or invalid copies", verdicts.get("missing", 0) + verdicts.get("invalid", 0),
                help="Catalog images without a local copy, or whose copy differs from the original.")
    c[3].metric("Potentially orphaned files",
                verdicts.get("orphan", 0) + verdicts.get("orphan_duplicate", 0) + verdicts.get("wrong_folder", 0),
                help="Cached files that no catalog record accounts for. Never deleted automatically.")


# --------------------------------------------------------------- filters + gallery
def _filters(opts) -> tuple:
    with st.container(border=True):
        a, b, c, d = st.columns(4)
        image_id = a.text_input("Image ID (or its beginning)", key="im_id")
        farmers = {"": "All farmers", **opts["farmers"]}
        farmer = b.selectbox("Farmer", list(farmers), format_func=farmers.get, key="im_farmer")
        location = c.selectbox("Municipality", [""] + opts["locations"],
                               format_func=lambda v: v or "All municipalities", key="im_location")
        dates = d.date_input("Uploaded between", value=(), key="im_dates",
                             help="Timor-Leste dates, both ends included.")
        e, f, g, h, i = st.columns(5)
        statuses = e.multiselect("Status", opts["statuses"], key="im_status")
        classes = f.multiselect("Predicted disease", opts["classes"] + [NO_PREDICTION], key="im_class")
        risks = g.multiselect("Risk level", ["HIGH", "MEDIUM", "LOW", NO_RISK], key="im_risk")
        review = h.selectbox("Review status", list(REVIEW_FILTERS), key="im_review")
        cache = i.selectbox("Local copy", list(CACHE_FILTERS), key="im_cache")
        if st.button("Reset filters", icon="🔄"):
            for k in _FILTER_KEYS:
                st.session_state.pop(k, None)
            st.rerun()
    dates = tuple(dates) if dates else ()
    f = CatalogFilters(image_id=image_id, farmer=farmer, location=location,
                       uploaded_from=dates[0] if len(dates) >= 1 else None,
                       uploaded_to=dates[1] if len(dates) == 2 else (dates[0] if len(dates) == 1 else None),
                       statuses=statuses, predicted_classes=classes, risk_levels=risks)
    return f, REVIEW_FILTERS[review], CACHE_FILTERS[cache]


def _gallery(filters, review_wanted, cache_wanted):
    if review_wanted is None and cache_wanted is None:
        total = image_catalog.count(filters)
        n_pages = max(1, -(-total // PAGE_SIZE))
        page = int(st.number_input("Page", 1, n_pages, 1, key="im_page")) if n_pages > 1 else 1
        rows = image_catalog.query(filters, limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE)
    else:
        # review and cache status live on disk, so those two filters run on
        # the (database-filtered) metadata rows, never on full images
        rows = [r for r in image_catalog.query(filters)
                if (review_wanted is None or image_cache.review_state(r["image_id"], r["review_decision"])
                    == review_wanted)
                and (cache_wanted is None or _cache(r).status in cache_wanted)]
        total = len(rows)
        n_pages = max(1, -(-total // PAGE_SIZE))
        page = int(st.number_input("Page", 1, n_pages, 1, key="im_page")) if n_pages > 1 else 1
        rows = rows[(page - 1) * PAGE_SIZE: page * PAGE_SIZE]
    st.caption(f"{total} image record(s) — page {page} of {n_pages}, newest upload first.")
    if not total:
        st.info("No farmer images match these filters." if image_catalog.count() else
                "No farmer images yet. Photos appear here once the Drive watcher has registered them.")
        return
    cols = st.columns(4)
    for n, r in enumerate(rows):
        with cols[n % 4], st.container(border=True):
            check = _cache(r)
            thumb = image_cache.thumbnail(str(check.path)) if check.status in ("valid", "unverified") else None
            if thumb:
                st.image(thumb, width="stretch")
            else:
                st.caption("No preview: " + ("not cached on this laptop" if check.status == "missing"
                                             else "cached copy does not match the original"))
            st.markdown(f"`{r['image_id'][:8]}…` · **{r['status']}**")
            pred = (f"{str(r['predicted_class']).replace('_', ' ')} · {_pct(r['prediction_confidence'])}"
                    if r["predicted_class"] else "No prediction")
            st.caption(f"Model: {pred} · risk {r['risk_level'] or '—'}")
            st.caption(f"{r['farmer_email'] or 'unknown farmer'} · {r['location'] or 'no location'}")
            st.caption(f"Uploaded {_when(r['uploaded_at'])} · last result {_when(r['predicted_at'] or r['processed_at'])}")
            st.caption(f"{check.label} · {image_cache.REVIEW_LABELS[image_cache.review_state(r['image_id'], r['review_decision'])]}")
            if st.button("Details", key=f"im_open_{r['image_id']}", width="stretch"):
                st.session_state["im_selected"] = r["image_id"]
                st.rerun()


# --------------------------------------------------------------- details
def _details(image_id: str, processed_image):
    row = image_catalog.get(image_id)
    if row is None:
        st.warning("That image is no longer in the catalog.")
        st.session_state.pop("im_selected", None)
        return
    with st.container(border=True):
        head, close = st.columns([5, 1])
        head.subheader(f"Image {row['image_id']}")
        if close.button("Close", icon="✖", key="im_close"):
            st.session_state.pop("im_selected", None)
            st.rerun()

        left, right = st.columns(2)
        original = image_cache.original_for_display(row, performed_by=_admin())
        with left:
            if original is not None:
                st.image(original.data, caption=f"Original — {original.source}", width="stretch")
            else:
                st.info("The original photo is not on this laptop and could not be fetched from Google Drive.")
        with right:
            processed = processed_image(row["image_id"], row["farmer_email"]) \
                if processed_image and row["status"] == "COMPLETED" else None
            if processed is not None:
                st.image(processed, caption="Processed — the marked-up image the farmer received",
                         width="stretch")
            else:
                st.info({"COMPLETED": "No processed image was found in the farmer's Drive results folder.",
                         "FAILED": "Processing failed, so there is no processed image."}
                        .get(row["status"], "Not processed yet, so there is no processed image."))
        st.caption("Both images belong to this record. Hover an image and use its expand button to enlarge it.")

        model, admin = st.columns(2)
        with model, st.container(border=True):
            st.markdown("**Model prediction** (not a confirmed diagnosis)")
            if row["predicted_class"]:
                st.write({"Predicted disease": row["predicted_class"],
                          "Confidence": _pct(row["prediction_confidence"]),
                          "Uncertain": "yes" if row["is_uncertain"] else "no",
                          "Risk level": row["risk_level"] or "—",
                          "Model version": row["model_version"] or "—",
                          "Predicted at": _when(row["predicted_at"])})
            else:
                st.caption("No prediction for this image.")
        with admin, st.container(border=True):
            st.markdown("**Administrator review**")
            state = image_cache.review_state(row["image_id"], row["review_decision"])
            st.write({"Review status": image_cache.REVIEW_LABELS[state],
                      "Confirmed label": row["confirmed_class"] or "—",
                      "Reviewed by": row["reviewed_by"] or "—", "Reviewed at": _when(row["reviewed_at"])})
            if state == "awaiting_label" and st.button("Open Label photos", icon="🏷️", key="im_to_label"):
                st.session_state["page"] = "Label Photos"
                st.rerun()

        info, files = st.columns(2)
        with info, st.container(border=True):
            st.markdown("**Record and processing**")
            st.write({"Farmer": f"{row['farmer_name'] or ''} {row['farmer_email'] or row['user_id']}".strip(),
                      "Farmer account": row["farmer_status"] or "—", "Municipality": row["location"] or "—",
                      "Status": row["status"], "Captured": _when(row["captured_at"]),
                      "Uploaded": _when(row["uploaded_at"]), "Ingested": _when(row["ingested_at"]),
                      "Processed": _when(row["processed_at"]),
                      "Result delivered to the farmer": _when(row["result_published_at"])})
            if row["error_message"]:
                st.error(f"Processing error: {row['error_message']}")
            if (row["status"] == "FAILED"
                    and not (row["error_message"] or "").startswith(UPLOAD_REJECTED_PREFIX)
                    and st.button("Retry processing", icon="🔄", key="im_retry")):
                _retry(row["image_id"])
        with files, st.container(border=True):
            st.markdown("**Local copy and duplicates**")
            check = _cache(row)
            st.write({"Local copy": check.label,
                      "Checksum (SHA-256)": (row["content_hash"] or "—")[:16] + ("…" if row["content_hash"] else ""),
                      "Stored in Google Drive": "yes" if (row["image_path"] or "").startswith("gdrive:") else "no"})
            same = image_catalog.same_content(row["image_id"], row["content_hash"])
            if same:
                st.caption("The same photo (identical bytes) was also uploaded as separate records; "
                           "they are kept separate:")
                st.dataframe(pd.DataFrame([{"image_id": s["image_id"], "farmer": s["farmer_email"],
                                            "same farmer": s["user_id"] == row["user_id"],
                                            "uploaded": _when(s["uploaded_at"]), "status": s["status"]}
                                           for s in same]), hide_index=True)
            else:
                st.caption("No other record has the same content.")


def _retry(image_id: str):
    """Send a FAILED image through the existing pipeline again — only while
    it is still FAILED (the state is re-read, not taken from the page)."""
    from storage.pipeline_submit import submit_image_for_processing
    with SessionLocal() as s:
        img = s.get(Image, image_id)
        if img is None or img.status != ProcessingStatus.FAILED:
            st.session_state["im_msg"] = "Not retried: the image is no longer FAILED."
        else:
            try:
                submit_image_for_processing(s, img)
                st.session_state["im_msg"] = f"Sent {image_id[:8]}… to the pipeline again."
            except Exception as e:  # noqa: BLE001
                st.session_state["im_msg"] = f"Could not queue it: {e}"
    st.rerun()


# --------------------------------------------------------------- storage
def _storage():
    usage = image_cache.usage()
    st.markdown("**Disk use by category**")
    st.dataframe(pd.DataFrame([{"Category": image_cache.CATEGORIES[u["category"]], "Files": u["files"],
                                "MB": round(u["bytes"] / 1e6, 1),
                                "Shared with another category (MB)": round(u["shared_bytes"] / 1e6, 1),
                                "Note": u["note"]} for u in usage]), hide_index=True, width="stretch")

    records = image_cache.reconcile()
    st.markdown("**Reconciliation of the original-photo cache with the database**")
    counts = pd.Series([image_cache.VERDICTS[r.verdict] for r in records]).value_counts()
    st.dataframe(counts.rename_axis("Result").reset_index(name="Files / records"), hide_index=True)
    in_use = [r for r in records if r.in_use]
    if in_use:
        st.caption(f"{len(in_use)} file(s) belong to photos being processed right now; they are never cleaned.")
    attention = [r.as_row() for r in records if r.verdict not in ("matched",)]
    if attention:
        with st.expander(f"Files and records needing attention ({len(attention)})"):
            table = pd.DataFrame(attention)
            st.dataframe(table, hide_index=True, width="stretch")
            st.download_button("Download this list (CSV)", table.to_csv(index=False).encode("utf-8"),
                               file_name=f"image_cache_reconciliation_{now_local():%Y%m%d_%H%M}.csv")

    st.markdown("**Clean-up**")
    st.caption("Choose what to look for, preview the exact files, then confirm. Review and training images, "
               "photos being processed and running scalability tests are never offered. Originals in Google "
               "Drive are never touched.")
    kinds = st.multiselect("Look for", list(image_cache.CLEANUP_KINDS), format_func=image_cache.CLEANUP_KINDS.get,
                           key="im_clean_kinds")
    if st.button("Preview clean-up (dry run)", icon="🔍", disabled=not kinds):
        st.session_state["im_preview"] = {"kinds": list(kinds),
                                          "rows": [c.rel for c in image_cache.cleanup_preview(kinds)]}
    preview = st.session_state.get("im_preview")
    if preview and preview["kinds"] == list(kinds):
        candidates = [c for c in image_cache.cleanup_preview(kinds) if c.rel in set(preview["rows"])]
        if not candidates:
            st.success("Nothing to clean up.")
        else:
            st.dataframe(pd.DataFrame([c.as_row() for c in candidates]), hide_index=True, width="stretch")
            mb = sum(c.size for c in candidates) / 1e6
            st.warning(f"{len(candidates)} file(s), {mb:,.2f} MB would be deleted from this laptop.")
            if "orphan_cache" in kinds:
                st.caption("Files without a database record cannot be fetched again by this app.")
            ok = st.checkbox(f"I checked this list: delete these {len(candidates)} file(s)", key="im_clean_ok")
            if st.button(f"Delete {len(candidates)} file(s)", icon="🗑️", type="primary", disabled=not ok):
                report = image_cache.cleanup(kinds, [c.rel for c in candidates], performed_by=_admin())
                st.session_state.pop("im_preview", None)
                st.session_state["im_msg"] = (f"Deleted {report['deleted']} file(s) ({report['bytes'] / 1e6:,.2f} MB); "
                                              f"skipped {len(report['skipped'])}, failed {len(report['failed'])}. "
                                              "Recorded in the audit log.")
                st.rerun()

    log = image_cache.recent_audit()
    if log:
        with st.expander("Audit log (latest 50 storage actions)"):
            st.dataframe(pd.DataFrame([{**r, "time": _when(r["time"])} for r in log]), hide_index=True,
                         width="stretch")


def render(processed_image=None):
    """`processed_image(image_id, email)` -> bytes or None (dashboard/app.py
    finds the marked-up image in the farmer's Drive results folder)."""
    if not st.session_state.get("im_reviews_imported"):
        reviews.import_labels_log()          # one-way backfill of earlier label decisions
        st.session_state["im_reviews_imported"] = True
    if msg := st.session_state.pop("im_msg", None):
        st.info(msg)
    _summary()
    gallery_tab, storage_tab = st.tabs(["Images", "Storage"])
    with gallery_tab:
        if st.session_state.get("im_selected"):
            _details(st.session_state["im_selected"], processed_image)
        filters, review_wanted, cache_wanted = _filters(image_catalog.options())
        _gallery(filters, review_wanted, cache_wanted)
    with storage_tab:
        _storage()
