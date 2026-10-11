"""
"Scalability test" page of the ADMIN dashboard (dashboard/app.py).

Front end for scalability/run.py: choose the photo source, the number of
photos and the number of iterations, start the test (as a background
process, like the Model training page), then see for the latest test:
  * the two averages — execution time per iteration and processing time per
    photo (scalability/report.series_averages);
  * "Disease data": where the diseases the CNN found in the test photos are
    — photos per municipality and disease, a GPS map, HIGH risk per
    municipality. These are the TEST's own CNN results (scale_test_items),
    never a farmer's diagnosis;
  * a gallery of every photo, like Pending Review: before and after
    preprocessing, with its result (previews made after the timer stopped).
The test runs in its own worker, isolated from the live pipeline
(scalability/worker.py); every other measurement is in the CSV files under
storage/scalability_reports/<run_id>/.
"""

import json
import os
import subprocess
import sys
from datetime import datetime

import plotly.express as px
import streamlit as st

from config.settings import BASE_DIR
from config.localtime import format_local
from kafka.config import broker_reachable
from storage.database import SessionLocal, ScaleTestRun, ScaleTestItem
from scalability import run as runner, reset as test_reset
from scalability.report import REPORTS_DIR, series_averages, items_frame
from scalability.worker import preview_paths
from spark.analytics import high_risk_by_location
from dashboard.theme import disease_colors, STATUS_BADGE_COLOR

LOG_DIR = REPORTS_DIR / "logs"
SIZES = [100, 500, 1000, 5000, 10000]
GALLERY_PAGE_SIZE = 24
GALLERY_COLUMNS = 4


def _launch(args: list, console: bool = False) -> str:
    """Starts `python -m scalability.run <args>` in the background; returns
    its log file. `console` (Windows): in its own command prompt window
    titled EWS-ScalabilityTest, showing the test live like the Spark and
    Kafka windows, and still writing the same log file. It stays the same
    separate process either way, on the test's own Kafka topic."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{args[0]}_{datetime.now():%Y%m%d_%H%M%S}.log"
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    if console and sys.platform == "win32":
        subprocess.Popen([sys.executable, "-u", "-m", "scalability.run", *args,
                          "--log-file", str(log_path), "--keep-open"], cwd=BASE_DIR, env=env,
                         creationflags=subprocess.CREATE_NEW_CONSOLE | subprocess.CREATE_NEW_PROCESS_GROUP)
        return str(log_path)
    kwargs = {}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True
    with open(log_path, "wb") as log:
        subprocess.Popen([sys.executable, "-u", "-m", "scalability.run", *args], cwd=BASE_DIR,
                         stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env, **kwargs)
    return str(log_path)


def _kafka_up() -> bool:
    return broker_reachable()


def _latest_test() -> list:
    """The newest test's iterations (ScaleTestRun rows in iteration order),
    or []. Runs from before iterations existed are a test of one."""
    runner.reap_dead_runs()
    with SessionLocal() as s:
        runs = s.query(ScaleTestRun).order_by(ScaleTestRun.started_at).all()
        s.expunge_all()
    if not runs:
        return []
    key = lambda r: json.loads(r.config_json or "{}").get("SERIES_ID") or r.run_id  # noqa: E731
    newest = key(runs[-1])
    return [r for r in runs if key(r) == newest]


@st.fragment(run_every=3)
def _active_panel():
    job = runner.active_job()
    was_active = st.session_state.get("scale_was_active", False)
    st.session_state["scale_was_active"] = bool(job)
    if not job:
        if was_active:
            st.rerun()          # the job just finished: refresh the whole page once
        return
    if job.get("kind") == "prepare":
        st.info("Preparing local copies…")
        return
    phase = ""
    with SessionLocal() as s:
        run = (s.query(ScaleTestRun).filter(ScaleTestRun.status.in_(runner.ACTIVE_STATUSES))
               .order_by(ScaleTestRun.started_at.desc()).first())
        if run is not None:
            phase = run.phase or ""
    if phase.startswith(("Paused", "Waiting")):
        st.info("The scalability test is paused while farmer photos are being processed: "
                "real-time processing comes first.")
    else:
        st.info("A scalability test is running. The results appear below when it finishes.")


def _setup_section(busy: bool):
    st.subheader("Run a test")
    # Google Drive is not a choice here: the test reads photos already on this
    # laptop (`python -m scalability.run start --from-drive` still can).
    use_folder = st.radio("Photo source", ["Local photos on this laptop", "Custom folder"], horizontal=True,
                          key="scale_source") == "Custom folder"
    source_dir = None
    if use_folder:
        source_dir = st.text_input("Folder on this computer", key="scale_source_dir",
                                   placeholder=r"e.g. D:\maize_photos").strip().strip('"') or None
        can_check = source_dir is not None
    else:
        can_check = True
        st.caption(f"The farmer photos already stored on this laptop ({runner.dataset.local_photos_dir()}). "
                   "Nothing is downloaded from Google Drive.")
    source = "folder" if use_folder else "local"
    if st.button("Check the folder" if use_folder else "Check the local photos", icon="🔎",
                 disabled=not can_check):
        with st.spinner("Scanning the folder…" if use_folder else "Scanning the local photos…"):
            try:
                st.session_state["scale_preview"] = {**runner.preview(source_dir), "source_dir": source_dir,
                                                     "source": source}
            except Exception as e:  # noqa: BLE001
                st.error(f"Could not read {'the folder' if use_folder else 'the local photos'}: {e}")
    pv = st.session_state.get("scale_preview")
    if not pv or pv.get("source_dir") != source_dir or pv.get("source") != source:
        return                                              # a check of the other source doesn't count
    valid = pv["valid"]
    if not valid:
        st.warning(f"There are no valid photos in {'this folder' if use_folder else 'the local photos'}.")
        return
    options = [n for n in SIZES if n < valid] + [valid]
    labels = {n: f"{n:,}" if n != valid else f"All available ({valid:,})" for n in options}
    c1, c2 = st.columns(2)
    size = c1.selectbox("Unique photos", options, index=len(options) - 1 if valid < 100 else 0,
                        format_func=labels.get, key="scale_size")
    iterations = int(c2.number_input("Iterations", 1, 10, 3, key="scale_iterations"))
    st.caption(f"Workload: {size:,} unique photos × {iterations} iteration(s) = "
               f"{size * iterations:,} photo processings. Every iteration uses the same photos under new "
               "test IDs. The test runs apart from farmer processing and pauses while farmer photos are "
               "being processed, but it shares this laptop's CPU, memory and GPU.")

    console = sys.platform == "win32" and st.checkbox(
        "Show the test live in a command prompt window", value=True, key="scale_console",
        help="Opens an EWS-ScalabilityTest window (like the Spark and Kafka windows) with a line per "
             "Kafka step and per photo, and the speed every 2 seconds. It is still the separate test "
             "process on its own Kafka topic: the live Spark job and farmer photos are not affected.")
    if st.button("Start test", icon="▶️", type="primary", disabled=busy or not _kafka_up()):
        _launch(["start", "--size", str(size), "--iterations", str(iterations)]
                + (["--source-dir", source_dir] if source_dir else [])
                + (["--live", "progress"] if size * iterations > 2000 else []), console=console)
        st.rerun()
    if not _kafka_up():
        from config.settings import KAFKA_BOOTSTRAP_SERVERS
        st.warning(f"Start test needs the Kafka broker running at {KAFKA_BOOTSTRAP_SERVERS} "
                   "(`python run_all.py`).")


def _comparison(runs: list):
    """The test's one before/after pair: files the run saved while it
    processed that photo (scalability/worker.save_comparison). No labels."""
    pair = next((json.loads(r.summary_json or "{}").get("comparison") for r in runs
                 if json.loads(r.summary_json or "{}").get("comparison")), None)
    owner = next((r for r in runs if json.loads(r.summary_json or "{}").get("comparison") == pair), None)
    if not pair or owner is None:
        st.caption("No image comparison was saved for this test.")
        return
    folder = runner.run_dir(owner.run_id)
    before, after = folder / pair["before"], folder / pair["after"]
    if not (before.is_file() and after.is_file()):
        st.caption("The image files of this test are no longer on disk.")
        return
    cols = st.columns(2)
    cols[0].image(str(before), caption="Before preprocessing", use_container_width=True)
    cols[1].image(str(after), caption="After preprocessing", use_container_width=True)


def _disease_section(first_run, df):
    """Where the diseases the CNN found in this test's photos are. Uses the
    first iteration only: later iterations repeat the same photos, so
    counting them would multiply every number by the iteration count."""
    st.subheader("Disease data")
    done = df[(df["status"] == "COMPLETED") & df["predicted_class"].notna()]
    if done.empty:
        st.caption("No photo of this test has a CNN result yet." if first_run.status in runner.ACTIVE_STATUSES
                   else "This test has no CNN results (tests run before this page showed disease data "
                        "did not keep them).")
        return
    n_places = done["location"].nunique()
    st.caption(f"The CNN's result for each of the {len(done):,} test photos, by where the farmer took it "
               f"({n_places} municipalit{'y' if n_places == 1 else 'ies'}). Test results only — no farmer "
               "receives them.")
    colors = disease_colors(sorted(done["predicted_class"].unique()))

    by_place = (done.groupby(["location", "predicted_class"]).size().reset_index(name="photos"))
    order = by_place.groupby("location")["photos"].sum().sort_values(ascending=False).index.tolist()
    fig = px.bar(by_place, x="photos", y="location", color="predicted_class", orientation="h",
                 color_discrete_map=colors, category_orders={"location": order},
                 labels={"photos": "Photos", "location": "Municipality", "predicted_class": "Disease"})
    fig.update_layout(barmode="stack", height=max(260, 46 * len(order) + 120), legend_title_text="Disease")
    st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns([3, 2])
    with c1:
        gps = done.dropna(subset=["gps_latitude", "gps_longitude"])
        if gps.empty:
            st.caption("None of these photos has a GPS position, so there is no map.")
        else:
            fig = px.scatter_mapbox(
                gps, lat="gps_latitude", lon="gps_longitude", color="predicted_class",
                color_discrete_map=colors, hover_name="location",
                hover_data={"prediction_confidence": ":.2%", "gps_latitude": ":.5f", "gps_longitude": ":.5f",
                            "predicted_class": True},
                labels={"predicted_class": "Disease", "prediction_confidence": "Confidence"},
                zoom=8, height=420)
            fig.update_layout(mapbox_style="open-street-map", margin=dict(l=0, r=0, t=0, b=0),
                              legend_title_text="Disease")
            fig.update_traces(marker=dict(size=10, opacity=0.85))
            st.plotly_chart(fig, use_container_width=True)
            missing = len(done) - len(gps)
            st.caption(f"{len(gps):,} photo(s) on the map"
                       + (f"; {missing:,} without a GPS position are not shown." if missing else "."))
    with c2:
        dist = done["predicted_class"].value_counts().rename_axis("predicted_class").reset_index(name="photos")
        fig = px.bar(dist, x="predicted_class", y="photos", color="predicted_class", color_discrete_map=colors,
                     labels={"predicted_class": "Disease", "photos": "Photos"}, height=420)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    high = high_risk_by_location(done)
    if not high.empty:
        st.markdown("**HIGH risk by municipality**")
        st.dataframe(
            high.assign(summary=[f"{h} of {t}" for h, t in zip(high["high"], high["total"])]),
            column_order=["location", "summary", "high_pct"],
            column_config={
                "location": st.column_config.TextColumn("Municipality"),
                "summary": st.column_config.TextColumn("HIGH risk photos"),
                "high_pct": st.column_config.ProgressColumn("Share HIGH", format="%.2f%%",
                                                            min_value=0, max_value=100),
            },
            hide_index=True, use_container_width=True,
        )


def _gallery(first_run, df):
    """Every photo of the test, like the Pending Review page: before and
    after preprocessing, with its CNN result. Paged, with filters."""
    st.subheader("All test photos")
    folder = runner.run_dir(first_run.run_id)
    if df.empty or not (folder / "previews").is_dir():
        st.caption("Preview images are made when a test finishes; this test has none.")
        return False
    f1, f2, f3 = st.columns([2, 2, 1])
    diseases = sorted(df["predicted_class"].dropna().unique())
    places = sorted(df["location"].unique())
    pick_disease = f1.multiselect("Disease", diseases, key="scale_g_disease")
    pick_place = f2.multiselect("Municipality", places, key="scale_g_place")
    failed_only = f3.checkbox("Failed only", key="scale_g_failed")
    shown = df
    if pick_disease:
        shown = shown[shown["predicted_class"].isin(pick_disease)]
    if pick_place:
        shown = shown[shown["location"].isin(pick_place)]
    if failed_only:
        shown = shown[shown["status"] == "FAILED"]
    if shown.empty:
        st.caption("No photo matches these filters.")
        return True
    pages = max(1, -(-len(shown) // GALLERY_PAGE_SIZE))
    p1, p2 = st.columns([1, 3])
    page = int(p1.number_input("Page", 1, pages, 1, key="scale_g_page")) if pages > 1 else 1
    p2.caption(f"{len(shown):,} photo(s), page {page} of {pages}.")
    rows = shown.iloc[(page - 1) * GALLERY_PAGE_SIZE: page * GALLERY_PAGE_SIZE].to_dict("records")
    for start in range(0, len(rows), GALLERY_COLUMNS):
        cols = st.columns(GALLERY_COLUMNS)
        for col, row in zip(cols, rows[start:start + GALLERY_COLUMNS]):
            with col, st.container(border=True):
                before, after = preview_paths(folder, row["image_id"])
                pics = st.columns(2)
                if before.is_file():
                    pics[0].image(str(before), caption="Before", use_container_width=True)
                if after.is_file():
                    pics[1].image(str(after), caption="After", use_container_width=True)
                st.badge(row["status"], color=STATUS_BADGE_COLOR.get(row["status"], "gray"))
                if row["predicted_class"]:
                    conf = row["prediction_confidence"]
                    st.caption(f"**{row['predicted_class']}**"
                               + (f" · {conf * 100:.2f}%" if conf is not None else "")
                               + (f" · {row['risk_level']} risk" if row["risk_level"] else ""))
                elif row["error_message"]:
                    st.caption(str(row["error_message"])[:120])
                st.caption(row["location"])
    return True


def _results_section(busy: bool):
    st.subheader("Results")
    runs = _latest_test()
    if not runs:
        st.info("**Status: Ready** — 0 test images, progress 0%. No previous results: start a new test above.")
        return
    st.caption(f"Latest test, started {format_local(runs[0].started_at)}.")

    with SessionLocal() as s:
        items = s.query(ScaleTestItem).filter(ScaleTestItem.run_id.in_([r.run_id for r in runs])).all()
    avg = series_averages(runs, items)
    if avg["total_end_to_end_s"] is None and avg["average_processing_s"] is None:
        st.info("The test is still running." if any(r.status in runner.ACTIVE_STATUSES for r in runs) else
                "This test has no valid timing measurement: no iteration finished with every photo processed "
                "while no farmer photos were being processed.")
    else:
        m = st.columns(2)
        m[0].metric("Total Execution Time",
                    f"{avg['total_end_to_end_s']:.2f} seconds" if avg["total_end_to_end_s"] is not None
                    else "Not available",
                    help="From pressing Start test until the system finished the last photo: preparing the "
                         "photos, loading the CNN, starting Spark, Kafka, preprocessing, CNN and saving the "
                         "results. Time spent waiting while farmer photos were processed is not counted; "
                         "several iterations are added together; making the gallery previews afterwards is "
                         "not included.")
        m[1].metric("Average Processing Time",
                    f"{avg['average_processing_s']:.2f} seconds/image" if avg["average_processing_s"] is not None
                    else "No valid measurement",
                    help="Per photo: preprocessing plus CNN inference, the same code the live pipeline runs. "
                         "Waiting in the queue, reading the file, Google Drive and the database are not "
                         "included. Photos processed while farmer photos were being processed are left out.")
        if avg["total_end_to_end_s"] is None:
            st.caption("Total Execution Time is not available for tests run before 11 October 2026, or when an "
                       "iteration had a failed photo or ran while farmer photos were being processed.")
    shared_gpu = [i + 1 for i, r in enumerate(runs)
                  if json.loads(r.summary_json or "{}").get("spark_job_running")]
    if shared_gpu:
        st.caption(f"The live Spark job was running during iteration(s) {', '.join(map(str, shared_gpu))}, "
                   "sharing the GPU — those times can be slower.")

    first = runs[0]
    df = items_frame([i for i in items if i.run_id == first.run_id])
    st.divider()
    _disease_section(first, df)
    st.divider()
    if not _gallery(first, df):
        _comparison(runs)               # tests from before the gallery: their one saved pair


def _reset_section():
    """The Delete test button: a complete reset of the scalability test
    only (scalability/reset.py), then this page back to its defaults."""
    st.subheader("Delete test")
    st.caption("Deletes every scalability test: its staged images, previews, results, logs, its records, its "
               "Kafka test events and a running test with its own Spark job. Your original photos, farmer "
               "photos, Google Drive, the live Kafka topic and Spark job, the model and the training data "
               "are not touched.")
    a1, a2 = st.columns(2)
    confirm = a1.checkbox("Yes, delete all scalability test data", key="scale_reset_confirm")
    if a2.button("Delete test", icon="🗑️", key="scale_reset", disabled=not confirm):
        with st.spinner("Resetting the scalability test…"):
            admin = (st.session_state.get("admin_user") or {}).get("username")
            result = test_reset.reset_all(performed_by=admin)
        for key in [k for k in st.session_state if str(k).startswith("scale_")]:
            del st.session_state[key]            # widgets back to their defaults
        st.session_state["scale_reset_result"] = result
        st.rerun()


def _reset_message(result: dict) -> None:
    if result["ok"]:
        st.success("Scalability test deleted. Status: Ready — 0 test images, progress 0%.")
        return
    lines = [f"- **{s['step']}**: {'done' if s['ok'] else 'FAILED'} — {s['detail']}" for s in result["steps"]]
    if result["left"]:
        lines.append(f"- Still left: {', '.join(result['left'])}")
    st.error("The scalability test was NOT completely deleted:\n\n" + "\n".join(lines)
             + "\n\nNothing outside the scalability test was touched. Fix the problem and press Delete test again.")


def render():
    if msg := st.session_state.pop("scale_msg", None):
        st.info(msg)
    if result := st.session_state.pop("scale_reset_result", None):
        _reset_message(result)
    _active_panel()
    busy = runner.active_job() is not None
    _setup_section(busy)
    st.divider()
    _results_section(busy)
    st.divider()
    _reset_section()
