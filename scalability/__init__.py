"""
Scalability test (Demo 2), isolated from the live pipeline: its own Kafka
topic, the same preprocessing + CNN code, farmer photos first.

    dataset.py  finds and checks the photos
    run.py      the runner (CLI; started by the dashboard)
    worker.py   test topic, real-time gate, per-photo timing
    monitor.py  CPU / memory / GPU sampling
    report.py   the averages and CSV files
"""
