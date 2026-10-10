"""
Scalability testing (Demo 2), isolated from the live pipeline.

A run takes photos from the Google Drive collection (or a custom folder),
makes sure each has a verified local copy (downloading only the missing
ones), and processes them in its own worker — its own Kafka topic and
consumer group, the same preprocessing and CNN code as a live upload — while
real-time farmer processing keeps priority. Only timings are kept; no test
photo ever becomes an image, a prediction or a Drive result.

    dataset.py  enumerate + validate the Drive collection, prepare local copies
    run.py      the runner (CLI; the dashboard starts it as a subprocess)
    worker.py   the isolated path: test topic, real-time gate, per-photo timing
    monitor.py  CPU / memory / GPU sampling during the measured stage
    report.py   the two averages and the CSV files
"""
