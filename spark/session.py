"""
The Kafka-reading SparkSession shared by the live streaming job
(spark/streaming_job.py) and the scalability test's own streaming query
(scalability/spark_engine.py): same Kafka connector, same Windows-safe
checkpoint manager, each with its own application name. Kept here so the
test never has to import the live job (which starts its upload pool when
imported).

Import this module BEFORE pyspark: on Windows it points Hadoop at winutils
first (see below).
"""
import os

# Windows-only workaround (winutils.exe location for Hadoop's local
# filesystem shim) — only set if not already configured, and never on a
# non-Windows deployment box, where C:\hadoop doesn't exist and shouldn't be
# assumed.
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    _hadoop_bin = r"C:\hadoop\bin"
    if _hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] += os.pathsep + _hadoop_bin

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def kafka_session(app_name: str, console_progress: bool = True):
    """A SparkSession that can read Kafka (the connector is downloaded from
    Maven Central the first time)."""
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # Spark 3.x defaults to FileContextBasedCheckpointFileManager, whose
        # atomic temp-file + rename against a Windows *local* filesystem
        # intermittently fails with
        #   FileNotFoundException: .../<checkpoint>/offsets does not exist
        # and terminates the streaming query (STREAM_FAILED). The older
        # FileSystem-based manager uses FileSystem.create + rename, which
        # works correctly on Windows local disk. Harmless on Linux/HDFS.
        .config("spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
    )
    if not console_progress:
        builder = builder.config("spark.ui.showConsoleProgress", "false")
    return builder.getOrCreate()
