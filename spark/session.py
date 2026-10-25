"""
The Kafka-reading SparkSession used by the live job and the scalability test.
Import it BEFORE pyspark: on Windows it points Hadoop at winutils first.
"""
import os

# Windows only: where Hadoop finds winutils.exe
if os.name == "nt" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    _hadoop_bin = r"C:\hadoop\bin"
    if _hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] += os.pathsep + _hadoop_bin

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def kafka_session(app_name: str, console_progress: bool = True):
    """A SparkSession with the Kafka connector (downloaded the first time)."""
    from pyspark.sql import SparkSession
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # the default checkpoint manager randomly fails on Windows local disk
        .config("spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager")
    )
    if not console_progress:
        builder = builder.config("spark.ui.showConsoleProgress", "false")
    return builder.getOrCreate()
