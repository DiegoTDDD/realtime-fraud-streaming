"""
Spark Structured Streaming job for real-time fraud analytics.

Reads card transactions from a Redpanda (Kafka) topic and runs three queries:

  1. Bronze      - every transaction as received, typed, in Parquet (the
                   faithful landing zone).
  2. Quarantine  - events that break the contract (contract/transaction.py),
                   with the original message and the reasons, in Parquet.
  3. Gold        - 1-minute tumbling windows with a 2-minute watermark over the
                   valid events: transaction counts, total/average amount and
                   counts of each fraud pattern. Written to Parquet and echoed
                   to the console as windows close.

The transformations live in spark/transforms.py, where they are unit-tested.

Run (inside the `fraud` conda env, with Redpanda up):
    python spark/streaming_job.py

Stop with Ctrl+C.
"""

import logging
import os
import platform
import sys

# ---------------------------------------------------------------------------
# Windows Hadoop shim: Spark needs winutils.exe + hadoop.dll to write files
# on Windows. We point HADOOP_HOME at C:\hadoop (where those binaries live)
# before any Spark import touches Hadoop. No-op on Linux/Mac.
# ---------------------------------------------------------------------------
if platform.system() == "Windows":
    hadoop_home = r"C:\hadoop"
    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)  # for the shared contract package

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.streaming import StreamingQuery  # noqa: E402

from spark.transforms import (  # noqa: E402
    gold_windows,
    invalid_events,
    parse_events,
    valid_events,
    with_violations,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths & config
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
GOLD_DIR = os.path.join(DATA_DIR, "gold")
QUARANTINE_DIR = os.path.join(DATA_DIR, "quarantine")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "_checkpoints")

KAFKA_BOOTSTRAP = "localhost:19092"
TOPIC = "transactions"

# Kafka connector package (Spark downloads the JAR on first run).
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("realtime-fraud-streaming")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def write_parquet(df: DataFrame, name: str, path: str) -> StreamingQuery:
    return (
        df.writeStream.format("parquet")
        .option("path", path)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, name))
        .outputMode("append")
        .queryName(name)
        .start()
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    checked = with_violations(parse_events(raw))

    write_parquet(checked.drop("raw_value", "violations"), "bronze", BRONZE_DIR)
    write_parquet(invalid_events(checked), "quarantine", QUARANTINE_DIR)

    # Gold: append mode, because Parquet cannot be written in complete mode.
    gold = gold_windows(valid_events(checked))
    write_parquet(gold, "gold", GOLD_DIR)
    # Also echo closed windows to the console for live visibility.
    (
        gold.writeStream.format("console")
        .option("truncate", "false")
        .outputMode("append")
        .queryName("gold_console")
        .start()
    )

    log.info("Streaming started. Bronze, quarantine and Gold running. Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
