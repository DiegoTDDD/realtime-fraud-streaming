"""
Spark Structured Streaming job for real-time fraud analytics.

Reads card transactions from a Redpanda (Kafka) topic and runs three queries, each writing a
Delta table with its own checkpoint, so a restart neither loses nor repeats a record (ADR 0007):

  1. Bronze      - every transaction as received, typed, with its Kafka partition, offset,
                   broker timestamp and schema id; partitioned by ingest date.
  2. Quarantine  - events that break the contract (contract/transaction.py) or arrived without
                   the schema registry's header, with the original message and the reasons.
  3. Gold        - 1-minute tumbling windows with a 2-minute watermark over the valid events:
                   transaction counts, total/average amount and counts of each fraud pattern,
                   also echoed to the console as windows close.

Every query's progress is appended to data/ops/progress.jsonl (spark/progress.py), which
spark/health.py reads. spark/maintenance.py compacts the tables. The transformations live in
spark/transforms.py, where they are unit-tested.

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

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql.streaming import StreamingQuery  # noqa: E402

from spark.progress import ProgressLog  # noqa: E402
from spark.session import build_spark  # noqa: E402
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
DELTA_DIR = os.path.join(DATA_DIR, "delta")
BRONZE_DIR = os.path.join(DELTA_DIR, "bronze")
GOLD_DIR = os.path.join(DELTA_DIR, "gold")
QUARANTINE_DIR = os.path.join(DELTA_DIR, "quarantine")
CHECKPOINT_DIR = os.path.join(DATA_DIR, "_checkpoints")
PROGRESS_LOG = os.path.join(DATA_DIR, "ops", "progress.jsonl")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = "transactions"
# One micro-batch every 10 s: fewer, larger files than "as fast as possible", and still well
# inside the 1-minute windows.
TRIGGER = "10 seconds"


def write_delta(
    df: DataFrame,
    name: str,
    path: str,
    partition_by: list[str] | None = None,
    checkpoint_dir: str = CHECKPOINT_DIR,
    available_now: bool = False,
) -> StreamingQuery:
    """Append a stream to a Delta table. The checkpoint records which source offsets each batch
    committed, and Delta records which batch ids it already wrote, so a restarted query neither
    skips nor repeats a batch. `available_now` processes what is there and stops (tests, backfills).
    """
    trigger = {"availableNow": True} if available_now else {"processingTime": TRIGGER}
    writer = (
        df.writeStream.format("delta")
        .option("checkpointLocation", os.path.join(checkpoint_dir, name))
        .outputMode("append")
        .trigger(**trigger)
        .queryName(name)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    return writer.start(path)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    spark = build_spark("realtime-fraud-streaming", kafka=True)
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLog(PROGRESS_LOG))

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    checked = with_violations(parse_events(raw))

    write_delta(checked.drop("raw_value", "violations"), "bronze", BRONZE_DIR, ["ingest_date"])
    write_delta(invalid_events(checked), "quarantine", QUARANTINE_DIR)

    # Gold: append mode, so each window is written once, when the watermark closes it.
    gold = gold_windows(valid_events(checked))
    write_delta(gold, "gold", GOLD_DIR)
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
