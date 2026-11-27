"""Compact and clean the Delta tables, and check Bronze for duplicates (ADR 0007).

    python spark/maintenance.py                      # OPTIMIZE and VACUUM every table
    python spark/maintenance.py --check-duplicates   # also check Bronze has each record once
    python spark/maintenance.py --retention-hours 168

Streaming writes one small file per partition per micro-batch. OPTIMIZE rewrites them into
larger files in one Delta commit, and VACUUM deletes files no version within the retention
window references. Both are safe while the stream is writing: Delta lets a blind append and a
compaction commit together. Exits 1 if a table's row count changes or Bronze has duplicates.

Run it from a scheduler (cron, Airflow, a CI schedule); it does not need the stream stopped.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession  # noqa: E402

from spark.streaming_job import BRONZE_DIR, GOLD_DIR, QUARANTINE_DIR  # noqa: E402

log = logging.getLogger(__name__)
TABLES = {"bronze": BRONZE_DIR, "quarantine": QUARANTINE_DIR, "gold": GOLD_DIR}


def active_files(spark: SparkSession, path: str) -> int:
    return spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]["numFiles"]


def compact(spark: SparkSession, path: str, retention_hours: float) -> dict:
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, path)
    rows_before, files_before = table.toDF().count(), active_files(spark, path)
    table.optimize().executeCompaction()
    table.vacuum(retention_hours)
    rows_after, files_after = table.toDF().count(), active_files(spark, path)
    return {"rows_before": rows_before, "rows_after": rows_after,
            "files_before": files_before, "files_after": files_after}  # fmt: skip


def bronze_integrity(spark: SparkSession, path: str) -> dict:
    """Each Kafka record (partition, offset) must appear once, and offsets must be contiguous
    within each partition from the first one read: a duplicate means a batch was written twice,
    a gap means one was lost."""
    df = spark.read.format("delta").load(path)
    dupes = df.groupBy("kafka_partition", "kafka_offset").count().filter("count > 1").count()
    gaps = spark.sql(
        f"""
        select kafka_partition,
               max(kafka_offset) - min(kafka_offset) + 1 - count(distinct kafka_offset) as missing
        from delta.`{path}`
        group by kafka_partition
        """
    ).collect()
    return {"duplicates": dupes, "missing": {str(r["kafka_partition"]): r["missing"] for r in gaps}}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--retention-hours", type=float, default=168.0)
    parser.add_argument("--check-duplicates", action="store_true")
    parser.add_argument("--skip-compaction", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    from spark.session import build_spark

    spark = build_spark("maintenance")
    spark.sparkContext.setLogLevel("WARN")
    report, ok = {}, True
    if not args.skip_compaction:
        for name, path in TABLES.items():
            if not os.path.isdir(os.path.join(path, "_delta_log")):
                log.info("%-10s no table yet", name)
                continue
            r = compact(spark, path, args.retention_hours)
            report[name] = r
            log.info(
                "%-10s files %5d -> %-5d rows %d -> %d",
                name, r["files_before"], r["files_after"], r["rows_before"], r["rows_after"],
            )  # fmt: skip
            ok &= r["rows_before"] == r["rows_after"]
    if args.check_duplicates:
        integrity = bronze_integrity(spark, BRONZE_DIR)
        report["bronze_integrity"] = integrity
        log.info(
            "bronze     duplicates %d, missing offsets %s",
            integrity["duplicates"],
            integrity["missing"],
        )
        ok &= integrity["duplicates"] == 0 and not any(integrity["missing"].values())
    log.info(json.dumps(report))
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
