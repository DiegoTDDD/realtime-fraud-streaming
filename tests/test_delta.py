"""Delta sinks: a restarted stream writes each record once; compaction keeps every row."""

import json
from datetime import datetime

import pytest
from conftest import SCHEMA_ID, event, wire
from pyspark.sql import functions as F

from spark.maintenance import active_files, bronze_integrity, compact
from spark.streaming_job import write_delta
from spark.transforms import (
    gold_windows,
    invalid_events,
    parse_events,
    valid_events,
    with_violations,
)

pytestmark = pytest.mark.spark
KAFKA_SCHEMA = "value binary, partition int, offset long, timestamp timestamp"


def _drop(spark, folder, first_offset, n, bad_every=0):
    """Write n Kafka-like records as one Parquet file the stream will pick up."""
    rows = []
    for i in range(first_offset, first_offset + n):
        e = event(transaction_id=f"tx-{i}", timestamp=f"2026-01-01T10:{i % 60:02d}:00+00:00")
        if bad_every and i % bad_every == 0:
            e["amount"] = -1
        rows.append(
            (
                bytearray(wire(json.dumps(e), SCHEMA_ID)),
                i % 3,
                i,
                datetime(2026, 1, 1, 10),
            )
        )
    spark.createDataFrame(rows, KAFKA_SCHEMA).coalesce(1).write.mode("append").parquet(str(folder))


def _run(spark, source, tables, checkpoints):
    raw = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(source))
    checked = with_violations(parse_events(raw))
    queries = [
        write_delta(checked.drop("raw_value", "violations"), "bronze", str(tables / "bronze"),
                    ["ingest_date"], str(checkpoints), available_now=True),
        write_delta(invalid_events(checked), "quarantine", str(tables / "quarantine"),
                    None, str(checkpoints), available_now=True),
        write_delta(gold_windows(valid_events(checked)), "gold", str(tables / "gold"),
                    None, str(checkpoints), available_now=True),
    ]  # fmt: skip
    for q in queries:
        q.awaitTermination()


def test_a_restarted_stream_writes_each_record_exactly_once(spark, tmp_path):
    source, tables, checkpoints = tmp_path / "source", tmp_path / "tables", tmp_path / "ckpt"
    _drop(spark, source, 0, 30, bad_every=10)
    _run(spark, source, tables, checkpoints)
    _run(spark, source, tables, checkpoints)  # restart with nothing new: writes nothing
    _drop(spark, source, 30, 30, bad_every=10)
    _run(spark, source, tables, checkpoints)  # restart after new data: writes only the new part

    bronze = spark.read.format("delta").load(str(tables / "bronze"))
    assert bronze.count() == 60
    assert bronze_integrity(spark, str(tables / "bronze"))["duplicates"] == 0
    assert bronze.filter(F.col("schema_id") == SCHEMA_ID).count() == 60
    quarantine = spark.read.format("delta").load(str(tables / "quarantine"))
    assert sorted(r["kafka_offset"] for r in quarantine.collect()) == [0, 10, 20, 30, 40, 50]


def test_compaction_reduces_files_and_keeps_every_row(spark, tmp_path):
    path = str(tmp_path / "t")
    for i in range(12):  # twelve small appends, like twelve micro-batches
        spark.range(i * 10, i * 10 + 10).write.format("delta").mode("append").save(path)
    assert active_files(spark, path) >= 12
    report = compact(spark, path, retention_hours=168)
    assert report["rows_before"] == report["rows_after"] == 120
    assert report["files_after"] == 1


def test_integrity_reports_duplicates_and_gaps(spark, tmp_path):
    path = str(tmp_path / "b")
    rows = [(0, 0), (0, 1), (0, 1), (0, 3), (1, 5)]
    spark.createDataFrame(rows, "kafka_partition int, kafka_offset long").write.format(
        "delta"
    ).save(path)
    assert bronze_integrity(spark, path) == {"duplicates": 1, "missing": {"0": 1, "1": 0}}
