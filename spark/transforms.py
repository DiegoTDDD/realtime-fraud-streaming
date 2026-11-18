"""Pure DataFrame transformations used by the streaming job.

Each function takes and returns a DataFrame and works the same on a streaming
or a static DataFrame, so tests/test_transforms.py runs them on small static
data with a local SparkSession, without Kafka.

  parse_events      Kafka `value` bytes -> typed columns + event_time
  with_violations   adds `violations`: the contract rules (contract/transaction.py)
  valid_events / invalid_events
  gold_windows      1-minute tumbling windows: counts, amounts, fraud by pattern
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

from contract.transaction import FIELDS, FRAUD_TYPES, REQUIRED

_SPARK_TYPES = {
    "amount": DoubleType(),
    "is_fraud": BooleanType(),
}
# Schema of the JSON payload, built from the contract so the two cannot disagree.
SCHEMA = StructType([StructField(name, _SPARK_TYPES.get(name, StringType())) for name in FIELDS])
WINDOW = "1 minute"
WATERMARK = "2 minutes"


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka records (binary `value`) -> one column per contract field, plus event_time.

    A field with the wrong JSON type becomes null rather than failing the batch,
    so the contract check below can report it.
    """
    return (
        raw.select(
            F.col("value").cast("string").alias("raw_value"),
            F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"),
        )
        .select("raw_value", "t.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
    )


def _violation_checks() -> list[Column]:
    is_fraud, fraud_type = F.col("is_fraud"), F.col("fraud_type")
    checks = [F.when(F.col(name).isNull(), F.lit(f"invalid:{name}")) for name in REQUIRED]
    checks += [
        F.when(F.col("timestamp").isNotNull() & F.col("event_time").isNull(), "bad_timestamp"),
        F.when(F.col("amount") <= 0, "amount_not_positive"),
        F.when(fraud_type.isNotNull() & ~fraud_type.isin(*FRAUD_TYPES), "unknown_fraud_type"),
        F.when(is_fraud.isNotNull() & (is_fraud != fraud_type.isNotNull()), "label_mismatch"),
    ]
    return checks


def with_violations(parsed: DataFrame) -> DataFrame:
    """Add `violations` (array of reasons, same wording and order as validate_event)."""
    return parsed.withColumn("violations", F.array_compact(F.array(*_violation_checks())))


def valid_events(checked: DataFrame) -> DataFrame:
    return checked.filter(F.size("violations") == 0).drop("violations", "raw_value")


def invalid_events(checked: DataFrame) -> DataFrame:
    """Rows for the quarantine folder: the original message and why it was rejected."""
    return checked.filter(F.size("violations") > 0).select(
        "raw_value", "violations", F.current_timestamp().alias("quarantined_at")
    )


def gold_windows(events: DataFrame) -> DataFrame:
    """Tumbling 1-minute windows with a 2-minute watermark for late events."""

    def count_type(name: str) -> Column:
        return F.sum(F.when(F.col("fraud_type") == name, 1).otherwise(0)).alias(f"fraud_{name}")

    return (
        events.withWatermark("event_time", WATERMARK)
        .groupBy(F.window(F.col("event_time"), WINDOW))
        .agg(
            F.count("*").alias("tx_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            *[count_type(name) for name in FRAUD_TYPES],
            F.sum(F.when(F.col("is_fraud"), 1).otherwise(0)).alias("fraud_total"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "tx_count",
            "total_amount",
            "avg_amount",
            *[f"fraud_{name}" for name in FRAUD_TYPES],
            "fraud_total",
        )
    )
