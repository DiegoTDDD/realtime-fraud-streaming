"""Shared fixtures: the project on sys.path, a small local SparkSession and event builders."""

import json
import os
import sys
import time
from pathlib import Path

import pytest

# Spark returns timestamps as naive datetimes in the local time zone; pin it so the
# expected values in the tests are the same on every machine.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "producer"))


def event(**overrides) -> dict:
    """A valid legitimate transaction; keyword arguments replace or add fields."""
    base = {
        "transaction_id": "tx-1",
        "card_id": "card_0001",
        "amount": 42.5,
        "merchant": "Test Co",
        "category": "groceries",
        "city": "Recife",
        "timestamp": "2026-01-01T10:00:30+00:00",
        "is_fraud": False,
        "fraud_type": None,
    }
    base.update(overrides)
    return base


def without(field: str) -> dict:
    e = event()
    del e[field]
    return e


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def kafka_frame(spark):
    """Build a DataFrame shaped like the Kafka source (binary `value`) from dicts or raw strings."""

    def build(messages):
        values = [m if isinstance(m, str) else json.dumps(m) for m in messages]
        return spark.createDataFrame([(bytearray(v.encode()),) for v in values], "value binary")

    return build
