"""Shared fixtures: the project on sys.path, a small local SparkSession and event builders."""

import json
import os
import sys
import time
from datetime import datetime
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


SCHEMA_ID = 7


def wire(payload: str, schema_id: int | None = SCHEMA_ID) -> bytes:
    """Confluent wire format: a 0 byte, the 4-byte schema id, then the JSON. None = no header."""
    body = payload.encode()
    return body if schema_id is None else b"\x00" + schema_id.to_bytes(4, "big") + body


@pytest.fixture(scope="session")
def spark():
    from spark.session import build_spark

    session = build_spark(
        "tests",
        master="local[1]",
        conf={"spark.ui.enabled": "false", "spark.sql.shuffle.partitions": "1"},
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def kafka_frame(spark):
    """Build a DataFrame shaped like the Kafka source from dicts or raw strings.

    Messages carry the registry header unless `registered=False`. Offsets count from 0 on
    partition 0, with a broker timestamp of 2026-01-01 10:00 UTC.
    """

    def build(messages, registered: bool = True):
        ts = datetime(2026, 1, 1, 10, 0, 0)
        rows = []
        for i, m in enumerate(messages):
            if isinstance(m, bytes):  # already serialized (e.g. by the producer's serializer)
                value = m
            else:
                text = m if isinstance(m, str) else json.dumps(m)
                value = wire(text, SCHEMA_ID if registered else None)
            rows.append((bytearray(value), 0, i, ts))
        return spark.createDataFrame(
            rows, "value binary, partition int, offset long, timestamp timestamp"
        )

    return build
