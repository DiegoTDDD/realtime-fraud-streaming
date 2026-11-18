"""Spark transformations on small static DataFrames with a local SparkSession."""

import json
from datetime import datetime

import pytest
from conftest import event, without

from contract.transaction import FIELDS, validate_event
from spark.transforms import (
    SCHEMA,
    gold_windows,
    invalid_events,
    parse_events,
    valid_events,
    with_violations,
)

pytestmark = pytest.mark.spark


def test_schema_has_every_contract_field_in_order():
    assert [f.name for f in SCHEMA.fields] == list(FIELDS)


def test_parse_types_the_columns_and_adds_event_time(kafka_frame):
    row = parse_events(kafka_frame([event()])).collect()[0]
    assert row["amount"] == 42.5 and row["is_fraud"] is False and row["fraud_type"] is None
    assert row["event_time"] == datetime(2026, 1, 1, 10, 0, 30)


# JSON messages on which the producer-side check and the Spark check must agree.
CASES = [
    event(),
    event(is_fraud=True, fraud_type="card_testing", amount=1.5),
    without("card_id"),
    event(amount=None),
    event(amount="12.5"),
    event(amount=True),
    event(is_fraud="no"),
    event(timestamp="yesterday"),
    event(amount=0),
    event(is_fraud=True, fraud_type="phishing"),
    event(is_fraud=True),
    event(fraud_type="high_amount"),
    event(amount=-1, timestamp="nope", card_id=None),
]


def test_spark_reports_the_same_violations_as_the_producer(kafka_frame):
    rows = with_violations(parse_events(kafka_frame(CASES))).collect()
    for case, row in zip(CASES, rows, strict=True):
        assert list(row["violations"]) == validate_event(case), json.dumps(case)


def test_unparseable_messages_are_quarantined(kafka_frame):
    checked = with_violations(parse_events(kafka_frame(["not json", "{}"])))
    rows = invalid_events(checked).collect()
    assert [r["raw_value"] for r in rows] == ["not json", "{}"]
    assert all("invalid:transaction_id" in r["violations"] for r in rows)


def test_valid_and_invalid_split_the_stream(kafka_frame):
    checked = with_violations(parse_events(kafka_frame([event(), event(amount=-3), event()])))
    assert valid_events(checked).count() == 2
    assert invalid_events(checked).count() == 1
    assert "violations" not in valid_events(checked).columns


def test_gold_windows_count_amounts_and_fraud_per_minute(kafka_frame):
    msgs = [
        event(timestamp="2026-01-01T10:00:05+00:00", amount=10),
        event(
            timestamp="2026-01-01T10:00:50+00:00",
            amount=30,
            is_fraud=True,
            fraud_type="card_testing",
        ),
        event(
            timestamp="2026-01-01T10:01:10+00:00",
            amount=8000,
            is_fraud=True,
            fraud_type="high_amount",
        ),
        event(timestamp="2026-01-01T10:01:20+00:00", amount=-1),  # invalid: left out of Gold
    ]
    checked = with_violations(parse_events(kafka_frame(msgs)))
    rows = gold_windows(valid_events(checked)).orderBy("window_start").collect()

    assert [r["window_start"] for r in rows] == [
        datetime(2026, 1, 1, 10, 0),
        datetime(2026, 1, 1, 10, 1),
    ]
    first, second = rows
    assert (first["tx_count"], first["total_amount"], first["avg_amount"]) == (2, 40.0, 20.0)
    assert (first["fraud_card_testing"], first["fraud_total"]) == (1, 1)
    assert (second["tx_count"], second["fraud_high_amount"], second["fraud_total"]) == (1, 1, 1)
    assert second["fraud_impossible_travel"] == 0
