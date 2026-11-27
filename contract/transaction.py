"""Contract for the events on the `transactions` topic.

One definition, two enforcers:
  - the producer checks every event with validate_event() before sending it;
  - the Spark job applies the same rules to what it reads (spark/transforms.py)
    and sends violating events to a quarantine folder instead of Gold.

tests/test_transforms.py checks that both enforcers give the same reasons for
the same JSON. One known difference: Spark's JSON parser turns a number into a
string when the field is a string (card_id 123 becomes "123"), so only the
producer rejects that case.

Reasons (in this order):
  invalid:<field>      required field missing, null or of the wrong type
  bad_timestamp        timestamp is not ISO 8601 (no timezone = UTC)
  amount_not_positive  amount <= 0
  unknown_fraud_type   fraud_type not one of FRAUD_TYPES
  label_mismatch       is_fraud is true without a fraud_type, or false with one
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

# field -> Python type(s) accepted; order matches the producer's JSON and the Spark schema
FIELDS: dict[str, tuple[type, ...]] = {
    "transaction_id": (str,),
    "card_id": (str,),
    "amount": (int, float),
    "merchant": (str,),
    "category": (str,),
    "city": (str,),
    "timestamp": (str,),
    "is_fraud": (bool,),
    "fraud_type": (str,),
}
OPTIONAL = frozenset({"fraud_type"})  # null for legitimate transactions
REQUIRED = [name for name in FIELDS if name not in OPTIONAL]
FRAUD_TYPES = ("high_amount", "card_testing", "impossible_travel")


def _has_type(value: Any, types: tuple[type, ...]) -> bool:
    if isinstance(value, bool) and bool not in types:
        return False  # in Python a bool is also an int; JSON true is not a number
    return isinstance(value, types)


def _parses_as_timestamp(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def validate_event(event: Mapping[str, Any]) -> list[str]:
    """Reasons the event breaks the contract, in a fixed order; empty if it is valid."""
    reasons = [
        f"invalid:{name}"
        for name in REQUIRED
        if event.get(name) is None or not _has_type(event[name], FIELDS[name])
    ]
    fraud_type = event.get("fraud_type")
    if fraud_type is not None and not _has_type(fraud_type, FIELDS["fraud_type"]):
        reasons.append("invalid:fraud_type")
        fraud_type = None

    timestamp = event.get("timestamp")
    if isinstance(timestamp, str) and not _parses_as_timestamp(timestamp):
        reasons.append("bad_timestamp")
    amount = event.get("amount")
    if _has_type(amount, FIELDS["amount"]) and amount <= 0:
        reasons.append("amount_not_positive")
    if fraud_type is not None and fraud_type not in FRAUD_TYPES:
        reasons.append("unknown_fraud_type")
    is_fraud = event.get("is_fraud")
    if isinstance(is_fraud, bool) and is_fraud != (fraud_type is not None):
        reasons.append("label_mismatch")
    return reasons


# ---------------------------------------------------------------------------
# JSON Schema for the schema registry (ADR 0008)
# ---------------------------------------------------------------------------
SUBJECT = "transactions-value"  # TopicNameStrategy: <topic>-value
SCHEMA_PATH = Path(__file__).with_name("transactions-value.schema.json")
_JSON_TYPES = {str: "string", int: "integer", float: "number", bool: "boolean"}


def json_schema() -> dict[str, Any]:
    """The topic's value schema, derived from FIELDS, REQUIRED and FRAUD_TYPES.

    It encodes what a schema can say (fields, types, required, allowed fraud types, positive
    amount). The cross-field rule (label_mismatch) stays in validate_event and in Spark.
    tests/test_schema.py fails if the committed file differs from this.
    """
    properties: dict[str, Any] = {}
    for name, types in FIELDS.items():
        json_types = sorted({_JSON_TYPES[t] for t in types})
        if set(json_types) == {"integer", "number"}:
            json_types = ["number"]
        spec: dict[str, Any] = {"type": json_types[0] if len(json_types) == 1 else json_types}
        if name in OPTIONAL:
            spec["type"] = [spec["type"], "null"]
        if name == "amount":
            spec["exclusiveMinimum"] = 0
        if name == "timestamp":
            spec["format"] = "date-time"
        if name == "fraud_type":
            spec["enum"] = [*FRAUD_TYPES, None]
        properties[name] = spec
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "transaction",
        "description": (
            "A card transaction on the transactions topic. Generated from contract/transaction.py."
        ),
        "type": "object",
        "properties": properties,
        "required": list(REQUIRED),
    }
