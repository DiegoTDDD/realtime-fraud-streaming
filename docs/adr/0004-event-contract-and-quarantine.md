# 0004. One event contract for producer and consumer; violations go to quarantine

**Status:** Accepted. Before this, the Spark job parsed whatever arrived: a malformed event became nulls in Bronze and was counted in Gold.

## Context
The producer and the Spark job each had their own copy of the event schema. Nothing checked that an event made sense, for example a negative amount or `is_fraud` true without a `fraud_type`. Spark's JSON parser turns a field with the wrong type into null without reporting an error. Bad events would therefore have distorted Gold's totals and the labels used for training without anyone noticing.

## Decision
- `contract/transaction.py` defines the fields, their types, the allowed fraud types and five rules. Each rule has a named reason, such as `invalid:amount` or `label_mismatch`.
- The producer checks every event before sending it, and logs and drops any event that fails.
- The Spark job builds its schema from the same contract and applies the same rules as column expressions (`spark/transforms.py`). Valid events go to Gold. Invalid ones go to `data/quarantine/` with the original message and the reasons. Bronze still keeps everything.
- `tests/test_transforms.py` feeds the same JSON messages to both checks and requires the same reasons.

## Alternatives considered
- **A schema registry (Avro or Protobuf).** The industry standard for typed topics, with compatibility checks. It adds a service and a serialization format for one topic and one producer.
- **Fail the streaming query on a bad event.** One bad message would stop the whole pipeline.
- **Drop bad events silently.** Hides producer bugs.

## Consequences
- Bad events no longer reach Gold, and they can be inspected afterwards.
- One known difference: Spark turns a number in a string field into a string (`123` becomes `"123"`), so only the producer rejects that case.
- After upgrading from a version without the quarantine query, delete `data/_checkpoints/`, because the Gold query's plan has changed.
