# 0008. The topic's schema lives in the schema registry, with BACKWARD compatibility

**Status:** Accepted. Extends [0004](0004-event-contract-and-quarantine.md), which listed a schema registry as the alternative not taken.

## Context
The contract was a Python module that only this repository's producer and job could see. A second producer, or a change to the event format, had nothing outside the code to stop it.

## Decision
- `contract/transactions-value.schema.json` is a JSON Schema generated from `contract/transaction.py` (fields, types, required fields, allowed fraud types, positive amount). A unit test fails if the committed file and the contract differ.
- It is registered in Redpanda's built-in schema registry under `transactions-value`, with BACKWARD compatibility: a new version must be readable by the consumer that reads the current one. `contract/registry.py check` asks the registry before a change is registered; CI shows it rejecting a version where `amount` becomes a string.
- The producer serializes with confluent-kafka's JSON Schema serializer: each message carries a 0 byte and the 4-byte schema id before the JSON, and the event is validated against the registered schema. It does not register schemas itself.
- Spark reads that header. A message without it is quarantined as `unregistered_schema` (its contract violations are listed too) and never reaches Gold. The schema id is stored in Bronze.
- The cross-field rule (`label_mismatch`) cannot be expressed in the schema, so it stays in `validate_event` and in Spark.

## Alternatives considered
- **Avro.** Smaller messages and the usual choice at scale, but Spark would need the spark-avro package and the console would show binary. JSON Schema gives the same compatibility checks for this volume.
- **Protobuf.** Same trade-off, plus code generation.
- **No registry.** Leaves every other producer unguarded.

## Consequences
- Run `python contract/registry.py register` once after `docker compose up`. The Windows check uses Confluent's community registry, which has the same API.
- Spark does not look up schema ids per record; a message with an unknown id is still kept in Bronze with its id, so it can be audited.
