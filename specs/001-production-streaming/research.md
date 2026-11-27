# Research: decisions behind the plan

## Delta Lake over Parquet sinks
- **Decision**: Delta tables for Bronze, quarantine and Gold.
- **Rationale**: transaction log gives atomic micro-batch commits (with the checkpoint, exactly-once
  output), `OPTIMIZE` compacts small files without stopping writers, `VACUUM` reclaims space, and
  readers never see half-written batches. Works on local disk and on object storage alike.
- **Alternatives**: Parquet plus a periodic rewrite job (no atomic swap; readers can see partial
  rewrites); Iceberg (needs a catalog service for this setup).

## Compaction outside the stream
- **Decision**: `python spark/maintenance.py` runs OPTIMIZE then VACUUM (168 h retention).
- **Rationale**: micro-batch latency stays independent of maintenance; a scheduler owns cadence.
  Delta allows a blind append and a compaction to commit concurrently.
- **Alternatives**: `foreachBatch` compaction every N batches (slows the batch that runs it).

## Schema registry: Redpanda, JSON Schema, BACKWARD
- **Decision**: register `contract/transactions-value.schema.json` under `transactions-value`,
  compatibility BACKWARD; producer uses confluent-kafka's JSONSerializer (wire format: magic byte 0,
  4-byte schema id, JSON).
- **Rationale**: Redpanda already ships the registry on port 8081; JSON keeps messages readable;
  BACKWARD is what a single consumer needs.
- **Alternatives**: Avro (binary, needs from_avro in Spark and the spark-avro package); keeping the
  Python-only contract (no guard for other producers).

## Spark reading the wire format
- **Decision**: in `parse_events`, a message whose first byte is 0 and is at least 5 bytes long is
  decoded as header + JSON; the schema id goes to Bronze. Anything else gets `unregistered_schema`
  and goes to quarantine. Contract rules apply unchanged to the JSON part.
- **Rationale**: no per-record registry call; the producer-side serializer already validated
  against the registered schema.

## Monitoring
- **Decision**: a Python `StreamingQueryListener` appends one JSON line per progress event;
  `spark/health.py` reads the latest line per query and the topic's end offsets (confluent-kafka
  `get_watermark_offsets`) and applies thresholds: progress age 90 s, lag 5,000, batch 30 s; late
  rows dropped are a warning. Webhook via `ALERT_WEBHOOK_URL`.
- **Alternatives**: Spark UI only (not scriptable); Prometheus and Grafana (two more services for
  a single-machine project; the same metrics are in the log for whoever adds them).
