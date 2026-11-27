# Feature Specification: Production-grade streaming storage, schema governance and monitoring

**Feature Branch**: `001-production-streaming`

**Created**: 2026-11-26

**Status**: Implemented

**Input**: User description: "Bring the streaming pipeline to the level a senior data engineer would
ship: fix the limitations the README admits (many small Parquet files, no schema registry, no
monitoring of the queries)."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Storage that survives restarts and stays fast (Priority: P1)

The data team restarts or redeploys the streaming job at any time. Every transaction ends up in
Bronze exactly once, Gold windows are not duplicated, and the tables do not degrade into
thousands of tiny files that make every read slower.

**Why this priority**: A pipeline that duplicates or loses events on restart produces wrong fraud
counts; one that degrades with small files gets slower every hour. Both break trust first.

**Independent Test**: Run the pipeline, kill the Spark job mid-stream, restart it, and check that
every (partition, offset) appears once in Bronze. Run compaction and check the file count drops
while row counts stay the same.

**Acceptance Scenarios**:

1. **Given** the job has written several micro-batches, **When** it is killed and restarted,
   **Then** Bronze holds each Kafka record exactly once and Gold holds each window once.
2. **Given** Bronze has many small files, **When** the maintenance job runs, **Then** the number
   of active files drops and every table has the same rows as before.
3. **Given** the maintenance job runs while the stream is writing, **When** both finish, **Then**
   neither fails and no row is lost.

---

### User Story 2 - A topic whose schema cannot be broken by accident (Priority: P2)

A developer changes the event format. The schema registry refuses a change that would break the
consumer, and the producer cannot publish events that do not match the registered schema. A
message published without a registered schema is quarantined, not silently parsed.

**Why this priority**: The contract today is a Python module only this repository sees. A second
producer would have no guard.

**Independent Test**: Register the schema, try to register an incompatible version, and check the
registry refuses it. Publish a raw JSON message without the schema header and check it lands in
quarantine with the reason `unregistered_schema`.

**Acceptance Scenarios**:

1. **Given** the schema is registered with BACKWARD compatibility, **When** a version that changes
   a field's type or adds a required field is submitted, **Then** the registry rejects it.
2. **Given** the producer serializes through the registry, **When** Spark reads the topic, **Then**
   Bronze records the schema id of each event.
3. **Given** a message without the registry's header, **When** Spark reads it, **Then** it goes to
   quarantine with `unregistered_schema` and never reaches Gold.
4. **Given** the contract module changes, **When** the committed JSON Schema is not regenerated,
   **Then** a unit test fails.

---

### User Story 3 - Someone knows when the stream is unhealthy (Priority: P3)

An operator runs one command, or a scheduler runs it, and learns whether the stream is healthy:
queries running, consumer lag small, batches fast enough, few late events dropped. When it is not,
the command exits non-zero, says why, and can notify a webhook.

**Why this priority**: Without it, a stalled query looks the same as a quiet minute.

**Independent Test**: With the pipeline running, the health check passes. After stopping the Spark
job, it fails with "stale progress". With the producer far ahead of Spark, it fails on lag.

**Acceptance Scenarios**:

1. **Given** the job is running and caught up, **When** the health check runs, **Then** it reports
   every query healthy and exits 0.
2. **Given** the job was stopped, **When** the health check runs, **Then** it exits 1 and names the
   query whose last progress is older than the threshold.
3. **Given** the consumer lag exceeds the threshold, **When** the health check runs, **Then** it
   exits 1 with the lag per partition.
4. **Given** `ALERT_WEBHOOK_URL` is set, **When** a check fails, **Then** a JSON message with the
   failures is posted to it.

### Edge Cases

- A message that is valid JSON but has no schema header: quarantine, `unregistered_schema`.
- A message with a header but a schema id the registry does not know: quarantine, `unknown_schema_id`
  is out of scope (Spark does not call the registry per record); the id is kept in Bronze so it can
  be audited.
- Compaction while the stream appends: Delta's optimistic concurrency lets blind appends and
  compaction commit together; the test runs both at once.
- First run with checkpoints from the Parquet version: the README says to delete them (new sinks).
- Health check before the job ever ran: fails with "no progress recorded".

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Bronze, quarantine and Gold MUST be Delta tables written by checkpointed streaming
  queries, so a restart neither loses nor duplicates records.
- **FR-002**: Bronze MUST keep Kafka lineage for every record: topic partition, offset, broker
  timestamp and schema id.
- **FR-003**: A maintenance command MUST compact small files (OPTIMIZE) and remove unreferenced
  files older than a retention window (VACUUM, 7 days by default), reporting files before and after.
- **FR-004**: The topic's value schema MUST be a JSON Schema generated from the contract, committed
  in the repository, registered under `transactions-value` with BACKWARD compatibility.
- **FR-005**: The producer MUST serialize every event through the schema registry (Confluent wire
  format) and keep the existing contract check.
- **FR-006**: Spark MUST read the wire format, quarantine messages without it as
  `unregistered_schema`, and apply the contract rules to the rest as before.
- **FR-007**: A compatibility command MUST check a schema against the registry before it is
  registered, and CI MUST show an incompatible change being rejected.
- **FR-008**: The job MUST record each query's progress (rows in, rows per second, batch duration,
  rows dropped by the watermark, end offsets) to an append-only file.
- **FR-009**: A health command MUST evaluate written thresholds (progress age, consumer lag, batch
  duration, late rows dropped) and exit non-zero with reasons; it MUST post to a webhook when one is
  configured.
- **FR-010**: The dashboard and the sample export MUST read the Delta Gold table.

### Key Entities

- **Bronze record**: a transaction as received plus Kafka lineage and schema id.
- **Quarantine record**: the original message, the reasons and when it was quarantined.
- **Gold window**: one minute of valid transactions: counts, amounts, fraud by pattern.
- **Query progress**: one row per micro-batch per query, with timing, throughput and offsets.
- **Health result**: pass or fail per rule, with the measured value and the threshold.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: After a kill and restart in CI, Bronze has zero duplicate (partition, offset) pairs and
  zero missing offsets in the produced range.
- **SC-002**: Compaction in CI reduces Bronze's active files by at least 80% with identical row counts.
- **SC-003**: CI shows the registry rejecting an incompatible schema and an unregistered message in
  quarantine with the right reason.
- **SC-004**: The health check passes on the running pipeline and fails within 2 minutes of the job
  stopping, in CI.
- **SC-005**: All existing unit tests still pass, plus new ones for the wire format, schema drift,
  maintenance and health rules.

## Assumptions

- Redpanda's built-in schema registry (already exposed on port 8081) is the registry.
- JSON Schema over Avro: events are JSON today; the change keeps them readable in the console.
- Single machine, local storage. Object storage and a scheduler (cron, Airflow) are where a
  deployment would put the same commands; the README says so.
- Alert delivery is a generic webhook (Slack, Discord and Teams all accept one).

## Clarifications

### Session 2026-11-26

- Q: Avro or JSON Schema? → A: JSON Schema; the payload stays JSON, the registry still enforces
  compatibility, and no codec change is needed in Spark.
- Q: Which compatibility mode? → A: BACKWARD, the registry default: the consumer (Spark) must read
  both old and new events.
- Q: Run compaction inside the streaming job or separately? → A: Separately, as a command a
  scheduler can run; compaction must not slow micro-batches.
- Q: Where do progress metrics go? → A: An append-only JSON Lines file per run directory; small,
  readable by the health check without Spark.
- Q: Thresholds? → A: progress older than 90 s, lag above 5,000 records, batch duration above 30 s,
  any late rows dropped in the last 10 batches reported as a warning (not a failure).
