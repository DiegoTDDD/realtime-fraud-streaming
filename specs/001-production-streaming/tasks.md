# Tasks: Production-grade streaming storage, schema governance and monitoring

**Input**: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md), [data-model.md](data-model.md)

## Phase 1: Setup

- [x] T001 Add delta-spark 3.3.2, deltalake and confluent-kafka schema-registry extras to requirements files
- [x] T002 Spark session builder with Delta extensions and the Kafka package in `spark/session.py`, shared by the job, maintenance and tests

## Phase 2: Foundational

- [x] T003 `contract.transaction.json_schema()` and committed `contract/transactions-value.schema.json`
- [x] T004 [P] Test: committed schema equals the generated one (`tests/test_schema.py`)

## Phase 3: User Story 1 - Storage that survives restarts (P1)

- [x] T005 [US1] Lineage columns (partition, offset, broker timestamp) and `ingest_date` in `spark/transforms.py`
- [x] T006 [US1] Delta sinks for Bronze, quarantine and Gold in `spark/streaming_job.py`
- [x] T007 [US1] `spark/maintenance.py`: OPTIMIZE, VACUUM, files before/after, duplicate check
- [x] T008 [P] [US1] Tests: a stream killed and restarted writes each record once; compaction keeps rows (`tests/test_delta.py`)
- [x] T009 [US1] Dashboard and `spark/export_sample.py` read the Delta Gold table

## Phase 4: User Story 2 - Schema governance (P2)

- [x] T010 [US2] `contract/registry.py`: register with BACKWARD, check compatibility, show subject
- [x] T011 [US2] Producer serializes through the registry (wire format)
- [x] T012 [US2] `parse_events` decodes the wire format; `unregistered_schema` to quarantine; `schema_id` to Bronze
- [x] T013 [P] [US2] Tests: wire-format decoding, unregistered messages, violations unchanged (`tests/test_transforms.py`)
- [x] T014 [US2] Fixture of an incompatible schema (`tests/fixtures/incompatible.schema.json`)

## Phase 5: User Story 3 - Monitoring (P3)

- [x] T015 [US3] `spark/progress.py` listener writing `data/ops/progress.jsonl`
- [x] T016 [US3] `spark/health.py`: rules, consumer lag, webhook, exit code
- [x] T017 [P] [US3] Tests on synthetic progress: healthy, stale, lagging, slow, webhook payload (`tests/test_health.py`)

## Phase 6: Polish

- [x] T018 Pipeline check: register schema, reject incompatible, unregistered message quarantined, health passes, kill and restart, no duplicates, compaction, health fails after stop
- [x] T019 ADRs 0007 (Delta), 0008 (schema registry), 0009 (monitoring); ADR 0003 and 0004 marked superseded where they changed
- [x] T020 README: architecture, commands, runbook, limits
