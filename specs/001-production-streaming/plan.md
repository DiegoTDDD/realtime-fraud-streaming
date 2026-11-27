# Implementation Plan: Production-grade streaming storage, schema governance and monitoring

**Branch**: `001-production-streaming` | **Date**: 2026-11-26 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-production-streaming/spec.md`

## Summary

Replace the Parquet sinks with Delta Lake tables (exactly-once with checkpoints, compaction and
vacuum through a maintenance command), put the topic under Redpanda's schema registry with a JSON
Schema generated from the contract, and record every query's progress so a health command can
pass or fail the stream against written thresholds and notify a webhook. CI proves each claim on
the real stack: kill-and-restart without duplicates, compaction with identical counts, an
incompatible schema rejected, an unregistered message quarantined, health passing then failing.

## Technical Context

**Language/Version**: Python 3.11

**Primary Dependencies**: PySpark 3.5.8, delta-spark 3.3.2 (Delta 3.3 is the line for Spark 3.5),
confluent-kafka 2.5.3 with its schema-registry client, jsonschema, deltalake (Python reader for the
dashboard, no Spark)

**Storage**: Delta tables on local disk (`data/delta/{bronze,quarantine,gold}`), checkpoints in
`data/_checkpoints/`, progress in `data/ops/progress.jsonl`

**Testing**: pytest on a local SparkSession with Delta; GitHub Actions pipeline check with Redpanda
in Docker

**Target Platform**: Linux, macOS and Windows (local); Streamlit Community Cloud for the dashboard

**Project Type**: data pipeline (producer, streaming job, maintenance and health commands, dashboard)

**Performance Goals**: batches under 30 s; lag under 5,000 records at 10 events/s

**Constraints**: free to run; no paid service; the public dashboard keeps serving a committed sample

**Scale/Scope**: one topic, three partitions, one producer, one Spark job

## Constitution Check

| Principle | How this plan meets it |
|---|---|
| I. One contract | JSON Schema generated from `contract/transaction.py`; a test fails on drift; registry enforces it for every producer; Spark quarantines unregistered messages. |
| II. Correct under failure | Delta sinks with checkpoints; CI kills the job mid-stream and counts (partition, offset) pairs. |
| III. Tested without infrastructure | Wire-format parsing, schema generation, maintenance and health rules are unit-tested locally; the pipeline check covers the real stack. |
| IV. Operable | Progress log, health command with thresholds and webhook, ADRs 0007-0009, runbook section in the README. |
| V. Honest limits | README limits rewritten: single machine, local disk, webhook instead of paging. |

Re-checked after design: no violations.

## Project Structure

### Documentation (this feature)

```text
specs/001-production-streaming/
├── spec.md
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
└── tasks.md
```

### Source Code (repository root)

```text
contract/
├── transaction.py              # unchanged rules; adds json_schema()
├── transactions-value.schema.json  # generated, committed, registered
└── registry.py                 # register / check compatibility / set mode
producer/producer.py            # serializes through the registry
spark/
├── transforms.py               # parse wire format, lineage columns, unregistered_schema
├── streaming_job.py            # Delta sinks, progress listener
├── progress.py                 # StreamingQueryListener -> data/ops/progress.jsonl
├── maintenance.py              # OPTIMIZE + VACUUM, files before/after
├── health.py                   # thresholds, consumer lag, webhook
└── export_sample.py            # reads Delta Gold
dashboard/app.py                # reads Delta Gold (deltalake), falls back to the sample
tests/
├── test_contract.py
├── test_transforms.py          # wire format cases added
├── test_schema.py              # committed schema == generated
├── test_delta.py               # sinks, restart without duplicates, compaction
└── test_health.py              # rules on synthetic progress
docs/adr/0007..0009
```

**Structure Decision**: keep the existing single-project layout; new commands sit beside the job in
`spark/` and the registry helper beside the contract.
