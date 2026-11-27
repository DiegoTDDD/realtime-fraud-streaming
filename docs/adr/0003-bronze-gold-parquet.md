# 0003. Bronze and Gold layers in Parquet, Gold in append mode

**Status:** Layering accepted. The storage format is superseded by [0007](0007-delta-lake-tables.md): the same layers are now Delta tables.

## Context
Two consumers need the data: the dashboard needs small per-minute aggregates, and model training needs every transaction. Both run locally, with no database server.

## Decision
- **Bronze** keeps every transaction as received, typed, in Parquet.
- **Gold** keeps the per-minute aggregates in Parquet, written in append mode: each window is written once, when the watermark closes it.
- There is no Silver layer. The only cleaning step, the contract check, sends bad events to a quarantine folder (see [0004](0004-event-contract-and-quarantine.md)).

## Alternatives considered
- **Delta Lake or Iceberg.** ACID tables, compaction and time travel. More setup than this project needs.
- **A database (PostgreSQL) for Gold.** Easy for the dashboard to query, but one more service to run.
- **Gold in complete or update mode.** Spark's Parquet sink supports only append mode.

## Consequences
- Streaming writes many small files. `spark/export_sample.py` merges Gold into one file for the deployed dashboard. A long-running deployment would need compaction, which Delta Lake would provide.
- Bronze can be replayed to rebuild Gold or to train a model.
