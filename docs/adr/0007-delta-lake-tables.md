# 0007. Delta Lake tables with checkpoints, compacted outside the stream

**Status:** Accepted. Supersedes the storage format in [0003](0003-bronze-gold-parquet.md); the layering (Bronze, quarantine, Gold in append mode) is unchanged.

## Context
The Parquet sinks wrote one small file per partition per micro-batch, so reads got slower every hour, and nothing compacted them safely: rewriting files under a reader could expose half a batch. A restart was safe for Parquet only because of the file sink's own metadata folder, which other tools do not read.

## Decision
- Bronze, quarantine and Gold are Delta tables (`data/delta/`), written by streaming queries with checkpoints. A micro-batch is one Delta commit, and Delta records the query id and batch id it already committed, so a restarted query neither skips nor repeats a batch. Readers only see committed versions.
- Bronze carries Kafka lineage (partition, offset, broker timestamp) and the schema id, and is partitioned by ingest date. `(kafka_partition, kafka_offset)` identifies a record.
- Queries trigger every 10 seconds instead of as fast as possible: fewer, larger files, well inside the 1-minute windows.
- `spark/maintenance.py` runs OPTIMIZE (bin-packing compaction) and VACUUM (7-day retention) as its own command, for a scheduler. It can run while the stream writes: compaction and blind appends commit together under Delta's optimistic concurrency. It fails if a table's row count changes. `--check-duplicates` checks that every Kafka record is in Bronze exactly once, with no gap in the offsets.
- The dashboard and the sample export read the Gold table with `deltalake`, which needs no Spark.

## Alternatives considered
- **Parquet plus a rewrite job.** No atomic swap: a reader can see the old and new files at once.
- **Apache Iceberg.** Equivalent guarantees, but it wants a catalog service; Delta works on a plain folder.
- **Compaction inside the stream (`foreachBatch`).** It would slow down the batch that runs it.

## Consequences
- CI kills the job with SIGKILL mid-stream, restarts it, and requires zero duplicate and zero missing offsets in Bronze; it also compacts while the stream writes.
- Old checkpoints from the Parquet version do not work with the new sinks: delete `data/_checkpoints/` after upgrading.
