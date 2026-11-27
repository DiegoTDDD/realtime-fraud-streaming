# Architecture decision records

| # | Decision | Status |
|---|---|---|
| [0001](0001-redpanda.md) | Redpanda as the Kafka-compatible broker | Accepted |
| [0002](0002-windows-and-watermark.md) | 1-minute tumbling windows on event time with a 2-minute watermark | Accepted |
| [0003](0003-bronze-gold-parquet.md) | Bronze and Gold layers in Parquet, Gold in append mode | Layering accepted; format superseded by 0007 |
| [0004](0004-event-contract-and-quarantine.md) | One event contract for producer and consumer; violations go to quarantine | Accepted |
| [0005](0005-partition-by-card.md) | Key messages by card_id over 3 partitions | Accepted |
| [0006](0006-static-sample-for-the-public-dashboard.md) | The public dashboard reads a committed Gold sample | Accepted |
| [0007](0007-delta-lake-tables.md) | Delta Lake tables with checkpoints, compacted outside the stream | Accepted |
| [0008](0008-schema-registry.md) | The topic's schema lives in the schema registry, with BACKWARD compatibility | Accepted |
| [0009](0009-stream-monitoring.md) | Query progress recorded to a log, health checked against written thresholds | Accepted |
