# Data model

## bronze (Delta, partitioned by `ingest_date`)
Contract fields (`transaction_id` ... `fraud_type`), `event_time`, `kafka_partition` (int),
`kafka_offset` (long), `kafka_timestamp` (timestamp), `schema_id` (int, null when unregistered),
`ingest_date` (date of `kafka_timestamp`). Key: (`kafka_partition`, `kafka_offset`).

## quarantine (Delta)
`raw_value` (string: the JSON part, or the whole message when it had no header), `violations`
(array<string>), `kafka_partition`, `kafka_offset`, `quarantined_at`.

## gold (Delta)
`window_start`, `window_end`, `tx_count`, `total_amount`, `avg_amount`, `fraud_high_amount`,
`fraud_card_testing`, `fraud_impossible_travel`, `fraud_total`. Key: (`window_start`).

## progress (`data/ops/progress.jsonl`)
`query`, `batch_id`, `timestamp`, `input_rows`, `input_rows_per_second`,
`processed_rows_per_second`, `batch_duration_ms`, `late_rows_dropped`, `end_offsets`
({partition: offset}).
