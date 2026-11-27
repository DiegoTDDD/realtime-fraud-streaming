# 0009. Query progress recorded to a log, health checked against written thresholds

**Status:** Accepted

## Context
A stalled query and a quiet minute looked the same: the job kept running, and nothing said whether it was keeping up with the topic.

## Decision
- A Python `StreamingQueryListener` (`spark/progress.py`) appends one JSON line per micro-batch to `data/ops/progress.jsonl`: rows read, rows per second, batch duration, rows dropped by the watermark, Kafka end offsets. Idle heartbeats are recorded too, so a quiet stream is not mistaken for a dead one; start and termination are recorded.
- `spark/health.py` reads the log and the topic's end offsets and applies written thresholds: every query running, last report under 90 s old, under 5,000 records of lag, last batch under 30 s. Late rows dropped by the watermark are a warning, not a failure (the watermark drops them by design). It exits 1 with the reasons, and posts them to `ALERT_WEBHOOK_URL` (Slack, Discord and Teams accept one) when set.
- A scheduler runs it every minute or two; the exit code is the alert.

## Alternatives considered
- **The Spark UI.** Shows the same numbers, but only while the job runs, and nothing can alert on it.
- **Prometheus and Grafana.** The standard for a fleet of jobs; two more services for one job on one machine. The log holds the same metrics for whoever adds them.

## Consequences
- CI runs the health check on the live pipeline (it must pass) and again after stopping the job (it must fail with stale progress or termination).
- The thresholds are flags on the command, so a deployment can tighten them without a code change.
