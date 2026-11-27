"""Is the stream healthy? Pass or fail against written thresholds (ADR 0009).

    python spark/health.py                 # exit 0 when healthy, 1 with the reasons when not
    ALERT_WEBHOOK_URL=https://... python spark/health.py   # also post failures to a webhook

Reads data/ops/progress.jsonl (written by the job, spark/progress.py) and the topic's end
offsets from Kafka. Rules:

  running     every query (bronze, quarantine, gold) has reported and none terminated
  fresh       each query's latest progress or idle heartbeat is under 90 s old
  lag         records on the topic that Bronze has not read: under 5,000
  batch time  the latest micro-batch of each query took under 30 s
  late rows   rows dropped by Gold's watermark in its last 10 batches (warning only: the
              2-minute watermark drops them by design, but a rising count means a slow producer)

Run it from a scheduler every minute or two; the exit code is the alert.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

log = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROGRESS_LOG = PROJECT_ROOT / "data" / "ops" / "progress.jsonl"
QUERIES = ("bronze", "quarantine", "gold")


@dataclass(frozen=True)
class Thresholds:
    max_age_s: float = 90
    max_lag: int = 5000
    max_batch_ms: int = 30_000
    late_window_batches: int = 10


@dataclass
class Check:
    rule: str
    query: str
    ok: bool
    detail: str
    warning: bool = False


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def read_log(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def evaluate(records: list[dict], end_offsets: dict[str, int] | None, now: datetime,
             t: Thresholds = Thresholds()) -> list[Check]:  # fmt: skip
    """The health rules on a progress log. `end_offsets` maps partition -> Kafka high watermark,
    or None when Kafka could not be reached."""
    checks: list[Check] = []
    by_query: dict[str, list[dict]] = {q: [] for q in QUERIES}
    for r in records:
        if r.get("query") in by_query:
            by_query[r["query"]].append(r)
    for q, rs in by_query.items():
        if not rs:
            checks.append(Check("running", q, False, "no progress recorded"))
            continue
        # The log is per run directory and append-only: a restart starts a new "started" record.
        last_start = max((i for i, r in enumerate(rs) if r["event"] == "started"), default=-1)
        current = rs[last_start + 1 :] if last_start >= 0 else rs
        terminated = [r for r in current if r["event"] == "terminated"]
        if terminated:
            checks.append(
                Check(
                    "running",
                    q,
                    False,
                    f"terminated: {terminated[-1].get('exception') or 'stopped'}",
                )
            )
            continue
        checks.append(Check("running", q, True, "running"))
        latest = max(rs, key=lambda r: _ts(r["timestamp"]))
        age = (now - _ts(latest["timestamp"])).total_seconds()
        stale = age > t.max_age_s
        detail = f"{'stale progress: ' if stale else ''}last report {age:.0f} s ago"
        checks.append(Check("fresh", q, not stale, detail))
        batches = [r for r in current if r["event"] == "progress"]
        if batches:
            ms = batches[-1]["batch_duration_ms"]
            checks.append(
                Check("batch time", q, ms <= t.max_batch_ms, f"last batch {ms / 1000:.1f} s")
            )
        if q == "gold":
            dropped = sum(r.get("late_rows_dropped", 0) for r in batches[-t.late_window_batches :])
            n = min(len(batches), t.late_window_batches)
            detail = f"{dropped} late rows dropped in the last {n} batches"
            checks.append(Check("late rows", q, True, detail, warning=dropped > 0))
        if q == "bronze":
            read = next((r["end_offsets"] for r in reversed(batches) if r.get("end_offsets")), {})
            if end_offsets is None:
                checks.append(Check("lag", q, False, "cannot reach Kafka to measure lag"))
            else:
                per = {p: max(0, hw - read.get(p, 0)) for p, hw in end_offsets.items()}
                lag = sum(per.values())
                checks.append(Check("lag", q, lag <= t.max_lag, f"{lag} records behind {per}"))
    return checks


def kafka_end_offsets(bootstrap: str, topic: str) -> dict[str, int] | None:
    try:
        from confluent_kafka import Consumer, TopicPartition

        c = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "health-check",
                "enable.auto.commit": False,
                "broker.address.family": "v4",  # "localhost" would try ::1 first and log noise
            }
        )
        meta = c.list_topics(topic, timeout=5)
        out = {}
        for p in meta.topics[topic].partitions:
            _, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=5)
            out[str(p)] = high
        c.close()
        return out
    except Exception:  # noqa: BLE001 - any failure to reach Kafka is reported as a failed check
        return None


def notify(url: str, failures: list[Check]) -> None:
    body = json.dumps({"text": "realtime-fraud-streaming is unhealthy",
                       "failures": [asdict(c) for c in failures]}).encode()  # fmt: skip
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=10).close()  # noqa: S310 - URL comes from the operator


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--progress", type=Path, default=PROGRESS_LOG)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--max-age", type=float, default=Thresholds.max_age_s)
    parser.add_argument("--max-lag", type=int, default=Thresholds.max_lag)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    t = Thresholds(max_age_s=args.max_age, max_lag=args.max_lag)
    checks = evaluate(
        read_log(args.progress), kafka_end_offsets(args.bootstrap, args.topic), datetime.now(UTC), t
    )
    for c in checks:
        mark = "WARN" if c.warning else ("ok  " if c.ok else "FAIL")
        log.info("%s %-10s %-10s %s", mark, c.rule, c.query, c.detail)
    failures = [c for c in checks if not c.ok]
    if failures and os.environ.get("ALERT_WEBHOOK_URL"):
        notify(os.environ["ALERT_WEBHOOK_URL"], failures)
    log.info("healthy" if not failures else f"UNHEALTHY: {len(failures)} failed check(s)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
