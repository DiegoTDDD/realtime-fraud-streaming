"""Record every streaming query's progress to an append-only JSON Lines file (ADR 0009).

One line per micro-batch (and per idle heartbeat, so a quiet stream is not mistaken for a dead
one), read by spark/health.py without Spark. The same numbers the Spark UI shows, kept after the
job stops.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryProgress,
)


def _offsets(end_offset: str | None) -> dict[str, int]:
    """Kafka source endOffset, {"topic": {"0": 12, ...}}, flattened to {"0": 12, ...}."""
    if not end_offset:
        return {}
    parsed = json.loads(end_offset)
    if not isinstance(parsed, dict):
        return {}
    return {p: int(o) for topic in parsed.values() for p, o in topic.items()}


def progress_record(progress: StreamingQueryProgress) -> dict:
    """The fields the health check needs from a StreamingQueryProgress."""
    dropped = sum(int(op.numRowsDroppedByWatermark or 0) for op in progress.stateOperators)
    offsets: dict[str, int] = {}
    for source in progress.sources:
        offsets.update(_offsets(source.endOffset))
    return {
        "query": progress.name,
        "event": "progress",
        "batch_id": progress.batchId,
        "timestamp": progress.timestamp,
        "input_rows": progress.numInputRows,
        "input_rows_per_second": progress.inputRowsPerSecond,
        "processed_rows_per_second": progress.processedRowsPerSecond,
        "batch_duration_ms": progress.batchDuration,
        "late_rows_dropped": dropped,
        "end_offsets": offsets,
    }


class ProgressLog(StreamingQueryListener):
    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.names: dict[str, str] = {}
        self.lock = threading.Lock()

    def _write(self, record: dict) -> None:
        with self.lock, self.path.open("a") as f:
            f.write(json.dumps(record) + "\n")

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        self.names[str(event.id)] = event.name
        self._write({"query": event.name, "event": "started", "timestamp": event.timestamp})

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        self._write(progress_record(event.progress))

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        name = self.names.get(str(event.id), str(event.id))
        self._write({"query": name, "event": "idle", "timestamp": event.timestamp})

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        name = self.names.get(str(event.id), str(event.id))
        now = datetime.now(UTC).isoformat()
        self._write(
            {"query": name, "event": "terminated", "exception": event.exception, "timestamp": now}
        )
