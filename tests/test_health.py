"""Health rules on synthetic progress logs."""

import json
from datetime import UTC, datetime, timedelta

from spark import health
from spark.health import Thresholds, evaluate

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)


def _iso(seconds_ago: float) -> str:
    return (NOW - timedelta(seconds=seconds_ago)).isoformat().replace("+00:00", "Z")


def progress(query, seconds_ago=5, batch_ms=2000, dropped=0, offsets=None):
    return {"query": query, "event": "progress", "batch_id": 1, "timestamp": _iso(seconds_ago),
            "input_rows": 100, "batch_duration_ms": batch_ms, "late_rows_dropped": dropped,
            "end_offsets": offsets or {}}  # fmt: skip


def healthy_log():
    return [{"query": q, "event": "started", "timestamp": _iso(600)} for q in health.QUERIES] + [
        progress("bronze", offsets={"0": 100, "1": 90, "2": 95}),
        progress("quarantine"),
        progress("gold"),
    ]


def failed(checks):
    return {(c.rule, c.query) for c in checks if not c.ok}


def test_a_running_caught_up_stream_is_healthy():
    checks = evaluate(healthy_log(), {"0": 100, "1": 92, "2": 95}, NOW)
    assert failed(checks) == set()
    lag = next(c for c in checks if c.rule == "lag")
    assert lag.detail.startswith("2 records behind")


def test_a_stopped_job_is_stale():
    log = healthy_log()
    checks = evaluate(log, {"0": 100, "1": 90, "2": 95}, NOW + timedelta(minutes=5))
    assert failed(checks) == {("fresh", "bronze"), ("fresh", "quarantine"), ("fresh", "gold")}


def test_idle_heartbeats_keep_a_quiet_stream_fresh():
    log = healthy_log()
    later = NOW + timedelta(minutes=5)
    log += [{"query": q, "event": "idle", "timestamp": (later - timedelta(seconds=3)).isoformat()}
            for q in health.QUERIES]  # fmt: skip
    assert failed(evaluate(log, {"0": 100, "1": 90, "2": 95}, later)) == set()


def test_lag_slow_batches_and_termination_fail():
    log = healthy_log()
    log.append(progress("gold", batch_ms=45_000))
    log.append({"query": "quarantine", "event": "terminated", "exception": "boom",
                "timestamp": _iso(1)})  # fmt: skip
    checks = evaluate(log, {"0": 9000, "1": 90, "2": 95}, NOW, Thresholds(max_lag=5000))
    assert failed(checks) == {("lag", "bronze"), ("batch time", "gold"), ("running", "quarantine")}


def test_a_restart_clears_an_old_termination():
    log = healthy_log()
    log.append({"query": "gold", "event": "terminated", "exception": None, "timestamp": _iso(30)})
    log.append({"query": "gold", "event": "started", "timestamp": _iso(20)})
    log.append(progress("gold", seconds_ago=2))
    assert failed(evaluate(log, {"0": 100, "1": 90, "2": 95}, NOW)) == set()


def test_late_rows_are_a_warning_not_a_failure():
    log = healthy_log() + [progress("gold", dropped=4)]
    late = next(
        c for c in evaluate(log, {"0": 100, "1": 90, "2": 95}, NOW) if c.rule == "late rows"
    )
    assert late.ok and late.warning and late.detail.startswith("4 late rows")


def test_no_log_and_no_kafka_fail():
    checks = evaluate([], None, NOW)
    assert {c.detail for c in checks} == {"no progress recorded"}
    checks = evaluate(healthy_log(), None, NOW)
    assert failed(checks) == {("lag", "bronze")}


def test_the_webhook_receives_the_failures(monkeypatch):
    sent = {}

    class Response:
        def close(self):
            pass

    def fake_urlopen(req, timeout):
        sent["url"], sent["body"] = req.full_url, json.loads(req.data)
        return Response()

    monkeypatch.setattr(health.urllib.request, "urlopen", fake_urlopen)
    failures = [c for c in evaluate([], None, NOW) if not c.ok]
    health.notify("https://hooks.example/abc", failures)
    assert sent["url"] == "https://hooks.example/abc"
    assert len(sent["body"]["failures"]) == 3
