"""End-to-end tests for the idempotent task queue.

Every test logs the idempotency key, delivery (attempt) count and the
decision basis for its assertions. Tests use per-test tmp_path databases
and unique idempotency keys, so repeated runs never interfere.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.store import QueueFullError, TaskStatus, TaskStore
from app.worker import Worker

logger = logging.getLogger("test.queue")


def key(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def make_settings(tmp_path, **overrides) -> Settings:
    base = dict(
        db_path=str(tmp_path / "tasks.db"),
        max_pending=100,
        max_attempts=3,
        lease_seconds=0.05,
        worker_count=0,
        poll_interval=0.01,
    )
    base.update(overrides)
    return Settings(**base)


def test_concurrent_duplicate_submit_executes_once(tmp_path):
    """Same idempotency key submitted concurrently -> stored once, executed once."""
    idem = key("dup")
    store = TaskStore(str(tmp_path / "t.db"), max_pending=100)
    outcomes = []

    def submit():
        outcomes.append(store.submit(idem, "payload", max_attempts=3))

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: submit(), range(32)))

    fresh = [o for o in outcomes if not o.deduplicated]
    deduped = [o for o in outcomes if o.deduplicated]
    logger.info(
        "key=%s submissions=%d accepted_new=%d deduplicated=%d basis=unique-key-constraint",
        idem, len(outcomes), len(fresh), len(deduped),
    )
    assert len(fresh) == 1, "exactly one submission may create the task"
    assert len(deduped) == 31
    assert all(o.record.idempotency_key == idem for o in outcomes)

    executions = []
    worker = Worker(store, lambda p: executions.append(p) or "done",
                    lease_seconds=5, poll_interval=0.01)
    while worker.run_once():
        pass
    record = store.get(idem)
    logger.info(
        "key=%s deliveries=%d executions=%d status=%s basis=single-claim-per-task",
        idem, record.attempts, len(executions), record.status.value,
    )
    assert len(executions) == 1, "handler must run exactly once"
    assert record.attempts == 1 and record.status == TaskStatus.SUCCEEDED
    store.close()


def test_concurrent_duplicate_submit_via_api(tmp_path):
    """Concurrent POSTs with the same key: one 201-new, rest deduplicated."""
    idem = key("api-dup")
    settings = make_settings(tmp_path)
    app = create_app(settings)
    with TestClient(app) as client:
        def post():
            return client.post("/tasks", json={"idempotency_key": idem, "payload": "x"})
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda _: post(), range(16)))
    assert all(r.status_code == 201 for r in responses)
    new = [r for r in responses if not r.json()["deduplicated"]]
    logger.info(
        "key=%s http_responses=%d new=%d basis=deduplicated-flag",
        idem, len(responses), len(new),
    )
    assert len(new) == 1


def test_crash_mid_processing_redelivers_after_restart(tmp_path):
    """Process dies while a task is RUNNING -> lease expires -> redelivered."""
    idem = key("crash")
    db = str(tmp_path / "t.db")
    store = TaskStore(db, max_pending=100)
    store.submit(idem, "p", max_attempts=3)
    claimed = store.claim(lease_seconds=0.05)
    assert claimed.attempts == 1
    # simulate crash: no ack, process "restarts" (new store on same db file)
    store.close()
    store2 = TaskStore(db, max_pending=100)
    time.sleep(0.06)  # let the lease expire
    redelivered = store2.claim(lease_seconds=5)
    logger.info(
        "key=%s first_delivery_attempt=%d redelivery_attempt=%s basis=lease-expiry-requeue",
        idem, claimed.attempts,
        redelivered.attempts if redelivered else None,
    )
    assert redelivered is not None, "crashed task must be redelivered after restart"
    assert redelivered.idempotency_key == idem and redelivered.attempts == 2
    store2.mark_succeeded(idem, "recovered")
    assert store2.get(idem).status == TaskStatus.SUCCEEDED
    store2.close()


def test_retry_until_exhausted_then_terminal_failed(tmp_path):
    """Failing handler: retried up to max_attempts, then FAILED with last error."""
    idem = key("retry")
    store = TaskStore(str(tmp_path / "t.db"), max_pending=100)
    store.submit(idem, "p", max_attempts=3)
    deliveries = []

    def handler(payload):
        deliveries.append(payload)
        raise RuntimeError(f"boom-{len(deliveries)}")

    worker = Worker(store, handler, lease_seconds=5, poll_interval=0.01)
    while worker.run_once():
        pass
    record = store.get(idem)
    logger.info(
        "key=%s deliveries=%d status=%s last_error=%s basis=max-attempts-exhausted",
        idem, record.attempts, record.status.value, record.last_error,
    )
    assert record.attempts == 3, "must attempt exactly max_attempts times"
    assert len(deliveries) == 3
    assert record.status == TaskStatus.FAILED
    assert record.last_error == "boom-3", "most recent failure reason must be kept"
    # terminal tasks are never redelivered
    assert not worker.run_once()
    store.close()


def test_transient_failure_recovers_within_attempts(tmp_path):
    """A task that fails once then succeeds ends SUCCEEDED with attempts=2."""
    idem = key("flaky")
    store = TaskStore(str(tmp_path / "t.db"), max_pending=100)
    store.submit(idem, "p", max_attempts=3)
    calls = []

    def handler(payload):
        calls.append(payload)
        if len(calls) == 1:
            raise RuntimeError("transient")
        return "ok"

    worker = Worker(store, handler, lease_seconds=5, poll_interval=0.01)
    while worker.run_once():
        pass
    record = store.get(idem)
    logger.info(
        "key=%s deliveries=%d status=%s basis=retry-after-transient-failure",
        idem, record.attempts, record.status.value,
    )
    assert record.status == TaskStatus.SUCCEEDED and record.attempts == 2
    assert record.last_error == "transient"
    store.close()


def test_queue_full_rejects_with_distinguishable_reason(tmp_path):
    """Over-cap submissions are rejected with reason=queue_full, not dropped."""
    settings = make_settings(tmp_path, max_pending=2, max_attempts=1)
    app = create_app(settings)  # worker_count=0: nothing drains the queue
    with TestClient(app) as client:
        keys = [key("full") for _ in range(3)]
        r1 = client.post("/tasks", json={"idempotency_key": keys[0], "payload": ""})
        r2 = client.post("/tasks", json={"idempotency_key": keys[1], "payload": ""})
        r3 = client.post("/tasks", json={"idempotency_key": keys[2], "payload": ""})
        logger.info(
            "keys=%s statuses=%s basis=http-429-queue_full",
            keys, [r1.status_code, r2.status_code, r3.status_code],
        )
        assert r1.status_code == 201 and r2.status_code == 201
        assert r3.status_code == 429
        assert r3.json()["detail"]["reason"] == "queue_full"
        # rejected task must not exist (explicit rejection, not silent drop)
        assert client.get(f"/tasks/{keys[2]}").status_code == 404
        # store-level rejection raises a typed error too
        with pytest.raises(QueueFullError):
            app.state.service.store.submit(key("full"), "", 1)


def test_duplicate_submit_returns_existing_result(tmp_path):
    """Resubmitting a finished key returns the stored result, no re-execution."""
    idem = key("done")
    settings = make_settings(tmp_path, worker_count=1)
    app = create_app(settings)
    with TestClient(app) as client:
        client.post("/tasks", json={"idempotency_key": idem, "payload": "hi"})
        deadline = time.time() + 5
        while time.time() < deadline:
            body = client.get(f"/tasks/{idem}").json()
            if body["status"] == "SUCCEEDED":
                break
            time.sleep(0.02)
        assert body["status"] == "SUCCEEDED"
        again = client.post("/tasks", json={"idempotency_key": idem, "payload": "hi"})
        logger.info(
            "key=%s resubmit_deduplicated=%s status=%s result=%s basis=return-existing-record",
            idem, again.json()["deduplicated"], again.json()["status"], again.json()["result"],
        )
        assert again.json()["deduplicated"] is True
        assert again.json()["status"] == "SUCCEEDED"
        assert again.json()["result"] == "echo:hi"
        assert again.json()["attempts"] == 1, "no re-execution on duplicate submit"
