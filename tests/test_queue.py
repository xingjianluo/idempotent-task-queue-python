"""Tests for the local idempotent task queue.

Every test gets its own SQLite file (tmp_path) so repeated runs and tests
running together never interfere with one another.
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.errors import QueueFull
from app.models import TaskStatus
from app.store import TaskStore
from app.worker import Worker


def make_store(tmp_path, **kw) -> TaskStore:
    settings = Settings(db_path=str(tmp_path / "queue.db"), **kw)
    store = TaskStore(settings)
    store.init_schema()
    return store


def drain(worker: Worker, limit: int = 100) -> int:
    ran = 0
    for _ in range(limit):
        if not worker.run_once():
            break
        ran += 1
    return ran


# --------------------------------------------------------------------- #
# 1. Concurrent duplicate submission: an idempotency key executes once
# --------------------------------------------------------------------- #
def test_concurrent_duplicate_submit_executes_once(tmp_path):
    settings = Settings(db_path=str(tmp_path / "q.db"))
    store = TaskStore(settings)
    store.init_schema()
    worker = Worker(store, settings)

    counter = {"n": 0}
    lock = threading.Lock()

    def handler(payload):
        with lock:
            counter["n"] += 1
        return {"value": payload["v"]}

    worker.register("add", handler)

    created_flags = []
    flags_lock = threading.Lock()

    def submit(_):
        task, created = store.submit(
            TaskStore.new_task_id(), "key-42", "add", json.dumps({"v": 7})
        )
        with flags_lock:
            created_flags.append(created)
        return task.id

    with ThreadPoolExecutor(max_workers=16) as pool:
        ids = list(pool.map(submit, range(50)))

    assert created_flags.count(True) == 1, "exactly one insert must succeed"
    assert len(set(ids)) == 1, "all callers must see the same task"

    drain(worker)

    task = store.get_by_key("key-42")
    assert task.status is TaskStatus.SUCCEEDED
    assert task.attempts == 1
    assert counter["n"] == 1, "handler must execute at most once for a key"
    assert task.result == {"value": 7}

    # A later repeat submit returns the existing result, never reruns.
    again, created = store.submit(
        TaskStore.new_task_id(), "key-42", "add", json.dumps({"v": 999})
    )
    assert created is False
    assert again.status is TaskStatus.SUCCEEDED
    assert again.result == {"value": 7}
    assert counter["n"] == 1


# --------------------------------------------------------------------- #
# 2. Retries until the limit, then terminal FAILED with the last error
# --------------------------------------------------------------------- #
def test_retries_exhausted_then_failed(tmp_path, caplog):
    settings = Settings(
        db_path=str(tmp_path / "q.db"), max_attempts=3, lease_seconds=0
    )
    store = TaskStore(settings)
    store.init_schema()
    worker = Worker(store, settings)

    def always_fail(payload):
        raise RuntimeError("boom")

    worker.register("bad", always_fail)
    store.submit(TaskStore.new_task_id(), "k-fail", "bad", "{}")

    caplog.set_level("INFO", logger="task_queue.worker")
    assert worker.run_once() is True  # attempt 1 -> requeued
    t = store.get_by_key("k-fail")
    assert t.status is TaskStatus.PENDING and t.attempts == 1
    assert t.last_error == "RuntimeError: boom"

    assert worker.run_once() is True  # attempt 2 -> requeued
    t = store.get_by_key("k-fail")
    assert t.status is TaskStatus.PENDING and t.attempts == 2

    assert worker.run_once() is True  # attempt 3 -> terminal failure
    t = store.get_by_key("k-fail")
    assert t.status is TaskStatus.FAILED and t.attempts == 3
    assert t.last_error == "RuntimeError: boom"

    # Failed tasks must never be redelivered and must not be lost.
    assert worker.run_once() is False
    assert store.get_by_key("k-fail").status is TaskStatus.FAILED

    joined = "\n".join(r.getMessage() for r in caplog.records)
    assert "idempotency_key=k-fail" in joined
    assert "delivery_count=3" in joined
    assert "terminal-failure" in joined
    assert "requeue-redeliver" in joined


# --------------------------------------------------------------------- #
# 3. Crash mid-processing / expired lease: task is redelivered
# --------------------------------------------------------------------- #
def test_redelivery_after_consumer_crash(tmp_path):
    # Negative lease: every claim is immediately considered expired, which
    # models a consumer that died right after claiming the task.
    settings = Settings(
        db_path=str(tmp_path / "q.db"), max_attempts=5, lease_seconds=-1
    )
    store = TaskStore(settings)
    store.init_schema()
    store.submit(TaskStore.new_task_id(), "k-crash", "anything", "{}")

    first = store.fetch_for_delivery()
    assert first.status is TaskStatus.RUNNING and first.attempts == 1

    # Consumer died without reporting success or failure: the same task is
    # claimed again by the next delivery round.
    second = store.fetch_for_delivery()
    assert second is not None
    assert second.id == first.id
    assert second.attempts == 2

    third = store.fetch_for_delivery()
    assert third.id == first.id and third.attempts == 3


def test_transient_failure_then_success(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "q.db"), max_attempts=3, lease_seconds=30
    )
    store = TaskStore(settings)
    store.init_schema()
    worker = Worker(store, settings)

    calls = {"n": 0}

    def flaky(payload):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return {"ok": True}

    worker.register("flaky", flaky)
    store.submit(TaskStore.new_task_id(), "k-flaky", "flaky", "{}")

    for _ in range(3):
        if not worker.run_once():
            break
    t = store.get_by_key("k-flaky")
    assert t.status is TaskStatus.SUCCEEDED
    assert t.attempts == 3 and t.result == {"ok": True}


# --------------------------------------------------------------------- #
# 4. Persistence across process restart
# --------------------------------------------------------------------- #
def test_state_survives_restart(tmp_path):
    db = str(tmp_path / "persist.db")
    settings = Settings(db_path=db)

    store = TaskStore(settings)
    store.init_schema()
    store.submit(
        TaskStore.new_task_id(), "k-restart", "echo", json.dumps({"a": 1})
    )
    store.close()

    # Simulate a brand-new process reopening the on-disk database.
    store2 = TaskStore(Settings(db_path=db))
    store2.init_schema()
    worker = Worker(store2, Settings(db_path=db))

    def echo(payload):
        return {"echoed": payload}

    worker.register("echo", echo)
    assert worker.run_once() is True
    store2.close()

    store3 = TaskStore(Settings(db_path=db))
    store3.init_schema()
    t = store3.get_by_key("k-restart")
    assert t.status is TaskStatus.SUCCEEDED
    assert t.result == {"echoed": {"a": 1}}
    store3.close()


# --------------------------------------------------------------------- #
# 5. Atomic exclusive delivery between two consumers
# --------------------------------------------------------------------- #
def test_two_consumers_never_get_same_task(tmp_path):
    settings = Settings(db_path=str(tmp_path / "q.db"), lease_seconds=60)
    store = TaskStore(settings)
    store.init_schema()
    store.submit(TaskStore.new_task_id(), "k-a", "t", "{}")
    store.submit(TaskStore.new_task_id(), "k-b", "t", "{}")

    claimed = []
    lock = threading.Lock()
    barrier = threading.Barrier(2)

    def claim():
        barrier.wait()  # maximise the race
        t = store.fetch_for_delivery()
        with lock:
            claimed.append(t.id if t else None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: claim(), range(2)))

    assert None not in claimed
    assert claimed[0] != claimed[1], "one task must not go to two consumers"
    assert store.fetch_for_delivery() is None


# --------------------------------------------------------------------- #
# 6. Backpressure: full queue is rejected with a distinguishable reason
# --------------------------------------------------------------------- #
def test_queue_full_rejected_store_level(tmp_path):
    store = make_store(tmp_path, max_pending=2)
    store.submit(TaskStore.new_task_id(), "p1", "t", "{}")
    store.submit(TaskStore.new_task_id(), "p2", "t", "{}")
    with pytest.raises(QueueFull):
        store.submit(TaskStore.new_task_id(), "p3", "t", "{}")


def test_api_rejects_with_distinguishable_reasons(tmp_path):
    db = str(tmp_path / "q.db")
    settings = Settings(db_path=db, max_pending=1)
    store = TaskStore(settings)
    store.init_schema()
    worker = Worker(store, settings)
    client = TestClient(create_app(store, worker, settings))

    r1 = client.post(
        "/tasks",
        json={"idempotency_key": "dup", "task_type": "echo", "payload": {}},
    )
    assert r1.status_code == 201 and r1.json()["reason"] == "accepted"

    # Duplicate while pending: rejected as a duplicate, not queue-full.
    r2 = client.post(
        "/tasks",
        json={"idempotency_key": "dup", "task_type": "echo", "payload": {}},
    )
    assert r2.status_code == 200
    assert r2.json()["reason"] == "idempotent_duplicate_pending"
    assert r2.json()["replayed"] is True

    # A *different* key under backpressure: explicit 503 queue_full.
    r3 = client.post(
        "/tasks",
        json={"idempotency_key": "other", "task_type": "echo", "payload": {}},
    )
    assert r3.status_code == 503
    body = r3.json()["detail"]
    assert body["reason"] == "queue_full"
    assert body["max_pending"] == 1

    # Finish the task; the same key now replays the completed result.
    worker.register("echo", lambda p: {"echoed": p})
    worker.run_once()
    r4 = client.post(
        "/tasks",
        json={"idempotency_key": "dup", "task_type": "echo",
              "payload": {"x": 1}},
    )
    assert r4.status_code == 200
    assert r4.json()["reason"] == "idempotent_replay_succeeded"
    assert r4.json()["task"]["result"] == {"echoed": {}}

    # Query endpoints.
    assert client.get(f"/tasks/{r1.json()['task']['id']}").status_code == 200
    assert client.get("/tasks/by-key/dup").json()["status"] == "succeeded"
    assert client.get("/tasks/nope").status_code == 404
