"""Application service: wires the durable store to a local worker pool."""

from __future__ import annotations

from app.config import Settings
from app.store import SubmitOutcome, TaskRecord, TaskStore
from app.worker import TaskHandler, Worker


class TaskService:
    """Owns the store and the consumer pool lifecycle.

    Restart recovery needs no special code path: tasks left RUNNING by a
    crashed process simply have expired leases, and the next claim()
    requeues them automatically (at-least-once delivery).
    """

    def __init__(self, settings: Settings, handler: TaskHandler) -> None:
        self._settings = settings
        self._store = TaskStore(settings.db_path, settings.max_pending)
        self._workers = [
            Worker(
                store=self._store,
                handler=handler,
                lease_seconds=settings.lease_seconds,
                poll_interval=settings.poll_interval,
                name=f"worker-{i}",
            )
            for i in range(settings.worker_count)
        ]

    @property
    def store(self) -> TaskStore:
        return self._store

    def submit(self, idempotency_key: str, payload: str) -> SubmitOutcome:
        return self._store.submit(
            idempotency_key, payload, max_attempts=self._settings.max_attempts
        )

    def get(self, idempotency_key: str) -> TaskRecord | None:
        return self._store.get(idempotency_key)

    def start(self) -> None:
        for worker in self._workers:
            worker.start()

    def stop(self) -> None:
        for worker in self._workers:
            worker.stop()
        self._store.close()
