"""Local consumer: claims tasks from the store and executes handlers.

Delivery semantics are at-least-once: a task is only marked terminal
(SUCCEEDED / FAILED) after the handler returns. If the process dies
mid-processing, the claim lease expires and the task is redelivered by
another claim. Failed deliveries are retried until max_attempts, then
the task becomes terminally FAILED with the last error preserved.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable

from app.store import TaskStore

logger = logging.getLogger("taskqueue.worker")

# handler(payload) -> result string; raising means the delivery failed
TaskHandler = Callable[[str], str]


class Worker:
    """Background consumer loop with graceful stop."""

    def __init__(
        self,
        store: TaskStore,
        handler: TaskHandler,
        lease_seconds: float,
        poll_interval: float,
        name: str = "worker",
    ) -> None:
        self._store = store
        self._handler = handler
        self._lease_seconds = lease_seconds
        self._poll_interval = poll_interval
        self._name = name
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name=self._name, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self._stop.is_set():
            if not self.run_once():
                self._stop.wait(self._poll_interval)

    def run_once(self) -> bool:
        """Claim and process at most one task. Returns True if one ran."""
        claimed = self._store.claim(self._lease_seconds)
        if claimed is None:
            return False
        key = claimed.idempotency_key
        logger.info(
            "deliver key=%s attempt=%d/%d", key, claimed.attempts, claimed.max_attempts
        )
        try:
            result = self._handler(claimed.payload)
        except Exception as exc:  # noqa: BLE001 - any handler failure triggers retry
            record = self._store.mark_failed_attempt(key, str(exc))
            logger.warning(
                "delivery failed key=%s attempt=%d/%d status=%s error=%s",
                key,
                claimed.attempts,
                claimed.max_attempts,
                record.status.value,
                exc,
            )
            return True
        self._store.mark_succeeded(key, result)
        logger.info("succeeded key=%s attempt=%d", key, claimed.attempts)
        return True
