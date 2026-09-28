"""Local task consumer.

Delivery semantics are at-least-once: a claimed task holds a lease. If the
process dies while handling it, the lease expires and TaskStore will hand
the same task out again. Failures are requeued until the attempt budget is
exhausted, after which the task reaches the terminal FAILED state.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from app.config import Settings
from app.models import Task, TaskStatus
from app.store import TaskStore

logger = logging.getLogger("task_queue.worker")

TaskHandler = Callable[[dict[str, Any]], dict[str, Any]]


class Worker:
    def __init__(self, store: TaskStore, settings: Settings):
        self._store = store
        self._settings = settings
        self._handlers: dict[str, TaskHandler] = {}
        self._stop = threading.Event()

    def register(self, task_type: str, handler: TaskHandler) -> None:
        self._handlers[task_type] = handler

    # ------------------------------------------------------------------ #
    def run_once(self) -> bool:
        """Claim and execute at most one task. Returns whether work ran."""
        task = self._store.fetch_for_delivery()
        if task is None:
            return False
        self._execute(task)
        return True

    def _execute(self, task: Task) -> None:
        logger.info(
            "delivering task idempotency_key=%s task_id=%s task_type=%s "
            "delivery_count=%d",
            task.idempotency_key, task.id, task.task_type, task.attempts,
        )
        handler = self._handlers.get(task.task_type)
        if handler is None:
            self._fail(
                task,
                f"no handler registered for task_type {task.task_type!r}",
            )
            return
        try:
            result = handler(task.payload) or {}
        except Exception as exc:  # handler failure -> retry or fail terminal
            logger.warning(
                "attempt failed idempotency_key=%s delivery_count=%d "
                "max_attempts=%d error=%r",
                task.idempotency_key, task.attempts,
                self._settings.max_attempts, exc,
            )
            self._fail(task, f"{type(exc).__name__}: {exc}")
            return
        try:
            encoded = json.dumps(result)
        except (TypeError, ValueError) as exc:
            self._fail(task, f"result is not JSON serializable: {exc}")
            return
        self._store.mark_succeeded(task.id, encoded)
        logger.info(
            "task succeeded idempotency_key=%s delivery_count=%d "
            "decision=terminal-success",
            task.idempotency_key, task.attempts,
        )

    def _fail(self, task: Task, error: str) -> None:
        status = self._store.mark_failed(
            task.id, error, self._settings.max_attempts
        )
        if status is TaskStatus.FAILED:
            decision = (
                f"terminal-failure attempts={task.attempts} reached "
                f"max_attempts={self._settings.max_attempts}"
            )
            logger.error(
                "task failed idempotency_key=%s delivery_count=%d "
                "decision=%s last_error=%s",
                task.idempotency_key, task.attempts, decision, error,
            )
        else:
            decision = "requeue-redeliver"
            logger.info(
                "task requeued idempotency_key=%s delivery_count=%d "
                "attempts_remaining=%d decision=%s last_error=%s",
                task.idempotency_key,
                task.attempts,
                self._settings.max_attempts - task.attempts,
                decision,
                error,
            )

    # ------------------------------------------------------------------ #
    def run_forever(self, poll_interval: float = 0.2) -> None:
        """Consume until stop() is called (used by the background thread)."""
        while not self._stop.is_set():
            try:
                did_work = self.run_once()
            except Exception:
                logger.exception("worker loop error; continuing")
                did_work = False
            if not did_work:
                self._stop.wait(poll_interval)

    def start_background(self) -> threading.Thread:
        thread = threading.Thread(
            target=self.run_forever, name="task-worker", daemon=True
        )
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()
