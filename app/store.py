"""SQLite-backed durable task store with atomic state transitions.

Durability: a single SQLite database on local disk (WAL mode). Committed
state survives process restarts.

Atomicity: every state transition runs inside a ``BEGIN IMMEDIATE``
transaction guarded by a process-wide lock, so concurrent submitters and
consumers can never observe or produce intermediate states. Claiming a
task is a single compare-and-swap (``UPDATE ... WHERE status='PENDING'``),
so one task can only be handed to one consumer.
"""

from __future__ import annotations

import enum
import sqlite3
import threading
import time
from dataclasses import dataclass


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class QueueFullError(Exception):
    """Raised when the number of unfinished tasks reaches the configured cap."""


@dataclass(frozen=True)
class TaskRecord:
    idempotency_key: str
    payload: str
    status: TaskStatus
    attempts: int  # number of deliveries so far
    max_attempts: int
    result: str | None
    last_error: str | None
    created_at: float
    updated_at: float
    lease_expires_at: float | None


@dataclass(frozen=True)
class SubmitOutcome:
    record: TaskRecord
    deduplicated: bool  # True when the idempotency key already existed


_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    idempotency_key TEXT PRIMARY KEY,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL,
    result          TEXT,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    lease_expires_at REAL
);
"""

# Statuses that still occupy queue capacity.
_UNFINISHED = (TaskStatus.PENDING.value, TaskStatus.RUNNING.value)


class TaskStore:
    """Durable task store. All state transitions are atomic."""

    def __init__(self, db_path: str, max_pending: int) -> None:
        self._max_pending = max_pending
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.isolation_level = None  # explicit transaction control
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)

    # -- helpers ---------------------------------------------------------

    @staticmethod
    def _to_record(row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            idempotency_key=row["idempotency_key"],
            payload=row["payload"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            result=row["result"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            lease_expires_at=row["lease_expires_at"],
        )

    def _get_locked(self, idempotency_key: str) -> TaskRecord | None:
        row = self._conn.execute(
            "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return self._to_record(row) if row else None

    # -- API -------------------------------------------------------------

    def submit(self, idempotency_key: str, payload: str, max_attempts: int) -> SubmitOutcome:
        """Insert a new task, or return the existing one for the same key.

        The capacity check and the insert happen in one IMMEDIATE
        transaction, so concurrent submitters cannot both pass the check
        and overflow the queue, nor insert the same key twice.
        """
        now = time.time()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._get_locked(idempotency_key)
                if existing is not None:
                    self._conn.execute("COMMIT")
                    return SubmitOutcome(record=existing, deduplicated=True)

                unfinished = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM tasks WHERE status IN (?, ?)",
                    _UNFINISHED,
                ).fetchone()["c"]
                if unfinished >= self._max_pending:
                    raise QueueFullError(
                        f"queue full: {unfinished} unfinished tasks >= cap {self._max_pending}"
                    )

                self._conn.execute(
                    "INSERT INTO tasks (idempotency_key, payload, status, attempts,"
                    " max_attempts, created_at, updated_at)"
                    " VALUES (?, ?, ?, 0, ?, ?, ?)",
                    (idempotency_key, payload, TaskStatus.PENDING.value, max_attempts, now, now),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return SubmitOutcome(record=self._get_locked(idempotency_key), deduplicated=False)

    def get(self, idempotency_key: str) -> TaskRecord | None:
        with self._lock:
            return self._get_locked(idempotency_key)

    def claim(self, lease_seconds: float) -> TaskRecord | None:
        """Atomically claim the oldest pending task, or None if none.

        First requeues RUNNING tasks whose lease expired (crashed or
        stalled consumers), then takes the oldest PENDING task via a
        compare-and-swap update. Two consumers can never claim the same
        task because the update only succeeds while status='PENDING'.
        """
        now = time.time()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # Re-deliver tasks whose consumer died mid-processing.
                self._conn.execute(
                    "UPDATE tasks SET status = ?, lease_expires_at = NULL, updated_at = ?"
                    " WHERE status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
                    (TaskStatus.PENDING.value, now, TaskStatus.RUNNING.value, now),
                )
                row = self._conn.execute(
                    "SELECT idempotency_key FROM tasks WHERE status = ?"
                    " ORDER BY created_at LIMIT 1",
                    (TaskStatus.PENDING.value,),
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None
                key = row["idempotency_key"]
                cursor = self._conn.execute(
                    "UPDATE tasks SET status = ?, attempts = attempts + 1,"
                    " lease_expires_at = ?, updated_at = ?"
                    " WHERE idempotency_key = ? AND status = ?",
                    (TaskStatus.RUNNING.value, now + lease_seconds, now, key, TaskStatus.PENDING.value),
                )
                if cursor.rowcount != 1:
                    self._conn.execute("ROLLBACK")
                    return None
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self._get_locked(key)

    def mark_succeeded(self, idempotency_key: str, result: str) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                "UPDATE tasks SET status = ?, result = ?, lease_expires_at = NULL, updated_at = ?"
                " WHERE idempotency_key = ? AND status = ?",
                (TaskStatus.SUCCEEDED.value, result, now, idempotency_key, TaskStatus.RUNNING.value),
            )

    def mark_failed_attempt(self, idempotency_key: str, error: str) -> TaskRecord:
        """Record a failed delivery; requeue or move to terminal FAILED.

        If the delivery count has reached max_attempts the task becomes
        FAILED (terminal, keeps last_error); otherwise it goes back to
        PENDING for at-least-once redelivery.
        """
        now = time.time()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                record = self._get_locked(idempotency_key)
                if record is None:
                    self._conn.execute("ROLLBACK")
                    raise KeyError(idempotency_key)
                exhausted = record.attempts >= record.max_attempts
                next_status = TaskStatus.FAILED if exhausted else TaskStatus.PENDING
                self._conn.execute(
                    "UPDATE tasks SET status = ?, last_error = ?,"
                    " lease_expires_at = NULL, updated_at = ?"
                    " WHERE idempotency_key = ? AND status = ?",
                    (next_status.value, error, now, idempotency_key, TaskStatus.RUNNING.value),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return self._get_locked(idempotency_key)

    def close(self) -> None:
        with self._lock:
            self._conn.close()
