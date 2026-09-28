"""SQLite-backed task store.

All state transitions are single, atomic SQL statements guarded by a
UNIQUE constraint on idempotency_key. SQLite's file locking makes every
transaction atomic across processes and threads, so a task can never be
delivered to two consumers at once.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone

from app.config import Settings
from app.errors import DuplicateIdempotencyKey, QueueFull, TaskNotFound
from app.models import Task, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    task_type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    last_error TEXT,
    lease_expires_at REAL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskStore:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._conn = sqlite3.connect(
            settings.db_path, timeout=30, check_same_thread=False,
            isolation_level=None,  # explicit transaction control
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        # Serializes write transactions inside this process (SQLite itself
        # serializes across processes via file locks).
        self._tx_lock = threading.RLock()

    def _begin(self) -> None:
        self._tx_lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")

    def _commit(self) -> None:
        self._conn.execute("COMMIT")
        self._tx_lock.release()

    def _rollback(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        finally:
            self._tx_lock.release()

    def _read_one(self, sql: str, args: tuple = ()):
        with self._tx_lock:
            return self._conn.execute(sql, args).fetchone()

    def init_schema(self) -> None:
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------ #
    # Submission
    # ------------------------------------------------------------------ #
    def submit(
        self, task_id: str, idempotency_key: str, task_type: str, payload: str
    ) -> tuple[Task, bool]:
        """Insert a new task. Returns (task, created).

        created=False means an existing task with the same idempotency key
        was returned instead of inserting a duplicate.
        """
        now = _now_iso()
        self._begin()
        finished = False
        try:
            # Duplicates take priority over backpressure: replaying an
            # existing key never adds queue load and must always succeed.
            existing = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                self._commit()
                finished = True
                return self._to_task(existing), False

            pending = self._conn.execute(
                "SELECT count(*) FROM tasks WHERE status = ?",
                (TaskStatus.PENDING.value,),
            ).fetchone()[0]
            if pending >= self._settings.max_pending:
                self._rollback()
                finished = True
                raise QueueFull(
                    f"queue is full: {pending} pending tasks "
                    f">= limit {self._settings.max_pending}"
                )
            self._conn.execute(
                "INSERT INTO tasks (id, idempotency_key, task_type, payload,"
                " status, attempts, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, 0, ?, ?)",
                (
                    task_id,
                    idempotency_key,
                    task_type,
                    payload,
                    TaskStatus.PENDING.value,
                    now,
                    now,
                ),
            )
            self._commit()
            finished = True
        except Exception:
            if not finished:
                self._rollback()
            raise
        row = self._read_one(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        )
        return self._to_task(row), True

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def get(self, task_id: str) -> Task:
        row = self._read_one(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        )
        if row is None:
            raise TaskNotFound(task_id)
        return self._to_task(row)

    def get_by_key(self, idempotency_key: str) -> Task | None:
        row = self._read_one(
            "SELECT * FROM tasks WHERE idempotency_key = ?",
            (idempotency_key,),
        )
        return self._to_task(row) if row is not None else None

    def pending_count(self) -> int:
        return self._read_one(
            "SELECT count(*) FROM tasks WHERE status = ?",
            (TaskStatus.PENDING.value,),
        )[0]

    # ------------------------------------------------------------------ #
    # Delivery / state transitions — each one atomic
    # ------------------------------------------------------------------ #
    def fetch_for_delivery(self) -> Task | None:
        """Atomically claim one deliverable task.

        Eligible tasks are PENDING ones, plus RUNNING ones whose lease has
        expired (a consumer died or stalled mid-attempt) — that is what
        gives at-least-once redelivery after a crash. The claim flips the
        row to RUNNING, bumps attempts and installs a fresh lease, all in
        one write transaction, so two consumers can never get the same
        task.
        """
        now_ts = time.time()
        self._begin()
        finished = False
        try:
            row = self._conn.execute(
                "SELECT * FROM tasks"
                " WHERE status = ?"
                "    OR (status = ? AND lease_expires_at IS NOT NULL"
                "        AND lease_expires_at <= ?)"
                " ORDER BY rowid LIMIT 1",
                (
                    TaskStatus.PENDING.value,
                    TaskStatus.RUNNING.value,
                    now_ts,
                ),
            ).fetchone()
            if row is None:
                self._commit()
                return None
            self._conn.execute(
                "UPDATE tasks SET status = ?, attempts = attempts + 1,"
                " lease_expires_at = ?, updated_at = ? WHERE id = ?",
                (
                    TaskStatus.RUNNING.value,
                    now_ts + self._settings.lease_seconds,
                    _now_iso(),
                    row["id"],
                ),
            )
            self._commit()
            finished = True
        except Exception:
            if not finished:
                self._rollback()
            raise
        return self.get(row["id"])

    def mark_succeeded(self, task_id: str, result: str) -> None:
        with self._tx_lock:
            cur = self._conn.execute(
                "UPDATE tasks SET status = ?, result = ?, last_error = NULL,"
                " lease_expires_at = NULL, updated_at = ?"
                " WHERE id = ? AND status = ?",
                (
                    TaskStatus.SUCCEEDED.value,
                    result,
                    _now_iso(),
                    task_id,
                    TaskStatus.RUNNING.value,
                ),
            )
        if cur.rowcount == 0:
            raise TaskNotFound(
                f"task {task_id} is not in running state; cannot complete"
            )

    def mark_failed(
        self, task_id: str, error: str, max_attempts: int
    ) -> TaskStatus:
        """Record a failed attempt.

        While attempts remain the task goes back to PENDING so it is
        redelivered. Once attempts >= max_attempts it enters the terminal
        FAILED state with the latest error preserved.
        """
        self._begin()
        finished = False
        try:
            row = self._conn.execute(
                "SELECT attempts, status FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if row is None or row["status"] != TaskStatus.RUNNING.value:
                self._rollback()
                finished = True
                raise TaskNotFound(
                    f"task {task_id} is not in running state; cannot fail"
                )
            new_status = (
                TaskStatus.FAILED
                if row["attempts"] >= max_attempts
                else TaskStatus.PENDING
            )
            self._conn.execute(
                "UPDATE tasks SET status = ?, last_error = ?,"
                " lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                (new_status.value, error, _now_iso(), task_id),
            )
            self._commit()
            finished = True
        except Exception:
            if not finished:
                self._rollback()
            raise
        return new_status

    # ------------------------------------------------------------------ #
    def _to_task(self, row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            task_type=row["task_type"],
            payload=json.loads(row["payload"]),
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            result=json.loads(row["result"]) if row["result"] else None,
            last_error=row["last_error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def new_task_id() -> str:
        return uuid.uuid4().hex
