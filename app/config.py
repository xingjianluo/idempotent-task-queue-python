"""Configuration for the task queue service."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Runtime settings, all overridable via environment variables."""

    db_path: str = "tasks.db"
    max_pending: int = 1000
    max_attempts: int = 3
    lease_seconds: float = 30.0
    worker_count: int = 2
    poll_interval: float = 0.2

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.getenv("TASK_DB_PATH", cls.db_path),
            max_pending=int(os.getenv("TASK_MAX_PENDING", cls.max_pending)),
            max_attempts=int(os.getenv("TASK_MAX_ATTEMPTS", cls.max_attempts)),
            lease_seconds=float(os.getenv("TASK_LEASE_SECONDS", cls.lease_seconds)),
            worker_count=int(os.getenv("TASK_WORKER_COUNT", cls.worker_count)),
            poll_interval=float(os.getenv("TASK_POLL_INTERVAL", cls.poll_interval)),
        )
