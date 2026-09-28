"""Configuration for the local task queue."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    db_path: str = os.environ.get(
        "TASK_QUEUE_DB", os.path.join(os.getcwd(), "task_queue.db")
    )
    max_attempts: int = int(os.environ.get("TASK_QUEUE_MAX_ATTEMPTS", "3"))
    lease_seconds: int = int(os.environ.get("TASK_QUEUE_LEASE_SECONDS", "30"))
    max_pending: int = int(os.environ.get("TASK_QUEUE_MAX_PENDING", "1000"))


def get_settings() -> Settings:
    return Settings()
