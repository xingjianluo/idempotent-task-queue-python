"""Domain models for tasks."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskStatus(str, enum.Enum):
    PENDING = "pending"        # accepted, waiting to be delivered
    RUNNING = "running"        # leased by a consumer, attempt in progress
    SUCCEEDED = "succeeded"    # terminal success
    FAILED = "failed"          # terminal failure (retries exhausted)


@dataclass
class Task:
    id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    result: dict[str, Any] | None = None
    last_error: str | None = None
    created_at: str = ""
    updated_at: str = ""


@dataclass
class SubmitRequest:
    idempotency_key: str
    task_type: str
    payload: dict[str, Any] = field(default_factory=dict)
