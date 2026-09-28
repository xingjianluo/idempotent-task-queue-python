"""Domain errors for the idempotent task queue."""


class TaskQueueError(Exception):
    """Base error."""


class DuplicateIdempotencyKey(TaskQueueError):
    """An existing task with the same idempotency key exists.

    Carries the existing task's public status so callers can distinguish
    "already finished, here is the result" from "still in flight".
    """

    def __init__(self, task_id: str, status: str):
        self.task_id = task_id
        self.status = status
        super().__init__(f"idempotency key already used by task {task_id} ({status})")


class QueueFull(TaskQueueError):
    """Backpressure: too many pending tasks, new submissions are rejected."""


class TaskNotFound(TaskQueueError):
    """No task exists for the given id."""
