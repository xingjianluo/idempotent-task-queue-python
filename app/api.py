"""HTTP interface: submit, query and locally consume tasks."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import Settings
from app.errors import QueueFull, TaskNotFound
from app.models import Task
from app.store import TaskStore
from app.worker import Worker

logger = logging.getLogger("task_queue.api")


class SubmitBody(BaseModel):
    idempotency_key: str = Field(min_length=1)
    task_type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)


def _task_view(task: Task) -> dict[str, Any]:
    return {
        "id": task.id,
        "idempotency_key": task.idempotency_key,
        "task_type": task.task_type,
        "payload": task.payload,
        "status": task.status.value,
        "attempts": task.attempts,
        "result": task.result,
        "last_error": task.last_error,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


def create_app(store: TaskStore, worker: Worker, settings: Settings) -> FastAPI:
    app = FastAPI(title="Idempotent Local Task Queue")

    @app.post("/tasks", status_code=201)
    def submit_task(body: SubmitBody) -> dict[str, Any]:
        import json

        logger.info(
            "submit received idempotency_key=%s task_type=%s",
            body.idempotency_key, body.task_type,
        )
        try:
            task, created = store.submit(
                TaskStore.new_task_id(),
                body.idempotency_key,
                body.task_type,
                json.dumps(body.payload),
            )
        except QueueFull as exc:
            # Backpressure: explicit rejection, never silent drop/hang.
            logger.warning(
                "submit rejected idempotency_key=%s reason=queue_full %s",
                body.idempotency_key, exc,
            )
            raise HTTPException(status_code=503, detail={
                "reason": "queue_full",
                "message": str(exc),
                "max_pending": settings.max_pending,
            })

        if not created:
            # Same idempotency key never runs twice; surface the existing
            # task (including any result) and a machine-readable reason.
            if task.status.value == "succeeded":
                reason = "idempotent_replay_succeeded"
            elif task.status.value == "failed":
                reason = "idempotent_replay_failed"
            elif task.status.value == "running":
                reason = "idempotent_duplicate_in_flight"
            else:
                reason = "idempotent_duplicate_pending"
            logger.info(
                "submit rejected idempotency_key=%s reason=%s "
                "existing_task_id=%s delivery_count=%d",
                body.idempotency_key, reason, task.id, task.attempts,
            )
            return JSONResponse(
                status_code=200,
                content={
                    "replayed": True,
                    "reason": reason,
                    "task": _task_view(task),
                },
            )

        return JSONResponse(
            status_code=201,
            content={"replayed": False, "reason": "accepted",
                     "task": _task_view(task)},
        )

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        try:
            return _task_view(store.get(task_id))
        except TaskNotFound:
            raise HTTPException(status_code=404, detail={
                "reason": "task_not_found",
                "task_id": task_id,
            })

    @app.get("/tasks/by-key/{idempotency_key}")
    def get_task_by_key(idempotency_key: str) -> dict[str, Any]:
        task = store.get_by_key(idempotency_key)
        if task is None:
            raise HTTPException(status_code=404, detail={
                "reason": "task_not_found",
                "idempotency_key": idempotency_key,
            })
        return _task_view(task)

    @app.post("/tasks/consume")
    def consume_one() -> dict[str, Any]:
        """Synchronously deliver at most one queued task on this process."""
        delivered = worker.run_once()
        return {"delivered": delivered}

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "pending": store.pending_count(),
            "max_pending": settings.max_pending,
        }

    return app
