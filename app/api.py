"""HTTP interface layer (FastAPI)."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from app.config import Settings
from app.service import TaskService
from app.store import QueueFullError, SubmitOutcome, TaskRecord
from app.worker import TaskHandler

logger = logging.getLogger("taskqueue.api")


class SubmitRequest(BaseModel):
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)]
    payload: str = ""


def _record_view(record: TaskRecord) -> dict:
    return {
        "idempotency_key": record.idempotency_key,
        "payload": record.payload,
        "status": record.status.value,
        "attempts": record.attempts,
        "max_attempts": record.max_attempts,
        "result": record.result,
        "last_error": record.last_error,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _submit_view(outcome: SubmitOutcome) -> dict:
    view = _record_view(outcome.record)
    view["deduplicated"] = outcome.deduplicated
    return view


def default_handler(payload: str) -> str:
    """Default local handler: echoes the payload."""
    return f"echo:{payload}"


def create_app(settings: Settings | None = None, handler: TaskHandler | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    service = TaskService(settings, handler or default_handler)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.start()
        yield
        service.stop()

    app = FastAPI(title="idempotent-task-queue", lifespan=lifespan)
    app.state.service = service

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.post("/tasks", status_code=201)
    def submit_task(body: SubmitRequest, request: Request):
        svc: TaskService = request.app.state.service
        try:
            outcome = svc.submit(body.idempotency_key, body.payload)
        except QueueFullError as exc:
            logger.info("submit rejected key=%s reason=queue_full", body.idempotency_key)
            raise HTTPException(
                status_code=429,
                detail={"reason": "queue_full", "message": str(exc)},
            ) from exc
        logger.info(
            "submit key=%s deduplicated=%s status=%s",
            body.idempotency_key,
            outcome.deduplicated,
            outcome.record.status.value,
        )
        return _submit_view(outcome)

    @app.get("/tasks/{idempotency_key}")
    def get_task(idempotency_key: str, request: Request):
        svc: TaskService = request.app.state.service
        record = svc.get(idempotency_key)
        if record is None:
            raise HTTPException(status_code=404, detail={"reason": "not_found"})
        return _record_view(record)

    return app
