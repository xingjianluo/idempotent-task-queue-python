"""Entry point: wires the store, worker and HTTP app together.

A background consumer thread is started unless TASK_QUEUE_WORKER=0. Even
with it disabled, POST /tasks/consume drives local consumption manually.
"""

from __future__ import annotations

import logging
import os

import uvicorn

from app.api import create_app
from app.config import get_settings
from app.store import TaskStore
from app.worker import Worker

logging.basicConfig(
    level=os.environ.get("TASK_QUEUE_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def build_app(start_worker: bool | None = None):
    settings = get_settings()
    store = TaskStore(settings)
    store.init_schema()
    worker = Worker(store, settings)

    # Built-in sample handler: echoes the payload back as the result.
    def echo(payload: dict) -> dict:
        return {"echoed": payload}

    worker.register("echo", echo)

    if start_worker is None:
        start_worker = os.environ.get("TASK_QUEUE_WORKER", "1") != "0"
    if start_worker:
        worker.start_background()

    return create_app(store, worker, settings)


app = build_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
