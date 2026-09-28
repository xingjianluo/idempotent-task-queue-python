import logging

import uvicorn

from app.api import create_app

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
