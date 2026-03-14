import logging

from backend.app import create_app


# Suppress per-request access logs such as GET /api/system/status 200 OK.
access_logger = logging.getLogger("uvicorn.access")
access_logger.handlers.clear()
access_logger.propagate = False
access_logger.disabled = True


app = create_app()
