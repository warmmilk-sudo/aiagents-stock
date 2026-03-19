import logging

from backend.app import create_app
from app_logging import suppress_uvicorn_access_logs


# Suppress per-request access logs such as GET /api/system/status 200 OK.
suppress_uvicorn_access_logs()


app = create_app()
