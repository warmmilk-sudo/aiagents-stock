import logging
import os


DEFAULT_LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
_LOGGING_CONFIGURED_ATTR = "_app_logging_configured"
_UVICORN_ACCESS_LOGGER_NAME = "uvicorn.access"


class _DropAllFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return False


def _resolve_log_level(level_name: str) -> int:
    normalized = str(level_name or "").strip().upper()
    return getattr(logging, normalized, logging.INFO)


def configure_logging(default_level: str = "INFO") -> int:
    """Ensure application logs are visible in different process runners."""
    level = _resolve_log_level(os.getenv("APP_LOG_LEVEL", default_level))
    root_logger = logging.getLogger()
    changed = False

    if not root_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        handler.setLevel(level)
        root_logger.addHandler(handler)
        changed = True
    else:
        formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
        for handler in root_logger.handlers:
            if handler.level == logging.NOTSET or handler.level > level:
                handler.setLevel(level)
                changed = True
            if handler.formatter is None:
                handler.setFormatter(formatter)
                changed = True

    if root_logger.level != level:
        changed = True
    root_logger.setLevel(level)

    already_configured = getattr(root_logger, _LOGGING_CONFIGURED_ATTR, False)
    if changed or not already_configured:
        logging.getLogger(__name__).info("应用日志已初始化，级别: %s", logging.getLevelName(level))
        setattr(root_logger, _LOGGING_CONFIGURED_ATTR, True)

    return level


def suppress_uvicorn_access_logs() -> None:
    """Prevent uvicorn access log lines from reaching any configured handler."""
    access_logger = logging.getLogger(_UVICORN_ACCESS_LOGGER_NAME)
    access_logger.handlers.clear()
    access_logger.propagate = False
    access_logger.setLevel(logging.CRITICAL + 1)
    access_logger.disabled = True

    if not any(isinstance(existing_filter, _DropAllFilter) for existing_filter in access_logger.filters):
        access_logger.addFilter(_DropAllFilter())
