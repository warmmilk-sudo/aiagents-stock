import logging
import os
import unittest
from unittest import mock

from app_logging import DEFAULT_LOG_FORMAT, configure_logging, suppress_uvicorn_access_logs


class ConfigureLoggingTests(unittest.TestCase):
    def setUp(self):
        self.root_logger = logging.getLogger()
        self.original_handlers = list(self.root_logger.handlers)
        self.original_level = self.root_logger.level
        self.original_env = os.environ.get("APP_LOG_LEVEL")
        self.original_configured = getattr(self.root_logger, "_app_logging_configured", None)

    def tearDown(self):
        current_handlers = list(self.root_logger.handlers)
        for handler in current_handlers:
            self.root_logger.removeHandler(handler)
            if handler not in self.original_handlers:
                handler.close()

        for handler in self.original_handlers:
            self.root_logger.addHandler(handler)

        self.root_logger.setLevel(self.original_level)
        if self.original_configured is None:
            if hasattr(self.root_logger, "_app_logging_configured"):
                delattr(self.root_logger, "_app_logging_configured")
        else:
            setattr(self.root_logger, "_app_logging_configured", self.original_configured)

        if self.original_env is None:
            os.environ.pop("APP_LOG_LEVEL", None)
        else:
            os.environ["APP_LOG_LEVEL"] = self.original_env

    def test_configure_logging_adds_handler_when_missing(self):
        for handler in list(self.root_logger.handlers):
            self.root_logger.removeHandler(handler)

        os.environ.pop("APP_LOG_LEVEL", None)

        level = configure_logging()

        self.assertEqual(level, logging.INFO)
        self.assertEqual(self.root_logger.level, logging.INFO)
        self.assertEqual(len(self.root_logger.handlers), 1)
        self.assertEqual(self.root_logger.handlers[0].formatter._fmt, DEFAULT_LOG_FORMAT)

    def test_configure_logging_updates_existing_handler_level(self):
        for handler in list(self.root_logger.handlers):
            self.root_logger.removeHandler(handler)

        handler = logging.StreamHandler()
        handler.setLevel(logging.WARNING)
        self.root_logger.addHandler(handler)
        os.environ["APP_LOG_LEVEL"] = "DEBUG"

        level = configure_logging()

        self.assertEqual(level, logging.DEBUG)
        self.assertEqual(self.root_logger.level, logging.DEBUG)
        self.assertEqual(handler.level, logging.DEBUG)
        self.assertEqual(handler.formatter._fmt, DEFAULT_LOG_FORMAT)

    def test_configure_logging_logs_init_once_when_config_unchanged(self):
        for handler in list(self.root_logger.handlers):
            self.root_logger.removeHandler(handler)

        os.environ.pop("APP_LOG_LEVEL", None)
        app_logger = logging.getLogger("app_logging")

        with mock.patch.object(app_logger, "info") as info_mock:
            configure_logging()
            configure_logging()

        self.assertEqual(info_mock.call_count, 1)

    def test_suppress_uvicorn_access_logs_drops_access_records(self):
        access_logger = logging.getLogger("uvicorn.access")
        original_handlers = list(access_logger.handlers)
        original_filters = list(access_logger.filters)
        original_propagate = access_logger.propagate
        original_disabled = access_logger.disabled
        original_level = access_logger.level

        try:
            suppress_uvicorn_access_logs()

            self.assertTrue(access_logger.disabled)
            self.assertFalse(access_logger.propagate)
            self.assertEqual(access_logger.level, logging.CRITICAL + 1)
            self.assertTrue(any(type(existing_filter).__name__ == "_DropAllFilter" for existing_filter in access_logger.filters))
        finally:
            access_logger.handlers[:] = original_handlers
            access_logger.filters[:] = original_filters
            access_logger.propagate = original_propagate
            access_logger.disabled = original_disabled
            access_logger.setLevel(original_level)


if __name__ == "__main__":
    unittest.main()
