import logging
import os
import unittest

from app_logging import DEFAULT_LOG_FORMAT, configure_logging


class ConfigureLoggingTests(unittest.TestCase):
    def setUp(self):
        self.root_logger = logging.getLogger()
        self.original_handlers = list(self.root_logger.handlers)
        self.original_level = self.root_logger.level
        self.original_env = os.environ.get("APP_LOG_LEVEL")

    def tearDown(self):
        current_handlers = list(self.root_logger.handlers)
        for handler in current_handlers:
            self.root_logger.removeHandler(handler)
            if handler not in self.original_handlers:
                handler.close()

        for handler in self.original_handlers:
            self.root_logger.addHandler(handler)

        self.root_logger.setLevel(self.original_level)

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


if __name__ == "__main__":
    unittest.main()
