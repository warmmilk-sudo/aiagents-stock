import sys
import types
import unittest
from unittest.mock import patch

sys.modules.setdefault(
    "smart_monitor_data",
    types.SimpleNamespace(
        SmartMonitorDataFetcher=type(
            "SmartMonitorDataFetcher",
            (),
            {"__init__": lambda self, *args, **kwargs: None},
        )
    ),
)
sys.modules.setdefault(
    "smart_monitor_deepseek",
    types.SimpleNamespace(
        SmartMonitorDeepSeek=type(
            "SmartMonitorDeepSeek",
            (),
            {
                "__init__": lambda self, *args, **kwargs: None,
                "http_timeout_seconds": 15,
            },
        )
    ),
)

import smart_monitor_engine as smart_monitor_engine_module


class SmartMonitorEngineTimeoutTests(unittest.TestCase):
    def test_engine_uses_configured_data_fetch_timeout(self):
        with patch.object(
            smart_monitor_engine_module.config_manager,
            "read_env",
            return_value={"DEEPSEEK_API_KEY": "stub"},
        ), patch.object(
            smart_monitor_engine_module.config,
            "SMART_MONITOR_DATA_FETCH_TIMEOUT_SECONDS",
            52,
        ), patch.object(
            smart_monitor_engine_module.config,
            "TDX_TIMEOUT_SECONDS",
            12,
        ), patch.object(
            smart_monitor_engine_module,
            "SmartMonitorDB",
            return_value=object(),
        ), patch.object(
            smart_monitor_engine_module,
            "notification_service",
            object(),
        ), patch.object(
            smart_monitor_engine_module,
            "investment_lifecycle_service",
            object(),
        ), patch.object(
            smart_monitor_engine_module.event_bus,
            "subscribe",
            return_value=None,
        ):
            engine = smart_monitor_engine_module.SmartMonitorEngine()

        self.assertEqual(engine.data_fetch_timeout_seconds, 52)
