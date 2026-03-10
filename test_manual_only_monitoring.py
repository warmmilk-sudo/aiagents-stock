import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.modules.setdefault(
    "streamlit",
    types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        success=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        write=lambda *args, **kwargs: None,
    ),
)
sys.modules.setdefault(
    "stock_data",
    types.SimpleNamespace(
        StockDataFetcher=type(
            "StockDataFetcher",
            (),
            {"get_stock_info": lambda self, *args, **kwargs: {}},
        )
    ),
)

import monitoring_orchestrator
import smart_monitor_engine as smart_monitor_engine_module
from monitor_db import StockMonitorDatabase


class FakeAssetRepository:
    def get_asset(self, asset_id):
        return None

    def get_asset_by_symbol(self, stock_code, account_name):
        return None


class FakeAssetService:
    def promote_to_watchlist(self, symbol, stock_name, account_name, note=""):
        return True, "ok", 101


class FakeAnalysisRepository:
    def get_latest_strategy_context(self, **kwargs):
        return {}


class FakeSmartMonitorDB:
    def __init__(self):
        self.asset_repository = FakeAssetRepository()
        self.asset_service = FakeAssetService()
        self.analysis_repository = FakeAnalysisRepository()
        self.saved_decisions = []
        self.pending_actions = []

    def get_monitor_task_by_code(self, *args, **kwargs):
        return {}

    def save_ai_decision(self, payload):
        self.saved_decisions.append(payload)
        return len(self.saved_decisions)

    def create_pending_action(self, **kwargs):
        self.pending_actions.append(kwargs)
        return len(self.pending_actions)


class ManualOnlyMonitoringTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_price_alert_triggers_only_notifications_without_quant_trade(self):
        temp_monitor_db = StockMonitorDatabase(str(self.base / "monitor.db"))
        stock_id = temp_monitor_db.add_monitored_stock(
            symbol="000001",
            name="平安银行",
            rating="买入",
            entry_range={"min": 10.0, "max": 10.5},
            take_profit=11.0,
            stop_loss=9.8,
            check_interval=30,
            notification_enabled=True,
        )
        stock = temp_monitor_db.get_stock_by_id(stock_id)
        send_calls = []

        with patch.object(monitoring_orchestrator, "monitor_db", temp_monitor_db), patch.object(
            monitoring_orchestrator,
            "SmartMonitorEngine",
            return_value=object(),
        ), patch.object(
            monitoring_orchestrator.notification_service,
            "send_notifications",
            side_effect=lambda: send_calls.append(True),
        ):
            orchestrator = monitoring_orchestrator.MonitoringOrchestrator()
            orchestrator._check_trigger_conditions(stock, 10.2)
            orchestrator._check_trigger_conditions(stock, 11.2)
            orchestrator._check_trigger_conditions(stock, 9.7)

        notification_types = {item["type"] for item in temp_monitor_db.get_pending_notifications()}
        self.assertEqual(notification_types, {"entry", "take_profit", "stop_loss"})
        self.assertNotIn("quant_trade", notification_types)
        self.assertEqual(len(send_calls), 3)

    def test_ai_monitor_buy_signal_creates_pending_action_in_manual_only_mode(self):
        fake_db = FakeSmartMonitorDB()

        with patch.object(smart_monitor_engine_module, "SmartMonitorDB", return_value=fake_db), patch.object(
            smart_monitor_engine_module.event_bus,
            "subscribe",
            return_value=None,
        ):
            engine = smart_monitor_engine_module.SmartMonitorEngine(deepseek_api_key="stub")

        engine.deepseek.get_trading_session = lambda: {
            "session": "上午盘",
            "can_trade": True,
            "recommendation": "",
        }
        engine.data_fetcher.get_comprehensive_data = lambda stock_code: {
            "name": "贵州茅台",
            "current_price": 1520.0,
            "change_pct": 1.25,
            "change_amount": 18.8,
            "volume": 123456,
            "turnover_rate": 0.75,
        }
        engine.deepseek.analyze_stock_and_decide = lambda **kwargs: {
            "success": True,
            "decision": {
                "action": "BUY",
                "confidence": 82,
                "reasoning": "回踩后放量企稳，适合分批吸纳。",
                "position_size_pct": 20,
                "stop_loss_pct": 5,
                "take_profit_pct": 12,
                "risk_level": "中",
                "key_price_levels": {"support": 1500, "resistance": 1560},
            },
        }
        engine._send_notification = lambda **kwargs: None

        result = engine.analyze_stock("600519", notify=False, account_name="测试账户")

        self.assertTrue(result["success"])
        self.assertEqual(result["execution_result"]["mode"], "manual_only")
        self.assertEqual(result["pending_action"]["pending_action_id"], 1)
        self.assertEqual(len(fake_db.pending_actions), 1)
        self.assertEqual(fake_db.pending_actions[0]["action_type"], "buy")
        self.assertEqual(fake_db.saved_decisions[0]["execution_mode"], "manual_only")
        self.assertEqual(fake_db.saved_decisions[0]["action_status"], "pending")


if __name__ == "__main__":
    unittest.main()
