import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

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
        self.monitoring_repository = types.SimpleNamespace(
            get_item_by_symbol=lambda *args, **kwargs: None,
            record_event=lambda *args, **kwargs: 1,
        )
        self.saved_decisions = []
        self.pending_actions = []

    def get_monitor_task_by_code(self, *args, **kwargs):
        return {}

    def save_ai_decision(self, payload):
        self.saved_decisions.append(payload)
        return len(self.saved_decisions)

    def save_ai_decision_if_changed(self, payload):
        return self.save_ai_decision(payload), True

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
        ):
            orchestrator = monitoring_orchestrator.MonitoringOrchestrator()
            orchestrator._check_trigger_conditions(stock, 10.2)
            orchestrator._check_trigger_conditions(stock, 11.2)
            orchestrator._check_trigger_conditions(stock, 9.7)

        notification_types = {item["type"] for item in temp_monitor_db.get_pending_notifications()}
        self.assertEqual(notification_types, {"entry", "take_profit", "stop_loss"})
        self.assertNotIn("quant_trade", notification_types)
        self.assertEqual(len(send_calls), 0)

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
                "monitor_levels": {
                    "entry_min": 1505,
                    "entry_max": 1515,
                    "take_profit": 1702.4,
                    "stop_loss": 1444.0,
                },
            },
        }
        engine._send_notification = lambda **kwargs: None
        engine._sync_runtime_thresholds = lambda **kwargs: True

        result = engine.analyze_stock("600519", notify=False, account_name="测试账户")

        self.assertTrue(result["success"])
        self.assertEqual(result["execution_result"]["mode"], "manual_only")
        self.assertEqual(result["pending_action"]["pending_action_id"], 1)
        self.assertEqual(len(fake_db.pending_actions), 1)
        self.assertEqual(fake_db.pending_actions[0]["action_type"], "buy")
        self.assertEqual(fake_db.saved_decisions[0]["execution_mode"], "manual_only")
        self.assertEqual(fake_db.saved_decisions[0]["action_status"], "pending")

    def test_same_action_as_latest_decision_does_not_create_new_message(self):
        fake_db = FakeSmartMonitorDB()
        notifications = []
        fake_db.save_ai_decision_if_changed = lambda payload: (7, False)

        with patch.object(smart_monitor_engine_module, "SmartMonitorDB", return_value=fake_db), patch.object(
            smart_monitor_engine_module.event_bus,
            "subscribe",
            return_value=None,
        ):
            engine = smart_monitor_engine_module.SmartMonitorEngine(deepseek_api_key="stub")

        engine.deepseek.get_trading_session = lambda: {
            "session": "涓婂崍鐩?",
            "can_trade": True,
            "recommendation": "",
        }
        engine.data_fetcher.get_comprehensive_data = lambda stock_code: {
            "name": "璐靛窞鑼呭彴",
            "current_price": 1520.0,
            "change_pct": 1.25,
            "change_amount": 18.8,
            "volume": 123456,
            "turnover_rate": 0.75,
        }
        engine.deepseek.analyze_stock_and_decide = lambda **kwargs: {
            "success": True,
            "decision": {
                "action": "SELL",
                "confidence": 80,
                "reasoning": "鍚屼竴鍐崇瓥涓嶉噸澶嶅啓鍏ユ柊娑堟伅銆?",
                "position_size_pct": 20,
                "stop_loss_pct": 5,
                "take_profit_pct": 12,
                "risk_level": "涓?",
                "key_price_levels": {"support": 1500, "resistance": 1560},
                "monitor_levels": {
                    "entry_min": 1505,
                    "entry_max": 1515,
                    "take_profit": 1702.4,
                    "stop_loss": 1444.0,
                },
            },
        }
        engine._send_notification = lambda **kwargs: notifications.append(kwargs)
        engine._sync_runtime_thresholds = lambda **kwargs: True

        result = engine.analyze_stock("600519", notify=True, account_name="娴嬭瘯璐︽埛")

        self.assertTrue(result["success"])
        self.assertFalse(result["decision_changed"])
        self.assertIsNone(result["execution_result"])
        self.assertIsNone(result["pending_action"])
        self.assertEqual(result["decision_id"], 7)
        self.assertEqual(len(fake_db.pending_actions), 0)
        self.assertEqual(notifications, [])

    def test_intraday_analysis_refreshes_latest_strategy_context_before_decision(self):
        fake_db = FakeSmartMonitorDB()
        latest_strategy_context = {
            "origin_analysis_id": 99,
            "analysis_scope": "research",
            "analysis_date": "2026-03-13 10:30:00",
            "rating": "买入",
            "entry_min": 1500.0,
            "entry_max": 1510.0,
            "take_profit": 1650.0,
            "stop_loss": 1460.0,
            "summary": "最新深度分析基线",
        }
        fake_db.analysis_repository.get_latest_strategy_context = lambda **kwargs: latest_strategy_context

        captured_strategy_context = {}

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
        engine.data_fetcher.get_comprehensive_data = lambda stock_code, intraday_strict=False: {
            "name": "贵州茅台",
            "current_price": 1520.0,
            "change_pct": 1.25,
            "change_amount": 18.8,
            "volume": 123456,
            "turnover_rate": 0.75,
        }

        def _fake_ai_decision(**kwargs):
            captured_strategy_context.update(kwargs.get("strategy_context") or {})
            return {
                "success": True,
                "decision": {
                    "action": "HOLD",
                    "confidence": 82,
                    "reasoning": "按最新基线继续观察。",
                    "position_size_pct": 20,
                    "stop_loss_pct": 5,
                    "take_profit_pct": 12,
                    "risk_level": "中",
                    "key_price_levels": {"support": 1500, "resistance": 1560},
                    "monitor_levels": {
                        "entry_min": 1500,
                        "entry_max": 1510,
                        "take_profit": 1650,
                        "stop_loss": 1460,
                    },
                },
            }

        engine.deepseek.analyze_stock_and_decide = _fake_ai_decision
        engine._send_notification = lambda **kwargs: None
        engine._sync_runtime_thresholds = lambda **kwargs: True

        result = engine.analyze_stock(
            "600519",
            notify=False,
            account_name="测试账户",
            strategy_context={
                "origin_analysis_id": 1,
                "analysis_scope": "portfolio",
                "analysis_date": "2026-03-12 09:00:00",
                "summary": "旧持仓分析基线",
            },
        )

        self.assertTrue(result["success"])
        self.assertEqual(captured_strategy_context["origin_analysis_id"], 99)
        self.assertEqual(captured_strategy_context["analysis_date"], "2026-03-13 10:30:00")
        self.assertEqual(result["strategy_context"]["origin_analysis_id"], 99)


if __name__ == "__main__":
    unittest.main()
