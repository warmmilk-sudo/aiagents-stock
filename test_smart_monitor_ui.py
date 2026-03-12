import sys
import types
import unittest
from unittest.mock import patch


class _DummyContext:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _DummyStreamlit(types.ModuleType):
    def __init__(self):
        super().__init__("streamlit")
        self.session_state = {}

    def __getattr__(self, name):
        if name == "fragment":
            def _fragment(*args, **kwargs):
                def _decorator(func):
                    return func
                return _decorator
            return _fragment
        if name in {"container", "expander", "form", "spinner"}:
            return lambda *args, **kwargs: _DummyContext()
        if name == "columns":
            return lambda spec, *args, **kwargs: [
                _DummyContext() for _ in range(spec if isinstance(spec, int) else len(spec))
            ]
        if name == "tabs":
            return lambda labels, *args, **kwargs: [_DummyContext() for _ in labels]
        if name in {"button", "checkbox", "form_submit_button"}:
            return lambda *args, **kwargs: False
        if name == "toggle":
            return lambda *args, **kwargs: False
        if name in {"selectbox", "radio", "text_input", "text_area", "date_input"}:
            return lambda *args, **kwargs: ""
        if name in {"number_input", "slider"}:
            return lambda *args, **kwargs: 0
        return lambda *args, **kwargs: None


class _FakeMonitorService:
    def ensure_started(self):
        return None

    def ensure_stopped_if_idle(self):
        return None


sys.modules.setdefault("streamlit", _DummyStreamlit())

_dummy_dotenv = types.ModuleType("dotenv")
_dummy_dotenv.load_dotenv = lambda *args, **kwargs: None
sys.modules.setdefault("dotenv", _dummy_dotenv)

pandas_module = types.ModuleType("pandas")
pandas_module.DataFrame = lambda *args, **kwargs: []
sys.modules.setdefault("pandas", pandas_module)

monitor_service_module = types.ModuleType("monitor_service")
monitor_service_module.monitor_service = _FakeMonitorService()
sys.modules.setdefault("monitor_service", monitor_service_module)

smart_monitor_engine_module = types.ModuleType("smart_monitor_engine")
smart_monitor_engine_module.SmartMonitorEngine = type("SmartMonitorEngine", (), {})
sys.modules.setdefault("smart_monitor_engine", smart_monitor_engine_module)

smart_monitor_db_module = types.ModuleType("smart_monitor_db")
smart_monitor_db_module.SmartMonitorDB = type("SmartMonitorDB", (), {})
sys.modules.setdefault("smart_monitor_db", smart_monitor_db_module)

config_manager_module = types.ModuleType("config_manager")
config_manager_module.config_manager = object()
sys.modules.setdefault("config_manager", config_manager_module)

portfolio_manager_module = types.ModuleType("portfolio_manager")
portfolio_manager_module.portfolio_manager = object()
sys.modules.setdefault("portfolio_manager", portfolio_manager_module)

ui_analysis_task_utils_module = types.ModuleType("ui_analysis_task_utils")
ui_analysis_task_utils_module.consume_finished_ui_analysis_task = lambda *args, **kwargs: None
ui_analysis_task_utils_module.get_active_ui_analysis_task = lambda *args, **kwargs: None
ui_analysis_task_utils_module.get_ui_analysis_button_state = lambda *args, **kwargs: ("立即执行", False, "")
ui_analysis_task_utils_module.render_ui_analysis_task_live_card = lambda *args, **kwargs: None
ui_analysis_task_utils_module.start_ui_analysis_task = lambda *args, **kwargs: None
sys.modules.setdefault("ui_analysis_task_utils", ui_analysis_task_utils_module)

ui_state_keys_module = types.ModuleType("ui_state_keys")
ui_state_keys_module.INVESTMENT_AI_TASK_PREFILL_KEY = "investment_ai_task_prefill"
ui_state_keys_module.INVESTMENT_WORKSPACE_ACTIVE_TAB_KEY = "investment_workspace_active_tab"
ui_state_keys_module.INVESTMENT_PRICE_ALERT_PREFILL_KEY = "investment_price_alert_prefill"
ui_state_keys_module.PORTFOLIO_ADD_ACCOUNT_NAME_KEY = "portfolio_add_account_name"
ui_state_keys_module.PORTFOLIO_ADD_ORIGIN_ANALYSIS_ID_KEY = "portfolio_add_origin_analysis_id"
ui_state_keys_module.SMART_MONITOR_ACTIVE_TAB_KEY = "smart_monitor_active_tab"
ui_state_keys_module.SMART_MONITOR_DB_KEY = "smart_monitor_db"
ui_state_keys_module.SMART_MONITOR_ENGINE_KEY = "smart_monitor_engine"
sys.modules.setdefault("ui_state_keys", ui_state_keys_module)

ui_shared_module = types.ModuleType("ui_shared")
ui_shared_module.A_SHARE_DOWN_COLOR = "#00ff00"
ui_shared_module.A_SHARE_UP_COLOR = "#ff0000"
ui_shared_module.NON_MARKET_PALETTE = {"gray": "#999999"}
ui_shared_module.format_price = lambda value, *args, **kwargs: value
ui_shared_module.get_dataframe_height = lambda *args, **kwargs: 300
ui_shared_module.get_action_color = lambda *args, **kwargs: "#ffffff"
ui_shared_module.get_market_color = lambda *args, **kwargs: "#ffffff"
ui_shared_module.render_a_share_change_metric = lambda *args, **kwargs: None
sys.modules.setdefault("ui_shared", ui_shared_module)

import smart_monitor_ui
from ui_state_keys import SMART_MONITOR_DB_KEY


class _FakeMonitoringRepository:
    def __init__(self, notifications=None):
        self._notifications = notifications or []

    def get_all_recent_notifications(self, limit=10):
        return list(self._notifications)

    def get_recent_events(self, limit=20):
        raise AssertionError("render_history should not query AI_ANALYSIS events anymore")


class _FakeDB:
    def __init__(self, decisions=None, notifications=None):
        self.monitoring_repository = _FakeMonitoringRepository(notifications=notifications)
        self._decisions = decisions or []

    def get_ai_decisions(self, limit=30):
        return list(self._decisions)


class SmartMonitorUIHistorySplitTests(unittest.TestCase):
    def setUp(self):
        smart_monitor_ui.st.session_state.clear()

    def test_select_latest_notification_events_keeps_latest_per_stock(self):
        notifications = [
            {"id": 5, "account_name": "默认账户", "symbol": "600519", "event_type": "sell", "message": "最新卖出信号", "is_read": False},
            {"id": 4, "account_name": "默认账户", "symbol": "600519", "event_type": "buy", "message": "旧买入信号", "is_read": True},
            {"id": 3, "account_name": "默认账户", "symbol": "000001", "event_type": "entry", "message": "平安银行进入区间", "is_read": True},
        ]

        latest = smart_monitor_ui._select_latest_notification_events(notifications)

        self.assertEqual([event["id"] for event in latest], [5, 3])
        self.assertEqual(latest[0]["message"], "最新卖出信号")
        self.assertFalse(latest[0]["is_read"])

    def test_ai_analysis_event_match_is_case_insensitive(self):
        self.assertTrue(
            smart_monitor_ui._is_ai_decision_history_event({"event_type": " AI_ANALYSIS "})
        )
        self.assertFalse(
            smart_monitor_ui._is_ai_decision_history_event({"event_type": "threshold_sync"})
        )

    def test_reasoning_brief_keeps_only_short_summary(self):
        reasoning = (
            "当前为盘后时段，无法进行交易，需等待次日开盘操作。"
            "当前未触发止盈或止损条件，且无明确信号，建议继续持有。"
        )
        self.assertEqual(
            smart_monitor_ui._format_decision_reasoning_brief(reasoning, max_length=22),
            "未触发止盈或止损条件，建议继续持有",
        )

    def test_monitor_event_message_uses_reason_without_repeating_symbol(self):
        event = {
            "event_type": "entry",
            "symbol": "002230",
            "name": "科大讯飞",
            "message": "股票 002230 (科大讯飞) 价格 53.08 进入进场区间 [51.2-55.0]",
        }
        self.assertEqual(
            smart_monitor_ui._format_monitor_event_message(event),
            "原因：价格 53.08 进入进场区间 [51.2-55.0]",
        )

    def test_monitor_event_message_supports_notification_type_fallback(self):
        event = {
            "type": "entry",
            "symbol": "002230",
            "name": "科大讯飞",
            "message": "股票 002230 (科大讯飞) 价格 53.08 进入进场区间 [51.2-55.0]",
        }
        self.assertEqual(
            smart_monitor_ui._format_monitor_event_message(event),
            "原因：价格 53.08 进入进场区间 [51.2-55.0]",
        )

    def test_build_watchlist_threshold_lines_uses_latest_intraday_decision(self):
        task = {
            "strategy_context": {
                "entry_min": 10.1,
                "entry_max": 10.6,
                "take_profit": 11.3,
                "stop_loss": 9.7,
                "analysis_scope": "portfolio",
            }
        }
        alert_item = {
            "config": {
                "runtime_thresholds": {
                    "entry_min": 10.2,
                    "entry_max": 10.7,
                    "take_profit": 11.5,
                    "stop_loss": 9.6,
                },
                "threshold_source": "ai_runtime",
            }
        }
        latest_decision = {
            "monitor_levels": {
                "entry_min": 10.3,
                "entry_max": 10.8,
                "take_profit": 11.7,
                "stop_loss": 9.5,
            }
        }

        primary_line, secondary_line = smart_monitor_ui._build_watchlist_threshold_lines(
            task,
            alert_item,
            latest_decision,
        )

        self.assertEqual(primary_line, "分析基线: 进场 10.1 - 10.6 | 止盈 11.3 | 止损 9.7")
        self.assertEqual(secondary_line, "盘中分析: 进场 10.3 - 10.8 | 止盈 11.7 | 止损 9.5")

    def test_build_watchlist_threshold_lines_falls_back_to_intraday_levels_only(self):
        task = {"strategy_context": {}}
        alert_item = {
            "config": {
                "runtime_thresholds": {
                    "entry_min": 51.2,
                    "entry_max": 55.0,
                    "take_profit": 59.0,
                    "stop_loss": 48.5,
                },
                "threshold_source": "ai_runtime",
            }
        }

        primary_line, secondary_line = smart_monitor_ui._build_watchlist_threshold_lines(task, alert_item, None)

        self.assertEqual(primary_line, "盘中分析: 进场 51.2 - 55.0 | 止盈 59.0 | 止损 48.5")
        self.assertIsNone(secondary_line)

    def test_sync_watchlist_toggle_session_state_updates_bulk_and_item_keys(self):
        tasks = [
            {"id": 1, "enabled": True},
            {"id": 2, "enabled": False},
        ]

        smart_monitor_ui._sync_watchlist_toggle_session_state(tasks)

        self.assertFalse(smart_monitor_ui.st.session_state["smart_monitor_all_ai_tasks_toggle"])
        self.assertTrue(smart_monitor_ui.st.session_state["smart_monitor_task_enabled_toggle_1"])
        self.assertFalse(smart_monitor_ui.st.session_state["smart_monitor_task_enabled_toggle_2"])

    def test_render_history_only_renders_intraday_decisions(self):
        fake_db = _FakeDB(
            decisions=[{"stock_code": "600519", "stock_name": "贵州茅台", "action": "HOLD", "decision_time": smart_monitor_ui.local_now().strftime("%Y-%m-%d %H:%M:%S")}],
            notifications=[],
        )
        smart_monitor_ui.st.session_state[SMART_MONITOR_DB_KEY] = fake_db
        monitor_service_module.monitor_service = _FakeMonitorService()

        rendered_decisions = []
        rendered_events = []
        with patch.object(
            smart_monitor_ui,
            "_render_ai_decision_notice",
            side_effect=lambda decision: rendered_decisions.append(decision["stock_code"]),
        ), patch.object(
            smart_monitor_ui,
            "_render_monitor_event_notice",
            side_effect=lambda event, **kwargs: rendered_events.append(event),
        ):
            smart_monitor_ui.render_history(show_header=False, title="决策事件")

        self.assertEqual(rendered_decisions, ["600519"])
        self.assertEqual(rendered_events, [])


if __name__ == "__main__":
    unittest.main()
