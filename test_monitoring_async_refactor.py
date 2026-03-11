import asyncio
import sys
import tempfile
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
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
from monitoring_repository import MonitoringRepository
from smart_monitor_db import SmartMonitorDB


class _FakeRepository:
    def __init__(self, items):
        self.items = [dict(item) for item in items]
        self.runtime_updates = []
        self.item_updates = []
        self.events = []

    def list_items(self, enabled_only=False, **kwargs):
        items = [dict(item) for item in self.items]
        if enabled_only:
            items = [item for item in items if item.get("enabled", True)]
        monitor_type = kwargs.get("monitor_type")
        if monitor_type:
            items = [item for item in items if item.get("monitor_type") == monitor_type]
        return items

    def get_due_items(self, now=None, service_running=True):
        if not service_running:
            return []
        return [dict(item) for item in self.items if item.get("enabled", True)]

    def get_item(self, item_id):
        for item in self.items:
            if item.get("id") == item_id:
                return dict(item)
        return None

    def update_runtime(self, item_id, **kwargs):
        self.runtime_updates.append({"item_id": item_id, **kwargs})
        for item in self.items:
            if item.get("id") == item_id:
                item.update(kwargs)
        return True

    def update_item(self, item_id, updates):
        self.item_updates.append({"item_id": item_id, **dict(updates)})
        for item in self.items:
            if item.get("id") == item_id:
                item.update(updates)
        return True

    def record_event(self, **kwargs):
        self.events.append(dict(kwargs))
        return len(self.events)

    def get_pending_notifications(self):
        return []

    def get_recent_events(self, limit=50):
        return self.events[-limit:]


class _FakeMonitorDB:
    def __init__(self, repo, stocks):
        self.repository = repo
        self._stocks = {stock_id: dict(stock) for stock_id, stock in stocks.items()}
        self.updated_prices = []
        self.last_checked = []
        self.notifications = []

    def get_stock_by_id(self, stock_id):
        stock = self._stocks.get(stock_id)
        return dict(stock) if stock else None

    def update_stock_price(self, stock_id, price):
        self.updated_prices.append((stock_id, price))
        if stock_id in self._stocks:
            self._stocks[stock_id]["current_price"] = price

    def update_last_checked(self, stock_id):
        self.last_checked.append(stock_id)

    def has_recent_notification(self, stock_id, notification_type, minutes=60):
        return False

    def add_notification(self, stock_id, notification_type, message):
        self.notifications.append((stock_id, notification_type, message))


class MonitoringOrchestratorAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_tick_keeps_price_alert_moving_while_ai_tasks_block(self):
        items = [
            {"id": 1, "symbol": "600519", "name": "贵州茅台", "monitor_type": "ai_task", "enabled": 1},
            {"id": 2, "symbol": "000001", "name": "平安银行", "monitor_type": "ai_task", "enabled": 1},
            {"id": 3, "symbol": "300750", "name": "宁德时代", "monitor_type": "price_alert", "enabled": 1},
        ]
        repo = _FakeRepository(items)
        fake_monitor_db = _FakeMonitorDB(
            repo,
            {
                3: {
                    "id": 3,
                    "symbol": "300750",
                    "name": "宁德时代",
                    "entry_range": {"min": 199.0, "max": 201.0},
                    "take_profit": 210.0,
                    "stop_loss": 190.0,
                    "notification_enabled": True,
                    "trading_hours_only": False,
                }
            },
        )
        ai_gate = threading.Event()
        ai_started = []
        ai_lock = threading.Lock()
        price_alert_done = threading.Event()

        class _FakeEngine:
            def analyze_stock(self, **kwargs):
                with ai_lock:
                    ai_started.append(kwargs["stock_code"])
                ai_gate.wait(timeout=1.0)
                return {"success": True, "decision": {"action": "HOLD"}}

        with patch.object(monitoring_orchestrator, "monitor_db", fake_monitor_db), patch.object(
            monitoring_orchestrator,
            "SmartMonitorEngine",
            return_value=_FakeEngine(),
        ), patch.object(monitoring_orchestrator, "TDX_AVAILABLE", False):
            orchestrator = monitoring_orchestrator.MonitoringOrchestrator()
            orchestrator.running = True
            orchestrator._ai_semaphore = asyncio.Semaphore(2)
            orchestrator._price_semaphore = asyncio.Semaphore(1)
            orchestrator._notify_semaphore = asyncio.Semaphore(1)
            orchestrator._background_tasks = set()
            orchestrator._get_latest_price = types.MethodType(lambda self, symbol: asyncio.sleep(0, result=200.5), orchestrator)
            orchestrator._check_trigger_conditions = lambda stock, price: price_alert_done.set()

            await orchestrator._run_tick()
            self.assertTrue(await asyncio.to_thread(price_alert_done.wait, 0.5))

            self.assertEqual(set(ai_started), {"600519", "000001"})
            self.assertEqual(fake_monitor_db.updated_prices, [(3, 200.5)])

            ai_gate.set()
            if orchestrator._background_tasks:
                await asyncio.gather(*list(orchestrator._background_tasks), return_exceptions=True)

    async def test_process_ai_task_timeout_marks_item_timeout(self):
        repo = _FakeRepository(
            [
                {
                    "id": 11,
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "monitor_type": "ai_task",
                    "enabled": 1,
                    "notification_enabled": 1,
                    "trading_hours_only": 0,
                }
            ]
        )
        fake_monitor_db = _FakeMonitorDB(repo, {})

        class _SlowEngine:
            def analyze_stock(self, **kwargs):
                time.sleep(0.2)
                return {"success": True, "decision": {"action": "BUY"}}

        with patch.object(monitoring_orchestrator, "monitor_db", fake_monitor_db), patch.object(
            monitoring_orchestrator,
            "SmartMonitorEngine",
            return_value=_SlowEngine(),
        ), patch.object(monitoring_orchestrator, "TDX_AVAILABLE", False):
            orchestrator = monitoring_orchestrator.MonitoringOrchestrator()

        orchestrator.AI_TASK_TIMEOUT_SECONDS = 0.05

        result = await orchestrator._process_ai_task(repo.items[0])

        self.assertFalse(result)
        self.assertTrue(repo.runtime_updates)
        self.assertEqual(repo.runtime_updates[-1]["last_status"], "timeout")
        self.assertEqual(repo.runtime_updates[-1]["last_message"], "AI分析超时")

    async def test_force_manual_ai_task_ignores_trading_hours_guard(self):
        repo = _FakeRepository(
            [
                {
                    "id": 12,
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "monitor_type": "ai_task",
                    "enabled": 1,
                    "notification_enabled": 1,
                    "trading_hours_only": 1,
                }
            ]
        )
        fake_monitor_db = _FakeMonitorDB(repo, {})
        captured = {}

        class _FakeEngine:
            def analyze_stock(self, **kwargs):
                captured.update(kwargs)
                return {"success": True, "decision": {"action": "HOLD"}}

        with patch.object(monitoring_orchestrator, "monitor_db", fake_monitor_db), patch.object(
            monitoring_orchestrator,
            "SmartMonitorEngine",
            return_value=_FakeEngine(),
        ), patch.object(monitoring_orchestrator, "TDX_AVAILABLE", False):
            orchestrator = monitoring_orchestrator.MonitoringOrchestrator()

        result = await orchestrator._dispatch_item_async(repo.items[0], force=True)

        self.assertTrue(result)
        self.assertIn("trading_hours_only", captured)
        self.assertFalse(captured["trading_hours_only"])

    async def test_get_latest_price_falls_back_after_tdx_timeout(self):
        repo = _FakeRepository([])
        fake_monitor_db = _FakeMonitorDB(repo, {})

        class _FakeEngine:
            def analyze_stock(self, **kwargs):
                return {"success": True, "decision": {"action": "HOLD"}}

        class _SlowTDX:
            def get_realtime_quote(self, symbol):
                time.sleep(0.1)
                return {"current_price": 18.8}

        class _FallbackFetcher:
            def get_stock_info(self, symbol, **kwargs):
                return {"current_price": 12.34}

        with patch.object(monitoring_orchestrator, "monitor_db", fake_monitor_db), patch.object(
            monitoring_orchestrator,
            "SmartMonitorEngine",
            return_value=_FakeEngine(),
        ), patch.object(monitoring_orchestrator, "TDX_AVAILABLE", False):
            orchestrator = monitoring_orchestrator.MonitoringOrchestrator()

        orchestrator.use_tdx = True
        orchestrator.tdx_fetcher = _SlowTDX()
        orchestrator.fetcher = _FallbackFetcher()
        orchestrator.TDX_FETCH_TIMEOUT_SECONDS = 0.02
        orchestrator.PRICE_FETCH_TIMEOUT_SECONDS = 0.2

        price = await orchestrator._get_latest_price("000001")

        self.assertEqual(price, 12.34)

    def test_ensure_started_and_idle_stop_follow_enabled_items(self):
        repo = _FakeRepository(
            [
                {"id": 1, "symbol": "600519", "name": "贵州茅台", "monitor_type": "ai_task", "enabled": 1},
            ]
        )
        fake_monitor_db = _FakeMonitorDB(repo, {})

        with patch.object(monitoring_orchestrator, "monitor_db", fake_monitor_db), patch.object(
            monitoring_orchestrator,
            "SmartMonitorEngine",
            return_value=object(),
        ), patch.object(monitoring_orchestrator, "TDX_AVAILABLE", False):
            orchestrator = monitoring_orchestrator.MonitoringOrchestrator()

        start_calls = []
        stop_calls = []

        def _fake_start():
            start_calls.append(True)
            orchestrator.running = True

        def _fake_stop():
            stop_calls.append(True)
            orchestrator.running = False

        orchestrator.start = _fake_start
        orchestrator.stop = _fake_stop

        orchestrator.ensure_started()
        self.assertEqual(len(start_calls), 1)
        self.assertTrue(orchestrator.running)

        repo.items[0]["enabled"] = 0
        orchestrator.ensure_stopped_if_idle()
        self.assertEqual(len(stop_calls), 1)
        self.assertFalse(orchestrator.running)


class MonitoringWriteSerializationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_concurrent_monitoring_writes_do_not_raise_sqlite_lock_errors(self):
        seed_path = str(self.base / "shared_monitor.db")
        repository = MonitoringRepository(seed_path)
        smart_db = SmartMonitorDB(seed_path)
        item_id = repository.upsert_item(
            {
                "symbol": "600519",
                "name": "贵州茅台",
                "monitor_type": "ai_task",
                "enabled": 1,
                "account_name": "默认账户",
                "config": {"task_name": "并发写测试"},
            }
        )

        errors = []

        def _capture(fn, index):
            try:
                return fn(index)
            except Exception as exc:  # pragma: no cover - failure path asserted below
                errors.append(exc)
                return None

        def _write_runtime(index):
            repository.update_runtime(
                item_id,
                last_status=f"runtime_{index}",
                last_message=f"runtime_message_{index}",
            )

        def _write_event(index):
            repository.record_event(
                item_id=item_id,
                event_type="tick",
                message=f"event_{index}",
                notification_pending=bool(index % 2),
            )

        def _write_decision(index):
            smart_db.save_ai_decision(
                {
                    "stock_code": "600519",
                    "stock_name": "贵州茅台",
                    "account_name": "默认账户",
                    "decision_time": f"2026-03-11 09:{index:02d}:00",
                    "action": "BUY" if index % 2 == 0 else "HOLD",
                    "confidence": 70 + index,
                    "reasoning": f"并发写入测试_{index}",
                    "market_data": {"current_price": 1500.0 + index},
                    "account_info": {"cash": 100000},
                }
            )

        jobs = []
        with ThreadPoolExecutor(max_workers=12) as executor:
            for index in range(15):
                jobs.append(executor.submit(_capture, _write_runtime, index))
                jobs.append(executor.submit(_capture, _write_event, index))
                jobs.append(executor.submit(_capture, _write_decision, index))
            for job in jobs:
                job.result(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(len(repository.get_recent_events(limit=100)), 15)
        self.assertEqual(len(smart_db.get_ai_decisions("600519", limit=100)), 15)


if __name__ == "__main__":
    unittest.main()
