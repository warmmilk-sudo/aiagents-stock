"""
持仓分析同步链路测试
"""
from typing import Dict

import monitor_db as monitor_db_module
import smart_monitor_db as smart_monitor_db_module
from monitor_db import StockMonitorDatabase
from portfolio_manager import PortfolioManager


class DummyPortfolioDB:
    def __init__(self):
        self.stock_map: Dict[str, Dict] = {}

    def get_stock_by_code(self, code: str):
        return self.stock_map.get(code)


class FakeSmartMonitorDB:
    def __init__(self):
        self.tasks_by_code = {}

    def batch_add_or_update_tasks(self, tasks_data):
        added = 0
        updated = 0
        for task in tasks_data:
            code = task.get("stock_code")
            if code in self.tasks_by_code:
                updated += 1
            else:
                added += 1
            self.tasks_by_code[code] = task
        return {
            "added": added,
            "updated": updated,
            "failed": 0,
            "total": added + updated
        }


def _build_analysis_result(code: str, final_decision: Dict, current_price=None):
    stock_info = {"name": "测试股票"}
    if current_price is not None:
        stock_info["current_price"] = current_price
    return {
        "success": True,
        "results": [
            {
                "code": code,
                "result": {
                    "success": True,
                    "stock_info": stock_info,
                    "final_decision": final_decision
                }
            }
        ]
    }


def test_sync_analysis_standard_levels(tmp_path, monkeypatch):
    manager = PortfolioManager(model="test")
    manager.db = DummyPortfolioDB()
    manager.db.stock_map["600519"] = {"id": 1, "name": "贵州茅台", "auto_monitor": 1}

    realtime_db = StockMonitorDatabase(db_path=str(tmp_path / "stock_monitor_test.db"))
    fake_smart_db = FakeSmartMonitorDB()
    monkeypatch.setattr(monitor_db_module, "monitor_db", realtime_db)
    monkeypatch.setattr(smart_monitor_db_module, "SmartMonitorDB", lambda: fake_smart_db)

    result = _build_analysis_result(
        code="600519.SH",
        final_decision={
            "rating": "买入",
            "entry_range": "1580-1620",
            "take_profit": "1700",
            "stop_loss": "1520"
        },
        current_price=1600
    )
    sync_result = manager.sync_analysis_to_monitors(result)

    assert sync_result["realtime_sync"]["total"] == 1
    assert sync_result["smart_sync"]["total"] == 1

    monitor_item = realtime_db.get_monitor_by_code("600519")
    assert monitor_item is not None
    assert monitor_item["entry_range"]["min"] == 1580.0
    assert monitor_item["entry_range"]["max"] == 1620.0
    assert "needs_review" not in monitor_item["entry_range"]

    task_item = fake_smart_db.tasks_by_code.get("600519")
    assert task_item is not None
    assert task_item["enabled"] == 0
    assert task_item["auto_trade"] == 0
    assert "[待确认]" not in task_item["task_name"]


def test_sync_analysis_fallback_levels_with_review_flag(tmp_path, monkeypatch):
    manager = PortfolioManager(model="test")
    manager.db = DummyPortfolioDB()
    manager.db.stock_map["000001"] = {"id": 2, "name": "平安银行", "auto_monitor": 1}

    realtime_db = StockMonitorDatabase(db_path=str(tmp_path / "stock_monitor_test2.db"))
    fake_smart_db = FakeSmartMonitorDB()
    monkeypatch.setattr(monitor_db_module, "monitor_db", realtime_db)
    monkeypatch.setattr(smart_monitor_db_module, "SmartMonitorDB", lambda: fake_smart_db)

    result = _build_analysis_result(
        code="000001",
        final_decision={
            "rating": "持有",
            "entry_range": "当前不建议进场",
            "take_profit": "N/A",
            "stop_loss": "N/A"
        },
        current_price=10.0
    )
    sync_result = manager.sync_analysis_to_monitors(result)

    assert sync_result["realtime_sync"]["total"] == 1
    monitor_item = realtime_db.get_monitor_by_code("000001")
    assert monitor_item is not None
    assert monitor_item["entry_range"]["needs_review"] is True
    assert monitor_item["entry_range"]["min"] == 9.8
    assert monitor_item["entry_range"]["max"] == 10.2

    task_item = fake_smart_db.tasks_by_code.get("000001")
    assert task_item is not None
    assert "[待确认]" in task_item["task_name"]
    assert task_item["enabled"] == 0
    assert task_item["auto_trade"] == 0


def test_sync_analysis_skip_when_no_current_price(tmp_path, monkeypatch):
    manager = PortfolioManager(model="test")
    manager.db = DummyPortfolioDB()
    manager.db.stock_map["300001"] = {"id": 3, "name": "特锐德", "auto_monitor": 1}

    realtime_db = StockMonitorDatabase(db_path=str(tmp_path / "stock_monitor_test3.db"))
    fake_smart_db = FakeSmartMonitorDB()
    monkeypatch.setattr(monitor_db_module, "monitor_db", realtime_db)
    monkeypatch.setattr(smart_monitor_db_module, "SmartMonitorDB", lambda: fake_smart_db)

    result = _build_analysis_result(
        code="300001",
        final_decision={
            "rating": "持有",
            "entry_range": "不适用",
            "take_profit": "N/A",
            "stop_loss": "N/A"
        },
        current_price=None
    )
    sync_result = manager.sync_analysis_to_monitors(result)

    assert sync_result["skipped"] == 1
    assert sync_result["realtime_sync"]["total"] == 0
    assert sync_result["smart_sync"]["total"] == 0
    assert len(sync_result["failed_reasons"]) == 1
    assert "current_price" in sync_result["failed_reasons"][0]["reason"]
    assert realtime_db.get_monitor_by_code("300001") is None
