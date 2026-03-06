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
        self.analysis_map: Dict[int, Dict] = {}

    def get_stock_by_code(self, code: str):
        return self.stock_map.get(code)

    def get_stock(self, stock_id: int):
        for stock in self.stock_map.values():
            if stock.get("id") == stock_id:
                return stock
        return None

    def get_all_stocks(self):
        return list(self.stock_map.values())

    def get_latest_analysis(self, stock_id: int):
        return self.analysis_map.get(stock_id)

    def update_stock(self, stock_id: int, **kwargs):
        stock = self.get_stock(stock_id)
        if not stock:
            return False
        stock.update(kwargs)
        return True

    def delete_stock(self, stock_id: int):
        target_code = None
        for code, stock in self.stock_map.items():
            if stock.get("id") == stock_id:
                target_code = code
                break
        if not target_code:
            return False
        del self.stock_map[target_code]
        self.analysis_map.pop(stock_id, None)
        return True


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

    def get_monitor_task_by_stock_code(self, stock_code: str):
        return self.tasks_by_code.get(stock_code)

    def update_monitor_task(self, stock_code: str, task_data: Dict):
        existing = self.tasks_by_code.get(stock_code, {})
        merged = dict(existing)
        merged.update(task_data)
        self.tasks_by_code[stock_code] = merged

    def delete_monitor_task_by_stock_code(self, stock_code: str) -> bool:
        if stock_code in self.tasks_by_code:
            del self.tasks_by_code[stock_code]
            return True
        return False

    def get_monitor_tasks(self, enabled_only: bool = False):
        tasks = list(self.tasks_by_code.values())
        if not enabled_only:
            return tasks
        return [t for t in tasks if t.get("enabled", 0) == 1]


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
    assert "current_price/cost_price" in sync_result["failed_reasons"][0]["reason"]
    assert realtime_db.get_monitor_by_code("300001") is None


def test_sync_portfolio_stock_realtime_with_position_fields(tmp_path, monkeypatch):
    manager = PortfolioManager(model="test")
    manager.db = DummyPortfolioDB()
    manager.db.stock_map["600000"] = {
        "id": 10,
        "code": "600000",
        "name": "浦发银行",
        "cost_price": 12.5,
        "quantity": 1000,
        "auto_monitor": 1,
    }
    manager.db.analysis_map[10] = {
        "rating": "持有",
        "current_price": 12.0,
        "entry_min": 11.5,
        "entry_max": 12.2,
        "take_profit": 13.2,
        "stop_loss": 11.2,
    }

    realtime_db = StockMonitorDatabase(db_path=str(tmp_path / "stock_monitor_test4.db"))
    fake_smart_db = FakeSmartMonitorDB()
    monkeypatch.setattr(monitor_db_module, "monitor_db", realtime_db)
    monkeypatch.setattr(smart_monitor_db_module, "SmartMonitorDB", lambda: fake_smart_db)

    sync_item = manager.sync_portfolio_stock_realtime(10)
    assert sync_item["success"] is True
    assert sync_item["realtime_sync"]["total"] == 1
    assert sync_item["smart_sync"]["total"] == 1

    monitor_item = realtime_db.get_monitor_by_code("600000")
    assert monitor_item is not None
    assert monitor_item["source_type"] == "portfolio"
    assert monitor_item["source_label"] == "持仓"
    assert monitor_item["portfolio_stock_id"] == 10
    assert monitor_item["has_position"] is True
    assert monitor_item["position_cost"] == 12.5
    assert monitor_item["position_quantity"] == 1000

    task_item = fake_smart_db.tasks_by_code.get("600000")
    assert task_item is not None
    assert task_item["source_type"] == "portfolio"
    assert task_item["source_label"] == "持仓"
    assert task_item["portfolio_stock_id"] == 10
    assert task_item["has_position"] == 1
    assert task_item["position_cost"] == 12.5
    assert task_item["position_quantity"] == 1000


def test_delete_stock_by_code_cascade_delete_downstream(tmp_path, monkeypatch):
    manager = PortfolioManager(model="test")
    manager.db = DummyPortfolioDB()
    manager.db.stock_map["600000"] = {
        "id": 11,
        "code": "600000",
        "name": "浦发银行",
        "cost_price": 12.5,
        "quantity": 1000,
        "auto_monitor": 1,
    }
    manager.db.analysis_map[11] = {
        "rating": "买入",
        "current_price": 12.6,
        "entry_min": 12.2,
        "entry_max": 12.8,
        "take_profit": 13.8,
        "stop_loss": 11.8,
    }

    realtime_db = StockMonitorDatabase(db_path=str(tmp_path / "stock_monitor_test5.db"))
    fake_smart_db = FakeSmartMonitorDB()
    monkeypatch.setattr(monitor_db_module, "monitor_db", realtime_db)
    monkeypatch.setattr(smart_monitor_db_module, "SmartMonitorDB", lambda: fake_smart_db)

    sync_item = manager.sync_portfolio_stock_realtime(11)
    assert sync_item["success"] is True
    assert realtime_db.get_monitor_by_code("600000") is not None
    assert fake_smart_db.get_monitor_task_by_stock_code("600000") is not None

    success, _ = manager.delete_stock_by_code("600000")
    assert success is True
    assert manager.db.get_stock_by_code("600000") is None
    assert realtime_db.get_monitor_by_code("600000") is None
    assert fake_smart_db.get_monitor_task_by_stock_code("600000") is None


def test_reconcile_marks_non_portfolio_entries_as_watch(tmp_path, monkeypatch):
    manager = PortfolioManager(model="test")
    manager.db = DummyPortfolioDB()
    manager.db.stock_map["300001"] = {
        "id": 21,
        "code": "300001",
        "name": "特锐德",
        "cost_price": 20.0,
        "quantity": 500,
        "auto_monitor": 1,
    }

    realtime_db = StockMonitorDatabase(db_path=str(tmp_path / "stock_monitor_test6.db"))
    fake_smart_db = FakeSmartMonitorDB()
    monkeypatch.setattr(monitor_db_module, "monitor_db", realtime_db)
    monkeypatch.setattr(smart_monitor_db_module, "SmartMonitorDB", lambda: fake_smart_db)

    # 非持仓来源的历史脏数据（来源误标为portfolio）
    realtime_db.add_monitored_stock(
        symbol="601111",
        name="中国国航",
        rating="持有",
        entry_range={"min": 7.0, "max": 7.4},
        take_profit=8.0,
        stop_loss=6.5,
        source_type="portfolio",
        source_label="持仓",
        portfolio_stock_id=999
    )
    fake_smart_db.tasks_by_code["601111"] = {
        "stock_code": "601111",
        "task_name": "中国国航盯盘",
        "enabled": 0,
        "source_type": "portfolio",
        "source_label": "持仓",
        "portfolio_stock_id": 999,
    }

    result = manager.reconcile_portfolio_sync_on_startup()
    assert result["success"] is True
    assert result["portfolio_synced"] == 1

    monitor_item = realtime_db.get_monitor_by_code("601111")
    assert monitor_item is not None
    assert monitor_item["source_type"] == "watch"
    assert monitor_item["source_label"] == "关注"
    assert monitor_item["portfolio_stock_id"] is None

    task_item = fake_smart_db.get_monitor_task_by_stock_code("601111")
    assert task_item is not None
    assert task_item["source_type"] == "watch"
    assert task_item["source_label"] == "关注"
    assert task_item["portfolio_stock_id"] is None
