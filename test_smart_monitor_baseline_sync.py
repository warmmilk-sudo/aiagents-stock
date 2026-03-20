import unittest
import sys
import types
from unittest.mock import call, patch

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)
sys.modules.setdefault("yfinance", types.SimpleNamespace())
sys.modules.setdefault("akshare", types.SimpleNamespace())
sys.modules.setdefault("ta", types.SimpleNamespace())
sys.modules.setdefault("pywencai", types.SimpleNamespace())
sys.modules.setdefault("pandas", types.SimpleNamespace())
sys.modules.setdefault("numpy", types.SimpleNamespace())
batch_analysis_stub = types.ModuleType("batch_analysis_service")
batch_analysis_stub.analyze_single_stock_for_batch = lambda *args, **kwargs: {}
sys.modules.setdefault("batch_analysis_service", batch_analysis_stub)

stub_specs = {
    "main_force_pdf_generator": {
        "MainForcePDFGenerator": type("MainForcePDFGenerator", (), {}),
        "generate_main_force_markdown_report": lambda *args, **kwargs: "",
    },
    "main_force_analysis": {
        "MainForceAnalyzer": type("MainForceAnalyzer", (), {}),
    },
    "sector_strategy_pdf": {
        "SectorStrategyPDFGenerator": type("SectorStrategyPDFGenerator", (), {}),
    },
    "sector_strategy_data": {
        "SectorStrategyDataFetcher": type("SectorStrategyDataFetcher", (), {}),
    },
    "sector_strategy_db": {
        "SectorStrategyDatabase": type("SectorStrategyDatabase", (), {}),
    },
    "sector_strategy_engine": {
        "SectorStrategyEngine": type("SectorStrategyEngine", (), {}),
    },
    "sector_strategy_scheduler": {
        "sector_strategy_scheduler": types.SimpleNamespace(),
    },
    "strategy_markdown_reports": {
        "generate_longhubang_markdown_report": lambda *args, **kwargs: "",
        "generate_sector_markdown_report": lambda *args, **kwargs: "",
    },
    "stock_data": {
        "StockDataFetcher": type("StockDataFetcher", (), {}),
    },
    "portfolio_analysis_tasks": {
        "portfolio_analysis_task_manager": types.SimpleNamespace(),
    },
    "portfolio_manager": {
        "portfolio_manager": types.SimpleNamespace(),
    },
    "portfolio_scheduler": {
        "portfolio_scheduler": types.SimpleNamespace(),
    },
    "low_price_bull_monitor": {
        "low_price_bull_monitor": types.SimpleNamespace(),
    },
    "low_price_bull_selector": {
        "LowPriceBullSelector": type("LowPriceBullSelector", (), {}),
    },
    "low_price_bull_service": {
        "low_price_bull_service": types.SimpleNamespace(),
    },
    "low_price_bull_strategy": {
        "LowPriceBullStrategy": type("LowPriceBullStrategy", (), {}),
    },
    "monitor_service": {
        "monitor_service": types.SimpleNamespace(
            get_scheduler=lambda: None,
            get_status=lambda: {},
            get_recent_events=lambda *args, **kwargs: [],
            get_registry_items=lambda *args, **kwargs: [],
        )
    },
    "notification_service": {
        "notification_service": types.SimpleNamespace(),
    },
}
for module_name, attributes in stub_specs.items():
    module = types.ModuleType(module_name)
    for attr_name, attr_value in attributes.items():
        setattr(module, attr_name, attr_value)
    sys.modules.setdefault(module_name, module)

from backend import services


class SmartMonitorBaselineSyncTests(unittest.TestCase):
    def test_price_alert_payload_refreshes_partial_strategy_context(self) -> None:
        asset = {
            "symbol": "300274",
            "name": "阳光电源",
            "status": "watchlist",
            "account_name": "zfy",
            "id": 13,
            "origin_analysis_id": 59,
        }
        existing_item = {
            "config": {
                "entry_range": {"min": 149.0, "max": 151.0},
                "take_profit": 175.0,
                "stop_loss": 143.5,
                "rating": "持有",
            }
        }
        latest_context = {
            "origin_analysis_id": 183,
            "rating": "持有",
            "entry_min": 157.0,
            "entry_max": 163.0,
            "take_profit": None,
            "stop_loss": 158.0,
            "analysis_date": "2026-03-19 10:43:58",
        }

        payload = services.asset_service._build_price_alert_payload(asset, latest_context, existing_item)

        self.assertEqual(payload["config"]["entry_range"], {"min": 157.0, "max": 163.0})
        self.assertIsNone(payload["config"]["take_profit"])
        self.assertEqual(payload["config"]["stop_loss"], 158.0)
        self.assertEqual(payload["config"]["strategy_context"]["origin_analysis_id"], 183)

    def test_monitor_tasks_use_latest_strategy_context_over_cached_config(self) -> None:
        cached_context = {"origin_analysis_id": 10, "entry_min": 149.0, "entry_max": 151.0}
        latest_context = {"origin_analysis_id": 183, "entry_min": 157.0, "entry_max": 163.0}
        tasks = [
            {
                "id": 18,
                "symbol": "300274",
                "name": "阳光电源",
                "monitor_type": "ai_task",
                "enabled": 1,
                "account_name": "zfy",
                "asset_id": 13,
                "portfolio_stock_id": 13,
                "origin_analysis_id": 59,
                "config": {"task_name": "阳光电源盯盘", "strategy_context": cached_context},
            }
        ]
        asset = {"id": 13, "status": "watchlist", "account_name": "zfy", "cost_price": None, "quantity": None}

        with (
            patch.object(services.smart_monitor_db.monitoring_repository, "list_items", return_value=tasks),
            patch.object(services.smart_monitor_db.analysis_repository, "get_latest_strategy_context", return_value=latest_context),
            patch.object(services.smart_monitor_db.asset_repository, "get_asset", return_value=asset),
        ):
            result = services.smart_monitor_db.get_monitor_tasks(enabled_only=False)

        self.assertEqual(result[0]["strategy_context"]["origin_analysis_id"], 183)
        self.assertEqual(result[0]["strategy_context"]["entry_min"], 157.0)
        self.assertEqual(result[0]["origin_analysis_id"], 183)

    def test_sync_deduplicates_asset_ids(self) -> None:
        tasks = [
            {"id": 1, "stock_code": "AAA", "asset_id": 10, "account_name": "默认"},
            {"id": 2, "stock_code": "AAA", "asset_id": 10, "account_name": "默认"},
            {"id": 3, "stock_code": "BBB", "asset_id": 20, "account_name": "默认"},
        ]

        with (
            patch.object(services.smart_monitor_db, "get_monitor_tasks", return_value=tasks),
            patch.object(
                services.asset_service,
                "sync_managed_monitors",
                side_effect=lambda asset_id: {
                    "ai_tasks_upserted": 1,
                    "price_alerts_upserted": 1,
                    "removed": 0,
                    "asset_id": asset_id,
                },
            ) as sync_mock,
        ):
            result = services.sync_smart_monitor_analysis_baselines()

        self.assertEqual(sync_mock.call_args_list, [call(10), call(20)])
        self.assertEqual(result["task_total"], 3)
        self.assertEqual(result["asset_total"], 2)
        self.assertEqual(result["asset_synced"], 2)
        self.assertEqual(result["ai_tasks_upserted"], 2)
        self.assertEqual(result["price_alerts_upserted"], 2)
        self.assertEqual(result["removed"], 0)

    def test_sync_resolves_missing_asset_id_by_symbol(self) -> None:
        tasks = [
            {"id": 1, "stock_code": "AAA", "asset_id": None, "account_name": "默认"},
        ]
        assets = [{"id": 99}]

        with (
            patch.object(services.smart_monitor_db, "get_monitor_tasks", return_value=tasks),
            patch.object(
                services.asset_service.asset_repository,
                "list_assets",
                return_value=assets,
            ) as list_assets_mock,
            patch.object(
                services.asset_service,
                "sync_managed_monitors",
                return_value={
                    "ai_tasks_upserted": 1,
                    "price_alerts_upserted": 1,
                    "removed": 0,
                },
            ) as sync_mock,
        ):
            result = services.sync_smart_monitor_analysis_baselines()

        list_assets_mock.assert_called_once_with(symbol="AAA", account_name="默认", include_deleted=False)
        sync_mock.assert_called_once_with(99)
        self.assertEqual(result["task_total"], 1)
        self.assertEqual(result["resolved_from_symbol"], 1)
        self.assertEqual(result["asset_synced"], 1)


if __name__ == "__main__":
    unittest.main()
