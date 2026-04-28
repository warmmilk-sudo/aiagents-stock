import json
import unittest

import config
from investment_action_utils import normalize_strategy_context
from monitor_memory_backfill_service import MonitorRelatedExecutionMemoryBackfillService
from smart_monitor_deepseek import SmartMonitorDeepSeek


class FakeAssetStore:
    def __init__(self, assets):
        self.assets = assets

    def list_assets(self, *, status=None, include_deleted=False, **_kwargs):
        return [item for item in self.assets if item.get("status") == status]


class FakeMonitoringStore:
    def __init__(self, items):
        self.items = items
        self.updated = []

    def list_items(
        self,
        monitor_type=None,
        managed_by_portfolio=None,
        enabled_only=False,
        symbol=None,
        **_kwargs,
    ):
        result = self.items
        if monitor_type:
            result = [item for item in result if item.get("monitor_type") == monitor_type]
        if managed_by_portfolio is not None:
            result = [item for item in result if bool(item.get("managed_by_portfolio")) == bool(managed_by_portfolio)]
        if enabled_only:
            result = [item for item in result if bool(item.get("enabled", True))]
        if symbol:
            result = [item for item in result if item.get("symbol") == symbol]
        return [dict(item) for item in result]

    def update_item(self, item_id, updates):
        self.updated.append((item_id, updates))
        for item in self.items:
            if item["id"] == item_id:
                item.update(updates)
                return True
        return False


class FakeAnalysisStore:
    def __init__(self, records):
        self.records = {int(record["id"]): dict(record) for record in records}
        self.updated = []

    def list_records(self, *, symbol=None, full_report_only=False, **_kwargs):
        records = list(self.records.values())
        if symbol:
            records = [item for item in records if item.get("symbol") == symbol]
        if full_report_only:
            records = [item for item in records if item.get("has_full_report", True)]
        return [dict(item) for item in records]

    def update_record_final_decision(self, record_id, final_decision):
        self.updated.append((record_id, final_decision))
        self.records[int(record_id)]["final_decision"] = dict(final_decision)
        return True

    def get_latest_strategy_context(self, *, symbol=None, **_kwargs):
        records = sorted(
            [item for item in self.records.values() if item.get("symbol") == symbol],
            key=lambda item: (item.get("analysis_date") or "", int(item.get("id") or 0)),
            reverse=True,
        )
        if not records:
            return None
        record = records[0]
        return normalize_strategy_context(
            {
                "origin_analysis_id": record["id"],
                "symbol": record["symbol"],
                "analysis_date": record["analysis_date"],
                "rating": record.get("rating"),
                "summary": record.get("summary"),
                "final_decision": record.get("final_decision") or {},
            }
        )


class FakeMemoryService:
    def __init__(self):
        self.calls = []

    def backfill_from_analysis_history(self, stock_code, *, clear_existing=False, compress_after=True):
        self.calls.append((stock_code, clear_existing, compress_after))
        return {"stock_code": stock_code, "record_count": 1, "working_saved": 1, "facts_saved": 3, "compressed": compress_after}


class FakeLLMClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def call_api(self, messages, **kwargs):
        self.calls.append((messages, kwargs))
        return json.dumps(self.payload, ensure_ascii=False)


class MonitorMemoryBackfillServiceTests(unittest.TestCase):
    def _service(self, records, llm_payload=None, assets=None, items=None):
        return MonitorRelatedExecutionMemoryBackfillService(
            analysis_store=FakeAnalysisStore(records),
            asset_store=FakeAssetStore(assets or []),
            monitoring_store=FakeMonitoringStore(items or []),
            memory_service=FakeMemoryService(),
            llm_client=FakeLLMClient(
                llm_payload
                or {
                    "entry_conditions": ["回踩进场区间并缩量企稳"],
                    "exit_conditions": ["放量跌破支撑位"],
                    "hold_conditions": ["趋势未坏继续持有"],
                    "invalidation_conditions": ["基线逻辑失效"],
                    "execution_plan_summary": "回踩确认后执行，破位则退出。",
                }
            ),
        )

    def test_backfill_extracts_execution_plan_and_rebuilds_memory(self):
        service = self._service(
            records=[
                {
                    "id": 1,
                    "symbol": "600519",
                    "stock_name": "贵州茅台",
                    "analysis_date": "2026-04-20 10:00:00",
                    "rating": "买入",
                    "summary": "等待回踩。",
                    "discussion_result": "回踩 MA20 缩量企稳再考虑建仓，跌破支撑应止损。",
                    "final_decision": {"rating": "买入", "operation_advice": "回踩后分批建仓"},
                    "has_full_report": True,
                }
            ],
            assets=[{"id": 9, "symbol": "600519", "name": "贵州茅台", "status": "focus"}],
            items=[{"id": 20, "symbol": "600519", "monitor_type": "ai_task", "enabled": True, "config": {}}],
        )

        result = service.run(apply=True, workers=1)

        self.assertEqual(result["success_total"], 1)
        self.assertEqual(result["records_updated"], 1)
        updated_decision = service.analysis_store.updated[0][1]
        self.assertEqual(updated_decision["entry_conditions"], ["回踩进场区间并缩量企稳"])
        self.assertIn("execution_plan", updated_decision)
        self.assertEqual(service.memory_service.calls, [("600519", True, True)])
        self.assertEqual(service.monitoring_store.updated[0][0], 20)
        synced_context = service.monitoring_store.updated[0][1]["config"]["strategy_context"]
        self.assertEqual(synced_context["execution_plan"]["exit_conditions"], ["放量跌破支撑位"])

    def test_force_overwrites_existing_execution_plan(self):
        service = self._service(
            records=[
                {
                    "id": 2,
                    "symbol": "000001",
                    "stock_name": "平安银行",
                    "analysis_date": "2026-04-20 10:00:00",
                    "rating": "持有",
                    "summary": "旧计划。",
                    "discussion_result": "",
                    "final_decision": {"entry_conditions": ["旧条件"]},
                    "has_full_report": True,
                }
            ],
            llm_payload={
                "entry_conditions": ["新条件"],
                "exit_conditions": ["新离场"],
                "hold_conditions": [],
                "invalidation_conditions": [],
                "execution_plan_summary": "新计划",
            },
            items=[{"id": 21, "symbol": "000001", "monitor_type": "ai_task", "enabled": True, "config": {}}],
        )

        result = service.run(apply=True, workers=1, force=True)

        self.assertEqual(result["records_updated"], 1)
        self.assertEqual(service.analysis_store.updated[0][1]["entry_conditions"], ["新条件"])

    def test_targets_are_limited_to_monitor_related_stocks(self):
        service = self._service(
            records=[],
            assets=[
                {"id": 1, "symbol": "111111", "name": "持仓股", "status": "holding"},
                {"id": 2, "symbol": "222222", "name": "关注股", "status": "focus"},
                {"id": 3, "symbol": "333333", "name": "研究池", "status": "research"},
            ],
            items=[
                {"id": 31, "symbol": "444444", "monitor_type": "ai_task", "enabled": True},
                {"id": 32, "symbol": "555555", "monitor_type": "price_alert", "enabled": True, "managed_by_portfolio": True},
                {"id": 33, "symbol": "666666", "monitor_type": "price_alert", "enabled": True, "managed_by_portfolio": False},
            ],
        )

        symbols = {item["symbol"] for item in service.list_monitor_related_targets()}

        self.assertEqual(symbols, {"111111", "222222", "444444", "555555"})

    def test_smart_monitor_prompt_includes_execution_conditions(self):
        client = SmartMonitorDeepSeek(api_key="test")
        context = client._build_prompt_context(
            "600519",
            {"current_price": 100, "name": "贵州茅台"},
            {"available_cash": 100000, "total_value": 100000, "total_market_value": 0, "position_usage_pct": 0, "positions_count": 0},
            False,
            {"session": "上午盘", "beijing_hour": 10, "volatility": "medium", "recommendation": "可交易", "can_trade": True},
            strategy_context=normalize_strategy_context(
                {
                    "rating": "买入",
                    "entry_min": 98,
                    "entry_max": 101,
                    "take_profit": 110,
                    "stop_loss": 94,
                    "final_decision": {
                        "entry_conditions": ["回踩后缩量企稳"],
                        "exit_conditions": ["放量跌破 MA20"],
                        "execution_plan_summary": "回踩确认后建仓。",
                    },
                }
            ),
        )

        self.assertIn("回踩后缩量企稳", context["optional_sections"])
        self.assertIn("放量跌破 MA20", context["optional_sections"])

    def test_backfill_requires_monitor_prompt_optimization_ready(self):
        service = self._service(
            records=[],
            assets=[{"id": 9, "symbol": "600519", "name": "贵州茅台", "status": "focus"}],
        )
        original_version = config.SMART_MONITOR_OPTIMIZATION_VERSION
        try:
            config.SMART_MONITOR_OPTIMIZATION_VERSION = "legacy"
            with self.assertRaisesRegex(RuntimeError, "优化未完成"):
                service.run(apply=True, workers=1)
        finally:
            config.SMART_MONITOR_OPTIMIZATION_VERSION = original_version

    def test_backfill_dry_run_reports_optimization_readiness(self):
        service = self._service(
            records=[],
            assets=[{"id": 9, "symbol": "600519", "name": "贵州茅台", "status": "focus"}],
        )

        result = service.run(apply=False, workers=1)

        self.assertTrue(result["optimization_readiness"]["ready"])
        self.assertEqual(result["optimization_readiness"]["required_version"], "execution_conditions_v1")


if __name__ == "__main__":
    unittest.main()
