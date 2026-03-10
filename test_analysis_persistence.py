import shutil
import sys
import time
import threading
import types
import unittest
import uuid
from pathlib import Path

from monitor_db import StockMonitorDatabase
from portfolio_analysis_tasks import PortfolioAnalysisTaskManager
from portfolio_db import PortfolioDB
from portfolio_manager import PortfolioManager
from smart_monitor_db import SmartMonitorDB

sys.modules.setdefault("streamlit", types.SimpleNamespace())

from ui_shared import (
    _format_display_value,
    _has_structured_decision_fields,
    _normalize_agents_results,
    _normalize_discussion_result,
    _normalize_mapping_input,
    _resolve_final_decision_content,
    _split_analysis_report_sections,
    _normalize_text_or_mapping,
)

try:
    from macro_cycle_db import MacroCycleDatabase
except ModuleNotFoundError:
    MacroCycleDatabase = None

try:
    from sector_strategy_engine import SectorStrategyEngine
except ModuleNotFoundError:
    SectorStrategyEngine = None


TEST_TMP_ROOT = Path(".codex_test_tmp")


def make_workspace_temp_dir(prefix: str) -> Path:
    TEST_TMP_ROOT.mkdir(exist_ok=True)
    path = TEST_TMP_ROOT / f"{prefix}{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


class PortfolioHistoryPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = make_workspace_temp_dir("portfolio_history_")
        base = self.temp_dir
        self.portfolio_db = PortfolioDB(str(base / "portfolio.db"))
        self.realtime_monitor_db = StockMonitorDatabase(str(base / "monitor.db"))
        self.smart_monitor_db = SmartMonitorDB(str(base / "smart.db"))
        self.manager = PortfolioManager(
            portfolio_store=self.portfolio_db,
            realtime_monitor_store=self.realtime_monitor_db,
            smart_monitor_store=self.smart_monitor_db,
        )
        self.manager._resolve_stock_name = lambda code: f"Stock{code}"

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _add_stock(self, code: str, cost_price: float = 10.0, quantity: int = 100):
        success, msg, stock_id = self.manager.add_stock(
            code=code,
            name=None,
            cost_price=cost_price,
            quantity=quantity,
            note="test",
            auto_monitor=True,
        )
        self.assertTrue(success, msg)
        return stock_id

    def test_summary_sanitization_and_fallback(self):
        clean_summary = self.manager._extract_analysis_summary(
            {
                "rating": "买入",
                "operation_advice": "<think>internal reasoning</think>建议分批低吸，跌破9.80元止损。",
                "entry_range": "10.00-10.50",
                "take_profit": "11.80元",
                "stop_loss": "9.80元",
            }
        )
        self.assertIn("建议分批低吸", clean_summary)
        self.assertNotIn("<think>", clean_summary)

        fallback_summary = self.manager._extract_analysis_summary(
            {
                "rating": "持有",
                "operation_advice": "【推理过程】我现在需要先逐步分析所有因子，然后再输出。",
                "entry_range": "10.00-10.50",
                "take_profit": "11.80元",
                "stop_loss": "9.80元",
            }
        )
        self.assertIn("评级: 持有", fallback_summary)
        self.assertIn("进场区间", fallback_summary)
        self.assertIn("止盈位", fallback_summary)

    def test_history_queries_only_return_full_reports(self):
        stock_id = self._add_stock("600519")
        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="持有",
            confidence=6.0,
            current_price=100.0,
            target_price=110.0,
            entry_min=98.0,
            entry_max=101.0,
            take_profit=112.0,
            stop_loss=95.0,
            summary="不完整记录",
            analysis_source="portfolio_batch_analysis",
            has_full_report=False,
        )
        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="买入",
            confidence=8.2,
            current_price=101.5,
            target_price=115.0,
            entry_min=100.0,
            entry_max=102.0,
            take_profit=115.0,
            stop_loss=96.0,
            summary="建议分批建仓。",
            stock_info={"symbol": "600519", "name": "Stock600519", "current_price": 101.5},
            agents_results={
                "technical": {
                    "agent_name": "技术分析师",
                    "agent_role": "技术面",
                    "focus_areas": ["趋势"],
                    "analysis": "趋势改善",
                    "timestamp": "2026-03-07 10:00:00",
                }
            },
            discussion_result="团队讨论认为回撤可控。",
            final_decision={
                "rating": "买入",
                "confidence_level": 8.2,
                "entry_range": "100.0-102.0",
                "take_profit": "115.0元",
                "stop_loss": "96.0元",
                "operation_advice": "建议分批建仓。",
            },
            analysis_source="portfolio_batch_analysis",
            has_full_report=True,
        )

        full_history = self.portfolio_db.get_analysis_history(stock_id, limit=10)
        self.assertEqual(len(full_history), 1)
        self.assertEqual(full_history[0]["analysis_source"], "portfolio_batch_analysis")
        self.assertTrue(full_history[0]["has_full_report"])
        self.assertEqual(full_history[0]["stock_info"]["symbol"], "600519")
        self.assertNotIn("stock_info_json", full_history[0])
        self.assertNotIn("agents_results_json", full_history[0])
        self.assertNotIn("final_decision_json", full_history[0])

        latest = self.portfolio_db.get_latest_analysis(stock_id)
        self.assertIsNotNone(latest)
        self.assertEqual(latest["summary"], "建议分批建仓。")
        self.assertEqual(latest["final_decision"]["rating"], "买入")
        self.assertEqual(latest["discussion_result"], "团队讨论认为回撤可控。")

    def test_latest_analysis_uses_newer_research_report_for_same_portfolio_asset(self):
        stock_id = self._add_stock("600519")
        self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="持有",
            confidence=6.8,
            current_price=101.0,
            target_price=110.0,
            entry_min=99.0,
            entry_max=102.0,
            take_profit=112.0,
            stop_loss=96.0,
            summary="较早的持仓分析",
            analysis_time="2026-03-09 09:00:00",
            analysis_source="portfolio_batch_analysis",
            has_full_report=True,
            stock_info={"symbol": "600519", "name": "Stock600519", "current_price": 101.0},
            agents_results={"technical": {"analysis": "持仓维持观察"}},
            discussion_result="旧持仓分析。",
            final_decision={
                "rating": "持有",
                "confidence_level": 6.8,
                "entry_min": 99.0,
                "entry_max": 102.0,
                "take_profit": 112.0,
                "stop_loss": 96.0,
                "operation_advice": "继续观察。",
            },
        )

        latest_research_id = self.portfolio_db.analysis_repository.save_record(
            symbol="600519",
            stock_name="Stock600519",
            period="1y",
            stock_info={"symbol": "600519", "name": "Stock600519", "current_price": 103.5},
            agents_results={"technical": {"analysis": "深度分析后趋势更强"}},
            discussion_result="更新后的深度分析。",
            final_decision={
                "rating": "买入",
                "confidence_level": 8.9,
                "entry_min": 102.0,
                "entry_max": 104.0,
                "take_profit": 118.0,
                "stop_loss": 98.0,
                "operation_advice": "最新深度分析建议加仓。",
            },
            account_name="默认账户",
            asset_id=stock_id,
            analysis_scope="research",
            analysis_source="home_single_analysis",
            analysis_date="2026-03-10 09:30:00",
            summary="最新深度分析建议加仓。",
            has_full_report=True,
            asset_status_snapshot="portfolio",
        )

        latest = self.portfolio_db.get_latest_analysis(stock_id)
        self.assertIsNotNone(latest)
        self.assertEqual(latest["id"], latest_research_id)
        self.assertEqual(latest["analysis_scope"], "research")
        self.assertEqual(latest["summary"], "最新深度分析建议加仓。")
        self.assertEqual(latest["final_decision"]["rating"], "买入")

        latest_rows = self.portfolio_db.get_all_latest_analysis()
        row = next(item for item in latest_rows if item["id"] == stock_id)
        self.assertEqual(row["summary"], "最新深度分析建议加仓。")
        self.assertEqual(row["analysis_scope"], "research")

    def test_build_analysis_payload_uses_clean_default_rating(self):
        payload = self.manager._build_analysis_payload(
            stock_info={"current_price": 10.5},
            final_decision={},
        )

        self.assertEqual(payload["rating"], "持有")
        self.assertEqual(payload["summary"], "评级: 持有")

    def test_manual_buy_trade_recalculates_weighted_cost(self):
        stock_id = self._add_stock("300750", cost_price=10.0, quantity=100)

        success, msg, updated_stock = self.manager.record_trade(
            stock_id=stock_id,
            trade_type="buy",
            quantity=50,
            price=13.0,
            trade_date="2026-03-10",
            note="手工加仓",
        )

        self.assertTrue(success, msg)
        self.assertIsNotNone(updated_stock)
        self.assertEqual(int(updated_stock["quantity"]), 150)
        self.assertAlmostEqual(float(updated_stock["cost_price"]), 11.0, places=6)

    def test_manual_sell_trade_recalculates_cost_by_tonghuashun_rule(self):
        stock_id = self._add_stock("300760", cost_price=10.0, quantity=100)

        success, msg, updated_stock = self.manager.record_trade(
            stock_id=stock_id,
            trade_type="sell",
            quantity=40,
            price=12.0,
            trade_date="2026-03-10",
            note="手工减仓",
        )

        self.assertTrue(success, msg)
        self.assertIsNotNone(updated_stock)
        self.assertEqual(int(updated_stock["quantity"]), 60)
        self.assertAlmostEqual(float(updated_stock["cost_price"]), (1000.0 - 480.0) / 60.0, places=6)

    def test_manual_clear_trade_auto_downgrades_to_watchlist(self):
        stock_id = self._add_stock("300136", cost_price=10.0, quantity=100)

        success, msg, updated_stock = self.manager.record_trade(
            stock_id=stock_id,
            trade_type="clear",
            quantity=1,
            price=12.0,
            trade_date="2026-03-10",
            note="全部卖出",
        )

        self.assertTrue(success, msg)
        self.assertIn("清仓", msg)
        self.assertIsNotNone(updated_stock)
        self.assertEqual(updated_stock["status"], "watchlist")
        self.assertEqual(updated_stock["position_status"], "watchlist")
        self.assertIsNone(updated_stock["quantity"])
        self.assertIsNone(updated_stock["cost_price"])

    def test_delete_analysis_record_removes_single_history_entry(self):
        stock_id = self._add_stock("000001")
        analysis_id = self.portfolio_db.save_analysis(
            stock_id=stock_id,
            rating="买入",
            confidence=7.5,
            current_price=12.3,
            target_price=13.8,
            summary="测试删除",
            analysis_source="portfolio_single_analysis",
            has_full_report=False,
        )

        success, msg = self.manager.delete_analysis_record(analysis_id)
        self.assertTrue(success, msg)
        self.assertEqual(self.portfolio_db.get_analysis_history(stock_id, limit=10), [])

    def test_build_stock_card_view_model_formats_name_pnl_and_summary(self):
        view_model = self.manager.build_stock_card_view_model(
            stock={
                "code": "300136",
                "name": "信维通信",
                "cost_price": 10.0,
                "quantity": 100,
                "note": "波段仓位",
                "auto_monitor": True,
            },
            latest_analysis={
                "rating": "鎸佹湁",
                "current_price": 12.5,
                "analysis_time": "2026-03-08 10:28:08.754695",
                "summary": "<p class='portfolio-stock-card__summary'>评级: 持有；短线可继续跟踪。</p>",
                "final_decision": {},
            },
        )

        self.assertEqual(view_model["display_name"], "信维通信")
        self.assertEqual(view_model["cost_text"], "¥10.000")
        self.assertEqual(view_model["quantity_text"], "100股")
        self.assertEqual(view_model["pnl_amount_text"], "+¥250.00")
        self.assertEqual(view_model["pnl_percent_text"], "+25.00%")
        self.assertEqual(view_model["rating"], "持有")
        self.assertEqual(view_model["analysis_time_text"], "2026-03-08 10:28")
        self.assertEqual(view_model["summary_text"], "短线可继续跟踪")
        self.assertEqual(view_model["note_text"], "波段仓位")

    def test_build_stock_card_view_model_resolves_rating_priority_and_missing_pnl(self):
        view_model = self.manager.build_stock_card_view_model(
            stock={
                "code": "600519",
                "name": "贵州茅台",
                "cost_price": 100.0,
                "quantity": 50,
                "auto_monitor": False,
            },
            latest_analysis={
                "rating": "未知",
                "analysis_time": "2026-03-08 09:15:00",
                "summary": "评级: 卖出；风险偏高",
                "final_decision": {"rating": "买入"},
            },
        )

        self.assertEqual(view_model["display_name"], "贵州茅台")
        self.assertEqual(view_model["rating"], "买入")
        self.assertEqual(view_model["summary_text"], "风险偏高")
        self.assertEqual(view_model["pnl_amount_text"], "")
        self.assertEqual(view_model["pnl_percent_text"], "")

        summary_only_view = self.manager.build_stock_card_view_model(
            stock={"code": "000001", "name": "平安银行", "auto_monitor": True},
            latest_analysis={
                "summary": "评级: 持有；等待企稳",
                "final_decision": {},
            },
        )
        self.assertEqual(summary_only_view["rating"], "持有")
        self.assertEqual(summary_only_view["summary_text"], "等待企稳")


class UiSharedNormalizationTests(unittest.TestCase):
    def test_mapping_normalization_accepts_dict_and_json_string(self):
        normalized, invalid = _normalize_mapping_input({"current_price": 123.45})
        self.assertEqual(normalized["current_price"], 123.45)
        self.assertFalse(invalid)

        normalized, invalid = _normalize_mapping_input('{"current_price": 123.45}')
        self.assertEqual(normalized["current_price"], 123.45)
        self.assertFalse(invalid)

    def test_mapping_normalization_rejects_invalid_or_non_object_strings(self):
        normalized, invalid = _normalize_mapping_input("not-json")
        self.assertEqual(normalized, {})
        self.assertTrue(invalid)

        normalized, invalid = _normalize_mapping_input('"text only"')
        self.assertEqual(normalized, {})
        self.assertTrue(invalid)

    def test_agents_and_final_decision_normalization_tolerate_json_strings(self):
        agents_results, invalid = _normalize_agents_results(
            '{"technical": {"agent_name": "技术分析师", "analysis": "趋势改善"}}'
        )
        self.assertFalse(invalid)
        self.assertEqual(agents_results["technical"]["analysis"], "趋势改善")

        final_decision, invalid = _normalize_text_or_mapping(
            '{"rating": "买入", "confidence_level": 8.5}'
        )
        self.assertFalse(invalid)
        self.assertEqual(final_decision["rating"], "买入")

        final_decision, invalid = _normalize_text_or_mapping("plain text decision")
        self.assertFalse(invalid)
        self.assertEqual(final_decision, "plain text decision")

    def test_discussion_normalization_decodes_json_encoded_text(self):
        self.assertEqual(
            _normalize_discussion_result('"团队讨论认为回撤可控。"'),
            "团队讨论认为回撤可控。",
        )
        self.assertEqual(_normalize_discussion_result("plain discussion"), "plain discussion")

    def test_split_analysis_report_sections_extracts_body_and_reasoning(self):
        body, reasoning = _split_analysis_report_sections(
            "【推理过程】\n先逐项梳理指标，再组织成报告。\n# 技术分析报告\n\n正文结论在这里。"
        )
        self.assertEqual(body, "# 技术分析报告\n\n正文结论在这里。")
        self.assertEqual(reasoning, "先逐项梳理指标，再组织成报告。")

        body, reasoning = _split_analysis_report_sections("直接给出报告正文")
        self.assertEqual(body, "直接给出报告正文")
        self.assertEqual(reasoning, "")

    def test_split_analysis_report_sections_extracts_macro_cycle_body_after_reasoning(self):
        body, reasoning = _split_analysis_report_sections(
            "【推理过程】\n先综合三位分析师的结论，再组织成最终策略。\n"
            "## 一、周期仪表盘（双指针+政策风向标）\n"
            "这里开始是宏观周期分析报告正文。\n"
            "## 二、综合资产配置建议\n"
            "继续正文内容。"
        )

        self.assertEqual(
            body,
            "## 一、周期仪表盘（双指针+政策风向标）\n"
            "这里开始是宏观周期分析报告正文。\n"
            "## 二、综合资产配置建议\n"
            "继续正文内容。",
        )
        self.assertEqual(reasoning, "先综合三位分析师的结论，再组织成最终策略。")

    def test_resolve_final_decision_content_extracts_embedded_json(self):
        final_decision, invalid, reasoning = _resolve_final_decision_content(
            {
                "decision_text": """【推理过程】
先核对会议结论，再整理成 JSON。

{
  "rating": "持有",
  "target_price": "76.00",
  "operation_advice": "等待信号后分批建仓",
  "entry_range": "60.00-64.00",
  "take_profit": "76.00",
  "stop_loss": "60.00",
  "holding_period": "3-6个月",
  "position_size": "轻仓",
  "risk_warning": "关注减持与解禁风险",
  "confidence_level": 5
}"""
            }
        )
        self.assertFalse(invalid)
        self.assertEqual(final_decision["rating"], "持有")
        self.assertEqual(final_decision["operation_advice"], "等待信号后分批建仓")
        self.assertIn("先核对会议结论", reasoning)

    def test_structured_final_decision_detection_allows_decision_text_sidecar(self):
        self.assertTrue(
            _has_structured_decision_fields(
                {
                    "rating": "买入",
                    "confidence_level": 8,
                    "decision_text": "这是补充说明，不应覆盖结构化布局。",
                }
            )
        )

    def test_format_display_value_supports_structured_entry_range(self):
        self.assertEqual(
            _format_display_value("entry_range", {"min": 10.5, "max": 12.0}),
            "¥10.50 - ¥12.00",
        )


class PortfolioAnalysisTaskManagerTests(unittest.TestCase):
    def test_task_manager_persists_active_and_finished_state(self):
        manager = PortfolioAnalysisTaskManager()
        started = threading.Event()
        finished = threading.Event()

        def runner(_task_id, report_progress):
            report_progress(
                current=0,
                total=1,
                step_code="300136",
                step_status="analyzing",
                message="正在分析 300136",
            )
            started.set()
            time.sleep(0.05)
            report_progress(
                current=1,
                total=1,
                step_code="300136",
                step_status="success",
                message="300136 分析完成",
            )
            finished.set()
            return {"stock_code": "300136", "saved_count": 1}

        task_id = manager.start_task(
            "session-a",
            task_type="single",
            label="单股分析",
            runner=runner,
            metadata={"stock_code": "300136"},
        )
        self.assertTrue(started.wait(1.0))
        active_task = manager.get_active_task("session-a")
        self.assertIsNotNone(active_task)
        self.assertEqual(active_task["id"], task_id)
        self.assertTrue(manager.has_active_task("session-a"))
        self.assertEqual(active_task["message"], "正在分析 300136")

        self.assertTrue(finished.wait(1.0))
        for _ in range(40):
            latest_task = manager.get_latest_task("session-a")
            if latest_task and latest_task.get("status") == "success":
                break
            time.sleep(0.02)
        else:
            self.fail("task did not finish in time")

        latest_task = manager.get_latest_task("session-a")
        self.assertEqual(latest_task["status"], "success")
        self.assertEqual(latest_task["result"]["saved_count"], 1)
        self.assertIsNone(manager.get_active_task("session-a"))

    def test_task_manager_queues_second_task_in_same_session(self):
        manager = PortfolioAnalysisTaskManager()
        blocker = threading.Event()
        started = threading.Event()
        second_started = threading.Event()
        execution_order = []

        def runner(_task_id, report_progress):
            report_progress(total=1, current=0, message="running")
            execution_order.append("first")
            started.set()
            blocker.wait(1.0)
            return {"ok": True}

        def second_runner(_task_id, report_progress):
            report_progress(total=1, current=0, message="queued")
            execution_order.append("second")
            second_started.set()
            return {"ok": True}

        manager.start_task(
            "session-a",
            task_type="single",
            label="first",
            runner=runner,
        )
        self.assertTrue(started.wait(1.0))
        second_task_id = manager.start_task(
            "session-a",
            task_type="batch",
            label="second",
            runner=second_runner,
        )
        pending_tasks = manager.get_pending_tasks("session-a")
        self.assertEqual(len(pending_tasks), 2)
        queued_task = next(task for task in pending_tasks if task["id"] == second_task_id)
        self.assertEqual(queued_task["status"], "queued")
        self.assertEqual(manager.count_queued_tasks("session-a"), 1)
        blocker.set()
        self.assertTrue(second_started.wait(1.0))
        for _ in range(40):
            latest_task = manager.get_task(second_task_id)
            if latest_task and latest_task.get("status") == "success":
                break
            time.sleep(0.02)
        else:
            self.fail("queued task did not finish in time")
        self.assertEqual(execution_order, ["first", "second"])

    def test_task_manager_can_recover_tasks_without_original_session_id(self):
        manager = PortfolioAnalysisTaskManager()
        blocker = threading.Event()
        started = threading.Event()

        def runner(_task_id, report_progress):
            report_progress(total=2, current=1, step_code="300136", message="running")
            started.set()
            blocker.wait(1.0)
            report_progress(total=2, current=2, step_code="300136", message="done")
            return {"ok": True}

        task_id = manager.start_task(
            "session-a",
            task_type="batch",
            label="batch-task",
            runner=runner,
        )
        self.assertTrue(started.wait(1.0))

        active_task = manager.get_active_task_any(task_type="batch")
        self.assertIsNotNone(active_task)
        self.assertEqual(active_task["id"], task_id)
        self.assertEqual(len(manager.get_pending_tasks_any(task_type="batch")), 1)

        blocker.set()
        for _ in range(40):
            latest_task = manager.get_latest_task_any(task_type="batch")
            if latest_task and latest_task.get("status") == "success":
                break
            time.sleep(0.02)
        else:
            self.fail("global task lookup did not observe completion")

        latest_task = manager.get_latest_task_any(task_type="batch")
        self.assertEqual(latest_task["status"], "success")
        self.assertEqual(latest_task["result"]["ok"], True)


@unittest.skipIf(MacroCycleDatabase is None, "macro cycle dependencies unavailable")
class MacroCyclePersistenceTests(unittest.TestCase):
    def test_macro_cycle_database_roundtrip(self):
        temp_dir = make_workspace_temp_dir("macro_cycle_")
        try:
            db = MacroCycleDatabase(str(temp_dir / "macro_cycle.db"))
            report_id = db.save_analysis_report(
                {
                    "success": True,
                    "timestamp": "2026-03-07 12:00:00",
                    "agents_analysis": {
                        "chief": {"analysis": "当前处于复苏后段，权益资产仍有配置价值。"}
                    },
                },
                "当前处于复苏后段，权益资产仍有配置价值。",
                "当前处于复苏后段，权益资产仍有配置价值。",
            )

            latest = db.get_latest_report()
            self.assertIsNotNone(latest)
            self.assertEqual(latest["id"], report_id)
            self.assertEqual(latest["result_parsed"]["timestamp"], "2026-03-07 12:00:00")

            history = db.get_historical_reports(limit=10)
            self.assertEqual(len(history), 1)

            detail = db.get_report_detail(report_id)
            self.assertEqual(detail["summary"], "当前处于复苏后段，权益资产仍有配置价值。")
            self.assertTrue(db.delete_report(report_id))
            self.assertIsNone(db.get_latest_report())
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


@unittest.skipIf(SectorStrategyEngine is None, "sector strategy dependencies unavailable")
class SectorStrategySummaryTests(unittest.TestCase):
    def test_generate_report_summary_uses_structured_predictions(self):
        engine = SectorStrategyEngine.__new__(SectorStrategyEngine)
        summary = engine._generate_report_summary(
            {
                "final_predictions": {
                    "summary": {
                        "market_view": "市场风险偏好回升",
                        "key_opportunity": "高景气成长板块有轮动机会",
                    },
                    "long_short": {
                        "bullish": [{"sector": "算力"}, {"sector": "机器人"}],
                        "bearish": [{"sector": "高位题材"}],
                    },
                }
            }
        )

        self.assertIn("市场风险偏好回升", summary)
        self.assertIn("高景气成长板块有轮动机会", summary)
        self.assertIn("看多板块", summary)
        self.assertIn("关注风险板块", summary)


if __name__ == "__main__":
    unittest.main()
