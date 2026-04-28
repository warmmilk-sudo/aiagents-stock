import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from asset_repository import AssetRepository, STATUS_FOCUS, STATUS_RESEARCH
from smart_selection_service import SmartSelectionService


def build_analysis_payload(startup_score: int, explosive_score: int, decay_score: int) -> dict:
    return {
        "final_predictions": {
            "heat": {
                "hottest": [
                    {"sector": "机器人", "score": explosive_score, "trend": "升温"},
                    {"sector": "高位题材", "score": decay_score, "trend": "降温"},
                ],
                "heating": [
                    {"sector": "算力租赁", "score": startup_score, "trend": "升温"},
                ],
                "cooling": [],
            }
        }
    }


class SmartSelectionServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.asset_repo = AssetRepository(str(Path(self.temp_dir.name) / "investment.db"))
        self.service = SmartSelectionService(str(Path(self.temp_dir.name) / "investment.db"))

        self.asset_repo.create_or_update_research_asset(symbol="600001", name="算力一号", note="观察")
        self.asset_repo.update_asset(self.asset_repo.get_asset_by_symbol("600001")["id"], sector_tags_json=["算力租赁"])
        self.asset_repo.create_or_update_research_asset(symbol="600002", name="机器人二号", note="执行")
        self.asset_repo.update_asset(self.asset_repo.get_asset_by_symbol("600002")["id"], sector_tags_json=["机器人"])
        self.asset_repo.create_or_update_research_asset(symbol="600003", name="高位三号", note="衰退")
        self.asset_repo.update_asset(self.asset_repo.get_asset_by_symbol("600003")["id"], sector_tags_json=["高位题材"])
        self.old_focus_id = self.asset_repo.promote_to_watchlist(symbol="600099", name="旧关注", note="旧关注")
        self.asset_repo.update_asset(self.old_focus_id, manual_pin=True)

        self.service.sector_strategy_db.save_analysis_report("2026-04-01 数据分析", build_analysis_payload(40, 70, 95), [], "r1")
        self.service.sector_strategy_db.save_analysis_report("2026-04-02 数据分析", build_analysis_payload(55, 90, 95), [], "r2")
        self.report_id = self.service.sector_strategy_db.save_analysis_report("2026-04-03 数据分析", build_analysis_payload(70, 95, 85), [], "r3")
        self.report = self.service.sector_strategy_db.get_analysis_report(self.report_id)
        self.market_context = {
            "state": "momentum",
            "state_label": "主升浪",
            "profile": {"heat_multiplier": 0.92, "agent_multiplier": 1.12, "weights": {}},
            "signals": ["指数均值 +1.20%", "上涨占比 72.0%", "涨跌停 85/4"],
        }
        self.extracted_sectors = [
            {"sector": "机器人", "heat_score": 95, "source": "heat.hottest"},
            {"sector": "算力租赁", "heat_score": 82, "source": "heat.heating"},
            {"sector": "高位题材", "heat_score": 78, "source": "heat.hottest"},
        ]

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def _fake_candidate(self, asset, _context, _lifecycle_snapshot):
        symbol = asset["symbol"]
        mapping = {
            "600001": ("算力租赁", 200.0, 78.0, 62.0, 70.0, 58.0, 60.0, 22.0),
            "600002": ("机器人", 150.0, 82.0, 72.0, 80.0, 76.0, 75.0, 18.0),
            "600003": ("高位题材", 180.0, 85.0, 80.0, 78.0, 74.0, 73.0, 15.0),
        }
        primary_sector, market_cap, trend, reversal, mean_reversion, order_flow, intraday, distribution = mapping[symbol]
        return {
            "asset_id": asset["id"],
            "symbol": symbol,
            "name": asset["name"],
            "matched_sectors": [{"sector": primary_sector, "canonical_sector": primary_sector, "heat_score": 90, "match_score": 1.0}],
            "primary_sector": primary_sector,
            "canonical_sector": primary_sector,
            "match_score": 1.0,
            "heat_score": 90.0,
            "tech_score": 80.0,
            "composite_score": 160.0,
            "technical_metrics": {
                "trend_score": trend,
                "reversal_score": reversal,
                "mean_reversion_score": mean_reversion,
                "order_flow_score": order_flow,
                "intraday_score": intraday,
                "chip_score": 70.0,
                "volume_score": 60.0,
                "distribution_risk": distribution,
                "volume_contraction_days": 3,
                "intraday_bias": "pullback_support",
                "tail_session": True,
                "latest_minute_time": "14:45",
                "latest_trade_time": "14:45",
                "realtime_freshness": {
                    "intraday_decision_ready": True,
                    "overall_status": "ready",
                },
                "bias_pct": -3.0,
            },
            "reason": f"{primary_sector} 候选",
            "market_cap": market_cap,
            "asset": asset,
        }

    def _tail_run_session(self):
        return {
            "trigger_source": "manual",
            "mode": "tail_execution",
            "label": "尾盘",
            "time": "14:45",
            "is_trading_day": True,
            "can_trade_now": True,
            "allow_final_selection": True,
            "requires_tail": True,
            "requires_freshness": True,
            "post_close_review": False,
            "recommendation": "当前处于尾盘窗口，可按执行门槛生成最终清单",
        }

    def _run_with_common_patches(
        self,
        *,
        score_side_effect=None,
        risk_side_effect=None,
        run_session=None,
        sync_result=None,
        longhubang_sync_result=None,
    ):
        external_discovery_patch = patch.object(SmartSelectionService, "_discover_external_candidates", return_value=[])
        external_discovery_patch.start()
        self.addCleanup(external_discovery_patch.stop)
        run_session_patch = patch.object(self.service, "_resolve_run_session", return_value=run_session or self._tail_run_session())
        run_session_patch.start()
        self.addCleanup(run_session_patch.stop)
        sync_patch = patch.object(
            self.service,
            "_sync_news_flow_context",
            return_value=sync_result or {"success": True, "snapshot_id": 1001, "duration": 1.2},
        )
        sync_patch.start()
        self.addCleanup(sync_patch.stop)
        longhubang_sync_patch = patch.object(
            self.service,
            "_sync_longhubang_data",
            return_value=longhubang_sync_result
            or {"attempted": True, "success": True, "date": "2026-04-03", "records": 12, "saved": 12},
        )
        longhubang_sync_patch.start()
        self.addCleanup(longhubang_sync_patch.stop)
        return patch("smart_selection_service.asset_repository", self.asset_repo), patch(
            "smart_selection_service.research_hub_service.ensure_recent_sector_strategy_report",
            return_value={"reused": True, "report_id": self.report_id, "report": self.report, "warnings": []},
        ), patch(
            "smart_selection_service.research_hub_service._build_selection_market_context",
            return_value=self.market_context,
        ), patch(
            "smart_selection_service.research_hub_service._extract_selection_sectors",
            return_value=self.extracted_sectors,
        ), patch(
            "smart_selection_service.research_hub_service._collect_asset_match_context",
            return_value={},
        ), patch(
            "smart_selection_service.research_hub_service._score_selection_candidate",
            side_effect=score_side_effect or self._fake_candidate,
        ), patch(
            "smart_selection_service.research_hub_service._group_recent_longhubang_by_symbol",
            return_value={},
        ), patch(
            "smart_selection_service.research_hub_service._evaluate_risk_for_symbol",
            side_effect=risk_side_effect or (lambda *_args, **_kwargs: {"vetoed": False, "risk_notes": [], "risk_level": "low"}),
        )

    def test_pipeline_routes_candidates_by_lifecycle(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        common_patches = self._run_with_common_patches()
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual([item["symbol"] for item in result["observed_startup_candidates"]], ["600001"])
        self.assertEqual([item["symbol"] for item in result["final_selected"]], ["600002"])
        self.assertEqual([item["symbol"] for item in result["excluded_by_lifecycle_veto"]], ["600003"])
        self.assertEqual(result["market_context"]["state"], "momentum")
        watch_pool = self.service.list_watch_pool()
        self.assertEqual([item["symbol"] for item in watch_pool], ["600001"])
        selected = result["final_selected"][0]
        self.assertAlmostEqual(selected["raw_agent_composite_score"], 160.0, places=2)
        self.assertAlmostEqual(selected["agent_composite_score"], 80.0, places=2)
        self.assertGreater(selected["execution_composite_score"], 0)
        expected_score = round(
            selected["agent_composite_score"] * selected["score_fusion_weights"]["agent"]
            + selected["execution_composite_score"] * selected["score_fusion_weights"]["execution"]
            + selected["readiness_adjustment"],
            2,
        )
        self.assertAlmostEqual(selected["score"], expected_score, places=2)
        self.assertTrue(result["news_flow_context"]["sync_success"])
        self.assertEqual(result["news_flow_context"]["sync_snapshot_id"], 1001)
        self.assertTrue(result["longhubang_sync"]["success"])

    def test_import_replaces_existing_focus(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        common_patches = self._run_with_common_patches()
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            self.service._run_pipeline(run_id)
            result = self.service.import_run_selection(run_id=run_id, symbols=["600002"], replace_existing_focus=True)

        self.assertEqual(result["imported_symbols"], ["600002"])
        self.assertEqual(self.asset_repo.get_asset(self.old_focus_id)["status"], STATUS_RESEARCH)
        self.assertFalse(self.asset_repo.get_asset(self.old_focus_id)["manual_pin"])
        self.assertEqual(self.asset_repo.get_asset_by_symbol("600002")["status"], STATUS_FOCUS)
        self.assertEqual([item["symbol"] for item in self.service.list_watch_pool()], ["600001"])

    def test_scheduler_config_round_trip_includes_max_workers(self):
        with patch("smart_selection_service.smart_selection_scheduler.apply_runtime_config") as apply_runtime_config, patch(
            "smart_selection_service.smart_selection_scheduler.get_status",
            side_effect=lambda: {
                "running": False,
                "enabled": self.service.get_scheduler_config()["enabled"],
                "schedule_time": self.service.get_scheduler_config()["schedule_time"],
                "max_workers": self.service.get_scheduler_config()["max_workers"],
                "next_run_time": None,
                "last_run_time": None,
            },
        ):
            status = self.service.update_scheduler_config(enabled=True, schedule_time="14:35", max_workers=8)

        apply_runtime_config.assert_called_once()
        self.assertTrue(status["enabled"])
        self.assertEqual(status["schedule_time"], "14:35")
        self.assertEqual(status["max_workers"], 8)
        self.assertEqual(self.service.get_scheduler_config()["max_workers"], 8)

    def test_longhubang_sync_skips_before_daily_publish_time(self):
        warnings = []

        result = self.service._sync_longhubang_data(warnings, now=datetime(2026, 4, 3, 17, 29))

        self.assertFalse(result["attempted"])
        self.assertIn("17:30", result["reason"])
        self.assertEqual(warnings, [])

    def test_longhubang_sync_fetches_and_saves_after_daily_publish_time(self):
        warnings = []
        fetcher_instance = unittest.mock.Mock()
        fetcher_instance.get_longhubang_data.return_value = {
            "data": [
                {
                    "rq": "2026-04-03",
                    "gpdm": "600002",
                    "gpmc": "机器人二号",
                    "yzmc": "测试游资",
                    "jlrje": 12000000,
                }
            ]
        }
        database_instance = unittest.mock.Mock()
        database_instance.save_longhubang_data.return_value = 1

        with patch("longhubang_data.LonghubangDataFetcher", return_value=fetcher_instance), patch(
            "longhubang_db.LonghubangDatabase",
            return_value=database_instance,
        ):
            result = self.service._sync_longhubang_data(warnings, now=datetime(2026, 4, 3, 17, 31))

        self.assertTrue(result["attempted"])
        self.assertTrue(result["success"])
        self.assertEqual(result["date"], "2026-04-03")
        self.assertEqual(result["records"], 1)
        self.assertEqual(result["saved"], 1)
        fetcher_instance.get_longhubang_data.assert_called_once_with("2026-04-03")
        database_instance.save_longhubang_data.assert_called_once()
        self.assertEqual(warnings, [])

    def test_pipeline_injects_market_context_into_candidate_scoring(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        score_calls = []

        def capture_candidate(asset, context, lifecycle_snapshot):
            score_calls.append((asset["symbol"], context, lifecycle_snapshot))
            return self._fake_candidate(asset, context, lifecycle_snapshot)

        common_patches = self._run_with_common_patches(score_side_effect=capture_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            self.service._run_pipeline(run_id)

        self.assertTrue(score_calls)
        self.assertTrue(all(call[1].get("market_context", {}).get("state") == "momentum" for call in score_calls))

    def test_scheduled_pipeline_requires_tail_session_for_final_selection(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def non_tail_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            candidate["technical_metrics"]["tail_session"] = False
            candidate["technical_metrics"]["latest_minute_time"] = "13:20"
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=non_tail_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id, trigger_source="scheduled")

        self.assertEqual(result["final_selected"], [])
        self.assertTrue(any("未到尾盘时段" in warning for warning in result["warnings"]))

    def test_manual_pipeline_relaxes_tail_session_requirement(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def non_tail_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            candidate["technical_metrics"]["tail_session"] = False
            candidate["technical_metrics"]["latest_minute_time"] = "13:20"
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=non_tail_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id, trigger_source="manual")

        self.assertEqual(result["final_selected"], [])
        self.assertCountEqual([item["symbol"] for item in result["observe_candidates"]], ["600001", "600002"])
        explosive_observe = next(item for item in result["observe_candidates"] if item["symbol"] == "600002")
        self.assertFalse(explosive_observe["execution_ready"])
        self.assertIn("未到尾盘执行时段", explosive_observe["reason"])
        self.assertTrue(any("观察级" in warning for warning in result["warnings"]))

    def test_manual_before_tail_is_preview_only_even_when_candidate_has_tail_data(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        run_session = {
            **self._tail_run_session(),
            "mode": "afternoon_preview",
            "label": "下午盘非尾盘",
            "time": "13:30",
            "allow_final_selection": False,
            "requires_tail": False,
            "requires_freshness": False,
            "recommendation": "尚未到 14:30 尾盘确认窗口，仅输出观察级候选",
        }

        common_patches = self._run_with_common_patches(run_session=run_session)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id, trigger_source="manual")

        self.assertEqual(result["final_selected"], [])
        self.assertEqual([item["symbol"] for item in result["excluded_by_execution_gate"]], ["600002"])
        self.assertEqual(result["excluded_by_execution_gate"][0]["execution_gate_type"], "session_time")
        self.assertEqual(result["match_diagnostics"]["session_time_gated_count"], 1)
        self.assertEqual(result["run_session"]["mode"], "afternoon_preview")
        self.assertTrue(any("手动触发时段" in warning for warning in result["warnings"]))

    def test_manual_after_close_allows_next_session_review_candidates_without_freshness(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        run_session = {
            **self._tail_run_session(),
            "mode": "post_close_review",
            "label": "盘后复盘",
            "time": "15:30",
            "can_trade_now": False,
            "allow_final_selection": True,
            "requires_tail": False,
            "requires_freshness": False,
            "post_close_review": True,
            "recommendation": "收盘后只能生成次日候选，次日开盘前需要重新确认",
        }

        def post_close_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            if asset["symbol"] == "600002":
                candidate["technical_metrics"]["realtime_freshness"] = {
                    "intraday_decision_ready": False,
                    "intraday_review_ready": True,
                    "has_tail_or_close_snapshot": True,
                    "overall_status": "after_close",
                }
                candidate["technical_metrics"]["tail_session"] = False
                candidate["technical_metrics"]["latest_minute_time"] = "15:00"
                candidate["technical_metrics"]["latest_trade_time"] = "15:00"
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=post_close_candidate, run_session=run_session)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id, trigger_source="manual")

        self.assertEqual([item["symbol"] for item in result["final_selected"]], ["600002"])
        self.assertFalse(result["final_selected"][0]["execution_ready"])
        self.assertEqual(result["final_selected"][0]["execution_mode"], "post_close_review")
        self.assertIn("次日开盘前", result["final_selected"][0]["reason"])
        self.assertEqual(result["run_session"]["mode"], "post_close_review")

    def test_scheduled_pipeline_records_execution_gate_items(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def gated_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            if asset["symbol"] == "600002":
                candidate["technical_metrics"]["tail_session"] = False
                candidate["technical_metrics"]["latest_minute_time"] = "13:20"
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=gated_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id, trigger_source="scheduled")

        self.assertEqual(result["final_selected"], [])
        self.assertEqual([item["symbol"] for item in result["excluded_by_execution_gate"]], ["600002"])
        self.assertEqual(result["excluded_by_execution_gate"][0]["execution_gate_type"], "tail_session")
        self.assertIn("未到尾盘执行时段", result["excluded_by_execution_gate"][0]["execution_gate_reason"])
        self.assertCountEqual([item["symbol"] for item in result["observe_candidates"]], ["600001", "600002"])

    def test_pipeline_applies_individual_risk_veto(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def risk_result(symbol, *_args, **_kwargs):
            if symbol == "600002":
                return {"vetoed": True, "risk_notes": ["近2天存在股东减持或减持公告"], "risk_level": "high"}
            return {"vetoed": False, "risk_notes": [], "risk_level": "low"}

        common_patches = self._run_with_common_patches(risk_side_effect=risk_result)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual(result["final_selected"], [])
        self.assertEqual([item["symbol"] for item in result["excluded_by_risk_veto"]], ["600002"])
        self.assertIn("个股风控否决", result["excluded_by_risk_veto"][0]["reason"])

    def test_pipeline_gates_high_distribution_risk_before_final_selection(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def distribution_risk_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            if asset["symbol"] == "600002":
                candidate["technical_metrics"]["distribution_risk"] = 82.0
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=distribution_risk_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual(result["final_selected"], [])
        self.assertEqual([item["symbol"] for item in result["excluded_by_execution_gate"]], ["600002"])
        self.assertEqual(result["excluded_by_execution_gate"][0]["execution_gate_type"], "distribution_risk")
        self.assertIn("派发风险过高", result["excluded_by_execution_gate"][0]["execution_gate_reason"])
        self.assertEqual(result["match_diagnostics"]["distribution_gated_count"], 1)

    def test_pipeline_gates_negative_news_context_before_final_selection(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        news_context = {
            "available": True,
            "fresh": True,
            "status": "fresh",
            "age_hours": 0.5,
            "snapshot": {"flow_level": "高", "total_score": 82},
            "sentiment": {"sentiment_index": 70, "sentiment_class": "乐观", "flow_stage": "加速"},
            "hot_topics": [{"topic": "机器人", "heat": 92, "count": 8}],
            "stock_news": [
                {
                    "title": "机器人二号收到监管函",
                    "content": "交易所问询相关事项",
                    "score": 80,
                    "cross_platform": 2,
                    "matched_keywords": ["机器人二号"],
                }
            ],
        }

        common_patches = self._run_with_common_patches()
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7], patch.object(
            self.service,
            "_load_news_flow_context",
            return_value=news_context,
        ):
            result = self.service._run_pipeline(run_id)

        self.assertEqual(result["final_selected"], [])
        self.assertEqual([item["symbol"] for item in result["excluded_by_execution_gate"]], ["600002"])
        gated_item = result["excluded_by_execution_gate"][0]
        self.assertEqual(gated_item["execution_gate_type"], "news_risk")
        self.assertTrue(gated_item["news_risk_flag"])
        self.assertIn("监管函", gated_item["news_negative_hits"][0]["title"])

    def test_execution_gate_degraded_freshness_is_soft_penalty_not_veto(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def degraded_freshness_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            if asset["symbol"] == "600002":
                candidate["technical_metrics"]["realtime_freshness"] = {
                    "intraday_decision_ready": False,
                    "overall_status": "degraded",
                    "minute_quality": {"status": "fair"},
                }
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=degraded_freshness_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual([item["symbol"] for item in result["final_selected"]], ["600002"])
        selected = result["final_selected"][0]
        self.assertGreater(selected["gate_soft_penalty"], 0)
        self.assertTrue(any(note["type"] == "realtime_freshness" for note in selected["execution_gate_notes"]))
        self.assertEqual(result["match_diagnostics"]["execution_gated_count"], 0)

    def test_execution_gate_low_adjusted_score_downgrades_to_observe(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def weak_execution_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            if asset["symbol"] == "600002":
                candidate["technical_metrics"].update(
                    {
                        "volume_score": 8.0,
                        "volume_contraction_days": 0,
                        "order_flow_score": 12.0,
                        "trend_score": 18.0,
                        "chip_score": 20.0,
                        "intraday_score": 12.0,
                        "intraday_bias": "selloff_pressure",
                    }
                )
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=weak_execution_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual(result["final_selected"], [])
        self.assertEqual([item["symbol"] for item in result["excluded_by_execution_gate"]], ["600002"])
        self.assertEqual(result["excluded_by_execution_gate"][0]["execution_gate_type"], "gate_score")
        self.assertIn("调整后执行分", result["excluded_by_execution_gate"][0]["execution_gate_reason"])

    def test_external_discovery_excludes_existing_research_pool_symbols(self):
        warnings = []

        def fake_query(**kwargs):
            return [
                {
                    "symbol": "600001",
                    "name": "算力一号",
                    "primary_sector": kwargs["sector"],
                    "score": 92.0,
                    "lifecycle_stage": kwargs["lifecycle_stage"],
                    "reason": "已有研究池标的",
                },
                {
                    "symbol": "600777",
                    "name": "外部龙头",
                    "primary_sector": kwargs["sector"],
                    "score": 88.0,
                    "lifecycle_stage": kwargs["lifecycle_stage"],
                    "reason": "研究池外发现",
                },
            ]

        with patch.object(self.service, "_query_external_sector_candidates", side_effect=fake_query):
            result = self.service._discover_external_candidates(
                selection_sectors=[
                    {
                        "sector": "算力租赁",
                        "canonical_sector": "算力租赁",
                        "lifecycle_stage": "startup",
                        "selection_veto": False,
                    }
                ],
                existing_symbols={"600001", "600002", "600003"},
                shortages=["研究池主线匹配不足 5 只"],
                warnings=warnings,
            )

        self.assertEqual([item["symbol"] for item in result], ["600777"])
        self.assertTrue(result[0]["external_discovery"])
        self.assertIn("未在研究池中", result[0]["reason"])
        self.assertTrue(any("研究池匹配不足" in warning for warning in warnings))

    def test_decay_candidates_are_not_silently_dropped(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        lifecycle_snapshot = [
            {
                "sector_name": "算力租赁",
                "heat_score": 82,
                "lifecycle_stage": "startup",
                "defense_line_type": "MA10",
                "selection_veto": False,
                "trajectory": [{"day_offset": -2, "score": 65}, {"day_offset": 0, "score": 82}],
                "delta_1": 5.0,
                "delta_2": 12.0,
                "action_hint": "启动期观察",
            },
            {
                "sector_name": "机器人",
                "heat_score": 95,
                "lifecycle_stage": "explosive",
                "defense_line_type": "MA5",
                "selection_veto": False,
                "trajectory": [{"day_offset": -2, "score": 80}, {"day_offset": 0, "score": 95}],
                "delta_1": 8.0,
                "delta_2": 15.0,
                "action_hint": "爆发期执行",
            },
            {
                "sector_name": "高位题材",
                "heat_score": 78,
                "lifecycle_stage": "decay",
                "defense_line_type": "NONE",
                "selection_veto": False,
                "trajectory": [{"day_offset": -2, "score": 92}, {"day_offset": 0, "score": 78}],
                "delta_1": -6.0,
                "delta_2": -14.0,
                "action_hint": "衰退期观察",
            },
        ]
        common_patches = self._run_with_common_patches()
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7], patch.object(
            self.service.sector_strategy_db,
            "get_lifecycle_items_for_analysis",
            return_value=lifecycle_snapshot,
        ):
            result = self.service._run_pipeline(run_id)

        self.assertEqual([item["symbol"] for item in result["observed_decay_candidates"]], ["600003"])
        self.assertEqual([item["symbol"] for item in result["excluded_by_lifecycle_veto"]], [])
        self.assertIn("保留观察", result["observed_decay_candidates"][0]["reason"])
        self.assertCountEqual([item["symbol"] for item in result["matched_candidates"]], ["600001", "600002", "600003"])

    def test_selection_sector_snapshot_supports_fuzzy_lifecycle_match(self):
        selection_sectors, lifecycle_by_name = self.service._build_selection_sector_snapshot(
            extracted_sectors=[{"sector": "通航", "heat_score": 88, "source": "heat.hottest"}],
            lifecycle_snapshot=[
                {
                    "sector_name": "通用航空",
                    "heat_score": 76,
                    "lifecycle_stage": "explosive",
                    "defense_line_type": "MA5",
                    "selection_veto": False,
                    "trajectory": [],
                    "delta_1": 4.0,
                    "delta_2": 9.0,
                    "action_hint": "爆发期执行",
                }
            ],
            warnings=[],
        )

        self.assertEqual(selection_sectors[0]["sector"], "通航")
        self.assertEqual(selection_sectors[0]["lifecycle_sector"], "通用航空")
        self.assertEqual(lifecycle_by_name["通航"]["sector_name"], "通用航空")

    def test_selection_sector_snapshot_ignores_non_tradeable_concept_boards(self):
        warnings = []
        selection_sectors, lifecycle_by_name = self.service._build_selection_sector_snapshot(
            extracted_sectors=[
                {"sector": "昨日连板", "heat_score": 99, "source": "heat.hottest"},
                {"sector": "季报预减", "heat_score": 96, "source": "heat.hottest"},
                {"sector": "机器人", "heat_score": 88, "source": "heat.hottest"},
            ],
            lifecycle_snapshot=[
                {
                    "sector_name": "昨日连板",
                    "heat_score": 99,
                    "lifecycle_stage": "explosive",
                    "selection_veto": False,
                },
                {
                    "sector_name": "季报预减",
                    "heat_score": 96,
                    "lifecycle_stage": "explosive",
                    "selection_veto": False,
                },
                {
                    "sector_name": "机器人",
                    "heat_score": 88,
                    "lifecycle_stage": "explosive",
                    "selection_veto": False,
                },
            ],
            warnings=warnings,
        )

        self.assertEqual([item["sector"] for item in selection_sectors], ["机器人"])
        self.assertIn("机器人", lifecycle_by_name)
        self.assertTrue(any("昨日连板" in warning for warning in warnings))
        self.assertTrue(any("季报预减" in warning for warning in warnings))

    def test_pipeline_tracks_match_diagnostics_and_watch_pool_requires_match_score(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)

        def partial_match_candidate(asset, context, lifecycle_snapshot):
            candidate = self._fake_candidate(asset, context, lifecycle_snapshot)
            if asset["symbol"] == "600001":
                candidate["match_score"] = 0.72
            return candidate

        common_patches = self._run_with_common_patches(score_side_effect=partial_match_candidate)
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertEqual(result["match_diagnostics"]["matched_candidate_count"], 2)
        self.assertEqual(result["match_diagnostics"]["lifecycle_veto_count"], 1)
        self.assertEqual(result["match_diagnostics"]["observe_candidate_count"], 1)
        self.assertEqual(self.service.list_watch_pool(), [])

    def test_pipeline_persists_daily_sector_heat_scores_for_lifecycle(self):
        run_id = self.service._insert_run(trigger_source="manual", lightweight_model=None, reasoning_model=None)
        common_patches = self._run_with_common_patches()
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], common_patches[4], common_patches[5], common_patches[6], common_patches[7]:
            result = self.service._run_pipeline(run_id)

        self.assertGreaterEqual(result.get("saved_sector_heat_count", 0), 3)

        conn = sqlite3.connect(str(Path(self.temp_dir.name) / "investment.db"))
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT board_date, sector_name, heat_score, lifecycle_stage, selection_veto
                FROM smart_selection_sector_heat_daily
                ORDER BY board_date ASC, rank_order ASC, id ASC
                """
            )
            rows = cursor.fetchall()
        finally:
            conn.close()

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], "2026-04-03")
        sector_names = [row[1] for row in rows]
        self.assertIn("机器人", sector_names)
        self.assertIn("算力租赁", sector_names)
        self.assertIn("高位题材", sector_names)
        lifecycle_by_sector = {row[1]: row[3] for row in rows}
        self.assertEqual(lifecycle_by_sector["机器人"], "explosive")
        self.assertEqual(lifecycle_by_sector["算力租赁"], "startup")
        self.assertEqual(lifecycle_by_sector["高位题材"], "decay")
        veto_by_sector = {row[1]: bool(row[4]) for row in rows}
        self.assertTrue(veto_by_sector["高位题材"])

    def test_backfill_sector_heat_daily_from_history_persists_existing_reports(self):
        result = self.service.backfill_sector_heat_daily_from_history()

        self.assertEqual(result["processed_reports"], 3)
        self.assertEqual(result["board_dates"], 3)
        self.assertGreaterEqual(result["saved_rows"], 9)

        conn = sqlite3.connect(str(Path(self.temp_dir.name) / "investment.db"))
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM smart_selection_sector_heat_daily
                """
            )
            total_rows = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT DISTINCT board_date
                FROM smart_selection_sector_heat_daily
                ORDER BY board_date ASC
                """
            )
            board_dates = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

        self.assertGreaterEqual(total_rows, 9)
        self.assertEqual(board_dates, ["2026-04-01", "2026-04-02", "2026-04-03"])


if __name__ == "__main__":
    unittest.main()
