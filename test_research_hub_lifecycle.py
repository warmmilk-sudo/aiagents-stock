import os
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from analysis_repository import AnalysisRepository
from asset_repository import AssetRepository, STATUS_FOCUS, STATUS_HOLDING, STATUS_RESEARCH

if "openai" not in sys.modules:
    openai_stub = types.ModuleType("openai")
    openai_stub.OpenAI = lambda *args, **kwargs: Mock()
    openai_stub.APIConnectionError = RuntimeError
    openai_stub.APITimeoutError = RuntimeError
    openai_stub.InternalServerError = RuntimeError
    openai_stub.RateLimitError = RuntimeError
    openai_stub.APIStatusError = RuntimeError
    sys.modules["openai"] = openai_stub

import research_hub_service


class ResearchHubLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base = Path(self.temp_dir.name)
        self.original_cwd = os.getcwd()
        os.chdir(self.base)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    def test_assets_are_single_account_and_use_focus_holding_statuses(self):
        repo = AssetRepository(str(self.base / "investment.db"))

        first_id = repo.create_or_update_research_asset(
            symbol="600519",
            name="贵州茅台",
            account_name="账户A",
            note="首次研究",
        )
        second_id = repo.create_or_update_research_asset(
            symbol="600519",
            name="贵州茅台",
            account_name="账户B",
            note="再次研究",
        )

        self.assertEqual(first_id, second_id)
        asset = repo.get_asset(first_id)
        self.assertEqual(asset["status"], STATUS_RESEARCH)

        focus_id = repo.promote_to_watchlist(
            symbol="600519",
            name="贵州茅台",
            account_name="任意账户",
            note="晋级备选",
        )
        self.assertEqual(first_id, focus_id)
        focused = repo.get_asset(focus_id)
        self.assertEqual(focused["status"], STATUS_FOCUS)

        repo.transition_asset_status(
            focus_id,
            STATUS_HOLDING,
            cost_price=1500.0,
            quantity=100,
            note="建仓",
        )
        holding = repo.get_asset(focus_id)
        self.assertEqual(holding["status"], STATUS_HOLDING)
        self.assertEqual(holding["position_status"], "active")

    def test_legacy_sqlite_pool_data_is_backfilled_to_lifecycle_assets(self):
        db_path = self.base / "investment.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE portfolio_stocks (
                id INTEGER PRIMARY KEY,
                account_name TEXT,
                code TEXT,
                name TEXT,
                cost_price REAL,
                quantity INTEGER,
                note TEXT,
                auto_monitor INTEGER,
                position_status TEXT,
                origin_analysis_id INTEGER,
                last_trade_at TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO portfolio_stocks
            VALUES (6, '旧账户', '000001', '平安银行', 10.5, 300, '旧持仓', 1, 'active', 12, '2026-04-10', '2026-04-01', '2026-04-10')
            """
        )
        cursor.execute(
            """
            CREATE TABLE monitoring_items (
                id INTEGER PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                monitor_type TEXT,
                enabled INTEGER,
                account_name TEXT,
                portfolio_stock_id INTEGER,
                origin_analysis_id INTEGER,
                last_message TEXT,
                config_json TEXT,
                created_at TEXT,
                updated_at TEXT,
                asset_id INTEGER
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO monitoring_items
            VALUES (1, '000002', '万科A', 'price_alert', 1, '旧账户', NULL, 13, '旧关注', '{}', '2026-04-02', '2026-04-11', NULL)
            """
        )
        cursor.execute(
            """
            CREATE TABLE analysis_records (
                id INTEGER PRIMARY KEY,
                symbol TEXT,
                stock_name TEXT,
                account_name TEXT,
                portfolio_stock_id INTEGER,
                analysis_scope TEXT,
                analysis_date TEXT,
                summary TEXT,
                stock_info_json TEXT,
                created_at TEXT,
                asset_id INTEGER
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO analysis_records
            VALUES (21, '000003', '国农科技', '旧账户', NULL, 'research', '2026-04-12', '研究摘要', '{"industry":"农业"}', '2026-04-12', NULL)
            """
        )
        cursor.execute(
            """
            CREATE TABLE portfolio_trade_history (
                id INTEGER PRIMARY KEY,
                portfolio_stock_id INTEGER,
                trade_date TEXT,
                trade_type TEXT,
                price REAL,
                quantity INTEGER,
                note TEXT,
                trade_source TEXT,
                created_at TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO portfolio_trade_history
            VALUES (31, 6, '2026-04-10', 'buy', 10.5, 300, '建仓', 'manual', '2026-04-10')
            """
        )
        conn.commit()
        conn.close()

        repo = AssetRepository(str(db_path))
        holding = repo.get_asset_by_symbol("000001")
        focus = repo.get_asset_by_symbol("000002")
        research = repo.get_asset_by_symbol("000003")

        self.assertEqual(holding["status"], STATUS_HOLDING)
        self.assertEqual(holding["quantity"], 300)
        self.assertEqual(focus["status"], STATUS_FOCUS)
        self.assertEqual(research["status"], STATUS_RESEARCH)
        self.assertEqual(research["sector_tags"], ["农业"])

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        self.assertEqual(
            cursor.execute("SELECT asset_id FROM analysis_records WHERE id = 21").fetchone()["asset_id"],
            research["id"],
        )
        self.assertEqual(
            cursor.execute("SELECT asset_id FROM monitoring_items WHERE id = 1").fetchone()["asset_id"],
            focus["id"],
        )
        self.assertEqual(
            cursor.execute(
                "SELECT COUNT(*) AS count FROM asset_trade_history WHERE asset_id = ?",
                (holding["id"],),
            ).fetchone()["count"],
            1,
        )
        conn.close()

        ghost_id = repo.create_or_update_research_asset(symbol="000004", name="国华网安")
        repo.transition_asset_status(ghost_id, STATUS_HOLDING, cost_price=8.0, quantity=100)
        repo.backfill_lifecycle_data_from_legacy(force=True)
        ghost = repo.get_asset(ghost_id)
        self.assertEqual(ghost["status"], STATUS_HOLDING)
        self.assertEqual(ghost["quantity"], 100)

    def test_recent_sector_strategy_report_is_reused_within_12_hours(self):
        latest_report = {"id": 8, "analysis_date": "2026-04-15 10:00:00", "summary_data": {"headline": "最新热点"}}

        with patch("research_hub_service._get_latest_sector_strategy_report_light", return_value=latest_report), patch(
            "research_hub_service._get_sector_strategy_report_light",
            return_value=latest_report,
        ), patch("research_hub_service.datetime") as mock_datetime:
            from datetime import datetime

            mock_datetime.now.return_value = datetime(2026, 4, 15, 18, 0, 0)
            mock_datetime.strptime = datetime.strptime
            mock_datetime.fromisoformat = datetime.fromisoformat
            freshness = research_hub_service.get_recent_sector_strategy_report()
            self.assertTrue(freshness["fresh"])

            report = research_hub_service.ensure_recent_sector_strategy_report()
            self.assertTrue(report["reused"])
            self.assertEqual(report["report_id"], 8)

    def test_extract_selection_sectors_prefers_structured_payload(self):
        report = {
            "analysis_content_parsed": {
                "final_predictions": {
                    "heat": {
                        "hottest": [{"sector": "算力租赁", "score": 92, "trend": "升温"}],
                        "heating": [{"sector": "固态电池", "score": 81, "trend": "升温"}],
                    },
                    "long_short": {
                        "bullish": [{"sector": "半导体", "confidence": 7, "reason": "景气改善"}],
                    },
                    "rotation": {
                        "potential": [{"sector": "机器人", "logic": "轮动增强"}],
                    },
                }
            }
        }

        warnings: list[str] = []
        sectors = research_hub_service._extract_selection_sectors(report, warnings)

        self.assertEqual([item["sector"] for item in sectors[:3]], ["算力租赁", "固态电池", "半导体"])
        self.assertEqual(warnings, [])

    def test_extract_selection_sectors_uses_llm_fallback_when_structured_insufficient(self):
        report = {"summary_data": {"headline": "热点切换"}}
        llm_output = json_text = """
        [
          {"sector": "算力租赁", "heat_score": 90, "source": "llm", "reason": "热度最高"},
          {"sector": "机器人", "heat_score": 84, "source": "llm", "reason": "资金扩散"},
          {"sector": "半导体", "heat_score": 78, "source": "llm", "reason": "景气回升"}
        ]
        """

        with patch("research_hub_service._extract_selection_sectors_with_llm", return_value=research_hub_service._extract_selection_sector_items(llm_output)):
            warnings: list[str] = []
            sectors = research_hub_service._extract_selection_sectors(report, warnings)

        self.assertEqual([item["sector"] for item in sectors], ["算力租赁", "机器人", "半导体"])
        self.assertEqual(warnings, [])

    def test_match_asset_to_themes_supports_alias_family_and_hierarchy_scores(self):
        context = {
            "tags": ["共封装光学(CPO)", "电子-元件-印制电路板", "风电"],
            "haystack": "公司业务聚焦光模块和风电零部件",
        }
        extracted = [
            {"sector": "CPO概念", "canonical_sector": "CPO概念", "aliases": ["共封装光学(CPO)", "光通信模块"], "heat_score": 90},
            {"sector": "印制电路板", "canonical_sector": "印制电路板", "aliases": ["PCB概念", "电子-元件-印制电路板"], "heat_score": 80},
            {"sector": "风力发电", "canonical_sector": "风力发电", "aliases": ["风电"], "heat_score": 78},
        ]

        matches = research_hub_service._match_asset_to_themes({}, context, extracted)
        by_sector = {item["sector"]: item for item in matches}

        self.assertEqual(by_sector["CPO概念"]["match_score"], 1.0)
        self.assertEqual(by_sector["印制电路板"]["match_score"], 1.0)
        self.assertEqual(by_sector["风力发电"]["match_score"], 1.0)

    def test_generic_theme_requires_llm_review_before_matching(self):
        context = {"tags": ["低估值"], "haystack": "公司暂无超跌股标签"}
        extracted = [
            {"sector": "超跌股", "canonical_sector": "超跌股", "aliases": ["超跌反弹"], "heat_score": 68, "generic_theme": True},
        ]

        with patch("research_hub_service._review_sector_alias_with_llm", return_value={"is_same_theme": False, "match_score": 0.0}):
            matches = research_hub_service._match_asset_to_themes({}, context, extracted)

        self.assertEqual(matches, [])

    def test_market_context_classifies_momentum_and_ice_states(self):
        momentum_report = {
            "analysis_content_parsed": {
                "data_summary": {
                    "market_overview": {
                        "up_ratio": 78.0,
                        "limit_up": 110,
                        "limit_down": 3,
                        "sh_index": {"change_pct": 0.8},
                        "sz_index": {"change_pct": 1.9},
                        "cyb_index": {"change_pct": 2.8},
                    }
                }
            },
            "summary_data": {"market_outlook": "偏强"},
        }
        ice_report = {
            "analysis_content_parsed": {
                "data_summary": {
                    "market_overview": {
                        "up_ratio": 25.0,
                        "limit_up": 8,
                        "limit_down": 24,
                        "sh_index": {"change_pct": -1.3},
                        "sz_index": {"change_pct": -2.2},
                        "cyb_index": {"change_pct": -3.1},
                    }
                }
            },
            "summary_data": {"market_outlook": "谨慎"},
        }

        momentum_context = research_hub_service._build_selection_market_context(momentum_report)
        ice_context = research_hub_service._build_selection_market_context(ice_report)

        self.assertEqual(momentum_context["state"], research_hub_service.SELECTION_MARKET_STATE_MOMO)
        self.assertEqual(ice_context["state"], research_hub_service.SELECTION_MARKET_STATE_ICE)

    def test_volume_price_agent_reweights_same_features_by_market_regime(self):
        asset = {"id": 1, "symbol": "600001", "name": "测试股"}
        matches = [{"sector": "算力租赁", "heat_score": 90}]
        bundle = {
            "stock_info": {"current_price": 10.5, "turnover_rate": 8.5, "market_cap": 100.0},
            "realtime_quote": {"order_book": {"bids": [{"price": 10.5, "volume": 2000}], "asks": [{"price": 10.51, "volume": 800}]}},
            "order_book": {"bids": [{"price": 10.5, "volume": 2000}], "asks": [{"price": 10.51, "volume": 800}]},
            "indicators": {
                "price": 10.5,
                "ma5": 10.2,
                "ma20": 9.8,
                "ma60": 9.1,
                "rsi": 66,
                "macd": 0.5,
                "macd_signal": 0.2,
                "volume_ratio": 1.8,
                "main_chip_peak_price": 10.0,
                "average_chip_cost": 9.9,
                "chip_concentration": "高 (52.0%)",
            },
            "stock_data": __import__("pandas").DataFrame(
                {
                    "Open": [8.8, 9.1, 9.4, 9.8, 10.0, 10.2],
                    "High": [9.0, 9.3, 9.7, 10.0, 10.4, 10.6],
                    "Low": [8.7, 9.0, 9.3, 9.7, 9.9, 10.1],
                    "Close": [8.9, 9.2, 9.6, 9.95, 10.25, 10.5],
                    "Volume": [100, 120, 130, 150, 180, 230],
                    "Volume_MA5": [100, 110, 115, 120, 136, 162],
                }
            ),
        }
        intraday_context = {
            "intraday_bias": "trend_continuation",
            "intraday_bias_text": "高位放量延续",
            "last_30m_change_pct": 1.2,
            "last_60m_change_pct": 1.6,
            "volume_acceleration_ratio": 1.35,
            "intraday_vwap": 10.2,
            "latest_minute_time": "14:56",
        }
        main_force_flow = {"main_net_pct": 18.0, "main_net": 3200.0, "super_net": 1800.0, "big_net": 900.0}

        with patch("research_hub_service._get_selection_intraday_context", return_value=intraday_context), patch(
            "research_hub_service._get_selection_main_force_flow",
            return_value=main_force_flow,
        ):
            momentum_result = research_hub_service._run_volume_price_resonance_agent(
                asset=asset,
                matches=matches,
                bundle=bundle,
                market_context={"state": research_hub_service.SELECTION_MARKET_STATE_MOMO, "state_label": "主升浪", "profile": research_hub_service.SELECTION_AGENT_PROFILES[research_hub_service.SELECTION_MARKET_STATE_MOMO]},
            )
            ice_result = research_hub_service._run_volume_price_resonance_agent(
                asset=asset,
                matches=matches,
                bundle=bundle,
                market_context={"state": research_hub_service.SELECTION_MARKET_STATE_ICE, "state_label": "冰点期", "profile": research_hub_service.SELECTION_AGENT_PROFILES[research_hub_service.SELECTION_MARKET_STATE_ICE]},
            )

        self.assertGreater(momentum_result["agent_score"], ice_result["agent_score"])
        self.assertGreater(momentum_result["component_scores"]["breakout"], 60)

    def test_selection_pipeline_filters_non_a_share_and_persists_focus_changes(self):
        db_path = self.base / "investment.db"
        repo = AssetRepository(str(db_path))
        analysis_repo = AnalysisRepository(str(db_path))

        compute_id = repo.create_or_update_research_asset(symbol="600001", name="算力一号", note="研究算力")
        repo.update_asset(compute_id, sector_tags_json=["算力租赁"])
        battery_id = repo.create_or_update_research_asset(symbol="600002", name="电池二号", note="研究电池")
        repo.update_asset(battery_id, sector_tags_json=["固态电池"])
        hk_id = repo.create_or_update_research_asset(symbol="02382", name="港股标的", note="港股")
        repo.update_asset(hk_id, sector_tags_json=["算力租赁"])
        focus_id = repo.promote_to_watchlist(symbol="600010", name="手动加星", note="保留")
        repo.update_asset(focus_id, manual_pin=True)
        stale_focus_id = repo.promote_to_watchlist(symbol="600011", name="旧备选", note="待淘汰")

        report = {
            "id": 9,
            "analysis_date": "2026-04-16 18:00:00",
            "analysis_content_parsed": {
                "final_predictions": {
                    "heat": {
                        "hottest": [{"sector": "算力租赁", "score": 90, "trend": "升温"}],
                        "heating": [{"sector": "固态电池", "score": 75, "trend": "升温"}],
                    },
                    "long_short": {"bullish": [{"sector": "机器人", "confidence": 6, "reason": "扩散"}]},
                    "rotation": {"potential": []},
                }
            },
            "summary_data": {"headline": "热点轮动"},
        }

        fake_memory_service = Mock()
        fake_memory_service.db.save_working_memory = Mock()

        def fake_score(asset, context, extracted_sectors):
            if asset["symbol"] == "600001":
                return {
                    "asset_id": asset["id"],
                    "symbol": asset["symbol"],
                    "name": asset["name"],
                    "matched_sectors": [extracted_sectors[0]],
                    "primary_sector": "算力租赁",
                    "heat_score": 90.0,
                    "tech_score": 82.0,
                    "composite_score": 172.0,
                    "technical_metrics": {"rsi": 62, "volume_ratio": 1.5},
                    "reason": "板块主线 算力租赁；技术分 82.0；RSI 62；量比 1.5",
                    "market_cap": 100.0,
                    "asset": asset,
                }
            if asset["symbol"] == "600002":
                return {
                    "asset_id": asset["id"],
                    "symbol": asset["symbol"],
                    "name": asset["name"],
                    "matched_sectors": [extracted_sectors[1]],
                    "primary_sector": "固态电池",
                    "heat_score": 75.0,
                    "tech_score": 79.0,
                    "composite_score": 154.0,
                    "technical_metrics": {"rsi": 59, "volume_ratio": 1.3},
                    "reason": "板块主线 固态电池；技术分 79.0；RSI 59；量比 1.3",
                    "market_cap": 120.0,
                    "asset": asset,
                }
            return {}

        with patch("research_hub_service.asset_repository", repo), patch(
            "research_hub_service.analysis_repository",
            analysis_repo,
        ), patch("research_hub_service.ensure_recent_sector_strategy_report", return_value={"reused": True, "task_id": None, "report_id": 9, "report": report}), patch(
            "research_hub_service.asset_service.sync_managed_monitors",
            return_value=None,
        ), patch("research_hub_service._get_agent_memory_service", return_value=fake_memory_service), patch(
            "research_hub_service._score_selection_candidate",
            side_effect=fake_score,
        ), patch(
            "research_hub_service._review_selection_candidates_with_llm",
            return_value={},
        ):
            result = research_hub_service._run_selection_pipeline(lambda **_: None)

        self.assertEqual([item["sector"] for item in result["extracted_sectors"][:2]], ["算力租赁", "固态电池"])
        self.assertCountEqual([item["symbol"] for item in result["mapped_candidates"]], ["600001", "600002"])
        self.assertNotIn("02382", [item["symbol"] for item in result["mapped_candidates"]])
        self.assertIn("600010", [item["symbol"] for item in result["kept_manual_pins"]])
        self.assertIn("600001", [item["symbol"] for item in result["final_selected"]])
        self.assertIn("600002", [item["symbol"] for item in result["final_selected"]])
        self.assertIn("600011", [item["symbol"] for item in result["demoted"]])
        self.assertEqual(repo.get_asset(compute_id)["status"], STATUS_FOCUS)
        self.assertEqual(repo.get_asset(stale_focus_id)["status"], STATUS_RESEARCH)

    def test_submit_selection_run_passes_model_preferences_to_pipeline(self):
        with patch("research_hub_service.start_ui_analysis_task", return_value="task-1") as start_task, patch(
            "research_hub_service._run_selection_pipeline",
            return_value={"ok": True},
        ) as run_pipeline:
            task_id = research_hub_service.submit_selection_run(
                lightweight_model="deepseek-chat",
                reasoning_model="deepseek-reasoner",
            )

            self.assertEqual(task_id, "task-1")
            runner = start_task.call_args.kwargs["runner"]
            runner("task-1", lambda **_: None)

        run_pipeline.assert_called_once()
        self.assertEqual(run_pipeline.call_args.kwargs["lightweight_model"], "deepseek-chat")
        self.assertEqual(run_pipeline.call_args.kwargs["reasoning_model"], "deepseek-reasoner")

    def test_list_hub_assets_backfills_display_name_and_concept_tags_for_cards(self):
        repo = AssetRepository(str(self.base / "investment.db"))
        analysis_repo = AnalysisRepository(str(self.base / "investment.db"))
        asset_id = repo.create_or_update_research_asset(
            symbol="600519",
            name="600519",
            note="测试标的",
        )
        analysis_repo.save_record(
            symbol="600519",
            stock_name="600519",
            period="1y",
            stock_info={"symbol": "600519", "industry": "食品饮料"},
            agents_results={"summary": "ok"},
            discussion_result="ok",
            final_decision={"rating": "买入", "operation_advice": "继续跟踪"},
        )

        fake_data_source_manager = Mock()
        fake_data_source_manager.get_stock_basic_info.return_value = {
            "symbol": "600519",
            "name": "贵州茅台",
            "industry": "食品饮料",
            "sector_tags": ["白酒", "高端消费", "沪深300"],
        }

        fake_data_source_module = types.ModuleType("data_source_manager")
        fake_data_source_module.data_source_manager = fake_data_source_manager

        with patch("research_hub_service.asset_repository", repo), patch(
            "research_hub_service.analysis_repository",
            analysis_repo,
        ), patch("research_hub_service._list_ai_decisions", return_value=[]), patch.dict(
            sys.modules,
            {"data_source_manager": fake_data_source_module},
        ):
            items = research_hub_service.list_hub_assets()

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["name"], "贵州茅台")
        self.assertEqual(items[0]["primary_industry"], "食品饮料")
        self.assertIn("白酒", items[0]["core_concepts"])
        self.assertIn("高端消费", items[0]["display_tags"])

        refreshed = repo.get_asset(asset_id)
        self.assertEqual(refreshed["name"], "贵州茅台")
        self.assertIn("白酒", refreshed["sector_tags"])

    def test_hub_overview_degrades_when_sector_report_loading_fails(self):
        repo = AssetRepository(str(self.base / "investment.db"))
        analysis_repo = AnalysisRepository(str(self.base / "investment.db"))
        repo.create_or_update_research_asset(symbol="600519", name="贵州茅台", note="测试概览")
        research_hub_service.invalidate_hub_cache("overview", "assets:", "sector-report:recent")

        with patch("research_hub_service.asset_repository", repo), patch(
            "research_hub_service.analysis_repository",
            analysis_repo,
        ), patch("research_hub_service._list_ai_decisions", return_value=[]), patch(
            "research_hub_service.get_recent_sector_strategy_report",
            side_effect=RuntimeError("sector report unavailable"),
        ):
            overview = research_hub_service.get_hub_overview()

        self.assertEqual(overview["counts"]["research"], 1)
        self.assertFalse(overview["sector_report"]["available"])
        self.assertIn("读取失败", overview["sector_report_warning"])

    def test_asset_match_context_limits_core_concepts_for_selection_matching(self):
        repo = AssetRepository(str(self.base / "investment.db"))
        asset_id = repo.create_or_update_research_asset(symbol="600123", name="概念过多示例", note="概念测试")
        repo.update_asset(
            asset_id,
            sector_tags_json=["通信", "算力租赁", "机器人", "固态电池", "半导体设备"],
        )
        asset = repo.get_asset(asset_id)

        with patch("research_hub_service.asset_repository", repo), patch(
            "research_hub_service.analysis_repository.get_latest_strategy_context",
            return_value={},
        ), patch(
            "research_hub_service._get_latest_analysis_record",
            return_value={},
        ):
            context = research_hub_service._collect_asset_match_context(asset, [])
            matches = research_hub_service._match_asset_to_themes(
                asset,
                context,
                [
                    {"sector": "半导体设备", "heat_score": 90},
                    {"sector": "机器人", "heat_score": 88},
                    {"sector": "固态电池", "heat_score": 85},
                ],
            )

        self.assertEqual(context["raw_tags"], ["通信", "算力租赁", "机器人", "固态电池", "半导体设备"])
        self.assertEqual(context["tags"], ["通信", "算力租赁", "机器人", "固态电池"])
        self.assertEqual(context["core_concepts"], ["算力租赁", "机器人", "固态电池"])
        self.assertNotIn("半导体设备", context["tags"])
        self.assertEqual({item["sector"] for item in matches}, {"机器人", "固态电池"})

    def test_research_pool_cards_prioritize_most_recent_analysis(self):
        repo = AssetRepository(str(self.base / "investment.db"))
        analysis_repo = AnalysisRepository(str(self.base / "investment.db"))

        older_asset_id = repo.create_or_update_research_asset(symbol="600001", name="旧分析卡")
        newer_asset_id = repo.create_or_update_research_asset(symbol="600002", name="新分析卡")
        repo.update_asset(older_asset_id, last_funnel_score=99.0)
        repo.update_asset(newer_asset_id, last_funnel_score=10.0)

        with patch("asset_repository.asset_repository", repo), patch("research_hub_service.asset_repository", repo), patch(
            "research_hub_service.analysis_repository",
            analysis_repo,
        ), patch("research_hub_service._list_ai_decisions", return_value=[]):
            analysis_repo.save_record(
                symbol="600001",
                stock_name="旧分析卡",
                period="1y",
                asset_id=older_asset_id,
                stock_info={"industry": "旧行业"},
                agents_results={"summary": "旧分析"},
                discussion_result="旧分析",
                final_decision={"rating": "买入"},
                summary="旧分析",
                analysis_date="2026-04-10 10:00:00",
                has_full_report=True,
            )
            analysis_repo.save_record(
                symbol="600002",
                stock_name="新分析卡",
                period="1y",
                asset_id=newer_asset_id,
                stock_info={"industry": "新行业"},
                agents_results={"summary": "新分析"},
                discussion_result="新分析",
                final_decision={"rating": "买入"},
                summary="新分析",
                analysis_date="2026-04-18 10:00:00",
                has_full_report=True,
            )

            items = research_hub_service.list_hub_assets(pool=STATUS_RESEARCH)

        self.assertGreaterEqual(len(items), 2)
        self.assertEqual([item["symbol"] for item in items[:2]], ["600002", "600001"])

    def test_delete_hub_asset_soft_deletes_research_card(self):
        repo = AssetRepository(str(self.base / "investment.db"))
        analysis_repo = AnalysisRepository(str(self.base / "investment.db"))
        asset_id = repo.create_or_update_research_asset(symbol="600003", name="待删除卡")

        fake_asset_service = Mock()
        fake_asset_service.sync_managed_monitors.return_value = {"removed": 0}

        with patch("research_hub_service.asset_repository", repo), patch(
            "research_hub_service.analysis_repository",
            analysis_repo,
        ), patch("research_hub_service.asset_service", fake_asset_service), patch(
            "research_hub_service._list_ai_decisions",
            return_value=[],
        ):
            deleted = research_hub_service.delete_hub_asset(asset_id)
            items = research_hub_service.list_hub_assets(pool=STATUS_RESEARCH)

        self.assertTrue(deleted)
        self.assertIsNone(repo.get_asset(asset_id))
        self.assertNotIn("600003", [item["symbol"] for item in items])


if __name__ == "__main__":
    unittest.main()
