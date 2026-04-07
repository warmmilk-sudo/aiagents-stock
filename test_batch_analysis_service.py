import sys
import time
import types
import unittest
from unittest.mock import MagicMock, patch

sys.modules.setdefault(
    "pandas",
    types.SimpleNamespace(
        DataFrame=type("DataFrame", (), {}),
        Series=type("Series", (), {}),
        Timestamp=type("Timestamp", (), {}),
        isna=lambda value: False,
        to_datetime=lambda value, *args, **kwargs: value,
        to_numeric=lambda value, *args, **kwargs: value,
        bdate_range=lambda *args, **kwargs: [],
        date_range=lambda *args, **kwargs: [],
        concat=lambda *args, **kwargs: None,
    ),
)
sys.modules.setdefault(
    "stock_data",
    types.SimpleNamespace(StockDataFetcher=type("StockDataFetcher", (), {})),
)
sys.modules.setdefault(
    "ai_agents",
    types.SimpleNamespace(StockAnalysisAgents=type("StockAnalysisAgents", (), {})),
)
stock_data_cache_stub = types.ModuleType("stock_data_cache")
stock_data_cache_stub.strip_cache_meta = lambda value: value
sys.modules.setdefault("stock_data_cache", stock_data_cache_stub)

import batch_analysis_service


class BatchAnalysisServiceTests(unittest.TestCase):
    @patch("batch_analysis_service.StockDataFetcher")
    def test_get_stock_data_prefers_realtime_quote_price(self, mock_fetcher_cls):
        mock_fetcher = MagicMock()
        mock_fetcher.get_stock_info.return_value = {
            "symbol": "600519",
            "name": "贵州茅台",
            "current_price": 12.34,
        }
        mock_fetcher.get_realtime_quote.return_value = {
            "current_price": 18.88,
            "change_percent": 1.2,
            "data_source": "tdx",
        }
        mock_fetcher.get_stock_data.return_value = [{"close": 18.88}]
        mock_fetcher.calculate_technical_indicators.side_effect = lambda data: data
        mock_fetcher.get_latest_indicators.return_value = {"rsi": 55.0}
        mock_fetcher_cls.return_value = mock_fetcher

        stock_info, stock_data, indicators = batch_analysis_service._get_stock_data("600519", "1y")

        self.assertEqual(stock_info["current_price"], 18.88)
        self.assertEqual(stock_info["realtime_data_source"], "tdx")
        self.assertEqual(stock_data, [{"close": 18.88}])
        self.assertEqual(indicators, {"rsi": 55.0})

    @patch("batch_analysis_service.StockDataFetcher")
    def test_get_stock_data_strips_stale_price_when_realtime_quote_missing(self, mock_fetcher_cls):
        mock_fetcher = MagicMock()
        mock_fetcher.get_stock_info.return_value = {
            "symbol": "600519",
            "name": "贵州茅台",
            "current_price": 12.34,
            "change_percent": 1.2,
        }
        mock_fetcher.get_realtime_quote.return_value = None
        mock_fetcher.get_stock_data.return_value = [{"close": 18.88}]
        mock_fetcher.calculate_technical_indicators.side_effect = lambda data: data
        mock_fetcher.get_latest_indicators.return_value = {"rsi": 55.0}
        mock_fetcher_cls.return_value = mock_fetcher

        stock_info, stock_data, indicators = batch_analysis_service._get_stock_data("600519", "1y")

        self.assertNotIn("current_price", stock_info)
        self.assertNotIn("change_percent", stock_info)
        self.assertNotIn("realtime_data_source", stock_info)
        self.assertEqual(stock_data, [{"close": 18.88}])
        self.assertEqual(indicators, {"rsi": 55.0})

    @patch("batch_analysis_service.db")
    @patch("batch_analysis_service.asset_service")
    @patch("batch_analysis_service.StockAnalysisAgents")
    @patch("batch_analysis_service.StockDataFetcher")
    @patch("batch_analysis_service._get_stock_data")
    def test_analyze_single_stock_for_batch_returns_record_id_when_saved(
        self,
        mock_get_stock_data,
        mock_fetcher_cls,
        mock_agents_cls,
        mock_asset_service,
        mock_db,
    ):
        mock_get_stock_data.return_value = (
            {"symbol": "600519", "name": "贵州茅台"},
            [{"close": 1688.0}],
            {"rsi": 55.0},
        )

        mock_fetcher = MagicMock()
        mock_fetcher.get_financial_data.return_value = {"roe": 30.0}
        mock_fetcher._is_chinese_stock.return_value = False
        mock_fetcher_cls.return_value = mock_fetcher

        mock_agents = MagicMock()
        mock_agents.run_multi_agent_analysis.return_value = {"technical": {"analysis": "趋势向上"}}
        mock_agents.conduct_team_discussion.return_value = "团队讨论结果"
        mock_agents.make_final_decision.return_value = {"rating": "买入", "confidence_level": 8.8}
        mock_agents_cls.return_value = mock_agents

        mock_db.save_analysis.return_value = 321
        mock_asset_service.sync_managed_monitors_for_symbol.return_value = {
            "ai_tasks_upserted": 1,
            "price_alerts_upserted": 1,
            "removed": 0,
        }

        result = batch_analysis_service.analyze_single_stock_for_batch(
            symbol="600519",
            period="1y",
            save_to_global_history=True,
        )

        self.assertTrue(result["success"])
        self.assertTrue(result["saved_to_db"])
        self.assertEqual(result["record_id"], 321)
        self.assertIsNone(result["db_error"])
        mock_asset_service.sync_managed_monitors_for_symbol.assert_called_once_with("600519")

    def test_analyze_single_stock_for_batch_rejects_empty_analyst_config(self):
        result = batch_analysis_service.analyze_single_stock_for_batch(
            symbol="600519",
            period="1y",
            enabled_analysts_config={
                "technical": False,
                "fundamental": False,
                "fund_flow": False,
                "risk": False,
                "sentiment": False,
                "news": False,
            },
        )

        self.assertFalse(result["success"])
        self.assertIn("请至少选择一位分析师", result["error"])

    @patch("batch_analysis_service.StockAnalysisAgents")
    @patch("batch_analysis_service.StockDataFetcher")
    @patch("batch_analysis_service._get_stock_data")
    def test_analyze_single_stock_for_batch_skips_financial_fetch_when_fundamental_disabled(
        self,
        mock_get_stock_data,
        mock_fetcher_cls,
        mock_agents_cls,
    ):
        mock_get_stock_data.return_value = (
            {"symbol": "600519", "name": "贵州茅台"},
            [{"close": 1688.0}],
            {"rsi": 55.0},
        )

        mock_fetcher = MagicMock()
        mock_fetcher._is_chinese_stock.return_value = False
        mock_fetcher_cls.return_value = mock_fetcher

        mock_agents = MagicMock()
        mock_agents.run_multi_agent_analysis.return_value = {"technical": {"analysis": "趋势向上"}}
        mock_agents.conduct_team_discussion.return_value = "团队讨论结果"
        mock_agents.make_final_decision.return_value = {"rating": "买入", "confidence_level": 8.8}
        mock_agents_cls.return_value = mock_agents

        result = batch_analysis_service.analyze_single_stock_for_batch(
            symbol="600519",
            period="1y",
            enabled_analysts_config={
                "technical": True,
                "fundamental": False,
                "fund_flow": False,
                "risk": False,
                "sentiment": False,
                "news": False,
            },
            save_to_global_history=False,
        )

        self.assertTrue(result["success"])
        mock_fetcher.get_financial_data.assert_not_called()

    @patch("batch_analysis_service.db")
    @patch("batch_analysis_service.StockAnalysisAgents")
    @patch("batch_analysis_service.StockDataFetcher")
    @patch("batch_analysis_service._get_stock_data")
    def test_analyze_single_stock_for_batch_fails_when_agent_pipeline_raises(
        self,
        mock_get_stock_data,
        mock_fetcher_cls,
        mock_agents_cls,
        mock_db,
    ):
        mock_get_stock_data.return_value = (
            {"symbol": "600519", "name": "贵州茅台"},
            [{"close": 1688.0}],
            {"rsi": 55.0},
        )

        mock_fetcher = MagicMock()
        mock_fetcher._is_chinese_stock.return_value = False
        mock_fetcher.get_financial_data.return_value = {"roe": 30.0}
        mock_fetcher_cls.return_value = mock_fetcher

        mock_agents = MagicMock()
        mock_agents.run_multi_agent_analysis.side_effect = RuntimeError("DeepSeek API调用失败")
        mock_agents_cls.return_value = mock_agents

        result = batch_analysis_service.analyze_single_stock_for_batch(
            symbol="600519",
            period="1y",
            save_to_global_history=True,
        )

        self.assertFalse(result["success"])
        self.assertIn("DeepSeek API调用失败", result["error"])
        mock_db.save_analysis.assert_not_called()

    @patch("batch_analysis_service.StockDataFetcher")
    def test_collect_optional_context_data_runs_fetches_in_parallel(self, mock_fetcher_cls):
        mock_fetcher = MagicMock()
        mock_fetcher._is_chinese_stock.return_value = True
        mock_fetcher_cls.return_value = mock_fetcher

        def sleep_and_return(value):
            def _inner(*args, **kwargs):
                time.sleep(0.2)
                return value
            return _inner

        quarterly_module = types.SimpleNamespace(
            QuarterlyReportDataFetcher=lambda: types.SimpleNamespace(
                get_quarterly_reports=sleep_and_return({"quarterly": True})
            )
        )
        fund_flow_module = types.SimpleNamespace(
            FundFlowAkshareDataFetcher=lambda: types.SimpleNamespace(
                get_fund_flow_data=sleep_and_return({"fund_flow": True})
            )
        )
        risk_fetcher = MagicMock()
        risk_fetcher.get_financial_data = sleep_and_return({"financial": True})
        risk_fetcher.get_risk_data = sleep_and_return({"risk": True})

        mock_fetcher_cls.side_effect = [mock_fetcher, risk_fetcher, risk_fetcher]

        started_at = time.perf_counter()
        with patch.dict(
            sys.modules,
            {
                "quarterly_report_data": quarterly_module,
                "fund_flow_akshare": fund_flow_module,
            },
        ):
            result = batch_analysis_service._collect_optional_context_data(
                "600519",
                stock_data=[{"close": 1}],
                enabled_analysts_config={
                    "technical": True,
                    "fundamental": True,
                    "fund_flow": True,
                    "risk": True,
                    "sentiment": False,
                    "news": False,
                },
            )
        elapsed = time.perf_counter() - started_at

        self.assertLess(elapsed, 0.45)
        self.assertEqual(result["financial_data"], {"financial": True})
        self.assertEqual(result["quarterly_data"], {"quarterly": True})
        self.assertEqual(result["fund_flow_data"], {"fund_flow": True})
        self.assertEqual(result["risk_data"], {"risk": True})

    @patch("batch_analysis_service.StockAnalysisAgents")
    @patch("batch_analysis_service.StockDataFetcher")
    @patch("batch_analysis_service._get_stock_data")
    def test_analyze_single_stock_for_batch_reports_stage_progress(
        self,
        mock_get_stock_data,
        mock_fetcher_cls,
        mock_agents_cls,
    ):
        mock_get_stock_data.return_value = (
            {"symbol": "600519", "name": "贵州茅台"},
            [{"close": 1688.0}],
            {"rsi": 55.0},
        )

        mock_fetcher = MagicMock()
        mock_fetcher._is_chinese_stock.return_value = False
        mock_fetcher_cls.return_value = mock_fetcher

        mock_agents = MagicMock()
        mock_agents.run_multi_agent_analysis.return_value = {"technical": {"analysis": "趋势向上"}}
        mock_agents.conduct_team_discussion.return_value = "团队讨论结果"
        mock_agents.make_final_decision.return_value = {"rating": "买入"}
        mock_agents_cls.return_value = mock_agents

        progress_events = []

        result = batch_analysis_service.analyze_single_stock_for_batch(
            symbol="600519",
            period="1y",
            save_to_global_history=False,
            progress_callback=lambda current, total, message: progress_events.append((current, total, message)),
        )

        self.assertTrue(result["success"])
        self.assertEqual(
            progress_events,
            [
                (5, 100, "正在获取 600519 的分析数据..."),
                (25, 100, "AI 分析师团队正在分析 600519..."),
                (75, 100, "AI 团队正在讨论 600519 的综合结论..."),
                (90, 100, "正在生成 600519 的最终决策..."),
            ],
        )


if __name__ == "__main__":
    unittest.main()
