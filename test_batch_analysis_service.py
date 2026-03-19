import sys
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


if __name__ == "__main__":
    unittest.main()
