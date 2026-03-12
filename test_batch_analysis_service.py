import sys
import types
import unittest
from unittest.mock import MagicMock, patch

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
    @patch("batch_analysis_service.db")
    @patch("batch_analysis_service.StockAnalysisAgents")
    @patch("batch_analysis_service.StockDataFetcher")
    @patch("batch_analysis_service._get_stock_data")
    def test_analyze_single_stock_for_batch_returns_record_id_when_saved(
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
        mock_fetcher.get_financial_data.return_value = {"roe": 30.0}
        mock_fetcher._is_chinese_stock.return_value = False
        mock_fetcher_cls.return_value = mock_fetcher

        mock_agents = MagicMock()
        mock_agents.run_multi_agent_analysis.return_value = {"technical": {"analysis": "趋势向上"}}
        mock_agents.conduct_team_discussion.return_value = "团队讨论结果"
        mock_agents.make_final_decision.return_value = {"rating": "买入", "confidence_level": 8.8}
        mock_agents_cls.return_value = mock_agents

        mock_db.save_analysis.return_value = 321

        result = batch_analysis_service.analyze_single_stock_for_batch(
            symbol="600519",
            period="1y",
            save_to_global_history=True,
        )

        self.assertTrue(result["success"])
        self.assertTrue(result["saved_to_db"])
        self.assertEqual(result["record_id"], 321)
        self.assertIsNone(result["db_error"])


if __name__ == "__main__":
    unittest.main()
