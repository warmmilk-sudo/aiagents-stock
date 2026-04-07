import importlib
import sys
import types
import unittest

import pandas as pd


class _FakeTushareApi:
    def index_daily(self, **kwargs):
        return pd.DataFrame(
            [
                {"trade_date": "20260403", "pct_chg": -0.99},
            ]
        )

    def limit_list_d(self, **kwargs):
        if kwargs.get("limit_type") == "U":
            return pd.DataFrame([{"ts_code": "000001.SZ"}, {"ts_code": "000002.SZ"}])
        if kwargs.get("limit_type") == "D":
            return pd.DataFrame([{"ts_code": "000004.SZ"}])
        return pd.DataFrame()

    def margin_detail(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260403",
                    "rzye": 1200000.0,
                    "rqye": 50000.0,
                    "rzmre": 180000.0,
                }
            ]
        )

    def daily_basic(self, **kwargs):
        return pd.DataFrame(
            [
                {"trade_date": "20260403", "turnover_rate": 2.5},
            ]
        )


class _FakeDataSourceManager:
    tushare_available = True
    tushare_api = _FakeTushareApi()

    @staticmethod
    def _convert_to_ts_code(symbol):
        return f"{symbol}.SH"


class MarketSentimentTusharePriorityTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {
            name: sys.modules.get(name)
            for name in ["akshare", "data_source_manager", "market_sentiment_data"]
        }
        sys.modules["akshare"] = types.SimpleNamespace(
            stock_zh_a_spot_em=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("akshare unavailable")),
            stock_zh_index_spot_em=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("akshare unavailable")),
            stock_zt_pool_em=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("akshare unavailable")),
            stock_zt_pool_dtgc_em=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("akshare unavailable")),
            stock_margin_szsh=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("akshare unavailable")),
        )
        sys.modules["data_source_manager"] = types.SimpleNamespace(
            data_source_manager=_FakeDataSourceManager()
        )
        sys.modules.pop("market_sentiment_data", None)
        self.module = importlib.import_module("market_sentiment_data")

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_market_index_uses_tushare_when_available(self):
        fetcher = self.module.MarketSentimentDataFetcher()
        result = fetcher._get_market_index_sentiment()
        self.assertEqual(result["index_name"], "上证指数")
        self.assertEqual(result["change_percent"], -0.99)

    def test_limit_up_down_uses_tushare_when_available(self):
        fetcher = self.module.MarketSentimentDataFetcher()
        result = fetcher._get_limit_up_down_stats()
        self.assertEqual(result["limit_up_count"], 2)
        self.assertEqual(result["limit_down_count"], 1)
        self.assertEqual(result["limit_ratio"], "66.7%")

    def test_margin_trading_uses_tushare_when_available(self):
        fetcher = self.module.MarketSentimentDataFetcher()
        result = fetcher._get_margin_trading_data("600905")
        self.assertEqual(result["margin_balance"], 1200000.0)
        self.assertEqual(result["short_balance"], 50000.0)
        self.assertEqual(result["margin_buy"], 180000.0)

    def test_fear_greed_can_be_computed_without_akshare_snapshot(self):
        fetcher = self.module.MarketSentimentDataFetcher()
        result = fetcher._get_fear_greed_index(
            market_index={"change_percent": -0.99, "sentiment_score": "12.5"},
            limit_up_down={"limit_ratio": "66.7%"},
            turnover_rate={"current_turnover_rate": 2.5},
        )
        self.assertIsNotNone(result)
        self.assertIn("score", result)
        self.assertTrue(result["factors"])


if __name__ == "__main__":
    unittest.main()
