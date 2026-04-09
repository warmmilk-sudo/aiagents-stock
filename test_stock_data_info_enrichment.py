import unittest
from types import SimpleNamespace

import pandas as pd

from stock_data import StockDataFetcher


class _FakeTushareAPI:
    def daily_basic(self, **kwargs):
        fields = kwargs.get("fields", "")
        if "ps" in fields:
            return pd.DataFrame(
                [
                    {"trade_date": "20260408", "ps": 10.8, "ps_ttm": 10.2},
                    {"trade_date": "20260407", "ps": 10.5, "ps_ttm": 10.0},
                ]
            )
        return pd.DataFrame(
            [
                {"trade_date": "20260408", "pe": None, "pb": 8.4, "total_mv": 2100000},
                {"trade_date": "20260407", "pe": 25.6, "pb": 8.3, "total_mv": 2090000},
            ]
        )

    def bak_basic(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "name": "贵州茅台",
                    "industry": "白酒",
                    "pe": 25.1,
                    "pb": 8.2,
                    "total_mv": 2080000,
                }
            ]
        )

    def index_daily(self, **kwargs):
        dates = pd.date_range("2025-04-01", periods=260, freq="B")
        closes = [3000 + i * 2 for i in range(len(dates))]
        return pd.DataFrame(
            {
                "trade_date": [d.strftime("%Y%m%d") for d in dates],
                "close": closes,
            }
        )


class _FakeDataSourceManager:
    tushare_available = True

    def __init__(self):
        self.tushare_api = _FakeTushareAPI()

    def _convert_to_ts_code(self, symbol):
        return f"{symbol}.SH"

    def get_stock_basic_info(self, symbol):
        return {"symbol": symbol, "name": "贵州茅台", "industry": "白酒"}

    def get_stock_hist_data(self, symbol, **kwargs):
        dates = pd.date_range("2025-04-01", periods=260, freq="B")
        close = pd.Series([1500 + i for i in range(len(dates))], dtype=float)
        return pd.DataFrame(
            {
                "date": dates,
                "open": close - 5,
                "close": close,
                "high": close + 10,
                "low": close - 10,
                "volume": 1000000,
            }
        )

    def get_realtime_quotes(self, symbol):
        return {
            "symbol": symbol,
            "name": "贵州茅台",
            "price": 1688.0,
            "change_percent": 1.23,
        }


class StockDataInfoEnrichmentTests(unittest.TestCase):
    def test_chinese_stock_info_is_enriched_from_tushare_and_history(self):
        fetcher = StockDataFetcher.__new__(StockDataFetcher)
        fetcher.data = None
        fetcher.info = None
        fetcher.financial_data = None
        fetcher.data_source_manager = _FakeDataSourceManager()
        fetcher.cache_service = SimpleNamespace()

        info = fetcher._get_chinese_stock_info("600519")

        self.assertEqual(info["name"], "贵州茅台")
        self.assertEqual(info["industry"], "白酒")
        self.assertEqual(info["sector"], "白酒")
        self.assertEqual(info["pe_ratio"], 25.6)
        self.assertEqual(info["pb_ratio"], 8.4)
        self.assertEqual(info["ps_ratio"], 10.2)
        self.assertEqual(info["current_price"], 1688.0)
        self.assertEqual(info["change_percent"], 1.23)
        self.assertIsNotNone(info["52_week_high"])
        self.assertIsNotNone(info["52_week_low"])
        self.assertIsNotNone(info["beta"])


if __name__ == "__main__":
    unittest.main()
