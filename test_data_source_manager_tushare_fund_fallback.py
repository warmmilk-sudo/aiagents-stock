import importlib
import sys
import types
import unittest

import pandas as pd


class _FakeTushareApi:
    def daily(self, **kwargs):
        return pd.DataFrame()

    def fund_daily(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260409",
                    "open": 3.95,
                    "high": 4.01,
                    "low": 3.93,
                    "close": 4.0,
                    "vol": 1234,
                    "amount": 5678,
                }
            ]
        )

    def stock_basic(self, **kwargs):
        return pd.DataFrame()

    def fund_basic(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": "510300.SH",
                    "name": "沪深300ETF",
                    "management": "华泰柏瑞",
                    "market": "E",
                    "list_date": "20120528",
                }
            ]
        )


class DataSourceManagerTushareFundFallbackTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {
            name: sys.modules.get(name)
            for name in ["data_source_manager", "dotenv", "tushare_utils", "config"]
        }
        sys.modules["dotenv"] = types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
        sys.modules["tushare_utils"] = types.SimpleNamespace(create_tushare_pro=lambda *args, **kwargs: (None, ""))
        sys.modules["config"] = types.SimpleNamespace(TDX_CONFIG={}, TDX_TIMEOUT_SECONDS=10)
        sys.modules.pop("data_source_manager", None)
        self.module = importlib.import_module("data_source_manager")

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_get_stock_hist_data_uses_fund_daily_when_daily_is_empty(self):
        manager = self.module.DataSourceManager()
        manager.tushare_available = True
        manager.tushare_api = _FakeTushareApi()

        df = manager.get_stock_hist_data("510300", start_date="20260401", end_date="20260409")

        self.assertIsNotNone(df)
        self.assertEqual(len(df.index), 1)
        self.assertEqual(float(df.iloc[0]["close"]), 4.0)

    def test_get_stock_basic_info_uses_fund_basic_for_etf(self):
        manager = self.module.DataSourceManager()
        manager.tushare_available = True
        manager.tushare_api = _FakeTushareApi()

        info = manager.get_stock_basic_info("510300")

        self.assertEqual(info["name"], "沪深300ETF")
        self.assertEqual(info["industry"], "华泰柏瑞")
        self.assertEqual(info["market"], "E")


if __name__ == "__main__":
    unittest.main()
