import importlib
import sys
import types
import unittest

import pandas as pd


class _FakeTushareApi:
    def moneyflow(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260403",
                    "buy_sm_amount": 100.0,
                    "sell_sm_amount": 80.0,
                    "buy_md_amount": 120.0,
                    "sell_md_amount": 90.0,
                    "buy_lg_amount": 200.0,
                    "sell_lg_amount": 120.0,
                    "buy_elg_amount": 180.0,
                    "sell_elg_amount": 60.0,
                }
            ]
        )

    def daily(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "trade_date": "20260403",
                    "close": 12.34,
                    "pct_chg": 1.23,
                }
            ]
        )


class _FlakyFundFlowTushareApi(_FakeTushareApi):
    def __init__(self):
        self.daily_calls = 0

    def daily(self, **kwargs):
        self.daily_calls += 1
        if self.daily_calls == 1:
            return pd.DataFrame()
        return super().daily(**kwargs)


class _FakeDataSourceManager:
    tushare_available = True
    tushare_api = _FakeTushareApi()

    @staticmethod
    def _convert_to_ts_code(symbol):
        return f"{symbol}.SH"


class FundFlowTusharePriorityTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {
            name: sys.modules.get(name)
            for name in ["data_source_manager", "fund_flow_data"]
        }
        sys.modules["data_source_manager"] = types.SimpleNamespace(
            data_source_manager=_FakeDataSourceManager()
        )
        sys.modules.pop("fund_flow_data", None)
        self.module = importlib.import_module("fund_flow_data")

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_tushare_is_primary_source_for_fund_flow(self):
        fetcher = self.module.FundFlowDataFetcher()

        data = fetcher.get_fund_flow_data("600905")

        self.assertTrue(data["data_success"])
        self.assertEqual(data["source"], "tushare")
        first = data["fund_flow_data"]["data"][0]
        self.assertEqual(first["日期"], "20260403")
        self.assertEqual(first["收盘价"], 12.34)
        self.assertEqual(first["涨跌幅"], 1.23)
        self.assertEqual(first["主力净流入-净额"], 200.0)

    def test_fund_flow_retries_daily_when_quote_frame_is_empty_once(self):
        flaky_manager = _FakeDataSourceManager()
        flaky_manager.tushare_api = _FlakyFundFlowTushareApi()
        self.module.data_source_manager = flaky_manager

        fetcher = self.module.FundFlowDataFetcher()
        data = fetcher.get_fund_flow_data("600905")

        self.assertTrue(data["data_success"])
        self.assertEqual(data["fund_flow_data"]["data"][0]["收盘价"], 12.34)
        self.assertEqual(flaky_manager.tushare_api.daily_calls, 2)


if __name__ == "__main__":
    unittest.main()
