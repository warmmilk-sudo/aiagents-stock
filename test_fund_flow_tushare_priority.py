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
            for name in ["akshare", "data_source_manager", "fund_flow_akshare"]
        }
        sys.modules["akshare"] = types.SimpleNamespace(
            stock_individual_fund_flow=lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("akshare should not be called when tushare is available")
            )
        )
        sys.modules["data_source_manager"] = types.SimpleNamespace(
            data_source_manager=_FakeDataSourceManager()
        )
        sys.modules.pop("fund_flow_akshare", None)
        self.module = importlib.import_module("fund_flow_akshare")

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_tushare_is_primary_source_for_fund_flow(self):
        fetcher = self.module.FundFlowAkshareDataFetcher()

        data = fetcher.get_fund_flow_data("600905")

        self.assertTrue(data["data_success"])
        self.assertEqual(data["source"], "tushare")
        first = data["fund_flow_data"]["data"][0]
        self.assertEqual(first["日期"], "20260403")
        self.assertEqual(first["收盘价"], 12.34)
        self.assertEqual(first["涨跌幅"], 1.23)
        self.assertEqual(first["主力净流入-净额"], 200.0)


if __name__ == "__main__":
    unittest.main()
