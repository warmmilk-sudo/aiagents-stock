import importlib
import sys
import types
import unittest

import pandas as pd


class _FakeTushareApi:
    def income(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "end_date": "20260331",
                    "ann_date": "20260420",
                    "total_revenue": 1000.0,
                    "revenue": 980.0,
                    "total_cogs": 600.0,
                    "operate_profit": 300.0,
                    "total_profit": 320.0,
                    "n_income": 250.0,
                    "n_income_attr_p": 230.0,
                    "basic_eps": 1.2,
                    "diluted_eps": 1.1,
                    "sell_exp": 30.0,
                    "admin_exp": 40.0,
                    "fin_exp": 5.0,
                    "rd_exp": 20.0,
                }
            ]
        )

    def balancesheet(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "end_date": "20260331",
                    "ann_date": "20260420",
                    "total_assets": 5000.0,
                    "total_cur_assets": 2000.0,
                    "total_nca": 3000.0,
                    "total_liab": 1800.0,
                    "total_cur_liab": 900.0,
                    "total_ncl": 900.0,
                    "total_hldr_eqy_inc_min_int": 3200.0,
                    "total_hldr_eqy_exc_min_int": 3000.0,
                }
            ]
        )

    def cashflow(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "end_date": "20260331",
                    "ann_date": "20260420",
                    "n_cashflow_act": 400.0,
                    "n_cashflow_inv_act": -120.0,
                    "n_cash_flows_fnc_act": -80.0,
                    "n_incr_cash_cash_equ": 200.0,
                    "c_cash_equ_end_period": 880.0,
                }
            ]
        )

    def fina_indicator(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "end_date": "20260331",
                    "ann_date": "20260420",
                    "roe": 18.5,
                    "roa": 9.2,
                    "netprofit_margin": 25.5,
                    "grossprofit_margin": 48.0,
                    "debt_to_assets": 36.0,
                    "current_ratio": 2.1,
                    "quick_ratio": 1.6,
                    "ar_turn": 7.5,
                    "assets_turn": 0.9,
                    "eps": 1.2,
                    "bps": 15.5,
                    "cfps": 2.8,
                }
            ]
        )


class _FakeDataSourceManager:
    tushare_available = True
    tushare_api = _FakeTushareApi()
    akshare_fallback_enabled = False

    @staticmethod
    def _convert_to_ts_code(symbol):
        return f"{symbol}.SH"


class _FakeQuarterlyCacheService:
    @staticmethod
    def get_stock_quarterly(symbol, fetch_fn, **kwargs):
        return fetch_fn()


class _FlakyQuarterlyTushareApi(_FakeTushareApi):
    def __init__(self):
        self.calls = {
            "income": 0,
            "balancesheet": 0,
            "cashflow": 0,
            "fina_indicator": 0,
        }

    def income(self, **kwargs):
        self.calls["income"] += 1
        if self.calls["income"] == 1:
            return pd.DataFrame()
        return super().income(**kwargs)

    def balancesheet(self, **kwargs):
        self.calls["balancesheet"] += 1
        if self.calls["balancesheet"] == 1:
            return pd.DataFrame()
        return super().balancesheet(**kwargs)

    def cashflow(self, **kwargs):
        self.calls["cashflow"] += 1
        if self.calls["cashflow"] == 1:
            return pd.DataFrame()
        return super().cashflow(**kwargs)

    def fina_indicator(self, **kwargs):
        self.calls["fina_indicator"] += 1
        if self.calls["fina_indicator"] == 1:
            return pd.DataFrame()
        return super().fina_indicator(**kwargs)


class QuarterlyReportTusharePriorityTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {
            name: sys.modules.get(name)
            for name in ["quarterly_report_data", "akshare", "data_source_manager", "stock_data_cache"]
        }
        sys.modules["akshare"] = types.SimpleNamespace(
            stock_financial_report_sina=lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("akshare should not be called when tushare data is available")
            ),
            stock_financial_abstract=lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("akshare should not be called when tushare data is available")
            ),
        )
        sys.modules["data_source_manager"] = types.SimpleNamespace(
            data_source_manager=_FakeDataSourceManager()
        )
        sys.modules["stock_data_cache"] = types.SimpleNamespace(
            stock_data_cache_service=_FakeQuarterlyCacheService()
        )
        sys.modules.pop("quarterly_report_data", None)
        self.module = importlib.import_module("quarterly_report_data")

    def tearDown(self):
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_tushare_is_primary_source_for_quarterly_reports(self):
        fetcher = self.module.QuarterlyReportDataFetcher()

        data = fetcher.get_quarterly_reports("600519")

        self.assertTrue(data["data_success"])
        self.assertEqual(data["source"], "tushare")
        self.assertEqual(data["income_statement"]["data"][0]["报告期"], "20260331")
        self.assertEqual(data["income_statement"]["data"][0]["营业总收入"], "1000.0")
        self.assertEqual(data["balance_sheet"]["data"][0]["资产总计"], "5000.0")
        self.assertEqual(data["cash_flow"]["data"][0]["经营活动产生的现金流量净额"], "400.0")
        self.assertEqual(data["financial_indicators"]["data"][0]["净资产收益率"], "18.5")

    def test_quarterly_fetch_retries_when_tushare_returns_empty_once(self):
        flaky_manager = _FakeDataSourceManager()
        flaky_manager.tushare_api = _FlakyQuarterlyTushareApi()
        self.module.data_source_manager = flaky_manager

        fetcher = self.module.QuarterlyReportDataFetcher()
        data = fetcher.get_quarterly_reports("600519")

        self.assertTrue(data["data_success"])
        self.assertEqual(data["income_statement"]["data"][0]["报告期"], "20260331")
        self.assertEqual(flaky_manager.tushare_api.calls["income"], 2)
        self.assertEqual(flaky_manager.tushare_api.calls["balancesheet"], 2)
        self.assertEqual(flaky_manager.tushare_api.calls["cashflow"], 2)
        self.assertEqual(flaky_manager.tushare_api.calls["fina_indicator"], 2)


if __name__ == "__main__":
    unittest.main()
