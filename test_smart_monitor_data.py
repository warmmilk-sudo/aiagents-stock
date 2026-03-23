import sys
import types
import os
import unittest
from unittest.mock import patch

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
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None))
sys.modules.setdefault("ta", types.SimpleNamespace())
sys.modules.setdefault(
    "akshare",
    types.SimpleNamespace(
        stock_individual_info_em=lambda *args, **kwargs: None,
        stock_zh_a_hist_min_em=lambda *args, **kwargs: None,
        stock_zh_a_hist=lambda *args, **kwargs: None,
    ),
)

from smart_monitor_data import SmartMonitorDataFetcher


class _FakeTDXFetcher:
    def __init__(self, quote_success_on=1):
        self.quote_success_on = quote_success_on
        self.quote_calls = 0

    def get_realtime_quote(self, stock_code):
        self.quote_calls += 1
        if self.quote_calls < self.quote_success_on:
            return None
        return {
            "code": stock_code,
            "name": "贵州茅台",
            "current_price": 1520.0,
            "change_pct": 1.25,
            "change_amount": 18.8,
            "high": 1528.0,
            "low": 1498.0,
            "open": 1501.0,
            "pre_close": 1501.2,
            "volume": 123456,
            "amount": 456789000.0,
            "update_time": "2026-03-13 10:15:00",
            "data_source": "tdx",
            }

    def get_technical_indicators(self, stock_code, period="daily"):
        raise AssertionError("TDX technical indicators should not be used when Tushare daily data is available")


class SmartMonitorDataFetcherTests(unittest.TestCase):
    def _build_fetcher(self, tdx_fetcher=None, retry_count=3):
        with patch.dict(os.environ, {"TUSHARE_TOKEN": ""}, clear=False):
            fetcher = SmartMonitorDataFetcher(use_tdx=False)
        fetcher.use_tdx = tdx_fetcher is not None
        fetcher.tdx_fetcher = tdx_fetcher
        fetcher.intraday_tdx_retry_count = retry_count
        return fetcher

    def test_intraday_strict_requires_tdx_and_retries_three_times(self):
        fetcher = self._build_fetcher(
            _FakeTDXFetcher(quote_success_on=3),
            retry_count=3,
        )
        fetcher.ts_pro = object()

        with patch.object(
            fetcher,
            "_get_technical_indicators_from_tushare",
            return_value={
                "ma5": 1508.0,
                "ma20": 1496.0,
                "ma60": 1452.0,
                "trend": "up",
                "technical_data_source": "tushare",
                "technical_period": "daily",
            },
        ) as tushare_mock, patch.object(
            fetcher,
            "_get_latest_turnover_rate",
            return_value=0.75,
        ) as turnover_mock, patch("smart_monitor_data.time.sleep", return_value=None):
            result = fetcher.get_comprehensive_data("600519", intraday_strict=True)

        self.assertEqual(result["precision_status"], "validated")
        self.assertEqual(result["precision_mode"], "tdx_quote_tushare_daily")
        self.assertEqual(result["data_source"], "tdx")
        self.assertEqual(result["technical_data_source"], "tushare")
        self.assertEqual(result["technical_period"], "daily")
        self.assertEqual(result["tdx_retry_count"], 3)
        self.assertEqual(result["tdx_quote_retry_attempts"], 3)
        self.assertNotIn("tdx_indicators_retry_attempts", result)
        self.assertEqual(result["turnover_rate"], 0.75)
        self.assertEqual(fetcher.tdx_fetcher.quote_calls, 3)
        tushare_mock.assert_called_once_with("600519", "daily")
        turnover_mock.assert_called_once_with("600519")

    def test_intraday_strict_fails_when_tdx_unavailable(self):
        fetcher = self._build_fetcher(None, retry_count=3)

        result = fetcher.get_comprehensive_data("600519", intraday_strict=True)

        self.assertEqual(result["precision_status"], "failed")
        self.assertEqual(result["precision_mode"], "tdx_quote_tushare_daily")
        self.assertIn("TDX", result["precision_error"])

    def test_daily_indicators_prefer_tushare_and_do_not_use_tdx(self):
        fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1)
        fetcher.ts_pro = object()

        with patch.object(
            fetcher,
            "_get_technical_indicators_from_tushare",
            return_value={
                "ma5": 1508.0,
                "ma20": 1496.0,
                "ma60": 1452.0,
                "trend": "up",
                "technical_data_source": "tushare",
                "technical_period": "daily",
            },
        ) as tushare_mock:
            indicators = fetcher.get_technical_indicators("600519")

        self.assertEqual(indicators["technical_data_source"], "tushare")
        self.assertEqual(indicators["technical_period"], "daily")
        tushare_mock.assert_called_once_with("600519", "daily")

    def test_daily_indicators_fail_closed_when_tushare_returns_no_data(self):
        fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1)
        fetcher.ts_pro = object()

        with patch.object(fetcher, "_get_technical_indicators_from_tushare", return_value=None):
            indicators = fetcher.get_technical_indicators("600519")

        self.assertIsNone(indicators)

    def test_get_realtime_quote_does_not_fall_back_to_tushare(self):
        fetcher = self._build_fetcher(None, retry_count=1)

        class _FakeTushare:
            def __init__(self):
                self.calls = 0

            def daily_basic(self, *args, **kwargs):
                self.calls += 1
                raise AssertionError("tushare should not be used for realtime quote")

            def daily(self, *args, **kwargs):
                self.calls += 1
                raise AssertionError("tushare should not be used for realtime quote")

        class _FakeInfoFrame:
            empty = False

            def __getitem__(self, key):
                if key == "item":
                    return ["股票简称"]
                if key == "value":
                    return ["贵州茅台"]
                raise KeyError(key)

        fetcher.ts_pro = _FakeTushare()

        with patch(
            "smart_monitor_data.ak.stock_individual_info_em",
            return_value=_FakeInfoFrame(),
        ), patch("smart_monitor_data.ak.stock_zh_a_hist_min_em", side_effect=RuntimeError("akshare failed")), patch(
            "smart_monitor_data.ak.stock_zh_a_hist",
            side_effect=RuntimeError("akshare failed"),
        ):
            quote = fetcher.get_realtime_quote("600519")

        self.assertIsNone(quote)
        self.assertEqual(fetcher.ts_pro.calls, 0)


if __name__ == "__main__":
    unittest.main()
