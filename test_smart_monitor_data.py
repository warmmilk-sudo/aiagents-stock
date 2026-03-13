import os
import unittest
from unittest.mock import patch

from smart_monitor_data import SmartMonitorDataFetcher


class _FakeTDXFetcher:
    def __init__(self, quote_success_on=1, indicator_success_on=1):
        self.quote_success_on = quote_success_on
        self.indicator_success_on = indicator_success_on
        self.quote_calls = 0
        self.indicator_calls = 0

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
        self.indicator_calls += 1
        if self.indicator_calls < self.indicator_success_on:
            return None
        return {
            "ma5": 1508.0,
            "ma20": 1496.0,
            "ma60": 1452.0,
            "trend": "up",
        }


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
            _FakeTDXFetcher(quote_success_on=3, indicator_success_on=3),
            retry_count=3,
        )

        with patch("smart_monitor_data.time.sleep", return_value=None):
            result = fetcher.get_comprehensive_data("600519", intraday_strict=True)

        self.assertEqual(result["precision_status"], "validated")
        self.assertEqual(result["precision_mode"], "tdx_intraday_strict")
        self.assertEqual(result["data_source"], "tdx")
        self.assertEqual(result["tdx_retry_count"], 3)
        self.assertEqual(result["tdx_quote_retry_attempts"], 3)
        self.assertEqual(result["tdx_indicators_retry_attempts"], 3)
        self.assertEqual(fetcher.tdx_fetcher.quote_calls, 3)
        self.assertEqual(fetcher.tdx_fetcher.indicator_calls, 3)

    def test_intraday_strict_fails_when_tdx_unavailable(self):
        fetcher = self._build_fetcher(None, retry_count=3)

        result = fetcher.get_comprehensive_data("600519", intraday_strict=True)

        self.assertEqual(result["precision_status"], "failed")
        self.assertEqual(result["precision_mode"], "tdx_intraday_strict")
        self.assertIn("TDX", result["precision_error"])


if __name__ == "__main__":
    unittest.main()
