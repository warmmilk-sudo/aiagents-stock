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
            "turnover_rate": None,
            "volume_ratio": None,
            "update_time": "2026-03-13 10:15:00",
            "data_source": "tdx",
            "precision_status": "validated",
            "precision_mode": "tdx_realtime_quote",
            }

    def get_technical_indicators(self, stock_code, period="daily"):
        raise AssertionError("TDX technical indicators should not be used when Tushare daily data is available")


class _FakeRow(dict):
    @property
    def index(self):
        return list(self.keys())


class _FakeILoc:
    def __init__(self, rows):
        self._rows = rows

    def __getitem__(self, index):
        return self._rows[index]


class _FakeFrame:
    def __init__(self, rows):
        self._rows = rows
        self.empty = not bool(rows)
        self.columns = list(rows[0].keys()) if rows else []
        self.iloc = _FakeILoc(rows)

    def __len__(self):
        return len(self._rows)


class SmartMonitorDataFetcherTests(unittest.TestCase):
    def _build_fetcher(self, tdx_fetcher=None, retry_count=3):
        with patch.dict(os.environ, {"TUSHARE_TOKEN": "", "AKSHARE_FALLBACK_ENABLED": "false"}, clear=False):
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
        ) as tushare_mock, patch("smart_monitor_data.time.sleep", return_value=None):
            result = fetcher.get_comprehensive_data("600519", intraday_strict=True)

        self.assertEqual(result["precision_status"], "validated")
        self.assertEqual(result["precision_mode"], "tdx_quote_tushare_daily")
        self.assertEqual(result["data_source"], "tdx")
        self.assertEqual(result["technical_data_source"], "tushare")
        self.assertEqual(result["technical_period"], "daily")
        self.assertEqual(result["tdx_retry_count"], 3)
        self.assertEqual(result["tdx_quote_retry_attempts"], 3)
        self.assertNotIn("tdx_indicators_retry_attempts", result)
        self.assertIsNone(result["turnover_rate"])
        self.assertIsNone(result["volume_ratio"])
        self.assertEqual(fetcher.tdx_fetcher.quote_calls, 3)
        tushare_mock.assert_called_once_with("600519", "daily")

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

    def test_get_comprehensive_data_prefers_realtime_volume_ratio(self):
        fetcher = self._build_fetcher(None, retry_count=1)

        with patch.object(
            fetcher,
            "get_realtime_quote",
            return_value={
                "code": "600519",
                "current_price": 1520.0,
                "volume_ratio": 2.6,
                "data_source": "tdx",
            },
        ), patch.object(
            fetcher,
            "get_technical_indicators",
            return_value={
                "ma5": 1508.0,
                "vol_ma5": 98765.0,
                "volume_ratio_vs_vol_ma5": 0.42,
            },
        ):
            result = fetcher.get_comprehensive_data("600519")

        self.assertEqual(result["volume_ratio"], 2.6)
        self.assertEqual(result["volume_ratio_vs_vol_ma5"], 0.42)
        self.assertEqual(result["vol_ma5"], 98765.0)

    def test_intraday_strict_prefers_realtime_volume_ratio(self):
        fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1)
        fetcher.ts_pro = object()

        fetcher.tdx_fetcher.get_realtime_quote = lambda stock_code: {
            "code": stock_code,
            "current_price": 1520.0,
            "volume_ratio": 3.1,
            "data_source": "tdx",
        }

        with patch.object(
            fetcher,
            "_get_technical_indicators_from_tushare",
            return_value={
                "ma5": 1508.0,
                "vol_ma5": 123456.0,
                "volume_ratio_vs_vol_ma5": 0.51,
                "technical_data_source": "tushare",
                "technical_period": "daily",
            },
        ):
            result = fetcher.get_comprehensive_data("600519", intraday_strict=True)

        self.assertEqual(result["volume_ratio"], 3.1)
        self.assertEqual(result["volume_ratio_vs_vol_ma5"], 0.51)
        self.assertEqual(result["precision_mode"], "tdx_quote_tushare_daily")

    def test_get_comprehensive_data_logs_stage_timings(self):
        fetcher = self._build_fetcher(None, retry_count=1)

        with patch.object(
            fetcher,
            "get_realtime_quote",
            return_value={"code": "600519", "current_price": 1520.0},
        ), patch.object(
            fetcher,
            "get_technical_indicators",
            return_value={"ma5": 1508.0},
        ), patch(
            "smart_monitor_data.time.perf_counter",
            side_effect=[10.0, 11.25, 12.0, 20.0, 22.25, 30.0],
        ), patch.object(fetcher.logger, "info") as info_mock:
            result = fetcher.get_comprehensive_data("600519")

        self.assertTrue(result)
        logged_messages = []
        for call in info_mock.call_args_list:
            template = call.args[0]
            values = call.args[1:]
            logged_messages.append(template % values if values else template)
        self.assertTrue(any("开始获取综合数据" in message for message in logged_messages))
        self.assertTrue(any("实时行情获取完成" in message for message in logged_messages))
        self.assertTrue(any("技术指标获取完成" in message for message in logged_messages))
        self.assertTrue(any("综合数据获取结束" in message for message in logged_messages))

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
            create=True,
        ), patch("smart_monitor_data.ak.stock_zh_a_hist_min_em", side_effect=RuntimeError("akshare failed"), create=True), patch(
            "smart_monitor_data.ak.stock_zh_a_hist",
            side_effect=RuntimeError("akshare failed"),
            create=True,
        ):
            quote = fetcher.get_realtime_quote("600519")

        self.assertIsNone(quote)
        self.assertEqual(fetcher.ts_pro.calls, 0)

    def test_get_realtime_quote_leaves_missing_intraday_fields_empty_without_daily_fallback(self):
        with patch.dict(os.environ, {"AKSHARE_FALLBACK_ENABLED": "true"}, clear=False):
            fetcher = SmartMonitorDataFetcher(use_tdx=False)
        fetcher.use_tdx = False
        fetcher.tdx_fetcher = None
        fetcher.intraday_tdx_retry_count = 1

        min_frame = _FakeFrame([
            _FakeRow(
                {
                    "时间": "2026-03-25 10:15:00",
                    "收盘": 18.88,
                }
            ),
        ])

        with patch.object(fetcher, "_resolve_stock_name", return_value="贵州茅台"), patch(
            "smart_monitor_data.ak.stock_zh_a_hist_min_em",
            return_value=min_frame,
            create=True,
        ), patch(
            "smart_monitor_data.ak.stock_zh_a_hist",
            side_effect=AssertionError("daily history should not be used for intraday quote fallback"),
            create=True,
        ):
            quote = fetcher.get_realtime_quote("600519")

        self.assertIsNotNone(quote)
        self.assertEqual(quote["current_price"], 18.88)
        self.assertIsNone(quote["change_pct"])
        self.assertIsNone(quote["change_amount"])
        self.assertIsNone(quote["high"])
        self.assertIsNone(quote["low"])
        self.assertIsNone(quote["open"])
        self.assertIsNone(quote["pre_close"])
        self.assertIsNone(quote["volume"])
        self.assertIsNone(quote["amount"])
        self.assertIsNone(quote["turnover_rate"])
        self.assertEqual(quote["precision_status"], "partial")
        self.assertEqual(quote["data_source"], "akshare")


if __name__ == "__main__":
    unittest.main()
