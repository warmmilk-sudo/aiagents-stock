import sys
import types
import os
import tempfile
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo
from datetime import datetime

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

    def get_intraday_context(self, stock_code):
        return {}


class SmartMonitorDataFetcherTests(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._temp_dir.cleanup()

    def _build_fetcher(self, tdx_fetcher=None, retry_count=3, cache_db_path=None):
        resolved_cache_db_path = cache_db_path or os.path.join(self._temp_dir.name, "smart_monitor_cache.db")
        with patch.dict(os.environ, {"TUSHARE_TOKEN": "", "AKSHARE_FALLBACK_ENABLED": "false"}, clear=False):
            fetcher = SmartMonitorDataFetcher(use_tdx=False, cache_db_path=resolved_cache_db_path)
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

    def test_tushare_daily_indicators_use_same_day_cache(self):
        fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1)
        fetcher.ts_pro = object()

        fake_df = object()
        fake_indicators = {
            "ma5": 1508.0,
            "ma20": 1496.0,
            "ma60": 1452.0,
            "trend": "up",
        }

        with patch.object(
            SmartMonitorDataFetcher,
            "_beijing_now",
            return_value=datetime(2026, 4, 10, 10, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        ), patch.object(
            fetcher,
            "_fetch_tushare_daily_history",
            return_value=fake_df,
        ) as history_mock, patch.object(
            fetcher,
            "_calculate_all_indicators",
            return_value=dict(fake_indicators),
        ) as calc_mock:
            first = fetcher.get_technical_indicators("600519")
            second = fetcher.get_technical_indicators("600519")

        self.assertEqual(history_mock.call_count, 1)
        self.assertEqual(calc_mock.call_count, 1)
        self.assertEqual(first["technical_data_source"], "tushare")
        self.assertEqual(second["technical_data_source"], "tushare")
        self.assertEqual(second["ma5"], 1508.0)

    def test_tushare_daily_indicator_failure_enters_short_cooldown(self):
        fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1)
        fetcher.ts_pro = object()
        fetcher.tushare_daily_failure_cooldown_seconds = 300

        with patch.object(
            SmartMonitorDataFetcher,
            "_beijing_now",
            return_value=datetime(2026, 4, 10, 10, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        ), patch.object(
            fetcher,
            "_fetch_tushare_daily_history",
            return_value=None,
        ) as history_mock:
            first = fetcher.get_technical_indicators("600519")
            second = fetcher.get_technical_indicators("600519")

        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(history_mock.call_count, 1)

    def test_tushare_daily_indicators_persist_across_fetcher_instances(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_db_path = os.path.join(temp_dir, "smart_monitor_cache.db")
            first_fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1, cache_db_path=cache_db_path)
            first_fetcher.ts_pro = object()
            fake_df = object()

            with patch.object(
                SmartMonitorDataFetcher,
                "_beijing_now",
                return_value=datetime(2026, 4, 10, 10, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            ), patch.object(
                first_fetcher,
                "_fetch_tushare_daily_history",
                return_value=fake_df,
            ) as history_mock, patch.object(
                first_fetcher,
                "_calculate_all_indicators",
                return_value={"ma5": 1508.0, "trend": "up"},
            ):
                first = first_fetcher.get_technical_indicators("600519")

            second_fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1, cache_db_path=cache_db_path)
            second_fetcher.ts_pro = object()

            with patch.object(
                SmartMonitorDataFetcher,
                "_beijing_now",
                return_value=datetime(2026, 4, 10, 11, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            ), patch.object(
                second_fetcher,
                "_fetch_tushare_daily_history",
                side_effect=AssertionError("should hit persistent cache before querying tushare"),
            ):
                second = second_fetcher.get_technical_indicators("600519")

            self.assertEqual(history_mock.call_count, 1)
            self.assertEqual(first["technical_data_source"], "tushare")
            self.assertEqual(second["technical_data_source"], "tushare")
            self.assertEqual(second["ma5"], 1508.0)

    def test_tushare_daily_indicator_failure_cooldown_persists_across_fetchers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_db_path = os.path.join(temp_dir, "smart_monitor_cache.db")
            first_fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1, cache_db_path=cache_db_path)
            first_fetcher.ts_pro = object()
            first_fetcher.tushare_daily_failure_cooldown_seconds = 300

            with patch.object(
                SmartMonitorDataFetcher,
                "_beijing_now",
                return_value=datetime(2026, 4, 10, 10, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            ), patch.object(
                first_fetcher,
                "_fetch_tushare_daily_history",
                return_value=None,
            ) as first_history_mock:
                first = first_fetcher.get_technical_indicators("600519")

            second_fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1, cache_db_path=cache_db_path)
            second_fetcher.ts_pro = object()
            second_fetcher.tushare_daily_failure_cooldown_seconds = 300

            with patch.object(
                SmartMonitorDataFetcher,
                "_beijing_now",
                return_value=datetime(2026, 4, 10, 10, 31, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            ), patch.object(
                second_fetcher,
                "_fetch_tushare_daily_history",
                side_effect=AssertionError("should respect persisted failure cooldown"),
            ):
                second = second_fetcher.get_technical_indicators("600519")

            self.assertIsNone(first)
            self.assertIsNone(second)
            self.assertEqual(first_history_mock.call_count, 1)

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

    def test_get_comprehensive_data_does_not_attach_market_and_sector_context(self):
        fetcher = self._build_fetcher(None, retry_count=1)

        with patch.object(
            fetcher,
            "get_realtime_quote",
            return_value={"code": "600519", "current_price": 1520.0},
        ), patch.object(
            fetcher,
            "get_technical_indicators",
            return_value={"ma5": 1508.0},
        ):
            result = fetcher.get_comprehensive_data("600519")

        self.assertNotIn("market_context", result)
        self.assertNotIn("sector_context", result)

    def test_get_comprehensive_data_attaches_tdx_intraday_context(self):
        tdx_fetcher = _FakeTDXFetcher()
        tdx_fetcher.get_intraday_context = lambda stock_code: {
            "minute_point_count": 120,
            "last_5m_change_pct": 0.88,
        }
        fetcher = self._build_fetcher(tdx_fetcher, retry_count=1)

        with patch.object(
            fetcher,
            "get_realtime_quote",
            return_value={"code": "600519", "current_price": 1520.0, "data_source": "tdx"},
        ), patch.object(
            fetcher,
            "get_technical_indicators",
            return_value={"ma5": 1508.0},
        ):
            result = fetcher.get_comprehensive_data("600519")

        self.assertEqual(result["intraday_context"]["minute_point_count"], 120)
        self.assertEqual(result["intraday_context"]["last_5m_change_pct"], 0.88)

    def test_get_comprehensive_data_attaches_realtime_freshness(self):
        tdx_fetcher = _FakeTDXFetcher()
        tdx_fetcher.get_intraday_context = lambda stock_code: {
            "latest_minute_time": "10:26",
            "latest_trade_time": "2026-04-10T10:28:00+08:00",
            "minute_coverage_ratio": 0.97,
            "max_minute_gap": 2,
            "minute_point_count": 115,
            "filled_minute_point_count": 118,
        }
        fetcher = self._build_fetcher(tdx_fetcher, retry_count=1)

        with patch.object(
            fetcher,
            "get_realtime_quote",
            return_value={
                "code": "600519",
                "current_price": 1520.0,
                "data_source": "tdx",
                "update_time": "2026-04-10 10:29:00",
            },
        ), patch.object(
            fetcher,
            "get_technical_indicators",
            return_value={"ma5": 1508.0},
        ), patch.object(
            SmartMonitorDataFetcher,
            "_beijing_now",
            return_value=datetime(2026, 4, 10, 10, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        ):
            result = fetcher.get_comprehensive_data("600519")

        freshness = result["realtime_freshness"]
        self.assertEqual(freshness["overall_status"], "ready")
        self.assertTrue(freshness["intraday_decision_ready"])
        self.assertEqual(freshness["minute"]["status"], "fresh")
        self.assertEqual(freshness["trade"]["status"], "fresh")
        self.assertEqual(freshness["quote"]["status"], "same_day_service_time")
        self.assertEqual(freshness["minute_quality"]["status"], "fair")
        self.assertEqual(freshness["minute_quality"]["max_gap"], 2)

    def test_realtime_freshness_degrades_when_minute_quality_is_poor(self):
        fetcher = self._build_fetcher(_FakeTDXFetcher(), retry_count=1)

        with patch.object(
            SmartMonitorDataFetcher,
            "_beijing_now",
            return_value=datetime(2026, 4, 10, 10, 30, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        ):
            freshness = fetcher._build_realtime_freshness(
                {
                    "update_time": "2026-04-10 10:30:00",
                    "intraday_context": {
                        "latest_minute_time": "10:29",
                        "latest_trade_time": "2026-04-10T10:29:30+08:00",
                        "minute_coverage_ratio": 0.82,
                        "max_minute_gap": 6,
                        "minute_point_count": 98,
                        "filled_minute_point_count": 120,
                    },
                }
            )

        self.assertEqual(freshness["overall_status"], "degraded")
        self.assertFalse(freshness["intraday_decision_ready"])
        self.assertEqual(freshness["minute_quality"]["status"], "poor")

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

        fetcher.ts_pro = _FakeTushare()

        quote = fetcher.get_realtime_quote("600519")

        self.assertIsNone(quote)
        self.assertEqual(fetcher.ts_pro.calls, 0)


if __name__ == "__main__":
    unittest.main()
