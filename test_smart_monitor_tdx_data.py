import unittest
from unittest.mock import patch

import requests

from smart_monitor_tdx_data import SmartMonitorTDXDataFetcher


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = {} if payload is None else payload
        self.headers = {} if headers is None else headers

    def json(self):
        return self._payload


class SmartMonitorTDXDataFetcherTests(unittest.TestCase):
    def test_init_requires_base_url(self):
        with self.assertRaisesRegex(ValueError, "TDX_BASE_URL 未配置"):
            SmartMonitorTDXDataFetcher(base_url="")

    def test_check_connection_falls_back_to_quote_probe_when_health_missing(self):
        calls = []

        def _fake_get(url, params=None, timeout=None):
            calls.append((url, params, timeout))
            if url.endswith("/api/health"):
                return _FakeResponse(status_code=404)
            if url.endswith("/api/quote"):
                return _FakeResponse(status_code=200, payload={"code": 0, "data": []})
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(requests, "get", side_effect=_fake_get):
            fetcher = SmartMonitorTDXDataFetcher(base_url="http://tdx.example.com:8181")

        self.assertTrue(fetcher.available)
        self.assertEqual(calls[0][0], "http://tdx.example.com:8181/api/health")
        self.assertEqual(calls[1][0], "http://tdx.example.com:8181/api/quote")
        self.assertEqual(calls[1][1], {"code": "000001"})

    def test_check_connection_marks_unreachable_when_health_and_quote_fail(self):
        def _fake_get(url, params=None, timeout=None):
            if url.endswith("/api/health"):
                raise requests.exceptions.ConnectionError("connect failed")
            if url.endswith("/api/quote"):
                raise requests.exceptions.Timeout("timeout")
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(requests, "get", side_effect=_fake_get):
            fetcher = SmartMonitorTDXDataFetcher(base_url="http://tdx.example.com:8181")

        self.assertFalse(fetcher.available)

    def test_get_realtime_quote_emits_debug_request_logs(self):
        def _fake_get(url, params=None, timeout=None):
            if url.endswith("/api/health"):
                return _FakeResponse(status_code=200, payload={"status": "ok"})
            if url.endswith("/api/quote"):
                return _FakeResponse(
                    status_code=200,
                    payload={
                        "code": 0,
                        "data": [
                            {
                                "K": {
                                    "Close": 12340,
                                    "Last": 12000,
                                    "Open": 12100,
                                    "High": 12400,
                                    "Low": 12050,
                                },
                                "TotalHand": 456789,
                                "Amount": 987654321,
                                "ServerTime": 1760000000,
                            }
                        ],
                    },
                    headers={
                        "Content-Type": "application/json",
                        "Date": "Mon, 23 Mar 2026 03:26:53 GMT",
                    },
                )
            if url.endswith("/api/search"):
                return _FakeResponse(
                    status_code=200,
                    payload={"code": 0, "data": [{"code": "600519", "name": "贵州茅台"}]},
                    headers={"Content-Type": "application/json"},
                )
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(requests, "get", side_effect=_fake_get):
            fetcher = SmartMonitorTDXDataFetcher(base_url="http://tdx.example.com:8181")
            with self.assertLogs("smart_monitor_tdx_data", level="DEBUG") as captured:
                quote = fetcher.get_realtime_quote("600519")

        self.assertEqual(quote["name"], "贵州茅台")
        self.assertEqual(quote["precision_status"], "validated")
        self.assertEqual(quote["precision_mode"], "tdx_realtime_quote")
        self.assertIsNone(quote["turnover_rate"])
        self.assertIsNone(quote["volume_ratio"])
        joined_logs = "\n".join(captured.output)
        self.assertIn("TDX请求 -> endpoint=/api/quote", joined_logs)
        self.assertIn("TDX响应 <- endpoint=/api/quote status=200", joined_logs)
        self.assertIn("TDX请求 -> endpoint=/api/search", joined_logs)
        self.assertIn("TDX行情摘要 600519", joined_logs)

    def test_get_comprehensive_data_prefers_realtime_volume_ratio(self):
        with patch.object(requests, "get", return_value=_FakeResponse(status_code=200, payload={"status": "ok"})):
            fetcher = SmartMonitorTDXDataFetcher(base_url="http://tdx.example.com:8181")

        with patch.object(
            fetcher,
            "get_realtime_quote",
            return_value={"code": "600519", "volume_ratio": 1.9, "current_price": 18.88},
        ), patch.object(
            fetcher,
            "get_technical_indicators",
            return_value={"vol_ma5": 20000.0, "volume_ratio_vs_vol_ma5": 0.64},
        ), patch.object(
            fetcher,
            "get_intraday_context",
            return_value={},
        ):
            result = fetcher.get_comprehensive_data("600519")

        self.assertEqual(result["volume_ratio"], 1.9)
        self.assertEqual(result["volume_ratio_vs_vol_ma5"], 0.64)
        self.assertEqual(result["vol_ma5"], 20000.0)

    def test_get_intraday_context_summarizes_minute_and_trade_data(self):
        with patch.object(requests, "get", return_value=_FakeResponse(status_code=200, payload={"status": "ok"})):
            fetcher = SmartMonitorTDXDataFetcher(base_url="http://tdx.example.com:8181")

        with patch.object(
            fetcher,
            "get_minute_data",
            return_value={
                "count": 12,
                "points": [
                    {"time": "10:00", "price": 10.00, "volume": 100},
                    {"time": "10:01", "price": 10.02, "volume": 110},
                    {"time": "10:02", "price": 10.03, "volume": 120},
                    {"time": "10:03", "price": 10.05, "volume": 130},
                    {"time": "10:04", "price": 10.08, "volume": 140},
                    {"time": "10:05", "price": 10.10, "volume": 150},
                    {"time": "10:06", "price": 10.12, "volume": 160},
                    {"time": "10:07", "price": 10.15, "volume": 170},
                    {"time": "10:08", "price": 10.18, "volume": 180},
                    {"time": "10:09", "price": 10.22, "volume": 190},
                    {"time": "10:10", "price": 10.26, "volume": 220},
                    {"time": "10:11", "price": 10.30, "volume": 260},
                ],
            },
        ), patch.object(
            fetcher,
            "get_trade_data",
            return_value={
                "count": 3,
                "points": [
                    {"time": "2026-04-10T10:11:59+08:00", "price": 10.30, "volume": 12, "status": 0},
                    {"time": "2026-04-10T10:11:58+08:00", "price": 10.29, "volume": 20, "status": 1},
                    {"time": "2026-04-10T10:11:57+08:00", "price": 10.28, "volume": 8, "status": 1},
                ],
            },
        ):
            context = fetcher.get_intraday_context("600519")

        self.assertEqual(context["minute_point_count"], 12)
        self.assertAlmostEqual(context["intraday_high"], 10.30, places=4)
        self.assertAlmostEqual(context["intraday_low"], 10.00, places=4)
        self.assertAlmostEqual(context["price_position_pct"], 100.0, places=4)
        self.assertGreater(context["last_5m_change_pct"], 0)
        self.assertGreater(context["volume_acceleration_ratio"], 1)
        self.assertEqual(context["trade_tick_count"], 3)
        self.assertEqual(context["latest_trade_time"], "2026-04-10T10:11:59+08:00")
        self.assertIn("当前价格接近日内高位", context["intraday_observations"])
        self.assertEqual(context["intraday_bias"], "trend_continuation")
        self.assertIn("高位放量延续", context["intraday_signal_labels"])
        self.assertIn("价格运行在分时均价上方", context["intraday_signal_labels"])


if __name__ == "__main__":
    unittest.main()
