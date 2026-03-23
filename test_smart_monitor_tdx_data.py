import types
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


class _FakeTurnoverDataFrame:
    empty = False
    columns = ["ts_code", "trade_date", "turnover_rate"]

    def __init__(self, rows):
        self._rows = rows

    def sort_values(self, *args, **kwargs):
        return self

    def reset_index(self, *args, **kwargs):
        return self

    def iterrows(self):
        for index, row in enumerate(self._rows):
            yield index, row


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
            fetcher.ts_pro = types.SimpleNamespace(
                daily_basic=lambda *args, **kwargs: _FakeTurnoverDataFrame(
                    [
                        {
                            "ts_code": "600519.SH",
                            "trade_date": "20260323",
                            "turnover_rate": 0.2087,
                        }
                    ]
                )
            )
            with self.assertLogs("smart_monitor_tdx_data", level="DEBUG") as captured:
                quote = fetcher.get_realtime_quote("600519")

        self.assertEqual(quote["name"], "贵州茅台")
        joined_logs = "\n".join(captured.output)
        self.assertIn("TDX请求 -> endpoint=/api/quote", joined_logs)
        self.assertIn("TDX响应 <- endpoint=/api/quote status=200", joined_logs)
        self.assertIn("TDX请求 -> endpoint=/api/search", joined_logs)
        self.assertIn("TDX行情摘要 600519", joined_logs)


if __name__ == "__main__":
    unittest.main()
