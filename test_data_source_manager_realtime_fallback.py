import importlib
import os
import sys
import types
import unittest


class _FakeTDXFetcher:
    def __init__(self, base_url: str = "", timeout_seconds: int = 10):
        self.base_url = base_url
        self.timeout_seconds = timeout_seconds
        self.available = True

    def get_realtime_quote(self, stock_code: str):
        return {
            "code": stock_code,
            "name": "贵州茅台",
            "current_price": 1520.0,
            "change_pct": 1.25,
            "change_amount": 18.8,
            "volume": 123456,
            "amount": 456789000.0,
            "high": 1528.0,
            "low": 1498.0,
            "open": 1501.0,
            "pre_close": 1501.2,
            "update_time": "2026-03-20 15:00:00",
            "data_source": "tdx",
        }


class _UnavailableTDXFetcher(_FakeTDXFetcher):
    def __init__(self, base_url: str = "", timeout_seconds: int = 10):
        super().__init__(base_url=base_url, timeout_seconds=timeout_seconds)
        self.available = False

    def get_realtime_quote(self, stock_code: str):
        return None


class DataSourceManagerRealtimeFallbackTests(unittest.TestCase):
    def setUp(self):
        self.original_modules = {name: sys.modules.get(name) for name in [
            "akshare",
            "data_source_manager",
            "dotenv",
            "pandas",
            "smart_monitor_tdx_data",
            "tushare_utils",
        ]}
        self.original_akshare_fallback = os.environ.get("AKSHARE_FALLBACK_ENABLED")
        sys.modules["pandas"] = types.SimpleNamespace(DataFrame=type("DataFrame", (), {}), isna=lambda value: False)
        sys.modules["dotenv"] = types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)
        sys.modules["tushare_utils"] = types.SimpleNamespace(create_tushare_pro=lambda *args, **kwargs: (None, ""))

    def tearDown(self):
        if self.original_akshare_fallback is None:
            os.environ.pop("AKSHARE_FALLBACK_ENABLED", None)
        else:
            os.environ["AKSHARE_FALLBACK_ENABLED"] = self.original_akshare_fallback
        for name, module in self.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def _reload_module(self, tdx_fetcher_class):
        sys.modules["smart_monitor_tdx_data"] = types.SimpleNamespace(
            SmartMonitorTDXDataFetcher=tdx_fetcher_class,
        )
        sys.modules.pop("data_source_manager", None)
        return importlib.import_module("data_source_manager")

    def test_get_realtime_quotes_falls_back_to_tdx_when_akshare_fails(self):
        sys.modules["akshare"] = types.SimpleNamespace(
            stock_zh_a_spot_em=lambda: (_ for _ in ()).throw(RuntimeError("akshare unavailable")),
        )
        module = self._reload_module(_FakeTDXFetcher)
        manager = module.DataSourceManager()
        manager.tdx_enabled = True
        manager.tdx_base_url = "http://tdx.example.com:8181"
        manager.tdx_timeout_seconds = 12

        quote = manager.get_realtime_quotes("600519")

        self.assertEqual(quote["data_source"], "tdx")
        self.assertEqual(quote["price"], 1520.0)
        self.assertEqual(quote["current_price"], 1520.0)
        self.assertEqual(quote["change_percent"], 1.25)
        self.assertEqual(quote["change"], 18.8)
        self.assertEqual(quote["pre_close"], 1501.2)
        self.assertEqual(quote["update_time"], "2026-03-20 15:00:00")

    def test_get_realtime_quotes_prefers_tdx_even_when_akshare_is_available(self):
        akshare_calls = {"count": 0}

        def fake_akshare_spot():
            akshare_calls["count"] += 1
            raise AssertionError("akshare should not be called when TDX is available")

        sys.modules["akshare"] = types.SimpleNamespace(stock_zh_a_spot_em=fake_akshare_spot)
        module = self._reload_module(_FakeTDXFetcher)
        manager = module.DataSourceManager()
        manager.tdx_enabled = True
        manager.tdx_base_url = "http://tdx.example.com:8181"
        manager.tdx_timeout_seconds = 12

        quote = manager.get_realtime_quotes("600519")

        self.assertEqual(quote["data_source"], "tdx")
        self.assertEqual(akshare_calls["count"], 0)

    def test_get_realtime_quotes_returns_empty_when_tdx_is_unavailable(self):
        os.environ["AKSHARE_FALLBACK_ENABLED"] = "true"
        sys.modules["akshare"] = types.SimpleNamespace(
            stock_zh_a_spot_em=lambda: (_ for _ in ()).throw(RuntimeError("akshare unavailable")),
        )
        module = self._reload_module(_UnavailableTDXFetcher)
        manager = module.DataSourceManager()
        manager.tdx_enabled = True
        manager.tdx_base_url = "http://tdx.example.com:8181"
        manager.tdx_timeout_seconds = 12

        quote = manager.get_realtime_quotes("600519")

        self.assertEqual(quote, {})

    def test_get_realtime_quotes_skips_akshare_when_fallback_disabled(self):
        os.environ["AKSHARE_FALLBACK_ENABLED"] = "false"
        akshare_calls = {"count": 0}

        def fake_akshare_spot():
            akshare_calls["count"] += 1
            return []

        sys.modules["akshare"] = types.SimpleNamespace(stock_zh_a_spot_em=fake_akshare_spot)
        module = self._reload_module(_UnavailableTDXFetcher)
        manager = module.DataSourceManager()
        manager.tdx_enabled = True
        manager.tdx_base_url = "http://tdx.example.com:8181"
        manager.tdx_timeout_seconds = 12

        quote = manager.get_realtime_quotes("600519")

        self.assertEqual(quote, {})
        self.assertEqual(akshare_calls["count"], 0)

    def test_convert_to_ts_code_handles_etf_and_convertible_prefixes(self):
        sys.modules["akshare"] = types.SimpleNamespace()
        module = self._reload_module(_UnavailableTDXFetcher)
        manager = module.DataSourceManager()

        self.assertEqual(manager._convert_to_ts_code("510300"), "510300.SH")
        self.assertEqual(manager._convert_to_ts_code("113001"), "113001.SH")
        self.assertEqual(manager._convert_to_ts_code("159915"), "159915.SZ")
        self.assertEqual(manager._convert_to_ts_code("123001"), "123001.SZ")


if __name__ == "__main__":
    unittest.main()
