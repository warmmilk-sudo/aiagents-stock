import sys
import types
import unittest
import importlib

existing_pandas = sys.modules.get("pandas")
if existing_pandas is not None and not getattr(existing_pandas, "__file__", ""):
    sys.modules.pop("pandas", None)

pd = importlib.import_module("pandas")

existing_stock_data = sys.modules.get("stock_data")
if existing_stock_data is not None and not getattr(existing_stock_data, "__file__", "").endswith("stock_data.py"):
    sys.modules.pop("stock_data", None)

existing_stock_data_cache = sys.modules.get("stock_data_cache")
if existing_stock_data_cache is not None and not getattr(existing_stock_data_cache, "__file__", ""):
    sys.modules.pop("stock_data_cache", None)

sys.modules.setdefault("yfinance", types.SimpleNamespace())
sys.modules.setdefault("akshare", types.SimpleNamespace())
sys.modules.setdefault("pywencai_runtime", types.SimpleNamespace(setup_pywencai_runtime_env=lambda: None))
sys.modules.setdefault("pywencai", types.SimpleNamespace())
sys.modules.setdefault("data_source_manager", types.SimpleNamespace(data_source_manager=None))
stock_data_cache_stub = types.ModuleType("stock_data_cache")
stock_data_cache_stub.stock_data_cache_service = None
stock_data_cache_stub.strip_cache_meta = lambda value: value
sys.modules.setdefault("stock_data_cache", stock_data_cache_stub)

from stock_data import StockDataFetcher


class _FakeTushareApi:
    def cyq_perf(self, **kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": kwargs["ts_code"],
                    "trade_date": "20260402",
                    "cost_5pct": 9.5,
                    "cost_15pct": 10.0,
                    "cost_50pct": 11.0,
                    "cost_85pct": 12.2,
                    "cost_95pct": 12.8,
                    "weight_avg": 11.1,
                    "winner_rate": 62.5,
                }
            ]
        )

    def cyq_chips(self, **kwargs):
        return pd.DataFrame(
            [
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 9.8, "percent": 6.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 10.0, "percent": 12.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 10.2, "percent": 24.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 10.4, "percent": 10.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 11.6, "percent": 8.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 12.0, "percent": 18.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 12.2, "percent": 9.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260402", "price": 12.5, "percent": 5.0},
                {"ts_code": kwargs["ts_code"], "trade_date": "20260401", "price": 10.0, "percent": 10.0},
            ]
        )


class _FakeDataSourceManager:
    tushare_available = True
    tushare_api = _FakeTushareApi()

    @staticmethod
    def _convert_to_ts_code(symbol):
        return f"{symbol}.SZ"


class StockDataChipProfileTests(unittest.TestCase):
    def test_calculate_chip_peak_metrics_returns_summary_fields(self):
        fetcher = StockDataFetcher.__new__(StockDataFetcher)
        df = pd.DataFrame(
            {
                "High": [10.4 + (i % 3) * 0.1 for i in range(60)],
                "Low": [9.8 + (i % 2) * 0.05 for i in range(60)],
                "Close": [10.0 + (i % 5) * 0.08 for i in range(60)],
                "Volume": [1000 + i * 25 for i in range(60)],
            },
            index=pd.date_range("2026-01-01", periods=60, freq="B"),
        )

        result = fetcher._calculate_chip_peak_metrics(df)

        self.assertIn(result["chip_peak_shape"], {"单峰密集", "双峰博弈", "多峰发散"})
        self.assertEqual(result["chip_data_source"], "ohlcv_volume_profile_estimate")
        self.assertNotEqual(result["main_chip_peak_price"], "N/A")
        self.assertNotEqual(result["chip_concentration"], "N/A")
        self.assertNotEqual(result["cost_band_70"], "N/A")
        self.assertTrue(str(result["profit_ratio_estimate"]).endswith("%"))
        self.assertTrue(str(result["trap_ratio_estimate"]).endswith("%"))

    def test_get_latest_indicators_prefers_tushare_chip_distribution(self):
        fetcher = StockDataFetcher.__new__(StockDataFetcher)
        fetcher.data_source_manager = _FakeDataSourceManager()

        df = pd.DataFrame(
            [
                {
                    "Close": 12.3,
                    "MA5": 12.0,
                    "MA10": 11.8,
                    "MA20": 11.5,
                    "MA60": 10.9,
                    "RSI": 58.0,
                    "MACD": 0.2,
                    "MACD_signal": 0.1,
                    "BB_upper": 12.8,
                    "BB_lower": 10.9,
                    "K": 67.0,
                    "D": 60.0,
                    "Volume_ratio": 1.3,
                }
            ],
            index=pd.to_datetime(["2026-04-02"]),
        )

        result = fetcher.get_latest_indicators(df, symbol="000001")

        self.assertEqual(result["chip_data_source"], "tushare.cyq_chips/cyq_perf")
        self.assertEqual(result["chip_trade_date"], "20260402")
        self.assertIn(result["chip_peak_shape"], {"双峰博弈", "多峰发散"})
        self.assertEqual(result["main_chip_peak_price"], 10.2)
        self.assertEqual(result["secondary_chip_peak_price"], 12.0)
        self.assertEqual(result["average_chip_cost"], 11.1)
        self.assertEqual(result["cost_band_70"], "10.00-12.20")
        self.assertEqual(result["cost_band_90"], "9.50-12.80")
        self.assertEqual(result["profit_ratio_estimate"], "62.5%")
        self.assertEqual(result["trap_ratio_estimate"], "37.5%")


if __name__ == "__main__":
    unittest.main()
