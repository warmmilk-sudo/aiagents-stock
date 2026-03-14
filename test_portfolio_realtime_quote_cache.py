import tempfile
import unittest

from monitor_db import StockMonitorDatabase
from portfolio_db import PortfolioDB
from portfolio_manager import PortfolioManager
from smart_monitor_db import SmartMonitorDB


class _FakeRealtimeFetcher:
    def __init__(self, price: float = 12.34, update_time: str = "2026-03-14 10:00:00"):
        self.price = price
        self.update_time = update_time
        self.calls = 0

    def get_realtime_quote(self, stock_code: str, retry: int = 1):
        self.calls += 1
        return {
            "code": stock_code,
            "current_price": self.price,
            "update_time": self.update_time,
            "data_source": "tdx",
        }


class PortfolioRealtimeQuoteCacheTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        base = self.temp_dir.name
        self.portfolio_db = PortfolioDB(f"{base}/portfolio.db")
        self.realtime_monitor_db = StockMonitorDatabase(f"{base}/monitor.db")
        self.smart_monitor_db = SmartMonitorDB(f"{base}/smart.db")
        self.manager = PortfolioManager(
            portfolio_store=self.portfolio_db,
            realtime_monitor_store=self.realtime_monitor_db,
            smart_monitor_store=self.smart_monitor_db,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_off_hours_reuses_cached_quote_without_fetching(self):
        self.manager._realtime_quote_fetcher = _FakeRealtimeFetcher()
        self.manager._realtime_quote_cache["000001"] = {
            "quote": {
                "code": "000001",
                "current_price": 10.88,
                "update_time": "2026-03-14 14:59:59",
                "data_source": "tdx",
            },
            "cached_at": 0,
        }
        self.manager._is_a_share_trading_time = lambda now=None: False

        quote = self.manager._get_realtime_quote("000001")

        self.assertEqual(quote["current_price"], 10.88)
        self.assertEqual(self.manager._realtime_quote_fetcher.calls, 0)

    def test_trading_hours_uses_process_cache_for_repeated_requests(self):
        fetcher = _FakeRealtimeFetcher()
        self.manager._realtime_quote_fetcher = fetcher
        self.manager._is_a_share_trading_time = lambda now=None: True

        first = self.manager._get_realtime_quote("000001")
        second = self.manager._get_realtime_quote("000001")

        self.assertEqual(first["current_price"], 12.34)
        self.assertEqual(second["current_price"], 12.34)
        self.assertEqual(fetcher.calls, 1)


if __name__ == "__main__":
    unittest.main()
