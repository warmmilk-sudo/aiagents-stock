import unittest
from datetime import datetime

from monitor_scheduler import TradingTimeScheduler


class _FakeMonitorService:
    def __init__(self):
        self.running = False
        self.start_calls = 0
        self.stop_calls = 0

    def start_monitoring(self):
        self.running = True
        self.start_calls += 1

    def stop_monitoring(self):
        self.running = False
        self.stop_calls += 1


class TradingTimeSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.service = _FakeMonitorService()
        self.scheduler = TradingTimeScheduler(self.service)

    def test_pre_and_post_offsets_expand_cn_window(self):
        self.scheduler.config.update(
            {
                "market": "CN",
                "trading_days": [1, 2, 3, 4, 5],
                "trading_hours": {"CN": [{"start": "09:30", "end": "11:30"}]},
                "pre_market_minutes": 5,
                "post_market_minutes": 5,
            }
        )
        monday_0927 = datetime(2026, 3, 2, 9, 27, 0)
        monday_0924 = datetime(2026, 3, 2, 9, 24, 0)
        monday_1134 = datetime(2026, 3, 2, 11, 34, 0)
        monday_1136 = datetime(2026, 3, 2, 11, 36, 0)

        self.assertTrue(self.scheduler.is_trading_time(monday_0927))
        self.assertFalse(self.scheduler.is_trading_time(monday_0924))
        self.assertTrue(self.scheduler.is_trading_time(monday_1134))
        self.assertFalse(self.scheduler.is_trading_time(monday_1136))

    def test_cross_day_us_window_supports_after_midnight_tail(self):
        self.scheduler.config.update(
            {
                "market": "US",
                "trading_days": [1, 2, 3, 4, 5],
                "trading_hours": {"US": [{"start": "21:30", "end": "04:00"}]},
                "pre_market_minutes": 0,
                "post_market_minutes": 0,
            }
        )
        friday_2200 = datetime(2026, 3, 6, 22, 0, 0)
        saturday_0200 = datetime(2026, 3, 7, 2, 0, 0)
        sunday_0200 = datetime(2026, 3, 8, 2, 0, 0)

        self.assertTrue(self.scheduler.is_trading_time(friday_2200))
        self.assertTrue(self.scheduler.is_trading_time(saturday_0200))
        self.assertFalse(self.scheduler.is_trading_time(sunday_0200))

    def test_get_next_trading_time_returns_adjusted_start_time(self):
        self.scheduler.config.update(
            {
                "market": "CN",
                "trading_days": [1, 2, 3, 4, 5],
                "trading_hours": {"CN": [{"start": "09:30", "end": "11:30"}]},
                "pre_market_minutes": 5,
                "post_market_minutes": 5,
            }
        )
        monday_0800 = datetime(2026, 3, 2, 8, 0, 0)
        monday_1000 = datetime(2026, 3, 2, 10, 0, 0)

        self.assertEqual(self.scheduler.get_next_trading_time(monday_0800), "2026-03-02 09:25")
        self.assertEqual(self.scheduler.get_next_trading_time(monday_1000), "交易时段内")


if __name__ == "__main__":
    unittest.main()
