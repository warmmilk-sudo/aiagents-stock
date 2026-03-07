import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from stock_data_cache import StockDataCacheDB, StockDataCacheService, extract_cache_meta


class StockDataCacheServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "stock_data_cache_test.db"
        self.db = StockDataCacheDB(str(self.db_path))
        self.service = StockDataCacheService(self.db)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _age_table(self, table_name: str, where_clause: str, params):
        expired_at = (datetime.now() - timedelta(days=10)).isoformat(timespec="seconds")
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                f"UPDATE {table_name} SET fetched_at = ?, last_success_at = ? WHERE {where_clause}",
                (expired_at, expired_at, *params),
            )
            conn.commit()
        finally:
            conn.close()

    def _assert_same_frame(self, actual: pd.DataFrame, expected: pd.DataFrame):
        actual_copy = actual.copy()
        expected_copy = expected.copy()
        actual_copy.attrs = {}
        expected_copy.attrs = {}
        assert_frame_equal(actual_copy, expected_copy)

    def test_stock_info_cache_hit_skips_live_fetch_when_fresh(self):
        calls = {"count": 0}

        def fetch_fn():
            calls["count"] += 1
            return {
                "symbol": "000001",
                "name": "PingAn",
                "market": "cn",
                "current_price": 12.34,
            }

        first = self.service.get_stock_info(
            symbol="000001",
            market="cn",
            fetch_fn=fetch_fn,
            max_age_seconds=300,
            allow_stale_on_failure=True,
            cache_first=True,
        )
        second = self.service.get_stock_info(
            symbol="000001",
            market="cn",
            fetch_fn=fetch_fn,
            max_age_seconds=300,
            allow_stale_on_failure=True,
            cache_first=True,
        )

        self.assertEqual(calls["count"], 1)
        self.assertFalse(extract_cache_meta(first)["from_cache"])
        self.assertTrue(extract_cache_meta(second)["from_cache"])
        self.assertEqual(second["name"], "PingAn")

    def test_stock_history_expired_cache_refreshes_from_live(self):
        calls = {"count": 0}
        initial_df = pd.DataFrame(
            {"Close": [10.0, 10.5], "Volume": [100, 110]},
            index=pd.to_datetime(["2026-03-01", "2026-03-02"]),
        )
        refreshed_df = pd.DataFrame(
            {"Close": [11.0, 11.6], "Volume": [120, 130]},
            index=pd.to_datetime(["2026-03-03", "2026-03-04"]),
        )

        def fetch_initial():
            calls["count"] += 1
            return initial_df.copy()

        def fetch_refreshed():
            calls["count"] += 1
            return refreshed_df.copy()

        self.service.get_stock_history(
            symbol="000001",
            period="1y",
            interval="1d",
            adjust="qfq",
            fetch_fn=fetch_initial,
            max_age_seconds=300,
            allow_stale_on_failure=True,
            cache_first=True,
        )
        self._age_table(
            "stock_history_cache",
            "symbol = ? AND period = ? AND interval = ? AND adjust = ?",
            ("000001", "1y", "1d", "qfq"),
        )

        result = self.service.get_stock_history(
            symbol="000001",
            period="1y",
            interval="1d",
            adjust="qfq",
            fetch_fn=fetch_refreshed,
            max_age_seconds=300,
            allow_stale_on_failure=True,
            cache_first=True,
        )

        self.assertEqual(calls["count"], 2)
        self.assertFalse(extract_cache_meta(result)["from_cache"])
        self._assert_same_frame(result, refreshed_df)

    def test_stock_history_uses_stale_cache_when_live_fetch_fails(self):
        cached_df = pd.DataFrame(
            {"Close": [9.8, 10.1], "Volume": [90, 95]},
            index=pd.to_datetime(["2026-02-27", "2026-02-28"]),
        )

        self.service.get_stock_history(
            symbol="000001",
            period="1y",
            interval="1d",
            adjust="qfq",
            fetch_fn=lambda: cached_df.copy(),
            max_age_seconds=60,
            allow_stale_on_failure=True,
            cache_first=True,
        )
        self._age_table(
            "stock_history_cache",
            "symbol = ? AND period = ? AND interval = ? AND adjust = ?",
            ("000001", "1y", "1d", "qfq"),
        )

        result = self.service.get_stock_history(
            symbol="000001",
            period="1y",
            interval="1d",
            adjust="qfq",
            fetch_fn=lambda: (_ for _ in ()).throw(RuntimeError("live fetch failed")),
            max_age_seconds=60,
            allow_stale_on_failure=True,
            cache_first=True,
        )

        meta = extract_cache_meta(result)
        self.assertTrue(meta["from_cache"])
        self.assertTrue(meta["stale"])
        self._assert_same_frame(result, cached_df)

    def test_live_fetch_failure_without_cache_returns_error(self):
        result = self.service.get_stock_info(
            symbol="600036",
            market="cn",
            fetch_fn=lambda: (_ for _ in ()).throw(RuntimeError("network down")),
            max_age_seconds=300,
            allow_stale_on_failure=True,
            cache_first=True,
        )

        self.assertIn("error", result)
        self.assertIn("network down", result["error"])

    def test_stock_history_cache_keys_are_isolated_by_period_and_interval(self):
        monthly_df = pd.DataFrame(
            {"Close": [8.0], "Volume": [50]},
            index=pd.to_datetime(["2026-03-01"]),
        )
        yearly_df = pd.DataFrame(
            {"Close": [12.0], "Volume": [150]},
            index=pd.to_datetime(["2026-03-01"]),
        )

        self.service.get_stock_history(
            symbol="000001",
            period="1mo",
            interval="1d",
            adjust="qfq",
            fetch_fn=lambda: monthly_df.copy(),
            max_age_seconds=86400,
            allow_stale_on_failure=True,
            cache_first=True,
        )
        self.service.get_stock_history(
            symbol="000001",
            period="1y",
            interval="1wk",
            adjust="qfq",
            fetch_fn=lambda: yearly_df.copy(),
            max_age_seconds=86400,
            allow_stale_on_failure=True,
            cache_first=True,
        )

        monthly = self.db.get_stock_history("000001", "1mo", "1d", "qfq")
        yearly = self.db.get_stock_history("000001", "1y", "1wk", "qfq")

        self.assertIsNotNone(monthly)
        self.assertIsNotNone(yearly)
        self.assertNotEqual(monthly["payload_json"], yearly["payload_json"])

    def test_clear_all_removes_all_cache_tables(self):
        self.service.get_stock_info(
            symbol="000001",
            market="cn",
            fetch_fn=lambda: {"symbol": "000001", "name": "PingAn", "market": "cn"},
            max_age_seconds=300,
            allow_stale_on_failure=True,
            cache_first=True,
        )
        self.service.get_stock_history(
            symbol="000001",
            period="1y",
            interval="1d",
            adjust="qfq",
            fetch_fn=lambda: pd.DataFrame(
                {"Close": [10.0], "Volume": [100]},
                index=pd.to_datetime(["2026-03-01"]),
            ),
            max_age_seconds=86400,
            allow_stale_on_failure=True,
            cache_first=True,
        )
        self.service.get_stock_financial(
            symbol="000001",
            market="cn",
            fetch_fn=lambda: {"symbol": "000001", "market": "cn", "roe": "12.5"},
            max_age_seconds=86400,
            allow_stale_on_failure=True,
            cache_first=True,
        )
        self.service.get_stock_quarterly(
            symbol="000001",
            fetch_fn=lambda: {"symbol": "000001", "data_success": True, "source": "akshare"},
            max_age_seconds=604800,
            allow_stale_on_failure=True,
            cache_first=True,
        )

        counts = self.service.clear_all()

        self.assertEqual(counts["total"], 4)
        self.assertIsNone(self.db.get_stock_info("000001"))
        self.assertIsNone(self.db.get_stock_history("000001", "1y", "1d", "qfq"))
        self.assertIsNone(self.db.get_stock_financial("000001"))
        self.assertIsNone(self.db.get_stock_quarterly("000001"))


if __name__ == "__main__":
    unittest.main()
