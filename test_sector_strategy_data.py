import contextlib
import io
import logging
import sys
import time
import types
import unittest
from unittest import mock

class _FakeILoc:
    def __init__(self, rows):
        self._rows = rows

    def __getitem__(self, index):
        return self._rows[index]


class _FakeDataFrame:
    def __init__(self, rows=None):
        self._rows = list(rows or [])
        self.iloc = _FakeILoc(self._rows)

    @property
    def empty(self):
        return len(self._rows) == 0

    def iterrows(self):
        for index, row in enumerate(self._rows):
            yield index, row

    def head(self, count):
        return _FakeDataFrame(self._rows[:count])

    def sort_values(self, key, ascending=True):
        return _FakeDataFrame(sorted(self._rows, key=lambda row: row.get(key), reverse=not ascending))

    def __len__(self):
        return len(self._rows)


sys.modules.setdefault(
    "pandas",
    types.SimpleNamespace(
        DataFrame=_FakeDataFrame,
        Series=dict,
        Timestamp=str,
        isna=lambda value: value is None,
    ),
)
sys.modules.setdefault("akshare", types.SimpleNamespace())
sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None))

import pandas as pd

from sector_strategy_data import SectorStrategyDataFetcher


class SectorStrategyDataFetcherTests(unittest.TestCase):
    def _make_fetcher(self):
        fetcher = SectorStrategyDataFetcher.__new__(SectorStrategyDataFetcher)
        fetcher.max_retries = 1
        fetcher.retry_delay = 0
        fetcher.request_delay = 0
        fetcher.database = None
        fetcher.logger = logging.getLogger("sector_strategy_data_test")
        fetcher._tushare_api = None
        fetcher._tushare_url = None
        fetcher._dc_index_cache = {}
        fetcher.max_fetch_workers = 3
        fetcher._save_raw_data_to_db = lambda data: None
        return fetcher

    def test_safe_request_delegates_to_akshare_guard(self):
        fetcher = self._make_fetcher()
        fetcher.akshare_guard = mock.Mock()
        target = mock.Mock(__name__="stock_board_industry_name_em")
        fetcher.akshare_guard.call.return_value = {"ok": True}

        result = fetcher._safe_request(target, symbol="test")

        self.assertEqual(result, {"ok": True})
        fetcher.akshare_guard.call.assert_called_once_with(
            target,
            request_name="stock_board_industry_name_em",
            symbol="test",
        )

    def test_sector_performance_falls_back_to_tushare(self):
        fetcher = self._make_fetcher()
        fetcher._safe_request = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ak failed"))
        fetcher._get_tushare_board_snapshot = lambda idx_type: pd.DataFrame(
            [
                {
                    "name": "半导体",
                    "pct_change": 2.3,
                    "turnover_rate": 4.5,
                    "total_mv": 123456.0,
                    "leading": "寒武纪",
                    "leading_pct": 7.8,
                    "up_num": 18,
                    "down_num": 5,
                    "ts_code": "BK001",
                }
            ]
        )

        result = fetcher._get_sector_performance()

        self.assertIn("半导体", result)
        self.assertEqual(result["半导体"]["top_stock"], "寒武纪")
        self.assertEqual(result["半导体"]["change_pct"], 2.3)

    def test_sector_fund_flow_falls_back_to_tushare(self):
        fetcher = self._make_fetcher()
        fetcher._safe_request = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ak failed"))
        fetcher._fetch_tushare_trade_data = lambda api_name, **kwargs: pd.DataFrame(
            [
                {
                    "name": "半导体",
                    "net_amount": 1000.0,
                    "net_amount_rate": 2.5,
                    "buy_elg_amount": 800.0,
                    "buy_elg_amount_rate": 1.8,
                    "buy_lg_amount": 200.0,
                    "buy_lg_amount_rate": 0.7,
                    "buy_md_amount": -100.0,
                    "buy_md_amount_rate": -0.3,
                    "buy_sm_amount": -150.0,
                    "buy_sm_amount_rate": -0.5,
                }
            ]
        )
        fetcher._get_tushare_board_snapshot = lambda idx_type: pd.DataFrame(
            [{"name": "半导体", "pct_change": 3.2}]
        )

        result = fetcher._get_sector_fund_flow()

        self.assertEqual(len(result["today"]), 1)
        self.assertEqual(result["today"][0]["sector"], "半导体")
        self.assertEqual(result["today"][0]["change_pct"], 3.2)
        self.assertEqual(result["today"][0]["small_net_inflow"], -150.0)

    def test_get_all_sector_data_fails_when_core_datasets_missing(self):
        fetcher = self._make_fetcher()
        fetcher._get_sector_performance = lambda: {}
        fetcher._get_concept_performance = lambda: {}
        fetcher._get_sector_fund_flow = lambda: {}
        fetcher._get_market_overview = lambda: {"sh_index": {"close": 1}}
        fetcher._get_north_money_flow = lambda: {}
        fetcher._get_financial_news = lambda: []

        with contextlib.redirect_stdout(io.StringIO()):
            result = fetcher.get_all_sector_data()

        self.assertFalse(result["success"])
        self.assertIn("核心板块数据缺失", result["error"])

    def test_get_all_sector_data_fetches_sources_in_parallel(self):
        fetcher = self._make_fetcher()

        def sleep_and_return(value):
            def _inner():
                time.sleep(0.2)
                return value
            return _inner

        fetcher._get_sector_performance = sleep_and_return({"半导体": {"change_pct": 1.2}})
        fetcher._get_concept_performance = sleep_and_return({"AI算力": {"change_pct": 2.3}})
        fetcher._get_sector_fund_flow = sleep_and_return({"today": [{"sector": "半导体"}]})
        fetcher._get_market_overview = sleep_and_return({"sh_index": {"close": 1}})
        fetcher._get_north_money_flow = sleep_and_return({"north_net_inflow": 10})
        fetcher._get_financial_news = sleep_and_return([{"title": "新闻"}])

        started_at = time.perf_counter()
        with contextlib.redirect_stdout(io.StringIO()):
            result = fetcher.get_all_sector_data()
        elapsed = time.perf_counter() - started_at

        self.assertTrue(result["success"])
        self.assertLess(elapsed, 0.45)
        self.assertIn("半导体", result["sectors"])
        self.assertIn("AI算力", result["concepts"])
        self.assertEqual(result["sector_fund_flow"]["today"][0]["sector"], "半导体")
        self.assertEqual(result["north_flow"]["north_net_inflow"], 10)
        self.assertEqual(result["news"][0]["title"], "新闻")


if __name__ == "__main__":
    unittest.main()
