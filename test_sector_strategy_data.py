import contextlib
import io
import logging
import sys
import threading
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

import sector_strategy_data
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
        fetcher._tushare_init_lock = threading.Lock()
        fetcher._tushare_call_lock = threading.Lock()
        fetcher._dc_index_cache = {}
        fetcher.max_fetch_workers = 3
        fetcher._save_raw_data_to_db = lambda data: None
        return fetcher

    def test_fetch_tushare_trade_data_rolls_back_to_recent_trade_date(self):
        fetcher = self._make_fetcher()
        calls = []

        class _RecentTradeApi:
            def dc_index(self, **kwargs):
                calls.append(kwargs["trade_date"])
                if kwargs["trade_date"] == "20260408":
                    return pd.DataFrame([{"name": "半导体", "pct_change": 2.3}])
                return pd.DataFrame()

        fetcher._tushare_api = _RecentTradeApi()
        fetcher._ensure_tushare_api = lambda: fetcher._tushare_api

        with mock.patch.object(
            sector_strategy_data,
            "datetime",
            wraps=sector_strategy_data.datetime,
        ) as mocked_datetime:
            mocked_datetime.now.return_value = sector_strategy_data.datetime(2026, 4, 9)
            mocked_datetime.side_effect = lambda *args, **kwargs: sector_strategy_data.datetime(*args, **kwargs)
            result = fetcher._fetch_tushare_trade_data("dc_index", idx_type="行业板块", max_days=3)

        self.assertFalse(result.empty)
        self.assertEqual(calls[:2], ["20260409", "20260408"])

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

    def test_sector_performance_falls_back_to_cache_when_tushare_unavailable(self):
        fetcher = self._make_fetcher()
        fetcher._safe_request = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ak failed"))
        fetcher._get_tushare_board_snapshot = lambda idx_type: pd.DataFrame()
        fetcher.database = mock.Mock()
        fetcher.database.get_latest_raw_data.return_value = {
            "data_content": {"半导体": {"name": "半导体", "change_pct": 1.5}}
        }

        result = fetcher._get_sector_performance()

        fetcher.database.get_latest_raw_data.assert_called_once_with("sectors")
        self.assertEqual(result["半导体"]["change_pct"], 1.5)

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

    def test_sector_fund_flow_falls_back_to_cache_when_tushare_unavailable(self):
        fetcher = self._make_fetcher()
        fetcher._safe_request = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("ak failed"))
        fetcher._fetch_tushare_trade_data = lambda api_name, **kwargs: pd.DataFrame()
        fetcher.database = mock.Mock()
        fetcher.database.get_latest_raw_data.return_value = {
            "data_content": {"today": [{"sector": "半导体", "main_net_inflow": 123.0}]}
        }

        result = fetcher._get_sector_fund_flow()

        fetcher.database.get_latest_raw_data.assert_called_once_with("fund_flow")
        self.assertEqual(result["today"][0]["sector"], "半导体")
        self.assertEqual(result["today"][0]["main_net_inflow"], 123.0)

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

    def test_get_all_sector_data_serializes_tushare_calls_for_shared_client(self):
        fetcher = self._make_fetcher()
        active_calls = {"count": 0}

        class _ConcurrencySensitiveApi:
            def dc_index(self, **kwargs):
                active_calls["count"] += 1
                try:
                    if active_calls["count"] > 1:
                        return pd.DataFrame()
                    time.sleep(0.05)
                    idx_type = kwargs.get("idx_type")
                    if idx_type == "行业板块":
                        return pd.DataFrame(
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
                    if idx_type == "概念板块":
                        return pd.DataFrame(
                            [
                                {
                                    "name": "AI算力",
                                    "pct_change": 3.1,
                                    "turnover_rate": 5.1,
                                    "total_mv": 223456.0,
                                    "leading": "中际旭创",
                                    "leading_pct": 6.8,
                                    "up_num": 28,
                                    "down_num": 3,
                                    "ts_code": "BK101",
                                }
                            ]
                        )
                    return pd.DataFrame()
                finally:
                    active_calls["count"] -= 1

            def moneyflow_ind_dc(self, **kwargs):
                active_calls["count"] += 1
                try:
                    if active_calls["count"] > 1:
                        return pd.DataFrame()
                    time.sleep(0.05)
                    return pd.DataFrame(
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
                finally:
                    active_calls["count"] -= 1

            def daily(self, **kwargs):
                return pd.DataFrame([{"pct_chg": 1.0}, {"pct_chg": -0.5}])

            def index_daily(self, **kwargs):
                return pd.DataFrame([{"close": 3200.0, "change": 12.0, "pct_chg": 0.38}])

            def moneyflow_hsgt(self, **kwargs):
                return pd.DataFrame()

        fetcher._tushare_api = _ConcurrencySensitiveApi()
        fetcher._ensure_tushare_api = lambda: fetcher._tushare_api
        fetcher._get_financial_news = lambda: [{"title": "新闻"}]
        fetcher._save_raw_data_to_db = lambda data: None

        with contextlib.redirect_stdout(io.StringIO()):
            result = fetcher.get_all_sector_data()

        self.assertTrue(result["success"])
        self.assertIn("半导体", result["sectors"])
        self.assertIn("AI算力", result["concepts"])
        self.assertEqual(result["sector_fund_flow"]["today"][0]["sector"], "半导体")

    def test_market_overview_builds_from_tushare_sources(self):
        fetcher = self._make_fetcher()
        fetcher.database = mock.Mock()
        fetcher.database.get_latest_raw_data.return_value = None
        fetcher._get_market_breadth_rows = lambda: [{"pct_chg": 1.2}, {"pct_chg": -0.4}, {"pct_chg": 0.0}, {"pct_chg": 9.9}, {"pct_chg": -10.1}]
        fetcher._get_market_index_overview = lambda: {
            "sh_index": {"code": "000001", "name": "上证指数"},
            "sz_index": {"code": "399001", "name": "深证成指"},
            "cyb_index": {"code": "399006", "name": "创业板指"},
        }

        result = fetcher._get_market_overview()

        self.assertEqual(result["total_stocks"], 5)
        self.assertEqual(result["up_count"], 2)
        self.assertEqual(result["down_count"], 2)
        self.assertEqual(result["flat_count"], 1)
        self.assertEqual(result["limit_up"], 1)
        self.assertEqual(result["limit_down"], 1)
        self.assertEqual(result["sh_index"]["code"], "000001")
        self.assertEqual(result["sz_index"]["code"], "399001")
        self.assertEqual(result["cyb_index"]["code"], "399006")

    def test_get_all_sector_data_keeps_running_when_optional_source_fails(self):
        fetcher = self._make_fetcher()
        fetcher._get_sector_performance = lambda: {"半导体": {"change_pct": 1.2}}
        fetcher._get_concept_performance = lambda: {"AI算力": {"change_pct": 2.3}}
        fetcher._get_sector_fund_flow = lambda: {"today": [{"sector": "半导体"}]}
        fetcher._get_market_overview = lambda: (_ for _ in ()).throw(RuntimeError("market failed"))
        fetcher._get_north_money_flow = lambda: {"north_net_inflow": 10}
        fetcher._get_financial_news = lambda: [{"title": "新闻"}]

        with contextlib.redirect_stdout(io.StringIO()):
            result = fetcher.get_all_sector_data()

        self.assertTrue(result["success"])
        self.assertEqual(result["market_overview"], {})
        self.assertEqual(result["north_flow"]["north_net_inflow"], 10)

    def test_market_overview_fills_missing_fields_from_cache_without_retrying(self):
        fetcher = self._make_fetcher()
        breadth_calls = {"count": 0}

        def _get_breadth_rows():
            breadth_calls["count"] += 1
            return []

        fetcher._get_market_breadth_rows = _get_breadth_rows
        fetcher._get_market_index_overview = lambda: {"sh_index": {"code": "000001", "name": "上证指数"}}
        fetcher.database = mock.Mock()
        fetcher.database.get_latest_raw_data.return_value = {
            "data_content": {
                "up_count": 3000,
                "down_count": 1800,
                "sz_index": {"code": "399001", "name": "深证成指"},
            }
        }

        result = fetcher._get_market_overview()

        self.assertEqual(breadth_calls["count"], 1)
        self.assertEqual(result["up_count"], 3000)
        self.assertEqual(result["down_count"], 1800)
        self.assertEqual(result["sh_index"]["code"], "000001")
        self.assertEqual(result["sz_index"]["code"], "399001")

    def test_market_overview_returns_cached_snapshot_when_realtime_empty(self):
        fetcher = self._make_fetcher()
        fetcher._get_market_breadth_rows = lambda: []
        fetcher._get_market_index_overview = lambda: {}
        fetcher.database = mock.Mock()
        fetcher.database.get_latest_raw_data.return_value = {
            "data_content": {
                "up_count": 3000,
                "down_count": 1800,
                "sh_index": {"code": "000001", "name": "上证指数"},
            }
        }

        result = fetcher._get_market_overview()

        self.assertEqual(result["up_count"], 3000)
        self.assertEqual(result["down_count"], 1800)
        self.assertEqual(result["sh_index"]["code"], "000001")

    def test_get_cached_data_with_fallback_marks_cached_snapshot(self):
        fetcher = self._make_fetcher()
        fetcher.get_all_sector_data = lambda: {"success": False, "error": "fresh failed"}
        fetcher._load_cached_data = lambda: {
            **fetcher._new_data_payload(success=True),
            "sectors": {"半导体": {"change_pct": 1.2}},
        }

        with contextlib.redirect_stdout(io.StringIO()):
            result = fetcher.get_cached_data_with_fallback()

        self.assertTrue(result["success"])
        self.assertTrue(result["from_cache"])
        self.assertIn("缓存数据", result["cache_warning"])
        self.assertIn("半导体", result["sectors"])

    def test_get_cached_data_with_fallback_returns_error_when_cache_missing(self):
        fetcher = self._make_fetcher()
        fetcher.get_all_sector_data = lambda: {"success": False, "error": "fresh failed"}
        fetcher._load_cached_data = lambda: None

        with contextlib.redirect_stdout(io.StringIO()):
            result = fetcher.get_cached_data_with_fallback()

        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "无法获取数据且无可用缓存")
        self.assertEqual(result["news"], [])


if __name__ == "__main__":
    unittest.main()
