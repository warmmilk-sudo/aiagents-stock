"""
板块策略交易日回退测试
"""
from datetime import datetime, timedelta

import pandas as pd

from sector_strategy_data import SectorStrategyDataFetcher


class StubTsPro:
    def __init__(self, today: str, yesterday: str):
        self.today = today
        self.yesterday = yesterday

    def trade_cal(self, **_kwargs):
        return pd.DataFrame(
            [
                {"cal_date": self.yesterday, "is_open": 1},
                {"cal_date": self.today, "is_open": 1},
            ]
        )

    def moneyflow_ind_ths(self, trade_date: str):
        if trade_date == self.today:
            return pd.DataFrame()
        if trade_date == self.yesterday:
            return pd.DataFrame(
                [
                    {
                        "industry": "半导体",
                        "company_num": 10,
                        "pct_change_stock": 20.0,
                        "ts_code": "TS001",
                        "pct_change": 1.2,
                        "net_amount": 1000000,
                        "lead_stock": "示例龙头",
                        "net_buy_amount": 600000,
                        "net_sell_amount": 400000,
                    }
                ]
            )
        return pd.DataFrame()

    def ths_index(self, **_kwargs):
        return pd.DataFrame(
            [
                {
                    "ts_code": "885001.TI",
                    "name": "AI概念",
                    "type": "N",
                    "count": 50,
                }
            ]
        )

    def ths_daily(self, trade_date: str, **_kwargs):
        if trade_date == self.today:
            return pd.DataFrame()
        if trade_date == self.yesterday:
            return pd.DataFrame(
                [
                    {
                        "ts_code": "885001.TI",
                        "trade_date": self.yesterday,
                        "pct_change": 1.5,
                        "turnover_rate": 2.3,
                    }
                ]
            )
        return pd.DataFrame()


def test_trade_date_fallback_to_previous_day(monkeypatch):
    fetcher = SectorStrategyDataFetcher()

    today = datetime.now().strftime("%Y%m%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    fetcher.ts_pro = StubTsPro(today=today, yesterday=yesterday)

    monkeypatch.setattr(fetcher, "_get_latest_trade_date", lambda: today)
    monkeypatch.setattr(fetcher, "_safe_request", lambda func, *args, **kwargs: func(*args, **kwargs))
    monkeypatch.setattr(fetcher, "_get_market_overview", lambda: {})
    monkeypatch.setattr(fetcher, "_get_north_money_flow", lambda: {})
    monkeypatch.setattr(fetcher, "_get_financial_news", lambda: [])
    monkeypatch.setattr(fetcher, "_save_raw_data_to_db", lambda _data: None)

    data = fetcher.get_all_sector_data()

    assert data["success"] is True
    assert data["used_trade_date"] == yesterday
    assert data["lag_days"] == 1
    assert data["data_source_summary"]["sectors"]["used_trade_date"] == yesterday
    assert data["data_source_summary"]["concepts"]["used_trade_date"] == yesterday
    assert data["data_source_summary"]["fund_flow"]["used_trade_date"] == yesterday


def test_core_data_empty_marks_failed_and_fallback_to_cache(monkeypatch):
    fetcher = SectorStrategyDataFetcher()

    monkeypatch.setattr(fetcher, "_get_sector_performance", lambda: {})
    monkeypatch.setattr(fetcher, "_get_concept_performance", lambda: {})
    monkeypatch.setattr(fetcher, "_get_sector_fund_flow", lambda: {})
    monkeypatch.setattr(fetcher, "_get_market_overview", lambda: {"total_stocks": 5000})
    monkeypatch.setattr(fetcher, "_get_north_money_flow", lambda: {})
    monkeypatch.setattr(fetcher, "_get_financial_news", lambda: [])
    monkeypatch.setattr(fetcher, "_save_raw_data_to_db", lambda _data: None)

    fresh_data = fetcher.get_all_sector_data()
    assert fresh_data["success"] is False
    assert "核心板块数据为空" in fresh_data.get("error", "")

    cache_called = {"called": False}

    def _fake_load_cached_data():
        cache_called["called"] = True
        return {
            "success": True,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sectors": {"半导体": {"name": "半导体"}},
            "concepts": {},
            "sector_fund_flow": {},
            "market_overview": {},
            "north_flow": {},
            "news": [],
            "used_trade_date": (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
            "lag_days": 1,
            "data_source_summary": {"sectors": {"source": "cache"}},
        }

    monkeypatch.setattr(fetcher, "_load_cached_data", _fake_load_cached_data)
    fallback_data = fetcher.get_cached_data_with_fallback()

    assert cache_called["called"] is True
    assert fallback_data["from_cache"] is True
    assert "cache_warning" in fallback_data
