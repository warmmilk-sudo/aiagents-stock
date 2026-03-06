import sys
import types
from datetime import datetime, timedelta

import pandas as pd
import pytest

import data_source_manager as dsm_mod


def make_manager(monkeypatch, pro_api=None, tushare_available=True, tushare_token="test-token"):
    monkeypatch.setattr(dsm_mod.policy, "tushare_token", tushare_token, raising=False)
    monkeypatch.setattr(dsm_mod.policy, "tushare_available", tushare_available, raising=False)
    monkeypatch.setattr(dsm_mod.policy, "tushare_api", pro_api, raising=False)
    return dsm_mod.DataSourceManager()


def test_get_stock_hist_data_prefers_akshare_before_tushare(monkeypatch):
    called = {"pro_bar": False}

    fake_ak = types.SimpleNamespace(
        stock_zh_a_hist=lambda **kwargs: pd.DataFrame(
            {
                "日期": ["2024-01-02"],
                "开盘": [10.0],
                "收盘": [10.5],
                "最高": [10.8],
                "最低": [9.9],
                "成交量": [123456],
                "成交额": [654321],
                "涨跌幅": [5.0],
                "涨跌额": [0.5],
                "换手率": [1.2],
            }
        )
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    def fake_get_tushare_sdk(self):
        def pro_bar(**_kwargs):
            called["pro_bar"] = True
            raise AssertionError("AkShare success path should not call Tushare pro_bar")

        return types.SimpleNamespace(pro_bar=pro_bar)

    monkeypatch.setattr(dsm_mod.DataSourceManager, "_get_tushare_sdk", fake_get_tushare_sdk)

    manager = make_manager(monkeypatch, pro_api=object(), tushare_available=True)
    df = manager.get_stock_hist_data("600519", start_date="2024-01-01", end_date="2024-01-31", adjust="qfq")

    assert not called["pro_bar"]
    assert list(df.columns)[:6] == ["date", "open", "close", "high", "low", "volume"]
    assert df.iloc[0]["close"] == 10.5


def test_get_stock_hist_data_tushare_fallback_uses_pro_bar_with_adjust(monkeypatch):
    calls = {}

    class FakeProApi:
        def daily(self, *args, **kwargs):
            raise AssertionError("Adjusted history fallback must use pro_bar, not daily()")

    fake_ak = types.SimpleNamespace(
        stock_zh_a_hist=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("akshare unavailable"))
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    def fake_get_tushare_sdk(self):
        def pro_bar(**kwargs):
            calls.update(kwargs)
            return pd.DataFrame(
                {
                    "trade_date": ["20240103"],
                    "open": [10.0],
                    "close": [10.2],
                    "high": [10.4],
                    "low": [9.8],
                    "pre_close": [9.9],
                    "change": [0.3],
                    "pct_chg": [3.0303],
                    "vol": [12.5],
                    "amount": [8.2],
                }
            )

        return types.SimpleNamespace(pro_bar=pro_bar)

    monkeypatch.setattr(dsm_mod.DataSourceManager, "_get_tushare_sdk", fake_get_tushare_sdk)

    manager = make_manager(monkeypatch, pro_api=FakeProApi(), tushare_available=True)
    df = manager.get_stock_hist_data("600519", start_date="2024-01-01", end_date="2024-01-31", adjust="qfq")

    assert calls["ts_code"] == "600519.SH"
    assert calls["freq"] == "D"
    assert calls["adj"] == "qfq"
    assert calls["api"].__class__.__name__ == "FakeProApi"
    assert df.iloc[0]["volume"] == pytest.approx(1250.0)
    assert df.iloc[0]["amount"] == pytest.approx(8200.0)


def test_get_realtime_quotes_uses_tushare_1min_and_returns_complete_fields(monkeypatch):
    today = datetime.now().strftime("%Y%m%d")
    today_with_dash = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    calls = {"pro_bar": None, "daily": None}

    class FakeProApi:
        def daily(self, **kwargs):
            calls["daily"] = kwargs
            return pd.DataFrame(
                [
                    {
                        "trade_date": yesterday,
                        "close": 98.0,
                        "pre_close": 97.0,
                    }
                ]
            )

        def stock_basic(self, **kwargs):
            return pd.DataFrame([{"ts_code": "600519.SH", "name": "贵州茅台"}])

    def fake_get_tushare_sdk(self):
        def pro_bar(**kwargs):
            calls["pro_bar"] = kwargs
            return pd.DataFrame(
                {
                    "trade_time": [
                        f"{today_with_dash} 09:30:00",
                        f"{today_with_dash} 09:31:00",
                    ],
                    "open": [100.0, 100.5],
                    "close": [100.5, 101.0],
                    "high": [101.0, 101.3],
                    "low": [99.8, 100.2],
                    "vol": [2000, 3000],
                    "amount": [150000.0, 230000.0],
                }
            )

        return types.SimpleNamespace(pro_bar=pro_bar)

    monkeypatch.setattr(dsm_mod.DataSourceManager, "_get_tushare_sdk", fake_get_tushare_sdk)
    monkeypatch.setenv("TDX_ENABLED", "false")

    manager = make_manager(monkeypatch, pro_api=FakeProApi(), tushare_available=True)
    quote = manager.get_realtime_quotes("600519")

    assert calls["pro_bar"]["freq"] == "1MIN"
    assert calls["pro_bar"]["adj"] is None
    assert calls["daily"] == {"ts_code": "600519.SH", "end_date": today}
    assert quote["name"] == "贵州茅台"
    assert quote["price"] == pytest.approx(101.0)
    assert quote["current_price"] == pytest.approx(101.0)
    assert quote["change"] == pytest.approx(3.0)
    assert quote["change_amount"] == pytest.approx(3.0)
    assert quote["change_percent"] == pytest.approx(3.0612)
    assert quote["change_pct"] == pytest.approx(3.0612)
    assert quote["pre_close"] == pytest.approx(98.0)
    assert quote["data_source"] == "tushare_1min"


def test_get_financial_data_uses_akshare_only_with_sina_code(monkeypatch):
    calls = {}

    class FakeProApi:
        def income(self, **kwargs):
            raise AssertionError("Financial reports should not use Tushare fallback anymore")

        def balancesheet(self, **kwargs):
            raise AssertionError("Financial reports should not use Tushare fallback anymore")

        def cashflow(self, **kwargs):
            raise AssertionError("Financial reports should not use Tushare fallback anymore")

    def stock_financial_report_sina(**kwargs):
        calls.update(kwargs)
        return pd.DataFrame([{"报告日": "2024-09-30", "营业总收入": 123456789}])

    fake_ak = types.SimpleNamespace(stock_financial_report_sina=stock_financial_report_sina)
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    manager = make_manager(monkeypatch, pro_api=FakeProApi(), tushare_available=True)
    df = manager.get_financial_data("600519", report_type="income")

    assert calls["stock"] == "sh600519"
    assert calls["symbol"] == "利润表"
    assert not df.empty
    assert df.iloc[0]["营业总收入"] == 123456789


def test_get_realtime_quotes_no_longer_uses_akshare_spot_em(monkeypatch):
    fake_ak = types.SimpleNamespace(
        stock_zh_a_spot_em=lambda: (_ for _ in ()).throw(
            AssertionError("Realtime fallback should not call stock_zh_a_spot_em")
        )
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)
    monkeypatch.setenv("TDX_ENABLED", "false")
    monkeypatch.setattr(
        dsm_mod.DataSourceManager,
        "_get_realtime_quote_from_sina",
        lambda self, symbol: self._build_realtime_quote(
            symbol=symbol,
            name="测试股票",
            price=12.3,
            change=0.3,
            change_percent=2.5,
            volume=1000,
            amount=12345,
            high=12.5,
            low=12.0,
            open_price=12.1,
            pre_close=12.0,
            update_time="2026-03-06 10:00:00",
            data_source="sina_http",
        ),
    )

    manager = make_manager(monkeypatch, pro_api=None, tushare_available=False, tushare_token="")
    quote = manager.get_realtime_quotes("600519")

    assert quote["name"] == "测试股票"
    assert quote["data_source"] == "sina_http"
