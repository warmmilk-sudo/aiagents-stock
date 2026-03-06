import sys
import types
from datetime import datetime, timedelta

import pandas as pd
import pytest

import smart_monitor_data as smd

if "plotly" not in sys.modules:
    plotly_module = types.ModuleType("plotly")
    graph_objects_module = types.ModuleType("plotly.graph_objects")
    subplots_module = types.ModuleType("plotly.subplots")
    graph_objects_module.Figure = object
    subplots_module.make_subplots = lambda *args, **kwargs: None
    plotly_module.graph_objects = graph_objects_module
    plotly_module.subplots = subplots_module
    sys.modules["plotly"] = plotly_module
    sys.modules["plotly.graph_objects"] = graph_objects_module
    sys.modules["plotly.subplots"] = subplots_module

import smart_monitor_kline as smk


def test_smart_monitor_realtime_tushare_uses_1min_bar(monkeypatch):
    today = datetime.now().strftime("%Y%m%d")
    today_with_dash = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    calls = {}

    class FakeProApi:
        def daily(self, **kwargs):
            calls["daily"] = kwargs
            return pd.DataFrame([{"trade_date": yesterday, "close": 100.0, "pre_close": 99.0}])

        def daily_basic(self, **kwargs):
            return pd.DataFrame([{"trade_date": yesterday, "turnover_rate": 1.5, "volume_ratio": 1.2}])

        def stock_basic(self, **kwargs):
            return pd.DataFrame([{"name": "贵州茅台"}])

    fetcher = smd.SmartMonitorDataFetcher(use_tdx=False)
    fetcher.ts_pro = FakeProApi()

    def fake_fetch_tushare_pro_bar(**kwargs):
        calls["pro_bar"] = kwargs
        return pd.DataFrame(
            {
                "trade_time": [
                    f"{today_with_dash} 09:30:00",
                    f"{today_with_dash} 09:31:00",
                ],
                "open": [100.2, 100.5],
                "close": [100.5, 101.0],
                "high": [100.8, 101.2],
                "low": [100.0, 100.4],
                "vol": [1500, 1800],
                "amount": [120000.0, 160000.0],
            }
        )

    monkeypatch.setattr(fetcher, "_fetch_tushare_pro_bar", fake_fetch_tushare_pro_bar)

    quote = fetcher._get_realtime_quote_from_tushare("600519")

    assert calls["pro_bar"]["freq"] == "1MIN"
    assert calls["pro_bar"]["adj"] is None
    assert calls["daily"] == {"ts_code": "600519.SH", "end_date": today, "fields": "trade_date,close,pre_close"}
    assert quote["name"] == "贵州茅台"
    assert quote["current_price"] == pytest.approx(101.0)
    assert quote["change_amount"] == pytest.approx(1.0)
    assert quote["change_pct"] == pytest.approx(1.0)


def test_smart_monitor_indicators_tushare_uses_qfq_pro_bar(monkeypatch):
    calls = {}
    captured = {}
    fetcher = smd.SmartMonitorDataFetcher(use_tdx=False)
    fetcher.ts_pro = object()

    rows = []
    for idx in range(70):
        day = datetime(2024, 1, 1) + timedelta(days=idx)
        rows.append(
            {
                "trade_date": day.strftime("%Y%m%d"),
                "open": 10.0 + idx * 0.1,
                "high": 10.5 + idx * 0.1,
                "low": 9.8 + idx * 0.1,
                "close": 10.2 + idx * 0.1,
                "vol": 10 + idx,
                "amount": 20 + idx,
            }
        )

    def fake_fetch_tushare_pro_bar(**kwargs):
        calls.update(kwargs)
        return pd.DataFrame(rows)

    def fake_calculate_all_indicators(df, stock_code):
        captured["df"] = df.copy()
        captured["stock_code"] = stock_code
        return {"ok": True}

    monkeypatch.setattr(fetcher, "_fetch_tushare_pro_bar", fake_fetch_tushare_pro_bar)
    monkeypatch.setattr(fetcher, "_calculate_all_indicators", fake_calculate_all_indicators)

    result = fetcher._get_technical_indicators_from_tushare("600519")

    assert result == {"ok": True}
    assert calls["freq"] == "D"
    assert calls["adj"] == "qfq"
    assert captured["stock_code"] == "600519"
    assert captured["df"].iloc[0]["成交量"] == pytest.approx(1000.0)
    assert captured["df"].iloc[0]["成交额"] == pytest.approx(20000.0)


def test_smart_monitor_kline_tushare_uses_qfq_pro_bar(monkeypatch):
    calls = {}
    kline = smk.SmartMonitorKline()

    def fake_pro_bar(**kwargs):
        calls.update(kwargs)
        return pd.DataFrame(
            {
                "trade_date": ["20240102", "20240103"],
                "open": [10.0, 10.5],
                "high": [10.8, 10.9],
                "low": [9.9, 10.1],
                "close": [10.4, 10.7],
                "vol": [11, 12],
                "amount": [21, 22],
            }
        )

    monkeypatch.setattr(smk.ts, "pro_bar", fake_pro_bar)

    df = kline._get_kline_from_tushare("600519", days=1, ts_pro=object())

    assert calls["freq"] == "D"
    assert calls["adj"] == "qfq"
    assert df.iloc[0]["成交量"] == pytest.approx(1200.0)
    assert df.iloc[0]["成交额"] == pytest.approx(22000.0)
