import importlib
import sys
import types

import pandas as pd

def test_quarterly_report_fetcher_uses_akshare_sina_code(monkeypatch):
    sys.modules.setdefault("streamlit", types.ModuleType("streamlit"))
    qrd = importlib.import_module("quarterly_report_data")
    calls = {}

    def fake_stock_financial_report_sina(**kwargs):
        calls.update(kwargs)
        return pd.DataFrame([{"报告期": "2024-09-30", "营业总收入": 123}])

    monkeypatch.setattr(qrd.ak, "stock_financial_report_sina", fake_stock_financial_report_sina)

    fetcher = qrd.QuarterlyReportDataFetcher()
    result = fetcher._get_income_statement("600519")

    assert fetcher.prefer_tushare is False
    assert calls["stock"] == "sh600519"
    assert calls["symbol"] == "利润表"
    assert result["source"] == "akshare"


def test_stock_data_chinese_financial_flow_uses_akshare_only(monkeypatch):
    sys.modules.setdefault("ta", types.ModuleType("ta"))
    sys.modules.setdefault("pywencai", types.ModuleType("pywencai"))

    stock_data = importlib.import_module("stock_data")
    fetcher = stock_data.StockDataFetcher()

    def fake_get_financial_data(symbol, report_type="income"):
        return pd.DataFrame([{"报告日": "2024-09-30", "字段": f"{symbol}-{report_type}"}])

    monkeypatch.setattr(fetcher.data_source_manager, "get_financial_data", fake_get_financial_data)
    monkeypatch.setattr(
        stock_data.ak,
        "stock_financial_abstract",
        lambda symbol: pd.DataFrame(
            [
                {"指标": "净资产收益率(ROE)", "2024-09-30": "12.5"},
                {"指标": "资产负债率", "2024-09-30": "45.0"},
            ]
        ),
    )

    result = fetcher._get_chinese_financial_data("600519")

    assert result["source"] == "akshare"
    assert result["source_chain"] == ["akshare"]
    assert result["income_statement"][0]["字段"] == "600519-income"
    assert result["balance_sheet"][0]["字段"] == "600519-balance"
    assert result["cash_flow"][0]["字段"] == "600519-cashflow"
