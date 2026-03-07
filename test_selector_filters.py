#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unit tests for selector filtering helpers and picker defaults."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from low_price_bull_selector import LowPriceBullSelector
from profit_growth_selector import ProfitGrowthSelector
from selector_filter_utils import parse_numeric_value
from small_cap_selector import SmallCapSelector
from value_stock_selector import ValueStockSelector


class SelectorFilterUtilsTests(unittest.TestCase):
    def test_parse_numeric_value_supports_common_units(self):
        self.assertEqual(parse_numeric_value("12.5%"), 12.5)
        self.assertEqual(parse_numeric_value("1,234.5"), 1234.5)
        self.assertEqual(parse_numeric_value("3.2亿"), 320000000.0)
        self.assertEqual(parse_numeric_value("4500万"), 45000000.0)
        self.assertIsNone(parse_numeric_value("--"))


class LowPriceBullSelectorTests(unittest.TestCase):
    def test_default_query_and_filters(self):
        fake_df = pd.DataFrame(
            [
                {"股票代码": "000001.SZ", "股票简称": "平安银行", "股价": 9.5, "净利润增长率": "120%", "成交额": "2亿", "总市值": "30亿"},
                {"股票代码": "300001.SZ", "股票简称": "特锐德", "股价": 8.0, "净利润增长率": "180%", "成交额": "1亿", "总市值": "20亿"},
                {"股票代码": "600001.SH", "股票简称": "邯郸钢铁", "股价": 6.0, "净利润增长率": "150%", "成交额": "0.8亿", "总市值": "15亿"},
                {"股票代码": "000002.SZ", "股票简称": "万科A", "股价": 12.0, "净利润增长率": "300%", "成交额": "0.6亿", "总市值": "40亿"},
            ]
        )

        with patch("low_price_bull_selector.pywencai.get", return_value=fake_df) as mocked_get:
            success, df, message = LowPriceBullSelector().get_low_price_stocks(top_n=2)

        self.assertTrue(success, message)
        self.assertEqual(df["股票代码"].tolist(), ["600001.SH", "000001.SZ"])
        query = mocked_get.call_args.kwargs["query"]
        self.assertIn("股价<=10.00元", query)
        self.assertIn("净利润增长率>=100.00%", query)
        self.assertIn("成交额从小到大排名", query)


class SmallCapSelectorTests(unittest.TestCase):
    def test_default_query_and_filters(self):
        fake_df = pd.DataFrame(
            [
                {"股票代码": "000001.SZ", "股票简称": "平安银行", "总市值": "40亿", "营收增长率": "15%", "净利润增长率": "120%", "股价": 9.0},
                {"股票代码": "688001.SH", "股票简称": "华兴源创", "总市值": "20亿", "营收增长率": "30%", "净利润增长率": "200%", "股价": 18.0},
                {"股票代码": "002001.SZ", "股票简称": "新和成", "总市值": "25亿", "营收增长率": "8%", "净利润增长率": "130%", "股价": 12.0},
                {"股票代码": "600001.SH", "股票简称": "邯郸钢铁", "总市值": "18亿", "营收增长率": "25%", "净利润增长率": "150%", "股价": 6.0},
            ]
        )

        with patch("small_cap_selector.pywencai.get", return_value=fake_df) as mocked_get:
            success, df, message = SmallCapSelector().get_small_cap_stocks(top_n=2)

        self.assertTrue(success, message)
        self.assertEqual(df["股票代码"].tolist(), ["600001.SH", "000001.SZ"])
        query = mocked_get.call_args.kwargs["query"]
        self.assertIn("总市值<=50.00亿", query)
        self.assertIn("营收增长率>=10.00%", query)
        self.assertIn("总市值从小到大排名", query)


class ProfitGrowthSelectorTests(unittest.TestCase):
    def test_turnover_filter_and_shenzhen_scope(self):
        fake_df = pd.DataFrame(
            [
                {"股票代码": "000001.SZ", "股票简称": "平安银行", "净利润增长率": "18%", "成交额": "1.5亿", "股价": 10.0},
                {"股票代码": "002001.SZ", "股票简称": "新和成", "净利润增长率": "25%", "成交额": "0.6亿", "股价": 16.0},
                {"股票代码": "300001.SZ", "股票简称": "特锐德", "净利润增长率": "30%", "成交额": "0.8亿", "股价": 20.0},
                {"股票代码": "600001.SH", "股票简称": "邯郸钢铁", "净利润增长率": "50%", "成交额": "0.7亿", "股价": 6.0},
            ]
        )

        with patch("profit_growth_selector.pywencai.get", return_value=fake_df) as mocked_get:
            success, df, message = ProfitGrowthSelector().get_profit_growth_stocks(
                top_n=3,
                min_profit_growth=15.0,
                min_turnover_yi=0.5,
                max_turnover_yi=1.6,
            )

        self.assertTrue(success, message)
        self.assertEqual(df["股票代码"].tolist(), ["002001.SZ", "000001.SZ"])
        query = mocked_get.call_args.kwargs["query"]
        self.assertIn("深圳A股", query)
        self.assertIn("成交额>=0.50亿", query)
        self.assertIn("成交额<=1.60亿", query)


class ValueStockSelectorTests(unittest.TestCase):
    def test_default_query_and_filters(self):
        fake_df = pd.DataFrame(
            [
                {"股票代码": "600001.SH", "股票简称": "邯郸钢铁", "市盈率": 8.0, "市净率": 0.8, "股息率": "2.5%", "资产负债率": "20%", "流通市值": "30亿"},
                {"股票代码": "688001.SH", "股票简称": "华兴源创", "市盈率": 10.0, "市净率": 1.0, "股息率": "1.5%", "资产负债率": "15%", "流通市值": "10亿"},
                {"股票代码": "000001.SZ", "股票简称": "平安银行", "市盈率": 12.0, "市净率": 2.0, "股息率": "3%", "资产负债率": "25%", "流通市值": "20亿"},
                {"股票代码": "002001.SZ", "股票简称": "新和成", "市盈率": 15.0, "市净率": 1.1, "股息率": "0.5%", "资产负债率": "18%", "流通市值": "15亿"},
            ]
        )

        with patch("value_stock_selector.pywencai.get", return_value=fake_df) as mocked_get:
            success, df, message = ValueStockSelector().get_value_stocks(top_n=2)

        self.assertTrue(success, message)
        self.assertEqual(df["股票代码"].tolist(), ["600001.SH"])
        query = mocked_get.call_args.kwargs["query"]
        self.assertIn("市盈率小于等于20.00", query)
        self.assertIn("市净率小于等于1.50", query)
        self.assertIn("按流通市值由小到大排名", query)


class LowPriceBullUiTests(unittest.TestCase):
    def test_low_price_explanation_restored(self):
        text = Path("low_price_bull_ui.py").read_text(encoding="utf-8")
        self.assertIn("股价 < 10元", text)
        self.assertNotIn("默认聚焦低价、高增长", text)


if __name__ == "__main__":
    unittest.main()
