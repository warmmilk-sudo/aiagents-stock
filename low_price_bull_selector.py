#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""低价擒牛选股数据获取与过滤。"""

from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd
import pywencai

from selector_filter_utils import (
    convert_result_to_dataframe,
    filter_board_flags,
    filter_numeric_range,
    normalize_stock_code,
    sort_dataframe,
)


class LowPriceBullSelector:
    """低价高成长选股器。"""

    PRICE_COLUMNS = ("股价", "最新价")
    GROWTH_COLUMNS = ("净利润增长率", "净利润同比增长率")
    TURNOVER_COLUMNS = ("成交额",)
    MARKET_CAP_COLUMNS = ("总市值",)

    SORT_OPTIONS = {
        "成交额升序": (TURNOVER_COLUMNS, True, "成交额从小到大排名"),
        "成交额降序": (TURNOVER_COLUMNS, False, "成交额从大到小排名"),
        "净利润增长率降序": (GROWTH_COLUMNS, False, "净利润增长率从大到小排名"),
        "股价升序": (PRICE_COLUMNS, True, "股价从小到大排名"),
        "总市值升序": (MARKET_CAP_COLUMNS, True, "总市值从小到大排名"),
    }

    def __init__(self) -> None:
        self.raw_data: Optional[pd.DataFrame] = None
        self.selected_stocks: Optional[pd.DataFrame] = None

    def get_low_price_stocks(
        self,
        top_n: int = 5,
        *,
        max_price: float = 10.0,
        min_profit_growth: float = 100.0,
        min_turnover_yi: Optional[float] = None,
        max_turnover_yi: Optional[float] = None,
        min_market_cap_yi: Optional[float] = None,
        max_market_cap_yi: Optional[float] = None,
        sort_by: str = "成交额升序",
        exclude_st: bool = True,
        exclude_kcb: bool = True,
        exclude_cyb: bool = True,
        only_hs_a: bool = True,
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """获取低价高成长股票并按高级参数过滤。"""
        filters = {
            "max_price": max_price,
            "min_profit_growth": min_profit_growth,
            "min_turnover_yi": min_turnover_yi,
            "max_turnover_yi": max_turnover_yi,
            "min_market_cap_yi": min_market_cap_yi,
            "max_market_cap_yi": max_market_cap_yi,
            "sort_by": sort_by,
            "exclude_st": exclude_st,
            "exclude_kcb": exclude_kcb,
            "exclude_cyb": exclude_cyb,
            "only_hs_a": only_hs_a,
        }

        try:
            query = self._build_query(filters)
            result = pywencai.get(query=query, loop=True)
            if result is None:
                return False, None, "问财未返回数据，请稍后重试"

            df_result = convert_result_to_dataframe(result)
            if df_result is None or df_result.empty:
                return False, None, "未获取到符合条件的股票数据"

            self.raw_data = df_result.copy()

            filtered = self._apply_post_filters(df_result, filters)
            if filtered.empty:
                return False, None, "高级筛选条件过严，未筛到符合条件的股票"

            selected = self._sort_result(filtered, sort_by).head(top_n).reset_index(drop=True)
            self.selected_stocks = selected
            return True, selected, f"成功筛选出 {len(selected)} 只低价高成长股票"
        except Exception as exc:
            return False, None, f"获取数据失败: {exc}"

    def _build_query(self, filters: dict) -> str:
        parts = [
            f"股价<={filters['max_price']:.2f}元",
            (
                f"净利润增长率>={filters['min_profit_growth']:.2f}%"
                f"或净利润同比增长率>={filters['min_profit_growth']:.2f}%"
            ),
        ]

        if filters["exclude_st"]:
            parts.append("非ST")
        if filters["exclude_kcb"]:
            parts.append("非科创板")
        if filters["exclude_cyb"]:
            parts.append("非创业板")
        if filters["only_hs_a"]:
            parts.append("沪深A股")
        if filters["min_turnover_yi"] is not None:
            parts.append(f"成交额>={filters['min_turnover_yi']:.2f}亿")
        if filters["max_turnover_yi"] is not None:
            parts.append(f"成交额<={filters['max_turnover_yi']:.2f}亿")
        if filters["min_market_cap_yi"] is not None:
            parts.append(f"总市值>={filters['min_market_cap_yi']:.2f}亿")
        if filters["max_market_cap_yi"] is not None:
            parts.append(f"总市值<={filters['max_market_cap_yi']:.2f}亿")

        sort_option = self.SORT_OPTIONS.get(filters["sort_by"])
        if sort_option:
            parts.append(sort_option[2])

        return "，".join(parts)

    def _apply_post_filters(self, df: pd.DataFrame, filters: dict) -> pd.DataFrame:
        result = filter_board_flags(
            df,
            exclude_st=filters["exclude_st"],
            exclude_kcb=filters["exclude_kcb"],
            exclude_cyb=filters["exclude_cyb"],
            only_hs_a=filters["only_hs_a"],
        )
        result = filter_numeric_range(result, self.PRICE_COLUMNS, max_value=filters["max_price"])
        result = filter_numeric_range(result, self.GROWTH_COLUMNS, min_value=filters["min_profit_growth"])

        if filters["min_turnover_yi"] is not None or filters["max_turnover_yi"] is not None:
            result = filter_numeric_range(
                result,
                self.TURNOVER_COLUMNS,
                min_value=filters["min_turnover_yi"] * 1e8 if filters["min_turnover_yi"] is not None else None,
                max_value=filters["max_turnover_yi"] * 1e8 if filters["max_turnover_yi"] is not None else None,
            )
        if filters["min_market_cap_yi"] is not None or filters["max_market_cap_yi"] is not None:
            result = filter_numeric_range(
                result,
                self.MARKET_CAP_COLUMNS,
                min_value=filters["min_market_cap_yi"] * 1e8 if filters["min_market_cap_yi"] is not None else None,
                max_value=filters["max_market_cap_yi"] * 1e8 if filters["max_market_cap_yi"] is not None else None,
            )
        return result

    def _sort_result(self, df: pd.DataFrame, sort_by: str) -> pd.DataFrame:
        candidates, ascending, _ = self.SORT_OPTIONS.get(sort_by, self.SORT_OPTIONS["成交额升序"])
        return sort_dataframe(df, candidates, ascending=ascending)

    def get_stock_codes(self) -> list[str]:
        """获取当前选中股票代码列表。"""
        if self.selected_stocks is None or self.selected_stocks.empty:
            return []

        return [normalize_stock_code(code) for code in self.selected_stocks["股票代码"].tolist()]
