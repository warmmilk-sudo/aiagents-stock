#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""低估值选股模块。"""

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


class ValueStockSelector:
    """低估值选股器。"""

    PE_COLUMNS = ("市盈率", "市盈率(动)", "市盈率TTM")
    PB_COLUMNS = ("市净率",)
    DIVIDEND_YIELD_COLUMNS = ("股息率", "股息率TTM")
    DEBT_RATIO_COLUMNS = ("资产负债率",)
    FLOAT_CAP_COLUMNS = ("流通市值",)

    SORT_OPTIONS = {
        "流通市值升序": (FLOAT_CAP_COLUMNS, True, "按流通市值由小到大排名"),
        "PE升序": (PE_COLUMNS, True, "按市盈率从小到大排名"),
        "PB升序": (PB_COLUMNS, True, "按市净率从小到大排名"),
        "股息率降序": (DIVIDEND_YIELD_COLUMNS, False, "按股息率从大到小排名"),
        "资产负债率升序": (DEBT_RATIO_COLUMNS, True, "按资产负债率从小到大排名"),
    }

    def __init__(self) -> None:
        self.raw_data: Optional[pd.DataFrame] = None
        self.selected_stocks: Optional[pd.DataFrame] = None

    def get_value_stocks(
        self,
        top_n: int = 10,
        *,
        max_pe: float = 20.0,
        max_pb: float = 1.5,
        min_dividend_yield: float = 1.0,
        max_debt_ratio: float = 30.0,
        min_float_cap_yi: Optional[float] = None,
        max_float_cap_yi: Optional[float] = None,
        sort_by: str = "流通市值升序",
        exclude_st: bool = True,
        exclude_kcb: bool = True,
        exclude_cyb: bool = True,
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """获取低估值优质股票。"""
        filters = {
            "max_pe": max_pe,
            "max_pb": max_pb,
            "min_dividend_yield": min_dividend_yield,
            "max_debt_ratio": max_debt_ratio,
            "min_float_cap_yi": min_float_cap_yi,
            "max_float_cap_yi": max_float_cap_yi,
            "sort_by": sort_by,
            "exclude_st": exclude_st,
            "exclude_kcb": exclude_kcb,
            "exclude_cyb": exclude_cyb,
        }

        try:
            query = self._build_query(filters)
            result = pywencai.get(query=query, loop=True)
            if result is None:
                return False, None, "问财接口返回为空，请稍后重试"

            df_result = convert_result_to_dataframe(result)
            if df_result is None or df_result.empty:
                return False, None, "未获取到符合条件的股票数据"

            self.raw_data = df_result.copy()
            filtered = self._apply_post_filters(df_result, filters)
            if filtered.empty:
                return False, None, "高级筛选条件过严，未筛到符合条件的股票"

            selected = self._sort_result(filtered, sort_by).head(top_n).reset_index(drop=True)
            self.selected_stocks = selected
            return True, selected, f"成功筛选出 {len(selected)} 只低估值优质股票"
        except Exception as exc:
            return False, None, f"获取数据失败: {exc}"

    def _build_query(self, filters: dict) -> str:
        parts = [
            f"市盈率小于等于{filters['max_pe']:.2f}",
            f"市净率小于等于{filters['max_pb']:.2f}",
            f"股息率大于等于{filters['min_dividend_yield']:.2f}%",
            f"资产负债率小于等于{filters['max_debt_ratio']:.2f}%",
        ]

        if filters["exclude_st"]:
            parts.append("非ST")
        if filters["exclude_kcb"]:
            parts.append("非科创板")
        if filters["exclude_cyb"]:
            parts.append("非创业板")
        if filters["min_float_cap_yi"] is not None:
            parts.append(f"流通市值>={filters['min_float_cap_yi']:.2f}亿")
        if filters["max_float_cap_yi"] is not None:
            parts.append(f"流通市值<={filters['max_float_cap_yi']:.2f}亿")

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
            only_hs_a=False,
        )
        result = filter_numeric_range(result, self.PE_COLUMNS, max_value=filters["max_pe"])
        result = filter_numeric_range(result, self.PB_COLUMNS, max_value=filters["max_pb"])
        result = filter_numeric_range(result, self.DIVIDEND_YIELD_COLUMNS, min_value=filters["min_dividend_yield"])
        result = filter_numeric_range(result, self.DEBT_RATIO_COLUMNS, max_value=filters["max_debt_ratio"])
        if filters["min_float_cap_yi"] is not None or filters["max_float_cap_yi"] is not None:
            result = filter_numeric_range(
                result,
                self.FLOAT_CAP_COLUMNS,
                min_value=filters["min_float_cap_yi"] * 1e8 if filters["min_float_cap_yi"] is not None else None,
                max_value=filters["max_float_cap_yi"] * 1e8 if filters["max_float_cap_yi"] is not None else None,
            )
        return result

    def _sort_result(self, df: pd.DataFrame, sort_by: str) -> pd.DataFrame:
        candidates, ascending, _ = self.SORT_OPTIONS.get(sort_by, self.SORT_OPTIONS["流通市值升序"])
        return sort_dataframe(df, candidates, ascending=ascending)

    def get_stock_codes(self) -> list[str]:
        """获取选中股票的代码列表。"""
        if self.selected_stocks is None or self.selected_stocks.empty:
            return []
        return [normalize_stock_code(code) for code in self.selected_stocks["股票代码"].tolist()]
