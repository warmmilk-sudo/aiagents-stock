#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""净利增长选股模块。"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

import pandas as pd
from pywencai_runtime import setup_pywencai_runtime_env

setup_pywencai_runtime_env()
import pywencai

from selector_filter_utils import (
    SZ_A_PREFIXES,
    convert_result_to_dataframe,
    filter_board_flags,
    filter_code_prefixes,
    filter_numeric_range,
    normalize_stock_code,
    parse_numeric_value,
    sort_dataframe,
)


class ProfitGrowthSelector:
    """净利增长选股器。"""

    PROFIT_GROWTH_COLUMNS = ("净利润增长率", "净利润同比增长率")
    TURNOVER_COLUMNS = ("成交额",)
    PRICE_COLUMNS = ("股价", "最新价")

    SORT_OPTIONS = {
        "成交额升序": (TURNOVER_COLUMNS, True, "成交额从小到大排名"),
        "成交额降序": (TURNOVER_COLUMNS, False, "成交额从大到小排名"),
        "净利润增长率降序": (PROFIT_GROWTH_COLUMNS, False, "净利润增长率从大到小排名"),
        "股价升序": (PRICE_COLUMNS, True, "股价从小到大排名"),
    }

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.raw_data: Optional[pd.DataFrame] = None
        self.selected_stocks: Optional[pd.DataFrame] = None

    def get_profit_growth_stocks(
        self,
        top_n: int = 5,
        *,
        min_profit_growth: float = 10.0,
        min_turnover_yi: Optional[float] = None,
        max_turnover_yi: Optional[float] = None,
        sort_by: str = "成交额升序",
        exclude_st: bool = True,
        exclude_kcb: bool = True,
        exclude_cyb: bool = True,
    ) -> Tuple[bool, Optional[pd.DataFrame], str]:
        """获取符合净利增长策略的股票。"""
        filters = {
            "min_profit_growth": min_profit_growth,
            "min_turnover_yi": min_turnover_yi,
            "max_turnover_yi": max_turnover_yi,
            "sort_by": sort_by,
            "exclude_st": exclude_st,
            "exclude_kcb": exclude_kcb,
            "exclude_cyb": exclude_cyb,
        }

        try:
            query = self._build_query(filters)
            self.logger.info("开始执行净利增长选股: %s", query)

            result = pywencai.get(query=query, loop=True)
            df_result = convert_result_to_dataframe(result)
            if df_result is None or df_result.empty:
                return False, None, "未找到符合条件的股票"

            self.raw_data = df_result.copy()
            filtered = self._apply_post_filters(df_result, filters)
            if filtered.empty:
                return False, None, "高级筛选条件过严，未筛到符合条件的股票"

            selected = self._sort_result(filtered, sort_by).head(top_n).reset_index(drop=True)
            self.selected_stocks = selected
            return True, selected, f"成功获取 {len(selected)} 只股票"
        except ImportError:
            error_msg = "pywencai 模块未安装，请执行 pip install pywencai"
            self.logger.error(error_msg)
            return False, None, error_msg
        except Exception as exc:
            error_msg = f"选股失败: {exc}"
            self.logger.error(error_msg, exc_info=True)
            return False, None, error_msg

    def _build_query(self, filters: dict) -> str:
        parts = [
            (
                f"净利润增长率>={filters['min_profit_growth']:.2f}%"
                f"或净利润同比增长率>={filters['min_profit_growth']:.2f}%"
            ),
            "深圳A股",
        ]

        if filters["exclude_kcb"]:
            parts.append("非科创板")
        if filters["exclude_cyb"]:
            parts.append("非创业板")
        if filters["exclude_st"]:
            parts.append("非ST")
        if filters["min_turnover_yi"] is not None:
            parts.append(f"成交额>={filters['min_turnover_yi']:.2f}亿")
        if filters["max_turnover_yi"] is not None:
            parts.append(f"成交额<={filters['max_turnover_yi']:.2f}亿")

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
        result = filter_code_prefixes(
            result,
            SZ_A_PREFIXES if not filters["exclude_cyb"] else ("000", "001", "002", "003"),
        )
        result = filter_numeric_range(result, self.PROFIT_GROWTH_COLUMNS, min_value=filters["min_profit_growth"])
        if filters["min_turnover_yi"] is not None or filters["max_turnover_yi"] is not None:
            result = filter_numeric_range(
                result,
                self.TURNOVER_COLUMNS,
                min_value=filters["min_turnover_yi"] * 1e8 if filters["min_turnover_yi"] is not None else None,
                max_value=filters["max_turnover_yi"] * 1e8 if filters["max_turnover_yi"] is not None else None,
            )
        return result

    def _sort_result(self, df: pd.DataFrame, sort_by: str) -> pd.DataFrame:
        candidates, ascending, _ = self.SORT_OPTIONS.get(sort_by, self.SORT_OPTIONS["成交额升序"])
        return sort_dataframe(df, candidates, ascending=ascending)

    def format_stock_info(self, df: pd.DataFrame) -> str:
        """格式化股票信息为文本。"""
        if df is None or df.empty:
            return "无数据"

        lines = []
        for idx, row in df.iterrows():
            stock_code = row.get("股票代码", "N/A")
            stock_name = row.get("股票简称", "N/A")
            profit_growth = row.get("净利润增长率", row.get("净利润同比增长率", "N/A"))
            turnover = row.get("成交额", row.get("成交额[20241213]", "N/A"))

            line = f"{idx + 1}. {stock_code} {stock_name}"
            details = []

            profit_growth_value = parse_numeric_value(profit_growth)
            if profit_growth_value is not None:
                details.append(f"净利增长:{profit_growth_value:.2f}%")

            turnover_value = parse_numeric_value(turnover)
            if turnover_value is not None:
                details.append(f"成交额:{turnover_value / 1e8:.2f}亿")

            if details:
                line += f" - {', '.join(details)}"
            lines.append(line)

        return "\n".join(lines)

    def get_stock_codes(self) -> list[str]:
        """获取当前选中股票代码列表。"""
        if self.selected_stocks is None or self.selected_stocks.empty:
            return []
        return [normalize_stock_code(code) for code in self.selected_stocks["股票代码"].tolist()]


profit_growth_selector = ProfitGrowthSelector()
