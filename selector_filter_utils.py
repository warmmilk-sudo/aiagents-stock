#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared helpers for stock selector result normalization and filtering."""

from __future__ import annotations

import re
from typing import Iterable, Optional, Sequence

import pandas as pd


CODE_COLUMNS = ("股票代码", "证券代码", "代码")
NAME_COLUMNS = ("股票简称", "证券简称", "名称")
HS_A_PREFIXES = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
SZ_A_PREFIXES = ("000", "001", "002", "003", "300", "301")


def convert_result_to_dataframe(result) -> Optional[pd.DataFrame]:
    """Convert a pywencai response into a DataFrame."""
    try:
        if isinstance(result, pd.DataFrame):
            return result
        if isinstance(result, dict):
            if "data" in result:
                return pd.DataFrame(result["data"])
            if "result" in result:
                return pd.DataFrame(result["result"])
            return pd.DataFrame(result)
        if isinstance(result, list):
            return pd.DataFrame(result)
        return None
    except Exception:
        return None


def find_matching_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Find the first exact or fuzzy matching column name."""
    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    for column in df.columns:
        for candidate in candidates:
            if candidate in column:
                return column
    return None


def parse_numeric_value(value) -> Optional[float]:
    """Parse numbers like '12.3亿', '1200万', '15%', '1,234.5'."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text.lower() in {"nan", "none", "n/a", "--"}:
        return None

    multiplier = 1.0
    for suffix, factor in (("亿元", 1e8), ("亿", 1e8), ("万元", 1e4), ("万", 1e4), ("元", 1.0)):
        if text.endswith(suffix):
            multiplier = factor
            text = text[: -len(suffix)]
            break

    try:
        return float(text) * multiplier
    except ValueError:
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        try:
            return float(match.group(0)) * multiplier
        except ValueError:
            return None


def filter_numeric_range(
    df: pd.DataFrame,
    candidates: Sequence[str],
    *,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> pd.DataFrame:
    """Filter a DataFrame by a numeric range on the first matching column."""
    column = find_matching_column(df, candidates)
    if column is None:
        return df

    numeric_values = df[column].apply(parse_numeric_value)
    mask = numeric_values.notna()
    if min_value is not None:
        mask &= numeric_values >= min_value
    if max_value is not None:
        mask &= numeric_values <= max_value
    return df.loc[mask].copy()


def sort_dataframe(
    df: pd.DataFrame,
    candidates: Sequence[str],
    *,
    ascending: bool,
) -> pd.DataFrame:
    """Sort a DataFrame by the first matching numeric column."""
    column = find_matching_column(df, candidates)
    if column is None:
        return df.reset_index(drop=True)

    numeric_values = df[column].apply(parse_numeric_value)
    sorted_df = df.assign(_sort_value=numeric_values).sort_values(
        by="_sort_value",
        ascending=ascending,
        na_position="last",
    )
    return sorted_df.drop(columns="_sort_value").reset_index(drop=True)


def normalize_stock_code(value) -> str:
    """Normalize codes like '000001.SZ' or 'SZ000001' into '000001'."""
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip().upper()
    if "." in text:
        text = text.split(".", 1)[0]

    digits = re.findall(r"\d+", text)
    if digits:
        joined = "".join(digits)
        if len(joined) >= 6:
            return joined[-6:]
        return joined.zfill(6)
    return text


def filter_code_prefixes(
    df: pd.DataFrame,
    allowed_prefixes: Iterable[str],
    *,
    code_candidates: Sequence[str] = CODE_COLUMNS,
) -> pd.DataFrame:
    """Keep rows whose normalized stock code starts with one of the prefixes."""
    column = find_matching_column(df, code_candidates)
    if column is None:
        return df

    prefixes = tuple(allowed_prefixes)
    normalized = df[column].apply(normalize_stock_code)
    mask = normalized.str.startswith(prefixes)
    return df.loc[mask].copy()


def filter_board_flags(
    df: pd.DataFrame,
    *,
    exclude_st: bool = False,
    exclude_kcb: bool = False,
    exclude_cyb: bool = False,
    only_hs_a: bool = False,
    code_candidates: Sequence[str] = CODE_COLUMNS,
    name_candidates: Sequence[str] = NAME_COLUMNS,
) -> pd.DataFrame:
    """Apply common A-share board filters."""
    result = df.copy()

    if exclude_st:
        name_column = find_matching_column(result, name_candidates)
        if name_column is not None:
            names = result[name_column].fillna("").astype(str).str.upper().str.replace(" ", "", regex=False)
            result = result.loc[~names.str.contains("ST", na=False)].copy()

    code_column = find_matching_column(result, code_candidates)
    if code_column is None:
        return result

    normalized = result[code_column].apply(normalize_stock_code)
    mask = normalized.ne("")

    if only_hs_a:
        mask &= normalized.str.startswith(HS_A_PREFIXES)
    if exclude_kcb:
        mask &= ~normalized.str.startswith(("688", "689"))
    if exclude_cyb:
        mask &= ~normalized.str.startswith(("300", "301"))

    return result.loc[mask].copy()
