"""
Helpers for resolving dynamic pywencai column names.

Examples:
- 总市值
- 总市值[20260304]
- 市盈率(pe)[20260304]
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Any


_SPACES = re.compile(r"\s+")


def _norm(text: str) -> str:
    return _SPACES.sub("", str(text or "")).lower()


def _match_priority(column: str, candidate: str) -> int:
    """
    Return match priority (higher is better):
    4 exact normalized
    3 startswith candidate + [ / (
    2 contains candidate
    0 no match
    """
    col = _norm(column)
    cand = _norm(candidate)

    if not col or not cand:
        return 0
    if col == cand:
        return 4
    if col.startswith(f"{cand}[") or col.startswith(f"{cand}("):
        return 3
    if cand in col:
        return 2
    return 0


def find_column(columns: Sequence[str], candidates: Iterable[str]) -> Optional[str]:
    """Find best-matching column name from candidate aliases."""
    best_col: Optional[str] = None
    best_score = 0

    for col in columns:
        for cand in candidates:
            score = _match_priority(col, cand)
            if score > best_score:
                best_score = score
                best_col = col
            if best_score == 4:
                return best_col
    return best_col


def get_row_value(row: Any, candidates: Iterable[str], default: Any = None) -> Any:
    """
    Resolve value from Series/dict-like row with dynamic column names.
    """
    if row is None:
        return default

    keys: List[str] = list(getattr(row, "index", [])) or list(getattr(row, "keys", lambda: [])())
    if not keys:
        return default

    col = find_column(keys, candidates)
    if not col:
        return default
    return row.get(col, default)

