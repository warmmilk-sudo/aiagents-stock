"""
Unified data-source policy helpers.

Rule:
- If TUSHARE_TOKEN is configured, prefer Tushare first.
- If Tushare fails or returns empty data, fallback to other providers.
"""

from __future__ import annotations

import config  # 触发 load_dotenv

import os
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Any


def _is_non_empty(result: Any) -> bool:
    """Generic non-empty validator for dict/list/DataFrame-like results."""
    if result is None:
        return False
    if hasattr(result, "empty"):
        try:
            return not bool(result.empty)
        except Exception:
            return True
    if isinstance(result, (list, tuple, set, dict)):
        return len(result) > 0
    return True


class DataSourcePolicy:
    """Centralized source-priority and fallback executor."""

    def __init__(self) -> None:
        self.tushare_token = os.getenv("TUSHARE_TOKEN", "").strip()
        self.tushare_url = os.getenv("TUSHARE_URL", "https://api.tushare.pro").strip()
        self.tushare_configured = bool(self.tushare_token)

        self.tushare_available = False
        self.tushare_api = None
        self.tushare_init_error: Optional[str] = None

        self._init_tushare()

    @property
    def prefer_tushare(self) -> bool:
        """Policy switch: available client means Tushare is preferred."""
        return self.tushare_available

    def _init_tushare(self) -> None:
        """Initialize reusable tushare pro client with old/new SDK compatibility."""
        if not self.tushare_configured:
            return

        try:
            import tushare as ts

            ts.set_token(self.tushare_token)
            try:
                pro = ts.pro_api(token=self.tushare_token, server=self.tushare_url)
            except TypeError:
                pro = ts.pro_api(token=self.tushare_token)
                if self.tushare_url and hasattr(pro, "_DataApi__http_url"):
                    pro._DataApi__http_url = self.tushare_url

            self.tushare_api = pro
            self.tushare_available = True
        except Exception as exc:
            self.tushare_available = False
            self.tushare_init_error = f"{type(exc).__name__}: {exc}"

    def execute_chain(
        self,
        sources: Iterable[Tuple[str, Callable[[], Any]]],
        validator: Optional[Callable[[Any], bool]] = None,
    ) -> Tuple[Any, Optional[str], List[str], Dict[str, str]]:
        """
        Execute providers in order until one succeeds.

        Returns:
            (result, hit_source, source_chain, error_detail)
        """
        source_chain: List[str] = []
        errors: Dict[str, str] = {}
        checker = validator or _is_non_empty

        for name, func in sources:
            source_chain.append(name)
            try:
                result = func()
                if checker(result):
                    return result, name, source_chain, errors
                errors[name] = "empty result"
            except Exception as exc:
                errors[name] = f"{type(exc).__name__}: {exc}"

        return None, None, source_chain, errors


# Shared singleton for lightweight modules.
policy = DataSourcePolicy()
