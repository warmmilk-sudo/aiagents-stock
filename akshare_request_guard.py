"""
AkShare 请求保护层
提供随机抖动、指数退避和受控并发，降低高频并发请求触发风控的概率。
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from typing import Any, Callable, Dict, Tuple


_SEMAPHORE_LOCK = threading.Lock()
_SEMAPHORE_CACHE: Dict[int, threading.BoundedSemaphore] = {}


def _get_shared_semaphore(max_concurrency: int) -> threading.BoundedSemaphore:
    normalized = max(1, int(max_concurrency or 1))
    with _SEMAPHORE_LOCK:
        semaphore = _SEMAPHORE_CACHE.get(normalized)
        if semaphore is None:
            semaphore = threading.BoundedSemaphore(normalized)
            _SEMAPHORE_CACHE[normalized] = semaphore
        return semaphore


class AkShareRequestGuard:
    """AkShare 请求包装器。"""

    def __init__(
        self,
        *,
        min_delay: float | None = None,
        max_delay: float | None = None,
        max_retries: int | None = None,
        retry_base_delay: float | None = None,
        retry_backoff: float | None = None,
        retry_jitter: float | None = None,
        max_retry_delay: float | None = None,
        max_concurrency: int | None = None,
        logger: logging.Logger | None = None,
        label: str = "AkShare",
    ) -> None:
        self.min_delay = float(min_delay if min_delay is not None else os.getenv("AKSHARE_GUARD_MIN_DELAY", "0.4"))
        self.max_delay = float(max_delay if max_delay is not None else os.getenv("AKSHARE_GUARD_MAX_DELAY", "1.2"))
        self.max_retries = max(1, int(max_retries if max_retries is not None else os.getenv("AKSHARE_GUARD_MAX_RETRIES", "3")))
        self.retry_base_delay = float(
            retry_base_delay if retry_base_delay is not None else os.getenv("AKSHARE_GUARD_RETRY_BASE_DELAY", "1.0")
        )
        self.retry_backoff = float(
            retry_backoff if retry_backoff is not None else os.getenv("AKSHARE_GUARD_RETRY_BACKOFF", "1.8")
        )
        self.retry_jitter = float(
            retry_jitter if retry_jitter is not None else os.getenv("AKSHARE_GUARD_RETRY_JITTER", "0.6")
        )
        self.max_retry_delay = float(
            max_retry_delay if max_retry_delay is not None else os.getenv("AKSHARE_GUARD_MAX_RETRY_DELAY", "6.0")
        )
        self.max_concurrency = max(
            1,
            int(max_concurrency if max_concurrency is not None else os.getenv("AKSHARE_GUARD_MAX_CONCURRENCY", "2")),
        )
        self.semaphore = _get_shared_semaphore(self.max_concurrency)
        self.logger = logger or logging.getLogger(__name__)
        self.label = label

    def _sleep_before_request(self) -> None:
        upper = max(self.min_delay, self.max_delay)
        lower = min(self.min_delay, self.max_delay)
        if upper <= 0:
            return
        time.sleep(random.uniform(lower, upper))

    def _retry_delay(self, attempt: int) -> float:
        delay = self.retry_base_delay * (self.retry_backoff ** max(0, attempt - 1))
        delay += random.uniform(0.0, max(0.0, self.retry_jitter))
        return min(self.max_retry_delay, delay)

    def call(self, func: Callable[..., Any], *args: Any, request_name: str | None = None, **kwargs: Any) -> Any:
        request_label = request_name or getattr(func, "__name__", "unknown")
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                with self.semaphore:
                    self._sleep_before_request()
                    return func(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self.max_retries:
                    break
                delay = self._retry_delay(attempt)
                self.logger.warning(
                    "[%s] %s 调用失败，第%s/%s次重试前等待 %.2fs: %s",
                    self.label,
                    request_label,
                    attempt,
                    self.max_retries,
                    delay,
                    exc,
                )
                time.sleep(delay)

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"{self.label} {request_label} 调用失败")
