from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as redis_asyncio
from redis.asyncio import Redis

import config


logger = logging.getLogger(__name__)

CACHE_PREFIX = "fastapi-cache"
_redis_client: Redis | None = None


def _normalize_cache_value(value: Any) -> Any:
    if isinstance(value, dict) and "fp" in value:
        return {"fp": value.get("fp")}
    if isinstance(value, (list, tuple)):
        return [_normalize_cache_value(item) for item in value]
    return value


def build_request_cache_key(
    func,
    namespace: str = "",
    *args,
    **kwargs,
) -> str:
    request = kwargs.get("request") or kwargs.get("__fastapi_cache_request")
    response = kwargs.get("response") or kwargs.get("__fastapi_cache_response")
    del response
    session_fingerprint = ""
    filtered_kwargs: dict[str, Any] = {}

    for key, value in kwargs.items():
        if key in {"request", "__fastapi_cache_request", "response", "__fastapi_cache_response"}:
            continue

        normalized_value = _normalize_cache_value(value)
        if isinstance(normalized_value, dict) and "fp" in normalized_value:
            session_fingerprint = str(normalized_value.get("fp") or "")
            continue

        filtered_kwargs[key] = normalized_value

    key_payload = {
        "func": f"{func.__module__}.{func.__qualname__}",
        "method": request.method.lower() if request else "",
        "path": request.url.path if request else "",
        "query": sorted(request.query_params.multi_items()) if request else [],
        "args": [_normalize_cache_value(item) for item in args],
        "kwargs": filtered_kwargs,
        "session_fp": session_fingerprint,
    }
    raw_key = json.dumps(
        key_payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()
    return ":".join(part for part in [namespace, digest] if part)


async def init_cache() -> None:
    global _redis_client

    redis_url = str(getattr(config, "REDIS_URL", "") or "").strip()

    if redis_url:
        try:
            _redis_client = redis_asyncio.from_url(
                redis_url,
                decode_responses=False,
                health_check_interval=30,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            await _redis_client.ping()
            FastAPICache.init(
                RedisBackend(_redis_client),
                prefix=CACHE_PREFIX,
                key_builder=build_request_cache_key,
            )
            logger.info("Redis cache backend initialized.")
            return
        except Exception as exc:
            if _redis_client is not None:
                close_method = getattr(_redis_client, "aclose", None)
                if callable(close_method):
                    await close_method()
                else:
                    await _redis_client.close()
                _redis_client = None
            logger.warning(
                "Redis cache backend unavailable at %s, falling back to in-memory cache: %s: %s",
                redis_url,
                type(exc).__name__,
                exc,
            )

    FastAPICache.init(
        InMemoryBackend(),
        prefix=CACHE_PREFIX,
        key_builder=build_request_cache_key,
    )
    if redis_url:
        logger.info("In-memory cache backend initialized after Redis fallback.")
    else:
        logger.info("In-memory cache backend initialized because REDIS_URL is not configured.")


async def close_cache() -> None:
    global _redis_client

    if _redis_client is not None:
        close_method = getattr(_redis_client, "aclose", None)
        if callable(close_method):
            await close_method()
        else:
            await _redis_client.close()
        _redis_client = None

    reset_method = getattr(FastAPICache, "reset", None)
    if callable(reset_method):
        reset_method()
