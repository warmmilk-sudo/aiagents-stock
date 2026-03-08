#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDX API integration check script.

This module is intentionally side-effect free on import so it will not break
`unittest discover` in environments without a local TDX service.
Run manually:
    venv\\Scripts\\python.exe test_tdx_api.py
"""

from __future__ import annotations

import os
import sys
from typing import Iterable, Optional, Tuple

import requests
from dotenv import load_dotenv


DEFAULT_TDX_BASE_URL = "http://127.0.0.1:8181"


def _echo(message: str) -> None:
    print(message)


def _request_json(url: str, *, params: Optional[dict] = None, timeout: int = 10):
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _extract_kline_list(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if payload.get("code") == 0 and isinstance(payload.get("data"), dict):
            maybe_list = payload["data"].get("List")
            if isinstance(maybe_list, list):
                return maybe_list
        maybe_data = payload.get("data")
        if isinstance(maybe_data, list):
            return maybe_data
    return []


def _iter_code_candidates() -> Iterable[Tuple[str, str]]:
    return [
        ("SZ000001", "PingAnBank"),
        ("000001", "PingAnBankNumeric"),
        ("SH600000", "SPD"),
        ("600000", "SPDNumeric"),
    ]


def run_tdx_integration_check() -> int:
    load_dotenv()
    base_url = os.getenv("TDX_BASE_URL", DEFAULT_TDX_BASE_URL).strip() or DEFAULT_TDX_BASE_URL

    _echo("=" * 60)
    _echo("TDX API integration check")
    _echo("=" * 60)
    _echo(f"Base URL: {base_url}")

    # 1) Health check
    _echo("\n[1/3] Health endpoint")
    try:
        health_resp = requests.get(f"{base_url}/api/health", timeout=5)
        health_resp.raise_for_status()
        _echo(f"[OK] /api/health -> {health_resp.text}")
    except Exception as exc:
        _echo(f"[ERR] health check failed: {exc}")
        _echo("Hint: start local TDX API service or adjust TDX_BASE_URL in .env")
        return 1

    # 2) Kline endpoint
    _echo("\n[2/3] Kline endpoint")
    selected_code = None
    selected_rows = []
    for code, alias in _iter_code_candidates():
        _echo(f"Try code: {code} ({alias})")
        try:
            payload = _request_json(f"{base_url}/api/kline", params={"code": code, "type": "day"}, timeout=10)
            rows = _extract_kline_list(payload)
            if rows:
                selected_code = code
                selected_rows = rows
                _echo(f"[OK] kline rows={len(rows)}")
                break
            _echo("[WARN] empty kline payload")
        except Exception as exc:
            _echo(f"[WARN] request failed: {exc}")

    if not selected_rows:
        _echo("[ERR] no valid kline payload returned by any candidate code format")
        return 1

    # 3) MA calculation sanity check
    _echo("\n[3/3] MA sanity")
    try:
        import pandas as pd

        df = pd.DataFrame(selected_rows)
        if "close" not in df.columns and "Close" in df.columns:
            df["close"] = df["Close"]
        df["close"] = pd.to_numeric(df.get("close"), errors="coerce")
        df["MA5"] = df["close"].rolling(window=5).mean()
        df["MA20"] = df["close"].rolling(window=20).mean()

        latest = df.iloc[-1]
        _echo(f"Code: {selected_code}")
        _echo(f"Close: {latest['close']}")
        _echo(f"MA5: {latest['MA5']}")
        _echo(f"MA20: {latest['MA20']}")
        if pd.isna(latest["MA5"]) or pd.isna(latest["MA20"]):
            _echo("[WARN] latest MA has NaN, likely due insufficient history rows")
        else:
            _echo("[OK] MA calculation succeeded")
    except Exception as exc:
        _echo(f"[ERR] MA calculation failed: {exc}")
        return 1

    _echo("\n[OK] TDX integration check completed")
    return 0


def main() -> int:
    return run_tdx_integration_check()


if __name__ == "__main__":
    raise SystemExit(main())
