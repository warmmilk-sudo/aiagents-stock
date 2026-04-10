import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

from investment_db_utils import connect_sqlite


DEFAULT_SMART_MONITOR_CACHE_DB = "smart_monitor_cache.db"


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


class SmartMonitorIndicatorCacheDB:
    def __init__(self, db_path: str = DEFAULT_SMART_MONITOR_CACHE_DB):
        self.db_path = db_path or DEFAULT_SMART_MONITOR_CACHE_DB
        self._init_database()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _init_database(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tushare_daily_indicator_cache (
                    cache_key TEXT PRIMARY KEY,
                    stock_code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT,
                    expires_at REAL NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tushare_daily_indicator_trade_date ON tushare_daily_indicator_cache(stock_code, trade_date)"
            )
            conn.commit()
        finally:
            conn.close()

    def get_daily_indicator_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT cache_key, stock_code, trade_date, status, payload_json, expires_at, updated_at
                FROM tushare_daily_indicator_cache
                WHERE cache_key = ?
                """,
                (cache_key,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_daily_indicator_cache(
        self,
        *,
        cache_key: str,
        stock_code: str,
        trade_date: str,
        status: str,
        payload: Optional[Dict[str, Any]],
        expires_at: float,
    ) -> None:
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True) if payload is not None else None
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO tushare_daily_indicator_cache
                (cache_key, stock_code, trade_date, status, payload_json, expires_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    stock_code = excluded.stock_code,
                    trade_date = excluded.trade_date,
                    status = excluded.status,
                    payload_json = excluded.payload_json,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (
                    cache_key,
                    stock_code,
                    trade_date,
                    status,
                    payload_json,
                    float(expires_at),
                    _now_text(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def delete_cache_entry(self, cache_key: str) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "DELETE FROM tushare_daily_indicator_cache WHERE cache_key = ?",
                (cache_key,),
            )
            conn.commit()
        finally:
            conn.close()
