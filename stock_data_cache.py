import io
import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, Optional

import pandas as pd


DB_PATH = "stock_data_cache.db"


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def extract_cache_meta(payload: Any) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    if isinstance(payload, dict):
        return payload.get("_cache_meta")
    attrs = getattr(payload, "attrs", None)
    if isinstance(attrs, dict):
        return attrs.get("_cache_meta")
    return None


def strip_cache_meta(payload: Any) -> Any:
    if isinstance(payload, dict):
        result = dict(payload)
        result.pop("_cache_meta", None)
        return result
    if isinstance(payload, pd.DataFrame):
        result = payload.copy(deep=True)
        attrs = dict(getattr(result, "attrs", {}) or {})
        attrs.pop("_cache_meta", None)
        result.attrs = attrs
        return result
    return payload


class StockDataCacheDB:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_database(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_info_cache (
                    symbol TEXT PRIMARY KEY,
                    market TEXT,
                    payload_json TEXT NOT NULL,
                    source TEXT,
                    fetched_at TEXT NOT NULL,
                    last_success_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_history_cache (
                    symbol TEXT NOT NULL,
                    period TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    adjust TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    row_count INTEGER DEFAULT 0,
                    start_date TEXT,
                    end_date TEXT,
                    source TEXT,
                    fetched_at TEXT NOT NULL,
                    last_success_at TEXT NOT NULL,
                    PRIMARY KEY (symbol, period, interval, adjust)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_financial_cache (
                    symbol TEXT PRIMARY KEY,
                    market TEXT,
                    payload_json TEXT NOT NULL,
                    source TEXT,
                    fetched_at TEXT NOT NULL,
                    last_success_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_quarterly_cache (
                    symbol TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    source TEXT,
                    fetched_at TEXT NOT NULL,
                    last_success_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_stock_history_lookup ON stock_history_cache(symbol, period, interval, adjust)"
            )
            conn.commit()
        finally:
            conn.close()

    def get_stock_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM stock_info_cache WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_stock_info(
        self,
        symbol: str,
        market: str,
        payload_json: str,
        source: str,
        fetched_at: str,
        last_success_at: str,
    ):
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO stock_info_cache
                (symbol, market, payload_json, source, fetched_at, last_success_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    market = excluded.market,
                    payload_json = excluded.payload_json,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at,
                    last_success_at = excluded.last_success_at
                """,
                (symbol, market, payload_json, source, fetched_at, last_success_at),
            )
            conn.commit()
        finally:
            conn.close()

    def get_stock_history(self, symbol: str, period: str, interval: str, adjust: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            row = conn.execute(
                """
                SELECT * FROM stock_history_cache
                WHERE symbol = ? AND period = ? AND interval = ? AND adjust = ?
                """,
                (symbol, period, interval, adjust),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_stock_history(
        self,
        symbol: str,
        period: str,
        interval: str,
        adjust: str,
        payload_json: str,
        row_count: int,
        start_date: Optional[str],
        end_date: Optional[str],
        source: str,
        fetched_at: str,
        last_success_at: str,
    ):
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO stock_history_cache
                (symbol, period, interval, adjust, payload_json, row_count, start_date, end_date, source, fetched_at, last_success_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, period, interval, adjust) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    row_count = excluded.row_count,
                    start_date = excluded.start_date,
                    end_date = excluded.end_date,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at,
                    last_success_at = excluded.last_success_at
                """,
                (
                    symbol,
                    period,
                    interval,
                    adjust,
                    payload_json,
                    row_count,
                    start_date,
                    end_date,
                    source,
                    fetched_at,
                    last_success_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_stock_financial(self, symbol: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM stock_financial_cache WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_stock_financial(
        self,
        symbol: str,
        market: str,
        payload_json: str,
        source: str,
        fetched_at: str,
        last_success_at: str,
    ):
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO stock_financial_cache
                (symbol, market, payload_json, source, fetched_at, last_success_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    market = excluded.market,
                    payload_json = excluded.payload_json,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at,
                    last_success_at = excluded.last_success_at
                """,
                (symbol, market, payload_json, source, fetched_at, last_success_at),
            )
            conn.commit()
        finally:
            conn.close()

    def get_stock_quarterly(self, symbol: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM stock_quarterly_cache WHERE symbol = ?",
                (symbol,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_stock_quarterly(
        self,
        symbol: str,
        payload_json: str,
        source: str,
        fetched_at: str,
        last_success_at: str,
    ):
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO stock_quarterly_cache
                (symbol, payload_json, source, fetched_at, last_success_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at,
                    last_success_at = excluded.last_success_at
                """,
                (symbol, payload_json, source, fetched_at, last_success_at),
            )
            conn.commit()
        finally:
            conn.close()

    def clear_all(self) -> Dict[str, int]:
        conn = self._get_connection()
        counts: Dict[str, int] = {}
        try:
            for table in (
                "stock_info_cache",
                "stock_history_cache",
                "stock_financial_cache",
                "stock_quarterly_cache",
            ):
                row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
                counts[table] = int(row["count"]) if row else 0
                conn.execute(f"DELETE FROM {table}")
            conn.commit()
            counts["total"] = sum(counts.values())
            return counts
        finally:
            conn.close()


class StockDataCacheService:
    def __init__(self, cache_db: Optional[StockDataCacheDB] = None):
        self.db = cache_db or stock_data_cache_db

    def _strip_cache_meta(self, payload: Any) -> Any:
        return strip_cache_meta(payload)

    def _attach_meta(
        self,
        payload: Any,
        *,
        from_cache: bool,
        stale: bool,
        fetched_at: str,
        max_age_seconds: int,
        cache_table: str,
    ) -> Any:
        meta = {
            "from_cache": from_cache,
            "stale": stale,
            "fetched_at": fetched_at,
            "max_age_seconds": max_age_seconds,
            "cache_table": cache_table,
        }
        if isinstance(payload, dict):
            result = dict(payload)
            result["_cache_meta"] = meta
            return result
        if isinstance(payload, pd.DataFrame):
            result = payload.copy(deep=True)
            attrs = dict(getattr(result, "attrs", {}) or {})
            attrs["_cache_meta"] = meta
            result.attrs = attrs
            return result
        return payload

    def _serialize_json_payload(self, payload: Dict[str, Any]) -> str:
        clean_payload = self._strip_cache_meta(payload)
        return json.dumps(clean_payload, ensure_ascii=False, default=str)

    def _deserialize_json_payload(self, payload_json: str) -> Dict[str, Any]:
        return json.loads(payload_json) if payload_json else {}

    def _serialize_dataframe_payload(self, df: pd.DataFrame) -> str:
        clean_df = self._strip_cache_meta(df)
        return clean_df.to_json(orient="split", date_format="iso")

    def _deserialize_dataframe_payload(self, payload_json: str) -> pd.DataFrame:
        df = pd.read_json(io.StringIO(payload_json), orient="split")
        try:
            df.index = pd.to_datetime(df.index)
        except (TypeError, ValueError):
            pass
        return df

    def _is_fresh(self, fetched_at: Optional[str], max_age_seconds: int) -> bool:
        fetched_dt = _parse_time(fetched_at)
        if not fetched_dt:
            return False
        return (datetime.now() - fetched_dt).total_seconds() <= max_age_seconds

    def _normalize_error_result(self, error: Exception) -> Dict[str, Any]:
        return {"error": str(error)}

    def _get_or_fetch(
        self,
        *,
        cache_row: Optional[Dict[str, Any]],
        deserialize_payload: Callable[[str], Any],
        fetch_fn: Callable[[], Any],
        validate_payload: Callable[[Any], bool],
        persist_payload: Callable[[Any, str], None],
        cache_table: str,
        max_age_seconds: int,
        allow_stale_on_failure: bool,
        cache_first: bool,
    ) -> Any:
        cached_payload = None
        cached_fetched_at = None
        cached_is_fresh = False

        if cache_row:
            cached_payload = deserialize_payload(cache_row.get("payload_json", ""))
            cached_fetched_at = cache_row.get("fetched_at") or cache_row.get("last_success_at") or _now_text()
            cached_is_fresh = self._is_fresh(cached_fetched_at, max_age_seconds)

        if cache_first and cached_payload is not None and cached_is_fresh:
            return self._attach_meta(
                cached_payload,
                from_cache=True,
                stale=False,
                fetched_at=cached_fetched_at,
                max_age_seconds=max_age_seconds,
                cache_table=cache_table,
            )

        live_payload = None
        live_error = None
        try:
            live_payload = fetch_fn()
        except Exception as error:
            live_error = error

        if live_error is None and validate_payload(live_payload):
            fetched_at = _now_text()
            persist_payload(live_payload, fetched_at)
            return self._attach_meta(
                live_payload,
                from_cache=False,
                stale=False,
                fetched_at=fetched_at,
                max_age_seconds=max_age_seconds,
                cache_table=cache_table,
            )

        if cached_payload is not None:
            if cache_first or allow_stale_on_failure or cached_is_fresh:
                return self._attach_meta(
                    cached_payload,
                    from_cache=True,
                    stale=not cached_is_fresh,
                    fetched_at=cached_fetched_at or _now_text(),
                    max_age_seconds=max_age_seconds,
                    cache_table=cache_table,
                )

        if live_error is not None:
            return self._normalize_error_result(live_error)

        return live_payload

    def get_stock_info(
        self,
        *,
        symbol: str,
        market: str,
        fetch_fn: Callable[[], Dict[str, Any]],
        max_age_seconds: int,
        allow_stale_on_failure: bool,
        cache_first: bool,
    ) -> Dict[str, Any]:
        row = self.db.get_stock_info(symbol)
        return self._get_or_fetch(
            cache_row=row,
            deserialize_payload=self._deserialize_json_payload,
            fetch_fn=fetch_fn,
            validate_payload=lambda payload: isinstance(payload, dict) and "error" not in payload,
            persist_payload=lambda payload, fetched_at: self.db.upsert_stock_info(
                symbol=symbol,
                market=payload.get("market", market),
                payload_json=self._serialize_json_payload(payload),
                source=str(payload.get("_data_source", "live")),
                fetched_at=fetched_at,
                last_success_at=fetched_at,
            ),
            cache_table="stock_info_cache",
            max_age_seconds=max_age_seconds,
            allow_stale_on_failure=allow_stale_on_failure,
            cache_first=cache_first,
        )

    def get_stock_history(
        self,
        *,
        symbol: str,
        period: str,
        interval: str,
        adjust: str,
        fetch_fn: Callable[[], Any],
        max_age_seconds: int,
        allow_stale_on_failure: bool,
        cache_first: bool,
    ) -> Any:
        normalized_adjust = adjust or ""
        row = self.db.get_stock_history(symbol, period, interval, normalized_adjust)

        def persist(payload: pd.DataFrame, fetched_at: str):
            start_date = None
            end_date = None
            if not payload.empty:
                try:
                    start_date = str(payload.index.min())
                    end_date = str(payload.index.max())
                except Exception:
                    start_date = None
                    end_date = None

            self.db.upsert_stock_history(
                symbol=symbol,
                period=period,
                interval=interval,
                adjust=normalized_adjust,
                payload_json=self._serialize_dataframe_payload(payload),
                row_count=len(payload.index),
                start_date=start_date,
                end_date=end_date,
                source="live",
                fetched_at=fetched_at,
                last_success_at=fetched_at,
            )

        return self._get_or_fetch(
            cache_row=row,
            deserialize_payload=self._deserialize_dataframe_payload,
            fetch_fn=fetch_fn,
            validate_payload=lambda payload: isinstance(payload, pd.DataFrame) and not payload.empty,
            persist_payload=persist,
            cache_table="stock_history_cache",
            max_age_seconds=max_age_seconds,
            allow_stale_on_failure=allow_stale_on_failure,
            cache_first=cache_first,
        )

    def get_stock_financial(
        self,
        *,
        symbol: str,
        market: str,
        fetch_fn: Callable[[], Dict[str, Any]],
        max_age_seconds: int,
        allow_stale_on_failure: bool,
        cache_first: bool,
    ) -> Dict[str, Any]:
        row = self.db.get_stock_financial(symbol)
        return self._get_or_fetch(
            cache_row=row,
            deserialize_payload=self._deserialize_json_payload,
            fetch_fn=fetch_fn,
            validate_payload=lambda payload: isinstance(payload, dict) and "error" not in payload,
            persist_payload=lambda payload, fetched_at: self.db.upsert_stock_financial(
                symbol=symbol,
                market=payload.get("market", market),
                payload_json=self._serialize_json_payload(payload),
                source=str(payload.get("_data_source", "live")),
                fetched_at=fetched_at,
                last_success_at=fetched_at,
            ),
            cache_table="stock_financial_cache",
            max_age_seconds=max_age_seconds,
            allow_stale_on_failure=allow_stale_on_failure,
            cache_first=cache_first,
        )

    def get_stock_quarterly(
        self,
        *,
        symbol: str,
        fetch_fn: Callable[[], Dict[str, Any]],
        max_age_seconds: int,
        allow_stale_on_failure: bool,
        cache_first: bool,
    ) -> Dict[str, Any]:
        row = self.db.get_stock_quarterly(symbol)
        return self._get_or_fetch(
            cache_row=row,
            deserialize_payload=self._deserialize_json_payload,
            fetch_fn=fetch_fn,
            validate_payload=lambda payload: isinstance(payload, dict) and payload.get("data_success", False),
            persist_payload=lambda payload, fetched_at: self.db.upsert_stock_quarterly(
                symbol=symbol,
                payload_json=self._serialize_json_payload(payload),
                source=str(payload.get("source", "live")),
                fetched_at=fetched_at,
                last_success_at=fetched_at,
            ),
            cache_table="stock_quarterly_cache",
            max_age_seconds=max_age_seconds,
            allow_stale_on_failure=allow_stale_on_failure,
            cache_first=cache_first,
        )

    def clear_all(self) -> Dict[str, int]:
        return self.db.clear_all()


stock_data_cache_db = StockDataCacheDB()
stock_data_cache_service = StockDataCacheService(stock_data_cache_db)


def clear_stock_data_cache() -> Dict[str, int]:
    return stock_data_cache_service.clear_all()
