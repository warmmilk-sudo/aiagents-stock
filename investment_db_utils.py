import json
import os
import sqlite3
import threading
from datetime import datetime
from typing import Callable, Iterable, Optional, TypeVar


CANONICAL_INVESTMENT_DB = "investment.db"
DEFAULT_ACCOUNT_NAME = "zfy"
SUPPORTED_ACCOUNT_NAMES = (DEFAULT_ACCOUNT_NAME,)
AGGREGATE_ACCOUNT_NAME = "全部账户"
METADATA_TABLE = "investment_metadata"
SQLITE_TIMEOUT_SECONDS = 30
SQLITE_BUSY_TIMEOUT_MILLISECONDS = 30000
_T = TypeVar("_T")

# Monitoring-related writes are serialized in-process to reduce SQLite lock contention.
MONITORING_WRITE_LOCK = threading.RLock()


def resolve_investment_db_path(seed_db_path: Optional[str] = None) -> str:
    if not seed_db_path:
        return CANONICAL_INVESTMENT_DB
    directory = os.path.dirname(seed_db_path)
    basename = os.path.basename(seed_db_path)
    if basename == CANONICAL_INVESTMENT_DB:
        return seed_db_path
    return os.path.join(directory, CANONICAL_INVESTMENT_DB) if directory else CANONICAL_INVESTMENT_DB


def is_legacy_seed_path(seed_db_path: Optional[str]) -> bool:
    if not seed_db_path:
        return False
    return os.path.abspath(seed_db_path) != os.path.abspath(resolve_investment_db_path(seed_db_path))


def ensure_db_directory(db_path: str) -> None:
    directory = os.path.dirname(db_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


def configure_sqlite_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MILLISECONDS}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError:
        pass
    return conn


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    ensure_db_directory(db_path)
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    return configure_sqlite_connection(conn)


def run_with_monitoring_write_lock(fn: Callable[..., _T], *args, **kwargs) -> _T:
    with MONITORING_WRITE_LOCK:
        return fn(*args, **kwargs)


def normalize_account_name(
    account_name: Optional[object],
    *,
    allow_aggregate: bool = False,
    keep_none: bool = False,
) -> Optional[str]:
    text = str(account_name or "").strip()
    if keep_none and not text:
        return None
    if allow_aggregate and text == AGGREGATE_ACCOUNT_NAME:
        return DEFAULT_ACCOUNT_NAME
    return DEFAULT_ACCOUNT_NAME


def normalize_account_name_list(
    account_names: Iterable[Optional[object]],
    *,
    allow_aggregate: bool = False,
) -> list[str]:
    normalized: list[str] = []
    for account_name in account_names:
        resolved = normalize_account_name(account_name, allow_aggregate=allow_aggregate)
        if resolved and resolved not in normalized:
            normalized.append(resolved)
    return normalized


def ensure_metadata_table(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {METADATA_TABLE} (
            meta_key TEXT PRIMARY KEY,
            meta_value TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def get_metadata(conn: sqlite3.Connection, key: str) -> Optional[str]:
    ensure_metadata_table(conn)
    cursor = conn.cursor()
    cursor.execute(f"SELECT meta_value FROM {METADATA_TABLE} WHERE meta_key = ?", (key,))
    row = cursor.fetchone()
    return row["meta_value"] if row else None


def set_metadata(conn: sqlite3.Connection, key: str, value: str) -> None:
    ensure_metadata_table(conn)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        INSERT INTO {METADATA_TABLE} (meta_key, meta_value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(meta_key) DO UPDATE SET
            meta_value = excluded.meta_value,
            updated_at = CURRENT_TIMESTAMP
        """,
        (key, value),
    )


def cleanup_single_account_data(
    conn: sqlite3.Connection,
    *,
    default_account_name: str = DEFAULT_ACCOUNT_NAME,
    metadata_key: str = "single_account_cleanup_v1",
) -> dict[str, int]:
    ensure_metadata_table(conn)
    if get_metadata(conn, metadata_key):
        return {}

    cursor = conn.cursor()
    cleaned: dict[str, int] = {}
    normalized_default = str(default_account_name).strip() or DEFAULT_ACCOUNT_NAME
    timestamp = datetime.now().isoformat()

    cursor.execute(
        """
        SELECT id
        FROM assets
        WHERE COALESCE(TRIM(account_name), '') != ?
          AND deleted_at IS NULL
        """,
        (normalized_default,),
    )
    removed_asset_ids = [int(row[0]) for row in cursor.fetchall()]

    if removed_asset_ids:
        placeholders = ",".join("?" for _ in removed_asset_ids)
        for table, column in (
            ("asset_action_queue", "asset_id"),
            ("asset_trade_history", "asset_id"),
            ("monitoring_events", "monitoring_item_id"),
            ("monitoring_price_history", "monitoring_item_id"),
        ):
            if table in {"monitoring_events", "monitoring_price_history"}:
                cursor.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE {column} IN (
                        SELECT id FROM monitoring_items
                        WHERE asset_id IN ({placeholders})
                           OR portfolio_stock_id IN ({placeholders})
                    )
                    """,
                    tuple(removed_asset_ids + removed_asset_ids),
                )
            else:
                cursor.execute(
                    f"DELETE FROM {table} WHERE {column} IN ({placeholders})",
                    tuple(removed_asset_ids),
                )
            cleaned[table] = cleaned.get(table, 0) + int(cursor.rowcount or 0)

    for table, account_column in (
        ("ai_decisions", "account_name"),
        ("analysis_records", "account_name"),
        ("assets", "account_name"),
        ("monitoring_items", "account_name"),
        ("portfolio_daily_snapshots", "account_name"),
        ("portfolio_stocks", "account_name"),
    ):
        cursor.execute(
            f"""
            DELETE FROM {table}
            WHERE COALESCE(TRIM({account_column}), '') != ?
            """,
            (normalized_default,),
        )
        cleaned[table] = cleaned.get(table, 0) + int(cursor.rowcount or 0)

    cursor.execute(
        """
        DELETE FROM monitoring_events
        WHERE monitoring_item_id NOT IN (SELECT id FROM monitoring_items)
        """
    )
    cleaned["monitoring_events"] = cleaned.get("monitoring_events", 0) + int(cursor.rowcount or 0)
    cursor.execute(
        """
        DELETE FROM monitoring_price_history
        WHERE monitoring_item_id NOT IN (SELECT id FROM monitoring_items)
        """
    )
    cleaned["monitoring_price_history"] = cleaned.get("monitoring_price_history", 0) + int(cursor.rowcount or 0)

    cursor.execute(
        """
        SELECT value
        FROM portfolio_settings
        WHERE key = 'portfolio_account_total_assets_v1'
        LIMIT 1
        """
    )
    row = cursor.fetchone()
    if row:
        try:
            parsed_settings = json.loads(row[0] or "{}")
        except (TypeError, json.JSONDecodeError):
            parsed_settings = {}
        next_total_assets = 0.0
        if isinstance(parsed_settings, dict):
            try:
                next_total_assets = max(0.0, float(parsed_settings.get(normalized_default) or 0.0))
            except (TypeError, ValueError):
                next_total_assets = 0.0
        cursor.execute(
            """
            UPDATE portfolio_settings
            SET value = ?, updated_at = CURRENT_TIMESTAMP
            WHERE key = 'portfolio_account_total_assets_v1'
            """,
            (json.dumps({normalized_default: next_total_assets}, ensure_ascii=False),),
        )
        cleaned["portfolio_settings"] = int(cursor.rowcount or 0)

    set_metadata(
        conn,
        metadata_key,
        json.dumps(
            {
                "cleaned": cleaned,
                "updated_at": timestamp,
            },
            ensure_ascii=False,
        ),
    )
    return cleaned
