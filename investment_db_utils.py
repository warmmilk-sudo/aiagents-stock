import os
import sqlite3
import threading
from typing import Callable, Optional, TypeVar


CANONICAL_INVESTMENT_DB = "investment.db"
DEFAULT_ACCOUNT_NAME = "默认账户"
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
