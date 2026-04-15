"""
agent_memory_db.py
SQLite data-access layer for the multi-agent memory module.
Manages ``agent_memory.db`` containing three core tables:
  - memory_working   (short-term decision footprints)
  - memory_factual   (mid-term fact pool with embeddings & decay)
  - memory_long_term (compressed macro profile per stock)
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent_memory.db")

# Maximum working-memory entries per stock before FIFO eviction.
WORKING_MEMORY_LIMIT = int(os.getenv("MEMORY_WORKING_LIMIT", "5"))


def _connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or _DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _serialize_json(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def _safe_json_loads(raw: Any, default: Any = None):
    if raw is None or raw == "":
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return default


def _serialize_embedding(vec: list[float] | None) -> Optional[bytes]:
    """Serialize a float vector as pickle bytes for BLOB storage."""
    if vec is None:
        return None
    return pickle.dumps(vec, protocol=pickle.HIGHEST_PROTOCOL)


def _deserialize_embedding(blob: bytes | None) -> Optional[list[float]]:
    if blob is None:
        return None
    try:
        return pickle.loads(blob)
    except Exception:
        return None


class AgentMemoryDB:
    """Data-access layer for the agent memory SQLite database."""

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or _DB_PATH
        self._init_database()

    def _connect(self) -> sqlite3.Connection:
        return _connect(self.db_path)

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------

    def _init_database(self) -> None:
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memory_working (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                analysis_date TEXT NOT NULL,
                decision_summary TEXT,
                strategy_json TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(stock_code, analysis_date)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memory_factual (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_code TEXT NOT NULL,
                fact_content TEXT NOT NULL,
                category TEXT DEFAULT 'general',
                concept_tags TEXT,
                importance_score REAL DEFAULT 50,
                embedding_blob BLOB,
                source_analysis_id INTEGER,
                timestamp TEXT NOT NULL,
                is_ignored INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memory_long_term (
                stock_code TEXT PRIMARY KEY,
                macro_profile TEXT NOT NULL,
                last_updated TEXT NOT NULL,
                fact_count_since_update INTEGER DEFAULT 0
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_factual_stock
            ON memory_factual(stock_code, is_ignored)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_working_stock
            ON memory_working(stock_code, analysis_date DESC)
        """)

        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Working memory
    # ------------------------------------------------------------------

    def get_working_memory(self, stock_code: str, limit: int = WORKING_MEMORY_LIMIT) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM memory_working
            WHERE stock_code = ?
            ORDER BY analysis_date DESC
            LIMIT ?
            """,
            (stock_code, limit),
        )
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        for row in rows:
            row["strategy"] = _safe_json_loads(row.pop("strategy_json", None), {})
        return rows

    def save_working_memory(
        self,
        stock_code: str,
        analysis_date: str,
        decision_summary: str,
        strategy: Optional[Dict] = None,
    ) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute(
            """
            INSERT INTO memory_working (stock_code, analysis_date, decision_summary, strategy_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(stock_code, analysis_date)
            DO UPDATE SET decision_summary = excluded.decision_summary,
                          strategy_json = excluded.strategy_json
            """,
            (stock_code, analysis_date, decision_summary, _serialize_json(strategy), now),
        )
        row_id = cursor.lastrowid
        # FIFO eviction: keep only the N most recent per stock
        cursor.execute(
            """
            DELETE FROM memory_working
            WHERE stock_code = ? AND id NOT IN (
                SELECT id FROM memory_working
                WHERE stock_code = ?
                ORDER BY analysis_date DESC
                LIMIT ?
            )
            """,
            (stock_code, stock_code, WORKING_MEMORY_LIMIT),
        )
        conn.commit()
        conn.close()
        return row_id

    # ------------------------------------------------------------------
    # Factual memory
    # ------------------------------------------------------------------

    def get_factual_memories(
        self,
        stock_code: str,
        include_ignored: bool = False,
    ) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        clause = "stock_code = ?" if include_ignored else "stock_code = ? AND is_ignored = 0"
        cursor.execute(
            f"SELECT * FROM memory_factual WHERE {clause} ORDER BY timestamp DESC",
            (stock_code,),
        )
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        for row in rows:
            row["concept_tags"] = _safe_json_loads(row.get("concept_tags"), [])
            row["embedding"] = _deserialize_embedding(row.pop("embedding_blob", None))
        return rows

    def search_cross_sector_facts(
        self,
        concept_tags: List[str],
        exclude_stock_code: str = "",
        importance_threshold: float = 80,
        limit: int = 5,
    ) -> List[Dict]:
        """
        Find high-importance facts from OTHER stocks that share concept tags.
        Used for cross-sector association / recall.
        """
        if not concept_tags:
            return []
        conn = self._connect()
        cursor = conn.cursor()
        # Build LIKE clauses for each tag (JSON array stored as text)
        like_clauses = " OR ".join(["concept_tags LIKE ?"] * len(concept_tags))
        params: list[Any] = [f'%"{tag}"%' for tag in concept_tags]
        params.append(importance_threshold)
        exclude_clause = ""
        if exclude_stock_code:
            exclude_clause = "AND stock_code != ?"
            params.append(exclude_stock_code)
        params.append(limit)
        cursor.execute(
            f"""
            SELECT * FROM memory_factual
            WHERE ({like_clauses})
              AND is_ignored = 0
              AND importance_score >= ?
              {exclude_clause}
            ORDER BY importance_score DESC, timestamp DESC
            LIMIT ?
            """,
            tuple(params),
        )
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()
        for row in rows:
            row["concept_tags"] = _safe_json_loads(row.get("concept_tags"), [])
            row["embedding"] = _deserialize_embedding(row.pop("embedding_blob", None))
        return rows

    def save_factual_memory(
        self,
        stock_code: str,
        fact_content: str,
        timestamp: str,
        importance_score: float = 50,
        category: str = "general",
        concept_tags: Optional[List[str]] = None,
        embedding: Optional[list[float]] = None,
        source_analysis_id: Optional[int] = None,
    ) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute(
            """
            INSERT INTO memory_factual (
                stock_code, fact_content, category, concept_tags,
                importance_score, embedding_blob, source_analysis_id,
                timestamp, is_ignored, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                stock_code,
                fact_content,
                category,
                _serialize_json(concept_tags or []),
                importance_score,
                _serialize_embedding(embedding),
                source_analysis_id,
                timestamp,
                now,
            ),
        )
        fact_id = cursor.lastrowid

        # Bump the long-term counter
        cursor.execute(
            """
            UPDATE memory_long_term
            SET fact_count_since_update = fact_count_since_update + 1
            WHERE stock_code = ?
            """,
            (stock_code,),
        )
        conn.commit()
        conn.close()
        return fact_id

    def update_factual_importance(self, fact_id: int, importance_score: float) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE memory_factual SET importance_score = ? WHERE id = ?",
            (importance_score, fact_id),
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def ignore_factual_memory(self, fact_id: int) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE memory_factual SET is_ignored = 1 WHERE id = ?",
            (fact_id,),
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def restore_factual_memory(self, fact_id: int) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE memory_factual SET is_ignored = 0 WHERE id = ?",
            (fact_id,),
        )
        changed = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def delete_factual_memory(self, fact_id: int) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM memory_factual WHERE id = ?", (fact_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def get_factual_memory(self, fact_id: int) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM memory_factual WHERE id = ?", (fact_id,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        result = dict(row)
        result["concept_tags"] = _safe_json_loads(result.get("concept_tags"), [])
        result["embedding"] = _deserialize_embedding(result.pop("embedding_blob", None))
        return result

    # ------------------------------------------------------------------
    # Long-term profile
    # ------------------------------------------------------------------

    def get_long_term_profile(self, stock_code: str) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM memory_long_term WHERE stock_code = ?",
            (stock_code,),
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def save_long_term_profile(self, stock_code: str, macro_profile: str) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        cursor.execute(
            """
            INSERT INTO memory_long_term (stock_code, macro_profile, last_updated, fact_count_since_update)
            VALUES (?, ?, ?, 0)
            ON CONFLICT(stock_code)
            DO UPDATE SET macro_profile = excluded.macro_profile,
                          last_updated = excluded.last_updated,
                          fact_count_since_update = 0
            """,
            (stock_code, macro_profile, now),
        )
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Aggregation helpers
    # ------------------------------------------------------------------

    def list_stocks_with_memory(self) -> List[str]:
        """Return distinct stock codes that have at least one memory entry."""
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT stock_code FROM (
                SELECT stock_code FROM memory_working
                UNION
                SELECT stock_code FROM memory_factual
                UNION
                SELECT stock_code FROM memory_long_term
            )
            ORDER BY stock_code
        """)
        codes = [row["stock_code"] for row in cursor.fetchall()]
        conn.close()
        return codes

    def get_memory_summary(self, stock_code: str) -> Dict:
        """Quick stats for a stock: counts of each memory tier."""
        conn = self._connect()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM memory_working WHERE stock_code = ?",
            (stock_code,),
        )
        working_count = cursor.fetchone()["cnt"]

        cursor.execute(
            "SELECT COUNT(*) AS cnt FROM memory_factual WHERE stock_code = ? AND is_ignored = 0",
            (stock_code,),
        )
        factual_count = cursor.fetchone()["cnt"]

        cursor.execute(
            "SELECT stock_code FROM memory_long_term WHERE stock_code = ?",
            (stock_code,),
        )
        has_profile = cursor.fetchone() is not None

        conn.close()
        return {
            "stock_code": stock_code,
            "working_count": working_count,
            "factual_count": factual_count,
            "has_long_term_profile": has_profile,
        }

    def clear_stock_memory(self, stock_code: str) -> Dict[str, int]:
        """Delete all memory tiers for a stock and return deleted counts."""
        conn = self._connect()
        cursor = conn.cursor()
        deleted: Dict[str, int] = {}
        try:
            for table_name in ("memory_working", "memory_factual", "memory_long_term"):
                cursor.execute(f"DELETE FROM {table_name} WHERE stock_code = ?", (stock_code,))
                deleted[table_name] = int(cursor.rowcount or 0)
            conn.commit()
            return deleted
        finally:
            conn.close()


# Module-level singleton
agent_memory_db = AgentMemoryDB()
