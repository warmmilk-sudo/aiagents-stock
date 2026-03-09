import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from investment_db_utils import (
    DEFAULT_ACCOUNT_NAME,
    connect_sqlite,
    get_metadata,
    is_legacy_seed_path,
    resolve_investment_db_path,
    set_metadata,
)


class AnalysisRepository:
    """Canonical analysis storage shared by research and portfolio domains."""

    def __init__(self, db_path: str = "investment.db", legacy_analysis_db_path: Optional[str] = None):
        self.seed_db_path = db_path
        self.db_path = resolve_investment_db_path(db_path)
        self.legacy_analysis_db_path = legacy_analysis_db_path or (
            db_path if is_legacy_seed_path(db_path) else "stock_analysis.db"
        )
        self._init_database()
        self.migrate_legacy_analysis_db(self.legacy_analysis_db_path)

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    @staticmethod
    def _serialize_json(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    @staticmethod
    def _safe_json_loads(raw_value, default):
        if raw_value in (None, ""):
            return default
        if isinstance(raw_value, (dict, list)):
            return raw_value
        try:
            return json.loads(raw_value)
        except (TypeError, json.JSONDecodeError):
            return default

    @staticmethod
    def _extract_first_number(value, allow_zero: bool = False) -> Optional[float]:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            number = float(value)
            return number if allow_zero or number != 0 else None
        text = str(value)
        digits = []
        current = []
        for char in text:
            if char.isdigit() or char in {".", "-"}:
                current.append(char)
            elif current:
                digits.append("".join(current))
                current = []
        if current:
            digits.append("".join(current))
        for candidate in digits:
            try:
                number = float(candidate)
            except ValueError:
                continue
            if allow_zero or number != 0:
                return number
        return None

    def _deserialize_row(self, row: sqlite3.Row) -> Dict:
        record = dict(row)
        record["stock_info"] = self._safe_json_loads(record.pop("stock_info_json", None), {})
        record["agents_results"] = self._safe_json_loads(record.pop("agents_results_json", None), {})
        record["discussion_result"] = self._safe_json_loads(record.get("discussion_result"), "")
        record["final_decision"] = self._safe_json_loads(record.pop("final_decision_json", None), {})
        record["has_full_report"] = bool(record.get("has_full_report"))
        return record

    def _init_database(self) -> None:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                stock_name TEXT,
                account_name TEXT,
                portfolio_stock_id INTEGER,
                analysis_scope TEXT NOT NULL DEFAULT 'research'
                    CHECK(analysis_scope IN ('research', 'portfolio')),
                analysis_source TEXT DEFAULT 'manual',
                analysis_date TEXT NOT NULL,
                period TEXT NOT NULL,
                rating TEXT,
                confidence REAL,
                current_price REAL,
                target_price REAL,
                entry_min REAL,
                entry_max REAL,
                take_profit REAL,
                stop_loss REAL,
                summary TEXT,
                stock_info_json TEXT,
                agents_results_json TEXT,
                discussion_result TEXT,
                final_decision_json TEXT,
                has_full_report INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_analysis_symbol_time
            ON analysis_records(symbol, datetime(analysis_date) DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_analysis_portfolio_stock_time
            ON analysis_records(portfolio_stock_id, datetime(analysis_date) DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_analysis_scope_time
            ON analysis_records(analysis_scope, datetime(analysis_date) DESC, id DESC)
            """
        )
        conn.commit()
        conn.close()

    def save_record(
        self,
        *,
        symbol: str,
        stock_name: str,
        period: str,
        stock_info: Optional[Dict] = None,
        agents_results: Optional[Dict] = None,
        discussion_result: Optional[Any] = None,
        final_decision: Optional[Dict] = None,
        account_name: Optional[str] = None,
        portfolio_stock_id: Optional[int] = None,
        analysis_scope: str = "research",
        analysis_source: str = "manual",
        analysis_date: Optional[str] = None,
        rating: Optional[str] = None,
        confidence: Optional[float] = None,
        current_price: Optional[float] = None,
        target_price: Optional[float] = None,
        entry_min: Optional[float] = None,
        entry_max: Optional[float] = None,
        take_profit: Optional[float] = None,
        stop_loss: Optional[float] = None,
        summary: str = "",
        has_full_report: Optional[bool] = None,
    ) -> int:
        normalized_scope = analysis_scope if analysis_scope in {"research", "portfolio"} else "research"
        final_decision = final_decision or {}
        stock_info = stock_info or {}
        analysis_date = analysis_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        created_at = datetime.now().isoformat()

        rating = rating or final_decision.get("rating")
        confidence = confidence if confidence is not None else self._extract_first_number(
            final_decision.get("confidence_level"),
            allow_zero=True,
        )
        current_price = current_price if current_price is not None else self._extract_first_number(
            stock_info.get("current_price"),
            allow_zero=True,
        )
        target_price = target_price if target_price is not None else self._extract_first_number(
            final_decision.get("target_price")
        )
        if entry_min is None or entry_max is None:
            entry_text = str(final_decision.get("entry_range") or "")
            if entry_text:
                numbers = []
                for token in entry_text.replace("~", "-").replace("至", "-").replace("到", "-").split("-"):
                    number = self._extract_first_number(token)
                    if number is not None:
                        numbers.append(number)
                if len(numbers) >= 2:
                    if entry_min is None:
                        entry_min = numbers[0]
                    if entry_max is None:
                        entry_max = numbers[1]
        take_profit = take_profit if take_profit is not None else self._extract_first_number(final_decision.get("take_profit"))
        stop_loss = stop_loss if stop_loss is not None else self._extract_first_number(final_decision.get("stop_loss"))
        if not summary:
            summary = str(
                final_decision.get("operation_advice")
                or final_decision.get("advice")
                or final_decision.get("summary")
                or ""
            ).strip()
        if has_full_report is None:
            has_full_report = any(
                value not in (None, "", {}, [])
                for value in (stock_info, agents_results, discussion_result, final_decision)
            )

        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO analysis_records (
                symbol, stock_name, account_name, portfolio_stock_id, analysis_scope,
                analysis_source, analysis_date, period, rating, confidence,
                current_price, target_price, entry_min, entry_max, take_profit, stop_loss,
                summary, stock_info_json, agents_results_json, discussion_result,
                final_decision_json, has_full_report, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                stock_name,
                account_name,
                portfolio_stock_id,
                normalized_scope,
                analysis_source,
                analysis_date,
                period,
                rating,
                confidence,
                current_price,
                target_price,
                entry_min,
                entry_max,
                take_profit,
                stop_loss,
                summary,
                self._serialize_json(stock_info),
                self._serialize_json(agents_results),
                self._serialize_json(discussion_result),
                self._serialize_json(final_decision),
                1 if has_full_report else 0,
                created_at,
            ),
        )
        record_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        return record_id

    def list_records(
        self,
        *,
        analysis_scope: Optional[str] = None,
        symbol: Optional[str] = None,
        portfolio_stock_id: Optional[int] = None,
        limit: Optional[int] = None,
        full_report_only: bool = False,
    ) -> List[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        clauses = ["1 = 1"]
        params: List[Any] = []
        if analysis_scope:
            clauses.append("analysis_scope = ?")
            params.append(analysis_scope)
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if portfolio_stock_id is not None:
            clauses.append("portfolio_stock_id = ?")
            params.append(portfolio_stock_id)
        if full_report_only:
            clauses.append("COALESCE(has_full_report, 0) = 1")
        sql = f"SELECT * FROM analysis_records WHERE {' AND '.join(clauses)} ORDER BY datetime(analysis_date) DESC, id DESC"
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        conn.close()
        return [self._deserialize_row(row) for row in rows]

    def get_record(self, record_id: int) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM analysis_records WHERE id = ?", (record_id,))
        row = cursor.fetchone()
        conn.close()
        return self._deserialize_row(row) if row else None

    def delete_record(self, record_id: int) -> bool:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM analysis_records WHERE id = ?", (record_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def get_latest_strategy_context(
        self,
        *,
        portfolio_stock_id: Optional[int] = None,
        symbol: Optional[str] = None,
        account_name: Optional[str] = None,
    ) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        clauses = ["COALESCE(has_full_report, 0) = 1"]
        params: List[Any] = []
        if portfolio_stock_id is not None:
            clauses.append("portfolio_stock_id = ?")
            params.append(portfolio_stock_id)
        elif symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
            if account_name:
                clauses.append("(account_name = ? OR account_name IS NULL)")
                params.append(account_name)
        else:
            conn.close()
            return None
        sql = (
            "SELECT * FROM analysis_records "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY CASE WHEN analysis_scope = 'portfolio' THEN 0 ELSE 1 END, "
            "datetime(analysis_date) DESC, id DESC LIMIT 1"
        )
        cursor.execute(sql, tuple(params))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        record = self._deserialize_row(row)
        return {
            "origin_analysis_id": record["id"],
            "symbol": record["symbol"],
            "stock_name": record.get("stock_name"),
            "account_name": record.get("account_name"),
            "portfolio_stock_id": record.get("portfolio_stock_id"),
            "analysis_scope": record.get("analysis_scope"),
            "analysis_source": record.get("analysis_source"),
            "analysis_date": record.get("analysis_date"),
            "rating": record.get("rating"),
            "confidence": record.get("confidence"),
            "current_price": record.get("current_price"),
            "entry_min": record.get("entry_min"),
            "entry_max": record.get("entry_max"),
            "take_profit": record.get("take_profit"),
            "stop_loss": record.get("stop_loss"),
            "summary": record.get("summary"),
            "final_decision": record.get("final_decision", {}),
        }

    def get_latest_portfolio_record(self, portfolio_stock_id: int) -> Optional[Dict]:
        records = self.list_records(
            analysis_scope="portfolio",
            portfolio_stock_id=portfolio_stock_id,
            limit=1,
            full_report_only=True,
        )
        return records[0] if records else None

    def get_latest_portfolio_records(self, portfolio_stock_ids: List[int]) -> Dict[int, Dict]:
        if not portfolio_stock_ids:
            return {}
        result: Dict[int, Dict] = {}
        for stock_id in portfolio_stock_ids:
            record = self.get_latest_portfolio_record(stock_id)
            if record:
                result[stock_id] = record
        return result

    def migrate_legacy_analysis_db(self, legacy_db_path: Optional[str]) -> int:
        if not legacy_db_path or not os.path.exists(legacy_db_path):
            return 0
        if os.path.abspath(legacy_db_path) == os.path.abspath(self.db_path):
            return 0

        conn = self._connect()
        key = f"migrated_analysis::{os.path.abspath(legacy_db_path)}"
        if get_metadata(conn, key):
            conn.close()
            return 0
        conn.close()

        legacy_conn = sqlite3.connect(legacy_db_path)
        legacy_conn.row_factory = sqlite3.Row
        legacy_cursor = legacy_conn.cursor()
        legacy_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='analysis_records'")
        if not legacy_cursor.fetchone():
            legacy_conn.close()
            conn = self._connect()
            set_metadata(conn, key, "missing")
            conn.commit()
            conn.close()
            return 0

        legacy_cursor.execute("SELECT * FROM analysis_records ORDER BY id ASC")
        migrated = 0
        for row in legacy_cursor.fetchall():
            record = dict(row)
            self.save_record(
                symbol=record.get("symbol") or "",
                stock_name=record.get("stock_name") or record.get("symbol") or "",
                period=record.get("period") or "1y",
                stock_info=self._safe_json_loads(record.get("stock_info"), {}),
                agents_results=self._safe_json_loads(record.get("agents_results"), {}),
                discussion_result=self._safe_json_loads(record.get("discussion_result"), ""),
                final_decision=self._safe_json_loads(record.get("final_decision"), {}),
                analysis_scope="research",
                analysis_source="legacy_home_analysis",
                analysis_date=record.get("analysis_date") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                has_full_report=True,
            )
            migrated += 1
        legacy_conn.close()

        conn = self._connect()
        set_metadata(conn, key, str(migrated))
        conn.commit()
        conn.close()
        return migrated

    def migrate_legacy_portfolio_db(self, legacy_db_path: Optional[str]) -> int:
        if not legacy_db_path or not os.path.exists(legacy_db_path):
            return 0
        if os.path.abspath(legacy_db_path) == os.path.abspath(self.db_path):
            return 0

        conn = self._connect()
        key = f"migrated_portfolio_analysis::{os.path.abspath(legacy_db_path)}"
        if get_metadata(conn, key):
            conn.close()
            return 0
        conn.close()

        legacy_conn = sqlite3.connect(legacy_db_path)
        legacy_conn.row_factory = sqlite3.Row
        legacy_cursor = legacy_conn.cursor()
        legacy_cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='portfolio_analysis_history'"
        )
        if not legacy_cursor.fetchone():
            legacy_conn.close()
            conn = self._connect()
            set_metadata(conn, key, "missing")
            conn.commit()
            conn.close()
            return 0

        legacy_cursor.execute(
            """
            SELECT
                h.*,
                s.code,
                s.name,
                s.account_name
            FROM portfolio_analysis_history h
            INNER JOIN portfolio_stocks s
                ON s.id = h.portfolio_stock_id
            ORDER BY h.id ASC
            """
        )
        migrated = 0
        for row in legacy_cursor.fetchall():
            record = dict(row)
            self.save_record(
                symbol=record.get("code") or "",
                stock_name=record.get("name") or record.get("code") or "",
                account_name=record.get("account_name") or DEFAULT_ACCOUNT_NAME,
                portfolio_stock_id=record.get("portfolio_stock_id"),
                analysis_scope="portfolio",
                analysis_source=record.get("analysis_source") or "legacy_portfolio_analysis",
                analysis_date=str(record.get("analysis_time") or datetime.now()),
                period=record.get("analysis_period") or "1y",
                rating=record.get("rating"),
                confidence=record.get("confidence"),
                current_price=record.get("current_price"),
                target_price=record.get("target_price"),
                entry_min=record.get("entry_min"),
                entry_max=record.get("entry_max"),
                take_profit=record.get("take_profit"),
                stop_loss=record.get("stop_loss"),
                summary=record.get("summary") or "",
                stock_info=self._safe_json_loads(record.get("stock_info_json"), {}),
                agents_results=self._safe_json_loads(record.get("agents_results_json"), {}),
                discussion_result=self._safe_json_loads(record.get("discussion_result"), ""),
                final_decision=self._safe_json_loads(record.get("final_decision_json"), {}),
                has_full_report=bool(record.get("has_full_report", 0)),
            )
            migrated += 1
        legacy_conn.close()

        conn = self._connect()
        set_metadata(conn, key, str(migrated))
        conn.commit()
        conn.close()
        return migrated


analysis_repository = AnalysisRepository()
