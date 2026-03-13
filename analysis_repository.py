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

    _MISSING_TEXT_VALUES = {"", "-", "--", "N/A", "NA", "未知", "未知行业", "null", "None", "nan"}
    _STOCK_INFO_INDUSTRY_KEYS = ("industry", "所属同花顺行业", "所属行业", "所处行业", "行业", "sector")
    _STOCK_INFO_ALIAS_KEYS = ("所属同花顺行业", "所属行业", "所处行业", "行业", "sector")

    def __init__(self, db_path: str = "investment.db", legacy_analysis_db_path: Optional[str] = None):
        self.seed_db_path = db_path
        self.db_path = resolve_investment_db_path(db_path)
        self.legacy_analysis_db_path = legacy_analysis_db_path or (
            db_path if is_legacy_seed_path(db_path) else "stock_analysis.db"
        )
        self._stock_info_industry_cache: Dict[str, str] = {}
        self._init_database()
        self.migrate_legacy_analysis_db(self.legacy_analysis_db_path)
        self.cleanup_duplicate_records()
        self.migrate_stock_info_schema()

    @staticmethod
    def _normalize_text(value: Any) -> str:
        return str(value or "").strip()

    @classmethod
    def _clean_stock_info_text(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and value != value:
            return ""
        text = str(value).strip()
        return "" if text in cls._MISSING_TEXT_VALUES else text

    @staticmethod
    def _sort_json_text(value: Any) -> str:
        if value in (None, ""):
            return ""
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return value.strip()
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

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
            if isinstance(default, str) and isinstance(raw_value, str):
                return raw_value
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

    @staticmethod
    def _extract_embedded_json_mapping(text: str) -> tuple[Dict[str, Any], str]:
        if not text:
            return {}, ""

        decoder = json.JSONDecoder()
        candidate_mapping: Dict[str, Any] = {}
        candidate_prefix = ""

        for index, char in enumerate(text):
            if char != "{":
                continue

            try:
                parsed, end = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue

            trailing = text[index + end :].strip()
            if trailing:
                continue
            if isinstance(parsed, dict):
                candidate_mapping = parsed
                candidate_prefix = text[:index].strip()

        return candidate_mapping, candidate_prefix

    @classmethod
    def _resolve_final_decision_payload(cls, final_decision: Optional[Dict]) -> tuple[Dict[str, Any], bool]:
        if not isinstance(final_decision, dict):
            return final_decision or {}, False

        structured_keys = (
            "rating",
            "confidence_level",
            "target_price",
            "operation_advice",
            "entry_range",
            "entry_min",
            "entry_max",
            "take_profit",
            "stop_loss",
            "holding_period",
            "position_size",
            "risk_warning",
        )
        has_structured_keys = any(key in final_decision for key in structured_keys)
        decision_text = str(final_decision.get("decision_text") or "").strip()
        if not decision_text or has_structured_keys:
            return dict(final_decision), False

        embedded_mapping, _ = cls._extract_embedded_json_mapping(decision_text)
        if not embedded_mapping:
            return dict(final_decision), False

        merged = dict(embedded_mapping)
        merged.setdefault("decision_text", decision_text)
        return merged, True

    def _extract_stock_info_industry(self, stock_info: Optional[Dict]) -> str:
        if not isinstance(stock_info, dict):
            return ""

        for key in self._STOCK_INFO_INDUSTRY_KEYS:
            candidate = self._clean_stock_info_text(stock_info.get(key))
            if candidate:
                return candidate
        return ""

    def _lookup_basic_info_industry(
        self,
        symbol: str,
        *,
        industry_cache: Optional[Dict[str, str]] = None,
    ) -> str:
        normalized_symbol = self._normalize_text(symbol)
        if not normalized_symbol:
            return ""

        cache = industry_cache if industry_cache is not None else self._stock_info_industry_cache
        if normalized_symbol in cache:
            return cache[normalized_symbol]

        industry = ""
        try:
            from data_source_manager import data_source_manager

            basic_info = data_source_manager.get_stock_basic_info(normalized_symbol)
            industry = self._extract_stock_info_industry(basic_info)
        except Exception:
            industry = ""

        cache[normalized_symbol] = industry
        return industry

    def _normalize_stock_info_payload(
        self,
        stock_info: Optional[Dict],
        *,
        symbol: str = "",
        industry_cache: Optional[Dict[str, str]] = None,
    ) -> Dict:
        if not isinstance(stock_info, dict):
            return {}

        normalized = dict(stock_info)
        had_industry_signal = any(key in normalized for key in self._STOCK_INFO_INDUSTRY_KEYS)
        industry = self._extract_stock_info_industry(normalized)
        if not industry and symbol:
            industry = self._lookup_basic_info_industry(symbol, industry_cache=industry_cache)

        for key in self._STOCK_INFO_ALIAS_KEYS:
            normalized.pop(key, None)

        if industry:
            normalized["industry"] = industry
        elif had_industry_signal:
            normalized["industry"] = "未知"

        return normalized

    def _deserialize_row(self, row: sqlite3.Row) -> Dict:
        record = dict(row)
        record["stock_info"] = self._safe_json_loads(record.pop("stock_info_json", None), {})
        record["agents_results"] = self._safe_json_loads(record.pop("agents_results_json", None), {})
        record["discussion_result"] = self._safe_json_loads(record.get("discussion_result"), "")
        record["final_decision"] = self._safe_json_loads(record.pop("final_decision_json", None), {})
        record["has_full_report"] = bool(record.get("has_full_report"))
        return record

    def _find_duplicate_record_id(
        self,
        cursor: sqlite3.Cursor,
        *,
        symbol: str,
        period: str,
        analysis_scope: str,
        analysis_date: str,
        rating: Optional[str],
        summary: str,
        final_decision: Optional[Dict],
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
    ) -> Optional[int]:
        cursor.execute(
            """
            SELECT id, asset_id, portfolio_stock_id, rating, summary, final_decision_json
            FROM analysis_records
            WHERE analysis_scope = ?
              AND symbol = ?
              AND period = ?
              AND analysis_date = ?
            ORDER BY id DESC
            """,
            (analysis_scope, symbol, period, analysis_date),
        )
        target_rating = self._normalize_text(rating)
        target_summary = self._normalize_text(summary)
        target_final_decision = self._sort_json_text(final_decision or {})
        current_asset_key = portfolio_stock_id or asset_id
        for row in cursor.fetchall():
            existing = dict(row)
            if analysis_scope == "portfolio":
                existing_asset_key = existing.get("portfolio_stock_id") or existing.get("asset_id")
                if current_asset_key is not None and existing_asset_key not in {None, current_asset_key}:
                    continue
            if self._normalize_text(existing.get("rating")) != target_rating:
                continue
            if self._normalize_text(existing.get("summary")) != target_summary:
                continue
            existing_final_decision = self._sort_json_text(existing.get("final_decision_json"))
            if target_final_decision and existing_final_decision and existing_final_decision != target_final_decision:
                continue
            return int(existing["id"])
        return None

    def _update_existing_record(
        self,
        cursor: sqlite3.Cursor,
        record_id: int,
        *,
        stock_name: str,
        account_name: str,
        asset_id: Optional[int],
        portfolio_stock_id: Optional[int],
        analysis_source: str,
        rating: Optional[str],
        confidence: Optional[float],
        current_price: Optional[float],
        target_price: Optional[float],
        entry_min: Optional[float],
        entry_max: Optional[float],
        take_profit: Optional[float],
        stop_loss: Optional[float],
        summary: str,
        stock_info: Optional[Dict],
        agents_results: Optional[Dict],
        discussion_result: Optional[Any],
        final_decision: Optional[Dict],
        has_full_report: bool,
        asset_status_snapshot: Optional[str],
    ) -> None:
        cursor.execute(
            """
            UPDATE analysis_records
            SET stock_name = ?,
                account_name = ?,
                asset_id = ?,
                portfolio_stock_id = ?,
                analysis_source = ?,
                rating = ?,
                confidence = ?,
                current_price = ?,
                target_price = ?,
                entry_min = ?,
                entry_max = ?,
                take_profit = ?,
                stop_loss = ?,
                summary = ?,
                stock_info_json = ?,
                agents_results_json = ?,
                discussion_result = ?,
                final_decision_json = ?,
                has_full_report = ?,
                asset_status_snapshot = ?
            WHERE id = ?
            """,
            (
                stock_name,
                account_name,
                asset_id,
                portfolio_stock_id,
                analysis_source,
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
                asset_status_snapshot,
                record_id,
            ),
        )

    def cleanup_duplicate_records(self) -> int:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM analysis_records
            ORDER BY COALESCE(has_full_report, 0) DESC,
                     CASE WHEN portfolio_stock_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                     CASE WHEN asset_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                     CASE WHEN account_name IS NOT NULL AND account_name != '' THEN 1 ELSE 0 END DESC,
                     datetime(created_at) DESC,
                     id DESC
            """
        )
        seen_keys = set()
        duplicate_ids: List[int] = []
        for row in cursor.fetchall():
            record = dict(row)
            scope = record.get("analysis_scope") or "research"
            asset_key = (
                record.get("symbol")
                if scope == "research"
                else (record.get("portfolio_stock_id") or record.get("asset_id") or record.get("symbol") or "")
            )
            key = (
                scope,
                asset_key,
                self._normalize_text(record.get("symbol")),
                self._normalize_text(record.get("period")),
                self._normalize_text(record.get("analysis_date")),
                self._normalize_text(record.get("rating")),
                self._normalize_text(record.get("summary")),
                self._sort_json_text(record.get("final_decision_json")),
            )
            if key in seen_keys:
                duplicate_ids.append(int(record["id"]))
                continue
            seen_keys.add(key)
        if duplicate_ids:
            placeholders = ",".join("?" for _ in duplicate_ids)
            cursor.execute(f"DELETE FROM analysis_records WHERE id IN ({placeholders})", duplicate_ids)
            conn.commit()
        conn.close()
        return len(duplicate_ids)

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
                asset_id INTEGER,
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
                asset_status_snapshot TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        self._ensure_column(cursor, "analysis_records", "asset_id", "INTEGER")
        self._ensure_column(cursor, "analysis_records", "asset_status_snapshot", "TEXT")
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_analysis_symbol_time
            ON analysis_records(symbol, datetime(analysis_date) DESC, id DESC)
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_analysis_asset_time
            ON analysis_records(asset_id, datetime(analysis_date) DESC, id DESC)
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

    @staticmethod
    def _ensure_column(cursor, table: str, column: str, definition: str) -> None:
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
        asset_id: Optional[int] = None,
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
        asset_status_snapshot: Optional[str] = None,
    ) -> int:
        normalized_scope = analysis_scope if analysis_scope in {"research", "portfolio"} else "research"
        final_decision, extracted_from_decision_text = self._resolve_final_decision_payload(final_decision or {})
        stock_info = self._normalize_stock_info_payload(stock_info or {}, symbol=symbol)
        analysis_date = analysis_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        created_at = datetime.now().isoformat()
        effective_account_name = account_name or DEFAULT_ACCOUNT_NAME

        resolved_rating = self._normalize_text(final_decision.get("rating")) or None
        if extracted_from_decision_text:
            rating = resolved_rating or rating
        else:
            rating = rating or resolved_rating

        resolved_confidence = self._extract_first_number(
            final_decision.get("confidence_level"),
            allow_zero=True,
        )
        if extracted_from_decision_text and resolved_confidence is not None:
            confidence = resolved_confidence
        elif confidence is None:
            confidence = resolved_confidence

        current_price = current_price if current_price is not None else self._extract_first_number(
            stock_info.get("current_price"),
            allow_zero=True,
        )
        resolved_target_price = self._extract_first_number(final_decision.get("target_price"))
        if extracted_from_decision_text and resolved_target_price is not None:
            target_price = resolved_target_price
        elif target_price is None:
            target_price = resolved_target_price

        resolved_entry_min = self._extract_first_number(final_decision.get("entry_min"))
        resolved_entry_max = self._extract_first_number(final_decision.get("entry_max"))
        if resolved_entry_min is None or resolved_entry_max is None:
            entry_text = str(final_decision.get("entry_range") or "")
            if entry_text:
                numbers = []
                for token in entry_text.replace("~", "-").replace("至", "-").replace("到", "-").split("-"):
                    number = self._extract_first_number(token)
                    if number is not None:
                        numbers.append(number)
                if len(numbers) >= 2:
                    if resolved_entry_min is None:
                        resolved_entry_min = numbers[0]
                    if resolved_entry_max is None:
                        resolved_entry_max = numbers[1]
        if extracted_from_decision_text:
            if resolved_entry_min is not None:
                entry_min = resolved_entry_min
            if resolved_entry_max is not None:
                entry_max = resolved_entry_max
        else:
            if entry_min is None:
                entry_min = resolved_entry_min
            if entry_max is None:
                entry_max = resolved_entry_max
        resolved_take_profit = self._extract_first_number(final_decision.get("take_profit"))
        if extracted_from_decision_text and resolved_take_profit is not None:
            take_profit = resolved_take_profit
        elif take_profit is None:
            take_profit = resolved_take_profit

        resolved_stop_loss = self._extract_first_number(final_decision.get("stop_loss"))
        if extracted_from_decision_text and resolved_stop_loss is not None:
            stop_loss = resolved_stop_loss
        elif stop_loss is None:
            stop_loss = resolved_stop_loss

        resolved_summary = str(
            final_decision.get("operation_advice")
            or final_decision.get("advice")
            or final_decision.get("summary")
            or ""
        ).strip()
        if extracted_from_decision_text and resolved_summary:
            summary = resolved_summary
        elif not summary:
            summary = resolved_summary
        if has_full_report is None:
            has_full_report = any(
                value not in (None, "", {}, [])
                for value in (stock_info, agents_results, discussion_result, final_decision)
            )

        if asset_id is None and symbol:
            from asset_repository import asset_repository

            if normalized_scope == "portfolio":
                existing_asset = asset_repository.get_asset_by_symbol(symbol, effective_account_name)
                if existing_asset:
                    asset_id = existing_asset["id"]
                    asset_status_snapshot = asset_status_snapshot or existing_asset.get("status")
            else:
                asset_id = asset_repository.create_or_update_research_asset(
                    symbol=symbol,
                    name=stock_name or symbol,
                    account_name=effective_account_name,
                    note=summary,
                    origin_analysis_id=None,
                )
                asset = asset_repository.get_asset(asset_id)
                asset_status_snapshot = asset_status_snapshot or (asset.get("status") if asset else "research")
        elif asset_id is not None and asset_status_snapshot is None:
            from asset_repository import asset_repository

            asset = asset_repository.get_asset(asset_id)
            asset_status_snapshot = asset.get("status") if asset else asset_status_snapshot
        if asset_id is not None and portfolio_stock_id is None and normalized_scope == "portfolio":
            portfolio_stock_id = asset_id

        conn = self._connect()
        cursor = conn.cursor()
        existing_record_id = self._find_duplicate_record_id(
            cursor,
            symbol=symbol,
            period=period,
            analysis_scope=normalized_scope,
            analysis_date=analysis_date,
            rating=rating,
            summary=summary,
            final_decision=final_decision,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
        )
        if existing_record_id is not None:
            self._update_existing_record(
                cursor,
                existing_record_id,
                stock_name=stock_name,
                account_name=effective_account_name,
                asset_id=asset_id,
                portfolio_stock_id=portfolio_stock_id,
                analysis_source=analysis_source,
                rating=rating,
                confidence=confidence,
                current_price=current_price,
                target_price=target_price,
                entry_min=entry_min,
                entry_max=entry_max,
                take_profit=take_profit,
                stop_loss=stop_loss,
                summary=summary,
                stock_info=stock_info,
                agents_results=agents_results,
                discussion_result=discussion_result,
                final_decision=final_decision,
                has_full_report=bool(has_full_report),
                asset_status_snapshot=asset_status_snapshot,
            )
            conn.commit()
            conn.close()
            return existing_record_id
        cursor.execute(
            """
            INSERT INTO analysis_records (
                symbol, stock_name, account_name, asset_id, portfolio_stock_id, analysis_scope,
                analysis_source, analysis_date, period, rating, confidence,
                current_price, target_price, entry_min, entry_max, take_profit, stop_loss,
                summary, stock_info_json, agents_results_json, discussion_result,
                final_decision_json, has_full_report, asset_status_snapshot, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                stock_name,
                effective_account_name,
                asset_id,
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
                asset_status_snapshot,
                created_at,
            ),
        )
        record_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()
        if asset_id is not None:
            from asset_repository import asset_repository

            asset = asset_repository.get_asset(asset_id)
            if asset and not asset.get("origin_analysis_id"):
                asset_repository.update_asset(asset_id, origin_analysis_id=record_id)
        return record_id

    def list_records(
        self,
        *,
        analysis_scope: Optional[str] = None,
        symbol: Optional[str] = None,
        asset_id: Optional[int] = None,
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
        if asset_id is not None:
            clauses.append("asset_id = ?")
            params.append(asset_id)
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
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
        symbol: Optional[str] = None,
        account_name: Optional[str] = None,
    ) -> Optional[Dict]:
        conn = self._connect()
        cursor = conn.cursor()
        match_clauses: List[str] = []
        params: List[Any] = []

        if asset_id is not None:
            match_clauses.append("asset_id = ?")
            params.append(asset_id)
        if portfolio_stock_id is not None:
            match_clauses.append("portfolio_stock_id = ?")
            params.append(portfolio_stock_id)
        if symbol:
            symbol_clause = "symbol = ?"
            symbol_params: List[Any] = [symbol]
            if account_name:
                symbol_clause += " AND (account_name = ? OR account_name = ? OR account_name IS NULL)"
                symbol_params.extend([account_name, DEFAULT_ACCOUNT_NAME])
            match_clauses.append(f"({symbol_clause})")
            params.extend(symbol_params)

        record = None
        if match_clauses:
            sql = (
                "SELECT * FROM analysis_records "
                "WHERE COALESCE(has_full_report, 0) = 1 "
                f"AND ({' OR '.join(match_clauses)}) "
                "ORDER BY datetime(analysis_date) DESC, "
                "CASE WHEN analysis_scope = 'portfolio' THEN 0 ELSE 1 END, "
                "id DESC LIMIT 1"
            )
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
            record = self._deserialize_row(row) if row else None
        conn.close()
        if not record:
            return None
        return {
            "origin_analysis_id": record["id"],
            "asset_id": record.get("asset_id"),
            "symbol": record["symbol"],
            "stock_name": record.get("stock_name"),
            "account_name": record.get("account_name"),
            "portfolio_stock_id": record.get("portfolio_stock_id"),
            "asset_status_snapshot": record.get("asset_status_snapshot"),
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
            asset_id=portfolio_stock_id,
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

    def get_latest_linked_record(
        self,
        *,
        asset_id: Optional[int] = None,
        portfolio_stock_id: Optional[int] = None,
        symbol: Optional[str] = None,
        account_name: Optional[str] = None,
    ) -> Optional[Dict]:
        def _fetch_one(clauses: List[str], params: List[Any]) -> Optional[Dict]:
            sql = (
                "SELECT * FROM analysis_records "
                f"WHERE {' AND '.join(clauses)} "
                "ORDER BY datetime(analysis_date) DESC, "
                "CASE WHEN analysis_scope = 'portfolio' THEN 0 ELSE 1 END, "
                "id DESC LIMIT 1"
            )
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
            return self._deserialize_row(row) if row else None

        conn = self._connect()
        cursor = conn.cursor()
        record = None
        if asset_id is not None:
            record = _fetch_one(["COALESCE(has_full_report, 0) = 1", "asset_id = ?"], [asset_id])
        if record is None and portfolio_stock_id is not None:
            record = _fetch_one(
                ["COALESCE(has_full_report, 0) = 1", "portfolio_stock_id = ?"],
                [portfolio_stock_id],
            )
        if record is None and symbol:
            clauses = ["COALESCE(has_full_report, 0) = 1", "symbol = ?"]
            params: List[Any] = [symbol]
            if account_name:
                clauses.append("(account_name = ? OR account_name = ? OR account_name IS NULL)")
                params.extend([account_name, DEFAULT_ACCOUNT_NAME])
            record = _fetch_one(clauses, params)
        conn.close()
        return record

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
                asset_status_snapshot="research",
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
                asset_id=record.get("portfolio_stock_id"),
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
                asset_status_snapshot="portfolio",
            )
            migrated += 1
        legacy_conn.close()

        conn = self._connect()
        set_metadata(conn, key, str(migrated))
        conn.commit()
        conn.close()
        return migrated

    def migrate_stock_info_schema(self) -> int:
        conn = self._connect()
        key = "migrated_stock_info_schema::industry_v2"
        if get_metadata(conn, key):
            conn.close()
            return 0

        cursor = conn.cursor()
        industry_cache: Dict[str, str] = {}
        updated = 0
        unresolved = 0
        try:
            cursor.execute(
                """
                SELECT id, symbol, stock_info_json
                FROM analysis_records
                WHERE stock_info_json IS NOT NULL
                  AND stock_info_json <> ''
                ORDER BY id ASC
                """
            )
            for row in cursor.fetchall():
                stock_info = self._safe_json_loads(row["stock_info_json"], {})
                if not isinstance(stock_info, dict):
                    continue

                normalized = self._normalize_stock_info_payload(
                    stock_info,
                    symbol=row["symbol"] or "",
                    industry_cache=industry_cache,
                )
                if normalized == stock_info:
                    pass
                else:
                    cursor.execute(
                        "UPDATE analysis_records SET stock_info_json = ? WHERE id = ?",
                        (self._serialize_json(normalized), row["id"]),
                    )
                    updated += 1

                if any(alias_key in normalized for alias_key in self._STOCK_INFO_ALIAS_KEYS):
                    unresolved += 1
                    continue

                if not self._clean_stock_info_text(normalized.get("industry")):
                    unresolved += 1

            if unresolved == 0:
                set_metadata(conn, key, str(updated))
            conn.commit()
            return updated
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


analysis_repository = AnalysisRepository()
