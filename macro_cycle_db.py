#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Persistence layer for macro cycle analysis reports."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd


class MacroCycleDatabase:
    """Store macro cycle reports in a dedicated SQLite database."""

    def __init__(self, db_path: str = "macro_cycle.db"):
        self.db_path = db_path
        self.init_database()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_database(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS macro_cycle_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_date TEXT NOT NULL,
                summary TEXT,
                chief_summary TEXT,
                result_json TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_macro_cycle_reports_created_at
            ON macro_cycle_reports(created_at DESC)
            """
        )
        conn.commit()
        conn.close()

    def save_analysis_report(self, result: Dict[str, Any], summary: str, chief_summary: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO macro_cycle_reports (analysis_date, summary, chief_summary, result_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                result.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                summary,
                chief_summary,
                json.dumps(result, ensure_ascii=False, default=str),
            ),
        )
        report_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return report_id

    def get_historical_reports(self, limit: int = 20) -> pd.DataFrame:
        conn = self.get_connection()
        query = """
        SELECT id, analysis_date, summary, chief_summary, created_at
        FROM macro_cycle_reports
        ORDER BY created_at DESC
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        return df

    def get_latest_report(self) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM macro_cycle_reports
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        conn.close()
        return self._parse_row(row)

    def get_report_detail(self, report_id: int) -> Optional[Dict[str, Any]]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM macro_cycle_reports WHERE id = ?", (report_id,))
        row = cursor.fetchone()
        conn.close()
        return self._parse_row(row)

    def delete_report(self, report_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM macro_cycle_reports WHERE id = ?", (report_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def _parse_row(self, row) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        result = dict(row)
        try:
            result["result_parsed"] = json.loads(result.get("result_json") or "{}")
        except json.JSONDecodeError:
            result["result_parsed"] = {}
        return result


macro_cycle_db = MacroCycleDatabase()
