#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


DB_PATH = Path("investment.db")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _is_valid_name(name: Any, code: str) -> bool:
    text = _normalize_text(name)
    if not text:
        return False
    invalid = {code, f"股票{code}", f"港股{code}", f"美股{code}", "N/A", "未知"}
    return text not in invalid and text.upper() != code.upper()


def _resolve_from_analysis_record(conn: sqlite3.Connection, symbol: str) -> Optional[str]:
    row = conn.execute(
        """
        SELECT stock_name
        FROM analysis_records
        WHERE symbol = ? AND stock_name IS NOT NULL AND TRIM(stock_name) != ''
        ORDER BY datetime(analysis_date) DESC, id DESC
        LIMIT 1
        """,
        (symbol,),
    ).fetchone()
    if row and _is_valid_name(row[0], symbol):
        return _normalize_text(row[0])
    return None


def _resolve_from_datasource(symbol: str) -> Optional[str]:
    try:
        from data_source_manager import data_source_manager

        info = data_source_manager.get_stock_basic_info(symbol)
    except Exception:
        return None
    return _normalize_text(info.get("name")) if _is_valid_name(info.get("name"), symbol) else None


def fix_names(symbols: list[str], apply: bool, overwrite: bool, backup: bool) -> int:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"未找到数据库文件: {DB_PATH}")

    if backup and apply:
        backup_path = DB_PATH.with_name(f"{DB_PATH.name}.name-fix-backup-{datetime.now().strftime('%Y%m%d%H%M%S')}")
        shutil.copy2(DB_PATH, backup_path)
        print(f"backup={backup_path}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    changed = 0
    try:
        for symbol in symbols:
            row = conn.execute(
                "SELECT id, symbol, name FROM assets WHERE symbol = ? AND deleted_at IS NULL ORDER BY id ASC LIMIT 1",
                (symbol,),
            ).fetchone()
            if row is None:
                print(f"{symbol}: not found")
                continue

            current_name = _normalize_text(row["name"])
            if _is_valid_name(current_name, symbol) and not overwrite:
                print(f"{symbol}: keep {current_name}")
                continue

            candidate = _resolve_from_analysis_record(conn, symbol) or _resolve_from_datasource(symbol)
            if not candidate:
                print(f"{symbol}: no candidate")
                continue

            print(f"{symbol}: {current_name or '-'} -> {candidate}")
            if apply:
                conn.execute("UPDATE assets SET name = ?, updated_at = ? WHERE id = ?", (candidate, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), row["id"]))
                changed += 1

        if apply:
            conn.commit()
    finally:
        conn.close()

    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description="回填资产名称，优先使用最新分析记录，其次使用数据源")
    parser.add_argument("--symbols", nargs="+", required=True, help="要修复的股票代码")
    parser.add_argument("--apply", action="store_true", help="执行写入")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有的有效名称")
    parser.add_argument("--no-backup", action="store_true", help="不创建数据库备份")
    args = parser.parse_args()

    changed = fix_names(
        [str(symbol).strip() for symbol in args.symbols if str(symbol).strip()],
        apply=bool(args.apply),
        overwrite=bool(args.overwrite),
        backup=not args.no_backup,
    )
    print(f"changed={changed}")


if __name__ == "__main__":
    main()
