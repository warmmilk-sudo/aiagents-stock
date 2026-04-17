#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from investment_db_utils import resolve_investment_db_path


STATUS_RESEARCH = "research"
STATUS_FOCUS = "focus"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="清理研究池中已经没有历史分析报告的股票。",
    )
    parser.add_argument(
        "--db-path",
        default="investment.db",
        help="投资数据库路径，默认使用项目根目录下的 investment.db",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="实际写入数据库；默认只做 dry-run",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="将目标股票软删除；默认仅把 status 从 research 改为 focus",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="写入前不创建数据库备份",
    )
    return parser.parse_args()


def resolve_db_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    return candidate.resolve()


def connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def backup_database(db_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak-{timestamp}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def load_report_indexes(cursor: sqlite3.Cursor) -> tuple[set[int], set[str]]:
    cursor.execute(
        """
        SELECT DISTINCT asset_id, portfolio_stock_id, symbol
        FROM analysis_records
        WHERE COALESCE(has_full_report, 0) = 1
        """
    )
    asset_ids: set[int] = set()
    symbols: set[str] = set()
    for row in cursor.fetchall():
        for key in ("asset_id", "portfolio_stock_id"):
            value = row[key]
            if value in (None, ""):
                continue
            try:
                asset_ids.add(int(value))
            except (TypeError, ValueError):
                continue
        symbol = str(row["symbol"] or "").strip().upper()
        if symbol:
            symbols.add(symbol)
    return asset_ids, symbols


def load_research_candidates(cursor: sqlite3.Cursor) -> list[sqlite3.Row]:
    cursor.execute(
        """
        SELECT id, symbol, name, account_name, status, deleted_at, updated_at
        FROM assets
        WHERE deleted_at IS NULL
          AND status = ?
        ORDER BY datetime(updated_at) DESC, id DESC
        """,
        (STATUS_RESEARCH,),
    )
    return cursor.fetchall()


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"数据库不存在: {db_path}")

    if args.apply and not args.no_backup:
        backup_path = backup_database(db_path)
        print(f"已创建备份: {backup_path}")

    conn = connect(db_path)
    try:
        cursor = conn.cursor()
        report_asset_ids, report_symbols = load_report_indexes(cursor)
        candidates = load_research_candidates(cursor)

        to_remove: list[dict[str, object]] = []
        kept: list[dict[str, object]] = []
        for row in candidates:
            asset_id = int(row["id"])
            symbol = str(row["symbol"] or "").strip().upper()
            if asset_id in report_asset_ids or symbol in report_symbols:
                kept.append(
                    {
                        "id": asset_id,
                        "symbol": symbol,
                        "name": row["name"],
                    }
                )
                continue
            to_remove.append(
                {
                    "id": asset_id,
                    "symbol": symbol,
                    "name": row["name"],
                    "account_name": row["account_name"],
                }
            )

        print(
            json.dumps(
                {
                    "research_total": len(candidates),
                    "with_history": len(kept),
                    "without_history": len(to_remove),
                    "apply": bool(args.apply),
                    "mode": "delete" if args.delete else "focus",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if to_remove:
            print("待处理股票:")
            for item in to_remove:
                print(f"- {item['symbol']} {item['name']} (id={item['id']})")

        if not args.apply or not to_remove:
            return 0

        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updated = 0
        cursor.execute("BEGIN")
        for item in to_remove:
            if args.delete:
                cursor.execute(
                    """
                    UPDATE assets
                    SET deleted_at = ?, updated_at = ?
                    WHERE id = ? AND deleted_at IS NULL
                    """,
                    (now_text, now_text, item["id"]),
                )
            else:
                cursor.execute(
                    """
                    UPDATE assets
                    SET status = ?,
                        pool_reason = ?,
                        pool_reason_source = ?,
                        updated_at = ?
                    WHERE id = ? AND deleted_at IS NULL
                    """,
                    (
                        STATUS_FOCUS,
                        "临时清理：无历史分析报告",
                        "cleanup_no_history_report",
                        now_text,
                        item["id"],
                    ),
                )
            updated += int(cursor.rowcount or 0)

        conn.commit()
        print(f"已处理 {updated} 条研究池股票。")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
