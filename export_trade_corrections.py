"""
Export trade history from investment DB into a JSON payload that is directly
compatible with apply_trade_corrections.py.

Examples:
python export_trade_corrections.py
python export_trade_corrections.py --output trade_corrections.json --account-name 默认账户
python export_trade_corrections.py --portfolio-only --include-empty
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from typing import Any, Dict, List

from investment_db_utils import DEFAULT_ACCOUNT_NAME, resolve_investment_db_path


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export trade records into correction JSON for offline editing."
    )
    parser.add_argument(
        "--db-path",
        default="investment.db",
        help="Path to investment database. Default: investment.db",
    )
    parser.add_argument(
        "--output",
        default="trade_corrections.export.json",
        help="Output JSON file path. Default: trade_corrections.export.json",
    )
    parser.add_argument(
        "--account-name",
        default=None,
        help="Only export one account.",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Only export one symbol (e.g. 000001).",
    )
    parser.add_argument(
        "--portfolio-only",
        action="store_true",
        help="Only export assets currently in portfolio status.",
    )
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Include assets with no trade history.",
    )
    parser.add_argument(
        "--indent",
        type=int,
        default=2,
        help="JSON indentation spaces. Default: 2",
    )
    return parser


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_trade_type(raw_trade_type: Any) -> str:
    value = str(raw_trade_type or "").strip().lower()
    if value in {"buy", "sell", "clear"}:
        return value
    if value in {"加仓", "买入", "建仓"}:
        return "buy"
    if value in {"减仓", "卖出"}:
        return "sell"
    if value in {"清仓", "liquidate", "清仓并降级"}:
        return "clear"
    return "sell" if value else "sell"


def _load_assets(
    conn: sqlite3.Connection,
    *,
    account_name: str | None,
    symbol: str | None,
    portfolio_only: bool,
) -> List[Dict[str, Any]]:
    sql = [
        """
        SELECT
            a.id,
            a.account_name,
            a.symbol,
            a.name,
            a.status
        FROM assets a
        WHERE a.deleted_at IS NULL
        """
    ]
    params: List[Any] = []
    if account_name:
        sql.append("AND a.account_name = ?")
        params.append(account_name)
    if symbol:
        sql.append("AND UPPER(a.symbol) = UPPER(?)")
        params.append(symbol.strip())
    if portfolio_only:
        sql.append("AND a.status = 'portfolio'")
    sql.append("ORDER BY a.id ASC")

    cursor = conn.cursor()
    cursor.execute(" ".join(sql), tuple(params))
    return [dict(row) for row in cursor.fetchall()]


def _load_trades_by_asset(conn: sqlite3.Connection, asset_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
    if not asset_ids:
        return {}

    placeholders = ",".join("?" for _ in asset_ids)
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT
            t.id,
            t.asset_id,
            t.trade_date,
            t.trade_type,
            t.price,
            t.quantity,
            t.note,
            t.trade_source
        FROM asset_trade_history t
        WHERE t.asset_id IN ({placeholders})
        ORDER BY t.asset_id ASC, t.trade_date ASC, t.id ASC
        """,
        tuple(asset_ids),
    )

    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in cursor.fetchall():
        record = dict(row)
        asset_id = int(record["asset_id"])
        grouped.setdefault(asset_id, []).append(
            {
                "trade_date": str(record.get("trade_date") or ""),
                "trade_type": _normalize_trade_type(record.get("trade_type")),
                "price": float(record.get("price") or 0.0),
                "quantity": int(record.get("quantity") or 0),
                "note": str(record.get("note") or ""),
                "trade_source": str(record.get("trade_source") or "manual_fix"),
            }
        )
    return grouped


def _status_when_flat(asset_status: str) -> str:
    normalized = str(asset_status or "").strip().lower()
    if normalized == "research":
        return "research"
    return "watchlist"


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    resolved_db_path = resolve_investment_db_path(args.db_path)
    if not os.path.exists(resolved_db_path):
        print(f"[ERR] Database file does not exist: {resolved_db_path}")
        return 1

    conn = _connect(resolved_db_path)
    try:
        assets = _load_assets(
            conn,
            account_name=args.account_name,
            symbol=args.symbol,
            portfolio_only=bool(args.portfolio_only),
        )
        asset_ids = [int(asset["id"]) for asset in assets]
        trades_by_asset = _load_trades_by_asset(conn, asset_ids)
    finally:
        conn.close()

    corrections: List[Dict[str, Any]] = []
    total_trade_count = 0
    skipped_empty_count = 0
    for asset in assets:
        stock_id = int(asset["id"])
        trades = trades_by_asset.get(stock_id, [])
        if not args.include_empty and not trades:
            skipped_empty_count += 1
            continue

        total_trade_count += len(trades)
        corrections.append(
            {
                "stock_id": stock_id,
                "symbol": str(asset.get("symbol") or ""),
                "stock_name": str(asset.get("name") or ""),
                "account_name": str(asset.get("account_name") or DEFAULT_ACCOUNT_NAME),
                "status_when_flat": _status_when_flat(str(asset.get("status") or "")),
                "trades": trades,
            }
        )

    payload: Dict[str, Any] = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "db_path": resolved_db_path,
        "default_account_name": str(args.account_name or DEFAULT_ACCOUNT_NAME),
        "default_trade_source": "manual_fix",
        "default_status_when_flat": "watchlist",
        "filters": {
            "account_name": args.account_name,
            "symbol": args.symbol,
            "portfolio_only": bool(args.portfolio_only),
            "include_empty": bool(args.include_empty),
        },
        "summary": {
            "matched_asset_count": len(assets),
            "exported_asset_count": len(corrections),
            "skipped_no_trade_asset_count": skipped_empty_count,
            "total_trade_count": total_trade_count,
        },
        "corrections": corrections,
    }

    output_path = os.path.abspath(args.output)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=max(args.indent, 0))
        handle.write("\n")

    print(
        "[OK] Export done: "
        f"matched={len(assets)}, exported={len(corrections)}, skipped_no_trade={skipped_empty_count}, "
        f"trades={total_trade_count} -> {output_path}"
    )
    print("[INFO] Edit this file and import with:")
    print(f"       python apply_trade_corrections.py --file \"{output_path}\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
