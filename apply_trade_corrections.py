"""
离线批量修正持仓交易流水脚本。

示例:
python apply_trade_corrections.py --file trade_corrections.json
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime
from typing import Any, Dict, List, Tuple


def _load_json_file(file_path: str) -> Any:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_corrections(payload: Any) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if isinstance(payload, list):
        return payload, {}

    if not isinstance(payload, dict):
        raise ValueError("JSON 顶层必须是对象或数组")

    corrections = None
    for key in ("corrections", "items", "stocks", "records", "修正列表"):
        value = payload.get(key)
        if isinstance(value, list):
            corrections = value
            break

    if corrections is None and (
        payload.get("trades") is not None
        or payload.get("trade_history") is not None
        or payload.get("交易记录") is not None
    ):
        corrections = [payload]

    if corrections is None:
        raise ValueError("未找到修正列表，请使用 corrections/items/stocks/records 字段")

    options = {
        "default_account_name": payload.get("default_account_name") or payload.get("account_name"),
        "default_trade_source": payload.get("default_trade_source") or payload.get("trade_source"),
        "default_status_when_flat": payload.get("default_status_when_flat") or payload.get("status_when_flat"),
    }
    return corrections, options


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="离线批量修正持仓交易流水")
    parser.add_argument("--file", required=True, help="修正 JSON 文件路径")
    parser.add_argument("--db-path", default="investment.db", help="投资数据库路径，默认 investment.db")
    parser.add_argument("--default-account-name", default=None, help="JSON 未指定账户时使用的默认账户名")
    parser.add_argument("--default-trade-source", default=None, help="JSON 未指定来源时使用的默认来源")
    parser.add_argument("--default-status-when-flat", default=None, help="最终空仓状态: watchlist/research")
    parser.add_argument("--sync", action="store_true", help="修正后同步下游监测联动")
    parser.add_argument("--snapshot", action="store_true", help="修正后补写当日快照")
    parser.add_argument("--no-backup", action="store_true", help="执行前不备份数据库")
    return parser


def _backup_database(db_path: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"{db_path}.bak.{timestamp}"
    shutil.copy2(db_path, backup_path)
    return backup_path


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()

    from portfolio_db import PortfolioDB
    from portfolio_manager import PortfolioManager

    json_payload = _load_json_file(args.file)
    corrections, options = _extract_corrections(json_payload)
    if not corrections:
        print("[WARN] 修正列表为空，无需执行")
        return 0

    portfolio_db = PortfolioDB(args.db_path)
    db_path = portfolio_db.db_path

    if not args.no_backup and os.path.exists(db_path):
        backup_path = _backup_database(db_path)
        print(f"[OK] 已备份数据库: {backup_path}")

    manager = PortfolioManager(portfolio_store=portfolio_db)
    summary = manager.apply_trade_corrections(
        corrections,
        default_account_name=args.default_account_name or options.get("default_account_name") or "默认账户",
        default_trade_source=args.default_trade_source or options.get("default_trade_source") or "manual_fix",
        default_status_when_flat=args.default_status_when_flat or options.get("default_status_when_flat") or "watchlist",
        capture_snapshot=bool(args.snapshot),
        sync_integrations=bool(args.sync),
    )

    print(f"[INFO] 总计: {summary['total']}，成功: {summary['succeeded']}，失败: {summary['failed']}")
    for item in summary["results"]:
        if item.get("success"):
            replace_result = item.get("replace_result") or {}
            print(
                f"[OK] #{item.get('index')} {item.get('symbol')}({item.get('account_name')}) "
                f"交易数={replace_result.get('trade_count', 0)} "
                f"状态={replace_result.get('final_status')} "
                f"数量={replace_result.get('final_quantity')}"
            )
            for warning in item.get("warnings") or []:
                print(f"  [WARN] {warning}")
        else:
            print(f"[ERR] #{item.get('index')} {item.get('message')}")

    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
