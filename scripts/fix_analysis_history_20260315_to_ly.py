#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from asset_repository import AssetRepository  # noqa: E402
from asset_service import AssetService  # noqa: E402
from analysis_repository import AnalysisRepository  # noqa: E402
from monitoring_repository import MonitoringRepository  # noqa: E402


SOURCE_ACCOUNT = "默认账户"
TARGET_ACCOUNT = "ly"
TARGET_START_AT = "2026-03-15 18:59:58"
EXPECTED_RECORDS = {
    116: {"symbol": "002407", "stock_name": "多氟多", "analysis_date": "2026-03-15 18:59:58"},
    117: {"symbol": "601208", "stock_name": "东材科技", "analysis_date": "2026-03-15 19:10:12"},
    118: {"symbol": "002837", "stock_name": "英维克", "analysis_date": "2026-03-15 19:19:14"},
    119: {"symbol": "002709", "stock_name": "天赐材料", "analysis_date": "2026-03-15 19:28:16"},
    120: {"symbol": "603986", "stock_name": "兆易创新", "analysis_date": "2026-03-15 19:37:11"},
    121: {"symbol": "002916", "stock_name": "深南电路", "analysis_date": "2026-03-15 19:45:52"},
}


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True)
class TargetBinding:
    record_id: int
    symbol: str
    stock_name: str
    analysis_date: str
    current_account_name: str
    current_asset_id: int | None
    current_portfolio_stock_id: int | None
    current_asset_status_snapshot: str | None
    target_asset_id: int
    target_origin_analysis_id: int | None
    source_asset_id: int | None
    source_asset_deleted_at: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "一次性修复 2026-03-15 18:59:58 起的 6 条分析历史，"
            "将其重挂到账户 ly 的正式持仓资产，并同步监控状态。"
        )
    )
    parser.add_argument(
        "--db-path",
        default="investment.db",
        help="投资数据库路径，默认使用项目根目录下的 investment.db",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="实际写入数据库；默认只做 dry-run 检查",
    )
    return parser.parse_args()


def resolve_db_path(raw_path: str) -> Path:
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    return candidate.resolve()


def backup_database(db_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = db_path.with_name(f"{db_path.name}.bak-{timestamp}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def fetch_rows(
    cursor: sqlite3.Cursor,
    sql: str,
    params: Iterable[object] = (),
) -> list[sqlite3.Row]:
    cursor.execute(sql, tuple(params))
    return cursor.fetchall()


def fetch_one(cursor: sqlite3.Cursor, sql: str, params: Iterable[object] = ()) -> sqlite3.Row | None:
    cursor.execute(sql, tuple(params))
    return cursor.fetchone()


def extract_first_number(value: object) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    for token in re.findall(r"\d+(?:\.\d+)?", str(value)):
        return float(token)
    return None


def extract_entry_range(value: object) -> tuple[float | None, float | None]:
    if value in (None, ""):
        return None, None
    if isinstance(value, dict):
        return extract_first_number(value.get("min")), extract_first_number(value.get("max"))
    text = str(value).replace("~", "-").replace("至", "-").replace("到", "-").replace("或", "-")
    numbers = [float(token) for token in re.findall(r"\d+(?:\.\d+)?", text)]
    if len(numbers) >= 2:
        return numbers[0], numbers[1]
    return (numbers[0], None) if numbers else (None, None)


def load_target_bindings(connection: sqlite3.Connection) -> list[TargetBinding]:
    cursor = connection.cursor()
    bindings: list[TargetBinding] = []

    actual_count = fetch_one(
        cursor,
        """
        SELECT COUNT(*) AS total
        FROM analysis_records
        WHERE analysis_date >= ?
        """,
        (TARGET_START_AT,),
    )
    if int(actual_count["total"]) != len(EXPECTED_RECORDS):
        raise MigrationError(
            f"从 {TARGET_START_AT} 开始的分析记录数量不是 6 条，当前为 {actual_count['total']} 条，停止执行。"
        )

    for record_id, expected in EXPECTED_RECORDS.items():
        record = fetch_one(
            cursor,
            """
            SELECT id, symbol, stock_name, account_name, asset_id, portfolio_stock_id,
                   analysis_date, asset_status_snapshot
            FROM analysis_records
            WHERE id = ?
            """,
            (record_id,),
        )
        if record is None:
            raise MigrationError(f"未找到分析记录 #{record_id}")
        if record["symbol"] != expected["symbol"]:
            raise MigrationError(
                f"分析记录 #{record_id} 股票代码不匹配，期望 {expected['symbol']}，实际 {record['symbol']}"
            )
        if (record["stock_name"] or "") != expected["stock_name"]:
            raise MigrationError(
                f"分析记录 #{record_id} 股票名称不匹配，期望 {expected['stock_name']}，实际 {record['stock_name']}"
            )
        if record["analysis_date"] != expected["analysis_date"]:
            raise MigrationError(
                f"分析记录 #{record_id} 时间不匹配，期望 {expected['analysis_date']}，实际 {record['analysis_date']}"
            )

        target_assets = fetch_rows(
            cursor,
            """
            SELECT id, origin_analysis_id
            FROM assets
            WHERE symbol = ?
              AND account_name = ?
              AND deleted_at IS NULL
            ORDER BY CASE status WHEN 'portfolio' THEN 0 WHEN 'watchlist' THEN 1 ELSE 2 END,
                     datetime(updated_at) DESC,
                     id DESC
            """,
            (record["symbol"], TARGET_ACCOUNT),
        )
        if len(target_assets) != 1:
            raise MigrationError(
                f"{record['symbol']} 在账户 {TARGET_ACCOUNT} 下应只有 1 条有效资产，当前为 {len(target_assets)} 条。"
            )
        target_asset = target_assets[0]
        existing_origin_analysis_id = target_asset["origin_analysis_id"]
        if existing_origin_analysis_id not in (None, record_id):
            raise MigrationError(
                f"{record['symbol']} 的目标资产 {target_asset['id']} 已绑定 origin_analysis_id="
                f"{existing_origin_analysis_id}，不会覆盖。"
            )

        source_asset = fetch_one(
            cursor,
            """
            SELECT id, deleted_at
            FROM assets
            WHERE symbol = ?
              AND account_name = ?
              AND status = 'research'
              AND origin_analysis_id = ?
            ORDER BY CASE WHEN deleted_at IS NULL THEN 0 ELSE 1 END, id ASC
            LIMIT 1
            """,
            (record["symbol"], SOURCE_ACCOUNT, record_id),
        )

        bindings.append(
            TargetBinding(
                record_id=record_id,
                symbol=record["symbol"],
                stock_name=record["stock_name"],
                analysis_date=record["analysis_date"],
                current_account_name=record["account_name"] or "",
                current_asset_id=record["asset_id"],
                current_portfolio_stock_id=record["portfolio_stock_id"],
                current_asset_status_snapshot=record["asset_status_snapshot"],
                target_asset_id=int(target_asset["id"]),
                target_origin_analysis_id=target_asset["origin_analysis_id"],
                source_asset_id=int(source_asset["id"]) if source_asset else None,
                source_asset_deleted_at=source_asset["deleted_at"] if source_asset else None,
            )
        )

    return bindings


def ensure_no_unexpected_references(connection: sqlite3.Connection, binding: TargetBinding) -> None:
    if binding.source_asset_id is None:
        return

    cursor = connection.cursor()
    unexpected = []

    def count(sql: str, params: tuple[object, ...]) -> int:
        row = fetch_one(cursor, sql, params)
        return int(row["total"]) if row is not None else 0

    extra_analysis_asset_refs = count(
        """
        SELECT COUNT(*) AS total
        FROM analysis_records
        WHERE asset_id = ?
          AND id <> ?
        """,
        (binding.source_asset_id, binding.record_id),
    )
    extra_analysis_portfolio_refs = count(
        """
        SELECT COUNT(*) AS total
        FROM analysis_records
        WHERE portfolio_stock_id = ?
          AND id <> ?
        """,
        (binding.source_asset_id, binding.record_id),
    )
    if extra_analysis_asset_refs:
        unexpected.append(f"analysis_records.asset_id={binding.source_asset_id} 还有 {extra_analysis_asset_refs} 条额外引用")
    if extra_analysis_portfolio_refs:
        unexpected.append(
            f"analysis_records.portfolio_stock_id={binding.source_asset_id} 还有 {extra_analysis_portfolio_refs} 条额外引用"
        )

    for table, column in (
        ("asset_trade_history", "asset_id"),
        ("asset_action_queue", "asset_id"),
    ):
        total = count(f"SELECT COUNT(*) AS total FROM {table} WHERE {column} = ?", (binding.source_asset_id,))
        if total:
            unexpected.append(f"{table}.{column} 仍引用 {binding.source_asset_id} 共 {total} 条")

    monitor_refs = count(
        """
        SELECT COUNT(*) AS total
        FROM monitoring_items
        WHERE asset_id = ?
           OR portfolio_stock_id = ?
           OR (
                origin_analysis_id = ?
                AND symbol = ?
                AND (
                    COALESCE(asset_id, -1) <> ?
                    OR COALESCE(portfolio_stock_id, -1) <> ?
                )
           )
        """,
        (
            binding.source_asset_id,
            binding.source_asset_id,
            binding.record_id,
            binding.symbol,
            binding.target_asset_id,
            binding.target_asset_id,
        ),
    )
    if monitor_refs:
        unexpected.append(f"monitoring_items 仍有 {monitor_refs} 条记录与 source asset/origin 关联")

    ai_decision_refs = count(
        """
        SELECT COUNT(*) AS total
        FROM ai_decisions
        WHERE asset_id = ?
           OR portfolio_stock_id = ?
           OR (
                origin_analysis_id = ?
                AND stock_code = ?
                AND (
                    COALESCE(asset_id, -1) <> ?
                    OR COALESCE(portfolio_stock_id, -1) <> ?
                )
           )
        """,
        (
            binding.source_asset_id,
            binding.source_asset_id,
            binding.record_id,
            binding.symbol,
            binding.target_asset_id,
            binding.target_asset_id,
        ),
    )
    if ai_decision_refs:
        unexpected.append(f"ai_decisions 仍有 {ai_decision_refs} 条记录与 source asset/origin 关联")

    if unexpected:
        details = "\n".join(f"- {item}" for item in unexpected)
        raise MigrationError(
            f"{binding.symbol} 的 source asset {binding.source_asset_id} 存在未预期引用，停止执行:\n{details}"
        )


def print_plan(bindings: list[TargetBinding]) -> None:
    print(f"目标时间起点: {TARGET_START_AT}")
    print(f"目标账户: {TARGET_ACCOUNT}")
    print(f"待处理记录: {len(bindings)}")
    print()
    for binding in bindings:
        print(
            f"#{binding.record_id} {binding.symbol} {binding.stock_name} | "
            f"当前 account={binding.current_account_name or '-'} asset_id={binding.current_asset_id} "
            f"portfolio_stock_id={binding.current_portfolio_stock_id} snapshot={binding.current_asset_status_snapshot or '-'}"
        )
        print(
            f"  -> 目标 account={TARGET_ACCOUNT} asset_id={binding.target_asset_id} "
            f"portfolio_stock_id={binding.target_asset_id} snapshot=portfolio"
        )
        if binding.source_asset_id is not None:
            source_state = "已软删除" if binding.source_asset_deleted_at else "待软删除"
            print(f"  -> source research asset={binding.source_asset_id} ({source_state})")
        else:
            print("  -> source research asset=未找到（如果之前已处理，可忽略）")
    print()


def apply_core_updates(connection: sqlite3.Connection, bindings: list[TargetBinding]) -> None:
    cursor = connection.cursor()
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("BEGIN IMMEDIATE")
    try:
        for binding in bindings:
            ensure_no_unexpected_references(connection, binding)
            strategy_row = fetch_one(
                cursor,
                """
                SELECT entry_min, entry_max, take_profit, stop_loss, final_decision_json
                FROM analysis_records
                WHERE id = ?
                """,
                (binding.record_id,),
            )
            if strategy_row is None:
                raise MigrationError(f"未找到分析记录 #{binding.record_id}")

            final_decision = {}
            if strategy_row["final_decision_json"]:
                try:
                    parsed = json.loads(strategy_row["final_decision_json"])
                    if isinstance(parsed, dict):
                        final_decision = parsed
                except json.JSONDecodeError:
                    final_decision = {}

            entry_min = strategy_row["entry_min"]
            entry_max = strategy_row["entry_max"]
            if entry_min is None or entry_max is None:
                parsed_entry_min, parsed_entry_max = extract_entry_range(final_decision.get("entry_range"))
                entry_min = entry_min if entry_min is not None else parsed_entry_min
                entry_max = entry_max if entry_max is not None else parsed_entry_max

            take_profit = strategy_row["take_profit"]
            if take_profit is None:
                take_profit = extract_first_number(final_decision.get("take_profit"))

            stop_loss = strategy_row["stop_loss"]
            if stop_loss is None:
                stop_loss = extract_first_number(final_decision.get("stop_loss"))

            cursor.execute(
                """
                UPDATE analysis_records
                SET account_name = ?,
                    asset_id = ?,
                    portfolio_stock_id = ?,
                    asset_status_snapshot = ?,
                    entry_min = ?,
                    entry_max = ?,
                    take_profit = ?,
                    stop_loss = ?
                WHERE id = ?
                """,
                (
                    TARGET_ACCOUNT,
                    binding.target_asset_id,
                    binding.target_asset_id,
                    "portfolio",
                    entry_min,
                    entry_max,
                    take_profit,
                    stop_loss,
                    binding.record_id,
                ),
            )

            cursor.execute(
                """
                UPDATE assets
                SET origin_analysis_id = ?,
                    updated_at = ?
                WHERE id = ?
                  AND deleted_at IS NULL
                  AND (origin_analysis_id IS NULL OR origin_analysis_id = ?)
                """,
                (binding.record_id, now_text, binding.target_asset_id, binding.record_id),
            )
            if cursor.rowcount != 1:
                raise MigrationError(
                    f"{binding.symbol} 的目标资产 {binding.target_asset_id} 未能安全回填 origin_analysis_id={binding.record_id}"
                )

            if binding.source_asset_id is not None and binding.source_asset_deleted_at is None:
                cursor.execute(
                    """
                    UPDATE assets
                    SET deleted_at = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND deleted_at IS NULL
                    """,
                    (now_text, now_text, binding.source_asset_id),
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def sync_monitoring(bindings: list[TargetBinding], db_path: Path) -> None:
    asset_repository = AssetRepository(str(db_path))
    analysis_repository = AnalysisRepository(str(db_path), legacy_analysis_db_path=str(db_path))
    monitoring_repository = MonitoringRepository(str(db_path))
    service = AssetService(
        asset_store=asset_repository,
        analysis_store=analysis_repository,
        monitoring_store=monitoring_repository,
    )
    synced_asset_ids: set[int] = set()
    for binding in bindings:
        if binding.target_asset_id in synced_asset_ids:
            continue
        synced_asset_ids.add(binding.target_asset_id)
        service.sync_managed_monitors(binding.target_asset_id)


def verify_state(connection: sqlite3.Connection, bindings: list[TargetBinding]) -> None:
    cursor = connection.cursor()
    for binding in bindings:
        record = fetch_one(
            cursor,
            """
            SELECT account_name, asset_id, portfolio_stock_id, asset_status_snapshot,
                   entry_min, entry_max, take_profit, stop_loss
            FROM analysis_records
            WHERE id = ?
            """,
            (binding.record_id,),
        )
        if record is None:
            raise MigrationError(f"校验失败: 分析记录 #{binding.record_id} 消失了")
        if record["account_name"] != TARGET_ACCOUNT:
            raise MigrationError(f"校验失败: 分析记录 #{binding.record_id} account_name 未改成 {TARGET_ACCOUNT}")
        if int(record["asset_id"] or 0) != binding.target_asset_id:
            raise MigrationError(f"校验失败: 分析记录 #{binding.record_id} asset_id 未改成 {binding.target_asset_id}")
        if int(record["portfolio_stock_id"] or 0) != binding.target_asset_id:
            raise MigrationError(
                f"校验失败: 分析记录 #{binding.record_id} portfolio_stock_id 未改成 {binding.target_asset_id}"
            )
        if (record["asset_status_snapshot"] or "") != "portfolio":
            raise MigrationError(f"校验失败: 分析记录 #{binding.record_id} asset_status_snapshot 未改成 portfolio")
        if None in (record["entry_min"], record["entry_max"], record["take_profit"], record["stop_loss"]):
            raise MigrationError(f"校验失败: 分析记录 #{binding.record_id} 仍存在缺失的监控阈值字段")

        asset = fetch_one(
            cursor,
            """
            SELECT origin_analysis_id
            FROM assets
            WHERE id = ?
              AND deleted_at IS NULL
            """,
            (binding.target_asset_id,),
        )
        if asset is None:
            raise MigrationError(f"校验失败: 目标资产 {binding.target_asset_id} 不存在")
        if int(asset["origin_analysis_id"] or 0) != binding.record_id:
            raise MigrationError(
                f"校验失败: 目标资产 {binding.target_asset_id} origin_analysis_id 不是 {binding.record_id}"
            )

        if binding.source_asset_id is not None:
            source_asset = fetch_one(
                cursor,
                """
                SELECT deleted_at
                FROM assets
                WHERE id = ?
                """,
                (binding.source_asset_id,),
            )
            if source_asset is None or not source_asset["deleted_at"]:
                raise MigrationError(f"校验失败: source asset {binding.source_asset_id} 没有被软删除")

        monitors = fetch_rows(
            cursor,
            """
            SELECT monitor_type, origin_analysis_id, config_json
            FROM monitoring_items
            WHERE asset_id = ?
            ORDER BY monitor_type ASC
            """,
            (binding.target_asset_id,),
        )
        if len(monitors) < 2:
            raise MigrationError(f"校验失败: 目标资产 {binding.target_asset_id} 的监控项数量不足")
        for monitor in monitors:
            if monitor["origin_analysis_id"] not in (binding.record_id, None):
                raise MigrationError(
                    f"校验失败: 监控项 {binding.symbol}/{monitor['monitor_type']} origin_analysis_id="
                    f"{monitor['origin_analysis_id']}，不是 {binding.record_id}"
                )
            config = json.loads(monitor["config_json"] or "{}")
            if monitor["monitor_type"] == "price_alert":
                if config.get("threshold_source") != "strategy_context":
                    raise MigrationError(
                        f"校验失败: {binding.symbol} 的价格预警 threshold_source 仍是 {config.get('threshold_source')}"
                    )
                levels = (
                    ((config.get("entry_range") or {}).get("min")),
                    ((config.get("entry_range") or {}).get("max")),
                    config.get("take_profit"),
                    config.get("stop_loss"),
                )
                if any(value is None for value in levels):
                    raise MigrationError(f"校验失败: {binding.symbol} 的价格预警阈值仍不完整")


def main() -> int:
    args = parse_args()
    db_path = resolve_db_path(args.db_path)
    if not db_path.exists():
        raise MigrationError(f"数据库不存在: {db_path}")

    connection = connect(db_path)
    try:
        bindings = load_target_bindings(connection)
        print_plan(bindings)

        if not args.apply:
            print("dry-run 完成，未写入数据库。")
            return 0
    finally:
        connection.close()

    backup_path = backup_database(db_path)
    print(f"已创建备份: {backup_path}")

    connection = connect(db_path)
    try:
        apply_core_updates(connection, bindings)
        sync_monitoring(bindings, db_path)
        verify_state(connection, bindings)
    finally:
        connection.close()

    print("已完成修复，并同步相关监控状态。")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MigrationError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise SystemExit(1)
