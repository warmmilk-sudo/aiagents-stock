from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent
BACKUP_ROOT = ROOT_DIR / "data_backups"
WEEKDAY_CONFIG_FILES = ("monitor_schedule_config.json",)
MANAGED_DATABASES = {
    "stock_analysis.db": "分析历史库",
    "portfolio_stocks.db": "持仓库",
    "smart_monitor.db": "智能盯盘库",
    "monitoring.db": "监测事件库",
    "stock_monitor.db": "价格预警库",
    "news_flow.db": "新闻流量库",
    "macro_cycle.db": "宏观周期库",
    "sector_strategy.db": "智策板块库",
    "longhubang.db": "智瞰龙虎库",
    "main_force_batch.db": "主力选股历史库",
    "low_price_bull_monitor.db": "低价擒牛监控库",
    "profit_growth_monitor.db": "净利增长监控库",
    "investment.db": "投资总库",
    "stock_data_cache.db": "行情缓存库",
}


class DatabaseAdmin:
    def _database_entries(self) -> list[tuple[Path, str]]:
        entries: list[tuple[Path, str]] = []
        for filename, label in MANAGED_DATABASES.items():
            path = ROOT_DIR / filename
            if path.exists():
                entries.append((path, label))
        return entries

    def _backup_sources(self) -> list[Path]:
        sources: list[Path] = []
        for path, _label in self._database_entries():
            sources.append(path)
            for suffix in (".wal", ".shm"):
                sidecar = path.with_name(f"{path.name}{suffix}")
                if sidecar.exists():
                    sources.append(sidecar)
        for filename in WEEKDAY_CONFIG_FILES:
            path = ROOT_DIR / filename
            if path.exists():
                sources.append(path)
        return sources

    @staticmethod
    def _format_file_info(path: Path, *, label: str | None = None) -> dict[str, Any]:
        stat = path.stat()
        return {
            "name": path.name,
            "label": label or path.name,
            "size_bytes": stat.st_size,
            "updated_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        )
        return cursor.fetchone() is not None

    @staticmethod
    def _column_exists(cursor: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
        cursor.execute(f"PRAGMA table_info({table_name})")
        return any(row[1] == column_name for row in cursor.fetchall())

    def _delete_by_cutoff(
        self,
        cursor: sqlite3.Cursor,
        *,
        table_name: str,
        column_name: str,
        cutoff_value: str,
    ) -> int:
        if not self._table_exists(cursor, table_name) or not self._column_exists(cursor, table_name, column_name):
            return 0
        cursor.execute(
            f"DELETE FROM {table_name} WHERE datetime({column_name}) < datetime(?)",
            (cutoff_value,),
        )
        return int(cursor.rowcount or 0)

    def _cleanup_news_flow_history(self, db_path: Path, cutoff_value: str) -> dict[str, int]:
        deleted: dict[str, int] = {}
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            snapshot_ids: list[int] = []
            if self._table_exists(cursor, "flow_snapshots") and self._column_exists(cursor, "flow_snapshots", "created_at"):
                cursor.execute(
                    "SELECT id FROM flow_snapshots WHERE datetime(created_at) < datetime(?)",
                    (cutoff_value,),
                )
                snapshot_ids = [int(row[0]) for row in cursor.fetchall()]

            if snapshot_ids:
                placeholders = ",".join("?" for _ in snapshot_ids)
                for table_name in ("platform_news", "stock_related_news", "hot_topics", "sentiment_records", "ai_analysis"):
                    if self._table_exists(cursor, table_name) and self._column_exists(cursor, table_name, "snapshot_id"):
                        cursor.execute(
                            f"DELETE FROM {table_name} WHERE snapshot_id IN ({placeholders})",
                            snapshot_ids,
                        )
                        deleted[table_name] = int(cursor.rowcount or 0)
                if self._table_exists(cursor, "flow_alerts") and self._column_exists(cursor, "flow_alerts", "snapshot_id"):
                    cursor.execute(
                        f"DELETE FROM flow_alerts WHERE snapshot_id IN ({placeholders})",
                        snapshot_ids,
                    )
                    deleted["flow_alerts_by_snapshot"] = int(cursor.rowcount or 0)
                cursor.execute(
                    f"DELETE FROM flow_snapshots WHERE id IN ({placeholders})",
                    snapshot_ids,
                )
                deleted["flow_snapshots"] = int(cursor.rowcount or 0)

            deleted["flow_alerts"] = deleted.get("flow_alerts", 0) + self._delete_by_cutoff(
                cursor,
                table_name="flow_alerts",
                column_name="created_at",
                cutoff_value=cutoff_value,
            )
            deleted["flow_statistics"] = self._delete_by_cutoff(
                cursor,
                table_name="flow_statistics",
                column_name="created_at",
                cutoff_value=cutoff_value,
            )
            deleted["scheduler_logs"] = self._delete_by_cutoff(
                cursor,
                table_name="scheduler_logs",
                column_name="executed_at",
                cutoff_value=cutoff_value,
            )

            conn.commit()
            if sum(deleted.values()):
                cursor.execute("VACUUM")
            return {key: value for key, value in deleted.items() if value}
        finally:
            conn.close()

    def _cleanup_generic_history(
        self,
        db_path: Path,
        cleanup_targets: list[tuple[str, str]],
    ) -> dict[str, int]:
        cutoff_value = self._cleanup_cutoff
        deleted: dict[str, int] = {}
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            for table_name, column_name in cleanup_targets:
                count = self._delete_by_cutoff(
                    cursor,
                    table_name=table_name,
                    column_name=column_name,
                    cutoff_value=cutoff_value,
                )
                if count:
                    deleted[table_name] = count
            conn.commit()
            if deleted:
                cursor.execute("VACUUM")
            return deleted
        finally:
            conn.close()

    def get_status(self) -> dict[str, Any]:
        BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
        databases = [
            self._format_file_info(path, label=label)
            for path, label in self._database_entries()
        ]
        backups: list[dict[str, Any]] = []
        for backup_dir in sorted(BACKUP_ROOT.glob("backup_*"), reverse=True):
            if not backup_dir.is_dir():
                continue
            metadata_path = backup_dir / "metadata.json"
            if metadata_path.exists():
                try:
                    backups.append(json.loads(metadata_path.read_text(encoding="utf-8")))
                    continue
                except Exception:
                    pass
            files = [item for item in backup_dir.iterdir() if item.is_file()]
            backups.append(
                {
                    "name": backup_dir.name,
                    "created_at": datetime.fromtimestamp(backup_dir.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "file_count": len(files),
                    "size_bytes": sum(item.stat().st_size for item in files),
                }
            )
        return {
            "databases": databases,
            "backups": backups,
        }

    def create_backup(self) -> dict[str, Any]:
        BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = BACKUP_ROOT / f"backup_{timestamp}"
        backup_dir.mkdir(parents=True, exist_ok=True)

        copied_files: list[dict[str, Any]] = []
        for source in self._backup_sources():
            target = backup_dir / source.name
            shutil.copy2(source, target)
            copied_files.append(self._format_file_info(target))

        metadata = {
            "name": backup_dir.name,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "file_count": len(copied_files),
            "size_bytes": sum(item["size_bytes"] for item in copied_files),
            "files": copied_files,
        }
        (backup_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        return metadata

    def restore_backup(self, backup_name: str) -> dict[str, Any]:
        backup_dir = BACKUP_ROOT / backup_name
        if not backup_dir.is_dir():
            raise ValueError("未找到指定备份")

        restored_files: list[str] = []
        for source in backup_dir.iterdir():
            if not source.is_file() or source.name == "metadata.json":
                continue
            shutil.copy2(source, ROOT_DIR / source.name)
            restored_files.append(source.name)

        return {
            "backup_name": backup_name,
            "restored_files": restored_files,
            "restored_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def cleanup_history(self, days: int) -> dict[str, Any]:
        days = max(1, int(days))
        self._cleanup_cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        deleted_summary: list[dict[str, Any]] = []

        cleanup_plans: dict[str, list[tuple[str, str]]] = {
            "stock_analysis.db": [("analysis_records", "created_at")],
            "macro_cycle.db": [("macro_cycle_reports", "created_at")],
            "sector_strategy.db": [("sector_analysis_reports", "created_at")],
            "longhubang.db": [("longhubang_analysis", "created_at")],
            "main_force_batch.db": [("batch_analysis_history", "created_at")],
            "smart_monitor.db": [
                ("ai_decisions", "created_at"),
                ("notifications", "created_at"),
                ("monitoring_events", "created_at"),
                ("monitoring_price_history", "created_at"),
            ],
            "monitoring.db": [
                ("monitoring_events", "created_at"),
                ("monitoring_price_history", "created_at"),
            ],
            "stock_monitor.db": [
                ("monitoring_events", "created_at"),
                ("monitoring_price_history", "created_at"),
            ],
        }

        for filename, label in MANAGED_DATABASES.items():
            db_path = ROOT_DIR / filename
            if not db_path.exists():
                continue
            if filename == "news_flow.db":
                deleted = self._cleanup_news_flow_history(db_path, self._cleanup_cutoff)
            else:
                deleted = self._cleanup_generic_history(db_path, cleanup_plans.get(filename, []))
            if deleted:
                deleted_summary.append(
                    {
                        "database": filename,
                        "label": label,
                        "deleted_rows": sum(deleted.values()),
                        "details": deleted,
                    }
                )

        return {
            "days": days,
            "cutoff": self._cleanup_cutoff,
            "cleaned": deleted_summary,
            "total_deleted_rows": sum(item["deleted_rows"] for item in deleted_summary),
        }


database_admin = DatabaseAdmin()
