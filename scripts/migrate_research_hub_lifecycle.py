from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from investment_db_utils import resolve_investment_db_path


def main() -> None:
    parser = argparse.ArgumentParser(description="迁移投研中心生命周期数据")
    parser.add_argument("--db", default="investment.db", help="SQLite 数据库路径")
    parser.add_argument("--force", action="store_true", help="忽略迁移标记并重新回填")
    parser.add_argument("--no-backup", action="store_true", help="不创建数据库备份")
    args = parser.parse_args()

    db_path = Path(resolve_investment_db_path(args.db))
    if db_path.exists() and not args.no_backup:
        backup_path = db_path.with_suffix(
            db_path.suffix + f".research-hub-backup-{datetime.now():%Y%m%d%H%M%S}"
        )
        shutil.copy2(db_path, backup_path)
        print(f"已创建备份: {backup_path}")

    from asset_repository import AssetRepository

    repository = AssetRepository(str(db_path))
    report = repository.backfill_lifecycle_data_from_legacy(force=args.force)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
