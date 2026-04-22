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

from smart_selection_service import SmartSelectionService


def main() -> None:
    parser = argparse.ArgumentParser(description="回填智能选股板块日热度历史")
    parser.add_argument("--db", default="investment.db", help="智能选股 SQLite 数据库路径")
    parser.add_argument("--no-backup", action="store_true", help="不创建数据库备份")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if db_path.exists() and not args.no_backup:
        backup_path = db_path.with_suffix(
            db_path.suffix + f".smart-selection-sector-heat-backup-{datetime.now():%Y%m%d%H%M%S}"
        )
        shutil.copy2(db_path, backup_path)
        print(f"已创建备份: {backup_path}")

    service = SmartSelectionService(str(db_path))
    result = service.backfill_sector_heat_daily_from_history()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
