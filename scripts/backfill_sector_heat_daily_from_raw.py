from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sector_strategy_db import SectorStrategyDatabase


def main() -> None:
    parser = argparse.ArgumentParser(description="按原始板块数据回填每日热度面板")
    parser.add_argument("--db", default="sector_strategy.db", help="SQLite 数据库路径")
    parser.add_argument("--date", action="append", dest="dates", help="指定要回填的交易日，可重复传入")
    parser.add_argument("--start-date", default=None, help="起始交易日")
    parser.add_argument("--end-date", default=None, help="结束交易日")
    args = parser.parse_args()

    database = SectorStrategyDatabase(str(Path(args.db).expanduser().resolve()))
    result = database.rebuild_daily_heat_panels(
        start_date=args.start_date,
        end_date=args.end_date,
        dates=args.dates,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
