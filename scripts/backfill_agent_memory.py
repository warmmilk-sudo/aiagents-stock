from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent_memory_db import agent_memory_db
from agent_memory_service import agent_memory_service


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill agent memory from analysis-history records.")
    parser.add_argument("stock_code", help="Stock code, e.g. 300502")
    parser.add_argument("--keep-existing", action="store_true", help="Append to existing memory instead of rebuilding.")
    parser.add_argument("--skip-compress", action="store_true", help="Skip long-term profile compression.")
    args = parser.parse_args()

    stock_code = args.stock_code.strip().upper()
    result = agent_memory_service.backfill_from_analysis_history(
        stock_code,
        clear_existing=not args.keep_existing,
        compress_after=not args.skip_compress,
    )
    archive_preview = {
        "summary": agent_memory_db.get_memory_summary(stock_code),
        "working_memories": agent_memory_db.get_working_memory(stock_code, limit=3),
        "long_term_profile": agent_memory_db.get_long_term_profile(stock_code),
        "factual_top5": [
            {
                "id": item.get("id"),
                "timestamp": item.get("timestamp"),
                "importance_score": item.get("importance_score"),
                "fact_content": item.get("fact_content"),
            }
            for item in agent_memory_db.get_factual_memories(stock_code, include_ignored=False)[:5]
        ],
    }
    print(json.dumps({"result": result, "archive_preview": archive_preview}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
