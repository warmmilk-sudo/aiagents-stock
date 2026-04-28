#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config
from monitor_memory_backfill_service import MonitorRelatedExecutionMemoryBackfillService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill execution-plan memory for monitor-related stocks."
    )
    parser.add_argument("--apply", action="store_true", help="Write updates. Without this flag, only prints targets.")
    parser.add_argument("--workers", type=int, default=None, help="Concurrent workers for apply mode.")
    parser.add_argument("--limit", type=int, default=None, help="Limit target stock count.")
    parser.add_argument("--stock-code", default="", help="Only process one stock code.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing execution-plan fields.")
    parser.add_argument("--no-force", action="store_true", help="Do not overwrite records that already have execution-plan fields.")
    parser.add_argument("--skip-compress", action="store_true", help="Skip long-term profile compression.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.force and args.no_force:
        raise SystemExit("--force and --no-force cannot be used together")

    force = True if args.force else False if args.no_force else getattr(config, "MEMORY_BACKFILL_FORCE_OVERWRITE", True)
    service = MonitorRelatedExecutionMemoryBackfillService()
    result = service.run(
        apply=bool(args.apply),
        workers=args.workers,
        limit=args.limit,
        stock_code=args.stock_code or None,
        force=force,
        compress_after=not args.skip_compress,
        progress=lambda **updates: print(
            f"[{updates.get('current', 0)}/{updates.get('total', 0)}] {updates.get('message', '')}",
            flush=True,
        ),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not result.get("failed_total") else 1


if __name__ == "__main__":
    raise SystemExit(main())
