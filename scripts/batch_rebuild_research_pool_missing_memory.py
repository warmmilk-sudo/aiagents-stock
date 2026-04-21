#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config
from agent_memory_db import agent_memory_db
from agent_memory_service import AgentMemoryService
from asset_repository import STATUS_RESEARCH, asset_repository
from deepseek_client import DeepSeekClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch rebuild memory for research-pool stocks that have no existing memory records."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually run rebuilds. Without this flag the script only prints the target list.",
    )
    parser.add_argument(
        "--skip-compress",
        action="store_true",
        help="Skip long-term profile compression after backfill.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent workers to use when applying rebuilds.",
    )
    return parser.parse_args()


def load_missing_research_pool_stocks() -> list[dict[str, object]]:
    assets = asset_repository.list_assets(status=STATUS_RESEARCH, include_deleted=False)
    targets: list[dict[str, object]] = []
    for asset in assets:
        symbol = str(asset.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        summary = agent_memory_db.get_memory_summary(symbol)
        if (
            int(summary.get("working_count") or 0) == 0
            and int(summary.get("factual_count") or 0) == 0
            and not bool(summary.get("has_long_term_profile"))
        ):
            targets.append(
                {
                    "symbol": symbol,
                    "name": asset.get("name") or symbol,
                }
            )
    return targets


def rebuild_one_target(symbol: str, name: str, skip_compress: bool) -> dict[str, object]:
    service = AgentMemoryService(
        db=agent_memory_db,
        llm_client=DeepSeekClient(model=config.LIGHTWEIGHT_MODEL_NAME),
    )
    result = service.backfill_from_analysis_history(
        stock_code=symbol,
        clear_existing=True,
        compress_after=not skip_compress,
    )
    return {
        "symbol": symbol,
        "name": name,
        "record_count": result.get("record_count", 0),
        "working_saved": result.get("working_saved", 0),
        "facts_saved": result.get("facts_saved", 0),
        "compressed": result.get("compressed", False),
        "summary": result.get("summary", {}),
    }


def main() -> int:
    args = parse_args()
    targets = load_missing_research_pool_stocks()

    print(
        json.dumps(
            {
                "research_pool_total": len(asset_repository.list_assets(status=STATUS_RESEARCH, include_deleted=False)),
                "missing_memory_total": len(targets),
                "lightweight_model": config.LIGHTWEIGHT_MODEL_NAME,
                "apply": bool(args.apply),
                "skip_compress": bool(args.skip_compress),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    for item in targets:
        print(f"- {item['symbol']} {item['name']}")

    if not args.apply:
        return 0

    rebuilt = []
    failed = []
    worker_count = max(1, int(args.workers or 1))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(rebuild_one_target, str(item["symbol"]), str(item["name"]), bool(args.skip_compress)): item
            for item in targets
        }
        for future in as_completed(future_map):
            item = future_map[future]
            symbol = str(item["symbol"])
            try:
                result = future.result()
                rebuilt.append(result)
                print(
                    f"[OK] {symbol} {item['name']} "
                    f"records={result.get('record_count', 0)} "
                    f"working={result.get('working_saved', 0)} "
                    f"facts={result.get('facts_saved', 0)} "
                    f"compressed={bool(result.get('compressed', False))}",
                    flush=True,
                )
            except Exception as exc:
                failed.append({"symbol": symbol, "name": item["name"], "error": str(exc)})
                print(f"[ERR] {symbol} {item['name']}: {exc}", flush=True)

    print(
        json.dumps(
            {
                "rebuilt_total": len(rebuilt),
                "failed_total": len(failed),
                "rebuilt": rebuilt[:50],
                "failed": failed,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
