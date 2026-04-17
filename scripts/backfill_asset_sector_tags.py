from __future__ import annotations

import argparse
import json
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from asset_repository import AssetRepository
from data_source_manager import data_source_manager
from investment_db_utils import resolve_investment_db_path


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _extract_tags_from_stock_info(raw_value: Any) -> List[str]:
    if isinstance(raw_value, dict):
        stock_info = raw_value
    else:
        try:
            stock_info = json.loads(raw_value or "{}")
        except Exception:
            stock_info = {}
    if not isinstance(stock_info, dict):
        return []

    tags: List[str] = []

    def _append_tag(raw_text: Any) -> None:
        text = _normalize_text(raw_text)
        if not text or text.isdigit():
            return
        for part in text.replace("；", ";").replace("，", ",").replace("、", ",").split(";"):
            for piece in part.split(","):
                candidate = _normalize_text(piece)
                if not candidate or candidate.isdigit() or len(candidate) < 2:
                    continue
                if candidate not in tags:
                    tags.append(candidate)

    for key in (
        "industry",
        "sector",
        "concept",
        "concepts",
        "sectors",
        "sector_tags",
        "所属行业",
        "所属板块",
        "概念板块",
    ):
        value = stock_info.get(key)
        if isinstance(value, list):
            candidates = value
        elif isinstance(value, str):
            candidates = value.replace("，", ",").replace("、", ",").split(",")
        else:
            candidates = []
        for candidate in candidates:
            _append_tag(candidate)
    return tags[:12]


def _collect_tags(asset: Dict[str, Any], *, preserve_existing: bool) -> List[str]:
    tags: List[str] = []

    if preserve_existing:
        for tag in asset.get("sector_tags") or []:
            text = _normalize_text(tag)
            if text and text not in tags:
                tags.append(text)

    try:
        basic_info = data_source_manager.get_stock_tag_info_from_wencai(asset.get("symbol"))
    except Exception:
        basic_info = {}
    for tag in _extract_tags_from_stock_info(basic_info):
        if tag not in tags:
            tags.append(tag)

    return tags[:12]


def main() -> None:
    parser = argparse.ArgumentParser(description="回填资产概念/行业标签到数据库")
    parser.add_argument("--db", default="investment.db", help="SQLite 数据库路径")
    parser.add_argument("--apply", action="store_true", help="真正写入数据库")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有 sector_tags_json")
    parser.add_argument("--workers", type=int, default=4, help="并发抓取问财标签的线程数")
    parser.add_argument("--no-backup", action="store_true", help="不创建数据库备份")
    args = parser.parse_args()

    db_path = Path(resolve_investment_db_path(args.db))
    if db_path.exists() and not args.no_backup:
        backup_path = db_path.with_suffix(
            db_path.suffix + f".sector-tags-backup-{datetime.now():%Y%m%d%H%M%S}"
        )
        shutil.copy2(db_path, backup_path)
        print(f"已创建备份: {backup_path}")

    asset_repo = AssetRepository(str(db_path))

    assets = asset_repo.list_assets(include_deleted=False)
    total = len(assets)
    updated = 0
    skipped = 0
    empty = 0
    dry_run_samples: List[Dict[str, Any]] = []

    pending_assets = []
    for asset in assets:
        existing_tags = list(asset.get("sector_tags") or [])
        if existing_tags and not args.overwrite:
            skipped += 1
            continue
        pending_assets.append(asset)

    def _worker(asset: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "asset": asset,
            "tags": _collect_tags(asset, preserve_existing=not args.overwrite),
        }

    with ThreadPoolExecutor(max_workers=max(1, int(args.workers or 1))) as executor:
        futures = [executor.submit(_worker, asset) for asset in pending_assets]
        for future in as_completed(futures):
            payload = future.result()
            asset = payload["asset"]
            tags = payload["tags"]
            if not tags:
                empty += 1
                continue

            if not args.apply:
                if len(dry_run_samples) < 20:
                    dry_run_samples.append(
                        {
                            "symbol": asset.get("symbol"),
                            "name": asset.get("name"),
                            "tags": tags,
                        }
                    )
                updated += 1
                continue

            asset_repo.update_asset(int(asset["id"]), sector_tags_json=tags)
            updated += 1

    report = {
        "total_assets": total,
        "updated": updated,
        "skipped_existing": skipped,
        "empty_after_generation": empty,
        "applied": bool(args.apply),
        "overwrite": bool(args.overwrite),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not args.apply and dry_run_samples:
        print(json.dumps({"samples": dry_run_samples}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
