from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any


DB_PATH = Path("/home/aiagents-stock/investment.db")
RATING_FIELDS = {"rating", "investment_rating", "raw_model_rating"}
LABEL_REPLACEMENTS = {
    "增持": "加仓",
    "减持": "减仓",
}
TABLES = ("analysis_records", "portfolio_analysis_history")


def _replace_label(value: Any) -> Any:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped in LABEL_REPLACEMENTS:
            return LABEL_REPLACEMENTS[stripped]
    return value


def _transform_payload(payload: Any) -> tuple[Any, bool]:
    changed = False

    if isinstance(payload, dict):
        updated = {}
        for key, value in payload.items():
            next_value = value
            if key in RATING_FIELDS:
                next_value = _replace_label(value)
            elif key == "calibration_notes" and isinstance(value, list):
                next_value = [
                    item.replace("增持", "加仓").replace("减持", "减仓") if isinstance(item, str) else item
                    for item in value
                ]
            elif isinstance(value, (dict, list)):
                next_value, nested_changed = _transform_payload(value)
                changed = changed or nested_changed
            if next_value != value:
                changed = True
            updated[key] = next_value
        return updated, changed

    if isinstance(payload, list):
        updated = []
        for item in payload:
            next_item = item
            if isinstance(item, (dict, list)):
                next_item, nested_changed = _transform_payload(item)
                changed = changed or nested_changed
            updated.append(next_item)
        return updated, changed

    return payload, False


def migrate_table(conn: sqlite3.Connection, table_name: str) -> dict[str, int]:
    cur = conn.cursor()
    cur.execute(f"SELECT id, rating, final_decision_json FROM {table_name}")
    rows = cur.fetchall()

    updated_rows = 0
    updated_rating = 0
    updated_json = 0

    for row_id, rating, final_decision_json in rows:
        next_rating = _replace_label(rating)
        rating_changed = next_rating != rating

        next_json_text = final_decision_json
        json_changed = False
        if final_decision_json:
            try:
                payload = json.loads(final_decision_json)
            except json.JSONDecodeError:
                payload = None
            if payload is not None:
                next_payload, json_changed = _transform_payload(payload)
                if json_changed:
                    next_json_text = json.dumps(next_payload, ensure_ascii=False, separators=(",", ":"))

        if not rating_changed and not json_changed:
            continue

        cur.execute(
            f"UPDATE {table_name} SET rating = ?, final_decision_json = ? WHERE id = ?",
            (next_rating, next_json_text, row_id),
        )
        updated_rows += 1
        updated_rating += int(rating_changed)
        updated_json += int(json_changed)

    return {
        "rows": len(rows),
        "updated_rows": updated_rows,
        "updated_rating": updated_rating,
        "updated_json": updated_json,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate analysis rating labels from 增持/减持 to 加仓/减仓.")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to investment sqlite db.")
    args = parser.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)
    try:
        totals = {}
        for table_name in TABLES:
            totals[table_name] = migrate_table(conn, table_name)
        conn.commit()
    finally:
        conn.close()

    for table_name, stats in totals.items():
        print(
            f"{table_name}: scanned={stats['rows']} updated_rows={stats['updated_rows']} "
            f"rating_updates={stats['updated_rating']} json_updates={stats['updated_json']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
