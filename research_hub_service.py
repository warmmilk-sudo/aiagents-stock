from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from analysis_repository import analysis_repository
from asset_repository import STATUS_FOCUS, STATUS_HOLDING, STATUS_RESEARCH, asset_repository
from asset_service import asset_service
from deepseek_client import DeepSeekClient
from investment_db_utils import connect_sqlite
from model_routing import ModelTier
from prompt_registry import build_messages
from ui_analysis_task_utils import (
    start_ui_analysis_task,
)


RESEARCH_HUB_SELECTION_TASK_TYPE = "research_hub_selection"
RESEARCH_HUB_FUNNEL_TASK_TYPE = RESEARCH_HUB_SELECTION_TASK_TYPE
FOCUS_CAPACITY = 10
SELECTION_TOP_K = 10
SELECTION_RANK_LIMIT = 15
SECTOR_MATCH_FALLBACK_LIMIT = 60
SECTOR_REPORT_MAX_AGE_HOURS = 12
SECTOR_WAIT_TIMEOUT_SECONDS = 900
SECTOR_WAIT_INTERVAL_SECONDS = 2
SELECTION_NEGATIVE_KEYWORDS = ("减持", "监管函", "立案", "处罚", "问询", "亏损", "预减", "ST")
RISK_NEWS_LOOKBACK_DAYS = 2
RISK_EVENT_LOOKAHEAD_DAYS = 2
RISK_ITEM_LIMIT = 6

SELECTION_MARKET_STATE_ICE = "ice"
SELECTION_MARKET_STATE_RANGE = "range"
SELECTION_MARKET_STATE_MOMO = "momentum"
SELECTION_MARKET_STATE_RETREAT = "retreat"

SELECTION_MARKET_STATE_LABELS = {
    SELECTION_MARKET_STATE_ICE: "冰点期",
    SELECTION_MARKET_STATE_RANGE: "震荡市",
    SELECTION_MARKET_STATE_MOMO: "主升浪",
    SELECTION_MARKET_STATE_RETREAT: "高位退潮期",
}

SELECTION_AGENT_PROFILES: Dict[str, Dict[str, Any]] = {
    SELECTION_MARKET_STATE_MOMO: {
        "label": "主升浪",
        "heat_multiplier": 0.92,
        "agent_multiplier": 1.12,
        "weights": {
            "trend": 24,
            "breakout": 18,
            "volume": 18,
            "intraday": 14,
            "order_flow": 12,
            "chip": 8,
            "reversal": 3,
            "mean_reversion": 3,
        },
        "bonus_rules": {"momentum_confirmation": 8, "smart_money_follow": 6, "distribution_penalty": -14},
    },
    SELECTION_MARKET_STATE_RANGE: {
        "label": "震荡市",
        "heat_multiplier": 0.96,
        "agent_multiplier": 1.0,
        "weights": {
            "trend": 18,
            "breakout": 10,
            "volume": 12,
            "intraday": 12,
            "order_flow": 12,
            "chip": 14,
            "reversal": 10,
            "mean_reversion": 12,
        },
        "bonus_rules": {"tight_structure": 6, "smart_money_follow": 4, "distribution_penalty": -10},
    },
    SELECTION_MARKET_STATE_ICE: {
        "label": "冰点期",
        "heat_multiplier": 1.0,
        "agent_multiplier": 0.94,
        "weights": {
            "trend": 10,
            "breakout": 4,
            "volume": 8,
            "intraday": 10,
            "order_flow": 10,
            "chip": 14,
            "reversal": 20,
            "mean_reversion": 24,
        },
        "bonus_rules": {"washout_reversal": 10, "smart_money_follow": 5, "distribution_penalty": -8},
    },
    SELECTION_MARKET_STATE_RETREAT: {
        "label": "高位退潮期",
        "heat_multiplier": 0.82,
        "agent_multiplier": 0.98,
        "weights": {
            "trend": 12,
            "breakout": 6,
            "volume": 10,
            "intraday": 12,
            "order_flow": 14,
            "chip": 16,
            "reversal": 12,
            "mean_reversion": 18,
        },
        "bonus_rules": {"defensive_rotation": 4, "smart_money_follow": 4, "distribution_penalty": -18},
    },
}


def _parse_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _get_agent_memory_service():
    from agent_memory_service import agent_memory_service

    return agent_memory_service


def _get_backend_services():
    from backend import services

    return services


def _status_label(status: str) -> str:
    return {
        STATUS_RESEARCH: "研究池",
        STATUS_FOCUS: "备选关注",
        STATUS_HOLDING: "持仓中",
    }.get(status, status)


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_json_loads(raw_value: Any, default: Any):
    if raw_value in (None, ""):
        return default
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        import json

        return json.loads(raw_value)
    except (TypeError, ValueError):
        return default


def _is_a_share_symbol(symbol: Any) -> bool:
    text = str(symbol or "").strip()
    return text.isdigit() and len(text) == 6


def _normalize_sector_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).replace("概念", "").replace("板块", "")


def _extract_tags_from_mapping(raw_value: Any) -> List[str]:
    payload = raw_value if isinstance(raw_value, dict) else _safe_json_loads(raw_value, {})
    if not isinstance(payload, dict):
        return []

    tags: List[str] = []
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
        value = payload.get(key)
        if isinstance(value, list):
            candidates = value
        elif isinstance(value, str):
            candidates = value.replace("，", ",").replace("、", ",").split(",")
        else:
            candidates = []
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text and text not in tags:
                tags.append(text)
    return tags[:16]


def _extract_name_from_mapping(raw_value: Any) -> str:
    payload = raw_value if isinstance(raw_value, dict) else _safe_json_loads(raw_value, {})
    if not isinstance(payload, dict):
        return ""
    for key in ("name", "stock_name", "股票名称", "股票简称", "证券简称", "简称"):
        text = str(payload.get(key) or "").strip()
        if text:
            return text
    return ""


def _collect_display_tags_from_sources(*sources: Any) -> List[str]:
    tags: List[str] = []
    for source in sources:
        if isinstance(source, list):
            candidates = source
        elif isinstance(source, dict):
            candidates = _extract_tags_from_mapping(source)
        else:
            candidates = []
        for candidate in candidates:
            text = str(candidate or "").strip()
            if text and text not in tags:
                tags.append(text)
    return tags[:16]


def _looks_like_primary_industry_tag(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if "-" in text or "－" in text or "—" in text:
        return True
    if "行业" in text or "产业链" in text:
        return True
    if re.search(r"(?:[ⅠⅡⅢⅣⅤ]|II|III|IV|V)(?:$|[^A-Za-z])", text):
        return True
    if any(prefix in text for prefix in ("通信", "电子", "机械设备", "电力设备", "医药生物", "食品饮料", "汽车", "计算机", "传媒", "国防军工", "基础化工", "有色金属", "家用电器", "建筑装饰", "建筑材料", "房地产", "煤炭", "石油石化", "农林牧渔", "美容护理", "轻工制造", "纺织服饰", "商贸零售", "社会服务", "交通运输", "钢铁", "环保", "银行", "非银金融", "综合")):
        return True
    return False


def _split_display_tags(tags: List[str], *, core_limit: int = 5) -> Dict[str, Any]:
    normalized: List[str] = []
    for tag in tags:
        text = str(tag or "").strip()
        if text and text not in normalized:
            normalized.append(text)

    primary_industry = ""
    primary_index = -1
    for index, tag in enumerate(normalized):
        if _looks_like_primary_industry_tag(tag):
            primary_industry = tag
            primary_index = index
            break

    if not primary_industry and normalized:
        primary_industry = normalized[0]
        primary_index = 0

    core_concepts: List[str] = []
    for index, tag in enumerate(normalized):
        if index == primary_index:
            continue
        if tag in core_concepts:
            continue
        core_concepts.append(tag)
        if len(core_concepts) >= core_limit:
            break

    total_non_primary = len(normalized) - (1 if primary_industry else 0)
    extra_tags_count = max(0, total_non_primary - len(core_concepts))
    display_tag_summary = []
    if primary_industry:
        display_tag_summary.append(primary_industry)
    display_tag_summary.extend(core_concepts)
    if extra_tags_count > 0:
        display_tag_summary.append(f"+{extra_tags_count}")

    return {
        "primary_industry": primary_industry,
        "core_concepts": core_concepts,
        "extra_tags_count": extra_tags_count,
        "display_tag_summary": display_tag_summary,
    }


def _is_invalid_asset_display_name(name: Any, symbol: Any) -> bool:
    text = str(name or "").strip()
    normalized_symbol = str(symbol or "").strip()
    if not text:
        return True
    invalid_names = {
        normalized_symbol,
        f"股票{normalized_symbol}",
        f"港股{normalized_symbol}",
        f"美股{normalized_symbol}",
    }
    return text in invalid_names or text.upper() == normalized_symbol.upper()


def _decode_first_json_value(text: str) -> Any:
    decoder = json.JSONDecoder()
    normalized = str(text or "").strip()
    if not normalized:
        return None

    candidates = []
    fenced_blocks = re.findall(r"```(?:json)?\s*([\s\S]*?)```", normalized, flags=re.IGNORECASE)
    candidates.extend(block.strip() for block in fenced_blocks if block.strip())
    candidates.append(normalized)

    for candidate in candidates:
        for marker in ("[", "{"):
            search_from = 0
            while True:
                index = candidate.find(marker, search_from)
                if index < 0:
                    break
                search_from = index + 1
                snippet = candidate[index:].strip()
                if not snippet:
                    continue
                try:
                    parsed, _ = decoder.raw_decode(snippet)
                except json.JSONDecodeError:
                    continue
                return parsed
    return None


def _extract_selection_sector_items(value: Any) -> List[Dict[str, Any]]:
    parsed = _decode_first_json_value(str(value or ""))
    if isinstance(parsed, dict):
        parsed = parsed.get("items") or parsed.get("sectors") or parsed.get("data") or []
    if not isinstance(parsed, list):
        return []

    items: List[Dict[str, Any]] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        sector = str(item.get("sector") or item.get("name") or "").strip()
        if not sector:
            continue
        items.append(
            {
                "sector": sector,
                "heat_score": max(0, min(100, _safe_int(item.get("heat_score") or item.get("score")))),
                "source": str(item.get("source") or "").strip(),
                "reason": str(item.get("reason") or item.get("logic") or "").strip(),
            }
        )
    return items


def _list_ai_decisions(stock_code: Optional[str], *, limit: int = 20) -> List[Dict[str, Any]]:
    if not stock_code:
        return []
    conn = connect_sqlite("investment.db")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM ai_decisions
            WHERE stock_code = ?
            ORDER BY datetime(decision_time) DESC, id DESC
            LIMIT ?
            """,
            (stock_code, int(limit)),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()

    for decision in rows:
        for key in ("key_price_levels", "monitor_levels", "decision_context", "market_data", "account_info"):
            decision[key] = _safe_json_loads(decision.get(key), {})
        decision_context = decision.get("decision_context") if isinstance(decision.get("decision_context"), dict) else {}
        decision["intraday_signal_labels"] = decision_context.get("intraday_signal_labels") or []
        decision["intraday_observations"] = decision_context.get("intraday_observations") or []
    return rows


def _extract_sector_candidates(report: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    heat = _extract_sector_heat(report)
    for key in ("hottest", "heating"):
        for item in heat.get(key) or []:
            text = str(item.get("sector") or item.get("name") or "").strip()
            if text:
                names.append(text)
    for item in report.get("recommended_sectors_parsed") or []:
        if isinstance(item, dict):
            for key in ("sector_name", "name", "sector"):
                text = str(item.get(key) or "").strip()
                if text:
                    names.append(text)
                    break
        else:
            text = str(item or "").strip()
            if text:
                names.append(text)
    summary_data = report.get("summary_data") or {}
    for key in ("bullish", "neutral"):
        for item in summary_data.get(key) or []:
            if isinstance(item, dict):
                text = str(item.get("sector") or item.get("name") or "").strip()
            else:
                text = str(item or "").strip()
            if text:
                names.append(text)
    return list(dict.fromkeys(names))[:8]


def _extract_sector_heat(report: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    parsed = report.get("analysis_content_parsed") if isinstance(report.get("analysis_content_parsed"), dict) else {}
    predictions = parsed.get("predictions") if isinstance(parsed.get("predictions"), dict) else {}
    final_predictions = parsed.get("final_predictions") if isinstance(parsed.get("final_predictions"), dict) else {}
    heat = (
        parsed.get("heat")
        if isinstance(parsed.get("heat"), dict)
        else predictions.get("heat")
        if isinstance(predictions.get("heat"), dict)
        else final_predictions.get("heat")
        if isinstance(final_predictions.get("heat"), dict)
        else {}
    )
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for key in ("hottest", "heating", "cooling"):
        items: List[Dict[str, Any]] = []
        for raw_item in heat.get(key) or []:
            if isinstance(raw_item, dict):
                sector = str(raw_item.get("sector") or raw_item.get("name") or "").strip()
                if not sector:
                    continue
                items.append(
                    {
                        "sector": sector,
                        "score": _safe_float(raw_item.get("score")),
                        "trend": str(raw_item.get("trend") or "").strip(),
                        "sustainability": str(raw_item.get("sustainability") or "").strip(),
                        "reason": str(raw_item.get("reason") or raw_item.get("logic") or "").strip(),
                    }
                )
            else:
                sector = str(raw_item or "").strip()
                if sector:
                    items.append({"sector": sector, "score": 0, "trend": "", "sustainability": "", "reason": ""})
        groups[key] = items
    return groups


def _build_sector_summary_data(report: Dict[str, Any]) -> Dict[str, Any]:
    parsed = report.get("analysis_content_parsed") if isinstance(report.get("analysis_content_parsed"), dict) else {}
    summary = report.get("summary") or parsed.get("summary") or parsed.get("market_view") or ""
    headline = str(summary or "").strip().splitlines()[0][:80] if summary else "智策板块报告"
    return {
        "headline": headline,
        "market_view": parsed.get("market_view") or summary or "暂无",
        "key_opportunity": parsed.get("key_opportunity") or "暂无",
        "major_risk": parsed.get("major_risk") or "暂无",
        "strategy": parsed.get("strategy") or "暂无",
        "bullish": parsed.get("bullish") if isinstance(parsed.get("bullish"), list) else [],
        "neutral": parsed.get("neutral") if isinstance(parsed.get("neutral"), list) else [],
        "bearish": parsed.get("bearish") if isinstance(parsed.get("bearish"), list) else [],
        "risk_level": report.get("risk_level") or parsed.get("risk_level") or "中等",
        "market_outlook": report.get("market_outlook") or parsed.get("market_outlook") or "中性",
        "confidence_score": _safe_int(report.get("confidence_score") or parsed.get("confidence_score")),
    }


def _parse_sector_report_row(row: Dict[str, Any]) -> Dict[str, Any]:
    report = dict(row)
    report["analysis_content_parsed"] = _safe_json_loads(report.get("analysis_content"), {})
    report["recommended_sectors_parsed"] = _safe_json_loads(report.get("recommended_sectors"), [])
    report["summary_data"] = _build_sector_summary_data(report)
    return report


def _get_latest_sector_strategy_report_light() -> Optional[Dict[str, Any]]:
    conn = connect_sqlite("sector_strategy.db")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'sector_analysis_reports'
            """
        )
        if cursor.fetchone() is None:
            return None
        cursor.execute(
            """
            SELECT *
            FROM sector_analysis_reports
            ORDER BY datetime(COALESCE(created_at, analysis_date)) DESC, id DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        return _parse_sector_report_row(dict(row)) if row else None
    finally:
        conn.close()


def _get_sector_strategy_report_light(report_id: int) -> Optional[Dict[str, Any]]:
    conn = connect_sqlite("sector_strategy.db")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'sector_analysis_reports'
            """
        )
        if cursor.fetchone() is None:
            return None
        cursor.execute("SELECT * FROM sector_analysis_reports WHERE id = ?", (int(report_id),))
        row = cursor.fetchone()
        return _parse_sector_report_row(dict(row)) if row else None
    finally:
        conn.close()


def get_recent_sector_strategy_report() -> Dict[str, Any]:
    latest = _get_latest_sector_strategy_report_light()
    if not latest:
        return {"available": False, "reused": False, "max_age_hours": SECTOR_REPORT_MAX_AGE_HOURS}
    report_time = _parse_dt(latest.get("analysis_date") or latest.get("created_at"))
    is_fresh = False
    if report_time is not None:
        is_fresh = datetime.now() - report_time <= timedelta(hours=SECTOR_REPORT_MAX_AGE_HOURS)
    summary_data = latest.get("summary_data") or {}
    return {
        "available": True,
        "fresh": bool(is_fresh),
        "reused": bool(is_fresh),
        "report_id": latest.get("id"),
        "analysis_date": latest.get("analysis_date") or latest.get("created_at"),
        "headline": summary_data.get("headline") or latest.get("summary") or "智策板块报告",
        "market_view": summary_data.get("market_view") or "暂无",
        "major_risk": summary_data.get("major_risk") or "暂无",
        "strategy": summary_data.get("strategy") or "暂无",
        "heat": _extract_sector_heat(latest),
        "max_age_hours": SECTOR_REPORT_MAX_AGE_HOURS,
    }


def _wait_for_sector_strategy_task(task_id: str) -> Dict[str, Any]:
    services = _get_backend_services()
    deadline = time.time() + SECTOR_WAIT_TIMEOUT_SECONDS
    while time.time() < deadline:
        task = services.get_ui_task(services.SECTOR_STRATEGY_TASK_TYPE, task_id)
        if task and task.get("status") in {"success", "failed", "cancelled"}:
            return task
        time.sleep(SECTOR_WAIT_INTERVAL_SECONDS)
    task = services.get_ui_task(services.SECTOR_STRATEGY_TASK_TYPE, task_id)
    if task and task.get("status") in {"success", "failed", "cancelled"}:
        return task
    latest_report = _get_latest_sector_strategy_report_light()
    if latest_report:
        return {
            "id": task_id,
            "status": "success",
            "message": "智策板块任务等待超时，已回退使用最近可用报告",
            "result": {
                "report_id": latest_report.get("id"),
            },
            "timeout_fallback": True,
        }
    raise TimeoutError("等待智策板块任务超时")


def ensure_recent_sector_strategy_report(
    *,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> Dict[str, Any]:
    warnings: List[str] = []
    freshness = get_recent_sector_strategy_report()
    if freshness.get("fresh"):
        report = _get_sector_strategy_report_light(int(freshness["report_id"]))
        return {
            "reused": True,
            "task_id": None,
            "report_id": freshness["report_id"],
            "report": report,
            "warnings": warnings,
        }

    services = _get_backend_services()
    task_id = services.submit_sector_strategy_task(
        lightweight_model=lightweight_model,
        reasoning_model=reasoning_model,
    )
    task = _wait_for_sector_strategy_task(task_id)
    if task.get("timeout_fallback"):
        warnings.append("智策板块任务等待超时，已回退使用最近可用报告")
    if task.get("status") != "success":
        raise RuntimeError(task.get("error") or task.get("message") or "智策板块分析失败")

    reports = services.list_sector_strategy_reports(limit=1)
    if not reports:
        raise RuntimeError("智策板块任务已完成，但未生成可用报告")
    report_id = reports[0]["id"]
    report = services.get_sector_strategy_report(int(report_id))
    return {
        "reused": False,
        "task_id": task_id,
        "report_id": report_id,
        "report": report,
        "warnings": warnings,
    }


def _build_asset_item(asset: Dict[str, Any]) -> Dict[str, Any]:
    strategy_context = analysis_repository.get_latest_strategy_context(
        asset_id=asset.get("id"),
        symbol=asset.get("symbol"),
    ) or {}
    latest_record = None
    records = analysis_repository.list_records(
        asset_id=asset.get("id"),
        symbol=asset.get("symbol"),
        limit=1,
        full_report_only=False,
    )
    if records:
        latest_record = records[0]
    latest_decisions = _list_ai_decisions(asset.get("symbol"), limit=1)
    display_tags = _collect_display_tags_from_sources(
        asset.get("sector_tags") or [],
        latest_record.get("stock_info") if isinstance(latest_record, dict) else {},
    )
    resolved_name = _derive_asset_display_name(asset)

    tag_display = _split_display_tags(display_tags)
    should_backfill_basic_info = _is_invalid_asset_display_name(asset.get("name"), asset.get("symbol")) or not display_tags or not tag_display.get("core_concepts")
    if should_backfill_basic_info:
        try:
            from data_source_manager import data_source_manager

            basic_info = data_source_manager.get_stock_basic_info(str(asset.get("symbol") or ""))
        except Exception:
            basic_info = {}

        basic_name = _extract_name_from_mapping(basic_info)
        merged_tags = _collect_display_tags_from_sources(display_tags, basic_info)

        if basic_name and not _is_invalid_asset_display_name(basic_name, asset.get("symbol")):
            resolved_name = basic_name
        if merged_tags:
            display_tags = merged_tags
            tag_display = _split_display_tags(display_tags)

        update_payload: Dict[str, Any] = {}
        if basic_name and not _is_invalid_asset_display_name(basic_name, asset.get("symbol")) and str(asset.get("name") or "").strip() != basic_name:
            update_payload["name"] = basic_name
        if display_tags != (asset.get("sector_tags") or []):
            update_payload["sector_tags_json"] = display_tags
        if update_payload and asset.get("id") is not None:
            try:
                asset_repository.update_asset(int(asset["id"]), **update_payload)
            except Exception:
                pass

    return {
        **asset,
        "name": resolved_name,
        "status_label": _status_label(str(asset.get("status") or "")),
        "strategy_context": strategy_context,
        "latest_analysis_id": (latest_record or {}).get("id") or strategy_context.get("origin_analysis_id"),
        "latest_analysis_time": (latest_record or {}).get("analysis_date") or strategy_context.get("analysis_date"),
        "latest_analysis_summary": (latest_record or {}).get("summary") or strategy_context.get("summary") or asset.get("note") or "",
        "latest_analysis_rating": (latest_record or {}).get("rating") or strategy_context.get("rating") or "",
        "latest_decision": latest_decisions[0] if latest_decisions else None,
        "display_tags": display_tags,
        **tag_display,
    }


def list_hub_assets(*, pool: Optional[str] = None, search_term: str = "") -> List[Dict[str, Any]]:
    if pool == STATUS_RESEARCH:
        statuses = [STATUS_HOLDING, STATUS_FOCUS, STATUS_RESEARCH]
    elif pool in {STATUS_FOCUS, STATUS_HOLDING}:
        statuses = [pool]
    else:
        statuses = [STATUS_HOLDING, STATUS_FOCUS, STATUS_RESEARCH]
    items: List[Dict[str, Any]] = []
    search = str(search_term or "").strip().lower()
    for status in statuses:
        assets = asset_repository.list_assets(status=status, include_deleted=False)
        for asset in assets:
            item = _build_asset_item(asset)
            if search:
                haystack = " ".join(
                    [
                        str(item.get("symbol") or ""),
                        str(item.get("name") or ""),
                        str(item.get("latest_analysis_summary") or ""),
                        str(item.get("pool_reason") or ""),
                        " ".join(str(tag or "") for tag in item.get("display_tags") or []),
                    ]
                ).lower()
                if search not in haystack:
                    continue
            items.append(item)
    items.sort(
        key=lambda item: (
            1 if item.get("manual_pin") else 0,
            2 if item.get("status") == STATUS_HOLDING else 1 if item.get("status") == STATUS_FOCUS else 0,
            (_parse_dt(item.get("latest_analysis_time")) or datetime.min) if item.get("status") == STATUS_RESEARCH else datetime.min,
            _safe_float(item.get("last_funnel_score")),
            str(item.get("updated_at") or ""),
            int(item.get("id") or 0),
        ),
        reverse=True,
    )
    return items


def get_hub_overview() -> Dict[str, Any]:
    assets = list_hub_assets()
    grouped = {
        STATUS_HOLDING: [item for item in assets if item.get("status") == STATUS_HOLDING],
        STATUS_FOCUS: [item for item in assets if item.get("status") == STATUS_FOCUS],
        STATUS_RESEARCH: [item for item in assets if item.get("status") == STATUS_RESEARCH],
    }
    return {
        "counts": {
            "holding": len(grouped[STATUS_HOLDING]),
            "focus": len(grouped[STATUS_FOCUS]),
            "research": len(grouped[STATUS_RESEARCH]),
            "manual_pin": sum(1 for item in assets if item.get("manual_pin")),
        },
        "focus_capacity": FOCUS_CAPACITY,
        "top_focus": grouped[STATUS_FOCUS][:FOCUS_CAPACITY],
        "holdings": grouped[STATUS_HOLDING],
        "sector_report": get_recent_sector_strategy_report(),
    }


def get_hub_asset_detail(asset_id: int) -> Optional[Dict[str, Any]]:
    asset = asset_repository.get_asset(int(asset_id))
    if not asset:
        return None
    item = _build_asset_item(asset)
    latest_record = analysis_repository.get_latest_strategy_context(asset_id=asset_id, symbol=asset.get("symbol")) or {}
    latest_decisions = _list_ai_decisions(asset.get("symbol"), limit=1)
    item["memory_summary"] = _get_agent_memory_service().db.get_memory_summary(asset.get("symbol"))
    item["recent_trades"] = asset_repository.get_trade_history(asset_id, limit=10)
    item["pending_actions"] = asset_repository.list_pending_actions(asset_id=asset_id, limit=10)
    item["latest_strategy_context"] = latest_record
    item["latest_decision"] = latest_decisions[0] if latest_decisions else None
    return item


def get_hub_asset_timeline(asset_id: int) -> List[Dict[str, Any]]:
    asset = asset_repository.get_asset(int(asset_id))
    if not asset:
        return []
    symbol = asset.get("symbol")
    events: List[Dict[str, Any]] = []
    for record in analysis_repository.list_record_summaries(symbol=symbol, asset_id=asset_id, limit=20):
        events.append(
            {
                "event_type": "analysis_added",
                "title": "入研究池",
                "time": record.get("analysis_date"),
                "summary": record.get("summary") or record.get("rating") or "已完成分析",
                "payload": {
                    "record_id": record.get("id"),
                    "rating": record.get("rating"),
                    "analysis_scope": record.get("analysis_scope"),
                },
            }
        )
    for trade in asset_repository.get_trade_history(asset_id, limit=20):
        trade_type = str(trade.get("trade_type") or "").lower()
        event_type = "position_opened" if trade_type == "buy" else "position_closed_to_research" if trade_type == "sell" else "position_adjusted"
        title = "建仓与调仓" if trade_type == "buy" else "清仓回研究池" if not asset.get("quantity") else "建仓与调仓"
        events.append(
            {
                "event_type": event_type,
                "title": title,
                "time": trade.get("trade_date"),
                "summary": f"{'买入' if trade_type == 'buy' else '卖出'} {trade.get('quantity')} 股 @ {trade.get('price')}",
                "payload": trade,
            }
        )
    for action in asset_repository.list_pending_actions(asset_id=asset_id, limit=20):
        events.append(
            {
                "event_type": "position_adjusted",
                "title": "待办动作",
                "time": action.get("created_at"),
                "summary": f"{action.get('action_type')} | {action.get('status')}",
                "payload": action,
            }
        )
    decisions = _list_ai_decisions(symbol, limit=20)
    for decision in decisions:
        action = str(decision.get("action") or "").upper()
        title = "清仓回研究池" if action == "SELL" else "晋级备选" if action == "BUY" else "建仓与调仓"
        events.append(
            {
                "event_type": "focus_promoted" if action == "BUY" else "position_closed_to_research" if action == "SELL" else "position_adjusted",
                "title": title,
                "time": decision.get("decision_time"),
                "summary": decision.get("reasoning") or decision.get("action_detail") or action,
                "payload": {
                    "decision_id": decision.get("id"),
                    "action": action,
                    "risk_level": decision.get("risk_level"),
                },
            }
        )
    events.sort(key=lambda item: str(item.get("time") or ""), reverse=True)
    return events


def update_hub_asset(
    asset_id: int,
    *,
    target_status: Optional[str] = None,
    manual_pin: Optional[bool] = None,
    note: Optional[str] = None,
    pool_reason: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    asset = asset_repository.get_asset(int(asset_id))
    if not asset:
        return None
    updates: Dict[str, Any] = {}
    if manual_pin is not None:
        updates["manual_pin"] = bool(manual_pin)
    if note is not None:
        updates["note"] = note
    if pool_reason is not None:
        updates["pool_reason"] = pool_reason
        updates["pool_reason_source"] = "manual"
    if updates:
        asset_repository.update_asset(asset_id, **updates)
    if target_status and target_status != asset.get("status"):
        if target_status == STATUS_FOCUS:
            asset_service.promote_to_watchlist(
                symbol=asset.get("symbol"),
                stock_name=asset.get("name") or asset.get("symbol"),
                note=pool_reason or note or "手动晋级到备选关注",
                origin_analysis_id=asset.get("origin_analysis_id"),
                monitor_enabled=bool(asset.get("monitor_enabled", True)),
            )
            _get_agent_memory_service().db.save_working_memory(
                stock_code=asset.get("symbol"),
                analysis_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                decision_summary=f"晋级备选关注 | {(pool_reason or note or '手动操作')[:180]}",
                strategy={"status": STATUS_FOCUS},
            )
        elif target_status == STATUS_RESEARCH:
            asset_service.remove_from_watchlist(
                asset_id,
                note=pool_reason or note or "手动移回研究池",
            )
        elif target_status == STATUS_HOLDING:
            raise ValueError("持仓状态只能通过交易记录变更")
    return get_hub_asset_detail(asset_id)


def delete_hub_asset(asset_id: int) -> bool:
    asset = asset_repository.get_asset(int(asset_id))
    if not asset:
        return False
    if str(asset.get("status") or "").strip().lower() != STATUS_RESEARCH:
        raise ValueError("仅支持删除研究池卡片")

    asset_service.sync_managed_monitors(asset_id)
    return asset_repository.soft_delete_asset(asset_id)


def quick_analyze_and_add_to_research(symbol: str) -> Dict[str, Any]:
    from batch_analysis_service import analyze_single_stock_for_batch

    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("请输入有效股票代码")
    result = analyze_single_stock_for_batch(
        symbol=normalized_symbol,
        period="1y",
        save_to_global_history=True,
    )
    if not result.get("success"):
        raise RuntimeError(result.get("error") or f"{normalized_symbol} 分析失败")
    record_id = result.get("record_id")
    asset = asset_repository.get_asset_by_symbol(normalized_symbol)
    return {
        "symbol": normalized_symbol,
        "record_id": record_id,
        "asset_id": asset.get("id") if asset else None,
        "stock_info": result.get("stock_info"),
        "final_decision": result.get("final_decision"),
    }


def _resolve_sector_report_view(report: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(report.get("report_view"), dict):
        return report["report_view"]
    try:
        from sector_strategy_normalization import normalize_sector_strategy_result

        return normalize_sector_strategy_result(report or {})
    except Exception:
        return {}


def _upsert_sector_item(store: Dict[str, Dict[str, Any]], item: Dict[str, Any]) -> None:
    sector = str(item.get("sector") or "").strip()
    if not sector:
        return
    normalized = _normalize_sector_text(sector)
    if not normalized:
        return

    payload = {
        "sector": sector,
        "heat_score": max(0, min(100, _safe_int(item.get("heat_score") or item.get("score")))),
        "source": str(item.get("source") or "").strip(),
        "reason": str(item.get("reason") or item.get("logic") or item.get("trend") or "").strip(),
    }
    existing = store.get(normalized)
    if existing is None or payload["heat_score"] > existing.get("heat_score", 0):
        store[normalized] = payload


def _extract_structured_selection_sectors(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    report_view = _resolve_sector_report_view(report)
    predictions = report_view.get("predictions") if isinstance(report_view.get("predictions"), dict) else {}
    long_short = predictions.get("long_short") if isinstance(predictions.get("long_short"), dict) else {}
    rotation = predictions.get("rotation") if isinstance(predictions.get("rotation"), dict) else {}
    heat = predictions.get("heat") if isinstance(predictions.get("heat"), dict) else _extract_sector_heat(report)

    store: Dict[str, Dict[str, Any]] = {}
    for group_name, default_score in (("hottest", 90), ("heating", 78)):
        for item in heat.get(group_name) or []:
            _upsert_sector_item(
                store,
                {
                    "sector": item.get("sector"),
                    "heat_score": _safe_int(item.get("score")) or default_score,
                    "source": f"heat.{group_name}",
                    "reason": item.get("reason") or item.get("trend") or item.get("sustainability"),
                },
            )

    for item in long_short.get("bullish") or []:
        confidence_value = _safe_float(item.get("confidence"))
        if confidence_value <= 10:
            confidence_value *= 10
        _upsert_sector_item(
            store,
            {
                "sector": item.get("sector"),
                "heat_score": max(60, min(95, _safe_int(confidence_value))),
                "source": "long_short.bullish",
                "reason": item.get("reason"),
            },
        )

    for item in rotation.get("potential") or []:
        _upsert_sector_item(
            store,
            {
                "sector": item.get("sector"),
                "heat_score": 68,
                "source": "rotation.potential",
                "reason": item.get("logic") or item.get("advice"),
            },
        )

    structured = sorted(store.values(), key=lambda item: item["heat_score"], reverse=True)
    return structured[:5]


def _extract_selection_sectors_with_llm(
    report: Dict[str, Any],
    *,
    client: Optional[DeepSeekClient] = None,
) -> List[Dict[str, Any]]:
    summary_data = report.get("summary_data") or {}
    structured_heat = _extract_sector_heat(report)
    recommended = report.get("recommended_sectors_parsed") or []
    report_excerpt = str(report.get("summary") or report.get("analysis_content") or "")[:3000]
    messages = build_messages(
        "research_hub/selection_extract.system.txt",
        "research_hub/selection_extract.user.txt",
        analysis_date=report.get("analysis_date") or report.get("created_at") or "",
        headline=summary_data.get("headline") or "智策板块报告",
        market_view=summary_data.get("market_view") or "",
        key_opportunity=summary_data.get("key_opportunity") or "",
        major_risk=summary_data.get("major_risk") or "",
        strategy=summary_data.get("strategy") or "",
        heat_payload=json.dumps(structured_heat, ensure_ascii=False, indent=2),
        recommended_payload=json.dumps(recommended, ensure_ascii=False, indent=2),
        report_excerpt=report_excerpt,
    )
    llm_client = client or DeepSeekClient()
    response = llm_client.call_api(
        messages,
        max_tokens=1200,
        tier=ModelTier.LIGHTWEIGHT,
    )
    return _extract_selection_sector_items(response)


def _review_selection_candidates_with_llm(
    *,
    lightweight_model: Optional[str],
    reasoning_model: Optional[str],
    extracted_sectors: List[Dict[str, Any]],
    ranked_top15: List[Dict[str, Any]],
    final_selected: List[Dict[str, Any]],
    kept_manual_pins: List[Dict[str, Any]],
    excluded_by_risk: List[Dict[str, Any]],
    excluded_by_dedup: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not ranked_top15 or not final_selected:
        return {}

    client = DeepSeekClient(
        lightweight_model=lightweight_model,
        reasoning_model=reasoning_model,
    )

    messages = build_messages(
        "research_hub/selection_review.system.txt",
        "research_hub/selection_review.user.txt",
        extracted_payload=json.dumps(extracted_sectors, ensure_ascii=False, indent=2),
        ranked_payload=json.dumps(ranked_top15, ensure_ascii=False, indent=2),
        final_payload=json.dumps(final_selected, ensure_ascii=False, indent=2),
        manual_payload=json.dumps(kept_manual_pins, ensure_ascii=False, indent=2),
        risk_payload=json.dumps(excluded_by_risk, ensure_ascii=False, indent=2),
        dedup_payload=json.dumps(excluded_by_dedup, ensure_ascii=False, indent=2),
    )
    response = client.call_api(
        messages,
        max_tokens=1200,
        tier=ModelTier.REASONING,
    )
    parsed = _decode_first_json_value(response)
    if isinstance(parsed, dict):
        return parsed
    return {}


def _apply_selection_review_order(
    selected_auto: List[Dict[str, Any]],
    review_payload: Dict[str, Any],
    warnings: List[str],
) -> List[Dict[str, Any]]:
    if not selected_auto or not review_payload:
        return selected_auto

    ordered_symbols_raw = review_payload.get("ordered_symbols")
    if not isinstance(ordered_symbols_raw, list):
        ordered_symbols_raw = review_payload.get("symbols") if isinstance(review_payload.get("symbols"), list) else []

    ordered_symbols: List[str] = []
    for raw_symbol in ordered_symbols_raw:
        symbol = str(raw_symbol or "").strip()
        if symbol and symbol not in ordered_symbols:
            ordered_symbols.append(symbol)

    if not ordered_symbols:
        return selected_auto

    selected_lookup = {item["symbol"]: item for item in selected_auto}
    if set(ordered_symbols) != set(selected_lookup):
        warnings.append("推理模型复核结果包含未入选标的，已忽略其排序建议")
        return selected_auto

    ordered_items = [selected_lookup[symbol] for symbol in ordered_symbols if symbol in selected_lookup]
    if len(ordered_items) != len(selected_auto):
        warnings.append("推理模型复核结果未覆盖全部自动入选标的，已保留规则排序")
        return selected_auto
    return ordered_items


def _extract_selection_sectors(
    report: Dict[str, Any],
    warnings: List[str],
    *,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> List[Dict[str, Any]]:
    structured = _extract_structured_selection_sectors(report)
    positive_count = sum(1 for item in structured if item.get("heat_score", 0) > 0)
    if len(structured) >= 3 and positive_count >= 3:
        return structured[:5]

    try:
        fallback_client = DeepSeekClient(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        fallback = _extract_selection_sectors_with_llm(report, client=fallback_client)
    except Exception as exc:
        warnings.append(f"主题抽取智能体降级: {exc}")
        fallback = []

    store: Dict[str, Dict[str, Any]] = {}
    for item in structured:
        _upsert_sector_item(store, item)
    for item in fallback:
        _upsert_sector_item(store, item)
    if not store:
        warnings.append("未能从智策板块报告提取到有效热点板块")
    return sorted(store.values(), key=lambda item: item["heat_score"], reverse=True)[:5]


def _get_latest_analysis_record(asset: Dict[str, Any]) -> Dict[str, Any]:
    records = analysis_repository.list_records(
        symbol=asset.get("symbol"),
        asset_id=asset.get("id"),
        limit=1,
        full_report_only=False,
    )
    return records[0] if records else {}


def _maybe_backfill_asset_tags(asset: Dict[str, Any], warnings: List[str]) -> List[str]:
    try:
        from data_source_manager import data_source_manager

        basic_info = data_source_manager.get_stock_basic_info(asset.get("symbol"))
    except Exception as exc:
        warnings.append(f"{asset.get('symbol')} 标签补抓失败: {exc}")
        return []

    tags = _extract_tags_from_mapping(basic_info)
    if tags:
        try:
            asset_repository.update_asset(int(asset["id"]), sector_tags_json=tags)
        except Exception:
            pass
    return tags


def _collect_asset_match_context(asset: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
    latest_record = _get_latest_analysis_record(asset)
    strategy_context = analysis_repository.get_latest_strategy_context(
        asset_id=asset.get("id"),
        symbol=asset.get("symbol"),
    ) or {}
    tags: List[str] = []
    for tag in asset.get("sector_tags") or []:
        text = str(tag or "").strip()
        if text and text not in tags:
            tags.append(text)
    for tag in _extract_tags_from_mapping(latest_record.get("stock_info")):
        if tag not in tags:
            tags.append(tag)
    if not tags:
        for tag in _maybe_backfill_asset_tags(asset, warnings):
            if tag not in tags:
                tags.append(tag)
    texts = [
        str(asset.get("name") or ""),
        str(asset.get("note") or ""),
        str(asset.get("pool_reason") or ""),
        str(strategy_context.get("summary") or ""),
        str(latest_record.get("summary") or ""),
    ]
    return {
        "latest_record": latest_record,
        "strategy_context": strategy_context,
        "tags": tags,
        "haystack": " ".join(texts),
    }


def _derive_asset_primary_sector(asset: Dict[str, Any]) -> str:
    tags = asset.get("sector_tags") or []
    for tag in tags:
        text = str(tag or "").strip()
        if text:
            return text
    return ""


def _derive_asset_display_name(asset: Dict[str, Any]) -> str:
    symbol = str(asset.get("symbol") or "").strip()
    latest_record = _get_latest_analysis_record(asset)
    stock_info = latest_record.get("stock_info_json")
    if isinstance(stock_info, str):
        try:
            stock_info = json.loads(stock_info)
        except Exception:
            stock_info = {}

    candidates = [
        asset.get("name"),
        latest_record.get("stock_name"),
        (stock_info or {}).get("name") if isinstance(stock_info, dict) else None,
        (stock_info or {}).get("股票名称") if isinstance(stock_info, dict) else None,
        (stock_info or {}).get("stock_name") if isinstance(stock_info, dict) else None,
    ]
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        if _is_invalid_asset_display_name(text, symbol):
            continue
        return text
    return symbol


def _match_asset_to_themes(
    asset: Dict[str, Any],
    context: Dict[str, Any],
    extracted_sectors: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    haystack = _normalize_sector_text(context.get("haystack"))
    normalized_tags = [_normalize_sector_text(tag) for tag in context.get("tags") or []]
    for sector_item in extracted_sectors:
        normalized_sector = _normalize_sector_text(sector_item.get("sector"))
        if not normalized_sector:
            continue
        matched = any(
            normalized_sector == tag
            or normalized_sector in tag
            or tag in normalized_sector
            for tag in normalized_tags
            if tag
        )
        if not matched and normalized_sector:
            matched = normalized_sector in haystack
        if matched:
            matches.append(sector_item)
    return matches


def _build_stock_data_bundle(symbol: str) -> Dict[str, Any]:
    try:
        import pandas as pd
        from stock_data import StockDataFetcher
    except ImportError as exc:
        return {"error": str(exc)}

    fetcher = StockDataFetcher()
    stock_info = fetcher.get_stock_info(symbol)
    realtime_quote = fetcher.get_realtime_quote(symbol)
    stock_data = fetcher.get_stock_data(symbol, period="6mo")
    if not isinstance(stock_data, pd.DataFrame) or stock_data.empty:
        return {"error": "量价数据缺失"}
    stock_data = fetcher.calculate_technical_indicators(stock_data)
    if isinstance(stock_data, dict) and stock_data.get("error"):
        return {"error": stock_data.get("error")}
    indicators = fetcher.get_latest_indicators(stock_data, symbol=symbol)
    if isinstance(indicators, dict) and indicators.get("error"):
        return {"error": indicators.get("error")}
    stock_info = stock_info if isinstance(stock_info, dict) else {}
    realtime_quote = realtime_quote if isinstance(realtime_quote, dict) else {}
    merged_stock_info = dict(stock_info)
    for field in ("turnover_rate", "volume_ratio", "current_price", "price", "market_cap"):
        if realtime_quote.get(field) not in (None, ""):
            merged_stock_info[field] = realtime_quote.get(field)
    if realtime_quote.get("price") not in (None, "") and merged_stock_info.get("current_price") in (None, ""):
        merged_stock_info["current_price"] = realtime_quote.get("price")
    return {
        "fetcher": fetcher,
        "stock_info": merged_stock_info,
        "realtime_quote": realtime_quote,
        "order_book": realtime_quote.get("order_book"),
        "indicators": indicators if isinstance(indicators, dict) else {},
        "stock_data": stock_data,
    }


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _safe_round(value: Optional[float], digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def _ratio_to_score(value: Optional[float], *, low: float, high: float) -> float:
    if value is None or high <= low:
        return 50.0
    return _clamp((float(value) - low) / (high - low) * 100.0)


def _extract_selection_market_overview(report: Dict[str, Any]) -> Dict[str, Any]:
    parsed = report.get("analysis_content_parsed") if isinstance(report.get("analysis_content_parsed"), dict) else {}
    data_summary = parsed.get("data_summary") if isinstance(parsed.get("data_summary"), dict) else {}
    overview = data_summary.get("market_overview") if isinstance(data_summary.get("market_overview"), dict) else {}
    return overview if isinstance(overview, dict) else {}


def _build_selection_market_context(report: Dict[str, Any]) -> Dict[str, Any]:
    overview = _extract_selection_market_overview(report)
    summary_data = report.get("summary_data") if isinstance(report.get("summary_data"), dict) else {}
    index_changes = [
        _optional_float(((overview.get(index_key) or {}).get("change_pct")))
        for index_key in ("sh_index", "sz_index", "cyb_index")
    ]
    valid_index_changes = [item for item in index_changes if item is not None]
    avg_index_change = sum(valid_index_changes) / len(valid_index_changes) if valid_index_changes else 0.0
    up_ratio = _optional_float(overview.get("up_ratio")) or 50.0
    limit_up = _safe_int(overview.get("limit_up"))
    limit_down = _safe_int(overview.get("limit_down"))
    market_outlook = str(summary_data.get("market_outlook") or report.get("market_outlook") or "").strip()
    market_view = str(summary_data.get("market_view") or "").strip()

    if up_ratio >= 67 and limit_up >= 70 and avg_index_change >= 1.0:
        state = SELECTION_MARKET_STATE_MOMO
    elif up_ratio <= 38 and avg_index_change <= -0.8 and limit_down >= max(10, limit_up):
        state = SELECTION_MARKET_STATE_ICE
    elif (
        (up_ratio < 50 and limit_down >= 12 and avg_index_change <= -0.3)
        or any(text in f"{market_outlook} {market_view}" for text in ("退潮", "谨慎", "风险", "分歧"))
    ):
        state = SELECTION_MARKET_STATE_RETREAT
    else:
        state = SELECTION_MARKET_STATE_RANGE

    profile = SELECTION_AGENT_PROFILES.get(state, SELECTION_AGENT_PROFILES[SELECTION_MARKET_STATE_RANGE])
    signals: List[str] = []
    if valid_index_changes:
        signals.append(f"指数均值 {avg_index_change:+.2f}%")
    signals.append(f"上涨占比 {up_ratio:.1f}%")
    signals.append(f"涨跌停 {limit_up}/{limit_down}")
    return {
        "state": state,
        "state_label": SELECTION_MARKET_STATE_LABELS.get(state, state),
        "profile": profile,
        "market_outlook": market_outlook or "中性",
        "market_view": market_view or "暂无",
        "market_overview": overview,
        "avg_index_change": round(avg_index_change, 2),
        "up_ratio": round(up_ratio, 2),
        "limit_up": limit_up,
        "limit_down": limit_down,
        "signals": signals,
    }


def _get_selection_smart_monitor_fetcher():
    if hasattr(_get_selection_smart_monitor_fetcher, "_resolved"):
        cached = getattr(_get_selection_smart_monitor_fetcher, "_fetcher", None)
        return cached if cached is not False else None
    try:
        from smart_monitor_data import SmartMonitorDataFetcher

        fetcher = SmartMonitorDataFetcher()
    except Exception:
        fetcher = False
    setattr(_get_selection_smart_monitor_fetcher, "_resolved", True)
    setattr(_get_selection_smart_monitor_fetcher, "_fetcher", fetcher)
    return fetcher if fetcher is not False else None


def _get_selection_intraday_context(symbol: str) -> Dict[str, Any]:
    fetcher = _get_selection_smart_monitor_fetcher()
    if fetcher is None or getattr(fetcher, "tdx_fetcher", None) is None:
        return {}
    try:
        payload = fetcher.tdx_fetcher.get_intraday_context(symbol)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _get_selection_realtime_freshness(
    intraday_context: Dict[str, Any],
    *,
    update_time: Any = None,
) -> Dict[str, Any]:
    fetcher = _get_selection_smart_monitor_fetcher()
    if fetcher is None or not hasattr(fetcher, "_build_realtime_freshness"):
        return {}
    try:
        payload = {
            "intraday_context": intraday_context if isinstance(intraday_context, dict) else {},
            "update_time": update_time,
        }
        freshness = fetcher._build_realtime_freshness(payload)
    except Exception:
        return {}
    return freshness if isinstance(freshness, dict) else {}


def _should_fetch_main_force_flow(volume_ratio: Optional[float], turnover_rate: Optional[float]) -> bool:
    return (volume_ratio or 0.0) >= 1.0 or (turnover_rate or 0.0) >= 2.0


def _get_selection_main_force_flow(symbol: str, *, volume_ratio: Optional[float], turnover_rate: Optional[float]) -> Dict[str, Any]:
    if not _should_fetch_main_force_flow(volume_ratio, turnover_rate):
        return {}
    fetcher = _get_selection_smart_monitor_fetcher()
    if fetcher is None or not getattr(fetcher, "ts_pro", None):
        return {}
    try:
        payload = fetcher.get_main_force_flow(symbol)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_order_book_features(order_book: Any) -> Dict[str, Optional[float]]:
    if not isinstance(order_book, dict):
        return {"imbalance": None, "spread_bps": None}
    bids = order_book.get("bids") if isinstance(order_book.get("bids"), list) else []
    asks = order_book.get("asks") if isinstance(order_book.get("asks"), list) else []
    bid_volume = sum(_optional_float(item.get("volume")) or 0.0 for item in bids[:3] if isinstance(item, dict))
    ask_volume = sum(_optional_float(item.get("volume")) or 0.0 for item in asks[:3] if isinstance(item, dict))
    top_bid = _optional_float((bids[0] if bids and isinstance(bids[0], dict) else {}).get("price"))
    top_ask = _optional_float((asks[0] if asks and isinstance(asks[0], dict) else {}).get("price"))
    imbalance = None
    if bid_volume > 0 or ask_volume > 0:
        imbalance = (bid_volume - ask_volume) / max(bid_volume + ask_volume, 1.0)
    spread_bps = None
    if top_bid and top_ask and top_bid > 0 and top_ask >= top_bid:
        spread_bps = (top_ask - top_bid) / top_bid * 10000
    return {"imbalance": imbalance, "spread_bps": spread_bps}


def _extract_selection_agent_features(
    *,
    bundle: Dict[str, Any],
    market_context: Dict[str, Any],
    intraday_context: Dict[str, Any],
    main_force_flow: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        import pandas as pd
    except ImportError:
        pd = None

    stock_info = bundle.get("stock_info") or {}
    indicators = bundle.get("indicators") or {}
    stock_data = bundle.get("stock_data")
    realtime_quote = bundle.get("realtime_quote") or {}

    latest_row = {}
    prev_row = {}
    if pd is not None and hasattr(stock_data, "empty") and not stock_data.empty:
        latest_row = stock_data.iloc[-1].to_dict()
        if len(stock_data) > 1:
            prev_row = stock_data.iloc[-2].to_dict()

    close = _optional_float(indicators.get("price") or latest_row.get("Close") or stock_info.get("current_price") or stock_info.get("price")) or 0.0
    open_price = _optional_float(latest_row.get("Open") or realtime_quote.get("open"))
    high_price = _optional_float(latest_row.get("High") or realtime_quote.get("high"))
    low_price = _optional_float(latest_row.get("Low") or realtime_quote.get("low"))
    prev_close = _optional_float(prev_row.get("Close") or realtime_quote.get("pre_close"))
    ma5 = _optional_float(indicators.get("ma5"))
    ma20 = _optional_float(indicators.get("ma20"))
    ma60 = _optional_float(indicators.get("ma60"))
    rsi = _optional_float(indicators.get("rsi"))
    macd = _optional_float(indicators.get("macd"))
    macd_signal = _optional_float(indicators.get("macd_signal"))
    volume_ratio = _optional_float(indicators.get("volume_ratio") or stock_info.get("volume_ratio"))
    turnover_rate = _optional_float(stock_info.get("turnover_rate"))

    breakout_reference = None
    position_reference_low = None
    position_reference_high = None
    ma_density_pct = None
    recent_return_5d = None
    recent_return_20d = None
    volume_contraction_days = 0
    if pd is not None and hasattr(stock_data, "empty") and not stock_data.empty and len(stock_data) >= 5:
        recent_high_20 = stock_data["High"].shift(1).tail(20).max()
        recent_low_60 = stock_data["Low"].tail(60).min()
        recent_high_60 = stock_data["High"].tail(60).max()
        breakout_reference = _optional_float(recent_high_20)
        position_reference_low = _optional_float(recent_low_60)
        position_reference_high = _optional_float(recent_high_60)
        if len(stock_data) >= 6:
            close_5 = _optional_float(stock_data["Close"].iloc[-6])
            if close_5 not in (None, 0):
                recent_return_5d = (close - close_5) / close_5 * 100
        if len(stock_data) >= 21:
            close_20 = _optional_float(stock_data["Close"].iloc[-21])
            if close_20 not in (None, 0):
                recent_return_20d = (close - close_20) / close_20 * 100
        latest_volume_series = stock_data[["Volume", "Volume_MA5"]].tail(8).iloc[::-1]
        for _, row in latest_volume_series.iterrows():
            current_volume = _optional_float(row.get("Volume"))
            volume_ma5 = _optional_float(row.get("Volume_MA5"))
            if current_volume is None or volume_ma5 in (None, 0):
                break
            if current_volume <= volume_ma5 * 0.85:
                volume_contraction_days += 1
            else:
                break
    if close > 0 and ma5 and ma20 and ma60:
        ma_density_pct = (max(ma5, ma20, ma60) - min(ma5, ma20, ma60)) / close * 100

    breakout_pct = None
    if close > 0 and breakout_reference and breakout_reference > 0:
        breakout_pct = (close - breakout_reference) / breakout_reference * 100
    position_pct_60d = None
    if close > 0 and position_reference_low is not None and position_reference_high is not None and position_reference_high > position_reference_low:
        position_pct_60d = (close - position_reference_low) / (position_reference_high - position_reference_low) * 100

    bias_pct = None
    if close > 0 and ma20 and ma20 > 0:
        bias_pct = (close - ma20) / ma20 * 100

    upper_shadow_pct = None
    lower_shadow_pct = None
    body_pct = None
    if close > 0 and open_price is not None and high_price is not None and low_price is not None:
        upper_shadow_pct = max(0.0, high_price - max(open_price, close)) / close * 100
        lower_shadow_pct = max(0.0, min(open_price, close) - low_price) / close * 100
        if open_price != 0:
            body_pct = abs(close - open_price) / open_price * 100

    chip_peak_price = _optional_float(indicators.get("main_chip_peak_price"))
    avg_chip_cost = _optional_float(indicators.get("average_chip_cost"))
    chip_concentration_text = str(indicators.get("chip_concentration") or "")

    order_book_features = _extract_order_book_features(bundle.get("order_book"))
    intraday_bias = str(intraday_context.get("intraday_bias") or "").strip()
    intraday_bias_text = str(intraday_context.get("intraday_bias_text") or "").strip()
    last_15m_change_pct = _optional_float(intraday_context.get("last_15m_change_pct"))
    last_30m_change_pct = _optional_float(intraday_context.get("last_30m_change_pct"))
    last_60m_change_pct = _optional_float(intraday_context.get("last_60m_change_pct"))
    price_position_pct = _optional_float(intraday_context.get("price_position_pct"))
    volume_acceleration_ratio = _optional_float(intraday_context.get("volume_acceleration_ratio"))
    intraday_vwap = _optional_float(intraday_context.get("intraday_vwap"))
    latest_minute_time = str(intraday_context.get("latest_minute_time") or "")
    tail_session = bool(latest_minute_time and latest_minute_time >= "14:30")

    main_net_pct = _optional_float(main_force_flow.get("main_net_pct"))
    main_net = _optional_float(main_force_flow.get("main_net"))
    super_net = _optional_float(main_force_flow.get("super_net"))
    big_net = _optional_float(main_force_flow.get("big_net"))

    return {
        "close": close,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "prev_close": prev_close,
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "rsi": rsi,
        "macd": macd,
        "macd_signal": macd_signal,
        "volume_ratio": volume_ratio,
        "turnover_rate": turnover_rate,
        "breakout_pct": breakout_pct,
        "position_pct_60d": position_pct_60d,
        "bias_pct": bias_pct,
        "ma_density_pct": ma_density_pct,
        "recent_return_5d": recent_return_5d,
        "recent_return_20d": recent_return_20d,
        "volume_contraction_days": volume_contraction_days,
        "upper_shadow_pct": upper_shadow_pct,
        "lower_shadow_pct": lower_shadow_pct,
        "body_pct": body_pct,
        "chip_peak_price": chip_peak_price,
        "avg_chip_cost": avg_chip_cost,
        "chip_concentration_text": chip_concentration_text,
        "order_book_imbalance": order_book_features.get("imbalance"),
        "order_book_spread_bps": order_book_features.get("spread_bps"),
        "intraday_bias": intraday_bias,
        "intraday_bias_text": intraday_bias_text,
        "last_15m_change_pct": last_15m_change_pct,
        "last_30m_change_pct": last_30m_change_pct,
        "last_60m_change_pct": last_60m_change_pct,
        "price_position_pct": price_position_pct,
        "volume_acceleration_ratio": volume_acceleration_ratio,
        "intraday_vwap": intraday_vwap,
        "tail_session": tail_session,
        "main_net_pct": main_net_pct,
        "main_net": main_net,
        "super_net": super_net,
        "big_net": big_net,
        "market_state": market_context.get("state"),
        "market_state_label": market_context.get("state_label"),
    }


def _compute_selection_component_scores(features: Dict[str, Any]) -> Dict[str, float]:
    close = _optional_float(features.get("close")) or 0.0
    ma5 = _optional_float(features.get("ma5"))
    ma20 = _optional_float(features.get("ma20"))
    ma60 = _optional_float(features.get("ma60"))
    rsi = _optional_float(features.get("rsi"))
    macd = _optional_float(features.get("macd"))
    macd_signal = _optional_float(features.get("macd_signal"))
    volume_ratio = _optional_float(features.get("volume_ratio"))
    turnover_rate = _optional_float(features.get("turnover_rate"))
    breakout_pct = _optional_float(features.get("breakout_pct"))
    position_pct_60d = _optional_float(features.get("position_pct_60d"))
    bias_pct = _optional_float(features.get("bias_pct"))
    ma_density_pct = _optional_float(features.get("ma_density_pct"))
    recent_return_5d = _optional_float(features.get("recent_return_5d"))
    recent_return_20d = _optional_float(features.get("recent_return_20d"))
    volume_contraction_days = _safe_int(features.get("volume_contraction_days"))
    upper_shadow_pct = _optional_float(features.get("upper_shadow_pct"))
    lower_shadow_pct = _optional_float(features.get("lower_shadow_pct"))
    price_position_pct = _optional_float(features.get("price_position_pct"))
    last_30m_change_pct = _optional_float(features.get("last_30m_change_pct"))
    last_60m_change_pct = _optional_float(features.get("last_60m_change_pct"))
    volume_acceleration_ratio = _optional_float(features.get("volume_acceleration_ratio"))
    intraday_vwap = _optional_float(features.get("intraday_vwap"))
    main_net_pct = _optional_float(features.get("main_net_pct"))
    order_book_imbalance = _optional_float(features.get("order_book_imbalance"))
    spread_bps = _optional_float(features.get("order_book_spread_bps"))
    chip_peak_price = _optional_float(features.get("chip_peak_price"))
    avg_chip_cost = _optional_float(features.get("avg_chip_cost"))
    intraday_bias = str(features.get("intraday_bias") or "")

    trend = 38.0
    if close > 0 and ma5 and ma20 and ma60:
        if close >= ma5 >= ma20 >= ma60:
            trend = 92.0
        elif close >= ma5 >= ma20:
            trend = 78.0
        elif close >= ma20 >= ma60:
            trend = 70.0
        elif close >= ma20:
            trend = 58.0
        else:
            trend = 30.0
    if recent_return_20d is not None:
        trend = _clamp(trend * 0.7 + _ratio_to_score(recent_return_20d, low=-12, high=18) * 0.3)
    if macd is not None and macd_signal is not None:
        if macd >= macd_signal and macd > 0:
            trend = _clamp(trend + 8)
        elif macd < macd_signal and macd < 0:
            trend = _clamp(trend - 10)

    breakout = _clamp(_ratio_to_score(breakout_pct, low=-6, high=8) * 0.55 + _ratio_to_score(position_pct_60d, low=20, high=95) * 0.45)
    if volume_ratio is not None:
        breakout = _clamp(breakout * 0.75 + _ratio_to_score(volume_ratio, low=0.8, high=2.8) * 0.25)

    volume = _clamp(_ratio_to_score(volume_ratio, low=0.6, high=2.6) * 0.65 + _ratio_to_score(turnover_rate, low=1.0, high=12.0) * 0.35)
    if volume_contraction_days >= 3:
        volume = _clamp(volume - 8)

    intraday = 50.0
    intraday_bias_scores = {
        "trend_continuation": 88.0,
        "pullback_support": 70.0,
        "range_balance": 55.0,
        "high_level_stall": 32.0,
        "selloff_pressure": 18.0,
    }
    if intraday_bias:
        intraday = intraday_bias_scores.get(intraday_bias, intraday)
    if last_30m_change_pct is not None and last_60m_change_pct is not None:
        intraday = _clamp(intraday * 0.6 + _ratio_to_score(last_30m_change_pct * 0.6 + last_60m_change_pct * 0.4, low=-2.5, high=2.5) * 0.4)
    if volume_acceleration_ratio is not None:
        intraday = _clamp(intraday * 0.8 + _ratio_to_score(volume_acceleration_ratio, low=0.7, high=1.6) * 0.2)
    if close > 0 and intraday_vwap and intraday_vwap > 0:
        intraday = _clamp(intraday + (6 if close >= intraday_vwap else -6))

    order_flow = 50.0
    if main_net_pct is not None:
        order_flow = _clamp(_ratio_to_score(main_net_pct, low=-30, high=30) * 0.8 + 10)
    if order_book_imbalance is not None:
        order_flow = _clamp(order_flow * 0.7 + _ratio_to_score(order_book_imbalance, low=-0.4, high=0.4) * 0.3)
    if spread_bps is not None and spread_bps > 20:
        order_flow = _clamp(order_flow - 8)

    chip = 50.0
    if close > 0 and chip_peak_price and chip_peak_price > 0:
        price_vs_peak = (close - chip_peak_price) / chip_peak_price * 100
        chip = _clamp(_ratio_to_score(price_vs_peak, low=-8, high=10))
    if close > 0 and avg_chip_cost and avg_chip_cost > 0:
        price_vs_cost = (close - avg_chip_cost) / avg_chip_cost * 100
        chip = _clamp(chip * 0.65 + _ratio_to_score(price_vs_cost, low=-10, high=8) * 0.35)
    concentration_text = str(features.get("chip_concentration_text") or "")
    if "高" in concentration_text:
        chip = _clamp(chip + 8)
    elif "低" in concentration_text:
        chip = _clamp(chip - 6)

    reversal = 40.0
    if rsi is not None:
        reversal = _ratio_to_score(70 - abs(rsi - 35), low=0, high=70)
    if bias_pct is not None:
        reversal = _clamp(reversal * 0.65 + _ratio_to_score(-abs(bias_pct + 4), low=-12, high=0) * 0.35)
    if lower_shadow_pct is not None:
        reversal = _clamp(reversal + min(lower_shadow_pct * 4, 12))
    if intraday_bias == "pullback_support":
        reversal = _clamp(reversal + 10)

    mean_reversion = 42.0
    if ma_density_pct is not None:
        mean_reversion = _clamp(_ratio_to_score(6 - ma_density_pct, low=0, high=6))
    if volume_contraction_days:
        mean_reversion = _clamp(mean_reversion + min(volume_contraction_days * 7, 21))
    if bias_pct is not None:
        mean_reversion = _clamp(mean_reversion + max(0.0, 12 - abs(bias_pct)))

    distribution_risk = 18.0
    if upper_shadow_pct is not None:
        distribution_risk = _clamp(distribution_risk + min(upper_shadow_pct * 8, 28))
    if turnover_rate is not None and position_pct_60d is not None:
        if turnover_rate >= 10 and position_pct_60d >= 80:
            distribution_risk = _clamp(distribution_risk + 24)
    if rsi is not None and rsi >= 78:
        distribution_risk = _clamp(distribution_risk + 18)
    if intraday_bias in {"high_level_stall", "selloff_pressure"}:
        distribution_risk = _clamp(distribution_risk + 22)
    if main_net_pct is not None and main_net_pct < -10:
        distribution_risk = _clamp(distribution_risk + 16)

    return {
        "trend": round(trend, 2),
        "breakout": round(breakout, 2),
        "volume": round(volume, 2),
        "intraday": round(intraday, 2),
        "order_flow": round(order_flow, 2),
        "chip": round(chip, 2),
        "reversal": round(reversal, 2),
        "mean_reversion": round(mean_reversion, 2),
        "distribution_risk": round(distribution_risk, 2),
    }


def _apply_selection_agent_nonlinear_adjustments(
    *,
    features: Dict[str, Any],
    component_scores: Dict[str, float],
    market_context: Dict[str, Any],
) -> Dict[str, Any]:
    state = str(market_context.get("state") or SELECTION_MARKET_STATE_RANGE)
    profile = market_context.get("profile") or SELECTION_AGENT_PROFILES[SELECTION_MARKET_STATE_RANGE]
    bonus_rules = profile.get("bonus_rules") or {}
    adjustments: List[Dict[str, Any]] = []

    def _push(label: str, value: float) -> None:
        adjustments.append({"label": label, "value": round(value, 2)})

    if (
        state == SELECTION_MARKET_STATE_MOMO
        and component_scores["trend"] >= 76
        and component_scores["breakout"] >= 72
        and component_scores["intraday"] >= 68
    ):
        _push("动量突破共振", bonus_rules.get("momentum_confirmation", 8))
    if component_scores["order_flow"] >= 65 and component_scores["volume"] >= 55:
        _push("资金跟随确认", bonus_rules.get("smart_money_follow", 4))
    if (
        state == SELECTION_MARKET_STATE_ICE
        and component_scores["reversal"] >= 70
        and component_scores["mean_reversion"] >= 70
        and _safe_int(features.get("volume_contraction_days")) >= 3
    ):
        _push("冰点错杀修复", bonus_rules.get("washout_reversal", 10))
    if (
        state == SELECTION_MARKET_STATE_RANGE
        and (_optional_float(features.get("ma_density_pct")) or 99) <= 2.5
        and component_scores["chip"] >= 60
    ):
        _push("均线收敛待突破", bonus_rules.get("tight_structure", 6))
    if (
        state == SELECTION_MARKET_STATE_RETREAT
        and component_scores["chip"] >= 60
        and component_scores["order_flow"] >= 58
        and (_optional_float(features.get("position_pct_60d")) or 0) <= 70
    ):
        _push("退潮防守轮动", bonus_rules.get("defensive_rotation", 4))
    if component_scores["distribution_risk"] >= 72:
        _push("派发风险惩罚", bonus_rules.get("distribution_penalty", -12))

    total_adjustment = round(sum(item["value"] for item in adjustments), 2)
    return {"adjustments": adjustments, "total_adjustment": total_adjustment}


def _run_volume_price_resonance_agent(
    *,
    asset: Dict[str, Any],
    matches: List[Dict[str, Any]],
    bundle: Dict[str, Any],
    market_context: Dict[str, Any],
) -> Dict[str, Any]:
    indicators = bundle.get("indicators") or {}
    volume_ratio = _optional_float(indicators.get("volume_ratio") or (bundle.get("stock_info") or {}).get("volume_ratio"))
    turnover_rate = _optional_float((bundle.get("stock_info") or {}).get("turnover_rate"))
    intraday_context = _get_selection_intraday_context(str(asset.get("symbol") or ""))
    realtime_freshness = _get_selection_realtime_freshness(
        intraday_context,
        update_time=(bundle.get("realtime_quote") or {}).get("update_time"),
    )
    main_force_flow = _get_selection_main_force_flow(
        str(asset.get("symbol") or ""),
        volume_ratio=volume_ratio,
        turnover_rate=turnover_rate,
    )
    features = _extract_selection_agent_features(
        bundle=bundle,
        market_context=market_context,
        intraday_context=intraday_context,
        main_force_flow=main_force_flow,
    )
    component_scores = _compute_selection_component_scores(features)
    profile = market_context.get("profile") or SELECTION_AGENT_PROFILES[SELECTION_MARKET_STATE_RANGE]
    weights = profile.get("weights") or {}
    weighted_sum = 0.0
    total_weight = 0.0
    for key, weight in weights.items():
        score = component_scores.get(key)
        if score is None:
            continue
        weighted_sum += score * float(weight)
        total_weight += float(weight)
    base_score = weighted_sum / total_weight if total_weight > 0 else 50.0
    nonlinear = _apply_selection_agent_nonlinear_adjustments(
        features=features,
        component_scores=component_scores,
        market_context=market_context,
    )
    agent_score = _clamp(base_score + nonlinear["total_adjustment"])
    heat_score = max((_safe_float(item.get("heat_score")) for item in matches), default=0.0)
    composite_score = round(
        heat_score * float(profile.get("heat_multiplier") or 1.0)
        + agent_score * float(profile.get("agent_multiplier") or 1.0),
        2,
    )
    return {
        "agent_score": round(agent_score, 2),
        "composite_score": composite_score,
        "component_scores": component_scores,
        "features": features,
        "intraday_context": intraday_context,
        "realtime_freshness": realtime_freshness,
        "main_force_flow": main_force_flow,
        "market_context": market_context,
        "nonlinear_adjustments": nonlinear,
    }


def _build_selection_reason(item: Dict[str, Any]) -> str:
    sector_bits = ",".join(match.get("sector") for match in item.get("matched_sectors") or [])
    metrics = item.get("technical_metrics") or {}
    market_state = str(metrics.get("market_state_label") or "震荡市")
    signal_labels = metrics.get("signal_labels") if isinstance(metrics.get("signal_labels"), list) else []
    return (
        f"市场状态 {market_state}；"
        f"板块主线 {sector_bits or '未命中'}；"
        f"Agent分 {item.get('tech_score', 0):.1f}；"
        f"量比 {metrics.get('volume_ratio', 'N/A')}；"
        f"信号 {','.join(signal_labels[:3]) or '日线结构中性'}"
    )[:300]


def _score_selection_candidate(
    asset: Dict[str, Any],
    context: Dict[str, Any],
    extracted_sectors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    matches = _match_asset_to_themes(asset, context, extracted_sectors)
    if not matches:
        return {}
    bundle = _build_stock_data_bundle(str(asset.get("symbol") or ""))
    if bundle.get("error"):
        return {
            "asset_id": asset.get("id"),
            "symbol": asset.get("symbol"),
            "name": _derive_asset_display_name(asset),
            "matched_sectors": matches,
            "primary_sector": matches[0]["sector"],
            "heat_score": _safe_float(matches[0].get("heat_score")),
            "tech_score": 0.0,
            "composite_score": _safe_float(matches[0].get("heat_score")),
            "technical_metrics": {
                "error": bundle.get("error"),
                "market_state": str((context.get("market_context") or {}).get("state") or ""),
                "market_state_label": str((context.get("market_context") or {}).get("state_label") or ""),
            },
            "reason": f"仅板块命中，技术数据降级: {bundle.get('error')}",
            "market_cap": 0.0,
            "asset": asset,
        }

    heat_score = max((_safe_float(item.get("heat_score")) for item in matches), default=0.0)
    primary_sector = max(matches, key=lambda item: _safe_float(item.get("heat_score"))).get("sector")
    market_context = context.get("market_context") if isinstance(context.get("market_context"), dict) else {}
    agent_result = _run_volume_price_resonance_agent(
        asset=asset,
        matches=matches,
        bundle=bundle,
        market_context=market_context,
    )
    stock_info = bundle.get("stock_info") or {}
    indicators = bundle.get("indicators") or {}
    features = agent_result.get("features") if isinstance(agent_result.get("features"), dict) else {}
    component_scores = agent_result.get("component_scores") if isinstance(agent_result.get("component_scores"), dict) else {}
    adjustments = agent_result.get("nonlinear_adjustments") if isinstance(agent_result.get("nonlinear_adjustments"), dict) else {}
    realtime_freshness = agent_result.get("realtime_freshness") if isinstance(agent_result.get("realtime_freshness"), dict) else {}
    signal_labels = [item.get("label") for item in adjustments.get("adjustments") or [] if isinstance(item, dict) and str(item.get("label") or "").strip()]
    if not signal_labels:
        state_label = str(market_context.get("state_label") or "")
        if state_label:
            signal_labels = [f"{state_label}框架"]
    candidate = {
        "asset_id": asset.get("id"),
        "symbol": asset.get("symbol"),
        "name": _derive_asset_display_name(asset),
        "matched_sectors": matches,
        "primary_sector": primary_sector,
        "heat_score": round(heat_score, 2),
        "tech_score": _safe_float(agent_result.get("agent_score")),
        "composite_score": _safe_float(agent_result.get("composite_score")),
        "technical_metrics": {
            "close": _safe_round(_optional_float(features.get("close")), 4),
            "ma5": _safe_round(_optional_float(features.get("ma5")), 4),
            "ma20": _safe_round(_optional_float(features.get("ma20")), 4),
            "ma60": _safe_round(_optional_float(features.get("ma60")), 4),
            "rsi": _safe_round(_optional_float(features.get("rsi")), 2),
            "volume_ratio": _safe_round(_optional_float(features.get("volume_ratio")), 2),
            "turnover_rate": _safe_round(_optional_float(features.get("turnover_rate")), 2),
            "macd": _safe_round(_optional_float(features.get("macd")), 4),
            "macd_signal": _safe_round(_optional_float(features.get("macd_signal")), 4),
            "market_state": str(market_context.get("state") or ""),
            "market_state_label": str(market_context.get("state_label") or ""),
            "market_regime_signals": market_context.get("signals") or [],
            "trend_score": component_scores.get("trend"),
            "breakout_score": component_scores.get("breakout"),
            "volume_score": component_scores.get("volume"),
            "intraday_score": component_scores.get("intraday"),
            "order_flow_score": component_scores.get("order_flow"),
            "chip_score": component_scores.get("chip"),
            "reversal_score": component_scores.get("reversal"),
            "mean_reversion_score": component_scores.get("mean_reversion"),
            "distribution_risk": component_scores.get("distribution_risk"),
            "breakout_pct": _safe_round(_optional_float(features.get("breakout_pct")), 2),
            "position_pct_60d": _safe_round(_optional_float(features.get("position_pct_60d")), 2),
            "bias_pct": _safe_round(_optional_float(features.get("bias_pct")), 2),
            "volume_contraction_days": _safe_int(features.get("volume_contraction_days")),
            "intraday_bias": str(features.get("intraday_bias") or ""),
            "intraday_bias_text": str(features.get("intraday_bias_text") or ""),
            "tail_session": bool(features.get("tail_session")),
            "latest_minute_time": str((agent_result.get("intraday_context") or {}).get("latest_minute_time") or ""),
            "latest_trade_time": str((agent_result.get("intraday_context") or {}).get("latest_trade_time") or ""),
            "last_30m_change_pct": _safe_round(_optional_float(features.get("last_30m_change_pct")), 2),
            "last_60m_change_pct": _safe_round(_optional_float(features.get("last_60m_change_pct")), 2),
            "order_book_imbalance": _safe_round(_optional_float(features.get("order_book_imbalance")), 3),
            "main_net_pct": _safe_round(_optional_float(features.get("main_net_pct")), 2),
            "signal_labels": signal_labels,
            "nonlinear_adjustments": adjustments.get("adjustments") or [],
            "realtime_freshness": realtime_freshness,
        },
        "reason": "",
        "market_cap": _safe_float(stock_info.get("market_cap")),
        "asset": asset,
    }
    candidate["reason"] = _build_selection_reason(candidate)
    return candidate


def _group_recent_longhubang_by_symbol(days: int, warnings: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    try:
        from longhubang_data import LonghubangDataFetcher

        records = LonghubangDataFetcher().get_recent_days_data(days=days)
    except Exception as exc:
        warnings.append(f"龙虎榜数据降级: {exc}")
        return {}

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for record in records or []:
        symbol = str(record.get("gpdm") or record.get("股票代码") or "").strip()
        if symbol.isdigit():
            symbol = symbol.zfill(6)
        if not symbol:
            continue
        grouped.setdefault(symbol, []).append(record)
    return grouped


def _compact_text(value: Any, *, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _parse_flexible_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000.0
            return datetime.fromtimestamp(timestamp)
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    relative_match = re.match(r"^(今天|昨天|前天)(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?$", text)
    if relative_match:
        base = datetime.now()
        offset = {"今天": 0, "昨天": 1, "前天": 2}[relative_match.group(1)]
        date_value = (base - timedelta(days=offset)).date()
        time_part = relative_match.group(2) or "00:00:00"
        if len(time_part) == 5:
            time_part = f"{time_part}:00"
        return datetime.fromisoformat(f"{date_value.isoformat()} {time_part}")

    normalized = (
        text.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
        .replace("/", "-")
        .replace(".", "-")
        .replace("T", " ")
    )
    parsed = _parse_dt(normalized)
    if parsed is not None:
        return parsed

    if normalized.isdigit() and len(normalized) == 8:
        return _parse_dt(f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:]}")
    return None


def _is_within_risk_window(
    value: Any,
    *,
    lookback_days: int = RISK_NEWS_LOOKBACK_DAYS,
    lookahead_days: int = RISK_EVENT_LOOKAHEAD_DAYS,
) -> bool:
    parsed = _parse_flexible_datetime(value)
    if parsed is None:
        return False
    now = datetime.now()
    return now - timedelta(days=lookback_days) <= parsed <= now + timedelta(days=lookahead_days)


def _safe_records_from_table(raw_value: Any, limit: int = RISK_ITEM_LIMIT) -> List[Dict[str, Any]]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        records = [item for item in raw_value if isinstance(item, dict)]
        return records[:limit]
    if hasattr(raw_value, "to_dict"):
        try:
            records = raw_value.head(limit).to_dict("records")
            return [item for item in records if isinstance(item, dict)]
        except Exception:
            return []
    return []


def _extract_recent_news_items(
    items: List[Dict[str, Any]],
    *,
    lookback_days: int = RISK_NEWS_LOOKBACK_DAYS,
    limit: int = RISK_ITEM_LIMIT,
) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    undated: List[Dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        payload = {
            "title": _compact_text(item.get("title")),
            "content": _compact_text(item.get("content") or item.get("summary")),
            "publish_time": str(item.get("publish_time") or ""),
            "source": str(item.get("source") or ""),
        }
        parsed = _parse_flexible_datetime(payload["publish_time"])
        if parsed is None:
            undated.append(payload)
            continue
        if _is_within_risk_window(parsed, lookback_days=lookback_days, lookahead_days=0):
            collected.append(payload)
    merged = collected[:limit]
    if len(merged) < limit:
        merged.extend(undated[: max(0, limit - len(merged))])
    return merged[:limit]


def _extract_near_term_structured_events(risk_payload: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    def _build_rows(section: Dict[str, Any], *, date_columns: tuple[str, ...], detail_columns: tuple[str, ...], limit: int = 4) -> List[Dict[str, Any]]:
        if not isinstance(section, dict) or not section.get("has_data"):
            return []
        rows = _safe_records_from_table(section.get("data"), limit=12)
        events: List[Dict[str, Any]] = []
        now = datetime.now()
        for row in rows:
            date_value = None
            for column in date_columns:
                if column in row and row.get(column) not in (None, ""):
                    date_value = row.get(column)
                    break
            parsed_date = _parse_flexible_datetime(date_value)
            days_offset = (parsed_date.date() - now.date()).days if parsed_date else None
            if parsed_date:
                if detail_columns == ("解禁股数", "解禁市值", "占总股本比例", "占流通股比例", "股东名称"):
                    if days_offset is None or days_offset < 0 or days_offset > RISK_EVENT_LOOKAHEAD_DAYS:
                        continue
                elif detail_columns == ("股东名称", "减持股数", "减持比例", "变动方向", "股份类型"):
                    if days_offset is None or days_offset < -RISK_EVENT_LOOKAHEAD_DAYS or days_offset > 0:
                        continue
                else:
                    if days_offset is None or abs(days_offset) > RISK_EVENT_LOOKAHEAD_DAYS:
                        continue
            detail_bits: List[str] = []
            for column in detail_columns:
                value = row.get(column)
                if value in (None, ""):
                    continue
                detail_bits.append(f"{column}: {_compact_text(value, limit=60)}")
            events.append(
                {
                    "date": parsed_date.strftime("%Y-%m-%d %H:%M:%S") if parsed_date else str(date_value or ""),
                    "days_offset": days_offset,
                    "details": " | ".join(detail_bits)[:240],
                }
            )
        events.sort(key=lambda item: (abs(_safe_int(item.get("days_offset"))), str(item.get("date") or "")))
        return events[:limit]

    return {
        "lifting_ban": _build_rows(
            risk_payload.get("lifting_ban") or {},
            date_columns=("解禁时间", "限售解禁日", "日期"),
            detail_columns=("解禁股数", "解禁市值", "占总股本比例", "占流通股比例", "股东名称"),
        ),
        "shareholder_reduction": _build_rows(
            risk_payload.get("shareholder_reduction") or {},
            date_columns=("公告日期", "减持日期", "变动日期"),
            detail_columns=("股东名称", "减持股数", "减持比例", "变动方向", "股份类型"),
        ),
        "important_events": _build_rows(
            risk_payload.get("important_events") or {},
            date_columns=("事件时间", "公告日期", "日期"),
            detail_columns=("事件类型", "事件内容", "标题"),
        ),
    }


def _fallback_structured_risk_result(
    symbol: str,
    name: str,
    structured_events: Dict[str, List[Dict[str, Any]]],
    longhubang_note: str,
) -> Dict[str, Any]:
    risk_notes: List[str] = []
    vetoed = False
    if structured_events.get("shareholder_reduction"):
        vetoed = True
        risk_notes.append("近2天存在股东减持或减持公告")
    if structured_events.get("lifting_ban"):
        risk_notes.append("未来2天内存在限售解禁安排")
    if structured_events.get("important_events"):
        risk_notes.append("近2天存在重要事件")
    return {
        "symbol": symbol,
        "name": name,
        "vetoed": vetoed,
        "keyword_hits": [],
        "risk_notes": risk_notes,
        "longhubang_note": longhubang_note,
        "risk_level": "high" if vetoed else "medium" if risk_notes else "low",
    }


def _intelligent_risk_assessment_with_llm(
    *,
    symbol: str,
    name: str,
    recent_announcements: List[Dict[str, Any]],
    recent_news: List[Dict[str, Any]],
    supplemental_news: List[Dict[str, Any]],
    structured_events: Dict[str, List[Dict[str, Any]]],
    longhubang_note: str,
    client: Optional[DeepSeekClient],
) -> Dict[str, Any]:
    if client is None:
        return {}

    evidence_count = (
        len(recent_announcements)
        + len(recent_news)
        + len(supplemental_news)
        + sum(len(items) for items in structured_events.values())
    )
    if evidence_count == 0:
        return {}

    messages = build_messages(
        "research_hub/selection_risk_control.system.txt",
        "research_hub/selection_risk_control.user.txt",
        symbol=symbol,
        stock_name=name,
        lookback_days=RISK_NEWS_LOOKBACK_DAYS,
        lookahead_days=RISK_EVENT_LOOKAHEAD_DAYS,
        announcements_payload=json.dumps(recent_announcements, ensure_ascii=False, indent=2),
        news_payload=json.dumps(recent_news, ensure_ascii=False, indent=2),
        supplemental_payload=json.dumps(supplemental_news, ensure_ascii=False, indent=2),
        structured_payload=json.dumps(structured_events, ensure_ascii=False, indent=2),
        longhubang_note=longhubang_note,
    )
    response = client.call_api(messages, max_tokens=1200, tier=ModelTier.REASONING)
    parsed = _decode_first_json_value(response)
    if not isinstance(parsed, dict):
        return {}
    rationale = str(parsed.get("rationale") or "").strip()
    risk_notes = [
        str(note or "").strip()
        for note in parsed.get("risk_notes") or []
        if str(note or "").strip()
    ]
    if rationale and not risk_notes:
        risk_notes = [rationale]
    return {
        "symbol": symbol,
        "name": name,
        "vetoed": bool(parsed.get("vetoed")),
        "keyword_hits": [],
        "risk_notes": risk_notes,
        "longhubang_note": longhubang_note,
        "risk_level": str(parsed.get("risk_level") or "").strip() or ("high" if parsed.get("vetoed") else "low"),
        "rationale": rationale,
    }


def _evaluate_risk_for_symbol(
    symbol: str,
    name: str,
    longhubang_map: Dict[str, List[Dict[str, Any]]],
    warnings: List[str],
    *,
    risk_client: Optional[DeepSeekClient] = None,
) -> Dict[str, Any]:
    lhb_records = longhubang_map.get(symbol) or []

    try:
        from stock_research_news_data import StockResearchNewsDataFetcher

        news_payload = StockResearchNewsDataFetcher(max_items=5).get_stock_news(symbol)
    except Exception as exc:
        warnings.append(f"{symbol} 新闻公告风控降级: {exc}")
        news_payload = {}

    texts: List[str] = []
    for bucket in ("news_data", "announcement_data", "supplemental_news_data"):
        data = news_payload.get(bucket) or {}
        for item in data.get("items") or []:
            texts.append(f"{item.get('title') or ''} {item.get('content') or ''}")

    try:
        from stock_data import StockDataFetcher

        risk_payload = StockDataFetcher().get_risk_data(symbol)
    except Exception as exc:
        warnings.append(f"{symbol} 风险数据降级: {exc}")
        risk_payload = {}

    lhb_note = ""
    if lhb_records:
        lhb_note = f"最近3日龙虎榜上榜 {len(lhb_records)} 次"

    recent_announcements = _extract_recent_news_items((news_payload.get("announcement_data") or {}).get("items") or [])
    recent_news = _extract_recent_news_items((news_payload.get("news_data") or {}).get("items") or [])
    supplemental_news = _extract_recent_news_items((news_payload.get("supplemental_news_data") or {}).get("items") or [])
    structured_events = _extract_near_term_structured_events(risk_payload if isinstance(risk_payload, dict) else {})

    if str(name or "").upper().startswith("ST"):
        structured_events.setdefault("important_events", []).append(
            {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "days_offset": 0,
                "details": "股票简称包含 ST",
            }
        )

    try:
        intelligent_result = _intelligent_risk_assessment_with_llm(
            symbol=symbol,
            name=name,
            recent_announcements=recent_announcements,
            recent_news=recent_news,
            supplemental_news=supplemental_news,
            structured_events=structured_events,
            longhubang_note=lhb_note,
            client=risk_client,
        )
    except Exception as exc:
        warnings.append(f"{symbol} 智能风控降级: {exc}")
        intelligent_result = {}

    if intelligent_result:
        if not intelligent_result.get("vetoed") and not intelligent_result.get("risk_notes"):
            intelligent_result["risk_level"] = "low"
        return intelligent_result

    fallback = _fallback_structured_risk_result(symbol, name, structured_events, lhb_note)

    return fallback


def _persist_selection_results(
    *,
    selected_auto: List[Dict[str, Any]],
    kept_manual_pins: List[Dict[str, Any]],
    focus_assets: List[Dict[str, Any]],
    sector_info: Dict[str, Any],
    market_context: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    final_symbols = {item["symbol"] for item in selected_auto} | {item["symbol"] for item in kept_manual_pins}
    pinned_symbols = {item["symbol"] for item in kept_manual_pins}
    demoted: List[Dict[str, Any]] = []

    for item in selected_auto:
        asset = item["asset"]
        reason = (item.get("reason") or "智能选股入选")[:300]
        asset_repository.update_asset(
            int(asset["id"]),
            status=STATUS_FOCUS,
            pool_reason=reason,
            pool_reason_source="selection",
            last_funnel_score=item.get("composite_score"),
            last_funnel_snapshot_json={
                "selection_version": 2,
                "agent": "ResearchHubSelectionOrchestrator",
                "theme_extraction": item.get("matched_sectors"),
                "technical_metrics": item.get("technical_metrics"),
                "sector_report_id": sector_info.get("report_id"),
                "market_state": str((market_context or {}).get("state") or ""),
                "market_state_label": str((market_context or {}).get("state_label") or ""),
            },
        )
        asset_service.sync_managed_monitors(int(asset["id"]))
        _get_agent_memory_service().db.save_working_memory(
            stock_code=item["symbol"],
            analysis_date=now_text,
            decision_summary=f"智能选股入选 | {reason[:180]}",
            strategy={"status": STATUS_FOCUS, "score": item.get("composite_score")},
        )

    for asset in focus_assets:
        if asset.get("symbol") in final_symbols or asset.get("symbol") in pinned_symbols:
            continue
        asset_repository.transition_asset_status(
            int(asset["id"]),
            STATUS_RESEARCH,
            note=asset.get("note") or "本轮智能选股未入选",
            pool_reason="本轮智能选股未入选",
            pool_reason_source="selection",
        )
        asset_service.sync_managed_monitors(int(asset["id"]))
        demoted.append(
            {
                "asset_id": asset["id"],
                "symbol": asset["symbol"],
                "name": _derive_asset_display_name(asset),
                "reason": "本轮智能选股未入选",
            }
        )

    return demoted


def _run_selection_pipeline(
    report_progress,
    *,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> Dict[str, Any]:
    warnings: List[str] = []
    report_progress(current=5, total=100, message="检查智策板块报告...")
    sector_info = ensure_recent_sector_strategy_report(
        lightweight_model=lightweight_model,
        reasoning_model=reasoning_model,
    )
    warnings.extend(str(item).strip() for item in sector_info.get("warnings") or [] if str(item).strip())
    report = sector_info.get("report") or {}

    report_progress(current=20, total=100, message="主题抽取智能体正在提炼热点板块...")
    extracted_sectors = _extract_selection_sectors(
        report,
        warnings,
        lightweight_model=lightweight_model,
        reasoning_model=reasoning_model,
    )
    market_context = _build_selection_market_context(report)

    research_assets = [
        asset
        for asset in asset_repository.list_assets(status=STATUS_RESEARCH, include_deleted=False)
        if _is_a_share_symbol(asset.get("symbol"))
    ]
    focus_assets = [
        asset
        for asset in asset_repository.list_assets(status=STATUS_FOCUS, include_deleted=False)
        if _is_a_share_symbol(asset.get("symbol"))
    ]
    pinned_assets = [asset for asset in focus_assets if asset.get("manual_pin")]

    report_progress(current=40, total=100, message="基础池映射智能体正在筛选研究池股票...")
    mapped_candidates: List[Dict[str, Any]] = []
    for asset in research_assets:
        context = _collect_asset_match_context(asset, warnings)
        context["market_context"] = market_context
        matches = _match_asset_to_themes(asset, context, extracted_sectors)
        if not matches:
            continue
        mapped_candidates.append(
            {
                "asset_id": asset.get("id"),
                "symbol": asset.get("symbol"),
                "name": _derive_asset_display_name(asset),
                "sector_tags": context.get("tags") or [],
                "matched_sectors": matches,
            }
        )

    if not mapped_candidates and research_assets and extracted_sectors:
        warnings.append("热点板块未命中研究池标签，候选池为空")

    report_progress(current=60, total=100, message="量价共振智能体正在计算综合得分...")
    ranked_candidates: List[Dict[str, Any]] = []
    for asset in research_assets[:SECTOR_MATCH_FALLBACK_LIMIT]:
        context = _collect_asset_match_context(asset, warnings)
        context["market_context"] = market_context
        candidate = _score_selection_candidate(asset, context, extracted_sectors)
        if candidate:
            ranked_candidates.append(candidate)
    ranked_candidates.sort(key=lambda item: item.get("composite_score", 0), reverse=True)
    ranked_top15 = ranked_candidates[:SELECTION_RANK_LIMIT]

    excluded_by_risk: List[Dict[str, Any]] = []
    selection_candidates: List[Dict[str, Any]] = list(ranked_top15)

    kept_manual_pins: List[Dict[str, Any]] = []
    for asset in pinned_assets:
        kept_manual_pins.append(
            {
                "asset_id": asset["id"],
                "symbol": asset["symbol"],
                "name": _derive_asset_display_name(asset),
                "primary_sector": _derive_asset_primary_sector(asset),
                "reason": "手动加星保留",
                "risk_notes": [],
                "risk_flagged": False,
                "selection_type": "manual",
            }
        )

    report_progress(current=78, total=100, message="去重智能体正在生成最终 Top 10 关注名单...")
    auto_capacity = max(0, SELECTION_TOP_K - len(kept_manual_pins))
    if len(kept_manual_pins) > SELECTION_TOP_K:
        warnings.append(f"手动加星数量为 {len(kept_manual_pins)}，已超过 Top {SELECTION_TOP_K} 容量，本轮不新增自动入选标的")
        auto_capacity = 0

    excluded_by_dedup: List[Dict[str, Any]] = []
    bucket_counts: Dict[str, int] = {}
    selected_auto: List[Dict[str, Any]] = []
    for item in selection_candidates:
        if len(selected_auto) >= auto_capacity:
            break
        bucket = _normalize_sector_text(item.get("primary_sector")) or "other"
        if bucket_counts.get(bucket, 0) >= 2:
            excluded_by_dedup.append(
                {
                    "symbol": item["symbol"],
                    "name": item.get("name") or item["symbol"],
                    "bucket": item.get("primary_sector"),
                    "reason": "同一热点主线最多保留 2 只",
                }
            )
            continue
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        selected_auto.append(item)

    if auto_capacity > len(selected_auto):
        warnings.append(f"去重后仅保留 {len(selected_auto)} 只自动候选，未补满 Top {SELECTION_TOP_K}")

    final_selected = kept_manual_pins + [
        {
            "asset_id": item["asset_id"],
            "symbol": item["symbol"],
            "name": item.get("name") or item["symbol"],
            "score": item.get("composite_score"),
            "heat_score": item.get("heat_score"),
            "tech_score": item.get("tech_score"),
            "primary_sector": item.get("primary_sector"),
            "reason": item.get("reason"),
            "selection_type": "auto",
        }
        for item in selected_auto
    ]

    review_payload = {}
    try:
        review_payload = _review_selection_candidates_with_llm(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
            extracted_sectors=extracted_sectors,
            ranked_top15=ranked_top15,
            final_selected=final_selected,
            kept_manual_pins=kept_manual_pins,
            excluded_by_risk=excluded_by_risk,
            excluded_by_dedup=excluded_by_dedup,
        )
    except Exception as exc:
        warnings.append(f"推理模型复核失败: {exc}")
        review_payload = {}

    if review_payload:
        review_warnings = review_payload.get("warnings")
        if isinstance(review_warnings, list):
            for warning in review_warnings:
                text = str(warning or "").strip()
                if text:
                    warnings.append(text)
        selected_auto = _apply_selection_review_order(selected_auto, review_payload, warnings)
        final_selected = kept_manual_pins + [
            {
                "asset_id": item["asset_id"],
                "symbol": item["symbol"],
                "name": item.get("name") or item["symbol"],
                "score": item.get("composite_score"),
                "heat_score": item.get("heat_score"),
                "tech_score": item.get("tech_score"),
                "primary_sector": item.get("primary_sector"),
                "reason": item.get("reason"),
                "selection_type": "auto",
            }
            for item in selected_auto
        ]

    demoted = _persist_selection_results(
        selected_auto=selected_auto,
        kept_manual_pins=kept_manual_pins,
        focus_assets=focus_assets,
        sector_info=sector_info,
        market_context=market_context,
    )

    report_progress(current=100, total=100, message="智能选股完成")
    return {
        "sector_strategy_task_id": sector_info.get("task_id"),
        "sector_strategy_report_id": sector_info.get("report_id"),
        "sector_strategy_reused": bool(sector_info.get("reused")),
        "focus_capacity": FOCUS_CAPACITY,
        "market_context": market_context,
        "extracted_sectors": extracted_sectors,
        "mapped_candidates": mapped_candidates,
        "ranked_top15": [
            {
                "asset_id": item["asset_id"],
                "symbol": item["symbol"],
                "name": item.get("name") or item["symbol"],
                "score": item.get("composite_score"),
                "heat_score": item.get("heat_score"),
                "tech_score": item.get("tech_score"),
                "primary_sector": item.get("primary_sector"),
                "reason": item.get("reason"),
            }
            for item in ranked_top15
        ],
        "final_selected": final_selected,
        "kept_manual_pins": kept_manual_pins,
        "excluded_by_risk": excluded_by_risk,
        "excluded_by_dedup": excluded_by_dedup,
        "demoted": demoted,
        "warnings": list(dict.fromkeys(warnings)),
        "llm_review": review_payload or {},
        "promoted": final_selected,
        "kept": kept_manual_pins,
        "ranked_candidates": [
            {
                "asset_id": item["asset_id"],
                "symbol": item["symbol"],
                "name": item.get("name") or item["symbol"],
                "score": item.get("composite_score"),
            }
            for item in ranked_top15
        ],
    }


def submit_selection_run(
    *,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> str:
    def runner(_task_id: str, report_progress) -> Dict[str, Any]:
        return _run_selection_pipeline(
            report_progress,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )

    return start_ui_analysis_task(
        task_type=RESEARCH_HUB_SELECTION_TASK_TYPE,
        label="投研中心智能选股",
        runner=runner,
        metadata={
            "lightweight_model": lightweight_model,
            "reasoning_model": reasoning_model,
        },
    )


def submit_funnel_run(
    *,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> str:
    return submit_selection_run(
        lightweight_model=lightweight_model,
        reasoning_model=reasoning_model,
    )


def get_selection_task_status(task_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    services = _get_backend_services()
    if task_id:
        return services.get_ui_task(RESEARCH_HUB_SELECTION_TASK_TYPE, task_id)
    return services.get_latest_ui_task(RESEARCH_HUB_SELECTION_TASK_TYPE)


def get_funnel_task_status(task_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    return get_selection_task_status(task_id)
