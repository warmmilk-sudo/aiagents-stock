from __future__ import annotations

import json
import re
from typing import Any


DEFAULT_TEXT = "暂无"
DEFAULT_RISK_LEVEL = "中等"
DEFAULT_MARKET_OUTLOOK = "中性"
DEFAULT_CONFIDENCE_SCORE = 0
DEFAULT_INVESTMENT_HORIZON = "1-2周"

RAW_REPORT_KEYS = ("macro", "sector", "fund", "sentiment", "team")

LONG_SHORT_KEYS = ("bullish", "neutral", "bearish")
ROTATION_KEYS = ("current_strong", "potential", "declining")
HEAT_KEYS = ("hottest", "heating", "cooling")

SUMMARY_KEYS = ("market_view", "key_opportunity", "major_risk", "strategy")


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _unwrap_sector_strategy_payload(value: Any) -> dict[str, Any]:
    payload = _as_dict(value)
    if isinstance(payload.get("analysis_content_parsed"), dict):
        return payload["analysis_content_parsed"]
    if isinstance(payload.get("result"), dict) and any(
        key in payload["result"] for key in ("final_predictions", "agents_analysis", "comprehensive_report")
    ):
        return payload["result"]
    return payload


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _contains_cjk(text: str) -> bool:
    return bool(re.search(r"[\u3400-\u4dbf\u4e00-\u9fff]", text))


def _looks_english_only(text: str) -> bool:
    return bool(re.search(r"[A-Za-z]", text)) and not _contains_cjk(text)


def _normalize_text(
    value: Any,
    *,
    fallback: str = DEFAULT_TEXT,
    allow_english_only: bool = False,
    language_paths: list[str] | None = None,
    field_path: str = "",
) -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    if not text:
        return fallback
    if not allow_english_only and _looks_english_only(text):
        if language_paths is not None and field_path:
            language_paths.append(field_path)
        return fallback
    return text


def _normalize_score(value: Any, default: int = DEFAULT_CONFIDENCE_SCORE) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default

    if numeric <= 1:
        numeric *= 100
    elif numeric <= 10:
        numeric *= 10
    return max(0, min(100, int(round(numeric))))


def _normalize_number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if not _is_empty(value):
            return value
    return None


def _split_report_sections(value: Any) -> tuple[str, str]:
    if value is None:
        return "", ""
    if isinstance(value, dict):
        text = json.dumps(value, ensure_ascii=False, indent=2)
    else:
        text = str(value).strip()
    if not text:
        return "", ""

    reasoning_parts: list[str] = []

    def _collect_think(match: re.Match[str]) -> str:
        content = match.group(1).strip()
        if content:
            reasoning_parts.append(content)
        return ""

    text = re.sub(r"<think>([\s\S]*?)</think>", _collect_think, text, flags=re.IGNORECASE).strip()

    marker = re.search(r"[\[【]推理过程[\]】]|^\s*推理过程[:：]", text, flags=re.MULTILINE)
    if not marker:
        return text, "\n\n".join(part for part in reasoning_parts if part).strip()

    before = text[: marker.start()].strip()
    after = text[marker.end() :].strip()
    after = re.sub(r"^\s*推理过程[:：]\s*", "", after).strip()
    reasoning = "\n\n".join(part for part in ["\n\n".join(reasoning_parts).strip(), after] if part).strip()
    return before, reasoning


def _build_summary(value: Any) -> str:
    text = str(value or "")
    for line in text.replace("**", "").replace("#", " ").replace(">", " ").splitlines():
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return DEFAULT_TEXT


def _detect_report_key(name: str, payload: dict[str, Any]) -> str | None:
    text = " ".join(
        [
            name,
            str(payload.get("agent_name") or ""),
            str(payload.get("agent_role") or ""),
            " ".join(str(item) for item in _as_list(payload.get("focus_areas"))),
        ]
    ).lower()
    if re.search(r"macro|宏观", text):
        return "macro"
    if re.search(r"sector|板块|行业", text):
        return "sector"
    if re.search(r"fund[_\s-]?flow|资金|主力|北向", text):
        return "fund"
    if re.search(r"sentiment|情绪|热度", text):
        return "sentiment"
    if re.search(r"discussion|chief|团队|首席|综合", text):
        return "team"
    return None


def _normalize_long_short_item(
    item: Any,
    group_key: str,
    *,
    language_paths: list[str],
) -> dict[str, Any]:
    payload = _as_dict(item)
    defaults = {
        "bullish": "看多",
        "neutral": "中性",
        "bearish": "看空",
    }
    return {
        "sector": _normalize_text(
            _first_non_empty(payload.get("sector"), payload.get("name")),
            allow_english_only=True,
            language_paths=language_paths,
            field_path=f"long_short.{group_key}.sector",
        ),
        "direction": _normalize_text(
            payload.get("direction"),
            fallback=defaults[group_key],
            language_paths=language_paths,
            field_path=f"long_short.{group_key}.direction",
        ),
        "reason": _normalize_text(
            payload.get("reason"),
            language_paths=language_paths,
            field_path=f"long_short.{group_key}.reason",
        ),
        "confidence": _normalize_score(payload.get("confidence"), default=0),
        "risk": _normalize_text(
            payload.get("risk"),
            language_paths=language_paths,
            field_path=f"long_short.{group_key}.risk",
        ),
    }


def _normalize_rotation_item(
    item: Any,
    group_key: str,
    *,
    language_paths: list[str],
) -> dict[str, Any]:
    payload = _as_dict(item)
    defaults = {
        "current_strong": "强势",
        "potential": "潜力",
        "declining": "衰退",
    }
    return {
        "sector": _normalize_text(
            _first_non_empty(payload.get("sector"), payload.get("name")),
            allow_english_only=True,
            language_paths=language_paths,
            field_path=f"rotation.{group_key}.sector",
        ),
        "stage": _normalize_text(
            payload.get("stage"),
            fallback=defaults[group_key],
            language_paths=language_paths,
            field_path=f"rotation.{group_key}.stage",
        ),
        "logic": _normalize_text(
            payload.get("logic"),
            language_paths=language_paths,
            field_path=f"rotation.{group_key}.logic",
        ),
        "time_window": _normalize_text(
            payload.get("time_window"),
            fallback=DEFAULT_INVESTMENT_HORIZON,
            language_paths=language_paths,
            field_path=f"rotation.{group_key}.time_window",
        ),
        "advice": _normalize_text(
            payload.get("advice"),
            language_paths=language_paths,
            field_path=f"rotation.{group_key}.advice",
        ),
    }


def _normalize_heat_item(
    item: Any,
    group_key: str,
    *,
    language_paths: list[str],
) -> dict[str, Any]:
    payload = _as_dict(item)
    defaults = {
        "hottest": "最热",
        "heating": "升温",
        "cooling": "降温",
    }
    return {
        "sector": _normalize_text(
            _first_non_empty(payload.get("sector"), payload.get("name")),
            allow_english_only=True,
            language_paths=language_paths,
            field_path=f"heat.{group_key}.sector",
        ),
        "score": int(round(_normalize_number(payload.get("score"), 0.0))),
        "trend": _normalize_text(
            payload.get("trend"),
            fallback=defaults[group_key],
            language_paths=language_paths,
            field_path=f"heat.{group_key}.trend",
        ),
        "sustainability": _normalize_text(
            payload.get("sustainability"),
            fallback=DEFAULT_TEXT,
            language_paths=language_paths,
            field_path=f"heat.{group_key}.sustainability",
        ),
    }


def normalize_sector_strategy_predictions(raw_predictions: Any) -> dict[str, Any]:
    payload = _as_dict(raw_predictions)
    missing_fields: list[str] = []
    language_paths: list[str] = []
    parse_warning = ""
    raw_fallback_text = ""

    if not payload:
        parse_warning = "最终预测为空，已回退到默认结构。"
    if payload.get("prediction_text"):
        parse_warning = "最终预测未解析为结构化 JSON，已回退展示原始文本。"
        raw_fallback_text = str(payload.get("prediction_text") or "").strip()

    summary_payload = _as_dict(payload.get("summary"))
    summary: dict[str, str] = {}
    for key in SUMMARY_KEYS:
        normalized = _normalize_text(
            summary_payload.get(key),
            language_paths=language_paths,
            field_path=f"summary.{key}",
        )
        summary[key] = normalized
        if summary_payload.get(key) in (None, "", []):
            missing_fields.append(f"summary.{key}")

    long_short_payload = _as_dict(payload.get("long_short"))
    long_short: dict[str, list[dict[str, Any]]] = {}
    for group_key in LONG_SHORT_KEYS:
        items = _as_list(long_short_payload.get(group_key))
        if group_key not in long_short_payload:
            missing_fields.append(f"long_short.{group_key}")
        long_short[group_key] = [_normalize_long_short_item(item, group_key, language_paths=language_paths) for item in items]

    rotation_payload = _as_dict(payload.get("rotation"))
    rotation: dict[str, list[dict[str, Any]]] = {}
    for group_key in ROTATION_KEYS:
        items = _as_list(rotation_payload.get(group_key))
        if group_key not in rotation_payload:
            missing_fields.append(f"rotation.{group_key}")
        rotation[group_key] = [_normalize_rotation_item(item, group_key, language_paths=language_paths) for item in items]

    heat_payload = _as_dict(payload.get("heat"))
    heat: dict[str, list[dict[str, Any]]] = {}
    for group_key in HEAT_KEYS:
        items = _as_list(heat_payload.get(group_key))
        if group_key not in heat_payload:
            missing_fields.append(f"heat.{group_key}")
        heat[group_key] = [_normalize_heat_item(item, group_key, language_paths=language_paths) for item in items]

    if "long_short" not in payload:
        missing_fields.append("long_short")
    if "rotation" not in payload:
        missing_fields.append("rotation")
    if "heat" not in payload:
        missing_fields.append("heat")
    if "summary" not in payload:
        missing_fields.append("summary")
    if "confidence_score" not in payload:
        missing_fields.append("confidence_score")
    if "risk_level" not in payload:
        missing_fields.append("risk_level")
    if "market_outlook" not in payload:
        missing_fields.append("market_outlook")

    return {
        "long_short": long_short,
        "rotation": rotation,
        "heat": heat,
        "summary": summary,
        "confidence_score": _normalize_score(payload.get("confidence_score"), DEFAULT_CONFIDENCE_SCORE),
        "risk_level": _normalize_text(
            payload.get("risk_level"),
            fallback=DEFAULT_RISK_LEVEL,
            language_paths=language_paths,
            field_path="risk_level",
        ),
        "market_outlook": _normalize_text(
            payload.get("market_outlook"),
            fallback=DEFAULT_MARKET_OUTLOOK,
            language_paths=language_paths,
            field_path="market_outlook",
        ),
        "warnings": {
            "parse_warning": parse_warning,
            "language_warning": "；".join(dict.fromkeys(language_paths)) if language_paths else "",
            "missing_fields": list(dict.fromkeys(missing_fields)),
        },
        "raw_fallback_text": raw_fallback_text,
    }


def normalize_sector_strategy_reports(
    agents_analysis: Any,
    comprehensive_report: Any,
) -> dict[str, dict[str, Any] | None]:
    entries: dict[str, dict[str, Any] | None] = {key: None for key in RAW_REPORT_KEYS}

    for name, value in _as_dict(agents_analysis).items():
        payload = _as_dict(value)
        report_key = _detect_report_key(name, payload)
        if not report_key or entries.get(report_key):
            continue
        body, reasoning = _split_report_sections(
            _first_non_empty(payload.get("analysis"), payload.get("report"), value)
        )
        raw_body = body or _normalize_text(
            _first_non_empty(payload.get("analysis"), payload.get("report"), value),
            allow_english_only=True,
        )
        entries[report_key] = {
            "title": _normalize_text(payload.get("agent_name"), fallback=name, allow_english_only=True),
            "role": _normalize_text(payload.get("agent_role"), allow_english_only=True),
            "focus_areas": [str(item).strip() for item in _as_list(payload.get("focus_areas")) if str(item).strip()],
            "timestamp": _normalize_text(payload.get("timestamp"), fallback="", allow_english_only=True),
            "body": raw_body,
            "reasoning": reasoning,
            "summary": _build_summary(raw_body),
        }

    team_body, team_reasoning = _split_report_sections(comprehensive_report)
    if team_body or team_reasoning:
        entries["team"] = {
            "title": "综合研判",
            "role": "首席策略官综合判断",
            "focus_areas": ["宏观", "板块", "资金", "情绪"],
            "timestamp": "",
            "body": team_body or _normalize_text(comprehensive_report, allow_english_only=True),
            "reasoning": team_reasoning,
            "summary": _build_summary(team_body or comprehensive_report),
        }

    return entries


def _normalize_market_snapshot(data_summary: Any) -> dict[str, Any] | None:
    payload = _as_dict(data_summary)
    if not payload:
        return None
    sectors = _as_dict(payload.get("sectors"))
    concepts = _as_dict(payload.get("concepts"))
    market_overview = _as_dict(payload.get("market_overview"))
    if not sectors and not concepts and not market_overview and not payload.get("cache_warning"):
        return None
    return {
        "from_cache": bool(payload.get("from_cache")),
        "cache_warning": _normalize_text(payload.get("cache_warning"), fallback="", allow_english_only=True),
        "data_timestamp": _normalize_text(payload.get("data_timestamp"), fallback="", allow_english_only=True),
        "market_overview": market_overview,
        "sectors_count": len(sectors),
        "concepts_count": len(concepts),
    }


def _build_sector_strategy_headline(summary: dict[str, Any], bullish: list[dict[str, Any]]) -> str:
    headline_parts = [summary.get("market_view"), summary.get("key_opportunity")]
    headline = "；".join(part for part in headline_parts if part and part != DEFAULT_TEXT).strip("；")
    if headline:
        return headline
    names = [item.get("sector") for item in bullish if item.get("sector") and item.get("sector") != DEFAULT_TEXT]
    if names:
        return f"重点关注 {'、'.join(names[:3])}"
    return "智策板块分析报告"


def normalize_sector_strategy_result(raw_result: Any, data_summary: Any = None) -> dict[str, Any]:
    payload = _unwrap_sector_strategy_payload(raw_result)
    if not payload and isinstance(raw_result, dict) and isinstance(raw_result.get("report_view"), dict):
        return raw_result["report_view"]

    cache_meta = _as_dict(payload.get("cache_meta"))
    normalized_predictions = normalize_sector_strategy_predictions(payload.get("final_predictions"))
    market_snapshot = _normalize_market_snapshot(data_summary)
    reports = normalize_sector_strategy_reports(payload.get("agents_analysis"), payload.get("comprehensive_report"))

    meta = {
        "timestamp": _normalize_text(
            _first_non_empty(payload.get("timestamp"), payload.get("analysis_date"), payload.get("created_at")),
            fallback="",
            allow_english_only=True,
        ),
        "from_cache": bool(
            _first_non_empty(
                cache_meta.get("from_cache"),
                _as_dict(data_summary).get("from_cache") if isinstance(data_summary, dict) else None,
                False,
            )
        ),
        "cache_warning": _normalize_text(
            _first_non_empty(cache_meta.get("cache_warning"), _as_dict(data_summary).get("cache_warning")),
            fallback="",
            allow_english_only=True,
        ),
        "data_timestamp": _normalize_text(
            _first_non_empty(cache_meta.get("data_timestamp"), _as_dict(data_summary).get("data_timestamp")),
            fallback="",
            allow_english_only=True,
        ),
    }

    summary = {
        "headline": _build_sector_strategy_headline(
            normalized_predictions["summary"],
            normalized_predictions["long_short"]["bullish"],
        ),
        **normalized_predictions["summary"],
        "confidence_score": normalized_predictions["confidence_score"],
        "risk_level": normalized_predictions["risk_level"],
        "market_outlook": normalized_predictions["market_outlook"],
    }

    return {
        "meta": meta,
        "summary": summary,
        "predictions": {
            "long_short": normalized_predictions["long_short"],
            "rotation": normalized_predictions["rotation"],
            "heat": normalized_predictions["heat"],
            "raw_fallback_text": normalized_predictions["raw_fallback_text"],
        },
        "market_snapshot": market_snapshot,
        "raw_reports": reports,
        "warnings": normalized_predictions["warnings"],
    }


def build_sector_strategy_summary(source: Any) -> dict[str, Any]:
    report_view = source if isinstance(source, dict) and isinstance(source.get("summary"), dict) and "predictions" in source else normalize_sector_strategy_result(source)
    summary = _as_dict(report_view.get("summary"))
    predictions = _as_dict(report_view.get("predictions"))
    long_short = _as_dict(predictions.get("long_short"))
    bullish = [item.get("sector") for item in _as_list(long_short.get("bullish")) if isinstance(item, dict) and item.get("sector")]
    bearish = [item.get("sector") for item in _as_list(long_short.get("bearish")) if isinstance(item, dict) and item.get("sector")]
    neutral = [item.get("sector") for item in _as_list(long_short.get("neutral")) if isinstance(item, dict) and item.get("sector")]
    return {
        "headline": summary.get("headline") or "智策板块分析报告",
        "market_view": summary.get("market_view") or DEFAULT_TEXT,
        "key_opportunity": summary.get("key_opportunity") or DEFAULT_TEXT,
        "major_risk": summary.get("major_risk") or DEFAULT_TEXT,
        "strategy": summary.get("strategy") or DEFAULT_TEXT,
        "bullish": bullish[:3],
        "neutral": neutral[:3],
        "bearish": bearish[:3],
        "risk_level": summary.get("risk_level") or DEFAULT_RISK_LEVEL,
        "market_outlook": summary.get("market_outlook") or DEFAULT_MARKET_OUTLOOK,
        "confidence_score": summary.get("confidence_score") if isinstance(summary.get("confidence_score"), int) else DEFAULT_CONFIDENCE_SCORE,
    }


def derive_sector_strategy_recommended_sectors(source: Any) -> list[dict[str, Any]]:
    report_view = source if isinstance(source, dict) and "predictions" in source else normalize_sector_strategy_result(source)
    predictions = _as_dict(report_view.get("predictions"))
    long_short = _as_dict(predictions.get("long_short"))
    rotation = _as_dict(predictions.get("rotation"))
    heat = _as_dict(predictions.get("heat"))

    candidates = [
        ("看多主线", _as_list(long_short.get("bullish")), "reason", "confidence"),
        ("轮动潜力", _as_list(rotation.get("potential")), "logic", None),
        ("热度主线", _as_list(heat.get("hottest")), "trend", "score"),
    ]

    recommended: list[dict[str, Any]] = []
    seen: set[str] = set()
    for group_label, items, reason_key, confidence_key in candidates:
        for item in items:
            payload = _as_dict(item)
            sector_name = _normalize_text(payload.get("sector"), allow_english_only=True)
            if not sector_name or sector_name == DEFAULT_TEXT or sector_name in seen:
                continue
            seen.add(sector_name)
            recommended.append(
                {
                    "sector_name": sector_name,
                    "reason": _normalize_text(payload.get(reason_key), allow_english_only=True),
                    "confidence": payload.get(confidence_key) if confidence_key else DEFAULT_TEXT,
                    "type": group_label,
                }
            )
    return recommended


def derive_sector_strategy_investment_horizon(source: Any) -> str:
    report_view = source if isinstance(source, dict) and "predictions" in source else normalize_sector_strategy_result(source)
    rotation = _as_dict(_as_dict(report_view.get("predictions")).get("rotation"))
    for group_key in ("current_strong", "potential", "declining"):
        for item in _as_list(rotation.get(group_key)):
            if isinstance(item, dict) and item.get("time_window"):
                return _normalize_text(item.get("time_window"), fallback=DEFAULT_INVESTMENT_HORIZON, allow_english_only=True)
    return DEFAULT_INVESTMENT_HORIZON


def normalize_sector_strategy_export_payload(result: Any, data_summary: Any = None) -> dict[str, Any]:
    payload = _unwrap_sector_strategy_payload(result)
    report_view = normalize_sector_strategy_result(payload, data_summary=data_summary)
    predictions = _as_dict(report_view.get("predictions"))
    summary = _as_dict(report_view.get("summary"))
    export_predictions = {
        "long_short": _as_dict(predictions.get("long_short")),
        "rotation": _as_dict(predictions.get("rotation")),
        "heat": _as_dict(predictions.get("heat")),
        "summary": {
            "market_view": summary.get("market_view"),
            "key_opportunity": summary.get("key_opportunity"),
            "major_risk": summary.get("major_risk"),
            "strategy": summary.get("strategy"),
        },
        "confidence_score": summary.get("confidence_score"),
        "risk_level": summary.get("risk_level"),
        "market_outlook": summary.get("market_outlook"),
    }
    raw_fallback_text = predictions.get("raw_fallback_text")
    if raw_fallback_text:
        export_predictions["prediction_text"] = raw_fallback_text

    normalized_payload = dict(payload)
    normalized_payload["timestamp"] = report_view.get("meta", {}).get("timestamp") or normalized_payload.get("timestamp")
    normalized_payload["final_predictions"] = export_predictions
    normalized_payload["report_view"] = report_view
    normalized_payload["agents_analysis"] = _as_dict(payload.get("agents_analysis"))
    normalized_payload["comprehensive_report"] = payload.get("comprehensive_report") or ""
    normalized_payload["recommended_sectors"] = derive_sector_strategy_recommended_sectors(report_view)
    return normalized_payload
