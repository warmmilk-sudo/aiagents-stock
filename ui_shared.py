#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared pure helpers for analysis content normalization and formatting."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional


STRUCTURED_FINAL_DECISION_KEYS = (
    "rating",
    "confidence_level",
    "target_price",
    "operation_advice",
    "entry_range",
    "entry_min",
    "entry_max",
    "take_profit",
    "stop_loss",
    "holding_period",
    "position_size",
    "risk_warning",
)

FINAL_DECISION_FIELD_LABELS = {
    "rating": "投资评级",
    "confidence_level": "信心度",
    "target_price": "目标价格",
    "operation_advice": "操作建议",
    "entry_range": "进场区间",
    "entry_min": "进场下沿",
    "entry_max": "进场上沿",
    "take_profit": "止盈位",
    "stop_loss": "止损位",
    "holding_period": "持有周期",
    "position_size": "建议仓位",
    "risk_warning": "风险提示",
    "decision_text": "决策文本",
}

REPORT_BODY_PRIORITY_KEYS = (
    "report",
    "report_body",
    "body",
    "content",
    "content_text",
    "analysis",
    "summary",
    "conclusion",
    "recommendation",
    "discussion_result",
    "comprehensive_report",
    "prediction_text",
    "decision_text",
    "text",
    "message",
)

REPORT_METADATA_KEYS = {
    "agent_name",
    "agent_role",
    "focus_areas",
    "analysis_time",
    "timestamp",
    "rating",
    "confidence_level",
    "target_price",
    "entry_range",
    "entry_min",
    "entry_max",
    "take_profit",
    "stop_loss",
    "holding_period",
    "position_size",
    "risk_warning",
}

REPORT_START_PATTERNS = (
    r"(?m)^#\s*.+(?:分析报告|报告|深度分析|分析).*$",
    r"(?m)^##\s*基本概况.*$",
    r"(?m)^##\s*.+$",
    r"(?m)^(?:一、|1[\.、])\s*(?:趋势分析|基本概况|核心结论|技术分析|投资建议|市场分析|新闻分析|风险分析|资金分析).*$",
    r"(?m)^(?:##\s*)?(?:一、|1[\.、])\s*(?:周期仪表盘|康波周期仪表盘|综合资产配置建议|不同人群的具体建议|核心观点总结|周金涛名言对照).*$",
    r"(?m)^以下(?:为|是).*(?:分析报告|报告|研判|复盘).*$",
    r"(?m)^整体结论先行[:：]?\s*$",
    r"(?m)^\*\*(?:核心判断|核心结论|总体判断).*\*\*$",
)

REPORT_BODY_MARKERS = (
    r"(?m)^\s*(?:以下|下面)是(?:最终)?(?:正式)?(?:分析)?报告[:：]?\s*$",
    r"[\[【]?(?:报告正文|正文内容|分析正文|最终报告|正式报告)[\]】]?[:：]?\s*",
)

PREAMBLE_LINE_PATTERNS = (
    r"^(?:好的|下面|以下|基于|根据|综合|结合|我将|我会|先对|接下来|这里是)",
    r"(?:分析报告如下|正式报告如下|报告如下|为你提供|为您提供)",
)


def get_dataframe_height(
    row_count: int,
    *,
    min_rows: int = 4,
    max_rows: int = 50,
    row_px: int = 35,
    header_px: int = 38,
    padding_px: int = 8,
) -> int:
    """Return a practical table height based on row count."""
    try:
        rows = max(int(row_count), 0)
    except (TypeError, ValueError):
        rows = 0

    visible_rows = min(max(rows, min_rows), max_rows)
    return header_px + visible_rows * row_px + padding_px


def _coerce_json_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return ""

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _normalize_mapping_input(value: Any) -> tuple[Dict[str, Any], bool]:
    if not value:
        return {}, False

    if isinstance(value, dict):
        return value, False

    parsed = _coerce_json_value(value)
    if isinstance(parsed, dict):
        return parsed, False

    return {}, True


def _normalize_agents_results(value: Any) -> tuple[Dict[str, Dict[str, Any]], bool]:
    normalized, invalid = _normalize_mapping_input(value)
    if not normalized:
        return {}, invalid

    result: Dict[str, Dict[str, Any]] = {}
    had_invalid_entry = False

    for key, agent_result in normalized.items():
        parsed = _coerce_json_value(agent_result)
        if isinstance(parsed, dict):
            result[str(key)] = parsed
            continue

        if parsed in (None, ""):
            had_invalid_entry = True
            continue

        had_invalid_entry = True
        result[str(key)] = {
            "agent_name": str(key),
            "analysis": str(parsed),
        }

    return result, invalid or had_invalid_entry


def _normalize_text_or_mapping(value: Any) -> tuple[Any, bool]:
    if not value:
        return None, False

    if isinstance(value, dict):
        return value, False

    parsed = _coerce_json_value(value)
    if isinstance(parsed, (dict, str)):
        return parsed, False

    if parsed in (None, ""):
        return None, False

    return json.dumps(parsed, ensure_ascii=False, default=str), True


def _normalize_discussion_result(value: Any) -> Any:
    if not value:
        return ""

    parsed = _coerce_json_value(value)
    if parsed in (None, ""):
        return ""

    if isinstance(parsed, str):
        parsed = re.sub(r"^\s*[【\[]?推理过程[】\]]?\s*", "", parsed, count=1)
        parsed = re.sub(r"^\s*推理过程[:：]\s*", "", parsed, count=1)
        parsed = parsed.strip()

    return parsed


def _extract_embedded_json_mapping(text: str) -> tuple[Dict[str, Any], str]:
    if not text:
        return {}, ""

    decoder = json.JSONDecoder()
    candidate_mapping: Dict[str, Any] = {}
    candidate_prefix = ""

    for index, char in enumerate(text):
        if char != "{":
            continue

        try:
            parsed, end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue

        trailing = text[index + end :].strip()
        if trailing:
            continue
        if isinstance(parsed, dict):
            candidate_mapping = parsed
            candidate_prefix = text[:index].strip()

    return candidate_mapping, candidate_prefix


def _resolve_final_decision_content(final_decision: Any) -> tuple[Any, bool, str]:
    normalized_final_decision, invalid = _normalize_text_or_mapping(final_decision)
    extracted_reasoning = ""

    if isinstance(normalized_final_decision, dict):
        has_structured_keys = any(
            key in normalized_final_decision for key in STRUCTURED_FINAL_DECISION_KEYS
        )
        decision_text = str(normalized_final_decision.get("decision_text") or "").strip()
        if decision_text and not has_structured_keys:
            embedded_mapping, reasoning_prefix = _extract_embedded_json_mapping(decision_text)
            if embedded_mapping:
                extracted_reasoning = reasoning_prefix
                return embedded_mapping, invalid, extracted_reasoning

    return normalized_final_decision, invalid, extracted_reasoning


def _find_report_body_start(text: str) -> Optional[int]:
    positions = []
    for pattern in REPORT_START_PATTERNS:
        match = re.search(pattern, text)
        if match:
            positions.append(match.start())
    if not positions:
        return None
    return min(positions)


def _clean_reasoning_label(text: str) -> str:
    return (
        text
        .replace("【推理过程】", "")
        .replace("[推理过程]", "")
        .replace("【思考过程】", "")
        .replace("[思考过程]", "")
        .replace("【分析过程】", "")
        .replace("[分析过程]", "")
        .replace("【推演过程】", "")
        .replace("[推演过程]", "")
        .strip()
    )


def _clean_body_label(text: str) -> str:
    cleaned = re.sub(r"^\s*[\[【]?(?:报告正文|正文内容|分析正文|最终报告|正式报告)[\]】]?\s*", "", text, count=1)
    cleaned = re.sub(r"^\s*(?:以下|下面)是(?:最终)?(?:正式)?(?:分析)?报告[:：]\s*", "", cleaned, count=1)
    return cleaned.strip()


def _find_body_marker(text: str) -> Optional[re.Match[str]]:
    for pattern in REPORT_BODY_MARKERS:
        match = re.search(pattern, text)
        if match:
            return match
    return None


def _is_preamble_line(line: str) -> bool:
    normalized = line.strip()
    if not normalized:
        return True
    return any(re.search(pattern, normalized) for pattern in PREAMBLE_LINE_PATTERNS)


def _split_leading_preamble(text: str) -> tuple[str, str]:
    report_start = _find_report_body_start(text)
    if report_start is None or report_start <= 0:
        return text.strip(), ""

    preamble = text[:report_start].strip()
    body = text[report_start:].strip()
    if not preamble:
        return body, ""

    preamble_lines = [line.strip() for line in preamble.splitlines() if line.strip()]
    if len(preamble) <= 220 or all(_is_preamble_line(line) for line in preamble_lines):
        return body, preamble

    return text.strip(), ""


def _split_analysis_report_sections(value: Any) -> tuple[str, str]:
    parsed = _coerce_json_value(value)
    text = "" if parsed is None else str(parsed).strip()
    if not text:
        return "", ""

    reasoning_parts: list[str] = []

    def _collect_think(match: re.Match[str]) -> str:
        content = match.group(1).strip()
        if content:
            reasoning_parts.append(content)
        return ""

    text = re.sub(r"<think>([\s\S]*?)</think>", _collect_think, text, flags=re.IGNORECASE).strip()

    body_marker = _find_body_marker(text)
    if body_marker:
        before_marker = text[: body_marker.start()].strip()
        after_marker = _clean_body_label(text[body_marker.end() :])
        body, preamble = _split_leading_preamble(after_marker)
        reasoning = "\n\n".join(part for part in ["\n\n".join(reasoning_parts).strip(), before_marker, preamble] if part).strip()
        return body, reasoning

    marker = re.search(
        r"(?m)[\[【](?:推理过程|思考过程|分析过程|推演过程)[\]】]|^\s*(?:推理过程|思考过程|分析过程|推演过程)[:：]",
        text,
    )
    if not marker:
        body, preamble = _split_leading_preamble(text)
        reasoning = "\n\n".join(part for part in ["\n\n".join(reasoning_parts).strip(), preamble] if part).strip()
        return body, reasoning

    before_marker = text[:marker.start()].strip()
    after_marker = _clean_reasoning_label(text[marker.end() :].lstrip("：:\n ").strip())

    if before_marker:
        body = before_marker
        reasoning = after_marker
    else:
        report_start = _find_report_body_start(after_marker)
        if report_start is not None and report_start > 0:
            reasoning = after_marker[:report_start].strip()
            body = after_marker[report_start:].strip()
        else:
            body = after_marker if report_start == 0 else ""
            reasoning = after_marker

    body = re.sub(r"^\s*分析报告(?:正文)?\s*[:：]\s*", "", body, count=1)
    reasoning = re.sub(r"^\s*[\[【]?推理过程[\]】]?\s*", "", reasoning, count=1)
    reasoning = re.sub(r"^\s*推理过程[:：]\s*", "", reasoning, count=1)
    reasoning = "\n\n".join(part for part in ["\n\n".join(reasoning_parts).strip(), reasoning] if part).strip()
    return body.strip(), reasoning.strip()


def to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        text = str(value).replace(",", "").strip()
        if not text:
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        return float(match.group(0))
    except (TypeError, ValueError):
        return None


def format_price(value: Any, precision: int = 2, currency: str = "¥") -> str:
    number = to_float(value)
    if number is None:
        return "N/A"
    return f"{currency}{number:.{precision}f}"


def format_entry_range(entry_min: Any, entry_max: Any, *, precision: int = 2, currency: str = "¥") -> str:
    min_value = to_float(entry_min)
    max_value = to_float(entry_max)
    if min_value is None and max_value is None:
        return "N/A"
    if min_value is None:
        return format_price(max_value, precision=precision, currency=currency)
    if max_value is None:
        return format_price(min_value, precision=precision, currency=currency)
    return (
        f"{format_price(min_value, precision=precision, currency=currency)} - "
        f"{format_price(max_value, precision=precision, currency=currency)}"
    )


def _is_empty_display_value(value: Any) -> bool:
    parsed = _coerce_json_value(value)
    if parsed is None:
        return True
    if isinstance(parsed, str):
        return parsed.strip() in {"", "N/A", "NA", "None", "null"}
    if isinstance(parsed, (list, tuple, set, dict)):
        return len(parsed) == 0
    return False


def _humanize_field_label(key: str) -> str:
    text = str(key or "").strip()
    if not text:
        return ""
    return FINAL_DECISION_FIELD_LABELS.get(text, text.replace("_", " ").strip().title())


def _format_display_value(key: str, value: Any) -> Any:
    parsed = _coerce_json_value(value)
    if _is_empty_display_value(parsed):
        return "N/A"

    if key == "confidence_level":
        number = to_float(parsed)
        return f"{number:.1f}/10" if number is not None else str(parsed)

    if key in {"target_price", "take_profit", "stop_loss", "entry_min", "entry_max"}:
        number = to_float(parsed)
        return format_price(number) if number is not None else str(parsed)

    if key == "entry_range":
        if isinstance(parsed, dict):
            return format_entry_range(parsed.get("min"), parsed.get("max"))
        if isinstance(parsed, (list, tuple)) and len(parsed) >= 2:
            return format_entry_range(parsed[0], parsed[1])
        return str(parsed)

    if isinstance(parsed, bool):
        return "是" if parsed else "否"

    return parsed


def _has_structured_decision_fields(value: Any) -> bool:
    mapping, _ = _normalize_mapping_input(value)
    if not mapping:
        return False
    return any(
        key != "decision_text" and key in mapping and not _is_empty_display_value(mapping.get(key))
        for key in STRUCTURED_FINAL_DECISION_KEYS
    )


def _deduplicate_text_segments(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = re.sub(r"\s+", " ", text)
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result


def _format_report_scalar(key: str, value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    label = _humanize_field_label(key)
    if key in REPORT_BODY_PRIORITY_KEYS or key in {"title", "heading"}:
        return text
    if "\n" in text or len(text) >= 120:
        return f"#### {label}\n\n{text}" if label else text
    if label:
        return f"**{label}：** {text}"
    return text


def _collect_report_text_segments(value: Any, *, depth: int = 0) -> list[str]:
    parsed = _coerce_json_value(value)
    if parsed in (None, ""):
        return []

    if isinstance(parsed, str):
        return [parsed.strip()]

    if isinstance(parsed, dict):
        prioritized_segments: list[str] = []
        prioritized_keys_seen: set[str] = set()
        for key in REPORT_BODY_PRIORITY_KEYS:
            if key not in parsed:
                continue
            nested_segments = _collect_report_text_segments(parsed.get(key), depth=depth + 1)
            if nested_segments:
                prioritized_segments.extend(nested_segments)
                prioritized_keys_seen.add(key)

        fallback_segments: list[str] = []
        for key, item in parsed.items():
            if key in REPORT_METADATA_KEYS or key in prioritized_keys_seen:
                continue
            nested_value = _coerce_json_value(item)
            if nested_value in (None, ""):
                continue
            if isinstance(nested_value, str):
                fallback_segments.append(_format_report_scalar(str(key), nested_value))
                continue
            if isinstance(nested_value, (dict, list, tuple, set)):
                fallback_segments.extend(
                    _collect_report_text_segments(nested_value, depth=depth + 1)
                )
        return _deduplicate_text_segments(prioritized_segments + fallback_segments)

    if isinstance(parsed, (list, tuple, set)):
        segments: list[str] = []
        for item in parsed:
            segments.extend(_collect_report_text_segments(item, depth=depth + 1))
        return _deduplicate_text_segments(segments)

    return [str(parsed)]


def _resolve_report_body_text(value: Any) -> str:
    parsed = _coerce_json_value(value)
    segments = _collect_report_text_segments(parsed)
    if segments:
        return "\n\n".join(segments).strip()

    if isinstance(parsed, (dict, list, tuple, set)):
        return json.dumps(parsed, ensure_ascii=False, indent=2, default=str)
    if parsed in (None, ""):
        return ""
    return str(parsed).strip()
