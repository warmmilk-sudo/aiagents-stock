"""
智能盯盘 - DeepSeek AI 决策引擎
适配A股T+1交易规则的AI决策系统
"""

import ast
import json
import logging
import math
import re
from typing import Any, Dict, List, Optional
from datetime import date, datetime, time, timedelta
import time as time_module

import pytz
import requests

import config
from investment_action_utils import normalize_condition_list, normalize_strategy_context, normalize_swing_type, swing_type_label
from investment_db_utils import DEFAULT_ACCOUNT_NAME
from model_routing import ModelTier, resolve_model_name
from prompt_registry import build_messages, render_prompt


class DecisionValidationError(ValueError):
    """Structured decision payload is internally inconsistent."""


class SmartMonitorDeepSeek:
    """A股智能盯盘 - DeepSeek AI决策引擎"""

    OPTIONAL_SECTION_PRIORITY_HEADERS = (
        "[INTRADAY_FLOW]",
        "[STRATEGY_CONTEXT]",
    )
    MAX_STRATEGY_SUMMARY_CHARS = 240
    MAX_SEMANTIC_LABELS = 8
    PROMPT_SIZE_WARNING_CHARS = 22000
    PROMPT_SIZE_SAFETY_MARGIN_CHARS = 0
    PROMPT_BUILD_TARGET_CHARS = 6200
    MAX_OPTIONAL_SECTION_REASONING_CHARS = 180
    MAX_OPTIONAL_SECTION_SUMMARY_CHARS = 120
    MAX_OPTIONAL_SECTION_DELTA_CHARS = 120

    SYSTEM_TEMPLATE = "smart_monitor/intraday_decision.system.txt"
    USER_TEMPLATE = "smart_monitor/intraday_decision.user.txt"
    SECTION_TIMER_TEMPLATE = "smart_monitor/sections/timer.txt"
    SECTION_DATA_SCOPE_TEMPLATE = "smart_monitor/sections/data_scope.txt"
    SECTION_REALTIME_FRESHNESS_TEMPLATE = "smart_monitor/sections/realtime_freshness.txt"
    SECTION_STOCK_TEMPLATE = "smart_monitor/sections/stock.txt"
    SECTION_TECHNICAL_TEMPLATE = "smart_monitor/sections/technical.txt"
    SECTION_VOLUME_TEMPLATE = "smart_monitor/sections/volume.txt"
    SECTION_EXECUTION_CONTEXT_TEMPLATE = "smart_monitor/sections/execution_context.txt"
    SECTION_ACCOUNT_RISK_PROFILE_TEMPLATE = "smart_monitor/sections/account_risk_profile.txt"
    SECTION_INTRADAY_FLOW_TEMPLATE = "smart_monitor/sections/intraday_flow.txt"
    SECTION_STRATEGY_CONTEXT_TEMPLATE = "smart_monitor/sections/strategy_context.txt"
    SECTION_AI_PATTERN_RECOGNITION_TEMPLATE = "smart_monitor/sections/ai_pattern_recognition.txt"
    SECTION_POSITION_HOLDING_TEMPLATE = "smart_monitor/sections/position_holding.txt"
    SECTION_POSITION_EMPTY_TEMPLATE = "smart_monitor/sections/position_empty.txt"

    def __init__(self, api_key: str, base_url: str = None, model: str = None,
                 lightweight_model: str = None, reasoning_model: str = None):
        """
        初始化LLM客户端

        Args:
            api_key: LLM API密钥
        """
        self.api_key = api_key
        self.base_url = base_url or config.WARMMILK_BASE_URL
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        self.logger = logging.getLogger(__name__)
        self.http_timeout_seconds = max(
            15,
            int(getattr(config, "SMART_MONITOR_HTTP_TIMEOUT_SECONDS", 30) or 30),
        )
        self.http_retry_count = max(
            0,
            int(getattr(config, "SMART_MONITOR_HTTP_RETRY_COUNT", 1) or 1),
        )
        self.reasoning_max_tokens = max(
            1500,
            int(getattr(config, "SMART_MONITOR_REASONING_MAX_TOKENS", 3000) or 3000),
        )
        self.decision_repair_attempts = max(
            0,
            int(getattr(config, "SMART_MONITOR_DECISION_REPAIR_ATTEMPTS", 1) or 1),
        )

    def _resolve_request_credentials(self, model_name: str) -> tuple[str, str]:
        api_key, base_url = config.get_model_api_credentials(model_name)
        if not api_key:
            api_key = self.api_key
        if not base_url:
            base_url = self.base_url
        return api_key, base_url

    def set_model_overrides(self, model: str = None,
                            lightweight_model: str = None,
                            reasoning_model: str = None) -> None:
        """更新当前会话的模型覆盖配置。"""
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model

    @staticmethod
    def _truncate_prompt_text(value: object, limit: int, suffix: str = "…（已截断）") -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if len(text) <= limit:
            return text
        if limit <= len(suffix):
            return suffix[:limit]
        return text[: limit - len(suffix)].rstrip() + suffix

    @staticmethod
    def _is_missing_prompt_value(value: object) -> bool:
        text = str(value or "").strip()
        return text in {"", "N/A", "[N/A]"}

    @classmethod
    def _optional_prompt_value(cls, value: object) -> str:
        return "" if cls._is_missing_prompt_value(value) else str(value).strip()

    @classmethod
    def _join_prompt_values(cls, *values: object, separator: str = " | ") -> str:
        parts = [cls._optional_prompt_value(value) for value in values]
        return separator.join(part for part in parts if part)

    def _build_strategy_summary_brief(self, strategy_context: Dict[str, Any]) -> str:
        def _clean_text(value: object) -> str:
            text = str(value or "").strip()
            if not text:
                return ""
            text = re.sub(r"<p[^>]*>", " ", text, flags=re.IGNORECASE)
            text = re.sub(r"</p>", " ", text, flags=re.IGNORECASE)
            text = re.sub(r"<[^>]+>", " ", text)
            text = re.sub(r"<think>.*?</think>", " ", text, flags=re.IGNORECASE | re.DOTALL)
            text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
            text = re.sub(r"【推理过程】.*", " ", text, flags=re.DOTALL)
            text = re.sub(r"推理过程[:：].*", " ", text, flags=re.DOTALL)
            text = re.sub(r"\s+", " ", text).strip()
            return text

        final_decision = strategy_context.get("final_decision")
        final_decision = final_decision if isinstance(final_decision, dict) else {}
        candidates = [
            strategy_context.get("summary"),
            final_decision.get("operation_advice"),
            final_decision.get("advice"),
            final_decision.get("summary"),
            final_decision.get("risk_warning"),
        ]

        for candidate in candidates:
            cleaned = _clean_text(candidate)
            if not cleaned:
                continue
            if len(cleaned) <= self.MAX_STRATEGY_SUMMARY_CHARS:
                return cleaned

            fragments = [
                fragment.strip(" ；;，,")
                for fragment in re.split(r"(?<=[。！？；;])|\s+\d+[.)、]\s*", cleaned)
                if fragment and fragment.strip(" ；;，,")
            ]
            brief_parts: List[str] = []
            current_length = 0
            for fragment in fragments:
                next_fragment = fragment
                if current_length + len(next_fragment) > self.MAX_STRATEGY_SUMMARY_CHARS - 8:
                    break
                brief_parts.append(next_fragment)
                current_length += len(next_fragment)
                if len(brief_parts) >= 3:
                    break
            if brief_parts:
                return " ".join(brief_parts).strip()
            return self._truncate_prompt_text(cleaned, self.MAX_STRATEGY_SUMMARY_CHARS)

        return "N/A"

    @staticmethod
    def _estimate_messages_payload_chars(messages: List[Dict[str, str]]) -> int:
        return len(json.dumps(messages, ensure_ascii=False))

    @staticmethod
    def _estimate_request_payload_chars(
        messages: List[Dict[str, str]],
        *,
        model: str = "gemini-3-flash",
        temperature: float = 0.1,
        max_tokens: int = 1600,
        top_p: Optional[float] = None,
    ) -> int:
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        effective_top_p = config.LLM_DEFAULT_TOP_P if top_p is None else float(top_p)
        if effective_top_p < 1.0:
            payload["top_p"] = effective_top_p
        return len(json.dumps(payload, ensure_ascii=False))

    @staticmethod
    def _parse_prompt_date(value: object) -> Optional[date]:
        text = str(value or "").strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        for candidate in (normalized, text):
            try:
                return datetime.fromisoformat(candidate).date()
            except ValueError:
                continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    @classmethod
    def _resolve_prompt_reference_date(cls, market_data: Optional[Dict[str, Any]]) -> Optional[date]:
        realtime_freshness = (
            market_data.get("realtime_freshness")
            if isinstance(market_data, dict) and isinstance(market_data.get("realtime_freshness"), dict)
            else {}
        )
        candidates: List[object] = []
        if isinstance(realtime_freshness, dict):
            candidates.append(realtime_freshness.get("asof_time"))
        if isinstance(market_data, dict):
            candidates.extend(
                [
                    market_data.get("update_time"),
                    market_data.get("trade_date"),
                    market_data.get("date"),
                ]
            )
        for candidate in candidates:
            parsed = cls._parse_prompt_date(candidate)
            if parsed:
                return parsed
        return None

    @staticmethod
    def _count_weekday_holding_days(start_date: date, end_date: date) -> Optional[int]:
        if end_date < start_date:
            return None
        total = 0
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                total += 1
            current += timedelta(days=1)
        return max(total, 1)

    @classmethod
    def _estimate_holding_trading_days(
        cls,
        *,
        position_date: Optional[str],
        market_data: Optional[Dict[str, Any]],
    ) -> Optional[int]:
        start_date = cls._parse_prompt_date(position_date)
        reference_date = cls._resolve_prompt_reference_date(market_data)
        if start_date is None or reference_date is None:
            return None
        return cls._count_weekday_holding_days(start_date, reference_date)

    @staticmethod
    def _coerce_optional_float(value: Any) -> Optional[float]:
        try:
            if value in (None, ""):
                return None
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        return numeric

    @staticmethod
    def _coerce_optional_bool(value: Any) -> Optional[bool]:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        normalized = str(value).strip().lower()
        if normalized in {"true", "1", "yes", "y", "on", "是"}:
            return True
        if normalized in {"false", "0", "no", "n", "off", "否"}:
            return False
        return None

    @staticmethod
    def _normalize_structure_state(value: Any) -> str:
        text = str(value or "").strip()
        aliases = {
            "宽幅震荡洗盘": "宽幅震荡洗盘",
            "洗盘": "宽幅震荡洗盘",
            "趋势突破确认": "趋势突破确认",
            "突破确认": "趋势突破确认",
            "主升加速段": "主升加速段",
            "主升": "主升加速段",
            "筑顶高位派发": "筑顶高位派发",
            "高位派发": "筑顶高位派发",
        }
        return aliases.get(text, "结构状态未明确")

    @staticmethod
    def _normalize_feature_beacons(value: Any) -> List[str]:
        allowed = (
            "limit_up_hit",
            "consecutive_limit_up",
            "stage_new_high",
            "breakout_20d_high_with_2x_volume",
            "low_volume_pullback_above_ma20",
        )
        if isinstance(value, dict):
            raw_items = [key for key, enabled in value.items() if enabled]
        elif isinstance(value, list):
            raw_items = value
        else:
            raw_items = [segment.strip() for segment in str(value or "").split(",") if segment.strip()]
        normalized: List[str] = []
        for item in raw_items:
            beacon = str(item or "").strip()
            if beacon in allowed and beacon not in normalized:
                normalized.append(beacon)
        return normalized

    @classmethod
    def _resolve_effective_swing_type_code(
        cls,
        strategy_context: Optional[Dict[str, Any]],
        decision: Optional[Dict[str, Any]] = None,
    ) -> str:
        upgraded = normalize_swing_type((decision or {}).get("upgraded_swing_type"))
        if upgraded:
            return upgraded
        return normalize_swing_type((strategy_context or {}).get("swing_type_code") or (strategy_context or {}).get("swing_type"))

    @classmethod
    def _select_trend_anchor(
        cls,
        market_data: Optional[Dict[str, Any]],
    ) -> tuple[str, Optional[float]]:
        atr14_pct = cls._coerce_optional_float((market_data or {}).get("atr14_pct"))
        anchor_type = "MA20" if atr14_pct is not None and atr14_pct > 4.0 else "MA10"
        anchor_key = "ma20" if anchor_type == "MA20" else "ma10"
        anchor_value = cls._coerce_optional_float((market_data or {}).get(anchor_key))
        return anchor_type, anchor_value

    @classmethod
    def _derive_feature_beacons(cls, market_data: Optional[Dict[str, Any]]) -> List[str]:
        market_data = market_data if isinstance(market_data, dict) else {}
        current_price = cls._coerce_optional_float(market_data.get("current_price"))
        change_pct = cls._coerce_optional_float(market_data.get("change_pct"))
        ma20 = cls._coerce_optional_float(market_data.get("ma20"))
        volume_ratio = cls._coerce_optional_float(
            market_data.get("volume_ratio") if market_data.get("volume_ratio") not in (None, "") else market_data.get("volume_ratio_vs_vol_ma5")
        )
        prev_20d_high = cls._coerce_optional_float(market_data.get("prev_20d_high"))
        prev_60d_high = cls._coerce_optional_float(market_data.get("prev_60d_high"))
        limit_up_streak = int(cls._coerce_optional_float(market_data.get("recent_limit_up_streak")) or 0)

        beacons: List[str] = []
        if change_pct is not None and change_pct >= 9.7:
            beacons.append("limit_up_hit")
        if limit_up_streak >= 2 or ("limit_up_hit" in beacons and limit_up_streak >= 1):
            beacons.append("consecutive_limit_up")
        if current_price is not None and prev_60d_high is not None and current_price >= prev_60d_high:
            beacons.append("stage_new_high")
        if (
            current_price is not None
            and prev_20d_high is not None
            and current_price >= prev_20d_high
            and volume_ratio is not None
            and volume_ratio >= 2.0
        ):
            beacons.append("breakout_20d_high_with_2x_volume")
        if (
            current_price is not None
            and ma20 is not None
            and current_price > ma20
            and change_pct is not None
            and change_pct < 0
            and volume_ratio is not None
            and volume_ratio <= 0.8
        ):
            beacons.append("low_volume_pullback_above_ma20")
        return beacons

    @classmethod
    def _calculate_atr_stop_floor(
        cls,
        *,
        current_price: Optional[float],
        atr14: Optional[float],
        swing_type_code: str,
    ) -> Optional[float]:
        if current_price is None or atr14 is None:
            return None
        multiplier = 2.5 if swing_type_code == "standard_swing" else 1.2
        return round(float(current_price) - float(atr14) * multiplier, 3)

    @classmethod
    def _derive_position_runtime_metrics(
        cls,
        *,
        market_data: Optional[Dict[str, Any]],
        strategy_context: Optional[Dict[str, Any]],
        holding_days: Optional[int],
        profit_loss_pct: Optional[float],
    ) -> Dict[str, Any]:
        market_data = market_data if isinstance(market_data, dict) else {}
        strategy_context = strategy_context if isinstance(strategy_context, dict) else {}
        feature_beacons = cls._derive_feature_beacons(market_data)
        anchor_type, anchor_value = cls._select_trend_anchor(market_data)
        swing_type_code = cls._resolve_effective_swing_type_code(strategy_context)
        atr14 = cls._coerce_optional_float(market_data.get("atr14"))
        atr14_pct = cls._coerce_optional_float(market_data.get("atr14_pct"))
        current_price = cls._coerce_optional_float(market_data.get("current_price"))
        atr_stop_floor = cls._calculate_atr_stop_floor(
            current_price=current_price,
            atr14=atr14,
            swing_type_code=swing_type_code,
        )
        prior_state = cls._normalize_structure_state(strategy_context.get("structure_state"))
        trend_following_active = bool(
            swing_type_code == "standard_swing"
            and holding_days is not None
            and holding_days >= 10
            and profit_loss_pct is not None
            and atr14_pct is not None
            and profit_loss_pct >= atr14_pct * 2
            and current_price is not None
            and anchor_value is not None
            and current_price >= anchor_value
            and prior_state != "筑顶高位派发"
        )
        return {
            "feature_beacons": feature_beacons,
            "trend_anchor_type": anchor_type,
            "trend_anchor_value": anchor_value,
            "atr14": atr14,
            "atr14_pct": atr14_pct,
            "atr_stop_floor": atr_stop_floor,
            "trend_following_active": trend_following_active,
        }

    @classmethod
    def _compact_system_prompt(cls, text: str, max_chars: int) -> str:
        prompt = re.sub(r"\n{3,}", "\n\n", str(text or "").strip())
        if not prompt or len(prompt) <= max_chars:
            return prompt

        sections = [section.strip() for section in prompt.split("\n\n") if section.strip()]
        removable_headers = [
            "返回 JSON 结构：",
            "confidence 标尺：",
            "risk_level 标尺：",
            "action_ratio_pct 要求：",
        ]
        for header in removable_headers:
            matched_index = next((idx for idx, section in enumerate(sections) if section.startswith(header)), None)
            if matched_index is None:
                continue
            del sections[matched_index]
            compacted = "\n\n".join(sections).strip()
            if len(compacted) <= max_chars:
                return compacted

        compacted = "\n\n".join(sections).strip()
        return cls._truncate_prompt_text(compacted, max_chars, suffix="\n…")

    @classmethod
    def _fit_optional_sections_to_budget(cls, sections: List[str], max_chars: int) -> str:
        if max_chars <= 0:
            return ""

        normalized_sections = [str(raw_section or "").strip() for raw_section in sections if str(raw_section or "").strip()]
        if not normalized_sections:
            return ""

        prioritized_sections: List[str] = []
        consumed_indexes = set()
        for header in cls.OPTIONAL_SECTION_PRIORITY_HEADERS:
            for index, section in enumerate(normalized_sections):
                if index in consumed_indexes or not section.startswith(header):
                    continue
                prioritized_sections.append(section)
                consumed_indexes.add(index)
        for index, section in enumerate(normalized_sections):
            if index in consumed_indexes:
                continue
            prioritized_sections.append(section)

        kept_sections: List[str] = []
        current_length = 0
        for section in prioritized_sections:
            delimiter_length = 2 if kept_sections else 0
            available = max_chars - current_length - delimiter_length
            if available <= 0:
                break

            if len(section) > available:
                lines = section.splitlines()
                if len(lines) > 1:
                    header = lines[0].strip()
                    body = "\n".join(lines[1:]).strip()
                    body_budget = max(0, available - len(header) - 1)
                    if body_budget <= 0:
                        section = cls._truncate_prompt_text(header, available, suffix="…")
                    else:
                        section = f"{header}\n{cls._truncate_prompt_text(body, body_budget, suffix='…')}".strip()
                else:
                    section = cls._truncate_prompt_text(section, available, suffix="…")

            if not section:
                continue

            kept_sections.append(section)
            current_length += len(section) + delimiter_length

            if current_length >= max_chars:
                break

        return "\n\n".join(kept_sections)

    @staticmethod
    def _normalize_evidence_text(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _classify_intraday_evidence(cls, value: object) -> Dict[str, bool]:
        text = cls._normalize_evidence_text(value)
        flags = {
            "bullish": False,
            "bearish": False,
            "risk": False,
            "timing": False,
            "volume": False,
            "trend": False,
            "neutral": False,
        }
        if not text:
            return flags

        bullish_keywords = ("放量回升", "均价上方", "承接", "企稳", "突破", "延续", "走强", "拉升", "修复", "回升", "强势")
        bearish_keywords = ("均价下方", "破位", "跌破", "回落", "抛压", "走弱", "分歧", "冲高回落", "转弱", "失守", "弱势")
        risk_keywords = ("高位", "追高", "冲高", "回落", "抛压", "分歧", "失真", "过热", "风险")
        timing_keywords = ("回踩", "均价", "高位", "低位", "日内", "突破", "承接", "企稳", "震荡")
        volume_keywords = ("放量", "缩量", "量能", "成交", "量比", "加速度")
        trend_keywords = ("趋势", "多头", "空头", "延续", "走强", "走弱", "修复", "破位")
        neutral_keywords = ("横盘", "震荡", "整理", "观望", "蓄势", "中位")

        flags["bullish"] = any(keyword in text for keyword in bullish_keywords)
        flags["bearish"] = any(keyword in text for keyword in bearish_keywords)
        flags["risk"] = any(keyword in text for keyword in risk_keywords)
        flags["timing"] = any(keyword in text for keyword in timing_keywords)
        flags["volume"] = any(keyword in text for keyword in volume_keywords)
        flags["trend"] = any(keyword in text for keyword in trend_keywords)
        flags["neutral"] = any(keyword in text for keyword in neutral_keywords)
        return flags

    @classmethod
    def _score_intraday_evidence(
        cls,
        value: object,
        *,
        kind: str,
        has_position: bool,
        freshness_status: str,
        previous_items: set[str],
        current_bias_text: str,
    ) -> tuple[float, str]:
        text = cls._normalize_evidence_text(value)
        flags = cls._classify_intraday_evidence(text)
        normalized_bias = cls._normalize_evidence_text(current_bias_text)
        score = 0.0

        score += 5.0 if kind == "label" else 3.0
        if freshness_status == "ready":
            score += 2.0
        elif freshness_status == "degraded":
            score += 0.5
        else:
            score -= 1.5

        if text and text not in previous_items:
            score += 4.0
        elif text:
            score -= 1.0

        if has_position:
            if flags["bearish"]:
                score += 5.0
            if flags["risk"]:
                score += 4.0
            if flags["trend"]:
                score += 2.0
            if flags["bullish"]:
                score += 1.0
            if flags["bearish"] and flags["volume"]:
                score += 2.0
        else:
            if flags["bullish"]:
                score += 5.0
            if flags["timing"]:
                score += 3.0
            if flags["trend"]:
                score += 2.0
            if flags["risk"]:
                score += 2.5
            if flags["bearish"]:
                score += 1.5
            if flags["bullish"] and flags["volume"]:
                score += 2.5

        if flags["volume"]:
            score += 1.5
        if flags["neutral"]:
            score -= 0.5

        if normalized_bias and normalized_bias != "N/A":
            if flags["bullish"] and any(keyword in normalized_bias for keyword in ("上方", "偏强", "延续", "放量", "回升")):
                score += 1.5
            if flags["bearish"] and any(keyword in normalized_bias for keyword in ("下方", "转弱", "回落", "破位")):
                score += 1.5

        if flags["risk"]:
            category = "risk"
        elif flags["bearish"]:
            category = "bearish"
        elif flags["bullish"]:
            category = "bullish"
        elif flags["timing"]:
            category = "timing"
        elif flags["volume"]:
            category = "volume"
        else:
            category = "neutral"
        return score, category

    @staticmethod
    def _category_to_polarity(category: str) -> str:
        normalized = str(category or "").strip().lower()
        if normalized in {"bullish", "timing", "trend", "volume"}:
            return "positive"
        if normalized in {"bearish", "risk"}:
            return "negative"
        return "neutral"

    @staticmethod
    def _intraday_reliability_from_freshness(freshness_status: str) -> float:
        normalized = str(freshness_status or "").strip().lower()
        if normalized == "ready":
            return 1.0
        if normalized == "degraded":
            return 0.72
        return 0.45

    @classmethod
    def _intraday_role_from_category(cls, category: str, has_position: bool) -> str:
        normalized = str(category or "").strip().lower()
        if has_position:
            if normalized in {"bearish", "risk"}:
                return "constraint"
            if normalized in {"bullish", "trend", "timing", "volume"}:
                return "support"
            return "context"
        if normalized in {"bearish", "risk"}:
            return "constraint"
        if normalized in {"bullish", "timing", "trend", "volume"}:
            return "support"
        return "context"

    @classmethod
    def _action_relevance_from_category(cls, category: str, has_position: bool) -> float:
        normalized = str(category or "").strip().lower()
        if has_position:
            if normalized in {"bearish", "risk"}:
                return 1.0
            if normalized in {"trend", "volume"}:
                return 0.75
            return 0.45
        if normalized in {"bullish", "timing"}:
            return 1.0
        if normalized in {"trend", "volume"}:
            return 0.8
        if normalized in {"bearish", "risk"}:
            return 0.7
        return 0.4

    @classmethod
    def _build_evidence_item(
        cls,
        *,
        text: str,
        layer: str,
        source_kind: str,
        category: str,
        role: str,
        novelty: bool,
        reliability: float,
        action_relevance: float,
        score: float,
        polarity: str | None = None,
        horizon: str = "intraday",
    ) -> Dict[str, object]:
        normalized_text = cls._normalize_evidence_text(text)
        return {
            "text": normalized_text,
            "layer": str(layer),
            "source_kind": str(source_kind),
            "category": str(category),
            "role": str(role),
            "novelty": bool(novelty),
            "reliability": float(reliability),
            "action_relevance": float(action_relevance),
            "score": float(score),
            "polarity": str(polarity or cls._category_to_polarity(category)),
            "horizon": str(horizon),
        }

    @classmethod
    def _rank_intraday_evidence_candidates(
        cls,
        items: List[object],
        *,
        kind: str,
        has_position: bool,
        freshness_status: str,
        previous_items: List[object] | None = None,
        current_bias_text: str = "",
    ) -> List[dict[str, object]]:
        normalized_previous = {
            cls._normalize_evidence_text(item)
            for item in (previous_items or [])
            if cls._normalize_evidence_text(item)
        }
        candidates: List[dict[str, object]] = []
        for raw_item in items:
            text = cls._normalize_evidence_text(raw_item)
            if not text:
                continue
            score, category = cls._score_intraday_evidence(
                text,
                kind=kind,
                has_position=has_position,
                freshness_status=freshness_status,
                previous_items=normalized_previous,
                current_bias_text=current_bias_text,
            )
            candidates.append(cls._build_evidence_item(
                text=text,
                layer="intraday",
                source_kind=kind,
                category=category,
                role=cls._intraday_role_from_category(category, has_position),
                novelty=text not in normalized_previous,
                reliability=cls._intraday_reliability_from_freshness(freshness_status),
                action_relevance=cls._action_relevance_from_category(category, has_position),
                score=score,
            ))
        candidates.sort(key=lambda item: (-float(item["score"]), len(str(item["text"]))))
        return candidates

    @classmethod
    def _select_intraday_evidence_items(
        cls,
        items: List[object],
        *,
        kind: str,
        has_position: bool,
        freshness_status: str,
        previous_items: List[object] | None = None,
        current_bias_text: str = "",
    ) -> str:
        normalized_previous = {
            cls._normalize_evidence_text(item)
            for item in (previous_items or [])
            if cls._normalize_evidence_text(item)
        }

        budget_map = {
            "ready": {"label": 90, "observation": 100},
            "degraded": {"label": 72, "observation": 84},
            "stale": {"label": 56, "observation": 64},
        }
        freshness_key = freshness_status if freshness_status in budget_map else "degraded"
        char_budget = budget_map[freshness_key]["label" if kind == "label" else "observation"]
        min_score = 6.5 if freshness_key == "ready" else 5.5 if freshness_key == "degraded" else 4.5
        separator = " / "
        candidates = cls._rank_intraday_evidence_candidates(
            items,
            kind=kind,
            has_position=has_position,
            freshness_status=freshness_key,
            previous_items=list(normalized_previous),
            current_bias_text=current_bias_text,
        )
        if not candidates:
            return "N/A"
        score_floor = max(min_score, float(candidates[0]["score"]) - 3.0)

        selected: List[str] = []
        selected_categories: set[str] = set()
        used_chars = 0

        for candidate in candidates:
            score = float(candidate["score"])
            category = str(candidate["category"])
            text = str(candidate["text"])
            if selected and score < score_floor:
                continue
            extra_chars = len(text) + (len(separator) if selected else 0)
            if used_chars + extra_chars > char_budget:
                continue
            if category in selected_categories and score < float(candidates[0]["score"]) - 1.5:
                continue
            selected.append(text)
            selected_categories.add(category)
            used_chars += extra_chars

        if not selected:
            top_text = cls._truncate_prompt_text(str(candidates[0]["text"]), char_budget, suffix="…")
            return top_text or "N/A"

        return separator.join(selected)

    @classmethod
    def _route_intraday_evidence(
        cls,
        *,
        labels: List[object],
        observations: List[object],
        has_position: bool,
        freshness_status: str,
        previous_labels: List[object] | None = None,
        current_bias_text: str = "",
    ) -> Dict[str, str]:
        freshness_key = freshness_status if freshness_status in {"ready", "degraded", "stale"} else "degraded"
        candidates = cls._rank_intraday_evidence_candidates(
            labels,
            kind="label",
            has_position=has_position,
            freshness_status=freshness_key,
            previous_items=previous_labels,
            current_bias_text=current_bias_text,
        ) + cls._rank_intraday_evidence_candidates(
            observations,
            kind="observation",
            has_position=has_position,
            freshness_status=freshness_key,
            previous_items=[],
            current_bias_text=current_bias_text,
        )
        if not candidates:
            return {
                "primary_evidence": "N/A",
                "counter_evidence": "N/A",
                "delta_evidence": "N/A",
            }

        primary_role = "constraint" if has_position else "support"
        counter_role = "support" if has_position else "constraint"
        primary_target = {"bearish", "risk"} if has_position else {"bullish", "timing", "trend", "volume"}
        counter_target = {"bullish", "timing", "trend", "volume"} if has_position else {"bearish", "risk"}

        def _candidate_rank(item: Dict[str, object]) -> tuple[float, int, int, int]:
            return (
                float(item.get("score", 0.0)) + float(item.get("action_relevance", 0.0)) * 2 + float(item.get("reliability", 0.0)),
                int(bool(item.get("novelty"))),
                1 if str(item.get("source_kind")) == "label" else 0,
                -len(str(item.get("text") or "")),
            )

        def _pick(predicate) -> str:
            ranked = [
                candidate for candidate in candidates
                if predicate(candidate) and str(candidate["text"]) != "N/A"
            ]
            if ranked:
                ranked.sort(key=_candidate_rank, reverse=True)
                return str(ranked[0]["text"])
            return "N/A"

        primary = _pick(
            lambda item: str(item.get("role")) == primary_role and str(item.get("category")) in primary_target
        )
        if primary == "N/A":
            primary = _pick(lambda item: str(item.get("role")) == primary_role)
        if primary == "N/A":
            primary = str(candidates[0]["text"])

        counter = _pick(
            lambda item: (
                str(item.get("role")) == counter_role
                and str(item.get("category")) in counter_target
                and str(item["text"]) != primary
            )
        )
        if counter == "N/A":
            counter = _pick(lambda item: str(item.get("role")) == counter_role and str(item["text"]) != primary)
        delta = _pick(
            lambda item: (
                bool(item.get("novelty"))
                and str(item.get("source_kind")) == "label"
                and str(item["text"]) != primary
                and str(item["text"]) != counter
            )
        )
        if delta == "N/A" and counter == "N/A":
            delta = _pick(
                lambda item: (
                    bool(item.get("novelty"))
                    and str(item.get("source_kind")) == "label"
                    and str(item["text"]) != primary
                )
            )
        if delta == "N/A":
            delta = _pick(
            lambda item: (
                bool(item.get("novelty"))
                and str(item["text"]) != primary
                and str(item["text"]) != counter
            )
            )
        if delta == "N/A":
            delta = _pick(
                lambda item: (
                    bool(item.get("novelty"))
                    and str(item["text"]) != primary
                    and str(item["text"]) != counter
                )
            )

        return {
            "primary_evidence": cls._truncate_prompt_text(primary, 80, suffix="…") if primary != "N/A" else "N/A",
            "counter_evidence": cls._truncate_prompt_text(counter, 70, suffix="…") if counter != "N/A" else "N/A",
            "delta_evidence": cls._truncate_prompt_text(delta, 70, suffix="…") if delta != "N/A" else "N/A",
            "primary_category": next((str(item["category"]) for item in candidates if str(item["text"]) == primary), "neutral") if primary != "N/A" else "neutral",
            "counter_category": next((str(item["category"]) for item in candidates if str(item["text"]) == counter), "neutral") if counter != "N/A" else "neutral",
            "delta_category": next((str(item["category"]) for item in candidates if str(item["text"]) == delta), "neutral") if delta != "N/A" else "neutral",
        }

    @classmethod
    def _infer_strategy_bias(cls, strategy_context: Optional[Dict[str, Any]]) -> str:
        if not isinstance(strategy_context, dict):
            return "neutral"
        text = " ".join(
            cls._normalize_evidence_text(strategy_context.get(key))
            for key in ("rating", "summary")
        )
        if any(keyword in text for keyword in ("买入", "增持", "做多", "看多", "突破", "上行")):
            return "bullish"
        if any(keyword in text for keyword in ("卖出", "减持", "做空", "看空", "止盈", "止损", "回避")):
            return "bearish"
        return "neutral"

    @classmethod
    def _infer_evidence_category(cls, text: object, default: str = "neutral") -> str:
        normalized = cls._normalize_evidence_text(text)
        if not normalized:
            return default
        flags = cls._classify_intraday_evidence(normalized)
        if flags["risk"]:
            return "risk"
        if flags["bearish"]:
            return "bearish"
        if flags["bullish"]:
            return "bullish"
        if flags["timing"]:
            return "timing"
        if flags["volume"]:
            return "volume"
        if flags["trend"]:
            return "trend"
        return default

    @staticmethod
    def _strategy_role_from_bias(strategy_bias: str) -> str:
        normalized = str(strategy_bias or "").strip().lower()
        if normalized == "bullish":
            return "support"
        if normalized == "bearish":
            return "constraint"
        return "context"

    def _build_strategy_evidence_items(
        self,
        strategy_context: Optional[Dict[str, Any]],
        *,
        has_position: bool,
    ) -> List[Dict[str, object]]:
        if not isinstance(strategy_context, dict):
            return []

        strategy_bias = self._infer_strategy_bias(strategy_context)
        strategy_role = self._strategy_role_from_bias(strategy_bias)
        items: List[Dict[str, object]] = []

        items.append(self._build_evidence_item(
            text=(
                f"战略基线偏{strategy_context.get('rating', 'N/A')}，阈值 "
                f"{strategy_context.get('entry_min', 'N/A')} - {strategy_context.get('entry_max', 'N/A')}"
            ),
            layer="strategy",
            source_kind="strategy_context",
            category=strategy_bias if strategy_bias != "neutral" else "neutral",
            role=strategy_role,
            novelty=False,
            reliability=0.95,
            action_relevance=0.95 if strategy_role != "context" else 0.6,
            score=6.0,
            horizon="swing",
        ))

        strategy_summary = self._build_strategy_summary_brief(strategy_context)
        if strategy_summary and strategy_summary != "N/A":
            items.append(self._build_evidence_item(
                text=strategy_summary,
                layer="strategy",
                source_kind="strategy_summary",
                category=self._infer_evidence_category(strategy_summary, default=strategy_bias),
                role=strategy_role if strategy_role != "context" else "context",
                novelty=False,
                reliability=0.92,
                action_relevance=0.88 if strategy_role != "context" else 0.58,
                score=6.4,
                horizon="swing",
            ))

        swing_type = str(strategy_context.get("swing_type") or "").strip()
        swing_days_text = str(strategy_context.get("swing_horizon_days_text") or strategy_context.get("holding_period") or "").strip()
        if swing_type or swing_days_text:
            items.append(self._build_evidence_item(
                text=f"基线波段类型：{swing_type or 'N/A'} | 周期参考：{swing_days_text or 'N/A'}",
                layer="strategy",
                source_kind="strategy_horizon",
                category="timing",
                role="context",
                novelty=False,
                reliability=0.95,
                action_relevance=0.8,
                score=6.2,
                horizon="swing",
            ))

        style_summary = str(strategy_context.get("strategy_style_summary") or "").strip()
        if style_summary:
            items.append(self._build_evidence_item(
                text=f"基线执行风格：{style_summary}",
                layer="strategy",
                source_kind="strategy_style",
                category="timing",
                role="context",
                novelty=False,
                reliability=0.9,
                action_relevance=0.84,
                score=6.1,
                horizon="swing",
            ))

        exit_style = str(strategy_context.get("baseline_exit_style") or "").strip()
        if exit_style:
            items.append(self._build_evidence_item(
                text=f"基线退出方式：{exit_style}",
                layer="strategy",
                source_kind="strategy_exit_style",
                category="risk",
                role="constraint",
                novelty=False,
                reliability=0.92,
                action_relevance=0.9,
                score=6.3,
                horizon="swing",
            ))

        if has_position and strategy_context.get("take_profit") not in (None, "") and strategy_context.get("stop_loss") not in (None, ""):
            items.append(self._build_evidence_item(
                text=f"持仓风控参考：初始止盈 {strategy_context.get('take_profit', 'N/A')} / 止损 {strategy_context.get('stop_loss', 'N/A')}",
                layer="strategy",
                source_kind="strategy_risk_bounds",
                category="risk",
                role="constraint",
                novelty=False,
                reliability=0.96,
                action_relevance=0.98,
                score=6.8,
                horizon="swing",
            ))
        elif not has_position and strategy_context.get("entry_min") not in (None, "") and strategy_context.get("entry_max") not in (None, ""):
            items.append(self._build_evidence_item(
                text=f"计划进场区间：{strategy_context.get('entry_min', 'N/A')} - {strategy_context.get('entry_max', 'N/A')}",
                layer="strategy",
                source_kind="strategy_entry_bounds",
                category="timing",
                role="support",
                novelty=False,
                reliability=0.94,
                action_relevance=0.98,
                score=6.9,
                horizon="swing",
            ))

        entry_conditions = normalize_condition_list(strategy_context.get("entry_conditions"), max_items=3)
        if entry_conditions:
            items.append(self._build_evidence_item(
                text="进场条件：" + "；".join(entry_conditions),
                layer="strategy",
                source_kind="strategy_entry_conditions",
                category="timing",
                role="support",
                novelty=False,
                reliability=0.97,
                action_relevance=1.0,
                score=7.1,
                horizon="swing",
            ))

        exit_conditions = normalize_condition_list(strategy_context.get("exit_conditions"), max_items=3)
        if exit_conditions:
            items.append(self._build_evidence_item(
                text="离场条件：" + "；".join(exit_conditions),
                layer="strategy",
                source_kind="strategy_exit_conditions",
                category="risk",
                role="constraint",
                novelty=False,
                reliability=0.97,
                action_relevance=1.0,
                score=7.2,
                horizon="swing",
            ))

        invalidation_conditions = normalize_condition_list(strategy_context.get("invalidation_conditions"), max_items=3)
        if invalidation_conditions:
            items.append(self._build_evidence_item(
                text="基线失效条件：" + "；".join(invalidation_conditions),
                layer="strategy",
                source_kind="strategy_invalidation_conditions",
                category="risk",
                role="constraint",
                novelty=False,
                reliability=0.98,
                action_relevance=1.0,
                score=7.4,
                horizon="swing",
            ))

        hold_conditions = normalize_condition_list(strategy_context.get("hold_conditions"), max_items=3)
        if hold_conditions:
            items.append(self._build_evidence_item(
                text="继续持有/观望条件：" + "；".join(hold_conditions),
                layer="strategy",
                source_kind="strategy_hold_conditions",
                category="neutral",
                role="context",
                novelty=False,
                reliability=0.94,
                action_relevance=0.86,
                score=6.5,
                horizon="swing",
            ))

        return items

    @classmethod
    def _build_intraday_cross_layer_items(
        cls,
        evidence_summary: Optional[Dict[str, str]],
        *,
        has_position: bool,
    ) -> List[Dict[str, object]]:
        if not evidence_summary:
            return []

        items: List[Dict[str, object]] = []
        primary_category = str(evidence_summary.get("primary_category") or "neutral")
        primary_role = "constraint" if has_position and primary_category in {"bearish", "risk"} else "support"
        if not has_position and primary_category in {"risk", "bearish"}:
            primary_role = "constraint"

        items.append(cls._build_evidence_item(
            text=str(evidence_summary.get("primary_evidence") or "N/A"),
            layer="intraday",
            source_kind="intraday_primary",
            category=primary_category,
            role=primary_role,
            novelty=False,
            reliability=0.92,
            action_relevance=1.0,
            score=8.0,
            horizon="intraday",
        ))
        if str(evidence_summary.get("counter_evidence") or "N/A") != "N/A":
            items.append(cls._build_evidence_item(
                text=str(evidence_summary.get("counter_evidence") or "N/A"),
                layer="intraday",
                source_kind="intraday_counter",
                category=str(evidence_summary.get("counter_category") or "neutral"),
                role="constraint",
                novelty=False,
                reliability=0.9,
                action_relevance=0.95,
                score=7.0,
                horizon="intraday",
            ))
        if str(evidence_summary.get("delta_evidence") or "N/A") != "N/A":
            items.append(cls._build_evidence_item(
                text=str(evidence_summary.get("delta_evidence") or "N/A"),
                layer="intraday",
                source_kind="intraday_delta",
                category=str(evidence_summary.get("delta_category") or "neutral"),
                role="change",
                novelty=True,
                reliability=0.9,
                action_relevance=0.92,
                score=7.5,
                horizon="intraday",
            ))
        return items

    @staticmethod
    def _cross_layer_priority(layer: str, role: str, has_position: bool) -> float:
        normalized_layer = str(layer or "").strip().lower()
        normalized_role = str(role or "").strip().lower()
        priority_map = {
            "support": {
                "intraday": 1.0,
                "strategy": 0.94 if not has_position else 0.9,
                "previous": 0.72,
            },
            "constraint": {
                "intraday": 1.0,
                "strategy": 0.96,
                "previous": 0.74,
            },
            "change": {
                "intraday": 1.0,
                "previous": 0.96,
                "strategy": 0.45,
            },
            "context": {
                "strategy": 0.7,
                "previous": 0.68,
                "intraday": 0.66,
            },
        }
        return priority_map.get(normalized_role, priority_map["context"]).get(normalized_layer, 0.4)

    def _build_cross_layer_evidence_summary(
        self,
        *,
        has_position: bool,
        strategy_context: Optional[Dict[str, Any]],
        evidence_summary: Optional[Dict[str, str]],
    ) -> Dict[str, str]:
        strategy_bias = self._infer_strategy_bias(strategy_context)
        primary_category = str((evidence_summary or {}).get("primary_category") or "neutral")

        if isinstance(strategy_context, dict):
            threshold_parts = []
            entry_min = strategy_context.get("entry_min")
            entry_max = strategy_context.get("entry_max")
            if not self._is_missing_prompt_value(entry_min) and not self._is_missing_prompt_value(entry_max):
                threshold_parts.append(f"进场 {entry_min} - {entry_max}")
            take_profit = strategy_context.get("take_profit")
            if not self._is_missing_prompt_value(take_profit):
                threshold_parts.append(f"止盈 {take_profit}")
            stop_loss = strategy_context.get("stop_loss")
            if not self._is_missing_prompt_value(stop_loss):
                threshold_parts.append(f"止损 {stop_loss}")
            baseline_anchor = self._truncate_prompt_text(
                self._join_prompt_values(
                    strategy_context.get("rating"),
                    strategy_context.get("swing_type"),
                    (
                        f"周期 {strategy_context.get('swing_horizon_days_text') or strategy_context.get('holding_period')}"
                        if str(strategy_context.get("swing_horizon_days_text") or strategy_context.get("holding_period") or "").strip()
                        else ""
                    ),
                    self._join_prompt_values(*threshold_parts),
                ) or "无战略基线",
                100,
                suffix="…",
            )
        else:
            baseline_anchor = "无战略基线"

        strategy_execution_preference = (
            str((strategy_context or {}).get("intraday_execution_preference") or "").strip()
            if isinstance(strategy_context, dict)
            else ""
        )
        if has_position:
            execution_focus = "已有持仓，优先区分回踩确认加仓、突破确认加仓、主动减仓锁盈、防守减仓/清仓与继续持有，并同步考虑仓位上限与T+1约束。"
            if strategy_execution_preference:
                execution_focus = f"{execution_focus} 当前基线偏好：{strategy_execution_preference}"
        elif isinstance(strategy_context, dict):
            execution_focus = "无持仓，先看盘中证据是否支持沿用或微调基线阈值执行，避免情绪化追价。"
            if strategy_execution_preference:
                execution_focus = f"{execution_focus} 当前基线偏好：{strategy_execution_preference}"
        else:
            execution_focus = "无持仓，可根据盘中证据强度评估是否执行买点，但要兼顾时点与风险收益比。"

        if strategy_bias == "bullish" and primary_category in {"bullish", "timing", "volume", "trend"}:
            alignment = "盘中主导证据与战略基线基本一致。"
        elif strategy_bias == "bearish" and primary_category in {"bearish", "risk"}:
            alignment = "盘中主导证据与战略基线一致，当前更偏向控制回撤或择机退出。"
        elif strategy_bias == "neutral":
            alignment = "当前缺少明确战略方向，盘中证据权重更高。"
        else:
            alignment = "盘中主导证据与战略基线存在偏离，需要进一步区分短线扰动还是结构变化。"

        evidence_pool: List[Dict[str, object]] = []
        evidence_pool.extend(self._build_strategy_evidence_items(strategy_context, has_position=has_position))
        evidence_pool.extend(self._build_intraday_cross_layer_items(evidence_summary, has_position=has_position))

        def _pick_cross_layer(role: str, fallback: str) -> str:
            candidates = [item for item in evidence_pool if str(item["role"]) == role and str(item["text"]) != "N/A"]
            if not candidates:
                return fallback
            candidates.sort(
                key=lambda item: (
                    -(
                        float(item["score"])
                        + float(item["action_relevance"]) * 2
                        + float(item["reliability"])
                        + self._cross_layer_priority(str(item["layer"]), role, has_position) * 2
                    ),
                    -int(bool(item["novelty"])),
                    len(str(item["text"])),
                )
            )
            return self._truncate_prompt_text(str(candidates[0]["text"]), 96, suffix="…")

        return {
            "baseline_anchor": baseline_anchor,
            "execution_focus": execution_focus,
            "alignment_summary": alignment,
            "execution_support": _pick_cross_layer("support", "N/A"),
            "execution_constraint": _pick_cross_layer("constraint", "N/A"),
            "change_trigger": _pick_cross_layer("change", "N/A"),
        }

    @staticmethod
    def _resolve_risk_profile(risk_profile: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        defaults = dict(config.get_smart_monitor_risk_defaults())
        payload = risk_profile or {}

        def clamp(value: Any, minimum: int, maximum: int, fallback: int) -> int:
            try:
                numeric = int(value)
            except (TypeError, ValueError):
                numeric = fallback
            return max(minimum, min(maximum, numeric))

        return {
            "position_size_pct": clamp(payload.get("position_size_pct"), 0, 100, defaults["position_size_pct"]),
            "total_position_pct": clamp(payload.get("total_position_pct"), 0, 100, defaults["total_position_pct"]),
            "stop_loss_pct": clamp(payload.get("stop_loss_pct"), 0, 100, defaults["stop_loss_pct"]),
            "take_profit_pct": clamp(payload.get("take_profit_pct"), 0, 100, defaults["take_profit_pct"]),
        }

    def is_trading_time(self) -> bool:
        """
        判断当前是否在A股交易时间内
        
        Returns:
            bool: 是否可以交易
        """
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(beijing_tz)
        current_time = now.time()
        
        # 排除周末
        if now.weekday() >= 5:
            return False
        
        # 上午：9:30-11:30
        morning_start = time(9, 30)
        morning_end = time(11, 30)
        
        # 下午：13:00-15:00
        afternoon_start = time(13, 0)
        afternoon_end = time(15, 0)
        
        is_trading = (
            (morning_start <= current_time <= morning_end) or
            (afternoon_start <= current_time <= afternoon_end)
        )
        
        return is_trading

    def get_trading_session(self) -> Dict:
        """
        获取当前交易时段信息（A股版本）
        
        Returns:
            Dict: 时段信息
        """
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(beijing_tz)
        current_time = now.time()
        current_time_text = now.strftime("%H:%M")
        
        # 判断是否交易日
        if now.weekday() >= 5:
            return {
                'session': '休市',
                'volatility': 'none',
                'recommendation': '周末不可交易',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }
        
        # 开盘前（9:00-9:30）：集合竞价时段
        if time(9, 0) <= current_time < time(9, 30):
            return {
                'session': '集合竞价',
                'volatility': 'high',
                'recommendation': '可观察盘面情绪，准备开盘交易',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }
        
        # 上午盘（9:30-11:30）
        elif time(9, 30) <= current_time <= time(11, 30):
            return {
                'session': '上午盘',
                'volatility': 'high',
                'recommendation': '交易活跃，波动较大',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': True
            }
        
        # 午间休市（11:30-13:00）
        elif time(11, 30) < current_time < time(13, 0):
            return {
                'session': '午间休市',
                'volatility': 'none',
                'recommendation': '不可交易，可分析上午盘面',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }
        
        # 下午盘（13:00-15:00）
        elif time(13, 0) <= current_time <= time(15, 0):
            # 尾盘最后半小时（14:30-15:00）
            if current_time >= time(14, 30):
                return {
                    'session': '尾盘',
                    'volatility': 'high',
                    'recommendation': '尾盘波动大，谨慎操作',
                    'beijing_hour': now.hour,
                    'beijing_time': current_time_text,
                    'can_trade': True
                }
            else:
                return {
                    'session': '下午盘',
                    'volatility': 'medium',
                    'recommendation': '波动趋缓，适合布局',
                    'beijing_hour': now.hour,
                    'beijing_time': current_time_text,
                    'can_trade': True
                }
        
        # 盘后（15:00之后）
        else:
            return {
                'session': '盘后',
                'volatility': 'none',
                'recommendation': '收盘后，可复盘分析',
                'beijing_hour': now.hour,
                'beijing_time': current_time_text,
                'can_trade': False
            }

    @staticmethod
    def _get_intraday_session_progress(session_info: Dict[str, Any]) -> Optional[float]:
        """Return completed fraction of today's regular trading session."""
        session = str(session_info.get("session") or "")
        if session == "休市":
            return None

        beijing_time = str(session_info.get("beijing_time") or "").strip()
        if not beijing_time:
            return None

        try:
            hour_text, minute_text = beijing_time.split(":", 1)
            current_minutes = int(hour_text) * 60 + int(minute_text)
        except (ValueError, TypeError):
            return None

        morning_open = 9 * 60 + 30
        morning_close = 11 * 60 + 30
        afternoon_open = 13 * 60
        afternoon_close = 15 * 60
        total_minutes = 240

        if current_minutes < morning_open:
            elapsed_minutes = 0
        elif current_minutes <= morning_close:
            elapsed_minutes = current_minutes - morning_open
        elif current_minutes < afternoon_open:
            elapsed_minutes = 120
        elif current_minutes <= afternoon_close:
            elapsed_minutes = 120 + (current_minutes - afternoon_open)
        else:
            elapsed_minutes = total_minutes

        progress = elapsed_minutes / total_minutes if total_minutes > 0 else None
        if progress is None:
            return None
        return max(0.0, min(1.0, progress))

    def chat_completion(self, messages: List[Dict], model: str = None,
                       temperature: Optional[float] = None, max_tokens: int = 2000,
                       tier: ModelTier = ModelTier.LIGHTWEIGHT,
                       top_p: Optional[float] = None,
                       sampling_profile: str = "factual") -> Dict:
        """
        调用DeepSeek API
        
        Args:
            messages: 对话消息列表
            model: 模型名称
            temperature: 温度参数；默认使用事实优先配置
            max_tokens: 最大token数
            top_p: nucleus sampling 参数；默认使用全局配置
            sampling_profile: 采样参数配置档，默认事实优先
            
        Returns:
            API响应
        """
        model_to_use = resolve_model_name(
            tier=tier,
            explicit_model=model,
            forced_model=self.model,
            lightweight_model=self.lightweight_model,
            reasoning_model=self.reasoning_model,
        )
        api_model_to_use = config.get_model_api_name(model_to_use) or model_to_use

        if "reasoner" in model_to_use.lower() and max_tokens <= 2000:
            max_tokens = max(max_tokens, self.reasoning_max_tokens)

        effective_temperature, effective_top_p = config.resolve_llm_sampling_params(
            model_to_use,
            temperature=temperature,
            top_p=top_p,
            profile=sampling_profile,
        )
        
        payload = {
            "model": api_model_to_use,
            "messages": messages,
            "temperature": effective_temperature,
            "max_tokens": max_tokens
        }
        if effective_top_p < 1.0:
            payload["top_p"] = effective_top_p
        payload_size = len(json.dumps(payload, ensure_ascii=False))
        if payload_size >= self.PROMPT_SIZE_WARNING_CHARS:
            self.logger.warning(
                "LLM请求体较大，model=%s，payload_chars=%s，messages=%s",
                api_model_to_use,
                payload_size,
                len(messages),
            )
        else:
            self.logger.debug(
                "LLM请求体大小，model=%s，payload_chars=%s，messages=%s",
                api_model_to_use,
                payload_size,
                len(messages),
            )

        api_key, base_url = self._resolve_request_credentials(model_to_use)
        endpoint = f"{base_url.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        request_timeout = (10, self.http_timeout_seconds)
        total_attempts = self.http_retry_count + 1
        retryable_errors = (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
        last_error: Optional[Exception] = None

        for attempt_index in range(total_attempts):
            try:
                response = requests.post(
                    endpoint,
                    headers=headers,
                    json=payload,
                    timeout=request_timeout
                )
                response.raise_for_status()
                return response.json()
            except retryable_errors as exc:
                last_error = exc
                if attempt_index >= self.http_retry_count:
                    break
                self.logger.warning(
                    "LLM API请求超时或连接失败，准备重试 (%s/%s)，model=%s，read_timeout=%ss: %s",
                    attempt_index + 1,
                    total_attempts,
                    api_model_to_use,
                    self.http_timeout_seconds,
                    exc,
                )
                time_module.sleep(min(2, attempt_index + 1))
            except requests.exceptions.HTTPError as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code is not None and 500 <= int(status_code) < 600 and attempt_index < self.http_retry_count:
                    last_error = exc
                    self.logger.warning(
                        "LLM API返回%s，准备重试 (%s/%s)，model=%s，payload_chars=%s: %s",
                        status_code,
                        attempt_index + 1,
                        total_attempts,
                        api_model_to_use,
                        payload_size,
                        exc,
                    )
                    time_module.sleep(min(2, attempt_index + 1))
                    continue
                self.logger.error(
                    "LLM API调用失败，model=%s，status=%s，timeout=%ss，payload_chars=%s: %s",
                    api_model_to_use,
                    status_code,
                    self.http_timeout_seconds,
                    payload_size,
                    exc,
                )
                raise
            except Exception as exc:
                self.logger.error(
                    "LLM API调用失败，model=%s，timeout=%ss，payload_chars=%s: %s",
                    api_model_to_use,
                    self.http_timeout_seconds,
                    payload_size,
                    exc,
                )
                raise

        if last_error is not None:
            self.logger.error(
                "LLM API调用失败，重试后仍未成功，model=%s，timeout=%ss: %s",
                api_model_to_use,
                self.http_timeout_seconds,
                last_error,
            )
            raise last_error
        raise RuntimeError("LLM API调用失败: unknown_request_error")

    def analyze_stock_and_decide(self, stock_code: str, market_data: Dict,
                                 account_info: Dict, has_position: bool = False,
                                 position_cost: float = 0, position_quantity: int = 0,
                                 position_date: Optional[str] = None,
                                 can_sell_today: bool = True,
                                 account_name: str = DEFAULT_ACCOUNT_NAME,
                                 asset_id: Optional[int] = None,
                                 portfolio_stock_id: Optional[int] = None,
                                 strategy_context: Optional[Dict] = None,
                                 risk_profile: Optional[Dict[str, Any]] = None) -> Dict:
        """
        分析股票并做出交易决策（A股T+1规则）
        
        Args:
            stock_code: 股票代码（如：600519）
            market_data: 市场数据
            account_info: 账户信息
            has_position: 是否已持有该股票
            position_cost: 持仓成本价格
            position_quantity: 持仓数量
            position_date: 持仓日期
            can_sell_today: 今日是否允许卖出
            
        Returns:
            交易决策
        """
        # 获取交易时段
        session_info = self.get_trading_session()
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        reasoning_context = self._build_reasoning_context(
            has_position=has_position,
            market_data=market_data,
            strategy_context=strategy_context,
        )
        messages = self._build_prompt_messages(
            stock_code, market_data, account_info,
            has_position, session_info, position_cost, position_quantity,
            position_date=position_date,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            strategy_context=strategy_context,
            risk_profile=resolved_risk_profile,
        )

        try:
            ai_response = ""
            decision = None
            active_messages = list(messages)
            for attempt_index in range(self.decision_repair_attempts + 1):
                response = self.chat_completion(
                    active_messages,
                    temperature=0.1,
                    max_tokens=1600,
                    tier=ModelTier.LIGHTWEIGHT,
                )
                ai_response = response['choices'][0]['message']['content']
                try:
                    decision = self._parse_decision_strict(
                        ai_response,
                        risk_profile=resolved_risk_profile,
                        has_position=has_position,
                    )
                    break
                except Exception as exc:
                    if attempt_index >= self.decision_repair_attempts:
                        self.logger.error("盘中决策解析/校验失败，修正次数耗尽: %s", exc)
                        decision = self._build_invalid_decision_fallback(
                            error=exc,
                            risk_profile=resolved_risk_profile,
                            has_position=has_position,
                        )
                        break
                    self.logger.warning("盘中决策解析/校验失败，准备请求模型修正: %s", exc)
                    active_messages = self._build_repair_messages(
                        messages=active_messages,
                        ai_response=ai_response,
                        validation_error=str(exc),
                    )
            if decision is None:
                decision = self._build_invalid_decision_fallback(
                    error=RuntimeError("decision_repair_exhausted"),
                    risk_profile=resolved_risk_profile,
                    has_position=has_position,
                )

            decision = self._enforce_action_policy(
                decision,
                has_position=has_position,
                can_sell_today=can_sell_today,
            )
            decision = self._attach_execution_targets(
                decision,
                account_info=account_info,
                risk_profile=resolved_risk_profile,
            )
            decision["reasoning"] = self._normalize_reasoning_output(
                decision,
                reasoning_context=reasoning_context,
                strategy_context=strategy_context,
                has_position=has_position,
            )
            
            return {
                'success': True,
                'decision': decision,
                'raw_response': ai_response
            }
            
        except Exception as e:
            self.logger.error(f"AI决策失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _build_reasoning_context(
        self,
        *,
        has_position: bool,
        market_data: Dict[str, Any],
        strategy_context: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        intraday_context = market_data.get("intraday_context") if isinstance(market_data.get("intraday_context"), dict) else {}
        realtime_freshness = market_data.get("realtime_freshness") if isinstance(market_data.get("realtime_freshness"), dict) else {}
        evidence_summary: Optional[Dict[str, str]] = None
        if intraday_context:
            derived_intraday_observations = [
                self._derive_intraday_momentum_state(intraday_context),
                self._derive_intraday_acceptance_state(intraday_context),
                self._derive_take_profit_adjustment_hint(intraday_context, has_position),
            ]
            evidence_summary = self._route_intraday_evidence(
                labels=intraday_context.get("intraday_signal_labels") if isinstance(intraday_context.get("intraday_signal_labels"), list) else [],
                observations=[
                    *(intraday_context.get("intraday_observations") if isinstance(intraday_context.get("intraday_observations"), list) else []),
                    *derived_intraday_observations,
                ],
                has_position=has_position,
                freshness_status=str(realtime_freshness.get("overall_status") or "").strip().lower(),
                previous_labels=[],
                current_bias_text=str(intraday_context.get("intraday_bias_text") or ""),
            )
        return {
            "intraday_context": intraday_context,
            "freshness_status": str(realtime_freshness.get("overall_status") or "").strip().lower() or "unknown",
            "has_strategy": bool(strategy_context),
            "cross_layer_summary": self._build_cross_layer_evidence_summary(
                has_position=has_position,
                strategy_context=strategy_context,
                evidence_summary=evidence_summary,
            ),
        }

    @staticmethod
    def _risk_level_text(value: str) -> str:
        mapping = {
            "low": "低",
            "medium": "中",
            "high": "高",
        }
        return mapping.get(str(value or "").strip().lower(), "中")

    @staticmethod
    def _truncate_reasoning_sentence(text: str, limit: int = 220) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        normalized = normalized.rstrip("；;，,。.")
        if len(normalized) <= limit:
            return normalized + "。"
        shortened = normalized[:limit].rstrip("；;，,。.")
        return shortened + "。"

    def _threshold_origin_text(
        self,
        decision: Dict[str, Any],
        strategy_context: Optional[Dict[str, Any]],
    ) -> str:
        monitor_levels = decision.get("monitor_levels") if isinstance(decision.get("monitor_levels"), dict) else {}
        if not monitor_levels:
            return "预警阈值未完整返回"
        if not isinstance(strategy_context, dict):
            return "预警阈值基于当前盘中结构设置"

        def _same(a: Any, b: Any) -> bool:
            try:
                return abs(float(a) - float(b)) <= 1e-6
            except (TypeError, ValueError):
                return False

        strategy_pairs = {
            "entry_min": strategy_context.get("entry_min"),
            "entry_max": strategy_context.get("entry_max"),
            "take_profit": strategy_context.get("take_profit"),
            "stop_loss": strategy_context.get("stop_loss"),
        }
        if all(
            strategy_pairs.get(key) not in (None, "")
            and monitor_levels.get(key) not in (None, "")
            and _same(monitor_levels.get(key), strategy_pairs.get(key))
            for key in ("entry_min", "entry_max", "take_profit", "stop_loss")
        ):
            if monitor_levels.get("take_profit_max") not in (None, ""):
                try:
                    if float(monitor_levels["take_profit_max"]) > float(monitor_levels["take_profit"]):
                        return "止盈区间按盘中结构上修"
                except (TypeError, ValueError):
                    pass
            return "预警阈值沿用基线区间"
        return "预警阈值按盘中结构重算"

    def _build_intraday_feature_summary(self, intraday_context: Dict[str, Any]) -> str:
        if not isinstance(intraday_context, dict) or not intraday_context:
            return "无分时证据"

        features: List[str] = []
        bias_text = self._normalize_evidence_text(intraday_context.get("intraday_bias_text"))
        if bias_text and bias_text != "N/A":
            features.append(f"盘中偏向{bias_text}")
        last_5m = intraday_context.get("last_5m_change_pct")
        try:
            if last_5m not in (None, ""):
                features.append(f"近5分钟涨跌{float(last_5m):+.2f}%")
        except (TypeError, ValueError):
            pass
        volume_accel = intraday_context.get("volume_acceleration_ratio")
        try:
            if volume_accel not in (None, ""):
                features.append(f"量能加速度{float(volume_accel):.2f}")
        except (TypeError, ValueError):
            pass
        price_position = intraday_context.get("price_position_pct")
        try:
            if price_position not in (None, ""):
                features.append(f"日内位置{float(price_position):.1f}%")
        except (TypeError, ValueError):
            pass

        if not features:
            return "无分时证据"
        return "，".join(features[:2])

    @staticmethod
    def _intraday_numeric(value: Any) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        return numeric

    @classmethod
    def _derive_intraday_momentum_state(cls, intraday_context: Dict[str, Any]) -> str:
        last_15m = cls._intraday_numeric(intraday_context.get("last_15m_change_pct"))
        last_30m = cls._intraday_numeric(intraday_context.get("last_30m_change_pct"))
        last_60m = cls._intraday_numeric(intraday_context.get("last_60m_change_pct"))
        last_5m = cls._intraday_numeric(intraday_context.get("last_5m_change_pct"))

        if last_15m is None and last_30m is None and last_60m is None:
            return "缺少15/30/60分钟节奏数据"

        if all(value is not None for value in (last_15m, last_30m, last_60m)):
            assert last_15m is not None and last_30m is not None and last_60m is not None
            if last_15m <= -0.5 and last_30m <= -0.5 and last_60m <= -0.8:
                return "15/30/60分钟持续走坏"
            if last_60m > 0.5 and last_30m > 0.3 and last_15m <= -0.2:
                return "60/30分钟仍偏强，但15分钟已转弱"
            if last_60m <= -0.5 and last_30m <= -0.3 and last_15m >= 0.2:
                return "60/30分钟偏弱，但15分钟出现修复"
            if last_15m >= 0.4 and last_30m >= 0.3 and last_60m >= 0.5:
                return "15/30/60分钟整体走强，未见持续走坏"
            if last_15m <= -0.2 and last_30m <= -0.2:
                return "15/30分钟连续转弱，需防止扩散到60分钟"
            if last_15m >= 0.2 and last_30m >= 0.2:
                return "15/30分钟延续走强，60分钟结构仍偏稳"
            if last_5m is not None and abs(last_5m) >= 0.8:
                return "15/30/60分钟节奏分化，5分钟存在异动"
            return "15/30/60分钟节奏分化，暂未形成单边趋势"

        available_parts = []
        if last_15m is not None:
            available_parts.append(f"15分钟{last_15m:+.2f}%")
        if last_30m is not None:
            available_parts.append(f"30分钟{last_30m:+.2f}%")
        if last_60m is not None:
            available_parts.append(f"60分钟{last_60m:+.2f}%")
        return " / ".join(available_parts) if available_parts else "缺少15/30/60分钟节奏数据"

    @classmethod
    def _derive_intraday_acceptance_state(cls, intraday_context: Dict[str, Any]) -> str:
        text_pool = [
            cls._normalize_evidence_text(intraday_context.get("intraday_bias_text")),
            *[
                cls._normalize_evidence_text(item)
                for item in (intraday_context.get("intraday_signal_labels") or [])
            ],
            *[
                cls._normalize_evidence_text(item)
                for item in (intraday_context.get("intraday_observations") or [])
            ],
        ]
        blob = " ".join(part for part in text_pool if part and part != "N/A")

        positive_markers = (
            "承接", "企稳", "修复", "回升", "均价上方", "主动买盘占优", "主动性买单增强",
            "未见明显抢跑抛压", "高位承接正常", "低位回升承接", "回踩后修复", "放量回升", "接力",
        )
        negative_markers = (
            "承接一般", "承接下降", "跌破均价", "抛压", "回落", "量能衰减", "放量回落走弱",
            "高位量能衰减", "冲高回落", "抢跑", "走弱",
        )

        score = 0
        for marker in positive_markers:
            if marker in blob:
                score += 1
        for marker in negative_markers:
            if marker in blob:
                score -= 1

        volume_ratio_15m = cls._intraday_numeric(intraday_context.get("volume_ratio_15m"))
        volume_ratio_30m = cls._intraday_numeric(intraday_context.get("volume_ratio_30m"))
        if volume_ratio_15m is not None and volume_ratio_30m is not None:
            if volume_ratio_15m >= 1.1 and volume_ratio_30m >= 1.0:
                score += 1
            elif volume_ratio_15m <= 0.9 and volume_ratio_30m <= 0.92:
                score -= 1
        volume_acc = cls._intraday_numeric(intraday_context.get("volume_acceleration_ratio"))
        if volume_acc is not None:
            if volume_acc >= 1.2 and score > 0 and any(marker in blob for marker in ("承接", "企稳", "修复", "均价上方", "主动买盘")):
                score += 1
            elif volume_acc < 0.8 and score < 0 and any(marker in blob for marker in ("回落", "衰减", "抛压", "走弱")):
                score -= 1

        if score >= 2:
            return "承接优化，回踩后仍有资金接力"
        if score <= -2:
            return "承接转弱，抛压有所增加"
        return "承接一般，仍需观察"

    @classmethod
    def _derive_take_profit_adjustment_hint(cls, intraday_context: Dict[str, Any], has_position: bool) -> str:
        if not has_position:
            return "空仓场景，以入场节奏判断为主"

        momentum_state = cls._derive_intraday_momentum_state(intraday_context)
        acceptance_state = cls._derive_intraday_acceptance_state(intraday_context)
        last_60m = cls._intraday_numeric(intraday_context.get("last_60m_change_pct"))
        last_30m = cls._intraday_numeric(intraday_context.get("last_30m_change_pct"))
        last_15m = cls._intraday_numeric(intraday_context.get("last_15m_change_pct"))
        volume_ratio_60m = cls._intraday_numeric(intraday_context.get("volume_ratio_60m"))
        volume_ratio_30m = cls._intraday_numeric(intraday_context.get("volume_ratio_30m"))
        volume_ratio_15m = cls._intraday_numeric(intraday_context.get("volume_ratio_15m"))
        volume_acc = cls._intraday_numeric(intraday_context.get("volume_acceleration_ratio"))

        if (
            ("整体走强" in momentum_state or "延续走强" in momentum_state)
            and "承接优化" in acceptance_state
            and (last_60m is None or last_60m >= 0)
        ):
            return "若接近基线止盈位但60分钟结构未走坏，可继续持有或上修止盈位，不必主动锁盈"
        if (
            "持续走坏" in momentum_state
            or "已转弱" in momentum_state
            or "承接转弱" in acceptance_state
            or (
                last_60m is not None and last_60m <= -0.5
                and (
                    (last_30m is not None and last_30m <= -0.2)
                    or (last_15m is not None and last_15m <= -0.2)
                )
                and (
                    (volume_ratio_60m is not None and volume_ratio_60m < 1.0)
                    or (volume_ratio_30m is not None and volume_ratio_30m < 0.98)
                    or (volume_ratio_15m is not None and volume_ratio_15m < 0.98)
                    or (volume_acc is not None and volume_acc < 0.95)
                )
            )
        ):
            return "若接近止盈位且60分钟转弱、风险较高时，再结合15/30分钟与短时量能确认卖点"
        return "止盈区先看60分钟结构，再结合15/30分钟和短时量能寻找更好的买卖点"

    @classmethod
    def _derive_intraday_volume_structure(cls, intraday_context: Dict[str, Any]) -> str:
        volume_ratio_15m = cls._intraday_numeric(intraday_context.get("volume_ratio_15m"))
        volume_ratio_30m = cls._intraday_numeric(intraday_context.get("volume_ratio_30m"))
        volume_ratio_60m = cls._intraday_numeric(intraday_context.get("volume_ratio_60m"))
        if volume_ratio_15m is None and volume_ratio_30m is None and volume_ratio_60m is None:
            return "缺少15/30/60分钟量能结构数据"
        if (
            volume_ratio_15m is not None and volume_ratio_15m >= 1.15
            and volume_ratio_30m is not None and volume_ratio_30m >= 1.05
            and volume_ratio_60m is not None and volume_ratio_60m >= 1.0
        ):
            return "15/30/60分钟量能整体扩张"
        if (
            volume_ratio_15m is not None and volume_ratio_15m <= 0.9
            and volume_ratio_30m is not None and volume_ratio_30m <= 0.92
        ):
            return "15/30分钟量能持续收缩"
        if (
            volume_ratio_15m is not None and volume_ratio_15m >= 1.1
            and volume_ratio_30m is not None and volume_ratio_30m < 0.98
        ):
            return "5分钟触发较强，但15/30分钟量能未完全跟随"
        if (
            volume_ratio_30m is not None and volume_ratio_30m >= 1.02
            and volume_ratio_60m is not None and volume_ratio_60m >= 1.0
        ):
            return "30/60分钟量能保持扩张，更适合波段跟踪"
        if (
            volume_ratio_60m is not None and volume_ratio_60m < 0.98
            and volume_ratio_30m is not None and volume_ratio_30m >= 1.0
        ):
            return "60分钟量能未跟随，短时放量更适合当作执行确认"
        return "量能结构中性，等待进一步确认"

    @staticmethod
    def _format_action_ratio_text(action_ratio_pct: Any) -> str:
        if action_ratio_pct in (None, "", 0, 0.0):
            return ""
        try:
            numeric = float(action_ratio_pct)
        except (TypeError, ValueError):
            return ""
        if numeric <= 0:
            return ""
        if abs(numeric - round(numeric)) < 1e-6:
            return f"{int(round(numeric))}%"
        return f"{numeric:.1f}%"

    def _format_action_signature(self, action: str, action_detail: str, action_ratio_pct: Any) -> str:
        detail = str(action_detail or "").strip()
        if not detail:
            detail = self._resolve_action_detail(None, action=action, has_position=action == "SELL")
        ratio_text = self._format_action_ratio_text(action_ratio_pct)
        if ratio_text and str(action or "").strip().upper() in {"BUY", "SELL"}:
            return f"{detail}{ratio_text}"
        return detail

    @staticmethod
    def _round_position_pct(value: Any) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(numeric):
            return 0.0
        return round(max(0.0, min(100.0, numeric)), 1)

    @classmethod
    def _extract_current_position_pct(cls, account_info: Optional[Dict[str, Any]]) -> float:
        current_position = (account_info or {}).get("current_position") if isinstance(account_info, dict) else {}
        if not isinstance(current_position, dict):
            return 0.0
        raw_value = current_position.get("position_pct")
        try:
            numeric = float(raw_value)
        except (TypeError, ValueError):
            return 0.0
        if not math.isfinite(numeric):
            return 0.0
        if 0 <= numeric <= 1.5:
            numeric *= 100.0
        return cls._round_position_pct(numeric)

    def _attach_execution_targets(
        self,
        decision: Dict[str, Any],
        *,
        account_info: Optional[Dict[str, Any]],
        risk_profile: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        current_position_pct = self._extract_current_position_pct(account_info)
        single_position_cap = self._round_position_pct(resolved_risk_profile.get("position_size_pct"))
        total_position_cap = self._round_position_pct(resolved_risk_profile.get("total_position_pct"))
        effective_buy_cap = max(current_position_pct, min(single_position_cap, total_position_cap or single_position_cap))

        action = str(decision.get("action") or "").strip().upper()
        action_detail = str(decision.get("action_detail") or "").strip()
        try:
            action_ratio_pct = float(decision.get("action_ratio_pct")) if decision.get("action_ratio_pct") not in (None, "") else None
        except (TypeError, ValueError):
            action_ratio_pct = None

        target_position_pct = current_position_pct
        if action == "BUY":
            planned_delta = action_ratio_pct if action_ratio_pct is not None and action_ratio_pct > 0 else single_position_cap
            target_position_pct = min(effective_buy_cap, current_position_pct + planned_delta)
        elif action == "SELL":
            if action_detail == "清仓":
                target_position_pct = 0.0
            else:
                sell_fraction = action_ratio_pct if action_ratio_pct is not None and action_ratio_pct > 0 else 50.0
                target_position_pct = current_position_pct * max(0.0, 1 - sell_fraction / 100.0)

        if action == "BUY":
            trade_intent = "add" if current_position_pct > 0 else "open"
        elif action == "SELL":
            trade_intent = "exit" if action_detail == "清仓" or self._round_position_pct(target_position_pct) <= 0 else "reduce"
        else:
            trade_intent = "hold" if current_position_pct > 0 else "watch"

        decision["trade_intent"] = trade_intent
        decision["current_position_pct"] = self._round_position_pct(current_position_pct)
        decision["target_position_pct"] = self._round_position_pct(target_position_pct)
        decision["position_delta_pct"] = round(decision["target_position_pct"] - decision["current_position_pct"], 1)
        return decision

    def _compose_reasoning_body(
        self,
        *,
        baseline_clause: str,
        execution_support: str,
        execution_constraint: str,
        change_trigger: str,
        feature_summary: str,
        threshold_clause: str,
    ) -> str:
        segments: List[str] = []
        segments.append(baseline_clause.rstrip("。"))

        evidence_parts: List[str] = []
        if execution_support != "N/A":
            evidence_parts.append(execution_support)
        if execution_constraint != "N/A":
            evidence_parts.append(f"但需留意{execution_constraint}")
        if change_trigger != "N/A":
            evidence_parts.append(f"盘中新出现{change_trigger}")
        if feature_summary != "无分时证据":
            evidence_parts.append(feature_summary)

        if evidence_parts:
            segments.append("，".join(evidence_parts))
        else:
            segments.append("当前缺少足够分时证据")

        segments.append(threshold_clause)
        return "；".join(segment for segment in segments if segment)

    def _normalize_reasoning_output(
        self,
        decision: Dict[str, Any],
        *,
        reasoning_context: Dict[str, Any],
        strategy_context: Optional[Dict[str, Any]],
        has_position: bool,
    ) -> str:
        original_reasoning = str(decision.get("reasoning") or "").strip()
        if not original_reasoning or original_reasoning.startswith("AI响应解析失败:"):
            return original_reasoning
        normalized_reasoning = re.sub(r"\s+", " ", original_reasoning).strip()
        return normalized_reasoning

    def _build_prompt_context(self, stock_code: str, market_data: Dict,
                              account_info: Dict, has_position: bool,
                              session_info: Dict, position_cost: float = 0,
                              position_quantity: int = 0,
                              position_date: Optional[str] = None,
                              can_sell_today: bool = True,
                              account_name: str = DEFAULT_ACCOUNT_NAME,
                              asset_id: Optional[int] = None,
                              portfolio_stock_id: Optional[int] = None,
                              strategy_context: Optional[Dict] = None,
                              risk_profile: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Build template context for intraday decision prompts."""
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        has_strategy_context = isinstance(strategy_context, dict) and bool(strategy_context)
        strategy_profile = normalize_strategy_context(strategy_context or {})
        strategy_context = strategy_profile if has_strategy_context else {}

        def _to_float(value: object) -> Optional[float]:
            if value in (None, ""):
                return None
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return None
            if not math.isfinite(numeric):
                return None
            return numeric

        def _fmt_number(value: object, digits: int = 2, *, signed: bool = False, comma: bool = False) -> str:
            numeric = _to_float(value)
            if numeric is None:
                return "N/A"
            sign = "+" if signed else ""
            separator = "," if comma else ""
            return f"{numeric:{sign}{separator}.{digits}f}"

        def _fmt_money(value: object, *, signed: bool = False) -> str:
            text = _fmt_number(value, digits=2, signed=signed, comma=True)
            return f"¥{text}" if text != "N/A" else "N/A"

        def _fmt_pct(value: object) -> str:
            text = _fmt_number(value, digits=2, signed=True)
            return f"{text}%" if text != "N/A" else "N/A"

        def _fmt_volume(value: object) -> str:
            text = _fmt_number(value, digits=0, comma=True)
            return f"{text}手" if text != "N/A" else "N/A"

        def _volume_state_label(value: object) -> str:
            numeric = _to_float(value)
            if numeric is None:
                return "N/A"
            if numeric > 1.2:
                return "放量"
            if numeric < 0.8:
                return "缩量"
            return "正常"

        def _intraday_position_label(value: object) -> str:
            numeric = _to_float(value)
            if numeric is None:
                return "N/A"
            if numeric >= 85:
                return "接近日内高位"
            if numeric <= 15:
                return "接近日内低位"
            return "处于日内中位"

        def _format_conditions(value: object) -> str:
            if isinstance(value, dict):
                value = [f"{key}: {item}" for key, item in value.items() if str(item or "").strip()]
            if isinstance(value, list):
                items = [self._truncate_prompt_text(item, 80) for item in value if str(item or "").strip()]
            else:
                items = [self._truncate_prompt_text(value, 120)] if str(value or "").strip() else []
            return "；".join(items[:5]) if items else "N/A"

        def _optional_value(value: object) -> str:
            return self._optional_prompt_value(value)

        def _join_non_empty(*values: object, separator: str = " | ") -> str:
            return self._join_prompt_values(*values, separator=separator)

        def _status_overview(timestamp: object, status: object) -> str:
            timestamp_text = _optional_value(timestamp)
            status_text = _optional_value(status)
            if timestamp_text and status_text:
                return f"{timestamp_text} ({status_text})"
            return timestamp_text or status_text

        def _yes_no_text(value: Any) -> str:
            normalized = self._coerce_optional_bool(value)
            if normalized is None:
                return ""
            return "是" if normalized else "否"

        turnover_rate_text = _fmt_pct(market_data.get("turnover_rate"))
        turnover_rate_line = f"换手率: {turnover_rate_text}" if turnover_rate_text != "N/A" else ""
        current_price = _to_float(market_data.get("current_price"))
        current_price_text = _fmt_money(current_price)
        current_volume = _to_float(market_data.get("volume"))
        vol_ma5 = _to_float(market_data.get("vol_ma5"))
        realtime_volume_ratio = _to_float(market_data.get("volume_ratio"))
        trading_progress = self._get_intraday_session_progress(session_info)
        trading_progress_text = f"{trading_progress * 100:.1f}%" if trading_progress is not None else "N/A"
        projected_full_day_volume = None
        projected_volume_ratio_vs_vol_ma5 = None
        if current_volume is not None and trading_progress is not None and 0 < trading_progress <= 1:
            projected_full_day_volume = current_volume / trading_progress
        if projected_full_day_volume is not None and vol_ma5 is not None and vol_ma5 > 0:
            projected_volume_ratio_vs_vol_ma5 = projected_full_day_volume / vol_ma5
        intraday_volume_note = (
            "说明: 盘中累计成交量不能直接与历史全天均量比较，优先参考实时量比，其次参考按当前交易进度折算的全天成交量。"
            if trading_progress is not None and trading_progress < 1
            else "说明: 当前为非连续交易时段或已收盘，可直接参考全天累计成交量与历史均量。"
        )
        intraday_context = market_data.get("intraday_context") if isinstance(market_data.get("intraday_context"), dict) else {}
        timer_section = render_prompt(
            self.SECTION_TIMER_TEMPLATE,
            session_name=session_info["session"],
            beijing_time=session_info.get("beijing_time") or f"{session_info['beijing_hour']:02d}:00",
            volatility=str(session_info["volatility"]).upper(),
            recommendation=session_info["recommendation"],
            can_trade_text="是" if session_info["can_trade"] else "否",
        )

        data_scope_section = render_prompt(self.SECTION_DATA_SCOPE_TEMPLATE)
        realtime_freshness = market_data.get("realtime_freshness") if isinstance(market_data.get("realtime_freshness"), dict) else {}

        def _freshness_label(status: Any) -> str:
            mapping = {
                "ready": "可直接用于盘中执行",
                "review_ready": "可用于盘后复盘",
                "degraded": "可参考但需适度降权",
                "stale": "不适合盘中执行判断",
                "fresh": "新鲜",
                "stale_delay": "延迟过久",
                "stale": "延迟过久",
                "same_day_service_time": "同日服务响应时间",
                "same_day_out_of_session": "同日但非交易时段",
                "same_day_snapshot": "同日盘中快照",
                "cross_day": "跨日旧数据",
                "out_of_session": "时间不在交易时段",
                "unavailable": "不可用",
            }
            return mapping.get(str(status or "").strip(), "未知")

        quote_freshness = realtime_freshness.get("quote") if isinstance(realtime_freshness.get("quote"), dict) else {}
        minute_freshness = realtime_freshness.get("minute") if isinstance(realtime_freshness.get("minute"), dict) else {}
        trade_freshness = realtime_freshness.get("trade") if isinstance(realtime_freshness.get("trade"), dict) else {}
        minute_quality = realtime_freshness.get("minute_quality") if isinstance(realtime_freshness.get("minute_quality"), dict) else {}
        realtime_freshness_section = render_prompt(
            self.SECTION_REALTIME_FRESHNESS_TEMPLATE,
            omit_empty_lines=True,
            asof_time=_optional_value(realtime_freshness.get("asof_time")),
            is_trading_now_text=_yes_no_text(realtime_freshness.get("is_trading_now")),
            intraday_decision_ready_text=_yes_no_text(realtime_freshness.get("intraday_decision_ready")),
            overall_status_text=_optional_value(_freshness_label(realtime_freshness.get("overall_status"))),
            quote_line=_status_overview(
                quote_freshness.get("timestamp"),
                _freshness_label(quote_freshness.get("status")) if quote_freshness.get("status") else "",
            ),
            minute_line=_status_overview(
                minute_freshness.get("timestamp"),
                _freshness_label(minute_freshness.get("status")) if minute_freshness.get("status") else "",
            ),
            trade_line=_status_overview(
                trade_freshness.get("timestamp"),
                _freshness_label(trade_freshness.get("status")) if trade_freshness.get("status") else "",
            ),
            minute_coverage_ratio_text=(
                f"{float(minute_quality.get('coverage_ratio')) * 100:.1f}%"
                if minute_quality.get("coverage_ratio") is not None
                else ""
            ),
            minute_max_gap_text=(
                f"{int(minute_quality.get('max_gap'))} 分钟"
                if minute_quality.get("max_gap") is not None
                else ""
            ),
            minute_quality_text=_optional_value(minute_quality.get("label")),
            freshness_summary=_optional_value(self._truncate_prompt_text(
                realtime_freshness.get("summary"),
                self.MAX_OPTIONAL_SECTION_SUMMARY_CHARS,
            )),
        )

        rsi6_value = _to_float(market_data.get("rsi6"))
        if rsi6_value is not None and rsi6_value > 80:
            rsi6_state = "[超买]"
        elif rsi6_value is not None and rsi6_value < 20:
            rsi6_state = "[超卖]"
        elif rsi6_value is not None:
            rsi6_state = "[正常]"
        else:
            rsi6_state = "[N/A]"

        stock_section = render_prompt(
            self.SECTION_STOCK_TEMPLATE,
            omit_empty_lines=True,
            stock_code=stock_code,
            stock_name=_optional_value(market_data.get("name")),
            data_source=_optional_value(str(market_data.get("data_source", "")).upper()),
            update_time=_optional_value(market_data.get("update_time")),
            current_price=_optional_value(current_price_text),
            change_pct=_optional_value(_fmt_pct(market_data.get("change_pct"))),
            change_amount=_optional_value(_fmt_money(market_data.get("change_amount"), signed=True)),
            high=_optional_value(_fmt_money(market_data.get("high"))),
            low=_optional_value(_fmt_money(market_data.get("low"))),
            open_price=_optional_value(_fmt_money(market_data.get("open"))),
            pre_close=_optional_value(_fmt_money(market_data.get("pre_close"))),
            volume=_optional_value(_fmt_volume(market_data.get("volume"))),
            amount=_optional_value(_fmt_money(market_data.get("amount"))),
        )

        technical_section = render_prompt(
            self.SECTION_TECHNICAL_TEMPLATE,
            omit_empty_lines=True,
            ma5=_optional_value(_fmt_money(market_data.get("ma5"))),
            ma10=_optional_value(_fmt_money(market_data.get("ma10"))),
            ma20=_optional_value(_fmt_money(market_data.get("ma20"))),
            ma60=_optional_value(_fmt_money(market_data.get("ma60"))),
            trend_label=_optional_value(
                "多头排列" if market_data.get("trend") == "up" else "空头排列" if market_data.get("trend") == "down" else ""
            ),
            atr14=_optional_value(_fmt_money(market_data.get("atr14"))),
            atr14_pct=_optional_value(_fmt_pct(market_data.get("atr14_pct"))),
            macd_overview=_join_non_empty(
                f"DIF {_fmt_number(market_data.get('macd_dif'), digits=4)}" if _fmt_number(market_data.get("macd_dif"), digits=4) != "N/A" else "",
                f"DEA {_fmt_number(market_data.get('macd_dea'), digits=4)}" if _fmt_number(market_data.get("macd_dea"), digits=4) != "N/A" else "",
                f"MACD {_fmt_number(market_data.get('macd'), digits=4)}" if _fmt_number(market_data.get("macd"), digits=4) != "N/A" else "",
            ),
            rsi_overview=_join_non_empty(
                f"6 {_fmt_number(market_data.get('rsi6'))}{(' ' + rsi6_state) if rsi6_state != '[N/A]' else ''}" if _fmt_number(market_data.get("rsi6")) != "N/A" else "",
                f"12 {_fmt_number(market_data.get('rsi12'))}" if _fmt_number(market_data.get("rsi12")) != "N/A" else "",
                f"24 {_fmt_number(market_data.get('rsi24'))}" if _fmt_number(market_data.get("rsi24")) != "N/A" else "",
            ),
            kdj_overview=_join_non_empty(
                f"K {_fmt_number(market_data.get('kdj_k'))}" if _fmt_number(market_data.get("kdj_k")) != "N/A" else "",
                f"D {_fmt_number(market_data.get('kdj_d'))}" if _fmt_number(market_data.get("kdj_d")) != "N/A" else "",
                f"J {_fmt_number(market_data.get('kdj_j'))}" if _fmt_number(market_data.get("kdj_j")) != "N/A" else "",
            ),
            boll_overview=_join_non_empty(
                f"上 {_fmt_money(market_data.get('boll_upper'))}" if _fmt_money(market_data.get("boll_upper")) != "N/A" else "",
                f"中 {_fmt_money(market_data.get('boll_mid'))}" if _fmt_money(market_data.get("boll_mid")) != "N/A" else "",
                f"下 {_fmt_money(market_data.get('boll_lower'))}" if _fmt_money(market_data.get("boll_lower")) != "N/A" else "",
                f"位置 {market_data.get('boll_position')}" if str(market_data.get("boll_position") or "").strip() else "",
            ),
        )

        volume_section = render_prompt(
            self.SECTION_VOLUME_TEMPLATE,
            omit_empty_lines=True,
            current_volume=_optional_value(_fmt_volume(current_volume)),
            trading_progress=_optional_value(trading_progress_text),
            projected_full_day_volume=_optional_value(_fmt_volume(projected_full_day_volume)),
            vol_ma5=_optional_value(_fmt_volume(market_data.get("vol_ma5"))),
            realtime_volume_overview=_join_non_empty(
                _optional_value(_fmt_number(realtime_volume_ratio)),
                f"({_volume_state_label(realtime_volume_ratio)})" if _volume_state_label(realtime_volume_ratio) != "N/A" else "",
                separator=" ",
            ),
            projected_volume_overview=_join_non_empty(
                _optional_value(_fmt_number(projected_volume_ratio_vs_vol_ma5)),
                f"({_volume_state_label(projected_volume_ratio_vs_vol_ma5)})" if _volume_state_label(projected_volume_ratio_vs_vol_ma5) != "N/A" else "",
                separator=" ",
            ),
            intraday_volume_note=intraday_volume_note,
            turnover_rate_line=turnover_rate_line,
        )

        execution_context_section = render_prompt(
            self.SECTION_EXECUTION_CONTEXT_TEMPLATE,
            omit_empty_lines=True,
            available_cash=f"¥{account_info.get('available_cash', 0):,.2f}",
            total_value=f"¥{account_info.get('total_value', 0):,.2f}",
            total_market_value=f"¥{account_info.get('total_market_value', 0):,.2f}",
            position_usage_pct=f"{account_info.get('position_usage_pct', 0) * 100:.2f}%",
            positions_count=account_info.get("positions_count", 0),
            account_name=account_name,
        )

        account_risk_profile_section = render_prompt(
            self.SECTION_ACCOUNT_RISK_PROFILE_TEMPLATE,
            omit_empty_lines=True,
            position_size_pct=resolved_risk_profile["position_size_pct"],
            total_position_pct=resolved_risk_profile["total_position_pct"],
            stop_loss_pct=resolved_risk_profile["stop_loss_pct"],
            take_profit_pct=resolved_risk_profile["take_profit_pct"],
        )
        optional_sections: List[str] = []
        evidence_summary: Optional[Dict[str, str]] = None
        cross_layer_summary = self._build_cross_layer_evidence_summary(
            has_position=has_position,
            strategy_context=strategy_context,
            evidence_summary=evidence_summary,
        )
        intraday_momentum_state = "N/A"
        intraday_volume_structure = "N/A"
        acceptance_state = "N/A"
        take_profit_adjustment_hint = "N/A"
        if intraday_context:
            intraday_momentum_state = self._derive_intraday_momentum_state(intraday_context)
            intraday_volume_structure = self._derive_intraday_volume_structure(intraday_context)
            acceptance_state = self._derive_intraday_acceptance_state(intraday_context)
            take_profit_adjustment_hint = self._derive_take_profit_adjustment_hint(intraday_context, has_position)
            evidence_summary = self._route_intraday_evidence(
                labels=intraday_context.get("intraday_signal_labels") if isinstance(intraday_context.get("intraday_signal_labels"), list) else [],
                observations=[
                    *(intraday_context.get("intraday_observations") if isinstance(intraday_context.get("intraday_observations"), list) else []),
                    intraday_momentum_state,
                    intraday_volume_structure,
                    acceptance_state,
                    take_profit_adjustment_hint,
                ],
                has_position=has_position,
                freshness_status=str(realtime_freshness.get("overall_status") or "").strip().lower(),
                previous_labels=[],
                current_bias_text=str(intraday_context.get("intraday_bias_text") or ""),
            )
            cross_layer_summary = self._build_cross_layer_evidence_summary(
                has_position=has_position,
                strategy_context=strategy_context,
                evidence_summary=evidence_summary,
            )
        if strategy_context:
            summary_text = self._build_strategy_summary_brief(strategy_context)
            feature_beacons_text = "、".join(
                self._normalize_feature_beacons(strategy_context.get("feature_beacons"))
            ) or "无"
            optional_sections.append(render_prompt(
                self.SECTION_STRATEGY_CONTEXT_TEMPLATE,
                omit_empty_lines=True,
                analysis_meta=_join_non_empty(
                    strategy_context.get("analysis_date"),
                    strategy_context.get("analysis_source"),
                ),
                rating=_optional_value(strategy_context.get("rating")),
                swing_type=_optional_value(strategy_context.get("swing_type")),
                holding_period=_optional_value(strategy_context.get("holding_period")),
                horizon_meta=_join_non_empty(
                    strategy_context.get("swing_horizon_label"),
                    strategy_context.get("swing_horizon_days_text"),
                ),
                strategy_style_summary=_optional_value(strategy_context.get("strategy_style_summary")),
                baseline_exit_style=_optional_value(strategy_context.get("baseline_exit_style")),
                intraday_execution_preference=_optional_value(strategy_context.get("intraday_execution_preference")),
                swing_type_reason=_optional_value(strategy_context.get("swing_type_reason")),
                summary=_optional_value(summary_text),
                thresholds_line=_join_non_empty(
                    (
                        f"进场 {strategy_context.get('entry_min')} - {strategy_context.get('entry_max')}"
                        if not self._is_missing_prompt_value(strategy_context.get("entry_min"))
                        and not self._is_missing_prompt_value(strategy_context.get("entry_max"))
                        else ""
                    ),
                    f"止盈 {strategy_context.get('take_profit')}" if not self._is_missing_prompt_value(strategy_context.get("take_profit")) else "",
                    f"止损 {strategy_context.get('stop_loss')}" if not self._is_missing_prompt_value(strategy_context.get("stop_loss")) else "",
                ),
                execution_plan_summary=_optional_value(strategy_context.get("execution_plan_summary")),
                entry_conditions_text=_optional_value(_format_conditions(strategy_context.get("entry_conditions"))),
                exit_conditions_text=_optional_value(_format_conditions(strategy_context.get("exit_conditions"))),
                hold_conditions_text=_optional_value(_format_conditions(strategy_context.get("hold_conditions"))),
                invalidation_conditions_text=_optional_value(_format_conditions(strategy_context.get("invalidation_conditions"))),
                structure_state=_optional_value(strategy_context.get("structure_state")),
                atr_overview=_join_non_empty(
                    _optional_value(_fmt_money(strategy_context.get("atr14"))),
                    _optional_value(_fmt_pct(strategy_context.get("atr14_pct"))),
                ),
                trend_anchor_line=_join_non_empty(
                    strategy_context.get("trend_anchor_type"),
                    _fmt_money(strategy_context.get("trend_anchor_value")),
                    separator=" @ ",
                ),
                trend_following_active_text=(
                    _yes_no_text(strategy_context.get("trend_following_active"))
                ),
                feature_beacons_text=feature_beacons_text,
            ))
        if intraday_context or cross_layer_summary:
            optional_sections.append(render_prompt(
                self.SECTION_INTRADAY_FLOW_TEMPLATE,
                omit_empty_lines=True,
                minute_coverage_overview=_join_non_empty(
                    (
                        f"{float(intraday_context.get('minute_coverage_ratio')) * 100:.1f}%"
                        if intraday_context.get("minute_coverage_ratio") is not None
                        else ""
                    ),
                    (
                        f"{int(intraday_context.get('max_minute_gap'))} 分钟"
                        if intraday_context.get("max_minute_gap") is not None
                        else ""
                    ),
                ),
                intraday_vwap=_optional_value(_fmt_money(intraday_context.get("intraday_vwap"))),
                price_position_summary=(
                    f"{_fmt_number(intraday_context.get('price_position_pct'))}% ({_intraday_position_label(intraday_context.get('price_position_pct'))})"
                    if _fmt_number(intraday_context.get("price_position_pct")) != "N/A"
                    else ""
                ),
                momentum_overview=_join_non_empty(
                    _optional_value(_fmt_pct(intraday_context.get("last_15m_change_pct"))),
                    _optional_value(_fmt_pct(intraday_context.get("last_30m_change_pct"))),
                    _optional_value(_fmt_pct(intraday_context.get("last_60m_change_pct"))),
                    separator=" / ",
                ),
                last_5m_overview=_join_non_empty(
                    _optional_value(_fmt_pct(intraday_context.get("last_5m_change_pct"))),
                    (
                        f"量能加速度 {_fmt_number(intraday_context.get('volume_acceleration_ratio'))} ({_volume_state_label(intraday_context.get('volume_acceleration_ratio'))})"
                        if _fmt_number(intraday_context.get("volume_acceleration_ratio")) != "N/A"
                        else ""
                    ),
                ),
                volume_ratio_overview=_join_non_empty(
                    _optional_value(_fmt_number(intraday_context.get("volume_ratio_15m"))),
                    _optional_value(_fmt_number(intraday_context.get("volume_ratio_30m"))),
                    _optional_value(_fmt_number(intraday_context.get("volume_ratio_60m"))),
                    separator=" / ",
                ),
                intraday_volume_structure=_optional_value(intraday_volume_structure),
                intraday_momentum_state=_optional_value(intraday_momentum_state),
                acceptance_state=_optional_value(acceptance_state),
                take_profit_adjustment_hint=_optional_value(take_profit_adjustment_hint),
                intraday_bias_text=_optional_value(self._truncate_prompt_text(intraday_context.get("intraday_bias_text"), 90)),
                primary_evidence=_optional_value((evidence_summary or {}).get("primary_evidence")),
                counter_evidence=_optional_value((evidence_summary or {}).get("counter_evidence")),
                delta_evidence=_optional_value((evidence_summary or {}).get("delta_evidence")),
                baseline_anchor=_optional_value(cross_layer_summary.get("baseline_anchor")),
                execution_focus=_optional_value(cross_layer_summary.get("execution_focus")),
                alignment_summary=_optional_value(cross_layer_summary.get("alignment_summary")),
                execution_support=_optional_value(cross_layer_summary.get("execution_support")),
                execution_constraint=_optional_value(cross_layer_summary.get("execution_constraint")),
                change_trigger=_optional_value(cross_layer_summary.get("change_trigger")),
            ))
        # --- 注入语义化标签分析 ---
        labels = market_data.get('semantic_labels', [])
        if labels:
            normalized_labels = [str(label).strip() for label in labels if str(label).strip()]
            limited_labels = normalized_labels[: self.MAX_SEMANTIC_LABELS]
            if len(normalized_labels) > self.MAX_SEMANTIC_LABELS:
                limited_labels.append(f"其余{len(normalized_labels) - self.MAX_SEMANTIC_LABELS}条标签已省略")
            optional_sections.append(render_prompt(
                self.SECTION_AI_PATTERN_RECOGNITION_TEMPLATE,
                labels_block="\n".join(f"- {label}" for label in limited_labels),
            ))

        # 如果已持有该股票：即使成本缺失，也要显式注入持仓段落，避免模型误判为“无持仓”
        if has_position and position_quantity > 0:
            current_total = current_price * position_quantity if current_price is not None else None
            current_total_text = f"¥{current_total:,.2f}" if current_total is not None else "N/A"
            current_price_text = _fmt_money(current_price)
            resolved_position_date = position_date or ((account_info.get("current_position") or {}).get("position_date")) or "N/A"
            estimated_holding_days = self._estimate_holding_trading_days(
                position_date=resolved_position_date,
                market_data=market_data,
            )
            holding_days_text = (
                f"第{estimated_holding_days}个交易日（估算）"
                if estimated_holding_days is not None
                else "N/A"
            )

            position_cost_text = "N/A"
            profit_loss_text = "N/A"
            profit_loss_pct_value: Optional[float] = None
            if position_cost and position_cost > 0:
                position_cost_text = f"¥{position_cost:.2f}"
                cost_total = position_cost * position_quantity
                profit_loss = (current_total - cost_total) if current_total is not None else None
                profit_loss_pct_value = (profit_loss / cost_total * 100) if profit_loss is not None and cost_total > 0 else None
                if profit_loss is not None and profit_loss_pct_value is not None:
                    profit_loss_text = f"¥{profit_loss:,.2f} ({profit_loss_pct_value:+.2f}%)"

            runtime_metrics = self._derive_position_runtime_metrics(
                market_data=market_data,
                strategy_context=strategy_context,
                holding_days=estimated_holding_days,
                profit_loss_pct=profit_loss_pct_value,
            )
            feature_beacons_text = "、".join(runtime_metrics["feature_beacons"]) if runtime_metrics["feature_beacons"] else "无"
            trend_anchor_text = (
                f'{runtime_metrics["trend_anchor_type"]} @ {_fmt_money(runtime_metrics["trend_anchor_value"])}'
                if runtime_metrics.get("trend_anchor_value") is not None
                else runtime_metrics["trend_anchor_type"]
            )

            position_section = render_prompt(
                self.SECTION_POSITION_HOLDING_TEMPLATE,
                omit_empty_lines=True,
                stock_code=stock_code,
                position_date=_optional_value(resolved_position_date),
                holding_days_text=_optional_value(holding_days_text),
                swing_type_label=strategy_context.get("swing_horizon_label", "未明确"),
                swing_horizon_days_text=strategy_context.get("swing_horizon_days_text", "未明确"),
                strategy_style_summary=strategy_context.get("strategy_style_summary", "未明确"),
                baseline_exit_style=strategy_context.get("baseline_exit_style", "未明确"),
                can_sell_today_text="是" if can_sell_today else "否（T+1限制）",
                position_quantity=position_quantity,
                position_cost=_optional_value(position_cost_text),
                current_price=_optional_value(current_price_text),
                current_total=_optional_value(current_total_text),
                profit_loss_text=_optional_value(profit_loss_text),
                stop_loss_pct=resolved_risk_profile["stop_loss_pct"],
                atr14_text=_optional_value(_fmt_money(runtime_metrics.get("atr14"))),
                atr14_pct_text=_optional_value(_fmt_pct(runtime_metrics.get("atr14_pct"))),
                atr_stop_floor_text=_optional_value(_fmt_money(runtime_metrics.get("atr_stop_floor"))),
                trend_anchor_text=_optional_value(trend_anchor_text),
                feature_beacons_text=feature_beacons_text,
            )
        else:
            position_section = render_prompt(
                self.SECTION_POSITION_EMPTY_TEMPLATE,
                omit_empty_lines=True,
                position_size_pct=resolved_risk_profile["position_size_pct"],
            )

        # 主力资金数据（已禁用 - 接口不稳定）
        # if 'main_force' in market_data:
        #     mf = market_data['main_force']
        #     prompt += f"""
        # [MONEY] 主力资金流向
        # ═══════════════════════════════════════════════════════════
        # 主力净额: ¥{mf.get('main_net', 0):,.2f}万 ({mf.get('main_net_pct', 0):+.2f}%)
        # 超大单: ¥{mf.get('super_net', 0):,.2f}万
        # 大单: ¥{mf.get('big_net', 0):,.2f}万
        # 中单: ¥{mf.get('mid_net', 0):,.2f}万
        # 小单: ¥{mf.get('small_net', 0):,.2f}万
        # 主力动向: {mf.get('trend', '观望')}
        # """

        normalized_optional_sections = [section.strip() for section in optional_sections if str(section).strip()]
        optional_sections_text = "\n\n".join(normalized_optional_sections)
        strategy_execution_preference = (
            str(strategy_profile.get("intraday_execution_preference") or "").strip()
            if has_strategy_context
            else ""
        )
        if has_position:
            position_mode_label = "持仓波段管理模式"
            position_mode_allowed_actions = "BUY / SELL / HOLD"
            position_mode_forbidden_actions = "做空、日内回转、忽视T+1的卖出"
            position_mode_focus = "先判断止损与风险收缩是否触发，再区分回踩确认加仓、突破确认加仓、主动减仓锁盈或继续持有"
            if strategy_execution_preference:
                position_mode_focus = f"{position_mode_focus}；当前基线更偏{strategy_execution_preference}"
        else:
            position_mode_label = "空仓建仓模式"
            position_mode_allowed_actions = "BUY / HOLD"
            position_mode_forbidden_actions = "SELL、减仓、止盈卖出"
            position_mode_focus = "优先结合战略基线与盘中信号判断，条件匹配时可执行 BUY"
            if strategy_execution_preference:
                position_mode_focus = f"{position_mode_focus}；当前基线更偏{strategy_execution_preference}"
        return {
            "position_size_pct": str(resolved_risk_profile["position_size_pct"]),
            "total_position_pct": str(resolved_risk_profile["total_position_pct"]),
            "stop_loss_pct": str(resolved_risk_profile["stop_loss_pct"]),
            "take_profit_pct": str(resolved_risk_profile["take_profit_pct"]),
            "stop_loss_pct_float": f"{float(resolved_risk_profile['stop_loss_pct']):.1f}",
            "take_profit_pct_float": f"{float(resolved_risk_profile['take_profit_pct']):.1f}",
            "swing_type_label": (
                str(strategy_profile.get("swing_type") or "未明确")
                if has_strategy_context
                else "未明确"
            ),
            "swing_horizon_days_text": (
                str(strategy_profile.get("swing_horizon_days_text") or "未明确")
                if has_strategy_context
                else "未明确"
            ),
            "strategy_style_summary": (
                str(strategy_profile.get("strategy_style_summary") or "未明确")
                if has_strategy_context
                else "未明确"
            ),
            "baseline_exit_style": (
                str(strategy_profile.get("baseline_exit_style") or "未明确")
                if has_strategy_context
                else "未明确"
            ),
            "position_mode_label": position_mode_label,
            "position_mode_allowed_actions": position_mode_allowed_actions,
            "position_mode_forbidden_actions": position_mode_forbidden_actions,
            "position_mode_focus": position_mode_focus,
            "timer_section": timer_section.strip(),
            "data_scope_section": data_scope_section,
            "realtime_freshness_section": realtime_freshness_section.strip(),
            "stock_section": stock_section.strip(),
            "technical_section": technical_section.strip(),
            "volume_section": volume_section.strip(),
            "execution_context_section": execution_context_section.strip(),
            "account_risk_profile_section": account_risk_profile_section.strip(),
            "optional_sections": optional_sections_text,
            "_optional_sections_list": normalized_optional_sections,
            "position_section": position_section.strip(),
        }

    def _build_prompt_messages(self, stock_code: str, market_data: Dict,
                               account_info: Dict, has_position: bool,
                               session_info: Dict, position_cost: float = 0,
                               position_quantity: int = 0,
                               position_date: Optional[str] = None,
                               can_sell_today: bool = True,
                               account_name: str = DEFAULT_ACCOUNT_NAME,
                               asset_id: Optional[int] = None,
                               portfolio_stock_id: Optional[int] = None,
                               strategy_context: Optional[Dict] = None,
                               risk_profile: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
        context = self._build_prompt_context(
            stock_code, market_data, account_info,
            has_position, session_info, position_cost, position_quantity,
            position_date=position_date,
            can_sell_today=can_sell_today,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            strategy_context=strategy_context,
            risk_profile=risk_profile,
        )
        optional_sections = context.pop("_optional_sections_list", [])
        messages = build_messages(self.SYSTEM_TEMPLATE, self.USER_TEMPLATE, **context)
        messages_budget = max(0, self.PROMPT_SIZE_WARNING_CHARS - self.PROMPT_SIZE_SAFETY_MARGIN_CHARS)

        if optional_sections and self._estimate_request_payload_chars(messages) > messages_budget:
            compact_context = dict(context)
            compact_context["optional_sections"] = ""
            base_messages = build_messages(self.SYSTEM_TEMPLATE, self.USER_TEMPLATE, **compact_context)
            remaining_budget = max(0, messages_budget - self._estimate_request_payload_chars(base_messages))
            compact_context["optional_sections"] = self._fit_optional_sections_to_budget(optional_sections, remaining_budget)
            messages = build_messages(self.SYSTEM_TEMPLATE, self.USER_TEMPLATE, **compact_context)

        estimated_chars = self._estimate_request_payload_chars(messages)
        raw_strategy_summary = ""
        if isinstance(strategy_context, dict):
            raw_strategy_summary = str(strategy_context.get("summary") or "")
        should_compact_system = (
            len(raw_strategy_summary) > self.MAX_STRATEGY_SUMMARY_CHARS * 2
            and estimated_chars > self.PROMPT_BUILD_TARGET_CHARS
        )
        if should_compact_system and messages:
            compact_messages = [dict(message) for message in messages]
            system_content = str(compact_messages[0].get("content") or "")
            excess_chars = estimated_chars - self.PROMPT_BUILD_TARGET_CHARS
            system_budget = max(2800, len(system_content) - excess_chars - 120)
            compact_messages[0]["content"] = self._compact_system_prompt(system_content, system_budget)
            messages = compact_messages

        return messages

    def _build_a_stock_prompt(self, stock_code: str, market_data: Dict,
                              account_info: Dict, has_position: bool,
                              session_info: Dict, position_cost: float = 0,
                              position_quantity: int = 0,
                              position_date: Optional[str] = None,
                              can_sell_today: bool = True,
                              account_name: str = DEFAULT_ACCOUNT_NAME,
                              asset_id: Optional[int] = None,
                              portfolio_stock_id: Optional[int] = None,
                              strategy_context: Optional[Dict] = None,
                              risk_profile: Optional[Dict[str, Any]] = None) -> str:
        """构建A股分析提示词。"""
        return self._build_prompt_messages(
            stock_code, market_data, account_info,
            has_position, session_info, position_cost, position_quantity,
            position_date=position_date,
            can_sell_today=can_sell_today,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            strategy_context=strategy_context,
            risk_profile=risk_profile,
        )[1]["content"]

    @staticmethod
    def _iter_json_candidates(ai_response: str) -> List[str]:
        text = str(ai_response or "").strip()
        if not text:
            return []

        candidates: List[str] = []
        for match in re.finditer(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE):
            candidate = str(match.group(1) or "").strip()
            if candidate:
                candidates.append(candidate)

        braced = SmartMonitorDeepSeek._extract_balanced_braces(text)
        if braced:
            candidates.append(braced)

        candidates.append(text)

        deduped: List[str] = []
        seen = set()
        for candidate in candidates:
            normalized = candidate.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _extract_balanced_braces(text: str) -> Optional[str]:
        for start_index, char in enumerate(text):
            if char != "{":
                continue
            depth = 0
            quote_char = ""
            escape = False
            for index in range(start_index, len(text)):
                current_char = text[index]
                if quote_char:
                    if escape:
                        escape = False
                    elif current_char == "\\":
                        escape = True
                    elif current_char == quote_char:
                        quote_char = ""
                    continue
                if current_char in {'"', "'"}:
                    quote_char = current_char
                    continue
                if current_char == "{":
                    depth += 1
                elif current_char == "}":
                    depth -= 1
                    if depth == 0:
                        return text[start_index:index + 1]
        return None

    @staticmethod
    def _strip_json_comments(text: str) -> str:
        result: List[str] = []
        quote_char = ""
        escape = False
        index = 0
        while index < len(text):
            current_char = text[index]
            next_char = text[index + 1] if index + 1 < len(text) else ""
            if quote_char:
                result.append(current_char)
                if escape:
                    escape = False
                elif current_char == "\\":
                    escape = True
                elif current_char == quote_char:
                    quote_char = ""
                index += 1
                continue

            if current_char in {'"', "'"}:
                quote_char = current_char
                result.append(current_char)
                index += 1
                continue

            if current_char == "/" and next_char == "/":
                index += 2
                while index < len(text) and text[index] not in "\r\n":
                    index += 1
                continue

            if current_char == "/" and next_char == "*":
                index += 2
                while index + 1 < len(text) and text[index:index + 2] != "*/":
                    index += 1
                index += 2
                continue

            result.append(current_char)
            index += 1
        return "".join(result)

    @staticmethod
    def _quote_unquoted_keys(text: str) -> str:
        pattern = re.compile(r'([{\[,]\s*)([A-Za-z_\u4e00-\u9fff][A-Za-z0-9_\-\u4e00-\u9fff]*)(\s*:)')
        return pattern.sub(r'\1"\2"\3', text)

    @staticmethod
    def _quote_known_string_values(text: str) -> str:
        replacements = {
            "action": r"BUY|SELL|HOLD|买入|卖出|持有|观望|等待|加仓|减仓|止盈|止损",
            "action_detail": r"建仓|加仓|买入|减仓|清仓|卖出|持有|观望|等待",
            "swing_execution_mode": (
                r"pullback_entry|breakout_entry|pullback_add|breakout_add|proactive_trim|defensive_trim|"
                r"defensive_exit|trend_hold|watch_hold|回踩建仓|突破建仓|回踩确认加仓|突破确认加仓|"
                r"主动减仓锁盈|防守减仓|防守清仓|趋势持有|观察持有"
            ),
            "risk_level": r"low|medium|high|低|中|高",
        }
        normalized = text
        for field, options in replacements.items():
            pattern = re.compile(
                rf'("{field}"\s*:\s*)(?P<value>{options})(\s*[,}}])',
                re.IGNORECASE,
            )
            normalized = pattern.sub(r'\1"\g<value>"\3', normalized)
        return normalized

    @staticmethod
    def _strip_trailing_commas(text: str) -> str:
        return re.sub(r",\s*([}\]])", r"\1", text)

    @staticmethod
    def _replace_json_literals_for_python(text: str) -> str:
        replacements = {"true": "True", "false": "False", "null": "None"}
        result: List[str] = []
        quote_char = ""
        escape = False
        index = 0
        while index < len(text):
            current_char = text[index]
            if quote_char:
                result.append(current_char)
                if escape:
                    escape = False
                elif current_char == "\\":
                    escape = True
                elif current_char == quote_char:
                    quote_char = ""
                index += 1
                continue

            if current_char in {'"', "'"}:
                quote_char = current_char
                result.append(current_char)
                index += 1
                continue

            replaced = False
            for source, target in replacements.items():
                end_index = index + len(source)
                if (
                    text[index:end_index] == source
                    and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_"))
                    and (end_index >= len(text) or not (text[end_index].isalnum() or text[end_index] == "_"))
                ):
                    result.append(target)
                    index = end_index
                    replaced = True
                    break
            if replaced:
                continue

            result.append(current_char)
            index += 1
        return "".join(result)

    @staticmethod
    def _sanitize_json_like_text(text: str) -> str:
        translation = str.maketrans({
            "“": '"',
            "”": '"',
            "‘": "'",
            "’": "'",
            "，": ",",
            "：": ":",
            "；": ";",
        })
        sanitized = str(text or "").strip().translate(translation)
        sanitized = SmartMonitorDeepSeek._strip_json_comments(sanitized)
        sanitized = SmartMonitorDeepSeek._quote_unquoted_keys(sanitized)
        sanitized = SmartMonitorDeepSeek._quote_known_string_values(sanitized)
        sanitized = SmartMonitorDeepSeek._strip_trailing_commas(sanitized)
        return sanitized

    @staticmethod
    def _coerce_numeric(value: Any, *, default: float = 0.0, scale_fraction_to_pct: bool = False) -> float:
        if isinstance(value, bool):
            return float(default)
        if isinstance(value, (int, float)):
            number = float(value)
        else:
            text = str(value or "").replace(",", "").strip()
            match = re.search(r"-?\d+(?:\.\d+)?", text)
            if not match:
                return float(default)
            number = float(match.group(0))
        if scale_fraction_to_pct and 0 <= number <= 1:
            number *= 100
        return number

    @staticmethod
    def _normalize_action_value(value: Any) -> str:
        text = str(value or "").strip().upper()
        mapping = {
            "BUY": "BUY",
            "买入": "BUY",
            "加仓": "BUY",
            "建仓": "BUY",
            "SELL": "SELL",
            "卖出": "SELL",
            "减仓": "SELL",
            "止盈": "SELL",
            "止损": "SELL",
            "HOLD": "HOLD",
            "持有": "HOLD",
            "观望": "HOLD",
            "等待": "HOLD",
        }
        return mapping.get(text, "HOLD")

    @staticmethod
    def _normalize_action_detail_value(value: Any) -> str:
        text = str(value or "").strip()
        mapping = {
            "BUY": "买入",
            "建仓": "建仓",
            "买入": "买入",
            "加仓": "加仓",
            "SELL": "卖出",
            "卖出": "卖出",
            "减仓": "减仓",
            "清仓": "清仓",
            "止盈": "减仓",
            "止损": "清仓",
            "HOLD": "持有",
            "持有": "持有",
            "观望": "观望",
            "等待": "观望",
        }
        return mapping.get(text, "")

    @staticmethod
    def _normalize_swing_execution_mode_value(value: Any) -> str:
        text = str(value or "").strip().lower()
        mapping = {
            "pullback_entry": "pullback_entry",
            "回踩建仓": "pullback_entry",
            "breakout_entry": "breakout_entry",
            "突破建仓": "breakout_entry",
            "pullback_add": "pullback_add",
            "回踩确认加仓": "pullback_add",
            "回踩加仓": "pullback_add",
            "breakout_add": "breakout_add",
            "突破确认加仓": "breakout_add",
            "突破加仓": "breakout_add",
            "proactive_trim": "proactive_trim",
            "主动减仓锁盈": "proactive_trim",
            "主动减仓": "proactive_trim",
            "锁盈减仓": "proactive_trim",
            "defensive_trim": "defensive_trim",
            "防守减仓": "defensive_trim",
            "风险减仓": "defensive_trim",
            "defensive_exit": "defensive_exit",
            "防守清仓": "defensive_exit",
            "防守退出": "defensive_exit",
            "trend_hold": "trend_hold",
            "趋势持有": "trend_hold",
            "继续持有": "trend_hold",
            "watch_hold": "watch_hold",
            "观察持有": "watch_hold",
            "等待观察": "watch_hold",
            "观望等待": "watch_hold",
        }
        return mapping.get(text, "")

    @classmethod
    def _resolve_action_detail(cls, value: Any, *, action: str, has_position: bool) -> str:
        normalized_action = cls._normalize_action_value(action)
        normalized_detail = cls._normalize_action_detail_value(value)
        if normalized_action == "BUY":
            if normalized_detail in {"建仓", "加仓", "买入"}:
                return "加仓" if has_position and normalized_detail != "建仓" else "建仓" if not has_position else "加仓"
            return "加仓" if has_position else "建仓"
        if normalized_action == "SELL":
            if normalized_detail in {"减仓", "清仓", "卖出"}:
                return normalized_detail
            return "卖出"
        if normalized_detail in {"持有", "观望"}:
            return normalized_detail
        return "持有" if has_position else "观望"

    @classmethod
    def _resolve_swing_execution_mode(
        cls,
        value: Any,
        *,
        action: str,
        action_detail: str,
        has_position: bool,
        reasoning: Any = None,
    ) -> str:
        normalized_action = cls._normalize_action_value(action)
        normalized_detail = cls._resolve_action_detail(action_detail, action=normalized_action, has_position=has_position)
        explicit = cls._normalize_swing_execution_mode_value(value)
        reasoning_text = str(reasoning or "")

        if normalized_action == "BUY":
            allowed = {"pullback_add", "breakout_add"} if has_position else {"pullback_entry", "breakout_entry"}
            if explicit in allowed:
                return explicit
            pullback_markers = ("回踩", "支撑", "企稳", "均线", "成本区", "缩量回踩", "回踩确认")
            breakout_markers = ("突破", "站稳", "新高", "压力位", "放量突破", "突破确认")
            if any(marker in reasoning_text for marker in pullback_markers):
                return "pullback_add" if has_position else "pullback_entry"
            if any(marker in reasoning_text for marker in breakout_markers):
                return "breakout_add" if has_position else "breakout_entry"
            return "pullback_add" if has_position else "pullback_entry"

        if normalized_action == "SELL":
            if normalized_detail == "清仓":
                return "defensive_exit"
            allowed = {"proactive_trim", "defensive_trim"}
            if explicit in allowed:
                return explicit
            proactive_trim_markers = ("锁盈", "止盈", "偏离", "过热", "背离", "兑现", "高位震荡")
            defensive_markers = ("破位", "走坏", "转弱", "承接恶化", "风控", "止损", "失守", "风险扩大", "放量回落")
            if any(marker in reasoning_text for marker in proactive_trim_markers):
                return "proactive_trim"
            if any(marker in reasoning_text for marker in defensive_markers):
                return "defensive_trim"
            return "defensive_trim"

        allowed = {"trend_hold", "watch_hold"}
        if explicit in allowed:
            return explicit
        trend_hold_markers = ("趋势未坏", "继续持有", "结构未坏", "承接仍在", "上修止盈", "继续跟踪")
        watch_hold_markers = ("观望", "等待", "约束", "t+1", "不追高", "证据不足", "受限")
        if any(marker in reasoning_text for marker in trend_hold_markers):
            return "trend_hold"
        if any(marker in reasoning_text.lower() for marker in watch_hold_markers):
            return "watch_hold"
        return "trend_hold" if has_position else "watch_hold"

    @classmethod
    def _resolve_action_ratio_pct(
        cls,
        value: Any,
        *,
        action: str,
        action_detail: str,
        has_position: bool,
        position_size_pct: Any = None,
        swing_execution_mode: Any = None,
    ) -> Optional[int]:
        try:
            numeric = int(round(cls._coerce_numeric(value, default=0.0)))
        except (TypeError, ValueError):
            numeric = 0

        normalized_action = cls._normalize_action_value(action)
        normalized_detail = cls._resolve_action_detail(action_detail, action=normalized_action, has_position=has_position)
        normalized_swing_mode = cls._normalize_swing_execution_mode_value(swing_execution_mode)
        try:
            fallback_position_size = int(round(cls._coerce_numeric(position_size_pct, default=20.0)))
        except (TypeError, ValueError):
            fallback_position_size = 20

        if normalized_action == "HOLD":
            return None
        if normalized_action == "SELL":
            if normalized_detail == "清仓":
                return 100
            if numeric <= 0:
                if normalized_swing_mode == "proactive_trim":
                    numeric = 25
                elif normalized_swing_mode == "defensive_trim":
                    numeric = 35
                else:
                    numeric = 30
            return max(1, min(99, numeric))
        if normalized_action == "BUY":
            if numeric <= 0:
                numeric = fallback_position_size if normalized_detail == "建仓" else min(fallback_position_size, 20)
            return max(1, min(100, numeric))
        return None

    @staticmethod
    def _normalize_risk_level(value: Any) -> str:
        text = str(value or "").strip().lower()
        mapping = {
            "low": "low",
            "低": "low",
            "medium": "medium",
            "中": "medium",
            "high": "high",
            "高": "high",
        }
        return mapping.get(text, "medium")

    @staticmethod
    def _normalize_key_price_levels(value: Any) -> Dict[str, float]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, float] = {}
        for key in ("support", "resistance", "stop_loss"):
            raw_value = value.get(key)
            if raw_value in (None, ""):
                continue
            try:
                normalized[key] = float(SmartMonitorDeepSeek._coerce_numeric(raw_value))
            except (TypeError, ValueError):
                continue
        return normalized

    def _normalize_decision_payload(
        self,
        decision: Dict[str, Any],
        risk_profile: Optional[Dict[str, Any]] = None,
        *,
        has_position: bool = False,
    ) -> Dict[str, Any]:
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        reasoning_text = decision.get("reasoning")
        if isinstance(reasoning_text, (dict, list)):
            reasoning_text = json.dumps(reasoning_text, ensure_ascii=False)
        reasoning = str(reasoning_text or "").strip()
        if not reasoning:
            raise ValueError("缺少必需字段: reasoning")

        normalized: Dict[str, Any] = {
            "action": self._normalize_action_value(decision.get("action")),
            "confidence": int(max(0, min(100, round(self._coerce_numeric(
                decision.get("confidence"),
                default=0,
                scale_fraction_to_pct=True,
            ))))),
            "reasoning": reasoning,
            "action_detail": self._normalize_action_detail_value(decision.get("action_detail")),
            "position_size_pct": int(max(0, min(100, round(self._coerce_numeric(
                decision.get("position_size_pct"),
                default=resolved_risk_profile["position_size_pct"],
            ))))),
            "stop_loss_pct": round(max(0.0, self._coerce_numeric(
                decision.get("stop_loss_pct"),
                default=float(resolved_risk_profile["stop_loss_pct"]),
            )), 2),
            "take_profit_pct": round(max(0.0, self._coerce_numeric(
                decision.get("take_profit_pct"),
                default=float(resolved_risk_profile["take_profit_pct"]),
            )), 2),
            "risk_level": self._normalize_risk_level(decision.get("risk_level")),
            "key_price_levels": self._normalize_key_price_levels(decision.get("key_price_levels")),
            "structure_state": self._normalize_structure_state(decision.get("structure_state")),
            "structure_state_reason": str(decision.get("structure_state_reason") or "").strip(),
            "trend_following_active": bool(self._coerce_optional_bool(decision.get("trend_following_active"))),
            "trend_anchor_type": str(decision.get("trend_anchor_type") or "").strip().upper(),
            "trend_anchor_value": self._coerce_optional_float(decision.get("trend_anchor_value")),
            "atr14": self._coerce_optional_float(decision.get("atr14")),
            "atr14_pct": self._coerce_optional_float(decision.get("atr14_pct")),
            "atr_stop_floor": self._coerce_optional_float(decision.get("atr_stop_floor")),
            "swing_type_upgrade": bool(self._coerce_optional_bool(decision.get("swing_type_upgrade"))),
            "upgraded_swing_type": swing_type_label(decision.get("upgraded_swing_type")),
            "upgrade_reason": str(decision.get("upgrade_reason") or "").strip(),
            "feature_beacons": self._normalize_feature_beacons(decision.get("feature_beacons")),
        }
        normalized["swing_execution_mode"] = self._resolve_swing_execution_mode(
            decision.get("swing_execution_mode"),
            action=normalized["action"],
            action_detail=decision.get("action_detail"),
            has_position=has_position,
            reasoning=reasoning,
        )
        normalized["action_ratio_pct"] = self._resolve_action_ratio_pct(
            decision.get("action_ratio_pct"),
            action=normalized["action"],
            action_detail=normalized["action_detail"],
            has_position=has_position,
            position_size_pct=normalized["position_size_pct"],
            swing_execution_mode=normalized["swing_execution_mode"],
        )

        monitor_levels = self._normalize_monitor_levels(decision)
        if monitor_levels:
            normalized["monitor_levels"] = monitor_levels
        return normalized

    def _salvage_decision_fields(self, text: str) -> Optional[Dict[str, Any]]:
        normalized = self._sanitize_json_like_text(text)
        action_match = re.search(r'(?i)(?:^|[,{]\s*)"action"\s*:\s*"?([A-Za-z\u4e00-\u9fff]+)', normalized)
        confidence_match = re.search(r'(?i)(?:^|[,{]\s*)"confidence"\s*:\s*"?([0-9]+(?:\.[0-9]+)?%?)', normalized)
        reasoning_match = re.search(
            r'(?is)(?:^|[,{]\s*)"reasoning"\s*:\s*"?(.*?)(?:"?\s*(?:,\s*"[A-Za-z_][A-Za-z0-9_]*"\s*:|\}\s*$))',
            normalized,
        )
        if not action_match or not confidence_match or not reasoning_match:
            return None
        return {
            "action": action_match.group(1),
            "confidence": confidence_match.group(1),
            "reasoning": reasoning_match.group(1).strip().strip('"').strip(),
        }

    def _decode_decision_text(self, ai_response: str) -> Dict[str, Any]:
        errors: List[str] = []
        for candidate in self._iter_json_candidates(ai_response):
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, str):
                    parsed = json.loads(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"strict_json: {exc}")

            sanitized = self._sanitize_json_like_text(candidate)
            try:
                parsed = json.loads(sanitized)
                if isinstance(parsed, str):
                    parsed = json.loads(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"sanitized_json: {exc}")

            try:
                python_like = self._replace_json_literals_for_python(sanitized)
                parsed = ast.literal_eval(python_like)
                if isinstance(parsed, str):
                    parsed = ast.literal_eval(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"python_literal: {exc}")

        salvaged = self._salvage_decision_fields(ai_response)
        if salvaged:
            return salvaged

        error_message = errors[-1] if errors else "未找到可解析的JSON对象"
        raise ValueError(error_message)

    def _parse_decision(
        self,
        ai_response: str,
        risk_profile: Optional[Dict[str, Any]] = None,
        *,
        has_position: bool = False,
    ) -> Dict:
        """解析AI决策响应。"""
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        try:
            return self._parse_decision_strict(
                ai_response,
                risk_profile=resolved_risk_profile,
                has_position=has_position,
            )
        except Exception as e:
            self.logger.error("解析AI决策失败: %s; response=%s", e, str(ai_response or "")[:300])
            return self._build_invalid_decision_fallback(
                error=e,
                risk_profile=resolved_risk_profile,
                has_position=has_position,
            )

    def _build_invalid_decision_fallback(
        self,
        *,
        error: Exception,
        risk_profile: Optional[Dict[str, Any]] = None,
        has_position: bool = False,
    ) -> Dict[str, Any]:
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        return {
            'action': 'HOLD',
            'action_detail': '持有' if has_position else '观望',
            'swing_execution_mode': 'watch_hold' if has_position else 'watch_hold',
            'confidence': 0,
            'reasoning': f'AI响应解析/校验失败，已降级为HOLD: {str(error)}',
            'position_size_pct': 0,
            'stop_loss_pct': float(resolved_risk_profile["stop_loss_pct"]),
            'take_profit_pct': float(resolved_risk_profile["take_profit_pct"]),
            'risk_level': 'high',
            'key_price_levels': {},
            'action_ratio_pct': None,
        }

    @staticmethod
    def _reasoning_vetoes_buy(reasoning: Any) -> bool:
        text = str(reasoning or "").strip()
        if not text:
            return False
        veto_patterns = (
            r"暂不(?:执行)?(?:加仓|买入)",
            r"不(?:宜|建议|执行).{0,6}(?:加仓|买入)",
            r"等待.{0,30}(?:再|后再)考虑.{0,12}(?:加仓|买入)",
            r"(?:建议|应|宜)?(?:维持|继续)持有",
            r"暂不执行",
        )
        return any(re.search(pattern, text) for pattern in veto_patterns)

    @staticmethod
    def _reasoning_vetoes_sell(reasoning: Any) -> bool:
        text = str(reasoning or "").strip()
        if not text:
            return False
        veto_patterns = (
            r"暂不(?:执行)?(?:减仓|卖出|清仓)",
            r"不(?:宜|建议|执行).{0,6}(?:减仓|卖出|清仓)",
            r"等待.{0,30}(?:再|后再)考虑.{0,12}(?:减仓|卖出|清仓)",
            r"(?:建议|应|宜)?(?:维持|继续)持有",
            r"暂不执行",
        )
        return any(re.search(pattern, text) for pattern in veto_patterns)

    def _validate_decision_consistency(
        self,
        decision: Dict[str, Any],
        *,
        has_position: bool = False,
    ) -> None:
        action = str(decision.get("action") or "").strip().upper()
        reasoning = str(decision.get("reasoning") or "").strip()
        action_detail = self._resolve_action_detail(
            decision.get("action_detail"),
            action=action,
            has_position=has_position,
        )
        if action == "BUY" and self._reasoning_vetoes_buy(reasoning):
            raise DecisionValidationError(
                "action=BUY 与 reasoning 冲突：理由明确表示暂不执行买入/加仓或应维持持有。"
            )
        if action == "SELL" and self._reasoning_vetoes_sell(reasoning):
            raise DecisionValidationError(
                "action=SELL 与 reasoning 冲突：理由明确表示暂不执行卖出/减仓或应维持持有。"
            )
        if action == "BUY" and action_detail == "建仓" and has_position:
            raise DecisionValidationError("当前已有持仓时，action=BUY 不得输出 action_detail=建仓。")
        if action == "HOLD" and self._normalize_action_detail_value(decision.get("action_detail")) in {"加仓", "建仓", "减仓", "清仓"}:
            raise DecisionValidationError("action=HOLD 时，action_detail 只能是 持有/观望。")

    def _parse_decision_strict(
        self,
        ai_response: str,
        risk_profile: Optional[Dict[str, Any]] = None,
        *,
        has_position: bool = False,
    ) -> Dict[str, Any]:
        resolved_risk_profile = self._resolve_risk_profile(risk_profile)
        decoded = self._decode_decision_text(ai_response)
        normalized = self._normalize_decision_payload(
            decoded,
            risk_profile=resolved_risk_profile,
            has_position=has_position,
        )
        self._validate_decision_consistency(normalized, has_position=has_position)
        return normalized

    @staticmethod
    def _build_repair_messages(
        *,
        messages: List[Dict[str, str]],
        ai_response: str,
        validation_error: str,
    ) -> List[Dict[str, str]]:
        repair_prompt = (
            "上一条回答未通过结构校验。\n"
            f"失败原因：{validation_error}\n"
            "请基于同一份行情与基线信息，重新输出一个自洽的最终执行 JSON。\n"
            "要求：\n"
            "- action 表示最终可执行动作，不是候选动作。\n"
            "- 若理由表示暂不执行、继续持有、等待再考虑，则 action 必须为 HOLD。\n"
            "- 只返回一个 JSON 对象，不要解释，不要 Markdown。"
        )
        return [
            *messages,
            {"role": "assistant", "content": str(ai_response or "")},
            {"role": "user", "content": repair_prompt},
        ]

    @staticmethod
    def _normalize_monitor_levels(decision: Dict) -> Optional[Dict]:
        raw_levels = decision.get("monitor_levels")
        if isinstance(raw_levels, dict):
            candidates = raw_levels
        else:
            candidates = {
                "entry_min": decision.get("entry_min"),
                "entry_max": decision.get("entry_max"),
                "take_profit": decision.get("take_profit"),
                "take_profit_max": decision.get("take_profit_max"),
                "stop_loss": decision.get("stop_loss"),
            }
            entry_range = decision.get("entry_range")
            if isinstance(entry_range, dict):
                candidates["entry_min"] = candidates.get("entry_min") or entry_range.get("min")
                candidates["entry_max"] = candidates.get("entry_max") or entry_range.get("max")

        normalized: Dict[str, float] = {}
        for key in ("entry_min", "entry_max", "take_profit", "stop_loss"):
            value = candidates.get(key)
            if value in (None, ""):
                return None
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError):
                return None
        take_profit_max = candidates.get("take_profit_max")
        if take_profit_max not in (None, ""):
            try:
                normalized["take_profit_max"] = float(take_profit_max)
            except (TypeError, ValueError):
                pass
        if (
            normalized.get("take_profit_max") is not None
            and normalized["take_profit_max"] < normalized["take_profit"]
        ):
            normalized["take_profit_max"] = normalized["take_profit"]
        return normalized

    def _enforce_action_policy(self, decision: Dict, has_position: bool, can_sell_today: bool = True) -> Dict:
        def _append_constraint_reasoning(message: str) -> None:
            original_reasoning = str(decision.get("reasoning") or "").strip()
            if original_reasoning:
                decision["reasoning"] = f"{original_reasoning}\n\n补充说明：{message}"
            else:
                decision["reasoning"] = f"补充说明：{message}"

        allowed_actions = {"BUY", "SELL", "HOLD"}
        action = str(decision.get("action", "HOLD") or "HOLD").upper()
        if action not in allowed_actions:
            decision["action"] = "HOLD"
            _append_constraint_reasoning(
                f"原始动作 {action} 不在允许集合 {sorted(allowed_actions)} 中，已降级为 HOLD。"
            )
            decision["risk_level"] = "high"
            decision["action_detail"] = self._resolve_action_detail(decision.get("action_detail"), action="HOLD", has_position=has_position)
            decision["swing_execution_mode"] = self._resolve_swing_execution_mode(
                decision.get("swing_execution_mode"),
                action="HOLD",
                action_detail=decision["action_detail"],
                has_position=has_position,
                reasoning=decision.get("reasoning"),
            )
            decision["action_ratio_pct"] = None
            return decision

        if not has_position and action == "SELL":
            decision["action"] = "HOLD"
            _append_constraint_reasoning("当前无持仓，SELL 不可执行，已降级为 HOLD。")
            decision["risk_level"] = "high"
            decision["action_detail"] = self._resolve_action_detail(decision.get("action_detail"), action="HOLD", has_position=has_position)
            decision["swing_execution_mode"] = self._resolve_swing_execution_mode(
                decision.get("swing_execution_mode"),
                action="HOLD",
                action_detail=decision["action_detail"],
                has_position=has_position,
                reasoning=decision.get("reasoning"),
            )
            decision["action_ratio_pct"] = None
            return decision

        if has_position and not can_sell_today and action == "SELL":
            decision["action"] = "HOLD"
            _append_constraint_reasoning("受A股T+1限制，今日新开仓位不可卖出，SELL 不可执行，已降级为 HOLD。")
            decision["risk_level"] = "high"
            decision["action_detail"] = self._resolve_action_detail(decision.get("action_detail"), action="HOLD", has_position=has_position)
            decision["swing_execution_mode"] = self._resolve_swing_execution_mode(
                decision.get("swing_execution_mode"),
                action="HOLD",
                action_detail=decision["action_detail"],
                has_position=has_position,
                reasoning=decision.get("reasoning"),
            )
            decision["action_ratio_pct"] = None
            return decision

        decision["action"] = action
        decision["action_detail"] = self._resolve_action_detail(
            decision.get("action_detail"),
            action=action,
            has_position=has_position,
        )
        decision["swing_execution_mode"] = self._resolve_swing_execution_mode(
            decision.get("swing_execution_mode"),
            action=action,
            action_detail=decision["action_detail"],
            has_position=has_position,
            reasoning=decision.get("reasoning"),
        )
        decision["action_ratio_pct"] = self._resolve_action_ratio_pct(
            decision.get("action_ratio_pct"),
            action=action,
            action_detail=decision["action_detail"],
            has_position=has_position,
            position_size_pct=decision.get("position_size_pct"),
            swing_execution_mode=decision["swing_execution_mode"],
        )
        return decision