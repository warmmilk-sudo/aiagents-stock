from enum import Enum
from typing import Optional, Union

import config


class ModelTier(str, Enum):
    LIGHTWEIGHT = "lightweight"
    REASONING = "reasoning"


_TIER_DEFAULTS = {
    ModelTier.LIGHTWEIGHT: "gemini-3-flash",
    ModelTier.REASONING: "doubao-2-0-pro",
}


def normalize_model_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    normalized = str(name).strip()
    return normalized or None


def parse_tier(tier: Optional[Union[ModelTier, str]]) -> ModelTier:
    if isinstance(tier, ModelTier):
        return tier

    raw = str(tier or ModelTier.LIGHTWEIGHT).strip().lower().replace("-", "_")
    if raw in {"reasoning", "reasoner", "strong"}:
        return ModelTier.REASONING
    return ModelTier.LIGHTWEIGHT


def get_env_model_name(tier: Optional[Union[ModelTier, str]] = None) -> str:
    parsed_tier = parse_tier(tier)
    if parsed_tier == ModelTier.REASONING:
        return normalize_model_name(config.REASONING_MODEL_NAME) or _TIER_DEFAULTS[parsed_tier]
    return normalize_model_name(config.LIGHTWEIGHT_MODEL_NAME) or _TIER_DEFAULTS[parsed_tier]


def resolve_model_name(
    tier: Optional[Union[ModelTier, str]] = None,
    explicit_model: Optional[str] = None,
    forced_model: Optional[str] = None,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> str:
    model_name = normalize_model_name(explicit_model)
    if model_name:
        return model_name

    model_name = normalize_model_name(forced_model)
    if model_name:
        return model_name

    parsed_tier = parse_tier(tier)
    if parsed_tier == ModelTier.REASONING:
        model_name = normalize_model_name(reasoning_model)
        if model_name:
            return model_name
    else:
        model_name = normalize_model_name(lightweight_model)
        if model_name:
            return model_name

    return get_env_model_name(parsed_tier)


def get_model_fallback_candidates(
    tier: Optional[Union[ModelTier, str]] = None,
    explicit_model: Optional[str] = None,
    forced_model: Optional[str] = None,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> list[str]:
    """Return the ordered model names to try for a request.

    The primary model is derived from the normal routing rules. The remaining
    candidates follow the configured per-tier options before falling back to
    the tier defaults and then the opposite tier as a last resort.
    """

    parsed_tier = parse_tier(tier)
    candidates: list[str] = []

    def add_candidate(name: Optional[str]) -> None:
        normalized = normalize_model_name(name)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    add_candidate(
        resolve_model_name(
            tier=parsed_tier,
            explicit_model=explicit_model,
            forced_model=forced_model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
    )

    if parsed_tier == ModelTier.REASONING:
        for item in config.REASONING_MODEL_OPTIONS:
            add_candidate(item)
        add_candidate(reasoning_model)
        add_candidate(config.REASONING_MODEL_NAME)
        add_candidate(_TIER_DEFAULTS[ModelTier.REASONING])
        for item in config.LIGHTWEIGHT_MODEL_OPTIONS:
            add_candidate(item)
        add_candidate(lightweight_model)
        add_candidate(config.LIGHTWEIGHT_MODEL_NAME)
        add_candidate(_TIER_DEFAULTS[ModelTier.LIGHTWEIGHT])
    else:
        for item in config.LIGHTWEIGHT_MODEL_OPTIONS:
            add_candidate(item)
        add_candidate(lightweight_model)
        add_candidate(config.LIGHTWEIGHT_MODEL_NAME)
        add_candidate(_TIER_DEFAULTS[ModelTier.LIGHTWEIGHT])
        for item in config.REASONING_MODEL_OPTIONS:
            add_candidate(item)
        add_candidate(reasoning_model)
        add_candidate(config.REASONING_MODEL_NAME)
        add_candidate(_TIER_DEFAULTS[ModelTier.REASONING])

    return candidates


def describe_model_selection(
    forced_model: Optional[str] = None,
    lightweight_model: Optional[str] = None,
    reasoning_model: Optional[str] = None,
) -> str:
    normalized_forced = normalize_model_name(forced_model)
    if normalized_forced:
        return f"forced={normalized_forced}"

    light = resolve_model_name(ModelTier.LIGHTWEIGHT, lightweight_model=lightweight_model)
    reasoning = resolve_model_name(ModelTier.REASONING, reasoning_model=reasoning_model)
    return f"light={light}, reasoning={reasoning}"
