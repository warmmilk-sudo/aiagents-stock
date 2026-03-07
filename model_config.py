"""Model option helpers backed only by environment configuration."""

import config


def _normalize_model_name(model_name):
    if model_name is None:
        return None
    normalized = str(model_name).strip()
    return normalized or None


def _build_model_options(configured_models, *additional_models):
    ordered_models = []

    for model_name in (*configured_models, *additional_models):
        normalized = _normalize_model_name(model_name)
        if normalized and normalized not in ordered_models:
            ordered_models.append(normalized)

    return {model_name: model_name for model_name in ordered_models}


def get_lightweight_model_options(*additional_models):
    return _build_model_options(config.LIGHTWEIGHT_MODEL_OPTIONS, *additional_models)


def get_reasoning_model_options(*additional_models):
    return _build_model_options(config.REASONING_MODEL_OPTIONS, *additional_models)


def get_model_options(*additional_models):
    configured_models = []

    for model_name in (
        *config.LIGHTWEIGHT_MODEL_OPTIONS,
        *config.REASONING_MODEL_OPTIONS,
    ):
        normalized = _normalize_model_name(model_name)
        if normalized and normalized not in configured_models:
            configured_models.append(normalized)

    return _build_model_options(configured_models, *additional_models)


model_options = get_model_options()
