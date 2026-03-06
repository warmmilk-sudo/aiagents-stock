"""模型配置文件"""

import config


_PRESET_MODELS = {
    "deepseek-chat": "DeepSeek Chat",
    "deepseek-reasoner": "DeepSeek Reasoner (推理增强)",
    "qwen-plus": "qwen-plus (阿里百炼)",
    "qwen-plus-latest": "qwen-plus-latest (阿里百炼)",
    "qwen-flash": "qwen-flash (阿里百炼)",
    "qwen-turbo": "qwen-turbo (阿里百炼)",
    "qwen3-max": "qwen-max (阿里百炼)",
    "qwen-long": "qwen-long (阿里百炼)",
    "deepseek-ai/DeepSeek-R1-0528-Qwen3-8B": "DeepSeek-R1 免费(硅基流动)",
    "Qwen/Qwen2.5-7B-Instruct": "Qwen 免费(硅基流动)",
    "Pro/deepseek-ai/DeepSeek-V3.1-Terminus": "DeepSeek-V3.1-Terminus (硅基流动)",
    "deepseek-ai/DeepSeek-R1": "DeepSeek-R1 (硅基流动)",
    "Qwen/Qwen3-235B-A22B-Thinking-2507": "Qwen3-235B (硅基流动)",
    "zai-org/GLM-4.6": "智谱(硅基流动)",
    "moonshotai/Kimi-K2-Instruct-0905": "Kimi (硅基流动)",
    "Ring-1T": "蚂蚁百灵 (硅基流动)",
    "step3": "阶跃星辰(硅基流动)",
}


def _normalize_model_name(model_name):
    if model_name is None:
        return None
    normalized = str(model_name).strip()
    return normalized or None


def _label_for_model(model_name):
    return _PRESET_MODELS.get(model_name, f"{model_name} (自定义)")


def get_model_options(*additional_models):
    ordered_models = []

    for model_name in (
        config.LIGHTWEIGHT_MODEL_NAME,
        config.REASONING_MODEL_NAME,
        *additional_models,
    ):
        normalized = _normalize_model_name(model_name)
        if normalized and normalized not in ordered_models:
            ordered_models.append(normalized)

    options = {}
    for model_name in ordered_models:
        options[model_name] = _label_for_model(model_name)

    for model_name, label in _PRESET_MODELS.items():
        if model_name not in options:
            options[model_name] = label

    return options


model_options = get_model_options()
