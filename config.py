import os
import json
import time as time_module

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False


load_dotenv(override=True)


_configured_timezone = (os.getenv("TZ") or "").strip()
if _configured_timezone:
    os.environ["TZ"] = _configured_timezone
    if hasattr(time_module, "tzset"):
        try:
            time_module.tzset()
        except Exception:
            pass


def _safe_str_env(key: str, default: str = "") -> str:
    """Read a string env var and strip surrounding whitespace."""
    return (os.getenv(key, default) or default).strip()


def _safe_int_env(key: str, default: int) -> int:
    """Read int env safely; fallback to default on invalid values."""
    try:
        return int(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def _clamp_int(value: int, minimum: int, maximum: int) -> int:
    """Clamp int values to an inclusive range."""
    return max(minimum, min(maximum, int(value)))


def _parse_model_options_env(key: str, fallback_model: str) -> list[str]:
    """Parse comma or newline separated model options from env."""
    raw_value = os.getenv(key, "")
    options = []

    for item in raw_value.replace("\n", ",").replace(";", ",").split(","):
        normalized = item.strip()
        if normalized and normalized not in options:
            options.append(normalized)

    fallback = (fallback_model or "").strip()
    if fallback and fallback not in options:
        options.insert(0, fallback)

    return options


WARMMILK_CONFIG = _safe_str_env("WARMMILK_CONFIG")
VOICE_CONFIG = _safe_str_env("VOICE_CONFIG")
LLM_API_TIMEOUT_SECONDS = max(30, _safe_int_env("LLM_API_TIMEOUT_SECONDS", 180))
ANALYSIS_TASK_TIMEOUT_SECONDS = max(
    LLM_API_TIMEOUT_SECONDS + 120,
    _safe_int_env("ANALYSIS_TASK_TIMEOUT_SECONDS", 600),
)

MODEL_CONFIG_ENV_BY_NAME = {
    "gemini-3-flash": "WARMMILK_CONFIG",
    "doubao-2-0-mini": "VOICE_CONFIG",
    "doubao-2-0-lite": "VOICE_CONFIG",
    "deepseek-v3-2": "VOICE_CONFIG",
    "doubao-2-0-pro": "VOICE_CONFIG",
}
MODEL_API_NAME_BY_CONFIG_ENV = {
    "VOICE_CONFIG": {
        "doubao-2-0-mini": "doubao-seed-2-0-mini-260215",
        "doubao-2-0-lite": "doubao-seed-2-0-lite-260215",
        "deepseek-v3-2": "deepseek-v3-2-251201",
        "doubao-2-0-pro": "doubao-seed-2-0-pro-260215",
    },
}
SUPPORTED_LLM_MODEL_NAMES = tuple(MODEL_CONFIG_ENV_BY_NAME.keys())

LIGHTWEIGHT_MODEL_NAME = _safe_str_env("LIGHTWEIGHT_MODEL_NAME", "gemini-3-flash")
REASONING_MODEL_NAME = _safe_str_env("REASONING_MODEL_NAME", "doubao-2-0-pro")
LIGHTWEIGHT_MODEL_OPTIONS = _parse_model_options_env(
    "LIGHTWEIGHT_MODEL_OPTIONS",
    LIGHTWEIGHT_MODEL_NAME,
)
REASONING_MODEL_OPTIONS = _parse_model_options_env(
    "REASONING_MODEL_OPTIONS",
    REASONING_MODEL_NAME,
)

# Embedding model for memory module (e.g. SiliconFlow BGE-m3)
EMBEDDING_API_KEY = _safe_str_env("EMBEDDING_API_KEY")
EMBEDDING_BASE_URL = _safe_str_env("EMBEDDING_BASE_URL", "https://api.siliconflow.cn/v1")
EMBEDDING_MODEL_NAME = _safe_str_env("EMBEDDING_MODEL_NAME", "BAAI/bge-m3")

def _parse_json_api_config(raw_value: str) -> tuple[str, str]:
    if not raw_value:
        return "", ""
    try:
        payload = json.loads(raw_value)
    except Exception:
        return "", ""
    if not isinstance(payload, dict):
        return "", ""
    api_key = str(payload.get("API_KEY", payload.get("api_key", "")) or "").strip()
    base_url = str(payload.get("BASE_URL", payload.get("base_url", "")) or "").strip()
    return api_key, base_url


_warmmilk_api_key, _warmmilk_base_url = _parse_json_api_config(WARMMILK_CONFIG)
_voice_api_key, _voice_base_url = _parse_json_api_config(VOICE_CONFIG)

WARMMILK_API_KEY = _warmmilk_api_key
WARMMILK_BASE_URL = _warmmilk_base_url or "https://generativelanguage.googleapis.com/v1beta/openai/"
VOICE_API_KEY = _voice_api_key
VOICE_BASE_URL = _voice_base_url or "https://api.deepseek.com/v1"


def get_model_config_env_key(model_name: str | None = None) -> str | None:
    """Return the env var name that stores a model JSON config."""
    normalized = str(model_name or "").strip()
    return MODEL_CONFIG_ENV_BY_NAME.get(normalized)


def _lookup_config_value(key: str, overrides: dict[str, str] | None = None) -> str:
    if overrides is not None and key in overrides:
        return (overrides.get(key) or "").strip()
    return _safe_str_env(key)


def get_model_api_credentials(
    model_name: str | None = None,
    overrides: dict[str, str] | None = None,
) -> tuple[str, str]:
    """Return the API key and base URL for an explicitly mapped model."""
    config_key = get_model_config_env_key(model_name)
    if not config_key:
        return "", ""

    api_key, base_url = _parse_json_api_config(_lookup_config_value(config_key, overrides))
    if api_key and base_url:
        return api_key, base_url
    return "", ""


def get_model_api_name(
    model_name: str | None = None,
    overrides: dict[str, str] | None = None,
) -> str:
    """Return the concrete upstream API model id for a configured model name."""
    normalized = str(model_name or "").strip()
    if not normalized:
        return ""

    config_key = get_model_config_env_key(normalized)
    if not config_key:
        return normalized

    if not all(get_model_api_credentials(normalized, overrides)):
        return normalized

    return MODEL_API_NAME_BY_CONFIG_ENV.get(config_key, {}).get(normalized, normalized)


def has_any_api_credentials(overrides: dict[str, str] | None = None) -> bool:
    """Return whether any configured model family has a complete credential pair."""
    return has_api_credentials_for_models(
        LIGHTWEIGHT_MODEL_NAME,
        REASONING_MODEL_NAME,
        overrides=overrides,
    )


def has_model_api_credentials(
    model_name: str | None = None,
    overrides: dict[str, str] | None = None,
) -> bool:
    """Return whether a model can be resolved to a usable API credential pair."""
    api_key, base_url = get_model_api_credentials(model_name, overrides)
    return bool(api_key and base_url)


def has_api_credentials_for_models(
    *model_names: str | None,
    overrides: dict[str, str] | None = None,
) -> bool:
    """Return whether every requested model name can be resolved to credentials."""
    normalized_models = [model_name for model_name in model_names if str(model_name or "").strip()]
    if not normalized_models:
        return has_any_api_credentials(overrides)
    return all(has_model_api_credentials(model_name, overrides) for model_name in normalized_models)

TUSHARE_TOKEN = _safe_str_env("TUSHARE_TOKEN")
TUSHARE_URL = _safe_str_env("TUSHARE_URL", "https://api.tushare.pro")
REDIS_URL = _safe_str_env("REDIS_URL")

ADMIN_PASSWORD = _safe_str_env("ADMIN_PASSWORD")
ADMIN_PASSWORD_HASH = _safe_str_env("ADMIN_PASSWORD_HASH")

LOGIN_MAX_ATTEMPTS = _safe_int_env("LOGIN_MAX_ATTEMPTS", 5)
LOGIN_LOCKOUT_SECONDS = _safe_int_env("LOGIN_LOCKOUT_SECONDS", 300)
ADMIN_SESSION_TTL_SECONDS = _safe_int_env("ADMIN_SESSION_TTL_SECONDS", 28800)

ICP_NUMBER = _safe_str_env("ICP_NUMBER", "")
ICP_LINK = _safe_str_env("ICP_LINK", "")

DATA_PERIOD = _safe_str_env("DATA_PERIOD", "1y")
DEFAULT_INTERVAL = "1d"
RISK_QUERY_TIMEOUT_SECONDS = _safe_int_env("RISK_QUERY_TIMEOUT_SECONDS", 10)
SMART_MONITOR_AI_INTERVAL_MINUTES = max(1, _safe_int_env("SMART_MONITOR_AI_INTERVAL_MINUTES", 60))
SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES = max(
    3,
    _safe_int_env("SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES", 3),
)
SMART_MONITOR_HTTP_TIMEOUT_SECONDS = max(
    15,
    _safe_int_env("SMART_MONITOR_HTTP_TIMEOUT_SECONDS", 30),
)
SMART_MONITOR_HTTP_RETRY_COUNT = max(
    0,
    _safe_int_env("SMART_MONITOR_HTTP_RETRY_COUNT", 1),
)
SMART_MONITOR_AI_TIMEOUT_SECONDS = max(
    SMART_MONITOR_HTTP_TIMEOUT_SECONDS + 5,
    _safe_int_env(
        "SMART_MONITOR_AI_TIMEOUT_SECONDS",
        SMART_MONITOR_HTTP_TIMEOUT_SECONDS * (SMART_MONITOR_HTTP_RETRY_COUNT + 1) + 10,
    ),
)
SMART_MONITOR_REASONING_MAX_TOKENS = max(
    1500,
    _safe_int_env("SMART_MONITOR_REASONING_MAX_TOKENS", 3000),
)
SMART_MONITOR_INTRADAY_TDX_RETRY_COUNT = max(
    1,
    _safe_int_env("SMART_MONITOR_INTRADAY_TDX_RETRY_COUNT", 3),
)
SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT = max(
    5,
    min(50, _safe_int_env("SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT", 20)),
)
SMART_MONITOR_DEFAULT_TOTAL_POSITION_PCT = _clamp_int(
    _safe_int_env("SMART_MONITOR_DEFAULT_TOTAL_POSITION_PCT", 100),
    0,
    100,
)
SMART_MONITOR_DEFAULT_STOP_LOSS_PCT = max(
    1,
    min(20, _safe_int_env("SMART_MONITOR_DEFAULT_STOP_LOSS_PCT", 5)),
)
SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT = max(
    1,
    min(30, _safe_int_env("SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT", 10)),
)
TDX_TIMEOUT_SECONDS = max(5, _safe_int_env("TDX_TIMEOUT_SECONDS", 10))
SMART_MONITOR_DATA_FETCH_TIMEOUT_SECONDS = max(
    TDX_TIMEOUT_SECONDS + 10,
    _safe_int_env(
        "SMART_MONITOR_DATA_FETCH_TIMEOUT_SECONDS",
        TDX_TIMEOUT_SECONDS * SMART_MONITOR_INTRADAY_TDX_RETRY_COUNT + 15,
    ),
)

TDX_CONFIG = {
    "enabled": _safe_str_env("TDX_ENABLED", "false").lower() == "true",
    "base_url": _safe_str_env("TDX_BASE_URL", ""),
    "timeout_seconds": TDX_TIMEOUT_SECONDS,
}


def get_smart_monitor_risk_defaults() -> dict[str, int]:
    """Return global fallback risk defaults for smart monitor tasks."""
    return {
        "position_size_pct": int(SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT),
        "total_position_pct": int(SMART_MONITOR_DEFAULT_TOTAL_POSITION_PCT),
        "stop_loss_pct": int(SMART_MONITOR_DEFAULT_STOP_LOSS_PCT),
        "take_profit_pct": int(SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT),
    }
