import os

from dotenv import load_dotenv


load_dotenv(override=True)


def _safe_str_env(key: str, default: str = "") -> str:
    """Read a string env var and strip surrounding whitespace."""
    return (os.getenv(key, default) or default).strip()


def _safe_int_env(key: str, default: int) -> int:
    """Read int env safely; fallback to default on invalid values."""
    try:
        return int(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


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


DEEPSEEK_API_KEY = _safe_str_env("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = _safe_str_env("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")

LIGHTWEIGHT_MODEL_NAME = _safe_str_env("LIGHTWEIGHT_MODEL_NAME", "deepseek-chat")
REASONING_MODEL_NAME = _safe_str_env("REASONING_MODEL_NAME", "deepseek-reasoner")
LIGHTWEIGHT_MODEL_OPTIONS = _parse_model_options_env(
    "LIGHTWEIGHT_MODEL_OPTIONS",
    LIGHTWEIGHT_MODEL_NAME,
)
REASONING_MODEL_OPTIONS = _parse_model_options_env(
    "REASONING_MODEL_OPTIONS",
    REASONING_MODEL_NAME,
)

# Backward compatible alias for older callers.
DEFAULT_MODEL_NAME = LIGHTWEIGHT_MODEL_NAME

TUSHARE_TOKEN = _safe_str_env("TUSHARE_TOKEN")
TUSHARE_URL = _safe_str_env("TUSHARE_URL", "https://api.tushare.pro")

ADMIN_PASSWORD = _safe_str_env("ADMIN_PASSWORD")
ADMIN_PASSWORD_HASH = _safe_str_env("ADMIN_PASSWORD_HASH")

LOGIN_MAX_ATTEMPTS = _safe_int_env("LOGIN_MAX_ATTEMPTS", 5)
LOGIN_LOCKOUT_SECONDS = _safe_int_env("LOGIN_LOCKOUT_SECONDS", 300)
ADMIN_SESSION_TTL_SECONDS = _safe_int_env("ADMIN_SESSION_TTL_SECONDS", 28800)

ICP_NUMBER = _safe_str_env("ICP_NUMBER", "")
ICP_LINK = _safe_str_env("ICP_LINK", "")

DATA_PERIOD = _safe_str_env("DATA_PERIOD", "1y")
DEFAULT_INTERVAL = "1d"

MINIQMT_CONFIG = {
    "enabled": _safe_str_env("MINIQMT_ENABLED", "false").lower() == "true",
    "account_id": _safe_str_env("MINIQMT_ACCOUNT_ID", ""),
    "host": _safe_str_env("MINIQMT_HOST", "127.0.0.1"),
    "port": _safe_int_env("MINIQMT_PORT", 58610),
}

TDX_CONFIG = {
    "enabled": _safe_str_env("TDX_ENABLED", "false").lower() == "true",
    "base_url": _safe_str_env("TDX_BASE_URL", "http://127.0.0.1:8181"),
}
