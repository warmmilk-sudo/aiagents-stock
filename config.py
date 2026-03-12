import os
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
SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT = max(
    5,
    min(50, _safe_int_env("SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT", 20)),
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

TDX_CONFIG = {
    "enabled": _safe_str_env("TDX_ENABLED", "false").lower() == "true",
    "base_url": _safe_str_env("TDX_BASE_URL", ""),
    "timeout_seconds": TDX_TIMEOUT_SECONDS,
}
