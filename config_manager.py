"""Environment configuration manager."""

import json
import importlib
import re
from pathlib import Path
from typing import Any, Dict, Optional

import requests

import config as app_config


class ConfigManager:
    """Read, validate, and write `.env` configuration."""

    _SYSTEM_CONFIG_HIDDEN_PREFIXES = ("SMART_MONITOR_",)
    _MASKED_PASSWORD_VALUE = "********"
    _MODEL_DISCOVERY_TIMEOUT_SECONDS = 8

    _ENV_ASSIGNMENT_RE = re.compile(
        r"^(?P<leading>\s*)(?P<export>export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)"
        r"(?P<separator>\s*=\s*)(?P<value>.*?)(?P<newline>\r?\n?)$"
    )

    @staticmethod
    def _default_api_config_value(api_key: str, base_url: str) -> str:
        return json.dumps(
            {
                "API_KEY": api_key,
                "BASE_URL": base_url,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def _parse_json_field_value(value: Any) -> Optional[dict[str, Any]]:
        raw_value = str(value or "").strip()
        if not raw_value:
            return {}
        try:
            parsed = json.loads(raw_value)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _normalize_json_field_value(self, value: Any, *, mask_api_key: bool = False) -> str:
        payload = self._parse_json_field_value(value)
        if payload is None:
            return self._normalize_env_value(value)

        canonical_payload = {
            "API_KEY": str(payload.get("API_KEY", payload.get("api_key", "")) or "").strip(),
            "BASE_URL": str(payload.get("BASE_URL", payload.get("base_url", "")) or "").strip(),
        }
        if mask_api_key and canonical_payload["API_KEY"]:
            canonical_payload["API_KEY"] = self._MASKED_PASSWORD_VALUE
        return json.dumps(canonical_payload, ensure_ascii=False, separators=(",", ":"))

    def __init__(self, env_file: str = ".env"):
        self.env_file = Path(env_file)
        self.default_config = {
            "WARMMILK_CONFIG": {
                "value": self._default_api_config_value(app_config.WARMMILK_API_KEY, app_config.WARMMILK_BASE_URL),
                "description": "WARMMILK 模型配置（JSON）",
                "required": False,
                "type": "json",
            },
            "VOICE_CONFIG": {
                "value": self._default_api_config_value(app_config.VOICE_API_KEY, app_config.VOICE_BASE_URL),
                "description": "VOICE 模型配置（JSON）",
                "required": False,
                "type": "json",
            },
            "SMART_MONITOR_HTTP_TIMEOUT_SECONDS": {
                "value": "30",
                "description": "智能盯盘 AI 单次请求读超时（秒）",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_HTTP_RETRY_COUNT": {
                "value": "1",
                "description": "智能盯盘 AI 请求超时重试次数",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_AI_TIMEOUT_SECONDS": {
                "value": "70",
                "description": "智能盯盘 AI 单股分析总超时（秒）",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_DATA_FETCH_TIMEOUT_SECONDS": {
                "value": "45",
                "description": "智能盯盘单股市场数据抓取总超时（秒）",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_REASONING_MAX_TOKENS": {
                "value": "3000",
                "description": "智能盯盘推理模型最大输出令牌数",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_INTRADAY_TDX_RETRY_COUNT": {
                "value": "3",
                "description": "盘中分析强制使用 TDX 时的重试次数",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_DEFAULT_POSITION_SIZE_PCT": {
                "value": "20",
                "description": "智能盯盘默认仓位百分比",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_DEFAULT_TOTAL_POSITION_PCT": {
                "value": "100",
                "description": "智能盯盘默认总仓位百分比",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_DEFAULT_STOP_LOSS_PCT": {
                "value": "5",
                "description": "智能盯盘默认止损百分比",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_DEFAULT_TAKE_PROFIT_PCT": {
                "value": "10",
                "description": "智能盯盘默认止盈百分比",
                "required": False,
                "type": "text",
            },
            "LIGHTWEIGHT_MODEL_NAME": {
                "value": "gemini-3-flash",
                "description": "轻量模型名称（技术/情绪/新闻/批量筛选等任务）",
                "required": False,
                "type": "text",
            },
            "LIGHTWEIGHT_MODEL_OPTIONS": {
                "value": "gemini-3-flash,doubao-2-0-mini,doubao-2-0-lite",
                "description": "轻量模型下拉候选（逗号或换行分隔）",
                "required": False,
                "type": "text",
            },
            "REASONING_MODEL_NAME": {
                "value": "doubao-2-0-pro",
                "description": "推理模型名称（基本面/风险/宏观/策略等任务）",
                "required": False,
                "type": "text",
            },
            "REASONING_MODEL_OPTIONS": {
                "value": "deepseek-v3-2,doubao-2-0-pro",
                "description": "推理模型下拉候选（逗号或换行分隔）",
                "required": False,
                "type": "text",
            },
            "DOUBAO_2_0_MINI_API_NAME": {
                "value": "doubao-seed-2-0-mini-260215",
                "description": "doubao-2-0-mini 对应的火山方舟实际模型ID；版本后缀更新时只改这里",
                "required": False,
                "type": "text",
            },
            "DOUBAO_2_0_LITE_API_NAME": {
                "value": "doubao-seed-2-0-lite-260215",
                "description": "doubao-2-0-lite 对应的火山方舟实际模型ID；版本后缀更新时只改这里",
                "required": False,
                "type": "text",
            },
            "DOUBAO_2_0_PRO_API_NAME": {
                "value": "doubao-seed-2-0-pro-260215",
                "description": "doubao-2-0-pro 对应的火山方舟实际模型ID；版本后缀更新时只改这里",
                "required": False,
                "type": "text",
            },
            "LLM_MODEL_API_NAME_ALIASES": {
                "value": "",
                "description": "模型别名到实际API模型ID的JSON映射，如 {\"doubao-2-0-mini\":\"doubao-seed-2-0-mini-260425\"}",
                "required": False,
                "type": "textarea",
            },
            "ADMIN_PASSWORD": {
                "value": "",
                "description": "管理员密码（为空则无需密码）",
                "required": False,
                "type": "password",
            },
            "ADMIN_PASSWORD_HASH": {
                "value": "",
                "description": "管理员密码哈希（优先于明文密码）",
                "required": False,
                "type": "password",
            },
            "LOGIN_MAX_ATTEMPTS": {
                "value": "5",
                "description": "登录最大失败次数",
                "required": False,
                "type": "text",
            },
            "LOGIN_LOCKOUT_SECONDS": {
                "value": "300",
                "description": "登录锁定时长（秒）",
                "required": False,
                "type": "text",
            },
            "ADMIN_SESSION_TTL_SECONDS": {
                "value": "28800",
                "description": "管理员会话有效期（秒）",
                "required": False,
                "type": "text",
            },
            "ICP_NUMBER": {
                "value": "",
                "description": "网站备案号（为空则不显示）",
                "required": False,
                "type": "text",
            },
            "ICP_LINK": {
                "value": "",
                "description": "备案号跳转地址",
                "required": False,
                "type": "text",
            },
            "TUSHARE_TOKEN": {
                "value": "",
                "description": "Tushare Token",
                "required": False,
                "type": "password",
            },
            "TUSHARE_URL": {
                "value": "https://api.tushare.pro",
                "description": "Tushare API 地址",
                "required": False,
                "type": "text",
            },
            "TDX_ENABLED": {
                "value": "false",
                "description": "启用 TDX 数据源",
                "required": False,
                "type": "boolean",
            },
            "TDX_BASE_URL": {
                "value": "",
                "description": "TDX API 地址（启用 TDX 时必填）",
                "required": False,
                "type": "text",
            },
            "TDX_TIMEOUT_SECONDS": {
                "value": "10",
                "description": "TDX 请求超时（秒）",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_AI_INTERVAL_MINUTES": {
                "value": "60",
                "description": "智能盯盘默认轻 AI 分析间隔（分钟）",
                "required": False,
                "type": "text",
            },
            "SMART_MONITOR_PRICE_ALERT_INTERVAL_MINUTES": {
                "value": "3",
                "description": "智能盯盘默认价格预警间隔（分钟）",
                "required": False,
                "type": "text",
            },
            "EMAIL_ENABLED": {
                "value": "false",
                "description": "启用邮件通知",
                "required": False,
                "type": "boolean",
            },
            "SMTP_SERVER": {
                "value": "",
                "description": "SMTP 服务器",
                "required": False,
                "type": "text",
            },
            "SMTP_PORT": {
                "value": "587",
                "description": "SMTP 端口",
                "required": False,
                "type": "text",
            },
            "EMAIL_FROM": {
                "value": "",
                "description": "发件邮箱",
                "required": False,
                "type": "text",
            },
            "EMAIL_PASSWORD": {
                "value": "",
                "description": "邮箱授权码",
                "required": False,
                "type": "password",
            },
            "EMAIL_TO": {
                "value": "",
                "description": "收件邮箱",
                "required": False,
                "type": "text",
            },
            "WEBHOOK_ENABLED": {
                "value": "false",
                "description": "启用 Webhook 通知",
                "required": False,
                "type": "boolean",
            },
            "WEBHOOK_TYPE": {
                "value": "dingtalk",
                "description": "Webhook 类型",
                "required": False,
                "type": "select",
                "options": ["dingtalk", "feishu"],
            },
            "WEBHOOK_URL": {
                "value": "",
                "description": "Webhook 地址",
                "required": False,
                "type": "text",
            },
            "WEBHOOK_KEYWORD": {
                "value": "aiagents通知",
                "description": "Webhook 安全关键词",
                "required": False,
                "type": "text",
            },
            "DATA_PERIOD": {
                "value": "1y",
                "description": "默认股票数据周期",
                "required": False,
                "type": "select",
                "options": ["6mo", "1y", "2y", "5y"],
            },
        }

    def _normalize_env_value(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    def _is_system_config_visible_key(self, key: str) -> bool:
        return not any(key.startswith(prefix) for prefix in self._SYSTEM_CONFIG_HIDDEN_PREFIXES)

    def _is_password_field(self, key: str) -> bool:
        field = self.default_config.get(key)
        return bool(field and field.get("type") == "password")

    def _parse_model_options_value(self, value: Any) -> list[str]:
        options: list[str] = []
        raw_value = self._normalize_env_value(value)
        for item in raw_value.replace("\n", ",").replace(";", ",").split(","):
            normalized = item.strip()
            if normalized and normalized not in options:
                options.append(normalized)
        return options

    def _fetch_available_llm_models(
        self,
        config_values: Dict[str, str],
        model_name: Optional[str] = None,
    ) -> Optional[list[str]]:
        api_key, base_url = app_config.get_model_api_credentials(model_name, config_values)
        if not api_key or not base_url:
            return None

        endpoint = f"{base_url.rstrip('/')}/models"
        try:
            response = requests.get(
                endpoint,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=(3, self._MODEL_DISCOVERY_TIMEOUT_SECONDS),
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return None

        models: list[str] = []
        for item in payload.get("data", []) if isinstance(payload, dict) else []:
            if not isinstance(item, dict):
                continue
            model_id = str(item.get("id", "")).strip()
            if model_id and model_id not in models:
                models.append(model_id)
        return models or None

    def _validate_llm_models(self, config_values: Dict[str, str]) -> tuple[bool, str]:
        available_models_cache: dict[tuple[str, str], Optional[list[str]]] = {}
        invalid_entries: list[str] = []

        def _get_available_models(model_name: str) -> Optional[list[str]]:
            api_key, base_url = app_config.get_model_api_credentials(model_name, config_values)
            if not api_key or not base_url:
                return None
            cache_key = (api_key, base_url)
            if cache_key not in available_models_cache:
                available_models_cache[cache_key] = self._fetch_available_llm_models(config_values, model_name)
            return available_models_cache[cache_key]

        for key in ("LIGHTWEIGHT_MODEL_NAME", "REASONING_MODEL_NAME"):
            value = str(config_values.get(key, "")).strip()
            if not value:
                continue
            api_key, base_url = app_config.get_model_api_credentials(value, config_values)
            if not api_key or not base_url:
                invalid_entries.append(f"{key}={value}")
                continue
            available_models = _get_available_models(value)
            api_model_name = app_config.get_model_api_name(value, config_values)
            if available_models and api_model_name not in available_models:
                invalid_entries.append(f"{key}={value}")

        for key in ("LIGHTWEIGHT_MODEL_OPTIONS", "REASONING_MODEL_OPTIONS"):
            invalid_options = []
            for item in self._parse_model_options_value(config_values.get(key, "")):
                api_key, base_url = app_config.get_model_api_credentials(item, config_values)
                if not api_key or not base_url:
                    invalid_options.append(item)
                    continue
                available_models = _get_available_models(item)
                api_model_name = app_config.get_model_api_name(item, config_values)
                if available_models and api_model_name not in available_models:
                    invalid_options.append(item)
            if invalid_options:
                invalid_entries.append(f"{key} 包含 {', '.join(invalid_options)}")

        if not invalid_entries:
            return True, "配置验证通过"

        available_preview = ", ".join(
            sorted(
                {
                    item
                    for models in available_models_cache.values()
                    if models
                    for item in models
                }
            )[:10]
        )
        return (
            False,
            "当前 LLM 网关不支持以下模型配置："
            f"{'; '.join(invalid_entries)}。"
            f"可用模型示例：{available_preview}",
        )

    def filter_system_config_values(self, config: Dict[str, str]) -> Dict[str, str]:
        return {
            key: value
            for key, value in config.items()
            if key in self.default_config
            and self._is_system_config_visible_key(key)
        }

    def resolve_masked_secrets(
        self,
        updates: Dict[str, str],
        current_values: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        existing_values = current_values or self.read_env()
        resolved: Dict[str, str] = {}

        for key, value in updates.items():
            if self._is_password_field(key) and value == self._MASKED_PASSWORD_VALUE:
                resolved[key] = existing_values.get(key, "")
            elif self.default_config.get(key, {}).get("type") == "json":
                if value == self._MASKED_PASSWORD_VALUE:
                    resolved[key] = existing_values.get(key, "")
                    continue
                parsed_value = self._parse_json_field_value(value)
                if parsed_value is None:
                    resolved[key] = value
                    continue
                existing_payload = self._parse_json_field_value(existing_values.get(key, ""))
                if isinstance(existing_payload, dict):
                    if str(parsed_value.get("API_KEY", parsed_value.get("api_key", "")) or "").strip() == self._MASKED_PASSWORD_VALUE:
                        parsed_value["API_KEY"] = existing_payload.get("API_KEY", existing_payload.get("api_key", ""))
                    if str(parsed_value.get("BASE_URL", parsed_value.get("base_url", "")) or "").strip() == self._MASKED_PASSWORD_VALUE:
                        parsed_value["BASE_URL"] = existing_payload.get("BASE_URL", existing_payload.get("base_url", ""))
                resolved[key] = json.dumps(
                    {
                        "API_KEY": str(parsed_value.get("API_KEY", parsed_value.get("api_key", "")) or "").strip(),
                        "BASE_URL": str(parsed_value.get("BASE_URL", parsed_value.get("base_url", "")) or "").strip(),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            else:
                resolved[key] = value

        return resolved

    def _split_value_and_comment(self, raw_value: str) -> tuple[str, str]:
        in_single_quote = False
        in_double_quote = False
        escaping = False

        for index, char in enumerate(raw_value):
            if char == "\\" and in_double_quote and not escaping:
                escaping = True
                continue

            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote and not escaping:
                in_double_quote = not in_double_quote
            elif char == "#" and not in_single_quote and not in_double_quote:
                if index == 0 or raw_value[index - 1].isspace():
                    return raw_value[:index], raw_value[index:]

            escaping = False

        return raw_value, ""

    def _detect_quote_style(self, raw_value: str) -> Optional[str]:
        stripped = raw_value.strip()
        if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {'"', "'"}:
            return stripped[0]
        return None

    def _decode_env_value(self, raw_value: str) -> str:
        value_part, _ = self._split_value_and_comment(raw_value)
        stripped = value_part.strip()
        quote_style = self._detect_quote_style(stripped)

        if quote_style is None:
            return stripped

        inner_value = stripped[1:-1]
        if quote_style == '"':
            return inner_value.replace("\\\"", '"').replace("\\\\", "\\")
        return inner_value.replace("\\'", "'").replace("\\\\", "\\")

    def _format_env_value(self, value: Any, quote_style: Optional[str] = None) -> str:
        normalized = self._normalize_env_value(value)

        if quote_style == '"':
            escaped = normalized.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'

        if quote_style == "'":
            escaped = normalized.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"

        if not normalized:
            return ""

        if any(char.isspace() for char in normalized) or "#" in normalized:
            escaped = normalized.replace("\\", "\\\\").replace('"', '\\"')
            return f'"{escaped}"'

        return normalized

    def _render_updated_assignment(self, match: re.Match[str], value: Any) -> str:
        raw_value = match.group("value")
        value_part, comment = self._split_value_and_comment(raw_value)
        leading_space = value_part[: len(value_part) - len(value_part.lstrip())]
        trailing_space = value_part[len(value_part.rstrip()):]
        quote_style = self._detect_quote_style(value_part)
        rendered_value = self._format_env_value(value, quote_style=quote_style)

        return (
            f"{match.group('leading')}{match.group('export') or ''}{match.group('key')}"
            f"{match.group('separator')}{leading_space}{rendered_value}{trailing_space}"
            f"{comment}{match.group('newline')}"
        )

    def _get_newline(self, content: str) -> str:
        if "\r\n" in content:
            return "\r\n"
        return "\n"

    def _build_minimal_env_content(self, updates: Dict[str, str]) -> str:
        ordered_keys = [key for key in self.default_config if key in updates]
        for key in updates:
            if key not in ordered_keys:
                ordered_keys.append(key)

        if not ordered_keys:
            return ""

        lines = [f"{key}={self._format_env_value(updates[key])}" for key in ordered_keys]
        return "\n".join(lines) + "\n"

    def _merge_env_content(self, content: str, updates: Dict[str, str]) -> str:
        lines = content.splitlines(keepends=True)
        updated_lines = []
        updated_keys = set()

        for raw_line in lines:
            match = self._ENV_ASSIGNMENT_RE.match(raw_line)
            if not match:
                updated_lines.append(raw_line)
                continue

            key = match.group("key")
            if key not in updates:
                updated_lines.append(raw_line)
                continue

            updated_lines.append(self._render_updated_assignment(match, updates[key]))
            updated_keys.add(key)

        missing_keys = [
            key for key in self.default_config if key in updates and key not in updated_keys
        ]
        if not missing_keys:
            return "".join(updated_lines)

        newline = self._get_newline(content)
        if updated_lines and not updated_lines[-1].endswith(("\n", "\r")):
            updated_lines[-1] = updated_lines[-1] + newline
        if updated_lines and updated_lines[-1].strip():
            updated_lines.append(newline)

        for key in missing_keys:
            updated_lines.append(f"{key}={self._format_env_value(updates[key])}{newline}")

        return "".join(updated_lines)

    def read_env(self) -> Dict[str, str]:
        """Read `.env` values and merge defaults."""
        config: Dict[str, str] = {}

        if self.env_file.exists():
            try:
                with open(self.env_file, "r", encoding="utf-8") as f:
                    for raw_line in f:
                        match = self._ENV_ASSIGNMENT_RE.match(raw_line)
                        if not match:
                            continue

                        key = match.group("key").strip()
                        if key in self.default_config:
                            config[key] = self._decode_env_value(match.group("value"))
            except Exception as e:
                print(f"读取 .env 失败: {e}")

        for key, info in self.default_config.items():
            config.setdefault(key, info["value"])

        return config

    def write_env(self, config: Dict[str, str]) -> bool:
        """Update managed configuration in `.env` while preserving existing layout."""
        try:
            updates = {
                key: (
                    self._normalize_json_field_value(value)
                    if self.default_config.get(key, {}).get("type") == "json"
                    else self._normalize_env_value(value)
                )
                for key, value in config.items()
                if key in self.default_config
            }

            if not updates:
                return True

            if self.env_file.exists():
                existing_content = self.env_file.read_text(encoding="utf-8")
                rendered_content = self._merge_env_content(existing_content, updates)
            else:
                rendered_content = self._build_minimal_env_content(updates)

            with open(self.env_file, "w", encoding="utf-8") as f:
                f.write(rendered_content)

            return True
        except Exception as e:
            print(f"保存 .env 失败: {e}")
            return False

    def get_config_info(self) -> Dict[str, Dict[str, Any]]:
        """Return config metadata with current values."""
        current_values = self.read_env()
        config_info = {}

        for key, info in self.default_config.items():
            if not self._is_system_config_visible_key(key):
                continue
            value = current_values.get(key, info["value"])
            if self._is_password_field(key) and value:
                value = self._MASKED_PASSWORD_VALUE
            elif info.get("type") == "json":
                value = self._normalize_json_field_value(value, mask_api_key=True)
            config_info[key] = {
                "value": value,
                "description": info["description"],
                "required": info["required"],
                "type": info["type"],
            }
            if "options" in info:
                config_info[key]["options"] = info["options"]

        return config_info

    def validate_config(self, config: Dict[str, str]) -> tuple[bool, str]:
        """Validate current config values."""
        for key, info in self.default_config.items():
            if info["required"] and not config.get(key):
                return False, f"必填项 {info['description']} 不能为空"
            if info.get("type") == "json":
                raw_value = str(config.get(key, "") or "").strip()
                if raw_value and self._parse_json_field_value(raw_value) is None:
                    return False, f"{info['description']} 必须是合法的 JSON"

        lightweight_model = str(
            config.get("LIGHTWEIGHT_MODEL_NAME") or getattr(app_config, "LIGHTWEIGHT_MODEL_NAME", "")
        ).strip()
        reasoning_model = str(
            config.get("REASONING_MODEL_NAME") or getattr(app_config, "REASONING_MODEL_NAME", "")
        ).strip()
        for model_name in (lightweight_model, reasoning_model):
            if not model_name:
                continue
            if not app_config.get_model_config_env_key(model_name):
                continue
            api_key, base_url = app_config.get_model_api_credentials(model_name, config)
            if not api_key or not base_url:
                return False, "请先为当前轻量/推理模型配置可用的 API 密钥和 BASE_URL"

        if str(config.get("TDX_ENABLED", "false")).strip().lower() == "true":
            if not str(config.get("TDX_BASE_URL", "")).strip():
                return False, "启用 TDX 数据源时，TDX API 地址不能为空"

        llm_valid, llm_message = self._validate_llm_models(config)
        if not llm_valid:
            return False, llm_message

        return True, "配置验证通过"

    def reload_config(self):
        """Reload `.env` into the process environment."""
        from dotenv import load_dotenv

        load_dotenv(override=True)
        try:
            import config as config_module

            importlib.reload(config_module)
        except Exception as e:
            print(f"重载 config 模块失败: {e}")


config_manager = ConfigManager()
