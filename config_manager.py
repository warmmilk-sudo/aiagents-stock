"""Environment configuration manager."""

import importlib
import re
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigManager:
    """Read, validate, and write `.env` configuration."""

    _SYSTEM_CONFIG_HIDDEN_PREFIXES = ("SMART_MONITOR_",)

    _ENV_ASSIGNMENT_RE = re.compile(
        r"^(?P<leading>\s*)(?P<export>export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)"
        r"(?P<separator>\s*=\s*)(?P<value>.*?)(?P<newline>\r?\n?)$"
    )

    def __init__(self, env_file: str = ".env"):
        self.env_file = Path(env_file)
        self.default_config = {
            "DEEPSEEK_API_KEY": {
                "value": "",
                "description": "DeepSeek API 密钥",
                "required": True,
                "type": "password",
            },
            "DEEPSEEK_BASE_URL": {
                "value": "https://api.deepseek.com/v1",
                "description": "DeepSeek API 地址",
                "required": False,
                "type": "text",
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
                "value": "deepseek-chat",
                "description": "轻量模型名称（技术/情绪/新闻/批量筛选等任务）",
                "required": False,
                "type": "text",
            },
            "LIGHTWEIGHT_MODEL_OPTIONS": {
                "value": "",
                "description": "轻量模型下拉候选（逗号或换行分隔）",
                "required": False,
                "type": "text",
            },
            "REASONING_MODEL_NAME": {
                "value": "deepseek-reasoner",
                "description": "推理模型名称（基本面/风险/宏观/策略等任务）",
                "required": False,
                "type": "text",
            },
            "REASONING_MODEL_OPTIONS": {
                "value": "",
                "description": "推理模型下拉候选（逗号或换行分隔）",
                "required": False,
                "type": "text",
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

    def filter_system_config_values(self, config: Dict[str, str]) -> Dict[str, str]:
        return {
            key: value
            for key, value in config.items()
            if key in self.default_config and self._is_system_config_visible_key(key)
        }

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
                key: self._normalize_env_value(config.get(key, self.default_config[key]["value"]))
                for key in self.default_config
                if key in config
            }

            for key, value in config.items():
                if key not in updates and key in self.default_config:
                    updates[key] = self._normalize_env_value(value)

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
            config_info[key] = {
                "value": current_values.get(key, info["value"]),
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

        if str(config.get("TDX_ENABLED", "false")).strip().lower() == "true":
            if not str(config.get("TDX_BASE_URL", "")).strip():
                return False, "启用 TDX 数据源时，TDX API 地址不能为空"

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
