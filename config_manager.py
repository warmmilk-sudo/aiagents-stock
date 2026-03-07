"""Environment configuration manager."""

from pathlib import Path
from typing import Any, Dict


class ConfigManager:
    """Read, validate, and write `.env` configuration."""

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
                "value": "http://127.0.0.1:8181",
                "description": "TDX API 地址",
                "required": False,
                "type": "text",
            },
            "MINIQMT_ENABLED": {
                "value": "false",
                "description": "启用 MiniQMT 量化交易",
                "required": False,
                "type": "boolean",
            },
            "MINIQMT_ACCOUNT_ID": {
                "value": "",
                "description": "MiniQMT 账户 ID",
                "required": False,
                "type": "text",
            },
            "MINIQMT_HOST": {
                "value": "127.0.0.1",
                "description": "MiniQMT 主机地址",
                "required": False,
                "type": "text",
            },
            "MINIQMT_PORT": {
                "value": "58610",
                "description": "MiniQMT 端口",
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
                "type": "text",
            },
        }

    def read_env(self) -> Dict[str, str]:
        """Read `.env` values and merge defaults."""
        config: Dict[str, str] = {}

        if self.env_file.exists():
            try:
                with open(self.env_file, "r", encoding="utf-8") as f:
                    for raw_line in f:
                        line = raw_line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue

                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip()

                        if value.startswith('"') and value.endswith('"'):
                            value = value[1:-1]
                        elif value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]

                        config[key] = value
            except Exception as e:
                print(f"读取 .env 失败: {e}")

        for key, info in self.default_config.items():
            config.setdefault(key, info["value"])

        return config

    def write_env(self, config: Dict[str, str]) -> bool:
        """Write managed configuration back to `.env`."""
        try:
            lines = [
                "# AI股票分析系统环境配置",
                "# 由系统自动生成和管理",
                "",
                "# ========== DeepSeek API 配置 ==========",
                f'DEEPSEEK_API_KEY="{config.get("DEEPSEEK_API_KEY", "")}"',
                f'DEEPSEEK_BASE_URL="{config.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")}"',
                f'LIGHTWEIGHT_MODEL_NAME="{config.get("LIGHTWEIGHT_MODEL_NAME", "deepseek-chat")}"',
                f'LIGHTWEIGHT_MODEL_OPTIONS="{config.get("LIGHTWEIGHT_MODEL_OPTIONS", "")}"',
                f'REASONING_MODEL_NAME="{config.get("REASONING_MODEL_NAME", "deepseek-reasoner")}"',
                f'REASONING_MODEL_OPTIONS="{config.get("REASONING_MODEL_OPTIONS", "")}"',
                f'ADMIN_PASSWORD="{config.get("ADMIN_PASSWORD", "")}"',
                f'ADMIN_PASSWORD_HASH="{config.get("ADMIN_PASSWORD_HASH", "")}"',
                f'LOGIN_MAX_ATTEMPTS="{config.get("LOGIN_MAX_ATTEMPTS", "5")}"',
                f'LOGIN_LOCKOUT_SECONDS="{config.get("LOGIN_LOCKOUT_SECONDS", "300")}"',
                f'ADMIN_SESSION_TTL_SECONDS="{config.get("ADMIN_SESSION_TTL_SECONDS", "28800")}"',
                f'ICP_NUMBER="{config.get("ICP_NUMBER", "")}"',
                f'ICP_LINK="{config.get("ICP_LINK", "")}"',
                "",
                "# ========== Tushare 配置 ==========",
                f'TUSHARE_TOKEN="{config.get("TUSHARE_TOKEN", "")}"',
                f'TUSHARE_URL="{config.get("TUSHARE_URL", "https://api.tushare.pro")}"',
                "",
                "# ========== 全局分析配置 ==========",
                f'DATA_PERIOD="{config.get("DATA_PERIOD", "1y")}"',
                f'TDX_ENABLED="{config.get("TDX_ENABLED", "false")}"',
                f'TDX_BASE_URL="{config.get("TDX_BASE_URL", "http://127.0.0.1:8181")}"',
                "",
                "# ========== MiniQMT 配置 ==========",
                f'MINIQMT_ENABLED="{config.get("MINIQMT_ENABLED", "false")}"',
                f'MINIQMT_ACCOUNT_ID="{config.get("MINIQMT_ACCOUNT_ID", "")}"',
                f'MINIQMT_HOST="{config.get("MINIQMT_HOST", "127.0.0.1")}"',
                f'MINIQMT_PORT="{config.get("MINIQMT_PORT", "58610")}"',
                "",
                "# ========== 邮件通知配置 ==========",
                f'EMAIL_ENABLED="{config.get("EMAIL_ENABLED", "false")}"',
                f'SMTP_SERVER="{config.get("SMTP_SERVER", "")}"',
                f'SMTP_PORT="{config.get("SMTP_PORT", "587")}"',
                f'EMAIL_FROM="{config.get("EMAIL_FROM", "")}"',
                f'EMAIL_PASSWORD="{config.get("EMAIL_PASSWORD", "")}"',
                f'EMAIL_TO="{config.get("EMAIL_TO", "")}"',
                "",
                "# ========== Webhook 配置 ==========",
                f'WEBHOOK_ENABLED="{config.get("WEBHOOK_ENABLED", "false")}"',
                f'WEBHOOK_TYPE="{config.get("WEBHOOK_TYPE", "dingtalk")}"',
                f'WEBHOOK_URL="{config.get("WEBHOOK_URL", "")}"',
                f'WEBHOOK_KEYWORD="{config.get("WEBHOOK_KEYWORD", "aiagents通知")}"',
            ]

            with open(self.env_file, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))

            return True
        except Exception as e:
            print(f"保存 .env 失败: {e}")
            return False

    def get_config_info(self) -> Dict[str, Dict[str, Any]]:
        """Return config metadata with current values."""
        current_values = self.read_env()
        config_info = {}

        for key, info in self.default_config.items():
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

        api_key = config.get("DEEPSEEK_API_KEY", "")
        if api_key and len(api_key) < 20:
            return False, "DeepSeek API Key 格式不正确（长度太短）"

        return True, "配置验证通过"

    def reload_config(self):
        """Reload `.env` into the process environment."""
        from dotenv import load_dotenv

        load_dotenv(override=True)


config_manager = ConfigManager()
