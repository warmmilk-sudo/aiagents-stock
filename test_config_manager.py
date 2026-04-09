import tempfile
import unittest
from pathlib import Path

from config_manager import ConfigManager


class ConfigManagerEnvFormatTests(unittest.TestCase):
    def test_write_env_preserves_existing_layout_and_quote_style(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "# top comment\n"
                "LIGHTWEIGHT_MODEL_NAME=\"deepseek-chat\"\n"
                "DEEPSEEK_BASE_URL = https://api.deepseek.com/v1  # inline comment\n"
                "REASONING_MODEL_NAME='deepseek-reasoner'\n"
                "OTHER_KEY=keepme\n",
                encoding="utf-8",
            )

            manager = ConfigManager(str(env_path))
            saved = manager.write_env(
                {
                    "LIGHTWEIGHT_MODEL_NAME": "qwen-plus",
                    "DEEPSEEK_BASE_URL": "https://example.com/v1",
                    "REASONING_MODEL_NAME": "qwen-max",
                }
            )

            self.assertTrue(saved)
            self.assertEqual(
                env_path.read_text(encoding="utf-8"),
                "# top comment\n"
                "LIGHTWEIGHT_MODEL_NAME=\"qwen-plus\"\n"
                "DEEPSEEK_BASE_URL = https://example.com/v1  # inline comment\n"
                "REASONING_MODEL_NAME='qwen-max'\n"
                "OTHER_KEY=keepme\n",
            )

    def test_write_env_only_appends_missing_managed_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("CUSTOM_KEY=1\n", encoding="utf-8")

            manager = ConfigManager(str(env_path))
            saved = manager.write_env({"LIGHTWEIGHT_MODEL_NAME": "qwen-plus"})

            self.assertTrue(saved)
            self.assertEqual(
                env_path.read_text(encoding="utf-8"),
                "CUSTOM_KEY=1\n\nLIGHTWEIGHT_MODEL_NAME=qwen-plus\n",
            )

    def test_read_env_ignores_inline_comments_and_decodes_quotes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "LIGHTWEIGHT_MODEL_NAME=\"qwen-plus\" # active model\n"
                "REASONING_MODEL_NAME='qwen-max'\n",
                encoding="utf-8",
            )

            manager = ConfigManager(str(env_path))
            values = manager.read_env()

            self.assertEqual(values["LIGHTWEIGHT_MODEL_NAME"], "qwen-plus")
            self.assertEqual(values["REASONING_MODEL_NAME"], "qwen-max")

    def test_read_env_uses_blank_tdx_url_default(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

        values = manager.read_env()

        self.assertEqual(values["TDX_BASE_URL"], "")
        self.assertEqual(values["TDX_TIMEOUT_SECONDS"], "10")
        self.assertEqual(values["SMART_MONITOR_DATA_FETCH_TIMEOUT_SECONDS"], "45")

    def test_validate_config_requires_tdx_url_when_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

            is_valid, message = manager.validate_config(
                {
                    "DEEPSEEK_API_KEY": "x" * 20,
                    "TDX_ENABLED": "true",
                    "TDX_BASE_URL": "",
                }
            )

            self.assertFalse(is_valid)
            self.assertIn("TDX API 地址不能为空", message)

    def test_validate_config_allows_short_deepseek_api_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

            is_valid, message = manager.validate_config(
                {
                    "DEEPSEEK_API_KEY": "short-key",
                    "TDX_ENABLED": "false",
                    "TDX_BASE_URL": "",
                }
            )

            self.assertTrue(is_valid)
            self.assertEqual(message, "配置验证通过")

    def test_get_config_info_hides_smart_monitor_fields(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

            config_info = manager.get_config_info()

            self.assertNotIn("SMART_MONITOR_AI_INTERVAL_MINUTES", config_info)
            self.assertNotIn("SMART_MONITOR_DEFAULT_STOP_LOSS_PCT", config_info)
            self.assertIn("DEEPSEEK_API_KEY", config_info)

    def test_filter_system_config_values_excludes_smart_monitor_fields(self):
        manager = ConfigManager()

        filtered = manager.filter_system_config_values(
            {
                "DEEPSEEK_API_KEY": "key",
                "SMART_MONITOR_AI_INTERVAL_MINUTES": "15",
                "SMART_MONITOR_DEFAULT_STOP_LOSS_PCT": "6",
            }
        )

        self.assertEqual(filtered, {"DEEPSEEK_API_KEY": "key"})


if __name__ == "__main__":
    unittest.main()
