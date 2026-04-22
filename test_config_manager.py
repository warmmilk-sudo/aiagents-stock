import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import config
from config_manager import ConfigManager


class ConfigManagerEnvFormatTests(unittest.TestCase):
    def test_write_env_preserves_existing_layout_and_quote_style(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "# top comment\n"
                'WARMMILK_CONFIG={"API_KEY":"old-key","BASE_URL":"https://old.example/v1"}\n'
                "LIGHTWEIGHT_MODEL_NAME=\"gemini-3-flash\"\n"
                "REASONING_MODEL_NAME='doubao-2-0-pro'\n"
                "OTHER_KEY=keepme\n",
                encoding="utf-8",
            )

            manager = ConfigManager(str(env_path))
            saved = manager.write_env(
                {
                    "WARMMILK_CONFIG": json.dumps(
                        {"API_KEY": "new-key", "BASE_URL": "https://new.example/v1"},
                        ensure_ascii=False,
                    ),
                    "LIGHTWEIGHT_MODEL_NAME": "doubao-2-0-mini",
                    "REASONING_MODEL_NAME": "deepseek-v3-2",
                }
            )

            self.assertTrue(saved)
            self.assertEqual(
                env_path.read_text(encoding="utf-8"),
                "# top comment\n"
                'WARMMILK_CONFIG={"API_KEY":"new-key","BASE_URL":"https://new.example/v1"}\n'
                "LIGHTWEIGHT_MODEL_NAME=\"doubao-2-0-mini\"\n"
                "REASONING_MODEL_NAME='deepseek-v3-2'\n"
                "OTHER_KEY=keepme\n",
            )

    def test_write_env_only_appends_missing_managed_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("CUSTOM_KEY=1\n", encoding="utf-8")

            manager = ConfigManager(str(env_path))
            saved = manager.write_env({"LIGHTWEIGHT_MODEL_NAME": "doubao-2-0-lite"})

            self.assertTrue(saved)
            self.assertEqual(
                env_path.read_text(encoding="utf-8"),
                "CUSTOM_KEY=1\n\nLIGHTWEIGHT_MODEL_NAME=doubao-2-0-lite\n",
            )

    def test_read_env_ignores_inline_comments_and_decodes_quotes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "LIGHTWEIGHT_MODEL_NAME=\"doubao-2-0-mini\" # active model\n"
                "REASONING_MODEL_NAME='deepseek-v3-2'\n",
                encoding="utf-8",
            )

            manager = ConfigManager(str(env_path))
            values = manager.read_env()

            self.assertEqual(values["LIGHTWEIGHT_MODEL_NAME"], "doubao-2-0-mini")
            self.assertEqual(values["REASONING_MODEL_NAME"], "deepseek-v3-2")

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
                    "WARMMILK_CONFIG": json.dumps(
                        {"API_KEY": "x" * 20, "BASE_URL": "https://generativelanguage.googleapis.com/v1beta/openai/"},
                        ensure_ascii=False,
                    ),
                    "LIGHTWEIGHT_MODEL_NAME": "gemini-3-flash",
                    "REASONING_MODEL_NAME": "gemini-3-flash",
                    "TDX_ENABLED": "true",
                    "TDX_BASE_URL": "",
                }
            )

            self.assertFalse(is_valid)
            self.assertIn("TDX API 地址不能为空", message)

    def test_validate_config_allows_short_warmmilk_api_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

            is_valid, message = manager.validate_config(
                {
                    "WARMMILK_CONFIG": json.dumps(
                        {"API_KEY": "short-key", "BASE_URL": "https://generativelanguage.googleapis.com/v1beta/openai/"},
                        ensure_ascii=False,
                    ),
                    "LIGHTWEIGHT_MODEL_NAME": "gemini-3-flash",
                    "REASONING_MODEL_NAME": "gemini-3-flash",
                    "TDX_ENABLED": "false",
                    "TDX_BASE_URL": "",
                }
            )

            self.assertTrue(is_valid)
            self.assertEqual(message, "配置验证通过")

    def test_validate_config_allows_model_specific_credentials(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

            is_valid, message = manager.validate_config(
                {
                    "LIGHTWEIGHT_MODEL_NAME": "doubao-2-0-mini",
                    "REASONING_MODEL_NAME": "deepseek-v3-2",
                    "VOICE_CONFIG": json.dumps(
                        {"API_KEY": "voice-key", "BASE_URL": "https://voice.example/v1"},
                        ensure_ascii=False,
                    ),
                    "TDX_ENABLED": "false",
                    "TDX_BASE_URL": "",
                }
            )

            self.assertTrue(is_valid)
            self.assertEqual(message, "配置验证通过")

    def test_validate_config_rejects_unsupported_llm_models(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

            with patch.object(
                manager,
                "_fetch_available_llm_models",
                return_value=["gemini-3-flash", "deepseek-v3-2", "doubao-2-0-pro"],
            ):
                is_valid, message = manager.validate_config(
                {
                    "LIGHTWEIGHT_MODEL_NAME": "gpt-5.4-mini",
                    "REASONING_MODEL_NAME": "gpt-5.4",
                    "WARMMILK_CONFIG": json.dumps(
                        {"API_KEY": "warmmilk-key", "BASE_URL": "https://generativelanguage.googleapis.com/v1beta/openai/"},
                        ensure_ascii=False,
                    ),
                    "VOICE_CONFIG": json.dumps(
                        {"API_KEY": "voice-key", "BASE_URL": "http://llmapi.example/v1"},
                        ensure_ascii=False,
                    ),
                    "LIGHTWEIGHT_MODEL_OPTIONS": "gemini-3-flash,gpt-5.4-mini",
                    "REASONING_MODEL_OPTIONS": "gpt-5.4,deepseek-v3-2",
                    "TDX_ENABLED": "false",
                    "TDX_BASE_URL": "",
                    }
                )

        self.assertFalse(is_valid)
        self.assertIn("LIGHTWEIGHT_MODEL_NAME=gpt-5.4-mini", message)
        self.assertIn("REASONING_MODEL_NAME=gpt-5.4", message)
        self.assertIn("LIGHTWEIGHT_MODEL_OPTIONS 包含 gpt-5.4-mini", message)
        self.assertIn("REASONING_MODEL_OPTIONS 包含 gpt-5.4", message)

    def test_validate_config_allows_supported_llm_models(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            manager = ConfigManager(str(env_path))

            with patch.object(
                manager,
                "_fetch_available_llm_models",
                return_value=["gemini-3-flash", "deepseek-v3-2-251201", "doubao-seed-2-0-pro-260215"],
            ):
                is_valid, message = manager.validate_config(
                {
                    "LIGHTWEIGHT_MODEL_NAME": "gemini-3-flash",
                    "REASONING_MODEL_NAME": "deepseek-v3-2",
                    "WARMMILK_CONFIG": json.dumps(
                        {"API_KEY": "warmmilk-key", "BASE_URL": "https://generativelanguage.googleapis.com/v1beta/openai/"},
                        ensure_ascii=False,
                    ),
                    "VOICE_CONFIG": json.dumps(
                        {"API_KEY": "voice-key", "BASE_URL": "https://api.deepseek.com/v1"},
                        ensure_ascii=False,
                    ),
                    "LIGHTWEIGHT_MODEL_OPTIONS": "gemini-3-flash,doubao-2-0-pro",
                    "REASONING_MODEL_OPTIONS": "deepseek-v3-2,doubao-2-0-pro",
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
            self.assertIn("WARMMILK_CONFIG", config_info)
            self.assertIn("VOICE_CONFIG", config_info)

    def test_get_config_info_masks_password_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                'WARMMILK_CONFIG={"API_KEY":"secret-key","BASE_URL":"https://warmmilk.example/v1"}\n',
                encoding="utf-8",
            )
            manager = ConfigManager(str(env_path))

            config_info = manager.get_config_info()

            self.assertEqual(config_info["WARMMILK_CONFIG"]["value"], '{"API_KEY":"********","BASE_URL":"https://warmmilk.example/v1"}')

    def test_get_config_info_masks_json_api_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                'VOICE_CONFIG={"API_KEY":"secret-key","BASE_URL":"https://voice.example/v1"}\n',
                encoding="utf-8",
            )
            manager = ConfigManager(str(env_path))

            config_info = manager.get_config_info()

            self.assertIn('"API_KEY":"********"', config_info["VOICE_CONFIG"]["value"])
            self.assertIn('"BASE_URL":"https://voice.example/v1"', config_info["VOICE_CONFIG"]["value"])

    def test_resolve_masked_secrets_preserves_existing_password_values(self):
        manager = ConfigManager()

        resolved = manager.resolve_masked_secrets(
            {
                "WARMMILK_CONFIG": '{"API_KEY":"********","BASE_URL":"https://warmmilk.example/v1"}',
                "LIGHTWEIGHT_MODEL_NAME": "doubao-2-0-mini",
            },
            {
                "WARMMILK_CONFIG": '{"API_KEY":"existing-secret","BASE_URL":"https://warmmilk.example/v1"}',
                "LIGHTWEIGHT_MODEL_NAME": "gemini-3-flash",
            },
        )

        self.assertEqual(
            resolved,
            {
                "WARMMILK_CONFIG": '{"API_KEY":"existing-secret","BASE_URL":"https://warmmilk.example/v1"}',
                "LIGHTWEIGHT_MODEL_NAME": "doubao-2-0-mini",
            },
        )

    def test_filter_system_config_values_excludes_smart_monitor_fields(self):
        manager = ConfigManager()

        filtered = manager.filter_system_config_values(
            {
                "WARMMILK_CONFIG": "key",
                "SMART_MONITOR_AI_INTERVAL_MINUTES": "15",
                "SMART_MONITOR_DEFAULT_STOP_LOSS_PCT": "6",
            }
        )

        self.assertEqual(filtered, {"WARMMILK_CONFIG": "key"})

    def test_read_env_ignores_unknown_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "UNKNOWN_API_KEY=legacy-key\n"
                "UNKNOWN_BASE_URL=https://legacy.example.com/v1\n",
                encoding="utf-8",
            )

            manager = ConfigManager(str(env_path))
            values = manager.read_env()

            self.assertNotIn("UNKNOWN_API_KEY", values)
            self.assertNotIn("UNKNOWN_BASE_URL", values)
            self.assertEqual(values["WARMMILK_CONFIG"], config.WARMMILK_CONFIG or json.dumps({"API_KEY": config.WARMMILK_API_KEY, "BASE_URL": config.WARMMILK_BASE_URL}, ensure_ascii=False, separators=(",", ":")))
            self.assertEqual(values["VOICE_CONFIG"], config.VOICE_CONFIG or json.dumps({"API_KEY": config.VOICE_API_KEY, "BASE_URL": config.VOICE_BASE_URL}, ensure_ascii=False, separators=(",", ":")))

    def test_get_model_api_credentials_uses_shared_provider_configs(self):
        overrides = {
            "WARMMILK_CONFIG": json.dumps(
                {"API_KEY": "gemini-key", "BASE_URL": "https://gemini.example/v1"},
                ensure_ascii=False,
            ),
            "VOICE_CONFIG": json.dumps(
                {"API_KEY": "voice-key", "BASE_URL": "https://voice.example/v1"},
                ensure_ascii=False,
            ),
        }

        self.assertEqual(
            config.get_model_api_credentials("gemini-3-flash", overrides),
            ("gemini-key", "https://gemini.example/v1"),
        )
        self.assertEqual(
            config.get_model_api_credentials("doubao-2-0-pro", overrides),
            ("voice-key", "https://voice.example/v1"),
        )
        self.assertEqual(
            config.get_model_api_credentials("deepseek-v3-2", overrides),
            ("voice-key", "https://voice.example/v1"),
        )
        self.assertEqual(
            config.get_model_api_credentials("deepseek-chat", overrides),
            ("", ""),
        )
        self.assertEqual(
            config.get_model_api_name("doubao-2-0-pro", overrides),
            "doubao-seed-2-0-pro-260215",
        )
        self.assertEqual(
            config.get_model_api_name("deepseek-v3-2", overrides),
            "deepseek-v3-2-251201",
        )


if __name__ == "__main__":
    unittest.main()
