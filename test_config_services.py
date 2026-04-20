import unittest
from unittest.mock import patch

from backend import services


class ConfigServicesTests(unittest.TestCase):
    def test_save_config_values_merges_current_env_for_partial_updates(self):
        current_values = {
            "LLM_API_KEY": "existing-secret",
            "LIGHTWEIGHT_MODEL_NAME": "deepseek-chat",
            "REASONING_MODEL_NAME": "deepseek-reasoner",
        }

        with patch.object(
            services.config_manager,
            "filter_system_config_values",
            return_value={"LIGHTWEIGHT_MODEL_NAME": "qwen-plus"},
        ) as filter_mock, patch.object(
            services.config_manager,
            "read_env",
            return_value=current_values,
        ) as read_env_mock, patch.object(
            services.config_manager,
            "validate_config",
            return_value=(True, "配置验证通过"),
        ) as validate_mock, patch.object(
            services.config_manager,
            "write_env",
            return_value=True,
        ) as write_env_mock, patch.object(
            services.config_manager,
            "reload_config",
            return_value=None,
        ) as reload_mock, patch.object(
            type(services.notification_service),
            "__init__",
            return_value=None,
        ) as reinit_mock, patch.object(
            services,
            "ensure_runtime_started",
            return_value=None,
        ) as ensure_mock:
            success, message = services.save_config_values({"LIGHTWEIGHT_MODEL_NAME": "qwen-plus"})

        self.assertTrue(success)
        self.assertEqual(message, "配置已保存")
        filter_mock.assert_called_once_with({"LIGHTWEIGHT_MODEL_NAME": "qwen-plus"})
        read_env_mock.assert_called_once()
        validate_mock.assert_called_once_with(
            {
                "LLM_API_KEY": "existing-secret",
                "LIGHTWEIGHT_MODEL_NAME": "qwen-plus",
                "REASONING_MODEL_NAME": "deepseek-reasoner",
            }
        )
        write_env_mock.assert_called_once_with({"LIGHTWEIGHT_MODEL_NAME": "qwen-plus"})
        reload_mock.assert_called_once()
        reinit_mock.assert_called_once()
        ensure_mock.assert_called_once()

    def test_save_config_values_preserves_masked_password_fields(self):
        current_values = {
            "LLM_API_KEY": "existing-secret",
            "REASONING_MODEL_NAME": "deepseek-reasoner",
        }

        with patch.object(
            services.config_manager,
            "filter_system_config_values",
            return_value={
                "LLM_API_KEY": "********",
                "REASONING_MODEL_NAME": "gpt-5.4",
            },
        ) as filter_mock, patch.object(
            services.config_manager,
            "read_env",
            return_value=current_values,
        ) as read_env_mock, patch.object(
            services.config_manager,
            "validate_config",
            return_value=(True, "配置验证通过"),
        ) as validate_mock, patch.object(
            services.config_manager,
            "write_env",
            return_value=True,
        ) as write_env_mock, patch.object(
            services.config_manager,
            "reload_config",
            return_value=None,
        ), patch.object(
            type(services.notification_service),
            "__init__",
            return_value=None,
        ), patch.object(
            services,
            "ensure_runtime_started",
            return_value=None,
        ):
            success, message = services.save_config_values(
                {
                    "LLM_API_KEY": "********",
                    "REASONING_MODEL_NAME": "gpt-5.4",
                }
            )

        self.assertTrue(success)
        self.assertEqual(message, "配置已保存")
        filter_mock.assert_called_once_with(
            {
                "LLM_API_KEY": "********",
                "REASONING_MODEL_NAME": "gpt-5.4",
            }
        )
        read_env_mock.assert_called_once()
        validate_mock.assert_called_once_with(
            {
                "LLM_API_KEY": "existing-secret",
                "REASONING_MODEL_NAME": "gpt-5.4",
            }
        )
        write_env_mock.assert_called_once_with(
            {
                "LLM_API_KEY": "existing-secret",
                "REASONING_MODEL_NAME": "gpt-5.4",
            }
        )


if __name__ == "__main__":
    unittest.main()
