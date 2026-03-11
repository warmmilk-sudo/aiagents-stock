import unittest
from unittest.mock import MagicMock, patch

import requests

from smart_monitor_deepseek import SmartMonitorDeepSeek


class SmartMonitorDeepSeekTests(unittest.TestCase):
    @patch("smart_monitor_deepseek.time_module.sleep", return_value=None)
    @patch("smart_monitor_deepseek.requests.post")
    def test_chat_completion_retries_timeout_and_uses_reasoner_budget(self, mock_post, _mock_sleep):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"choices": [{"message": {"content": "{}"}}]}
        mock_post.side_effect = [
            requests.exceptions.ReadTimeout("slow response"),
            response,
        ]

        client = SmartMonitorDeepSeek(api_key="test-key")
        client.http_timeout_seconds = 31
        client.http_retry_count = 1
        client.reasoning_max_tokens = 3200

        result = client.chat_completion(
            messages=[{"role": "user", "content": "test"}],
            model="deepseek-reasoner",
            max_tokens=2000,
        )

        self.assertEqual(result, {"choices": [{"message": {"content": "{}"}}]})
        self.assertEqual(mock_post.call_count, 2)
        for call in mock_post.call_args_list:
            self.assertEqual(call.kwargs["timeout"], (10, 31))
            self.assertEqual(call.kwargs["json"]["max_tokens"], 3200)

    @patch("smart_monitor_deepseek.requests.post")
    def test_chat_completion_keeps_explicit_higher_max_tokens(self, mock_post):
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        mock_post.return_value = response

        client = SmartMonitorDeepSeek(api_key="test-key")
        client.reasoning_max_tokens = 3000

        client.chat_completion(
            messages=[{"role": "user", "content": "test"}],
            model="deepseek-reasoner",
            max_tokens=4800,
        )

        self.assertEqual(mock_post.call_args.kwargs["json"]["max_tokens"], 4800)


if __name__ == "__main__":
    unittest.main()
