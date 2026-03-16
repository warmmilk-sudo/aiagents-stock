import sys
import types
import unittest

from notification_service import NotificationService


class _DummyResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class NotificationServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = NotificationService()
        self.service.config.update(
            {
                "webhook_enabled": True,
                "webhook_url": "",
                "webhook_type": "feishu",
                "webhook_keyword": "",
            }
        )

    def _install_requests_stub(self, post_callable):
        timeout_error = type("Timeout", (Exception,), {})
        request_error = type("RequestException", (Exception,), {})
        stub = types.SimpleNamespace(
            post=post_callable,
            exceptions=types.SimpleNamespace(Timeout=timeout_error, RequestException=request_error),
        )
        original = sys.modules.get("requests")
        sys.modules["requests"] = stub
        return original, timeout_error, request_error

    def _restore_requests_stub(self, original):
        if original is None:
            sys.modules.pop("requests", None)
            return
        sys.modules["requests"] = original

    def test_send_test_webhook_reports_type_mismatch_for_feishu(self):
        self.service.config["webhook_type"] = "feishu"
        self.service.config["webhook_url"] = "https://oapi.dingtalk.com/robot/send?access_token=test"

        success, message = self.service.send_test_webhook()

        self.assertFalse(success)
        self.assertIn("更像钉钉机器人地址", message)

    def test_send_test_webhook_returns_feishu_api_message(self):
        original, _, _ = self._install_requests_stub(
            lambda *args, **kwargs: _DummyResponse(
                200,
                payload={"code": 19024, "msg": "Bot is not in the chat"},
                text='{"code":19024,"msg":"Bot is not in the chat"}',
            )
        )
        try:
            self.service.config["webhook_type"] = "feishu"
            self.service.config["webhook_url"] = "https://open.feishu.cn/open-apis/bot/v2/hook/test"

            success, message = self.service.send_test_webhook()
        finally:
            self._restore_requests_stub(original)

        self.assertFalse(success)
        self.assertIn("飞书Webhook返回错误", message)
        self.assertIn("Bot is not in the chat", message)

    def test_feishu_webhook_includes_keyword_when_configured(self):
        captured_payload = {}

        def post(*args, **kwargs):
            captured_payload.update(kwargs.get("json", {}))
            return _DummyResponse(200, payload={"code": 0, "msg": "success"})

        original, _, _ = self._install_requests_stub(post)
        try:
            self.service.config["webhook_type"] = "feishu"
            self.service.config["webhook_url"] = "https://open.feishu.cn/open-apis/bot/v2/hook/test"
            self.service.config["webhook_keyword"] = "aiagents通知"

            success = self.service._send_feishu_webhook(
                {
                    "symbol": "测试",
                    "name": "Webhook配置测试",
                    "type": "系统测试",
                    "message": "如果您收到此消息，说明Webhook配置正确！",
                    "triggered_at": "刚刚",
                }
            )
        finally:
            self._restore_requests_stub(original)

        self.assertTrue(success)
        payload_text = str(captured_payload)
        self.assertIn("aiagents通知", payload_text)


if __name__ == "__main__":
    unittest.main()
