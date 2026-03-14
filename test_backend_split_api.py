import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app
from backend.services import parse_stock_list


class BackendSplitApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.password_patcher = patch("backend.auth.config.ADMIN_PASSWORD", "split-secret")
        self.password_hash_patcher = patch("backend.auth.config.ADMIN_PASSWORD_HASH", "")
        self.password_patcher.start()
        self.password_hash_patcher.start()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.password_patcher.stop()
        self.password_hash_patcher.stop()

    def login(self) -> None:
        response = self.client.post("/api/auth/login", json={"password": "split-secret"})
        self.assertEqual(response.status_code, 200)

    def test_auth_session_defaults_to_unauthenticated(self):
        response = self.client.get("/api/auth/session")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success"])
        self.assertFalse(payload["data"]["authenticated"])

    def test_login_sets_cookie(self):
        response = self.client.post("/api/auth/login", json={"password": "split-secret"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["success"])
        self.assertTrue(payload["data"]["authenticated"])
        self.assertNotIn("legacy_token", payload["data"])
        self.assertIn("aiagents_session=", response.headers.get("set-cookie", ""))

    def test_parse_stock_list_normalizes_and_deduplicates(self):
        values = parse_stock_list("000001, 600519\nAAPL；600519\n 00700 ")
        self.assertEqual(values, ["000001", "600519", "AAPL", "00700"])

    def test_submit_main_force_task_requires_auth_and_calls_service(self):
        self.login()
        with patch("backend.services.submit_main_force_selection_task", return_value="task-main-force") as mocked:
            response = self.client.post(
                "/api/selectors/main-force/tasks",
                json={
                    "days_ago": 90,
                    "start_date": None,
                    "final_n": 5,
                    "max_change": 30,
                    "min_cap": 50,
                    "max_cap": 5000,
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-main-force")
        mocked.assert_called_once()

    def test_submit_low_price_bull_task_uses_service(self):
        self.login()
        with patch("backend.services.submit_low_price_bull_selection_task", return_value="task-low-price") as mocked:
            response = self.client.post(
                "/api/selectors/low-price-bull/tasks",
                json={
                    "top_n": 5,
                    "max_price": 10,
                    "min_profit_growth": 100,
                    "min_turnover_yi": 0,
                    "max_turnover_yi": 0,
                    "min_market_cap_yi": 0,
                    "max_market_cap_yi": 0,
                    "sort_by": "成交额升序",
                    "exclude_st": True,
                    "exclude_kcb": True,
                    "exclude_cyb": True,
                    "only_hs_a": True,
                    "filter_summary": "股价≤10元",
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-low-price")
        mocked.assert_called_once()

    def test_submit_longhubang_task_uses_service(self):
        self.login()
        with patch("backend.services.submit_longhubang_task", return_value="task-longhubang") as mocked:
            response = self.client.post(
                "/api/strategies/longhubang/tasks",
                json={
                    "date": "2026-03-13",
                    "days": 1,
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-longhubang")
        mocked.assert_called_once()

    def test_submit_small_cap_task_uses_service(self):
        self.login()
        with patch("backend.services.submit_small_cap_selection_task", return_value="task-small-cap") as mocked:
            response = self.client.post(
                "/api/selectors/small-cap/tasks",
                json={
                    "top_n": 5,
                    "max_market_cap_yi": 50,
                    "min_revenue_growth": 10,
                    "min_profit_growth": 100,
                    "sort_by": "总市值升序",
                    "exclude_st": True,
                    "exclude_kcb": True,
                    "exclude_cyb": True,
                    "only_hs_a": True,
                    "filter_summary": "总市值≤50亿",
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-small-cap")
        mocked.assert_called_once()

    def test_submit_profit_growth_task_uses_service(self):
        self.login()
        with patch("backend.services.submit_profit_growth_selection_task", return_value="task-profit-growth") as mocked:
            response = self.client.post(
                "/api/selectors/profit-growth/tasks",
                json={
                    "top_n": 5,
                    "min_profit_growth": 10,
                    "min_turnover_yi": 0,
                    "max_turnover_yi": 0,
                    "sort_by": "成交额升序",
                    "exclude_st": True,
                    "exclude_kcb": True,
                    "exclude_cyb": True,
                    "filter_summary": "净利增长≥10%",
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-profit-growth")
        mocked.assert_called_once()

    def test_submit_value_stock_task_uses_service(self):
        self.login()
        with patch("backend.services.submit_value_stock_selection_task", return_value="task-value-stock") as mocked:
            response = self.client.post(
                "/api/selectors/value-stock/tasks",
                json={
                    "top_n": 10,
                    "max_pe": 20,
                    "max_pb": 1.5,
                    "min_dividend_yield": 1,
                    "max_debt_ratio": 30,
                    "min_float_cap_yi": 0,
                    "max_float_cap_yi": 0,
                    "sort_by": "流通市值升序",
                    "exclude_st": True,
                    "exclude_kcb": True,
                    "exclude_cyb": True,
                    "filter_summary": "PE≤20",
                },
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-value-stock")
        mocked.assert_called_once()

    def test_submit_macro_cycle_task_uses_service(self):
        self.login()
        with patch("backend.services.submit_macro_cycle_task", return_value="task-macro-cycle") as mocked:
            response = self.client.post("/api/strategies/macro-cycle/tasks", json={})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-macro-cycle")
        mocked.assert_called_once()

    def test_submit_news_flow_task_uses_service(self):
        self.login()
        with patch("backend.services.submit_news_flow_task", return_value="task-news-flow") as mocked:
            response = self.client.post("/api/strategies/news-flow/tasks", json={"category": "finance"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["task_id"], "task-news-flow")
        mocked.assert_called_once()

    def test_sector_strategy_scheduler_status_requires_auth_and_returns_payload(self):
        self.login()
        with patch(
            "backend.services.get_sector_strategy_scheduler_status",
            return_value={"running": True, "schedule_time": "09:00"},
        ) as mocked:
            response = self.client.get("/api/strategies/sector-strategy/scheduler")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["data"]["running"])
        mocked.assert_called_once()

    def test_sector_strategy_history_delete_uses_service(self):
        self.login()
        with patch("backend.services.delete_sector_strategy_report", return_value=True) as mocked:
            response = self.client.delete("/api/strategies/sector-strategy/history/7")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["report_id"], 7)
        mocked.assert_called_once_with(7)

    def test_main_force_markdown_export_returns_attachment(self):
        self.login()
        with patch(
            "backend.services.export_main_force_markdown",
            return_value=(b"hello", "report.md", "text/markdown; charset=utf-8"),
        ) as mocked:
            response = self.client.post(
                "/api/exports/main-force/markdown",
                json={"result": {"success": True}, "context_snapshot": {}},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"hello")
        self.assertIn('attachment; filename="report.md"', response.headers.get("content-disposition", ""))
        mocked.assert_called_once()

    def test_longhubang_markdown_export_returns_attachment(self):
        self.login()
        with patch(
            "backend.services.export_longhubang_markdown",
            return_value=(b"longhubang", "longhubang.md", "text/markdown; charset=utf-8"),
        ) as mocked:
            response = self.client.post(
                "/api/exports/longhubang/markdown",
                json={"result": {"success": True}},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"longhubang")
        self.assertIn('attachment; filename="longhubang.md"', response.headers.get("content-disposition", ""))
        mocked.assert_called_once()

    def test_macro_cycle_markdown_export_returns_attachment(self):
        self.login()
        with patch(
            "backend.services.export_macro_cycle_markdown",
            return_value=(b"macro", "macro.md", "text/markdown; charset=utf-8"),
        ) as mocked:
            response = self.client.post("/api/exports/macro-cycle/markdown", json={"result": {"success": True}})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"macro")
        self.assertIn('attachment; filename="macro.md"', response.headers.get("content-disposition", ""))
        mocked.assert_called_once()

    def test_news_flow_pdf_export_returns_attachment(self):
        self.login()
        with patch(
            "backend.services.export_news_flow_pdf",
            return_value=(b"newsflow", "newsflow.pdf", "application/pdf"),
        ) as mocked:
            response = self.client.post("/api/exports/news-flow/pdf", json={"result": {"success": True}})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"newsflow")
        self.assertIn('attachment; filename="newsflow.pdf"', response.headers.get("content-disposition", ""))
        mocked.assert_called_once()


if __name__ == "__main__":
    unittest.main()
