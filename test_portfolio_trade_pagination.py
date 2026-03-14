import shutil
import unittest
import uuid
from pathlib import Path

from portfolio_db import PortfolioDB


TEST_TMP_ROOT = Path(".codex_test_tmp")


def make_workspace_temp_dir(prefix: str) -> Path:
    TEST_TMP_ROOT.mkdir(exist_ok=True)
    path = TEST_TMP_ROOT / f"{prefix}{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


class PortfolioTradePaginationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = make_workspace_temp_dir("portfolio_trade_pagination_")
        self.db = PortfolioDB(str(self.temp_dir / "portfolio.db"))

        self.account_a_stock_id = self.db.add_stock(
            "600519",
            "贵州茅台",
            cost_price=1500.0,
            quantity=100,
            account_name="账户A",
        )
        self.account_b_stock_id = self.db.add_stock(
            "000001",
            "平安银行",
            cost_price=10.0,
            quantity=200,
            account_name="账户B",
        )

        for index in range(25):
            self.db.add_trade_history(
                self.account_a_stock_id,
                trade_type="buy" if index % 2 == 0 else "sell",
                trade_date=f"2026-03-{(index % 9) + 1:02d} 09:{index % 60:02d}:00",
                price=1500.0 + index,
                quantity=100 + index,
                note=f"A-{index}",
            )

        for index in range(5):
            self.db.add_trade_history(
                self.account_b_stock_id,
                trade_type="buy",
                trade_date=f"2026-03-{(index % 9) + 1:02d} 10:{index % 60:02d}:00",
                price=10.0 + index,
                quantity=200 + index,
                note=f"B-{index}",
            )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_trade_records_page_returns_items_total_and_pagination(self):
        first_page = self.db.get_trade_records_page(account_name="账户A", page=1, page_size=20)
        second_page = self.db.get_trade_records_page(account_name="账户A", page=2, page_size=20)

        self.assertEqual(first_page["page"], 1)
        self.assertEqual(first_page["page_size"], 20)
        self.assertEqual(first_page["total"], 25)
        self.assertEqual(len(first_page["items"]), 20)

        self.assertEqual(second_page["page"], 2)
        self.assertEqual(second_page["page_size"], 20)
        self.assertEqual(second_page["total"], 25)
        self.assertEqual(len(second_page["items"]), 5)

    def test_get_trade_records_page_respects_account_filter(self):
        account_a_page = self.db.get_trade_records_page(account_name="账户A", page=1, page_size=50)
        account_b_page = self.db.get_trade_records_page(account_name="账户B", page=1, page_size=50)

        self.assertEqual(account_a_page["total"], 25)
        self.assertTrue(all(item["account_name"] == "账户A" for item in account_a_page["items"]))

        self.assertEqual(account_b_page["total"], 5)
        self.assertTrue(all(item["account_name"] == "账户B" for item in account_b_page["items"]))


if __name__ == "__main__":
    unittest.main()
