import sys
import types
import unittest

sys.modules.setdefault(
    "numpy",
    types.SimpleNamespace(
        inf=float("inf"),
        nan=float("nan"),
        where=lambda condition, x, y: x if condition else y,
        isnan=lambda value: False,
    ),
)
sys.modules.setdefault(
    "pandas",
    types.SimpleNamespace(
        DataFrame=type("DataFrame", (), {}),
        Series=type("Series", (), {}),
        Timestamp=type("Timestamp", (), {}),
        isna=lambda value: False,
        to_datetime=lambda value, *args, **kwargs: value,
        bdate_range=lambda *args, **kwargs: [],
        date_range=lambda *args, **kwargs: [],
        to_numeric=lambda value, *args, **kwargs: value,
        concat=lambda *args, **kwargs: None,
    ),
)

from portfolio_manager import portfolio_manager


class PortfolioManagerPriceSafetyTests(unittest.TestCase):
    def test_build_stock_card_view_model_ignores_non_realtime_price(self):
        stock = {
            "name": "贵州茅台",
            "cost_price": 10.0,
            "quantity": 100,
            "auto_monitor": True,
        }
        latest_analysis = {
            "current_price": 0.0,
            "analysis_time": "2026-03-19 10:00:00",
            "summary": "测试",
        }

        card = portfolio_manager.build_stock_card_view_model(stock, latest_analysis)

        self.assertEqual(card["pnl_amount_text"], "")
        self.assertEqual(card["pnl_percent_text"], "")

    def test_build_analysis_payload_leaves_current_price_empty_without_realtime_quote(self):
        payload = portfolio_manager._build_analysis_payload(
            stock_info={"symbol": "600519", "name": "贵州茅台", "current_price": "N/A"},
            final_decision={"rating": "持有"},
        )

        self.assertIsNone(payload["current_price"])


if __name__ == "__main__":
    unittest.main()
