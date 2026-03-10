import os
import tempfile
import unittest

from analysis_history_service import AnalysisHistoryService
from analysis_repository import AnalysisRepository
from asset_repository import AssetRepository, STATUS_PORTFOLIO


class AnalysisHistoryServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "investment.db")
        self.legacy_db_path = os.path.join(self.temp_dir.name, "legacy.db")
        self.repository = AnalysisRepository(
            self.db_path,
            legacy_analysis_db_path=self.legacy_db_path,
        )
        self.asset_store = AssetRepository(self.db_path)
        self.service = AnalysisHistoryService(repository=self.repository, asset_store=self.asset_store)

        self.research_asset_id = self.asset_store.create_or_update_research_asset(
            symbol="600519",
            name="贵州茅台",
            account_name="账户A",
            note="研究记录",
        )
        self.portfolio_asset_id = self.asset_store.create_or_update_research_asset(
            symbol="00700",
            name="腾讯控股",
            account_name="账户B",
            note="持仓记录",
        )
        self.asset_store.transition_asset_status(
            self.portfolio_asset_id,
            STATUS_PORTFOLIO,
            cost_price=392.5,
            quantity=100,
        )

        self.research_id = self.repository.save_record(
            symbol="600519",
            stock_name="贵州茅台",
            period="1y",
            stock_info={"symbol": "600519", "name": "贵州茅台", "current_price": 1688.0, "industry": "白酒"},
            agents_results={"technical": {"analysis": "趋势回稳"}},
            discussion_result="团队认为估值已经回到可观察区间。",
            final_decision={
                "rating": "买入",
                "confidence_level": 8.6,
                "entry_min": 1660.0,
                "entry_max": 1695.0,
                "take_profit": 1800.0,
                "stop_loss": 1608.0,
                "operation_advice": "分批建仓，等待确认放量。",
            },
            account_name="账户A",
            asset_id=self.research_asset_id,
            analysis_scope="research",
            analysis_source="home_single_analysis",
            asset_status_snapshot="research",
            has_full_report=True,
        )
        self.portfolio_id = self.repository.save_record(
            symbol="00700",
            stock_name="腾讯控股",
            period="6mo",
            stock_info={"symbol": "00700", "name": "腾讯控股", "current_price": 392.5, "industry": "互联网服务"},
            agents_results={"fundamental": {"analysis": "现金流维持稳健"}},
            discussion_result="持仓仓位适中，继续跟踪业绩兑现。",
            final_decision={
                "rating": "持有",
                "confidence_level": 7.4,
                "entry_min": 385.0,
                "entry_max": 395.0,
                "take_profit": 430.0,
                "stop_loss": 368.0,
                "operation_advice": "继续持有，等待下一个财报窗口。",
            },
            account_name="账户B",
            asset_id=self.portfolio_asset_id,
            portfolio_stock_id=self.portfolio_asset_id,
            analysis_scope="portfolio",
            analysis_source="portfolio_batch_analysis",
            asset_status_snapshot="portfolio",
            has_full_report=True,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_list_records_merges_research_and_portfolio_history(self):
        records = self.service.list_records()
        self.assertEqual(len(records), 2)
        self.assertEqual(self.service.count_records(), 2)
        self.assertEqual({record["analysis_scope"] for record in records}, {"research", "portfolio"})
        lookup = {record["id"]: record for record in records}
        self.assertFalse(lookup[self.research_id]["is_in_portfolio"])
        self.assertEqual(lookup[self.research_id]["portfolio_state_label"], "未持仓")
        self.assertTrue(lookup[self.portfolio_id]["is_in_portfolio"])
        self.assertEqual(lookup[self.portfolio_id]["portfolio_state_label"], "在持仓")

    def test_filters_by_scope_portfolio_state_account_and_search_term(self):
        research_only = self.service.list_records(scope="深度分析")
        self.assertEqual([record["id"] for record in research_only], [self.research_id])

        portfolio_only = self.service.list_records(scope="portfolio")
        self.assertEqual([record["id"] for record in portfolio_only], [self.portfolio_id])

        in_portfolio = self.service.list_records(portfolio_state="在持仓")
        self.assertEqual([record["id"] for record in in_portfolio], [self.portfolio_id])

        not_in_portfolio = self.service.list_records(portfolio_state="未持仓")
        self.assertEqual([record["id"] for record in not_in_portfolio], [self.research_id])

        account_b = self.service.list_records(account_name="账户B")
        self.assertEqual([record["id"] for record in account_b], [self.portfolio_id])

        search_result = self.service.list_records(search_term="腾讯")
        self.assertEqual([record["id"] for record in search_result], [self.portfolio_id])

    def test_research_record_tracks_current_portfolio_state(self):
        self.asset_store.transition_asset_status(
            self.research_asset_id,
            STATUS_PORTFOLIO,
            cost_price=1688.0,
            quantity=100,
        )

        record = self.service.get_record(self.research_id)
        self.assertIsNotNone(record)
        self.assertTrue(record["is_in_portfolio"])
        self.assertEqual(record["portfolio_state_label"], "在持仓")
        self.assertEqual(record["portfolio_action_label"], "跳转持仓")

    def test_delete_record_updates_unified_history(self):
        self.assertTrue(self.service.delete_record(self.portfolio_id))
        remaining = self.service.list_records()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["id"], self.research_id)
        self.assertIsNone(self.service.get_record(self.portfolio_id))


if __name__ == "__main__":
    unittest.main()
