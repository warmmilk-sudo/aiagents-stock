import os
import tempfile
import unittest

from analysis_repository import AnalysisRepository
from asset_repository import AssetRepository, STATUS_PORTFOLIO, STATUS_RESEARCH, STATUS_WATCHLIST
from asset_service import AssetService


class AssetServiceFollowupListTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "investment.db")
        self.legacy_db_path = os.path.join(self.temp_dir.name, "legacy.db")
        self.analysis_repository = AnalysisRepository(
            self.db_path,
            legacy_analysis_db_path=self.legacy_db_path,
        )
        self.asset_repository = AssetRepository(self.db_path)
        self.service = AssetService(
            asset_store=self.asset_repository,
            analysis_store=self.analysis_repository,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_list_followup_assets_uses_latest_analysis_context_and_excludes_portfolio(self):
        research_asset_id = self.asset_repository.create_or_update_research_asset(
            symbol="600519",
            name="贵州茅台",
            account_name="账户A",
            note="研究池摘要",
        )
        watchlist_asset_id = self.asset_repository.promote_to_watchlist(
            symbol="00700",
            name="腾讯控股",
            account_name="账户B",
            note="旧关注摘要",
        )
        portfolio_asset_id = self.asset_repository.create_or_update_research_asset(
            symbol="000001",
            name="平安银行",
            account_name="账户C",
            note="持仓摘要",
        )
        self.asset_repository.transition_asset_status(
            portfolio_asset_id,
            STATUS_PORTFOLIO,
            cost_price=12.3,
            quantity=100,
        )

        research_record_id = self.analysis_repository.save_record(
            symbol="600519",
            stock_name="贵州茅台",
            period="1y",
            account_name="账户A",
            asset_id=research_asset_id,
            analysis_scope="research",
            analysis_source="home_single_analysis",
            analysis_date="2026-03-12 09:30:00",
            summary="茅台最新深度分析",
            final_decision={"rating": "买入", "operation_advice": "回踩关注"},
            has_full_report=True,
            asset_status_snapshot="research",
        )
        self.analysis_repository.save_record(
            symbol="00700",
            stock_name="腾讯控股",
            period="6mo",
            account_name="账户B",
            asset_id=watchlist_asset_id,
            portfolio_stock_id=watchlist_asset_id,
            analysis_scope="portfolio",
            analysis_source="portfolio_single_analysis",
            analysis_date="2026-03-12 09:00:00",
            summary="较早的持仓分析",
            final_decision={"rating": "持有", "operation_advice": "先观察"},
            has_full_report=True,
            asset_status_snapshot="watchlist",
        )
        latest_watchlist_record_id = self.analysis_repository.save_record(
            symbol="00700",
            stock_name="腾讯控股",
            period="6mo",
            account_name="账户B",
            analysis_scope="research",
            analysis_source="home_single_analysis",
            analysis_date="2026-03-12 10:30:00",
            summary="更新后的深度分析",
            final_decision={"rating": "买入", "operation_advice": "重新转强后跟踪"},
            has_full_report=True,
            asset_status_snapshot="research",
        )
        self.analysis_repository.save_record(
            symbol="000001",
            stock_name="平安银行",
            period="1y",
            account_name="账户C",
            asset_id=portfolio_asset_id,
            portfolio_stock_id=portfolio_asset_id,
            analysis_scope="portfolio",
            analysis_source="portfolio_single_analysis",
            analysis_date="2026-03-12 11:00:00",
            summary="持仓记录不应出现在看过/关注列表",
            final_decision={"rating": "持有"},
            has_full_report=True,
            asset_status_snapshot="portfolio",
        )

        items = self.service.list_followup_assets(limit=None)

        self.assertEqual([item["status"] for item in items], [STATUS_WATCHLIST, STATUS_RESEARCH])
        self.assertEqual(items[0]["id"], watchlist_asset_id)
        self.assertEqual(items[0]["latest_analysis_id"], latest_watchlist_record_id)
        self.assertEqual(items[0]["latest_analysis_scope"], "research")
        self.assertEqual(items[0]["latest_analysis_rating"], "买入")
        self.assertEqual(items[0]["latest_analysis_summary"], "更新后的深度分析")
        self.assertEqual(items[0]["followup_status_label"], "关注中")

        self.assertEqual(items[1]["id"], research_asset_id)
        self.assertEqual(items[1]["latest_analysis_id"], research_record_id)
        self.assertEqual(items[1]["latest_analysis_scope"], "research")
        self.assertEqual(items[1]["followup_status_label"], "看过")

    def test_list_followup_assets_applies_status_and_search_filters(self):
        research_asset_id = self.asset_repository.create_or_update_research_asset(
            symbol="300750",
            name="宁德时代",
            account_name="账户A",
            note="电池龙头",
        )
        watchlist_asset_id = self.asset_repository.promote_to_watchlist(
            symbol="002594",
            name="比亚迪",
            account_name="账户B",
            note="汽车观察",
        )

        self.analysis_repository.save_record(
            symbol="300750",
            stock_name="宁德时代",
            period="1y",
            account_name="账户A",
            asset_id=research_asset_id,
            analysis_scope="research",
            analysis_source="home_single_analysis",
            analysis_date="2026-03-12 08:00:00",
            summary="电池链景气改善",
            final_decision={"rating": "持有"},
            has_full_report=True,
            asset_status_snapshot="research",
        )
        self.analysis_repository.save_record(
            symbol="002594",
            stock_name="比亚迪",
            period="1y",
            account_name="账户B",
            asset_id=watchlist_asset_id,
            analysis_scope="research",
            analysis_source="home_single_analysis",
            analysis_date="2026-03-12 09:00:00",
            summary="汽车链有反弹预期",
            final_decision={"rating": "买入"},
            has_full_report=True,
            asset_status_snapshot="watchlist",
        )

        research_only = self.service.list_followup_assets(statuses=(STATUS_RESEARCH,), limit=None)
        self.assertEqual([item["id"] for item in research_only], [research_asset_id])

        search_by_name = self.service.list_followup_assets(search_term="比亚迪", limit=None)
        self.assertEqual([item["id"] for item in search_by_name], [watchlist_asset_id])

        search_by_summary = self.service.list_followup_assets(search_term="景气改善", limit=None)
        self.assertEqual([item["id"] for item in search_by_summary], [research_asset_id])


if __name__ == "__main__":
    unittest.main()
