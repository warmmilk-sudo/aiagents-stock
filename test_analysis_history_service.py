import os
import sqlite3
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
        self.assertEqual(lookup[self.research_id]["decision_label"], "买入")
        self.assertTrue(lookup[self.portfolio_id]["is_in_portfolio"])
        self.assertEqual(lookup[self.portfolio_id]["portfolio_state_label"], "在持仓")
        self.assertEqual(lookup[self.portfolio_id]["decision_label"], "持有")

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

    def test_legacy_default_account_record_falls_back_to_current_portfolio_asset(self):
        new_account_asset_id = self.asset_store.create_or_update_research_asset(
            symbol="300750",
            name="宁德时代",
            account_name="新增账户",
            note="后来迁移到新账户",
        )
        self.asset_store.transition_asset_status(
            new_account_asset_id,
            STATUS_PORTFOLIO,
            cost_price=205.0,
            quantity=20,
        )
        record_id = self.repository.save_record(
            symbol="300750",
            stock_name="宁德时代",
            period="1y",
            summary="历史记录没有带正确账户。",
            final_decision={"rating": "持有", "confidence_level": 7.1},
            account_name="默认账户",
            analysis_scope="portfolio",
            analysis_source="legacy_portfolio_analysis",
            has_full_report=True,
        )

        record = self.service.get_record(record_id)
        self.assertIsNotNone(record)
        self.assertTrue(record["is_in_portfolio"])
        self.assertEqual(record["portfolio_state_label"], "在持仓")
        self.assertEqual(record["linked_asset_id"], new_account_asset_id)
        self.assertEqual(record["linked_asset_account_name"], "新增账户")

    def test_delete_record_updates_unified_history(self):
        self.assertTrue(self.service.delete_record(self.portfolio_id))
        remaining = self.service.list_records()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["id"], self.research_id)
        self.assertIsNone(self.service.get_record(self.portfolio_id))

    def test_normalizes_legacy_final_decision_threshold_strings(self):
        legacy_id = self.repository.save_record(
            symbol="300308",
            stock_name="中际旭创",
            period="6mo",
            summary="旧格式阈值字段兼容校验",
            final_decision={
                "rating": "买入",
                "entry_range": "115-118元",
                "take_profit": "125元（第一止盈）",
                "stop_loss": "108元（跌破后止损）",
                "operation_advice": "等待回踩后分批介入。",
            },
            account_name="账户A",
            analysis_scope="research",
            analysis_source="legacy_home_analysis",
            has_full_report=True,
        )

        record = self.service.get_record(legacy_id)
        self.assertIsNotNone(record)
        self.assertEqual(record["entry_min"], 115.0)
        self.assertEqual(record["entry_max"], 118.0)
        self.assertEqual(record["take_profit"], 125.0)
        self.assertEqual(record["stop_loss"], 108.0)
        self.assertEqual(record["final_decision"]["entry_min"], 115.0)
        self.assertEqual(record["final_decision"]["entry_max"], 118.0)
        self.assertEqual(record["final_decision"]["take_profit"], "125元（第一止盈）")
        self.assertEqual(record["final_decision"]["stop_loss"], "108元（跌破后止损）")

    def test_backfills_legacy_full_report_flag_for_existing_records(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO analysis_records (
                symbol, stock_name, account_name, analysis_scope, analysis_source,
                analysis_date, period, summary, stock_info_json, agents_results_json,
                discussion_result, final_decision_json, has_full_report, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "000001",
                "平安银行",
                "账户A",
                "research",
                "legacy_home_analysis",
                "2026-03-09 10:00:00",
                "1y",
                "旧记录未回填完整报告标记",
                '{"symbol":"000001","name":"平安银行"}',
                '{"technical":{"analysis":"趋势改善"}}',
                "团队讨论认为可以继续跟踪。",
                '{"rating":"买入","operation_advice":"分批低吸。"}',
                0,
                "2026-03-09T10:00:00",
            ),
        )
        legacy_id = int(cursor.lastrowid)
        conn.commit()
        conn.close()

        reloaded_repository = AnalysisRepository(
            self.db_path,
            legacy_analysis_db_path=self.legacy_db_path,
        )
        reloaded_service = AnalysisHistoryService(repository=reloaded_repository, asset_store=self.asset_store)

        record = reloaded_repository.get_record(legacy_id)
        self.assertIsNotNone(record)
        self.assertTrue(record["has_full_report"])
        self.assertIn(legacy_id, [item["id"] for item in reloaded_service.list_records()])

    def test_decision_label_falls_back_to_rating_and_summary(self):
        summary_only_id = self.repository.save_record(
            symbol="300475",
            stock_name="香农芯创",
            period="1y",
            summary="评级: 卖出；等待反弹减仓。",
            final_decision={},
            account_name="账户A",
            analysis_scope="research",
            analysis_source="home_single_analysis",
            has_full_report=True,
        )
        rating_only_id = self.repository.save_record(
            symbol="000001",
            stock_name="平安银行",
            period="1y",
            summary="等待后续确认。",
            final_decision={},
            rating="买入",
            account_name="账户A",
            analysis_scope="research",
            analysis_source="home_single_analysis",
            has_full_report=True,
        )

        records = {record["id"]: record for record in self.service.list_records()}
        self.assertEqual(records[summary_only_id]["decision_label"], "卖出")
        self.assertEqual(records[rating_only_id]["decision_label"], "买入")

    def test_legacy_position_labels_are_normalized_to_new_display_labels(self):
        record_id = self.repository.save_record(
            symbol="002594",
            stock_name="比亚迪",
            period="1y",
            summary="评级: 增持；等待放量确认。",
            final_decision={"rating": "减持", "confidence_level": 6.8},
            account_name="账户A",
            analysis_scope="portfolio",
            analysis_source="portfolio_single_analysis",
            has_full_report=True,
        )

        record = self.service.get_record(record_id)
        self.assertIsNotNone(record)
        self.assertEqual(record["decision_label"], "减仓")


if __name__ == "__main__":
    unittest.main()
