import tempfile
import unittest

import pandas as pd

from sector_strategy_db import SectorStrategyDatabase


class SectorStrategyDatabaseCacheTests(unittest.TestCase):
    def test_sector_data_uses_name_as_fallback_code_to_avoid_unique_collision(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = SectorStrategyDatabase(f"{temp_dir}/sector_strategy.db")
            sector_df = pd.DataFrame(
                [
                    {"板块名称": "半导体", "涨跌幅": 2.1, "成交额": 1000, "总市值": 10000, "市盈率": 30, "市净率": 3, "最新价": 0, "成交量": 0},
                    {"板块名称": "AI算力", "涨跌幅": 1.7, "成交额": 900, "总市值": 8000, "市盈率": 28, "市净率": 4, "最新价": 0, "成交量": 0},
                ]
            )

            database.save_sector_raw_data("2026-03-17", "industry", sector_df)
            summary = database.build_data_summary(data_date="2026-03-17")

            self.assertEqual(set(summary["sectors"].keys()), {"半导体", "AI算力"})

    def test_market_overview_breadth_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = SectorStrategyDatabase(f"{temp_dir}/sector_strategy.db")
            market_df = pd.DataFrame(
                [
                    {
                        "代码": "000001",
                        "名称": "上证指数",
                        "最新价": 3388.12,
                        "涨跌幅": 1.26,
                        "成交量": 123456789,
                        "成交额": 456789123,
                        "总市值": 0,
                        "市盈率": 0,
                        "市净率": 0,
                    },
                    {
                        "代码": "__MARKET_BREADTH__",
                        "名称": "__MARKET_BREADTH__",
                        "最新价": 5123,
                        "涨跌幅": 61.2,
                        "成交量": 3123,
                        "成交额": 1800,
                        "总市值": 200,
                        "市盈率": 102,
                        "市净率": 4,
                    },
                ]
            )

            database.save_sector_raw_data("2026-03-17", "market_overview", market_df)
            summary = database.build_data_summary(data_date="2026-03-17")

            overview = summary["market_overview"]
            self.assertEqual(overview["sh_index"]["close"], 3388.12)
            self.assertEqual(overview["up_count"], 3123)
            self.assertEqual(overview["down_count"], 1800)
            self.assertEqual(overview["flat_count"], 200)
            self.assertEqual(overview["limit_up"], 102)
            self.assertEqual(overview["limit_down"], 4)


if __name__ == "__main__":
    unittest.main()
