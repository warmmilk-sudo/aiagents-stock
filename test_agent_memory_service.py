import unittest

from agent_memory_service import AgentMemoryService


class AgentMemoryServiceProfileTests(unittest.TestCase):
    def test_fallback_profile_preserves_existing_business_opening(self):
        existing_profile = (
            "东山精密（002384）是国内PCB赛道核心厂商，通过收购索尔思光电切入AI光模块高景气赛道，"
            "业务覆盖消费电子、汽车电子配套PCB及数通光模块领域。当前其旧交易画像待更新。"
        )
        profile = AgentMemoryService._fallback_long_term_profile(
            stock_code="002384",
            stock_name="东山精密",
            existing_profile=existing_profile,
            facts=[
                {
                    "fact_content": "东山精密 执行计划: 仅适合1-2周微波段博弈，持仓需控制在总仓位10%以内",
                    "importance_score": 88,
                    "category": "execution",
                    "timestamp": "2026-05-12 14:50:04",
                },
                {
                    "fact_content": "东山精密 关键筹码: 上方188.7元主压力峰堆积44.5%套牢盘，下方180元为核心支撑位",
                    "importance_score": 86,
                    "category": "technical",
                    "timestamp": "2026-05-12 14:50:04",
                },
                {
                    "fact_content": "东山精密 风控纪律: 触发收盘价跌破180元、单日跌幅超5%等任一风控条件需立刻清仓",
                    "importance_score": 92,
                    "category": "risk",
                    "timestamp": "2026-05-12 14:50:04",
                },
            ],
        )

        self.assertTrue(profile.startswith("东山精密（002384）是国内PCB赛道核心厂商"))
        self.assertIn("188.7元主压力峰", profile)
        self.assertIn("主要风险包括", profile)
        self.assertNotIn("被持续跟踪的研究标的", profile)

    def test_fallback_profile_does_not_emit_generic_research_target_marker(self):
        profile = AgentMemoryService._fallback_long_term_profile(
            stock_code="603186",
            stock_name="华正新材",
            existing_profile="华正新材（603186）是A股市场中被持续跟踪的研究标的，收盘价有效跌破88.8元主筹码峰支撑。",
            facts=[
                {
                    "fact_content": "华正新材 关键筹码: 收盘价有效跌破88.8元主筹码峰支撑",
                    "importance_score": 86,
                    "category": "technical",
                    "timestamp": "2026-05-12 14:50:04",
                },
                {
                    "fact_content": "华正新材 风控纪律: 股价跌破84.00元风控止损线",
                    "importance_score": 90,
                    "category": "risk",
                    "timestamp": "2026-05-12 14:50:04",
                },
            ],
        )

        self.assertTrue(profile.startswith("华正新材（603186）的长期底色"))
        self.assertIn("88.8元主筹码峰支撑", profile)
        self.assertNotIn("被持续跟踪的研究标的", profile)


if __name__ == "__main__":
    unittest.main()
