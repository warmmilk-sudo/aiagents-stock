import unittest
import types

from news_flow_agents import NewsFlowAgents


class NewsFlowAgentsJsonParseTests(unittest.TestCase):
    def setUp(self):
        self.agent = NewsFlowAgents.__new__(NewsFlowAgents)

    def test_parse_json_response_ignores_trailing_text(self):
        response = '{"risk_level":"高","risk_score":75}\n补充说明：次日关注开盘。'

        result = self.agent._parse_json_response(response)

        self.assertEqual(result, {"risk_level": "高", "risk_score": 75})

    def test_parse_json_response_reads_first_object_from_multiple_blocks(self):
        response = (
            '```json\n'
            '{"advice":"观望","confidence":68}\n'
            '```\n'
            '{"extra":"ignored"}'
        )

        result = self.agent._parse_json_response(response)

        self.assertEqual(result, {"advice": "观望", "confidence": 68})

    def test_parse_json_response_supports_json_array_wrappers(self):
        response = '[{"sector_name":"AI算力","heat_score":88},{"sector_name":"机器人","heat_score":76}]'

        result = self.agent._parse_json_response(response)

        self.assertEqual(result, {"sector_name": "AI算力", "heat_score": 88})

    def test_sector_impact_agent_uses_external_prompt_templates(self):
        captured = {}
        self.agent.deepseek_client = types.SimpleNamespace()
        self.agent.is_available = lambda: True

        def fake_call_api(messages, temperature=None, max_tokens=None, tier=None):
            captured["messages"] = messages
            captured["tier"] = tier
            return '{"benefited_sectors":[{"name":"AI算力"}],"damaged_sectors":[],"hot_themes":[]}'

        self.agent.deepseek_client.call_api = fake_call_api

        result = self.agent.sector_impact_agent(
            hot_topics=[{"topic": "AI算力", "heat": 88, "cross_platform": 3}],
            stock_news=[{"platform_name": "财联社", "title": "算力链继续走强"}],
            flow_data={"total_score": 720, "level": "高热", "social_score": 88, "finance_score": 76},
        )

        self.assertTrue(result["success"])
        self.assertEqual(captured["tier"].value, "reasoning")
        self.assertIn("A股市场分析师", captured["messages"][0]["content"])
        self.assertIn("=== 全网热门话题TOP20 ===", captured["messages"][1]["content"])
        self.assertIn("流量得分: 720/1000", captured["messages"][1]["content"])

    def test_analyze_sector_deep_uses_external_prompt_templates(self):
        captured = {}
        self.agent.deepseek_client = types.SimpleNamespace()
        self.agent.is_available = lambda: True

        def fake_call_api(messages, temperature=None, max_tokens=None, tier=None):
            captured["messages"] = messages
            captured["tier"] = tier
            return '{"sector_name":"机器人","heat_score":91}'

        self.agent.deepseek_client.call_api = fake_call_api

        result = self.agent.analyze_sector_deep(
            "机器人",
            [{"platform_name": "证券时报", "title": "机器人板块获资金关注"}],
            [{"topic": "人形机器人", "heat": 77}],
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["sector_name"], "机器人")
        self.assertEqual(captured["tier"].value, "reasoning")
        self.assertIn("机器人板块专业分析师", captured["messages"][0]["content"])
        self.assertIn("请对以下与机器人相关的新闻进行深度分析", captured["messages"][1]["content"])


if __name__ == "__main__":
    unittest.main()
