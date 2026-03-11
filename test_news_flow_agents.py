import unittest

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


if __name__ == "__main__":
    unittest.main()
