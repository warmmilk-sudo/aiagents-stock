import importlib
import sys
import types
import unittest

sys.modules.setdefault(
    "openai",
    types.SimpleNamespace(
        OpenAI=type("OpenAI", (), {"__init__": lambda self, *args, **kwargs: None}),
    ),
)

existing_ai_agents = sys.modules.get("ai_agents")
if existing_ai_agents is not None and not getattr(existing_ai_agents, "__file__", "").endswith("ai_agents.py"):
    sys.modules.pop("ai_agents", None)

ai_agents_module = importlib.import_module("ai_agents")
StockAnalysisAgents = ai_agents_module.StockAnalysisAgents


class StockAnalysisAgentsPipelineTests(unittest.TestCase):
    def test_run_multi_agent_analysis_uses_risk_key(self):
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)
        agent.llm_client = None
        agent.technical_analyst_agent = lambda *args, **kwargs: {"analysis": "技术"}
        agent.fundamental_analyst_agent = lambda *args, **kwargs: {"analysis": "基本面"}
        agent.fund_flow_analyst_agent = lambda *args, **kwargs: {"analysis": "资金"}
        agent.risk_management_agent = lambda *args, **kwargs: {"analysis": "风险"}
        agent.market_sentiment_agent = lambda *args, **kwargs: {"analysis": "情绪"}
        agent.news_analyst_agent = lambda *args, **kwargs: {"analysis": "新闻"}

        result = agent.run_multi_agent_analysis(
            stock_info={"symbol": "600519"},
            stock_data=[],
            indicators={},
            enabled_analysts={
                "technical": False,
                "fundamental": False,
                "fund_flow": False,
                "risk": True,
                "sentiment": False,
                "news": False,
            },
        )

        self.assertIn("risk", result)
        self.assertNotIn("risk_management", result)

    def test_run_multi_agent_analysis_keeps_successful_reports_when_one_agent_fails(self):
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)
        agent.llm_client = None
        agent.technical_analyst_agent = lambda *args, **kwargs: {"analysis": "技术"}
        agent.fundamental_analyst_agent = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("llm_empty_response"))
        agent.fund_flow_analyst_agent = lambda *args, **kwargs: {"analysis": "资金"}
        agent.risk_management_agent = lambda *args, **kwargs: {"analysis": "风险"}
        agent.market_sentiment_agent = lambda *args, **kwargs: {"analysis": "情绪"}
        agent.news_analyst_agent = lambda *args, **kwargs: {"analysis": "新闻"}

        result = agent.run_multi_agent_analysis(
            stock_info={"symbol": "600519"},
            stock_data=[],
            indicators={},
            enabled_analysts={
                "technical": True,
                "fundamental": True,
                "fund_flow": True,
                "risk": False,
                "sentiment": False,
                "news": False,
            },
        )

        self.assertIn("technical", result)
        self.assertIn("fund_flow", result)
        self.assertNotIn("fundamental", result)
        self.assertIn("_analysis_errors", result)
        self.assertIn("fundamental", result["_analysis_errors"])

    def test_conduct_team_discussion_rejects_empty_reports(self):
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)
        agent.llm_client = types.SimpleNamespace()

        with self.assertRaisesRegex(RuntimeError, "没有可用于团队讨论的分析师报告"):
            agent.conduct_team_discussion({}, {"symbol": "600519", "name": "贵州茅台"})

    def test_conduct_team_discussion_strips_reasoning_and_keeps_titles(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["prompt"] = messages[1]["content"]
            return "讨论结果"

        agent.llm_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.conduct_team_discussion(
            {
                "technical": {"analysis": "【推理过程】\n内部推理\n\n正式技术分析正文"},
                "risk": {"analysis": "风险提示正文"},
            },
            {"symbol": "600519", "name": "贵州茅台"},
        )

        self.assertEqual(result, "讨论结果")
        self.assertIn("【技术分析师报告】", captured["prompt"])
        self.assertIn("【风险管理师报告】", captured["prompt"])
        self.assertIn("正式技术分析正文", captured["prompt"])
        self.assertNotIn("内部推理", captured["prompt"])
        self.assertIn("关键筹码摘要", captured["prompt"])

    def test_conduct_team_discussion_includes_chip_summary(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["prompt"] = messages[1]["content"]
            return "讨论结果"

        agent.llm_client = types.SimpleNamespace(call_api=fake_call_api)

        agent.conduct_team_discussion(
            {"technical": {"analysis": "技术分析正文"}},
            {"symbol": "600519", "name": "贵州茅台"},
            {
                "chip_data_source": "tushare.cyq_chips/cyq_perf",
                "main_chip_peak_price": 1680,
                "cost_band_70": "1600-1720",
                "profit_ratio_estimate": "61.2%",
            },
        )

        self.assertIn("数据源：tushare.cyq_chips/cyq_perf", captured["prompt"])
        self.assertIn("主筹码峰：1680", captured["prompt"])
        self.assertIn("70%成本区：1600-1720", captured["prompt"])
        self.assertIn("获利盘：61.2%", captured["prompt"])

    def test_conduct_team_discussion_requires_initial_holding_swing_confirmation(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["prompt"] = messages[1]["content"]
            return "讨论结果"

        agent.llm_client = types.SimpleNamespace(call_api=fake_call_api)

        agent.conduct_team_discussion(
            {"technical": {"analysis": "技术分析正文"}},
            {"symbol": "600519", "name": "贵州茅台", "has_position": True},
            strategy_context=None,
            is_initial_holding_analysis=True,
        )

        self.assertIn("这是加入持仓后的首次深度分析", captured["prompt"])
        self.assertIn("微波段 或 标准波段", captured["prompt"])
        self.assertIn("后续若无充分证据，不要频繁更新", captured["prompt"])

    def test_conduct_team_discussion_anchors_existing_holding_swing_baseline(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["prompt"] = messages[1]["content"]
            return "讨论结果"

        agent.llm_client = types.SimpleNamespace(call_api=fake_call_api)

        agent.conduct_team_discussion(
            {"technical": {"analysis": "技术分析正文"}},
            {"symbol": "600519", "name": "贵州茅台", "has_position": True},
            strategy_context={
                "swing_type": "标准波段",
                "holding_period": "5-15个交易日",
                "strategy_style_summary": "动量突破 / 均值回归",
                "intraday_execution_preference": "优先围绕阶段性主升或反弹主段执行",
            },
            is_initial_holding_analysis=False,
        )

        self.assertIn("历史持仓波段基线已确认", captured["prompt"])
        self.assertIn("已确认波段类型：标准波段", captured["prompt"])
        self.assertIn("本轮默认沿用上述波段类型", captured["prompt"])

    def test_risk_management_agent_includes_pe_pb_context(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["system"] = messages[0]["content"]
            captured["prompt"] = messages[1]["content"]
            return "风险分析"

        agent.llm_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.risk_management_agent(
            stock_info={
                "symbol": "600519",
                "name": "贵州茅台",
                "current_price": 1688.0,
                "pe_ratio": 25.6,
                "pb_ratio": 8.4,
                "market_cap": 2100000000000,
                "industry": "白酒",
                "sector": "食品饮料",
            },
            indicators={"rsi": 58.2},
            risk_data=None,
        )

        self.assertEqual(result["analysis"], "风险分析")
        self.assertIn("市盈率(PE)：25.6", captured["prompt"])
        self.assertIn("市净率(PB)：8.4", captured["prompt"])
        self.assertIn("总市值：2100000000000", captured["prompt"])
        self.assertIn("所属行业：白酒", captured["prompt"])
        self.assertIn("所属板块：食品饮料", captured["prompt"])
        self.assertIn("【通用约束】", captured["system"])
        self.assertIn("【任务步骤】", captured["system"])

    def test_risk_management_agent_includes_fundamental_and_liquidity_context(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["system"] = messages[0]["content"]
            captured["prompt"] = messages[1]["content"]
            captured["max_tokens"] = max_tokens
            return "增强风险分析"

        agent.llm_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.risk_management_agent(
            stock_info={
                "symbol": "002050",
                "name": "三花智控",
                "current_price": 22.5,
                "pe_ratio": 28.1,
                "pb_ratio": 5.2,
                "market_cap": 80000000000,
                "industry": "家电零部件",
                "sector": "汽车热管理",
                "business_summary": "制冷空调电器零部件与汽车热管理系统零部件双主业。",
                "volume": 356000,
                "amount": 785000000,
                "turnover_rate": 3.28,
                "volume_ratio": 1.46,
                "order_book": {
                    "summary": "买盘最优 22.49/1260，卖盘最优 22.50/980",
                    "bids": [{"level": "买一", "price": 22.49, "volume": 1260}],
                    "asks": [{"level": "卖一", "price": 22.50, "volume": 980}],
                },
            },
            indicators={"rsi": 58.2},
            risk_data=None,
            financial_data={
                "financial_ratios": {
                    "营业收入同比增长": "18.6%",
                    "净利润同比增长": "24.3%",
                    "销售毛利率": "27.1%",
                    "销售净利率": "12.4%",
                    "资产负债率": "43.2%",
                }
            },
            quarterly_data={
                "data_success": True,
                "income_statement": {
                    "key_metrics": {
                        "latest": {
                            "营业收入": "68.2亿元",
                            "净利润": "8.4亿元",
                            "营业收入同比": "18.6%",
                            "净利润同比": "24.3%",
                        }
                    }
                }
            },
        )

        self.assertEqual(result["analysis"], "增强风险分析")
        self.assertGreaterEqual(captured["max_tokens"], 12000)
        self.assertIn("营收增速：18.6%", captured["prompt"])
        self.assertIn("净利润增速：24.3%", captured["prompt"])
        self.assertIn("毛利率：27.1%", captured["prompt"])
        self.assertIn("业务结构/主营概况：制冷空调电器零部件与汽车热管理系统零部件双主业。", captured["prompt"])
        self.assertIn("换手率：3.28", captured["prompt"])
        self.assertIn("量比：1.46", captured["prompt"])
        self.assertIn("买卖盘深度摘要：买盘最优 22.49/1260，卖盘最优 22.50/980", captured["prompt"])
        self.assertIn("买盘五档：买一 22.49/1260", captured["prompt"])
        self.assertIn("卖盘五档：卖一 22.5/980", captured["prompt"])
        self.assertIn("【通用约束】", captured["system"])
        self.assertIn("【任务步骤】", captured["system"])

    def test_news_analyst_agent_includes_system_level_rules(self):
        captured = {}
        agent = StockAnalysisAgents.__new__(StockAnalysisAgents)

        def fake_call_api(messages, max_tokens=None, tier=None):
            captured["system"] = messages[0]["content"]
            captured["prompt"] = messages[1]["content"]
            return "新闻分析"

        agent.llm_client = types.SimpleNamespace(call_api=fake_call_api)

        result = agent.news_analyst_agent(
            stock_info={"symbol": "600519", "name": "贵州茅台", "sector": "食品饮料", "industry": "白酒"},
            news_data=None,
        )

        self.assertEqual(result["analysis"], "新闻分析")
        self.assertIn("【通用约束】", captured["system"])
        self.assertIn("【任务步骤】", captured["system"])


if __name__ == "__main__":
    unittest.main()
