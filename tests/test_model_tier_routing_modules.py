"""
关键业务模块的模型分层路由测试
"""

import sys
from types import SimpleNamespace

import pandas as pd

from ai_agents import StockAnalysisAgents
from ai_model_router import ModelTier
from news_flow_agents import NewsFlowAgents


class RecordingClient:
    def __init__(self):
        self.calls = []
        self.responses = []

    def queue_response(self, text):
        self.responses.append(text)

    def call_api(self, _messages, **kwargs):
        self.calls.append(kwargs.get("model_tier"))
        if self.responses:
            return self.responses.pop(0)
        return "{}"


def test_ai_agents_discussion_uses_reasoning_tier():
    agents = StockAnalysisAgents(model="test-model")
    recorder = RecordingClient()
    recorder.queue_response("discussion-result")
    agents.ai_model_client = recorder

    result = agents.conduct_team_discussion(
        agents_results={"technical": {"analysis": "ok"}},
        stock_info={"name": "测试股", "symbol": "600000"}
    )

    assert result == "discussion-result"
    assert recorder.calls[-1] == ModelTier.REASONING


def test_main_force_analyzer_routes_long_context_and_reasoning():
    if "pywencai" not in sys.modules:
        sys.modules["pywencai"] = SimpleNamespace(get=lambda **kwargs: None)
    if "ta" not in sys.modules:
        sys.modules["ta"] = SimpleNamespace()

    from main_force_analysis import MainForceAnalyzer

    analyzer = MainForceAnalyzer(model="test-model")
    recorder = RecordingClient()
    analyzer.ai_model_client = recorder

    df = pd.DataFrame(
        [
            {
                "股票代码": "600000",
                "股票简称": "浦发银行",
                "主力净流入": 1_000_000,
                "涨跌幅": 1.2,
                "所属同花顺行业": "银行"
            }
        ]
    )

    recorder.queue_response("fund-flow-analysis")
    _ = analyzer._fund_flow_overall_analysis(df, "summary")
    assert recorder.calls[-1] == ModelTier.LONG_CONTEXT

    recorder.queue_response('{"recommendations": []}')
    recommendations = analyzer._select_best_stocks(
        df=df,
        fund_analysis="fund",
        industry_analysis="industry",
        fundamental_analysis="fundamental",
        final_n=1
    )
    assert isinstance(recommendations, list)
    assert recorder.calls[-1] == ModelTier.REASONING


def test_news_flow_agents_routes_expected_tiers():
    agents = NewsFlowAgents(model="test-model")
    recorder = RecordingClient()
    agents.ai_model_client = recorder

    recorder.queue_response(
        '{"hot_themes":[],"benefited_sectors":[],"damaged_sectors":[],'
        '"opportunity_assessment":"","trading_suggestion":"","key_points":[]}'
    )
    sector_result = agents.sector_impact_agent(
        hot_topics=[{"topic": "AI热度", "heat": 10, "cross_platform": 2}],
        stock_news=[{"platform_name": "测试平台", "title": "测试新闻"}],
        flow_data={"total_score": 10, "level": "低", "social_score": 3, "finance_score": 2}
    )
    assert sector_result["success"] is True
    assert recorder.calls[-1] == ModelTier.LIGHTWEIGHT

    recorder.queue_response(
        '{"advice":"观望","confidence":60,"summary":"","action_plan":[],'
        '"position_suggestion":"","timing":"","key_message":""}'
    )
    advice_result = agents.investment_advisor_agent(
        sector_analysis={"benefited_sectors": [{"name": "AI人工智能"}], "opportunity_assessment": "中性"},
        stock_recommend={"recommended_stocks": [{"name": "测试股", "code": "600000"}]},
        risk_assess={"risk_level": "中等", "risk_score": 50, "risk_factors": ["波动"]},
        flow_data={"total_score": 100, "level": "一般"},
        sentiment_data={"sentiment_index": 50, "sentiment_class": "中性", "flow_stage": "平稳"}
    )
    assert advice_result["success"] is True
    assert recorder.calls[-1] == ModelTier.REASONING

    recorder.queue_response(
        '{"sector_name":"AI人工智能","heat_level":"高","heat_score":80,'
        '"drivers":[],"short_term_outlook":"看涨","outlook_reason":"",'
        '"leader_stocks":[],"investment_advice":"","risk_warning":"",'
        '"key_indicators":{"关注度":"高"}}'
    )
    deep_result = agents.analyze_sector_deep(
        sector_name="AI人工智能",
        related_news=[{"platform_name": "测试平台", "title": "AI新闻"}],
        hot_topics=[{"topic": "AI", "heat": 20}]
    )
    assert deep_result["success"] is True
    assert recorder.calls[-1] == ModelTier.LONG_CONTEXT
