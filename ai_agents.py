import concurrent.futures
import logging
import os
import time
from typing import Any, Dict, Optional

from llm_client import LLMClient
from investment_action_utils import build_holding_strategy_prompt_block
from model_routing import ModelTier
from prompt_registry import build_messages


logger = logging.getLogger(__name__)


class StockAnalysisAgents:
    """股票分析AI智能体集合"""

    _PER_REPORT_LIMIT = max(4000, int(os.getenv("ANALYSIS_DISCUSSION_PER_REPORT_LIMIT", "12000") or 12000))
    _DISCUSSION_INPUT_LIMIT = max(12000, int(os.getenv("ANALYSIS_DISCUSSION_INPUT_LIMIT", "48000") or 48000))

    def __init__(self, model=None, lightweight_model=None, reasoning_model=None):
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.llm_client = LLMClient(
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )

    @staticmethod
    def _strip_reasoning_content(text: Any) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        marker = "【推理过程】"
        marker_index = normalized.find(marker)
        if marker_index < 0:
            return normalized
        content_index = normalized.find("\n\n", marker_index)
        if content_index < 0:
            return normalized[:marker_index].strip()
        without_reasoning = f"{normalized[:marker_index].rstrip()}\n\n{normalized[content_index + 2:].lstrip()}".strip()
        return without_reasoning

    @classmethod
    def _trim_report_for_discussion(cls, title: str, text: Any) -> str:
        body = cls._strip_reasoning_content(text)
        if len(body) > cls._PER_REPORT_LIMIT:
            body = f"{body[:cls._PER_REPORT_LIMIT].rstrip()}\n\n[内容已截断]"
        return f"【{title}】\n{body}".strip()

    @staticmethod
    def _build_chip_summary(indicators: Dict[str, Any]) -> str:
        if not isinstance(indicators, dict):
            return "暂无可用筹码摘要。"

        chip_source = indicators.get("chip_data_source", "N/A")
        chip_trade_date = indicators.get("chip_trade_date", "N/A")
        chip_peak_shape = indicators.get("chip_peak_shape", "N/A")
        main_peak = indicators.get("main_chip_peak_price", "N/A")
        secondary_peak = indicators.get("secondary_chip_peak_price", "N/A")
        average_cost = indicators.get("average_chip_cost", "N/A")
        cost_band_70 = indicators.get("cost_band_70", "N/A")
        cost_band_90 = indicators.get("cost_band_90", "N/A")
        concentration = indicators.get("chip_concentration", "N/A")
        current_position = indicators.get("current_price_position", "N/A")
        pressure_peak = indicators.get("upper_pressure_peak", "N/A")
        support_peak = indicators.get("lower_support_peak", "N/A")
        profit_ratio = indicators.get("profit_ratio_estimate", "N/A")
        trap_ratio = indicators.get("trap_ratio_estimate", "N/A")

        return (
            "【关键筹码摘要】\n"
            f"- 数据源：{chip_source}\n"
            f"- 交易日：{chip_trade_date}\n"
            f"- 形态：{chip_peak_shape}\n"
            f"- 主筹码峰：{main_peak}\n"
            f"- 次筹码峰：{secondary_peak}\n"
            f"- 平均成本：{average_cost}\n"
            f"- 70%成本区：{cost_band_70}\n"
            f"- 90%成本区：{cost_band_90}\n"
            f"- 集中度：{concentration}\n"
            f"- 当前价格位置：{current_position}\n"
            f"- 上方压力峰：{pressure_peak}\n"
            f"- 下方支撑峰：{support_peak}\n"
            f"- 获利盘：{profit_ratio}\n"
            f"- 套牢盘：{trap_ratio}"
        )

    def technical_analyst_agent(self, stock_info: Dict, stock_data: Any, indicators: Dict) -> Dict[str, Any]:
        """技术面分析智能体"""
        print("🔍 技术分析师正在分析中...")
        analysis = self.llm_client.technical_analysis(stock_info, stock_data, indicators)

        return {
            "agent_name": "技术分析师",
            "agent_role": "负责技术指标分析、图表形态识别、趋势判断",
            "analysis": analysis,
            "focus_areas": ["技术指标", "趋势分析", "支撑阻力", "交易信号"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def fundamental_analyst_agent(self, stock_info: Dict, financial_data: Dict = None, quarterly_data: Dict = None) -> Dict[str, Any]:
        """基本面分析智能体"""
        print("📊 基本面分析师正在分析中...")
        
        # 如果有季报数据，显示数据来源
        if quarterly_data and quarterly_data.get('data_success'):
            income_count = quarterly_data.get('income_statement', {}).get('periods', 0) if quarterly_data.get('income_statement') else 0
            balance_count = quarterly_data.get('balance_sheet', {}).get('periods', 0) if quarterly_data.get('balance_sheet') else 0
            cash_flow_count = quarterly_data.get('cash_flow', {}).get('periods', 0) if quarterly_data.get('cash_flow') else 0
            print(f"   ✓ 已获取季报数据：利润表{income_count}期，资产负债表{balance_count}期，现金流量表{cash_flow_count}期")
        else:
            print("   ⚠ 未获取到季报数据，将基于基本财务数据分析")

        analysis = self.llm_client.fundamental_analysis(stock_info, financial_data, quarterly_data)

        return {
            "agent_name": "基本面分析师", 
            "agent_role": "负责公司财务分析、行业研究、估值分析",
            "analysis": analysis,
            "focus_areas": ["财务指标", "行业分析", "公司价值", "成长性", "季报趋势"],
            "quarterly_data": quarterly_data,  # 保存季报数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def fund_flow_analyst_agent(self, stock_info: Dict, indicators: Dict, fund_flow_data: Dict = None) -> Dict[str, Any]:
        """资金面分析智能体"""
        print("💰 资金面分析师正在分析中...")
        
        # 如果有资金流向数据，显示数据来源
        if fund_flow_data and fund_flow_data.get('data_success'):
            print("   ✓ 已获取资金流向数据（tushare数据源）")
        else:
            print("   ⚠ 未获取到资金流向数据，将基于技术指标分析")

        analysis = self.llm_client.fund_flow_analysis(stock_info, indicators, fund_flow_data)

        return {
            "agent_name": "资金面分析师",
            "agent_role": "负责资金流向分析、主力行为研究、市场情绪判断", 
            "analysis": analysis,
            "focus_areas": ["资金流向", "主力动向", "市场情绪", "流动性"],
            "fund_flow_data": fund_flow_data,  # 保存资金流向数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    @staticmethod
    def _build_risk_fundamental_context(
        stock_info: Dict,
        financial_data: Dict | None = None,
        quarterly_data: Dict | None = None,
    ) -> str:
        lines = ["", "【核心基本面风险上下文】"]

        ratios = financial_data.get("financial_ratios", {}) if isinstance(financial_data, dict) else {}
        report_period = ratios.get("报告期")
        if report_period:
            lines.append(f"- 最新财务指标报告期：{report_period}")

        def _ratio_value(*keys):
            for key in keys:
                value = ratios.get(key)
                if value not in (None, "", "N/A"):
                    return value
            return None

        ratio_mapping = (
            ("营收增速", _ratio_value("营业收入同比增长", "营业总收入同比增长", "收入增长")),
            ("净利润增速", _ratio_value("净利润同比增长", "扣非净利润同比增长", "盈利增长")),
            ("毛利率", _ratio_value("销售毛利率", "毛利率")),
            ("净利率", _ratio_value("销售净利率", "净利率")),
            ("ROE", _ratio_value("净资产收益率ROE", "ROE", "净资产收益率")),
            ("ROA", _ratio_value("总资产收益率ROA", "ROA", "总资产净利率")),
            ("资产负债率", _ratio_value("资产负债率")),
            ("流动比率", _ratio_value("流动比率")),
            ("速动比率", _ratio_value("速动比率")),
        )
        for label, value in ratio_mapping:
            if value not in (None, "", "N/A"):
                lines.append(f"- {label}：{value}")

        business_profile = (
            stock_info.get("business_summary")
            or stock_info.get("主营业务")
            or stock_info.get("主营构成")
            or stock_info.get("business_structure")
        )
        if business_profile:
            lines.append(f"- 业务结构/主营概况：{business_profile}")

        if isinstance(quarterly_data, dict) and quarterly_data.get("data_success"):
            income_statement = quarterly_data.get("income_statement") or {}
            key_metrics = income_statement.get("key_metrics") or {}
            latest_metrics = key_metrics.get("latest") if isinstance(key_metrics, dict) else {}
            if isinstance(latest_metrics, dict):
                if latest_metrics.get("营业收入") not in (None, "", "N/A"):
                    lines.append(f"- 最新已披露财报营业收入：{latest_metrics.get('营业收入')}")
                if latest_metrics.get("净利润") not in (None, "", "N/A"):
                    lines.append(f"- 最新已披露财报净利润：{latest_metrics.get('净利润')}")
                if latest_metrics.get("营业收入同比") not in (None, "", "N/A"):
                    lines.append(f"- 最新已披露财报营收同比：{latest_metrics.get('营业收入同比')}")
                if latest_metrics.get("净利润同比") not in (None, "", "N/A"):
                    lines.append(f"- 最新已披露财报净利润同比：{latest_metrics.get('净利润同比')}")

        if len(lines) == 2:
            lines.append("- 暂无额外基本面风险上下文。")
        return "\n".join(lines)

    @staticmethod
    def _build_risk_liquidity_context(stock_info: Dict, indicators: Dict) -> str:
        lines = ["", "【流动性与盘口上下文】"]

        for label, key in (
            ("实时成交量(手)", "volume"),
            ("实时成交额(元)", "amount"),
            ("换手率", "turnover_rate"),
            ("量比", "volume_ratio"),
            ("近5日均量比", "volume_ratio"),
            ("当日最高价", "high"),
            ("当日最低价", "low"),
            ("当日开盘价", "open"),
            ("昨收价", "pre_close"),
        ):
            value = stock_info.get(key)
            if value not in (None, "", "N/A"):
                lines.append(f"- {label}：{value}")

        order_book = stock_info.get("order_book")
        if isinstance(order_book, dict):
            summary = order_book.get("summary")
            if summary:
                lines.append(f"- 买卖盘深度摘要：{summary}")
            bids = order_book.get("bids") or []
            asks = order_book.get("asks") or []
            if bids:
                lines.append("- 买盘五档：" + "；".join(f"{item.get('level')} {item.get('price')}/{item.get('volume')}" for item in bids))
            if asks:
                lines.append("- 卖盘五档：" + "；".join(f"{item.get('level')} {item.get('price')}/{item.get('volume')}" for item in asks))

        rsi = indicators.get("rsi")
        if rsi not in (None, "", "N/A"):
            lines.append(f"- RSI：{rsi}")

        if len(lines) == 2:
            lines.append("- 暂无额外流动性或盘口数据。")
        return "\n".join(lines)

    def risk_management_agent(
        self,
        stock_info: Dict,
        indicators: Dict,
        risk_data: Dict = None,
        financial_data: Dict = None,
        quarterly_data: Dict = None,
    ) -> Dict[str, Any]:
        """风险管理智能体（增强版）"""
        print("⚠️ 风险管理师正在评估中...")
        
        # 如果有风险数据，显示数据来源
        if risk_data and risk_data.get('data_success'):
            print("   ✓ 已获取问财风险数据（限售解禁、大股东减持、重要事件）")
        else:
            print("   ⚠ 未获取到风险数据，将基于基本信息分析")

        # 构建风险数据文本
        risk_data_text = ""
        if risk_data and risk_data.get('data_success'):
            # 使用格式化的风险数据
            from risk_data_fetcher import RiskDataFetcher
            fetcher = RiskDataFetcher()
            risk_data_text = f"""

【实际风险数据】（来自问财）
{fetcher.format_risk_data_for_ai(risk_data)}

以上是通过问财（pywencai）获取的实际风险数据，请重点关注这些数据进行深度风险分析。
"""
        risk_fundamental_text = self._build_risk_fundamental_context(stock_info, financial_data, quarterly_data)
        risk_liquidity_text = self._build_risk_liquidity_context(stock_info, indicators)
        messages = build_messages(
            "stock_analysis/risk.system.txt",
            "stock_analysis/risk.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            current_price=stock_info.get("current_price", "N/A"),
            pe_ratio=stock_info.get("pe_ratio", "N/A"),
            pb_ratio=stock_info.get("pb_ratio", "N/A"),
            market_cap=stock_info.get("market_cap", "N/A"),
            industry=stock_info.get("industry", "N/A"),
            sector=stock_info.get("sector", stock_info.get("industry", "N/A")),
            beta=stock_info.get("beta", "N/A"),
            high_52_week=stock_info.get("52_week_high", "N/A"),
            low_52_week=stock_info.get("52_week_low", "N/A"),
            rsi=indicators.get("rsi", "N/A"),
            risk_data_text=risk_data_text,
            risk_fundamental_text=risk_fundamental_text,
            risk_liquidity_text=risk_liquidity_text,
        )
        analysis = self.llm_client.call_api(
            messages,
            max_tokens=max(12000, int(os.getenv("RISK_ANALYSIS_MAX_TOKENS", "16000") or 16000)),
            tier=ModelTier.REASONING,
        )

        return {
            "agent_name": "风险管理师",
            "agent_role": "负责风险识别、风险评估、风险控制策略制定",
            "analysis": analysis,
            "focus_areas": ["限售解禁风险", "股东减持风险", "重要事件风险", "风险识别", "风险量化", "风险控制", "资产配置"],
            "risk_data": risk_data,  # 保存风险数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def market_sentiment_agent(self, stock_info: Dict, sentiment_data: Dict = None) -> Dict[str, Any]:
        """市场情绪分析智能体"""
        print("📈 市场情绪分析师正在分析中...")
        
        # 如果有市场情绪数据，显示数据来源
        if sentiment_data and sentiment_data.get('data_success'):
            print("   ✓ 已获取市场情绪数据（ARBR、换手率、涨跌停等）")
        else:
            print("   ⚠ 未获取到详细情绪数据，将基于基本信息分析")

        # 构建带有市场情绪数据的prompt
        sentiment_data_text = ""
        if sentiment_data and sentiment_data.get('data_success'):
            # 使用格式化的市场情绪数据
            from market_sentiment_data import MarketSentimentDataFetcher
            fetcher = MarketSentimentDataFetcher()
            sentiment_data_text = f"""

【市场情绪实际数据】
{fetcher.format_sentiment_data_for_ai(sentiment_data)}

以上是基于Tushare获取并计算的实际市场情绪数据，请重点基于这些数据进行分析。
"""
        
        messages = build_messages(
            "stock_analysis/market_sentiment.system.txt",
            "stock_analysis/market_sentiment.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            sector=stock_info.get("sector", "N/A"),
            industry=stock_info.get("industry", "N/A"),
            sentiment_data_text=sentiment_data_text,
        )

        analysis = self.llm_client.call_api(
            messages,
            max_tokens=4000,
            tier=ModelTier.LIGHTWEIGHT,
        )

        return {
            "agent_name": "市场情绪分析师",
            "agent_role": "负责市场情绪研究、投资者心理分析、热点追踪",
            "analysis": analysis,
            "focus_areas": ["ARBR指标", "市场情绪", "投资者心理", "资金活跃度", "恐慌贪婪指数"],
            "sentiment_data": sentiment_data,  # 保存市场情绪数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def news_analyst_agent(self, stock_info: Dict, news_data: Dict = None) -> Dict[str, Any]:
        """新闻分析智能体"""
        print("📰 新闻分析师正在分析中...")
        
        # 如果有新闻数据，显示数据来源
        if news_data and news_data.get('data_success'):
            news_count = news_data.get('news_data', {}).get('count', 0) if news_data.get('news_data') else 0
            announcement_count = news_data.get('announcement_data', {}).get('count', 0) if news_data.get('announcement_data') else 0
            supplemental_count = news_data.get('supplemental_news_data', {}).get('count', 0) if news_data.get('supplemental_news_data') else 0
            source = news_data.get('source', 'unknown')
            print(f"   ✓ 已从 {source} 获取 新闻{news_count}条 / 公告{announcement_count}条 / 补充{supplemental_count}条")
        else:
            print("   ⚠ 未获取到新闻数据，将基于基本信息分析")

        # 构建带有新闻数据的prompt
        news_text = ""
        if news_data and news_data.get('data_success'):
            # 使用格式化的新闻数据
            from stock_research_news_data import StockResearchNewsDataFetcher
            fetcher = StockResearchNewsDataFetcher()
            news_text = f"""

【最新新闻公告数据】
{fetcher.format_news_for_ai(news_data)}

以上是通过巨潮资讯、pywencai 和 RSSHub 聚合得到的实际新闻公告数据，请重点基于这些数据进行分析。
"""
        
        messages = build_messages(
            "stock_analysis/news.system.txt",
            "stock_analysis/news.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            sector=stock_info.get("sector", "N/A"),
            industry=stock_info.get("industry", "N/A"),
            news_text=news_text,
        )
        analysis = self.llm_client.call_api(
            messages,
            max_tokens=4000,
            tier=ModelTier.LIGHTWEIGHT,
        )

        return {
            "agent_name": "新闻分析师",
            "agent_role": "负责新闻事件分析、舆情研究、重大事件影响评估",
            "analysis": analysis,
            "focus_areas": ["新闻解读", "舆情分析", "事件影响", "市场反应", "投资机会"],
            "news_data": news_data,  # 保存新闻数据以供后续使用
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def run_multi_agent_analysis(self, stock_info: Dict, stock_data: Any, indicators: Dict, 
                                 financial_data: Dict = None, fund_flow_data: Dict = None, 
                                 sentiment_data: Dict = None, news_data: Dict = None,
                                 quarterly_data: Dict = None, risk_data: Dict = None,
                                 enabled_analysts: Dict = None) -> Dict[str, Any]:
        """运行多智能体分析
        
        Args:
            enabled_analysts: 字典，指定哪些分析师参与分析
                例如: {'technical': True, 'fundamental': True, ...}
                如果为None，则运行所有分析师
        """
        # 如果未指定，默认所有分析师都参与
        if enabled_analysts is None:
            enabled_analysts = {
                'technical': True,
                'fundamental': True,
                'fund_flow': True,
                'risk': True,
                'sentiment': True,
                'news': True
            }
        
        print("🚀 启动多智能体股票分析系统...")
        print("=" * 50)
        
        # 显示参与分析的分析师
        active_analysts = [name for name, enabled in enabled_analysts.items() if enabled]
        print(f"📋 参与分析的分析师: {', '.join(active_analysts)}")
        print("=" * 50)
        
        tasks = []
        if enabled_analysts.get('technical', True):
            tasks.append(("technical", lambda: self.technical_analyst_agent(stock_info, stock_data, indicators)))
        if enabled_analysts.get('fundamental', True):
            tasks.append(("fundamental", lambda: self.fundamental_analyst_agent(stock_info, financial_data, quarterly_data)))
        if enabled_analysts.get('fund_flow', True):
            tasks.append(("fund_flow", lambda: self.fund_flow_analyst_agent(stock_info, indicators, fund_flow_data)))
        if enabled_analysts.get('risk', True):
            tasks.append(("risk", lambda: self.risk_management_agent(stock_info, indicators, risk_data, financial_data, quarterly_data)))
        if enabled_analysts.get('sentiment', False):
            tasks.append(("market_sentiment", lambda: self.market_sentiment_agent(stock_info, sentiment_data)))
        if enabled_analysts.get('news', False):
            tasks.append(("news", lambda: self.news_analyst_agent(stock_info, news_data)))

        agents_results = {}
        if tasks:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
                future_pairs = [(key, executor.submit(task)) for key, task in tasks]
                analysis_errors = {}
                for key, future in future_pairs:
                    try:
                        agents_results[key] = future.result()
                    except Exception as exc:
                        analysis_errors[key] = str(exc)
                        logger.warning("[%s] 分析师任务失败: %s", key, exc, exc_info=True)
                if analysis_errors:
                    agents_results["_analysis_errors"] = analysis_errors

        print("✅ 所有已选择的分析师完成分析")
        print("=" * 50)
        
        return agents_results
    
    def conduct_team_discussion(
        self,
        agents_results: Dict[str, Any],
        stock_info: Dict,
        indicators: Dict = None,
        memory_context: Dict = None,
        strategy_context: Optional[Dict[str, Any]] = None,
        is_initial_holding_analysis: bool = False,
    ) -> str:
        """进行团队讨论"""
        print("🤝 分析团队正在进行综合讨论...")

        # 收集参与分析的分析师名单和报告
        participants = []
        reports = []

        if "technical" in agents_results:
            participants.append("技术分析师")
            reports.append(self._trim_report_for_discussion("技术分析师报告", agents_results['technical'].get('analysis', '')))

        if "fundamental" in agents_results:
            participants.append("基本面分析师")
            reports.append(self._trim_report_for_discussion("基本面分析师报告", agents_results['fundamental'].get('analysis', '')))

        if "fund_flow" in agents_results:
            participants.append("资金面分析师")
            reports.append(self._trim_report_for_discussion("资金面分析师报告", agents_results['fund_flow'].get('analysis', '')))

        if "risk" in agents_results:
            participants.append("风险管理师")
            reports.append(self._trim_report_for_discussion("风险管理师报告", agents_results['risk'].get('analysis', '')))

        if "market_sentiment" in agents_results:
            participants.append("市场情绪分析师")
            reports.append(self._trim_report_for_discussion("市场情绪分析师报告", agents_results['market_sentiment'].get('analysis', '')))

        if "news" in agents_results:
            participants.append("新闻分析师")
            reports.append(self._trim_report_for_discussion("新闻分析师报告", agents_results['news'].get('analysis', '')))

        if not reports:
            failed_agents = agents_results.get("_analysis_errors")
            if failed_agents:
                failed_text = "；".join(f"{key}: {value}" for key, value in failed_agents.items())
                raise RuntimeError(f"没有可用于团队讨论的分析师报告，失败原因：{failed_text}")
            raise RuntimeError("没有可用于团队讨论的分析师报告")

        # Inject memory context before all reports if available
        memory_block = ""
        if memory_context:
            from agent_memory_service import agent_memory_service
            memory_block = agent_memory_service.format_memory_prompt_block(memory_context)

        # 组合所有报告
        all_reports = "\n\n".join(reports)
        if memory_block:
            memory_limit = min(4000, max(1200, self._DISCUSSION_INPUT_LIMIT // 5))
            if len(memory_block) > memory_limit:
                memory_block = f"{memory_block[:memory_limit].rstrip()}\n\n[历史记忆已截断]"
            remaining_reports_limit = max(0, self._DISCUSSION_INPUT_LIMIT - len(memory_block) - 2)
            if len(all_reports) > remaining_reports_limit:
                all_reports = f"{all_reports[:remaining_reports_limit].rstrip()}\n\n[讨论材料已截断]"
            all_reports = f"{memory_block}\n\n{all_reports}"
        elif len(all_reports) > self._DISCUSSION_INPUT_LIMIT:
            all_reports = f"{all_reports[:self._DISCUSSION_INPUT_LIMIT].rstrip()}\n\n[讨论材料已截断]"
        
        messages = build_messages(
            "stock_analysis/team_discussion.system.txt",
            "stock_analysis/team_discussion.user.txt",
            participants=", ".join(participants),
            stock_name=stock_info.get("name", "N/A"),
            stock_symbol=stock_info.get("symbol", "N/A"),
            position_status="已持仓" if stock_info.get("has_position") else "未持仓",
            chip_summary=self._build_chip_summary(indicators or {}),
            holding_strategy_prompt_block=build_holding_strategy_prompt_block(
                has_position=bool(stock_info.get("has_position")),
                strategy_context=strategy_context,
                is_initial_holding_analysis=is_initial_holding_analysis,
            ),
            all_reports=all_reports,
        )
        
        discussion_result = self.llm_client.call_api(
            messages,
            max_tokens=max(8000, int(os.getenv("TEAM_DISCUSSION_MAX_TOKENS", "12000") or 12000)),
            tier=ModelTier.REASONING,
        )

        print("✅ 团队讨论完成")
        return discussion_result

    def make_final_decision(
        self,
        discussion_result: str,
        stock_info: Dict,
        indicators: Dict,
        strategy_context: Optional[Dict[str, Any]] = None,
        is_initial_holding_analysis: bool = False,
        memory_context: Dict = None,
    ) -> Dict[str, Any]:
        """制定最终投资决策"""
        print("📋 正在制定最终投资决策...")
        memory_prompt_block = ""
        if memory_context:
            from agent_memory_service import agent_memory_service
            memory_prompt_block = agent_memory_service.format_memory_prompt_block(memory_context)
            if len(memory_prompt_block) > 3000:
                memory_prompt_block = f"{memory_prompt_block[:3000].rstrip()}\n\n[历史记忆已截断]"
        decision = self.llm_client.final_decision(
            discussion_result,
            stock_info,
            indicators,
            strategy_context=strategy_context,
            is_initial_holding_analysis=is_initial_holding_analysis,
            memory_prompt_block=memory_prompt_block,
        )

        print("✅ 最终投资决策完成")
        return decision
