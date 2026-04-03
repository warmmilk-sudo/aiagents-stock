import json
from typing import Any, Dict, List, Optional

import openai

import config
from model_routing import ModelTier, describe_model_selection, resolve_model_name
from prompt_registry import build_messages


class DeepSeekClient:
    """DeepSeek API客户端"""

    def __init__(self, model=None, lightweight_model=None, reasoning_model=None):
        # 兼容旧接口：model 表示强制所有任务统一使用一个模型
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.client = openai.OpenAI(
            api_key=config.DEEPSEEK_API_KEY,
            base_url=config.DEEPSEEK_BASE_URL
        )
        self.model_selection = describe_model_selection(
            forced_model=self.model,
            lightweight_model=self.lightweight_model,
            reasoning_model=self.reasoning_model,
        )

    @staticmethod
    def _extract_json_object(text: str) -> Dict[str, Any]:
        decoder = json.JSONDecoder()
        for index, char in enumerate(text or ""):
            if char != "{":
                continue
            try:
                payload, _ = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                return payload
        raise RuntimeError("final_decision_invalid_json")

    @staticmethod
    def _build_technical_context(stock_data: Any) -> str:
        if stock_data is None or not hasattr(stock_data, "empty") or stock_data.empty:
            return "历史行情摘要：暂无可用历史行情。"

        close_series = stock_data.get("Close")
        volume_series = stock_data.get("Volume")
        ma20_series = stock_data.get("MA20")
        ma60_series = stock_data.get("MA60")
        if close_series is None or len(close_series) == 0:
            return "历史行情摘要：暂无可用历史行情。"

        latest_close = close_series.iloc[-1]
        latest_ma20 = ma20_series.iloc[-1] if ma20_series is not None and len(ma20_series) else None
        latest_ma60 = ma60_series.iloc[-1] if ma60_series is not None and len(ma60_series) else None

        def _pct_change(window: int) -> str:
            if len(close_series) <= window:
                return "N/A"
            base_value = close_series.iloc[-window - 1]
            if base_value in (None, 0):
                return "N/A"
            return f"{((latest_close / base_value) - 1) * 100:.2f}%"

        recent_20 = close_series.tail(20)
        high_20 = f"{recent_20.max():.2f}" if len(recent_20) else "N/A"
        low_20 = f"{recent_20.min():.2f}" if len(recent_20) else "N/A"

        volume_ratio_5_to_20 = "N/A"
        if volume_series is not None and len(volume_series) >= 20:
            recent_5_avg = volume_series.tail(5).mean()
            recent_20_avg = volume_series.tail(20).mean()
            if recent_20_avg:
                volume_ratio_5_to_20 = f"{recent_5_avg / recent_20_avg:.2f}x"

        relative_ma20 = "N/A"
        if latest_ma20 not in (None, 0):
            relative_ma20 = f"{((latest_close / latest_ma20) - 1) * 100:.2f}%"

        relative_ma60 = "N/A"
        if latest_ma60 not in (None, 0):
            relative_ma60 = f"{((latest_close / latest_ma60) - 1) * 100:.2f}%"

        return f"""
历史行情摘要：
- 最近收盘价相对MA20：{relative_ma20}
- 最近收盘价相对MA60：{relative_ma60}
- 近20日涨跌幅：{_pct_change(20)}
- 近60日涨跌幅：{_pct_change(60)}
- 近20日最高价：{high_20}
- 近20日最低价：{low_20}
- 最近5日均量/近20日均量：{volume_ratio_5_to_20}
"""

    def call_api(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        tier: Optional[ModelTier] = None,
        include_reasoning: bool = True,
    ) -> str:
        """调用DeepSeek API"""
        model_to_use = resolve_model_name(
            tier=tier,
            explicit_model=model,
            forced_model=self.model,
            lightweight_model=self.lightweight_model,
            reasoning_model=self.reasoning_model,
        )

        # 对于 reasoner 模型，自动增加 max_tokens
        if "reasoner" in model_to_use.lower() and max_tokens <= 2000:
            max_tokens = 8000  # reasoner 模型需要更多 tokens 来输出推理过程

        try:
            response = self.client.chat.completions.create(
                model=model_to_use,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )

            # 处理 reasoner 模型的响应
            message = response.choices[0].message

            # reasoner 模型可能包含 reasoning_content（推理过程）和 content（最终答案）
            # 我们返回完整内容，包括推理过程（如果有的话）
            result = ""

            # 检查是否有推理内容
            if include_reasoning and hasattr(message, 'reasoning_content') and message.reasoning_content:
                result += f"【推理过程】\n{message.reasoning_content}\n\n"

            # 添加最终内容
            if message.content:
                result += message.content

            return result if result else "API返回空响应"

        except Exception as e:
            raise RuntimeError(f"DeepSeek API调用失败: {str(e)}") from e

    def technical_analysis(self, stock_info: Dict, stock_data: Any, indicators: Dict) -> str:
        """技术面分析"""
        technical_context = self._build_technical_context(stock_data)
        messages = build_messages(
            "stock_analysis/technical.system.txt",
            "stock_analysis/technical.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            current_price=stock_info.get("current_price", "N/A"),
            change_percent=stock_info.get("change_percent", "N/A"),
            price=indicators.get("price", "N/A"),
            ma5=indicators.get("ma5", "N/A"),
            ma10=indicators.get("ma10", "N/A"),
            ma20=indicators.get("ma20", "N/A"),
            ma60=indicators.get("ma60", "N/A"),
            rsi=indicators.get("rsi", "N/A"),
            macd=indicators.get("macd", "N/A"),
            macd_signal=indicators.get("macd_signal", "N/A"),
            bb_upper=indicators.get("bb_upper", "N/A"),
            bb_lower=indicators.get("bb_lower", "N/A"),
            k_value=indicators.get("k_value", "N/A"),
            d_value=indicators.get("d_value", "N/A"),
            volume_ratio=indicators.get("volume_ratio", "N/A"),
            technical_context=technical_context,
        )

        return self.call_api(messages, tier=ModelTier.LIGHTWEIGHT)

    def fundamental_analysis(self, stock_info: Dict, financial_data: Dict = None, quarterly_data: Dict = None) -> str:
        """基本面分析"""
        
        # 构建财务数据部分
        financial_section = ""
        if financial_data and not financial_data.get('error'):
            ratios = financial_data.get('financial_ratios', {})
            if ratios:
                financial_section = f"""
详细财务指标：
【盈利能力】
- 净资产收益率(ROE)：{ratios.get('净资产收益率ROE', ratios.get('ROE', 'N/A'))}
- 总资产收益率(ROA)：{ratios.get('总资产收益率ROA', ratios.get('ROA', 'N/A'))}
- 销售毛利率：{ratios.get('销售毛利率', ratios.get('毛利率', 'N/A'))}
- 销售净利率：{ratios.get('销售净利率', ratios.get('净利率', 'N/A'))}

【偿债能力】
- 资产负债率：{ratios.get('资产负债率', 'N/A')}
- 流动比率：{ratios.get('流动比率', 'N/A')}
- 速动比率：{ratios.get('速动比率', 'N/A')}

【运营能力】
- 存货周转率：{ratios.get('存货周转率', 'N/A')}
- 应收账款周转率：{ratios.get('应收账款周转率', 'N/A')}
- 总资产周转率：{ratios.get('总资产周转率', 'N/A')}

【成长能力】
- 营业收入同比增长：{ratios.get('营业收入同比增长', ratios.get('收入增长', 'N/A'))}
- 净利润同比增长：{ratios.get('净利润同比增长', ratios.get('盈利增长', 'N/A'))}

【每股指标】
- 每股收益(EPS)：{ratios.get('EPS', 'N/A')}
- 每股账面价值：{ratios.get('每股账面价值', 'N/A')}
- 股息率：{ratios.get('股息率', stock_info.get('dividend_yield', 'N/A'))}
- 派息率：{ratios.get('派息率', 'N/A')}
"""
            
            # 添加报告期信息
            if ratios.get('报告期'):
                financial_section = f"\n财务数据报告期：{ratios.get('报告期')}\n" + financial_section
        
        # 构建季报数据部分
        quarterly_section = ""
        if quarterly_data and quarterly_data.get('data_success'):
            # 使用格式化的季报数据
            from quarterly_report_data import QuarterlyReportDataFetcher
            fetcher = QuarterlyReportDataFetcher()
            quarterly_section = f"""

【最近8期季报详细数据】
{fetcher.format_quarterly_reports_for_ai(quarterly_data)}

以上是通过akshare获取的最近8期季度财务报告，请重点基于这些数据进行趋势分析。
"""
        
        messages = build_messages(
            "stock_analysis/fundamental.system.txt",
            "stock_analysis/fundamental.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            current_price=stock_info.get("current_price", "N/A"),
            market_cap=stock_info.get("market_cap", "N/A"),
            sector=stock_info.get("sector", "N/A"),
            industry=stock_info.get("industry", "N/A"),
            pe_ratio=stock_info.get("pe_ratio", "N/A"),
            pb_ratio=stock_info.get("pb_ratio", "N/A"),
            ps_ratio=stock_info.get("ps_ratio", "N/A"),
            beta=stock_info.get("beta", "N/A"),
            high_52_week=stock_info.get("52_week_high", "N/A"),
            low_52_week=stock_info.get("52_week_low", "N/A"),
            financial_section=financial_section,
            quarterly_section=quarterly_section,
        )
        
        return self.call_api(messages, tier=ModelTier.REASONING)
    
    def fund_flow_analysis(self, stock_info: Dict, indicators: Dict, fund_flow_data: Dict = None) -> str:
        """资金面分析"""
        
        # 构建资金流向数据部分 - 使用akshare格式化数据
        fund_flow_section = ""
        if fund_flow_data and fund_flow_data.get('data_success'):
            # 使用格式化的资金流向数据
            from fund_flow_akshare import FundFlowAkshareDataFetcher
            fetcher = FundFlowAkshareDataFetcher()
            fund_flow_section = f"""

【近20个交易日资金流向详细数据】
{fetcher.format_fund_flow_for_ai(fund_flow_data)}

以上是通过akshare从东方财富获取的实际资金流向数据，请重点基于这些数据进行趋势分析。
"""
        else:
            fund_flow_section = "\n【资金流向数据】\n注意：未能获取到资金流向数据，将基于成交量进行分析。\n"
        
        messages = build_messages(
            "stock_analysis/fund_flow.system.txt",
            "stock_analysis/fund_flow.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            current_price=stock_info.get("current_price", "N/A"),
            market_cap=stock_info.get("market_cap", "N/A"),
            volume_ratio=indicators.get("volume_ratio", "N/A"),
            fund_flow_section=fund_flow_section,
        )

        return self.call_api(messages, max_tokens=3000, tier=ModelTier.LIGHTWEIGHT)

    def comprehensive_discussion(self, technical_report: str, fundamental_report: str, 
                               fund_flow_report: str, stock_info: Dict) -> str:
        """综合讨论"""
        messages = build_messages(
            "stock_analysis/comprehensive_discussion.system.txt",
            "stock_analysis/comprehensive_discussion.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            current_price=stock_info.get("current_price", "N/A"),
            technical_report=technical_report,
            fundamental_report=fundamental_report,
            fund_flow_report=fund_flow_report,
        )
        
        return self.call_api(messages, max_tokens=6000, tier=ModelTier.REASONING)
    
    def final_decision(self, comprehensive_discussion: str, stock_info: Dict, 
                      indicators: Dict) -> Dict[str, Any]:
        """最终投资决策"""
        messages = build_messages(
            "stock_analysis/final_decision.system.txt",
            "stock_analysis/final_decision.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            current_price=stock_info.get("current_price", "N/A"),
            comprehensive_discussion=comprehensive_discussion,
            ma20=indicators.get("ma20", "N/A"),
            bb_upper=indicators.get("bb_upper", "N/A"),
            bb_lower=indicators.get("bb_lower", "N/A"),
        )

        response = self.call_api(
            messages,
            temperature=0.3,
            max_tokens=4000,
            tier=ModelTier.REASONING,
            include_reasoning=False,
        )

        decision_json = self._extract_json_object(response)
        return decision_json
