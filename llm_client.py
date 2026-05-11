import json
import os
import time
from typing import Any, Dict, List, Optional
from datetime import datetime
from types import SimpleNamespace

try:
    import openai
except ImportError:  # pragma: no cover - optional dependency in test envs
    class _MissingOpenAIClient:
        def __init__(self, *args, **kwargs):
            self.chat = SimpleNamespace(
                completions=SimpleNamespace(create=self._raise_missing_dependency)
            )

        @staticmethod
        def _raise_missing_dependency(*args, **kwargs):
            raise ImportError("openai package is not installed")

    openai = SimpleNamespace(OpenAI=_MissingOpenAIClient)

import config
from final_decision_calibration import calibrate_final_decision
from investment_action_utils import build_holding_strategy_prompt_block
from model_routing import (
    ModelTier,
    describe_model_selection,
    resolve_model_name,
)
from prompt_registry import build_messages


class LLMClient:
    """LLM API客户端"""

    def __init__(self, model=None, lightweight_model=None, reasoning_model=None):
        # 兼容旧接口：model 表示强制所有任务统一使用一个模型
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self._client_cache: dict[tuple[str, str], openai.OpenAI] = {}
        self._default_client_credentials = (config.WARMMILK_API_KEY, config.WARMMILK_BASE_URL)
        self.client = openai.OpenAI(
            api_key=config.WARMMILK_API_KEY,
            base_url=config.WARMMILK_BASE_URL,
            timeout=config.LLM_API_TIMEOUT_SECONDS,
        )
        self.api_retry_count = max(0, int(os.getenv("LLM_API_RETRY_COUNT", "2") or 2))
        self.api_retry_base_delay_seconds = max(0.2, float(os.getenv("LLM_API_RETRY_BASE_DELAY_SECONDS", "0.8") or 0.8))
        self.model_selection = describe_model_selection(
            forced_model=self.model,
            lightweight_model=self.lightweight_model,
            reasoning_model=self.reasoning_model,
        )

    class EmptyResponseError(RuntimeError):
        """Raised when the upstream model returns no usable content."""

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

    def _get_client_for_model(self, model_name: str):
        if not hasattr(self, "_client_cache"):
            return getattr(self, "client")

        api_key, base_url = config.get_model_api_credentials(model_name)
        if not api_key or not base_url:
            raise RuntimeError(f"模型 {model_name} 未配置可用的 API Key 和 BASE_URL")
        credentials = (api_key, base_url)

        if hasattr(self, "_default_client_credentials") and credentials == self._default_client_credentials:
            return self.client

        cached_client = self._client_cache.get(credentials)
        if cached_client is not None:
            return cached_client

        client = openai.OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=config.LLM_API_TIMEOUT_SECONDS,
        )
        self._client_cache[credentials] = client
        return client

    def call_api(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: int = 2000,
        tier: Optional[ModelTier] = None,
        include_reasoning: bool = False,
        top_p: Optional[float] = None,
        sampling_profile: str = "default",
    ) -> str:
        """调用LLM API"""
        model_to_use = resolve_model_name(
            tier=tier,
            explicit_model=model,
            forced_model=self.model,
            lightweight_model=self.lightweight_model,
            reasoning_model=self.reasoning_model,
        )
        api_model_to_use = config.get_model_api_name(model_to_use)
        total_attempts = max(1, int(getattr(self, "api_retry_count", 2)) + 1)
        base_delay = max(0.2, float(getattr(self, "api_retry_base_delay_seconds", 0.8)))
        last_error: Exception | None = None
        client = self._get_client_for_model(model_to_use)
        effective_temperature, effective_top_p = config.resolve_llm_sampling_params(
            model_to_use,
            temperature=temperature,
            top_p=top_p,
            profile=sampling_profile,
        )

        candidate_max_tokens = max_tokens
        if "reasoner" in model_to_use.lower() and candidate_max_tokens <= 2000:
            candidate_max_tokens = 8000  # reasoner 模型需要更多 tokens 来输出推理过程

        for attempt in range(1, total_attempts + 1):
            try:
                request_kwargs = {
                    "model": api_model_to_use,
                    "messages": messages,
                    "temperature": effective_temperature,
                    "max_tokens": candidate_max_tokens,
                }
                if effective_top_p < 1.0:
                    request_kwargs["top_p"] = effective_top_p
                response = client.chat.completions.create(
                    **request_kwargs,
                )

                # 处理 reasoner 模型的响应
                message = response.choices[0].message

                # reasoner 模型可能包含 reasoning_content（推理过程）和 content（最终答案）
                # 我们返回完整内容，包括推理过程（如果有的话）
                result = ""

                # 检查是否有推理内容
                if include_reasoning and hasattr(message, "reasoning_content") and message.reasoning_content:
                    result += f"【推理过程】\n{message.reasoning_content}\n\n"

                # 添加最终内容
                if message.content:
                    result += message.content

                if result.strip():
                    return result
                raise self.EmptyResponseError("llm_empty_response")
            except Exception as e:
                last_error = e
                if self._is_model_not_found_error(e):
                    break
                if attempt >= total_attempts or not self._is_retryable_llm_error(e):
                    break
                delay_seconds = base_delay * (2 ** (attempt - 1))
                print(
                    f"[LLM] 调用失败，正在重试 ({attempt}/{total_attempts - 1})，"
                    f"model={model_to_use} -> {api_model_to_use}，{delay_seconds:.1f}s 后重试：{self._format_error_message(e)}"
                )
                time.sleep(delay_seconds)

        raise RuntimeError(f"LLM API调用失败: {self._format_error_message(last_error)}") from last_error

    @staticmethod
    def _retryable_exception_types() -> tuple[type[BaseException], ...]:
        candidates = []
        for name in ("APIConnectionError", "APITimeoutError", "InternalServerError", "RateLimitError", "APIStatusError"):
            candidate = getattr(openai, name, None)
            if isinstance(candidate, type) and issubclass(candidate, BaseException):
                candidates.append(candidate)
        return tuple(candidates)

    @classmethod
    def _format_error_message(cls, error: Exception | None) -> str:
        if error is None:
            return "unknown_request_error"
        return str(error)

    @classmethod
    def _is_retryable_llm_error(cls, error: Exception) -> bool:
        retryable_types = cls._retryable_exception_types()
        if retryable_types and isinstance(error, retryable_types):
            status_code = getattr(error, "status_code", None)
            response = getattr(error, "response", None)
            if status_code is None and response is not None:
                status_code = getattr(response, "status_code", None)
            if status_code in {408, 409, 429}:
                return True
            if status_code is not None:
                try:
                    return int(status_code) >= 500
                except (TypeError, ValueError):
                    pass

        message = str(error).lower()
        transient_markers = (
            "llm_empty_response",
            "auth_unavailable",
            "server_error",
            "internal_server_error",
            "rate limit",
            "timeout",
            "timed out",
            "connection error",
            "connection reset",
            "temporarily unavailable",
            "try again",
        )
        return any(marker in message for marker in transient_markers)

    @staticmethod
    def _is_model_not_found_error(error: Exception) -> bool:
        status_code = getattr(error, "status_code", None)
        response = getattr(error, "response", None)
        if status_code is None and response is not None:
            status_code = getattr(response, "status_code", None)
        if status_code == 404:
            return True

        message = str(error).lower()
        return any(
            marker in message
            for marker in (
                "invalidendpointormodel.notfound",
                "does not exist or you do not have access",
                "model not found",
                "endpoint not found",
            )
        )

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
            chip_data_source=indicators.get("chip_data_source", "N/A"),
            chip_trade_date=indicators.get("chip_trade_date", "N/A"),
            chip_peak_shape=indicators.get("chip_peak_shape", "N/A"),
            main_chip_peak_price=indicators.get("main_chip_peak_price", "N/A"),
            secondary_chip_peak_price=indicators.get("secondary_chip_peak_price", "N/A"),
            chip_concentration=indicators.get("chip_concentration", "N/A"),
            average_chip_cost=indicators.get("average_chip_cost", "N/A"),
            cost_band_70=indicators.get("cost_band_70", "N/A"),
            cost_band_90=indicators.get("cost_band_90", "N/A"),
            current_price_position=indicators.get("current_price_position", "N/A"),
            upper_pressure_peak=indicators.get("upper_pressure_peak", "N/A"),
            lower_support_peak=indicators.get("lower_support_peak", "N/A"),
            profit_ratio_estimate=indicators.get("profit_ratio_estimate", "N/A"),
            trap_ratio_estimate=indicators.get("trap_ratio_estimate", "N/A"),
            technical_context=technical_context,
        )

        return self.call_api(messages, tier=ModelTier.LIGHTWEIGHT)

    def fundamental_analysis(self, stock_info: Dict, financial_data: Dict = None, quarterly_data: Dict = None) -> str:
        """基本面分析"""
        
        # 构建财务数据部分
        financial_section = ""
        business_profile = (
            stock_info.get("business_summary")
            or stock_info.get("主营业务")
            or stock_info.get("主营构成")
            or stock_info.get("business_structure")
        )
        if financial_data and not financial_data.get('error'):
            ratios = financial_data.get('financial_ratios', {})
            if ratios:
                def _ratio_value(*keys):
                    for key in keys:
                        value = ratios.get(key)
                        if value not in (None, "", "N/A"):
                            return value
                    return "N/A"

                dividend_yield = _ratio_value("股息率")
                if dividend_yield == "N/A" and stock_info.get("dividend_yield") not in (None, "", "N/A"):
                    dividend_yield = stock_info.get("dividend_yield")

                financial_section = f"""
主营业务/业务结构概况：
- {business_profile or 'N/A'}

详细财务指标：
【盈利能力】
- 净资产收益率(ROE)：{_ratio_value('净资产收益率ROE', 'ROE', '净资产收益率')}
- 总资产收益率(ROA)：{_ratio_value('总资产收益率ROA', 'ROA', '总资产净利率')}
- 销售毛利率：{_ratio_value('销售毛利率', '毛利率')}
- 销售净利率：{_ratio_value('销售净利率', '净利率')}

【偿债能力】
- 资产负债率：{_ratio_value('资产负债率')}
- 流动比率：{_ratio_value('流动比率')}
- 速动比率：{_ratio_value('速动比率')}

【运营能力】
- 存货周转率：{_ratio_value('存货周转率')}
- 应收账款周转率：{_ratio_value('应收账款周转率')}
- 总资产周转率：{_ratio_value('总资产周转率')}

【成长能力】
- 营业收入同比增长：{_ratio_value('营业收入同比增长', '营业总收入同比增长', '收入增长')}
- 净利润同比增长：{_ratio_value('净利润同比增长', '扣非净利润同比增长', '盈利增长')}

【每股指标】
- 每股收益(EPS)：{_ratio_value('EPS', '每股收益')}
- 每股账面价值：{_ratio_value('每股账面价值', '每股净资产')}
- 股息率：{dividend_yield}
- 派息率：{_ratio_value('派息率')}
"""
        elif business_profile:
            financial_section = f"""
主营业务/业务结构概况：
- {business_profile}
"""
            
            # 添加报告期信息
            if ratios.get('报告期'):
                financial_section = f"\n最新财务指标报告期（非当前分析日期）：{ratios.get('报告期')}\n" + financial_section
        
        # 构建季报数据部分
        quarterly_section = ""
        if quarterly_data and quarterly_data.get('data_success'):
            # 使用格式化的季报数据
            from quarterly_report_data import QuarterlyReportDataFetcher
            fetcher = QuarterlyReportDataFetcher()
            quarterly_section = f"""

【最近8期季报详细数据】
{fetcher.format_quarterly_reports_for_ai(quarterly_data)}

以上是通过Tushare获取的最近8期季度财务报告，请重点基于这些数据进行趋势分析。
"""
        
        messages = build_messages(
            "stock_analysis/fundamental.system.txt",
            "stock_analysis/fundamental.user.txt",
            market_date=datetime.now().strftime("%Y-%m-%d"),
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
        
        return self.call_api(
            messages,
            max_tokens=max(12000, int(os.getenv("FUNDAMENTAL_ANALYSIS_MAX_TOKENS", "24000") or 24000)),
            tier=ModelTier.REASONING,
        )
    
    def fund_flow_analysis(self, stock_info: Dict, indicators: Dict, fund_flow_data: Dict = None) -> str:
        """资金面分析"""
        
        # 构建资金流向数据部分
        fund_flow_section = ""
        if fund_flow_data and fund_flow_data.get('data_success'):
            from fund_flow_data import FundFlowDataFetcher
            fetcher = FundFlowDataFetcher()
            fund_flow_section = f"""

【近20个交易日资金流向详细数据】
{fetcher.format_fund_flow_for_ai(fund_flow_data)}

以上是通过Tushare获取的实际资金流向数据，请重点基于这些数据进行趋势分析。
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
    
    def final_decision(
        self,
        comprehensive_discussion: str,
        stock_info: Dict,
        indicators: Dict,
        strategy_context: Optional[Dict[str, Any]] = None,
        is_initial_holding_analysis: bool = False,
    ) -> Dict[str, Any]:
        """最终投资决策"""
        has_position = bool(stock_info.get("has_position"))
        position_status = "已持仓" if has_position else "未持仓"
        rating_options = "加仓/持有/减仓/卖出" if has_position else "买入/强烈买入/观望"
        messages = build_messages(
            "stock_analysis/final_decision.system.txt",
            "stock_analysis/final_decision.user.txt",
            symbol=stock_info.get("symbol", "N/A"),
            name=stock_info.get("name", "N/A"),
            current_price=stock_info.get("current_price", "N/A"),
            position_status=position_status,
            rating_options=rating_options,
            comprehensive_discussion=comprehensive_discussion,
            holding_strategy_prompt_block=build_holding_strategy_prompt_block(
                has_position=has_position,
                strategy_context=strategy_context,
                is_initial_holding_analysis=is_initial_holding_analysis,
            ),
            ma20=indicators.get("ma20", "N/A"),
            bb_upper=indicators.get("bb_upper", "N/A"),
            bb_lower=indicators.get("bb_lower", "N/A"),
            chip_data_source=indicators.get("chip_data_source", "N/A"),
            chip_trade_date=indicators.get("chip_trade_date", "N/A"),
            chip_peak_shape=indicators.get("chip_peak_shape", "N/A"),
            main_chip_peak_price=indicators.get("main_chip_peak_price", "N/A"),
            secondary_chip_peak_price=indicators.get("secondary_chip_peak_price", "N/A"),
            chip_concentration=indicators.get("chip_concentration", "N/A"),
            average_chip_cost=indicators.get("average_chip_cost", "N/A"),
            cost_band_70=indicators.get("cost_band_70", "N/A"),
            cost_band_90=indicators.get("cost_band_90", "N/A"),
            current_price_position=indicators.get("current_price_position", "N/A"),
            upper_pressure_peak=indicators.get("upper_pressure_peak", "N/A"),
            lower_support_peak=indicators.get("lower_support_peak", "N/A"),
            profit_ratio_estimate=indicators.get("profit_ratio_estimate", "N/A"),
            trap_ratio_estimate=indicators.get("trap_ratio_estimate", "N/A"),
        )

        response = self.call_api(
            messages,
            sampling_profile="factual",
            max_tokens=4000,
            tier=ModelTier.REASONING,
            include_reasoning=False,
        )

        decision_json = self._extract_json_object(response)
        return calibrate_final_decision(
            decision_json,
            stock_info=stock_info,
            has_position=has_position,
        )


class EmbeddingClient:
    """Lightweight client for text embedding via an OpenAI-compatible API.

    By default uses the SiliconFlow BGE-m3 endpoint configured through
    ``EMBEDDING_*`` env vars in ``config.py``.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self.api_key = api_key or config.EMBEDDING_API_KEY
        self.base_url = base_url or config.EMBEDDING_BASE_URL
        self.model = model or config.EMBEDDING_MODEL_NAME
        self._client: Optional[openai.OpenAI] = None

    def _get_client(self) -> openai.OpenAI:
        if self._client is None:
            if not self.api_key:
                raise RuntimeError("EMBEDDING_API_KEY is not configured")
            self._client = openai.OpenAI(
                api_key=self.api_key,
                base_url=self.base_url,
                timeout=30,
            )
        return self._client

    @property
    def is_available(self) -> bool:
        return bool(self.api_key and self.base_url and self.model)

    def get_embedding(self, text: str) -> List[float]:
        """Return the embedding vector for *text*.

        Raises ``RuntimeError`` when the API key is missing or the
        upstream request fails.
        """
        client = self._get_client()
        response = client.embeddings.create(
            model=self.model,
            input=text,
        )
        return response.data[0].embedding

    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Batch variant — returns one vector per input text."""
        if not texts:
            return []
        client = self._get_client()
        response = client.embeddings.create(
            model=self.model,
            input=texts,
        )
        # Sort by index to guarantee order matches input
        sorted_data = sorted(response.data, key=lambda d: d.index)
        return [item.embedding for item in sorted_data]
