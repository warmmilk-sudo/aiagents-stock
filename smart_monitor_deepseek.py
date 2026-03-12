"""
智能盯盘 - DeepSeek AI 决策引擎
适配A股T+1交易规则的AI决策系统
"""

import ast
import json
import logging
import re
from typing import Any, Dict, List, Optional
from datetime import datetime, time
import time as time_module

import pytz
import requests

import config
from model_routing import ModelTier, resolve_model_name


class SmartMonitorDeepSeek:
    """A股智能盯盘 - DeepSeek AI决策引擎"""

    def __init__(self, api_key: str, model: str = None,
                 lightweight_model: str = None, reasoning_model: str = None):
        """
        初始化DeepSeek客户端
        
        Args:
            api_key: DeepSeek API密钥
        """
        self.api_key = api_key
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.base_url = config.DEEPSEEK_BASE_URL
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.logger = logging.getLogger(__name__)
        self.http_timeout_seconds = max(
            15,
            int(getattr(config, "SMART_MONITOR_HTTP_TIMEOUT_SECONDS", 30) or 30),
        )
        self.http_retry_count = max(
            0,
            int(getattr(config, "SMART_MONITOR_HTTP_RETRY_COUNT", 1) or 1),
        )
        self.reasoning_max_tokens = max(
            1500,
            int(getattr(config, "SMART_MONITOR_REASONING_MAX_TOKENS", 3000) or 3000),
        )

    def set_model_overrides(self, model: str = None,
                            lightweight_model: str = None,
                            reasoning_model: str = None) -> None:
        """更新当前会话的模型覆盖配置。"""
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model

    def is_trading_time(self) -> bool:
        """
        判断当前是否在A股交易时间内
        
        Returns:
            bool: 是否可以交易
        """
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(beijing_tz)
        current_time = now.time()
        
        # 排除周末
        if now.weekday() >= 5:
            return False
        
        # 上午：9:30-11:30
        morning_start = time(9, 30)
        morning_end = time(11, 30)
        
        # 下午：13:00-15:00
        afternoon_start = time(13, 0)
        afternoon_end = time(15, 0)
        
        is_trading = (
            (morning_start <= current_time <= morning_end) or
            (afternoon_start <= current_time <= afternoon_end)
        )
        
        return is_trading

    def get_trading_session(self) -> Dict:
        """
        获取当前交易时段信息（A股版本）
        
        Returns:
            Dict: 时段信息
        """
        beijing_tz = pytz.timezone('Asia/Shanghai')
        now = datetime.now(beijing_tz)
        current_time = now.time()
        
        # 判断是否交易日
        if now.weekday() >= 5:
            return {
                'session': '休市',
                'volatility': 'none',
                'recommendation': '周末不可交易',
                'beijing_hour': now.hour,
                'can_trade': False
            }
        
        # 开盘前（9:00-9:30）：集合竞价时段
        if time(9, 0) <= current_time < time(9, 30):
            return {
                'session': '集合竞价',
                'volatility': 'high',
                'recommendation': '可观察盘面情绪，准备开盘交易',
                'beijing_hour': now.hour,
                'can_trade': False
            }
        
        # 上午盘（9:30-11:30）
        elif time(9, 30) <= current_time <= time(11, 30):
            return {
                'session': '上午盘',
                'volatility': 'high',
                'recommendation': '交易活跃，波动较大',
                'beijing_hour': now.hour,
                'can_trade': True
            }
        
        # 午间休市（11:30-13:00）
        elif time(11, 30) < current_time < time(13, 0):
            return {
                'session': '午间休市',
                'volatility': 'none',
                'recommendation': '不可交易，可分析上午盘面',
                'beijing_hour': now.hour,
                'can_trade': False
            }
        
        # 下午盘（13:00-15:00）
        elif time(13, 0) <= current_time <= time(15, 0):
            # 尾盘最后半小时（14:30-15:00）
            if current_time >= time(14, 30):
                return {
                    'session': '尾盘',
                    'volatility': 'high',
                    'recommendation': '尾盘波动大，谨慎操作',
                    'beijing_hour': now.hour,
                    'can_trade': True
                }
            else:
                return {
                    'session': '下午盘',
                    'volatility': 'medium',
                    'recommendation': '波动趋缓，适合布局',
                    'beijing_hour': now.hour,
                    'can_trade': True
                }
        
        # 盘后（15:00之后）
        else:
            return {
                'session': '盘后',
                'volatility': 'none',
                'recommendation': '收盘后，可复盘分析',
                'beijing_hour': now.hour,
                'can_trade': False
            }

    def chat_completion(self, messages: List[Dict], model: str = None,
                       temperature: float = 0.7, max_tokens: int = 2000,
                       tier: ModelTier = ModelTier.LIGHTWEIGHT) -> Dict:
        """
        调用DeepSeek API
        
        Args:
            messages: 对话消息列表
            model: 模型名称
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            API响应
        """
        model_to_use = resolve_model_name(
            tier=tier,
            explicit_model=model,
            forced_model=self.model,
            lightweight_model=self.lightweight_model,
            reasoning_model=self.reasoning_model,
        )

        if "reasoner" in model_to_use.lower() and max_tokens <= 2000:
            max_tokens = max(max_tokens, self.reasoning_max_tokens)
        
        payload = {
            "model": model_to_use,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        endpoint = f"{self.base_url.rstrip('/')}/chat/completions"
        request_timeout = (10, self.http_timeout_seconds)
        total_attempts = self.http_retry_count + 1
        retryable_errors = (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
        last_error: Optional[Exception] = None

        for attempt_index in range(total_attempts):
            try:
                response = requests.post(
                    endpoint,
                    headers=self.headers,
                    json=payload,
                    timeout=request_timeout
                )
                response.raise_for_status()
                return response.json()
            except retryable_errors as exc:
                last_error = exc
                if attempt_index >= self.http_retry_count:
                    break
                self.logger.warning(
                    "DeepSeek API请求超时或连接失败，准备重试 (%s/%s)，model=%s，read_timeout=%ss: %s",
                    attempt_index + 1,
                    total_attempts,
                    model_to_use,
                    self.http_timeout_seconds,
                    exc,
                )
                time_module.sleep(min(2, attempt_index + 1))
            except Exception as exc:
                self.logger.error(
                    "DeepSeek API调用失败，model=%s，timeout=%ss: %s",
                    model_to_use,
                    self.http_timeout_seconds,
                    exc,
                )
                raise

        if last_error is not None:
            self.logger.error(
                "DeepSeek API调用失败，重试后仍未成功，model=%s，timeout=%ss: %s",
                model_to_use,
                self.http_timeout_seconds,
                last_error,
            )
            raise last_error
        raise RuntimeError("DeepSeek API调用失败: unknown_request_error")

    def analyze_stock_and_decide(self, stock_code: str, market_data: Dict,
                                 account_info: Dict, has_position: bool = False,
                                 position_cost: float = 0, position_quantity: int = 0,
                                 account_name: str = "默认账户",
                                 asset_id: Optional[int] = None,
                                 portfolio_stock_id: Optional[int] = None,
                                 strategy_context: Optional[Dict] = None) -> Dict:
        """
        分析股票并做出交易决策（A股T+1规则）
        
        Args:
            stock_code: 股票代码（如：600519）
            market_data: 市场数据
            account_info: 账户信息
            has_position: 是否已持有该股票
            position_cost: 持仓成本价格
            position_quantity: 持仓数量
            
        Returns:
            交易决策
        """
        # 获取交易时段
        session_info = self.get_trading_session()
        
        # 构建Prompt
        prompt = self._build_a_stock_prompt(
            stock_code, market_data, account_info, 
            has_position, session_info, position_cost, position_quantity,
            account_name=account_name,
            asset_id=asset_id,
            portfolio_stock_id=portfolio_stock_id,
            strategy_context=strategy_context,
        )
        
        system_prompt = """你是一位资深的A股盘中执行分析专家，拥有15年实战经验。

你的职责是盘中战术执行，而不是重新做一遍盘后投资研究。
如果提供了 strategy_context，请把它视为最新的战略基线，只围绕实时行情、持仓盈亏和该基线决定是否执行 BUY / SELL / HOLD。
不要重新给出新的长期估值框架，不要扩展新的长期目标价体系。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ A股交易规则（与币圈完全不同！）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[CRITICAL] T+1规则：
- 今天买入的股票，**今天不能卖出**，必须等到下一个交易日
- 这意味着：一旦买入，至少要持有到明天才能卖出
- 因此买入决策必须**极其谨慎**，不能像币圈那样快进快出

[CRITICAL] 涨跌停限制：
- 主板/中小板：±10%涨跌停
- 创业板/科创板：±20%涨跌停
- ST股票：±5%涨跌停
- 一旦涨停，很难买入；一旦跌停，很难卖出

[CRITICAL] 交易时间：
- 上午：9:30-11:30
- 下午：13:00-15:00
- 其他时间不能交易

[CRITICAL] 只能做多：
- A股不能做空（融券门槛高，散户基本不用）
- 只有买入和卖出两个动作

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 你的交易哲学（适配T+1）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**因为T+1限制，你的策略必须更加稳健！**

1. **买入前三思**：
   - 买入后至少持有1天，所以必须确保趋势向上
   - 不能像币圈那样"试探性开仓"，一旦买入就是承诺
   - 最好在尾盘或第二天开盘前决策，避免盲目追高

2. **止损更困难**：
   - 如果今天买入后下跌，今天无法止损（T+1）
   - 只能等明天再卖，可能面临更大亏损
   - 因此：**宁可错过，不可做错**

3. **技术分析更重要**：
   - 日线级别趋势确认
   - 支撑位/阻力位
   - 成交量配合
   - 量价关系判断

4. **风险控制严格**：
   - 单只股票仓位 ≤ 30%（T+1风险大）
   - 止损位：-5%（明天开盘立即执行）
   - 止盈位：+8-15%（分批止盈）

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📊 可选的交易动作
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

**如果当前无持仓**：
- action = "BUY"（买入）- 必须确保技术面强势，趋势向上
- action = "HOLD"（观望）- 信号不明确时选择观望

**如果当前有持仓**：
- action = "SELL"（卖出）- 达到止盈/止损条件，或技术面转弱
- action = "HOLD"（持有）- 趋势未改变，继续持有
- ⚠️ 注意：如果股票是今天买入的，受T+1限制无法卖出，只能选择HOLD

**绝对禁止**：
- 不要在开盘前5分钟（9:30-9:35）买入，容易追高
- 不要在尾盘最后5分钟（14:55-15:00）买入，可能被套
- 不要逆趋势交易（趋势向下时买入）

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 买入信号（必须满足至少3个条件）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. ✅ 趋势向上：价格 > MA5 > MA20 > MA60（多头排列）
2. ✅ 量价配合：成交量 > 5日均量的120%（放量上涨）
3. ✅ MACD金叉：MACD > 0 且DIF上穿DEA
4. ✅ RSI健康：RSI在50-70区间（不超买不超卖）
5. ✅ 突破关键位：突破前期高点或重要阻力位
6. ✅ 布林带位置：价格接近布林中轨上方，有上行空间

**加分项**：
- 行业板块同步上涨
- 有重大利好消息
- 机构调研增加

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📉 卖出信号（满足任一条件立即卖出）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 🔴 止损触发：亏损 ≥ -5%（明天开盘立即卖出）
2. 🟢 止盈触发：盈利 ≥ +10%（分批止盈，先卖一半）
3. 🔴 趋势转弱：跌破MA20或MA60，且MACD死叉
4. 🔴 放量下跌：成交量放大但价格下跌（主力出货）
5. 🔴 技术破位：跌破重要支撑位
6. 🔴 重大利空：公司公告重大利空消息

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💬 返回格式（必须严格 JSON，对象外不要输出任何解释、Markdown、代码块）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{
    "action": "HOLD",
    "confidence": 72,
    "reasoning": "详细的决策理由，包括技术分析、风险评估等，200-300字",
    "position_size_pct": 20,
    "stop_loss_pct": 5.0,
    "take_profit_pct": 10.0,
    "risk_level": "medium",
    "key_price_levels": {
        "support": 12.34,
        "resistance": 13.10,
        "stop_loss": 11.72
    },
    "monitor_levels": {
        "entry_min": 12.10,
        "entry_max": 12.40,
        "take_profit": 13.20,
        "stop_loss": 11.70
    }
}

要求：
- 只能返回一个 JSON 对象本体，不要输出 ```json、注释、补充说明、前后缀文本。
- 所有 key 和字符串值都必须使用双引号。
- 不要输出尾逗号，不要使用 `//` 注释。
- `monitor_levels` 必须输出 4 个明确价格，不要省略。
- 如果沿用战略基线，也要把具体价格完整写入 `monitor_levels`。
- `key_price_levels` 用于解释，`monitor_levels` 用于系统实时预警回写。

**reasoning 示例**：
"茅台当前价格1650元，日线级别呈多头排列（MA5 1645 > MA20 1620 > MA60 1580），
MACD金叉且柱状图持续放大，RSI 62处于健康区间。今日成交量较5日均量放大135%，
显示有增量资金入场。技术面支撑位在1630元附近，阻力位在1680元。综合判断短期
趋势向上，但考虑T+1规则，建议仓位控制在20%，止损位设在1568元（-5%），
止盈目标1815元（+10%）。风险提示：如明日低开需谨慎..."
"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]

        try:
            response = self.chat_completion(
                messages,
                temperature=0.1,
                max_tokens=1600,
                tier=ModelTier.LIGHTWEIGHT,
            )
            ai_response = response['choices'][0]['message']['content']
            
            # 解析JSON决策
            decision = self._parse_decision(ai_response)
            decision = self._enforce_action_policy(decision, has_position=has_position)
            
            return {
                'success': True,
                'decision': decision,
                'raw_response': ai_response
            }
            
        except Exception as e:
            self.logger.error(f"AI决策失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _build_a_stock_prompt(self, stock_code: str, market_data: Dict,
                             account_info: Dict, has_position: bool,
                             session_info: Dict, position_cost: float = 0,
                             position_quantity: int = 0,
                             account_name: str = "默认账户",
                             asset_id: Optional[int] = None,
                             portfolio_stock_id: Optional[int] = None,
                             strategy_context: Optional[Dict] = None) -> str:
        """构建A股分析提示词"""
        
        prompt = f"""
[TIMER] 当前交易时段
═══════════════════════════════════════════════════════════
当前时段: {session_info['session']} (北京时间{session_info['beijing_hour']}:00)
市场状态: {session_info['volatility'].upper()}
时段建议: {session_info['recommendation']}
可交易: {'是' if session_info['can_trade'] else '否'}

[STOCK] 股票基本信息
═══════════════════════════════════════════════════════════
股票代码: {stock_code}
股票名称: {market_data.get('name', 'N/A')}
当前价格: ¥{market_data.get('current_price', 0):.2f}
今日涨跌: {market_data.get('change_pct', 0):+.2f}%
今日涨跌额: ¥{market_data.get('change_amount', 0):+.2f}
最高价: ¥{market_data.get('high', 0):.2f}
最低价: ¥{market_data.get('low', 0):.2f}
开盘价: ¥{market_data.get('open', 0):.2f}
昨收价: ¥{market_data.get('pre_close', 0):.2f}
成交量: {market_data.get('volume', 0):,.0f}手
成交额: ¥{market_data.get('amount', 0):,.2f}万

[TECHNICAL] 技术指标
═══════════════════════════════════════════════════════════
MA5: ¥{market_data.get('ma5', 0):.2f}
MA20: ¥{market_data.get('ma20', 0):.2f}
MA60: ¥{market_data.get('ma60', 0):.2f}
趋势判断: {'多头排列' if market_data.get('trend') == 'up' else '空头排列' if market_data.get('trend') == 'down' else '震荡'}

MACD:
  DIF: {market_data.get('macd_dif', 0):.4f}
  DEA: {market_data.get('macd_dea', 0):.4f}
  MACD: {market_data.get('macd', 0):.4f} ({'金叉' if market_data.get('macd', 0) > 0 else '死叉'})

RSI(6): {market_data.get('rsi6', 50):.2f} {'[超买]' if market_data.get('rsi6', 50) > 80 else '[超卖]' if market_data.get('rsi6', 50) < 20 else '[正常]'}
RSI(12): {market_data.get('rsi12', 50):.2f}
RSI(24): {market_data.get('rsi24', 50):.2f}

KDJ:
  K: {market_data.get('kdj_k', 50):.2f}
  D: {market_data.get('kdj_d', 50):.2f}
  J: {market_data.get('kdj_j', 50):.2f}

布林带:
  上轨: ¥{market_data.get('boll_upper', 0):.2f}
  中轨: ¥{market_data.get('boll_mid', 0):.2f}
  下轨: ¥{market_data.get('boll_lower', 0):.2f}
  位置: {market_data.get('boll_position', 'N/A')}

[VOLUME] 量能分析
═══════════════════════════════════════════════════════════
今日成交量: {market_data.get('volume', 0):,.0f}手
5日均量: {market_data.get('vol_ma5', 0):,.0f}手
量比: {market_data.get('volume_ratio', 0):.2f} ({'放量' if market_data.get('volume_ratio', 0) > 1.2 else '缩量' if market_data.get('volume_ratio', 0) < 0.8 else '正常'})
换手率: {market_data.get('turnover_rate', 0):.2f}%

[EXECUTION_CONTEXT] 执行与持仓上下文
═══════════════════════════════════════════════════════════
参考可用资金: ¥{account_info.get('available_cash', 0):,.2f}
参考总资产: ¥{account_info.get('total_value', 0):,.2f}
当前持仓笔数: {account_info.get('positions_count', 0)}
账户名称: {account_name}
资产ID: {asset_id or 'N/A'}
持仓ID: {portfolio_stock_id or 'N/A'}
"""
        if strategy_context:
            prompt += f"""
[STRATEGY_CONTEXT] 最新战略基线（来自盘后研究） ⭐ 重要
═══════════════════════════════════════════════════════════
分析时间: {strategy_context.get('analysis_date', 'N/A')}
分析来源: {strategy_context.get('analysis_source', 'N/A')}
战略评级: {strategy_context.get('rating', 'N/A')}
战略摘要: {strategy_context.get('summary', 'N/A')}
进场区间: {strategy_context.get('entry_min', 'N/A')} - {strategy_context.get('entry_max', 'N/A')}
止盈位: {strategy_context.get('take_profit', 'N/A')}
止损位: {strategy_context.get('stop_loss', 'N/A')}

执行要求:
- 优先沿用上述战略基线，不要重新发明长期结论
- 你的任务是判断当前盘中是否需要执行、等待或退出
"""
        # --- 注入语义化标签分析 ---
        labels = market_data.get('semantic_labels', [])
        if labels:
            prompt += f"""
[AI_PATTERN_RECOGNITION] AI形态识别标签 ⭐ 重要
═══════════════════════════════════════════════════════════
预处理引擎已发现以下关键技术形态，请在决策时重点参考：
- {chr(10) + '- '.join(labels)}
"""

        # 如果已持有该股票
        if has_position and position_cost > 0 and position_quantity > 0:
            current_price = market_data.get('current_price', 0)
            cost_total = position_cost * position_quantity
            current_total = current_price * position_quantity
            profit_loss = current_total - cost_total
            profit_loss_pct = (profit_loss / cost_total * 100) if cost_total > 0 else 0
            
            prompt += f"""
[POSITION] 当前持仓（{stock_code}） ⭐ 重要
═══════════════════════════════════════════════════════════
持仓数量: {position_quantity}股
成本价: ¥{position_cost:.2f}
当前价: ¥{current_price:.2f}
持仓市值: ¥{current_total:,.2f}
浮动盈亏: ¥{profit_loss:,.2f} ({profit_loss_pct:+.2f}%)

⚠️ T+1限制: 该股票可以卖出（不受T+1限制）

💡 决策建议：
- 如果盈利且技术指标转弱 → 建议止盈卖出
- 如果亏损超过止损线（通常-5%）→ 建议止损卖出
- 如果技术指标强势且未到止盈位 → 建议继续持有
- 如果盈利且看好后市 → 可考虑加仓（但注意仓位控制）
"""
        else:
            prompt += """
[POSITION] 当前无持仓
═══════════════════════════════════════════════════════════
可考虑买入，但必须确保：
1. 技术面强势（满足至少3个买入信号）
2. 有足够的安全边际
3. 考虑T+1规则，买入后至少持有1天
4. 控制仓位，建议单只股票仓位≤30%
"""

        # 主力资金数据（已禁用 - 接口不稳定）
        # if 'main_force' in market_data:
        #     mf = market_data['main_force']
        #     prompt += f"""
        # [MONEY] 主力资金流向
        # ═══════════════════════════════════════════════════════════
        # 主力净额: ¥{mf.get('main_net', 0):,.2f}万 ({mf.get('main_net_pct', 0):+.2f}%)
        # 超大单: ¥{mf.get('super_net', 0):,.2f}万
        # 大单: ¥{mf.get('big_net', 0):,.2f}万
        # 中单: ¥{mf.get('mid_net', 0):,.2f}万
        # 小单: ¥{mf.get('small_net', 0):,.2f}万
        # 主力动向: {mf.get('trend', '观望')}
        # """

        prompt += "\n请基于以上数据，给出交易决策（JSON格式）。"
        
        return prompt

    @staticmethod
    def _iter_json_candidates(ai_response: str) -> List[str]:
        text = str(ai_response or "").strip()
        if not text:
            return []

        candidates: List[str] = []
        for match in re.finditer(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE):
            candidate = str(match.group(1) or "").strip()
            if candidate:
                candidates.append(candidate)

        braced = SmartMonitorDeepSeek._extract_balanced_braces(text)
        if braced:
            candidates.append(braced)

        candidates.append(text)

        deduped: List[str] = []
        seen = set()
        for candidate in candidates:
            normalized = candidate.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    @staticmethod
    def _extract_balanced_braces(text: str) -> Optional[str]:
        for start_index, char in enumerate(text):
            if char != "{":
                continue
            depth = 0
            quote_char = ""
            escape = False
            for index in range(start_index, len(text)):
                current_char = text[index]
                if quote_char:
                    if escape:
                        escape = False
                    elif current_char == "\\":
                        escape = True
                    elif current_char == quote_char:
                        quote_char = ""
                    continue
                if current_char in {'"', "'"}:
                    quote_char = current_char
                    continue
                if current_char == "{":
                    depth += 1
                elif current_char == "}":
                    depth -= 1
                    if depth == 0:
                        return text[start_index:index + 1]
        return None

    @staticmethod
    def _strip_json_comments(text: str) -> str:
        result: List[str] = []
        quote_char = ""
        escape = False
        index = 0
        while index < len(text):
            current_char = text[index]
            next_char = text[index + 1] if index + 1 < len(text) else ""
            if quote_char:
                result.append(current_char)
                if escape:
                    escape = False
                elif current_char == "\\":
                    escape = True
                elif current_char == quote_char:
                    quote_char = ""
                index += 1
                continue

            if current_char in {'"', "'"}:
                quote_char = current_char
                result.append(current_char)
                index += 1
                continue

            if current_char == "/" and next_char == "/":
                index += 2
                while index < len(text) and text[index] not in "\r\n":
                    index += 1
                continue

            if current_char == "/" and next_char == "*":
                index += 2
                while index + 1 < len(text) and text[index:index + 2] != "*/":
                    index += 1
                index += 2
                continue

            result.append(current_char)
            index += 1
        return "".join(result)

    @staticmethod
    def _quote_unquoted_keys(text: str) -> str:
        pattern = re.compile(r'([{\[,]\s*)([A-Za-z_\u4e00-\u9fff][A-Za-z0-9_\-\u4e00-\u9fff]*)(\s*:)')
        return pattern.sub(r'\1"\2"\3', text)

    @staticmethod
    def _quote_known_string_values(text: str) -> str:
        replacements = {
            "action": r"BUY|SELL|HOLD|买入|卖出|持有|观望|等待|加仓|减仓|止盈|止损",
            "risk_level": r"low|medium|high|低|中|高",
        }
        normalized = text
        for field, options in replacements.items():
            pattern = re.compile(
                rf'("{field}"\s*:\s*)(?P<value>{options})(\s*[,}}])',
                re.IGNORECASE,
            )
            normalized = pattern.sub(r'\1"\g<value>"\3', normalized)
        return normalized

    @staticmethod
    def _strip_trailing_commas(text: str) -> str:
        return re.sub(r",\s*([}\]])", r"\1", text)

    @staticmethod
    def _replace_json_literals_for_python(text: str) -> str:
        replacements = {"true": "True", "false": "False", "null": "None"}
        result: List[str] = []
        quote_char = ""
        escape = False
        index = 0
        while index < len(text):
            current_char = text[index]
            if quote_char:
                result.append(current_char)
                if escape:
                    escape = False
                elif current_char == "\\":
                    escape = True
                elif current_char == quote_char:
                    quote_char = ""
                index += 1
                continue

            if current_char in {'"', "'"}:
                quote_char = current_char
                result.append(current_char)
                index += 1
                continue

            replaced = False
            for source, target in replacements.items():
                end_index = index + len(source)
                if (
                    text[index:end_index] == source
                    and (index == 0 or not (text[index - 1].isalnum() or text[index - 1] == "_"))
                    and (end_index >= len(text) or not (text[end_index].isalnum() or text[end_index] == "_"))
                ):
                    result.append(target)
                    index = end_index
                    replaced = True
                    break
            if replaced:
                continue

            result.append(current_char)
            index += 1
        return "".join(result)

    @staticmethod
    def _sanitize_json_like_text(text: str) -> str:
        translation = str.maketrans({
            "“": '"',
            "”": '"',
            "‘": "'",
            "’": "'",
            "，": ",",
            "：": ":",
            "；": ";",
        })
        sanitized = str(text or "").strip().translate(translation)
        sanitized = SmartMonitorDeepSeek._strip_json_comments(sanitized)
        sanitized = SmartMonitorDeepSeek._quote_unquoted_keys(sanitized)
        sanitized = SmartMonitorDeepSeek._quote_known_string_values(sanitized)
        sanitized = SmartMonitorDeepSeek._strip_trailing_commas(sanitized)
        return sanitized

    @staticmethod
    def _coerce_numeric(value: Any, *, default: float = 0.0, scale_fraction_to_pct: bool = False) -> float:
        if isinstance(value, bool):
            return float(default)
        if isinstance(value, (int, float)):
            number = float(value)
        else:
            text = str(value or "").replace(",", "").strip()
            match = re.search(r"-?\d+(?:\.\d+)?", text)
            if not match:
                return float(default)
            number = float(match.group(0))
        if scale_fraction_to_pct and 0 <= number <= 1:
            number *= 100
        return number

    @staticmethod
    def _normalize_action_value(value: Any) -> str:
        text = str(value or "").strip().upper()
        mapping = {
            "BUY": "BUY",
            "买入": "BUY",
            "加仓": "BUY",
            "建仓": "BUY",
            "SELL": "SELL",
            "卖出": "SELL",
            "减仓": "SELL",
            "止盈": "SELL",
            "止损": "SELL",
            "HOLD": "HOLD",
            "持有": "HOLD",
            "观望": "HOLD",
            "等待": "HOLD",
        }
        return mapping.get(text, "HOLD")

    @staticmethod
    def _normalize_risk_level(value: Any) -> str:
        text = str(value or "").strip().lower()
        mapping = {
            "low": "low",
            "低": "low",
            "medium": "medium",
            "中": "medium",
            "high": "high",
            "高": "high",
        }
        return mapping.get(text, "medium")

    @staticmethod
    def _normalize_key_price_levels(value: Any) -> Dict[str, float]:
        if not isinstance(value, dict):
            return {}
        normalized: Dict[str, float] = {}
        for key in ("support", "resistance", "stop_loss"):
            raw_value = value.get(key)
            if raw_value in (None, ""):
                continue
            try:
                normalized[key] = float(SmartMonitorDeepSeek._coerce_numeric(raw_value))
            except (TypeError, ValueError):
                continue
        return normalized

    def _normalize_decision_payload(self, decision: Dict[str, Any]) -> Dict[str, Any]:
        reasoning_text = decision.get("reasoning")
        if isinstance(reasoning_text, (dict, list)):
            reasoning_text = json.dumps(reasoning_text, ensure_ascii=False)
        reasoning = str(reasoning_text or "").strip()
        if not reasoning:
            raise ValueError("缺少必需字段: reasoning")

        normalized: Dict[str, Any] = {
            "action": self._normalize_action_value(decision.get("action")),
            "confidence": int(max(0, min(100, round(self._coerce_numeric(
                decision.get("confidence"),
                default=0,
                scale_fraction_to_pct=True,
            ))))),
            "reasoning": reasoning,
            "position_size_pct": int(max(0, min(100, round(self._coerce_numeric(
                decision.get("position_size_pct"),
                default=20,
            ))))),
            "stop_loss_pct": round(max(0.0, self._coerce_numeric(decision.get("stop_loss_pct"), default=5.0)), 2),
            "take_profit_pct": round(max(0.0, self._coerce_numeric(decision.get("take_profit_pct"), default=10.0)), 2),
            "risk_level": self._normalize_risk_level(decision.get("risk_level")),
            "key_price_levels": self._normalize_key_price_levels(decision.get("key_price_levels")),
        }

        monitor_levels = self._normalize_monitor_levels(decision)
        if monitor_levels:
            normalized["monitor_levels"] = monitor_levels
        return normalized

    def _salvage_decision_fields(self, text: str) -> Optional[Dict[str, Any]]:
        normalized = self._sanitize_json_like_text(text)
        action_match = re.search(r'(?i)(?:^|[,{]\s*)"action"\s*:\s*"?([A-Za-z\u4e00-\u9fff]+)', normalized)
        confidence_match = re.search(r'(?i)(?:^|[,{]\s*)"confidence"\s*:\s*"?([0-9]+(?:\.[0-9]+)?%?)', normalized)
        reasoning_match = re.search(
            r'(?is)(?:^|[,{]\s*)"reasoning"\s*:\s*"?(.*?)(?:"?\s*(?:,\s*"[A-Za-z_][A-Za-z0-9_]*"\s*:|\}\s*$))',
            normalized,
        )
        if not action_match or not confidence_match or not reasoning_match:
            return None
        return {
            "action": action_match.group(1),
            "confidence": confidence_match.group(1),
            "reasoning": reasoning_match.group(1).strip().strip('"').strip(),
        }

    def _decode_decision_text(self, ai_response: str) -> Dict[str, Any]:
        errors: List[str] = []
        for candidate in self._iter_json_candidates(ai_response):
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, str):
                    parsed = json.loads(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"strict_json: {exc}")

            sanitized = self._sanitize_json_like_text(candidate)
            try:
                parsed = json.loads(sanitized)
                if isinstance(parsed, str):
                    parsed = json.loads(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"sanitized_json: {exc}")

            try:
                python_like = self._replace_json_literals_for_python(sanitized)
                parsed = ast.literal_eval(python_like)
                if isinstance(parsed, str):
                    parsed = ast.literal_eval(parsed)
                if isinstance(parsed, dict):
                    return parsed
            except Exception as exc:
                errors.append(f"python_literal: {exc}")

        salvaged = self._salvage_decision_fields(ai_response)
        if salvaged:
            return salvaged

        error_message = errors[-1] if errors else "未找到可解析的JSON对象"
        raise ValueError(error_message)

    def _parse_decision(self, ai_response: str) -> Dict:
        """解析AI决策响应。"""
        try:
            decoded = self._decode_decision_text(ai_response)
            return self._normalize_decision_payload(decoded)
        except Exception as e:
            self.logger.error("解析AI决策失败: %s; response=%s", e, str(ai_response or "")[:300])
            return {
                'action': 'HOLD',
                'confidence': 0,
                'reasoning': f'AI响应解析失败: {str(e)}',
                'position_size_pct': 0,
                'stop_loss_pct': 5.0,
                'take_profit_pct': 10.0,
                'risk_level': 'high',
                'key_price_levels': {},
            }

    @staticmethod
    def _normalize_monitor_levels(decision: Dict) -> Optional[Dict]:
        raw_levels = decision.get("monitor_levels")
        if isinstance(raw_levels, dict):
            candidates = raw_levels
        else:
            candidates = {
                "entry_min": decision.get("entry_min"),
                "entry_max": decision.get("entry_max"),
                "take_profit": decision.get("take_profit"),
                "stop_loss": decision.get("stop_loss"),
            }
            entry_range = decision.get("entry_range")
            if isinstance(entry_range, dict):
                candidates["entry_min"] = candidates.get("entry_min") or entry_range.get("min")
                candidates["entry_max"] = candidates.get("entry_max") or entry_range.get("max")

        normalized: Dict[str, float] = {}
        for key in ("entry_min", "entry_max", "take_profit", "stop_loss"):
            value = candidates.get(key)
            if value in (None, ""):
                return None
            try:
                normalized[key] = float(value)
            except (TypeError, ValueError):
                return None
        return normalized

    def _enforce_action_policy(self, decision: Dict, has_position: bool) -> Dict:
        allowed_actions = {"SELL", "HOLD"} if has_position else {"BUY", "HOLD"}
        action = str(decision.get("action", "HOLD") or "HOLD").upper()
        if action not in allowed_actions:
            decision["action"] = "HOLD"
            original_reasoning = str(decision.get("reasoning") or "").strip()
            if original_reasoning:
                decision["reasoning"] = f"{original_reasoning}\n\n[动作约束] 原始动作 {action} 不在允许集合 {sorted(allowed_actions)} 中，已降级为 HOLD。"
            else:
                decision["reasoning"] = f"原始动作 {action} 不在允许集合 {sorted(allowed_actions)} 中，已降级为 HOLD。"
            decision["risk_level"] = "high"
        else:
            decision["action"] = action
        return decision

