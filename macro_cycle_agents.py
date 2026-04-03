"""
宏观周期分析 - AI智能体模块
包含四位专业分析师：康波周期分析师、美林时钟分析师、中国政策分析师、首席宏观策略师
"""

from deepseek_client import DeepSeekClient
from model_routing import ModelTier
from prompt_registry import build_messages
from typing import Dict, Any
import time


class MacroCycleAgents:
    """宏观周期AI智能体集合"""

    def __init__(self, model=None, lightweight_model=None, reasoning_model=None):
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.deepseek_client = DeepSeekClient(
            model=model,
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        print(f"[宏观周期] AI智能体系统初始化 (模型配置: {self.deepseek_client.model_selection})")

    def kondratieff_wave_agent(self, macro_data_text: str) -> Dict[str, Any]:
        """
        康波周期分析师 - 判断当前处于康德拉季耶夫长波的哪个阶段

        职责：
        - 分析当前技术革命阶段（第五轮信息技术康波的位置）
        - 判断回升/繁荣/衰退/萧条四阶段中的位置
        - 分析大宗商品与康波的关系
        - 给出战略性资产配置方向
        """
        print("🌊 康波周期分析师正在分析...")
        time.sleep(1)

        messages = build_messages(
            "macro_cycle/kondratieff_wave.system.txt",
            "macro_cycle/kondratieff_wave.user.txt",
            macro_data_text=macro_data_text,
        )

        analysis = self.deepseek_client.call_api(
            messages,
            max_tokens=6000,
            tier=ModelTier.REASONING,
        )
        print("  ✓ 康波周期分析师分析完成")

        return {
            "agent_name": "康波周期分析师",
            "agent_icon": "🌊",
            "agent_role": "判断当前处于康德拉季耶夫长波（50-60年大周期）的哪个阶段，给出战略性资产配置方向",
            "analysis": analysis,
            "focus_areas": ["康波定位", "技术革命", "大宗商品超级周期", "战略资产配置"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def merrill_lynch_clock_agent(self, macro_data_text: str) -> Dict[str, Any]:
        """
        美林投资时钟分析师 - 判断当前处于美林时钟的哪个象限

        职责：
        - 根据经济增长和通胀两个维度判断象限
        - 结合中国特色（政策第三维度）
        - 给出中短期资产配置建议
        """
        print("⏰ 美林时钟分析师正在分析...")
        time.sleep(1)

        messages = build_messages(
            "macro_cycle/merrill_lynch_clock.system.txt",
            "macro_cycle/merrill_lynch_clock.user.txt",
            macro_data_text=macro_data_text,
        )

        analysis = self.deepseek_client.call_api(
            messages,
            max_tokens=6000,
            tier=ModelTier.REASONING,
        )
        print("  ✓ 美林时钟分析师分析完成")

        return {
            "agent_name": "美林时钟分析师",
            "agent_icon": "⏰",
            "agent_role": "判断当前处于美林投资时钟的哪个象限（3-5年中短周期），给出资产配置建议",
            "analysis": analysis,
            "focus_areas": ["经济增长", "通胀水平", "政策方向", "资产配置"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def china_policy_agent(self, macro_data_text: str) -> Dict[str, Any]:
        """
        中国政策分析师 - 分析中国特色政策环境

        职责：
        - 分析当前政策环境
        - 评估政策对周期的影响
        - 识别政策驱动的投资机会
        """
        print("🏛️ 中国政策分析师正在分析...")
        time.sleep(1)

        messages = build_messages(
            "macro_cycle/china_policy.system.txt",
            "macro_cycle/china_policy.user.txt",
            macro_data_text=macro_data_text,
        )

        analysis = self.deepseek_client.call_api(
            messages,
            max_tokens=5000,
            tier=ModelTier.REASONING,
        )
        print("  ✓ 中国政策分析师分析完成")

        return {
            "agent_name": "中国政策分析师",
            "agent_icon": "🏛️",
            "agent_role": "分析中国特色政策环境，评估政策对周期的影响，识别政策驱动的投资机会",
            "analysis": analysis,
            "focus_areas": ["货币政策", "财政政策", "产业政策", "房地产政策", "政策拐点"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def chief_macro_strategist_agent(self, kondratieff_report: str, merrill_report: str, policy_report: str, macro_data_text: str) -> Dict[str, Any]:
        """
        首席宏观策略师 - 综合三位分析师的观点，形成最终策略

        职责：
        - 整合康波、美林时钟、政策三个维度
        - 构建"周期仪表盘"
        - 给出最终的综合建议
        """
        print("👔 首席宏观策略师正在综合研判...")
        time.sleep(1)

        messages = build_messages(
            "macro_cycle/chief_macro_strategist.system.txt",
            "macro_cycle/chief_macro_strategist.user.txt",
            kondratieff_report=kondratieff_report,
            merrill_report=merrill_report,
            policy_report=policy_report,
        )

        analysis = self.deepseek_client.call_api(
            messages,
            max_tokens=6000,
            tier=ModelTier.REASONING,
        )
        print("  ✓ 首席宏观策略师综合研判完成")

        return {
            "agent_name": "首席宏观策略师",
            "agent_icon": "👔",
            "agent_role": "整合康波周期、美林时钟、中国政策三维分析，构建周期仪表盘，给出最终综合策略",
            "analysis": analysis,
            "focus_areas": ["周期仪表盘", "综合资产配置", "双指针共振", "分人群建议"],
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }


# 测试
if __name__ == "__main__":
    print("=" * 60)
    print("测试宏观周期AI智能体系统")
    print("=" * 60)
    agents = MacroCycleAgents()
    print(f"模型: {agents.model}")
    print("初始化完成")
