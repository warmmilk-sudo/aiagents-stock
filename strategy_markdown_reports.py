"""Pure markdown exporters for strategy pages (no UI runtime dependency)."""

from __future__ import annotations

from time_utils import format_display_timestamp, local_now_str


def _fmt_text(value):
    return "" if value is None else str(value)


def _fmt_number(value, digits=0):
    if value is None:
        return None
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return None


def generate_sector_markdown_report(result_data: dict) -> str:
    """Generate sector strategy markdown report."""

    current_time = format_display_timestamp(local_now_str(), fmt="%Y年%m月%d日 %H:%M:%S")

    markdown_content = f"""# 智策板块策略分析报告

**AI驱动的多维度板块投资决策支持系统**

---

## 报告信息

- **生成时间**: {current_time}
- **分析周期**: 当日市场数据
- **AI模型**: DeepSeek Multi-Agent System
- **分析维度**: 宏观·板块·资金·情绪

> 本报告由AI系统自动生成，仅供参考，不构成投资建议。投资有风险，决策需谨慎。

---

## 市场概况

本报告基于{_fmt_text(result_data.get('timestamp'))}的实时市场数据，
通过四位AI智能体的多维度分析，为您提供板块投资策略建议。

### 分析师团队:

- **宏观策略师** - 分析宏观经济、政策导向、新闻事件
- **板块诊断师** - 分析板块走势、估值水平、轮动特征
- **资金流向分析师** - 分析主力资金、北向资金流向
- **市场情绪解码员** - 分析市场情绪、热度、赚钱效应

"""

    predictions = result_data.get("final_predictions", {})

    if predictions.get("prediction_text"):
        markdown_content += f"""
## 核心预测

{predictions.get('prediction_text', '')}

"""
    else:
        markdown_content += "## 核心预测\n\n"

        long_short = predictions.get("long_short", {})
        bullish = long_short.get("bullish", [])
        bearish = long_short.get("bearish", [])

        markdown_content += "### 板块多空预测\n\n"

        if bullish:
            markdown_content += "#### 看多板块\n\n"
            for idx, item in enumerate(bullish, 1):
                sector = item.get("sector")
                if sector is None:
                    continue
                markdown_content += f"{idx}. **{sector}**"
                confidence = item.get("confidence")
                if confidence is not None:
                    markdown_content += f" (信心度: {confidence}/10)"
                markdown_content += "\n"
                reason = item.get("reason")
                if reason is not None:
                    markdown_content += f"   - 理由: {reason}\n"
                risk = item.get("risk")
                if risk is not None:
                    markdown_content += f"   - 风险: {risk}\n"
                markdown_content += "\n"

        if bearish:
            markdown_content += "#### 看空板块\n\n"
            for idx, item in enumerate(bearish, 1):
                sector = item.get("sector")
                if sector is None:
                    continue
                markdown_content += f"{idx}. **{sector}**"
                confidence = item.get("confidence")
                if confidence is not None:
                    markdown_content += f" (信心度: {confidence}/10)"
                markdown_content += "\n"
                reason = item.get("reason")
                if reason is not None:
                    markdown_content += f"   - 理由: {reason}\n"
                risk = item.get("risk")
                if risk is not None:
                    markdown_content += f"   - 风险: {risk}\n"
                markdown_content += "\n"

        rotation = predictions.get("rotation", {})
        current_strong = rotation.get("current_strong", [])
        potential = rotation.get("potential", [])
        declining = rotation.get("declining", [])

        markdown_content += "### 板块轮动预测\n\n"

        if current_strong:
            markdown_content += "#### 当前强势板块\n\n"
            for item in current_strong:
                sector = item.get("sector")
                if sector is None:
                    continue
                markdown_content += f"- **{sector}**\n"
                logic = item.get("logic")
                if logic is not None:
                    markdown_content += f"  - 轮动逻辑: {logic}\n"
                time_window = item.get("time_window")
                if time_window is not None:
                    markdown_content += f"  - 时间窗口: {time_window}\n"
                advice = item.get("advice")
                if advice is not None:
                    markdown_content += f"  - 操作建议: {advice}\n"
                markdown_content += "\n"

        if potential:
            markdown_content += "#### 潜力接力板块\n\n"
            for item in potential:
                sector = item.get("sector")
                if sector is None:
                    continue
                markdown_content += f"- **{sector}**\n"
                logic = item.get("logic")
                if logic is not None:
                    markdown_content += f"  - 轮动逻辑: {logic}\n"
                time_window = item.get("time_window")
                if time_window is not None:
                    markdown_content += f"  - 时间窗口: {time_window}\n"
                advice = item.get("advice")
                if advice is not None:
                    markdown_content += f"  - 操作建议: {advice}\n"
                markdown_content += "\n"

        if declining:
            markdown_content += "#### 衰退板块\n\n"
            for item in declining:
                sector = item.get("sector")
                if sector is None:
                    continue
                markdown_content += f"- **{sector}**\n"
                logic = item.get("logic")
                if logic is not None:
                    markdown_content += f"  - 轮动逻辑: {logic}\n"
                time_window = item.get("time_window")
                if time_window is not None:
                    markdown_content += f"  - 时间窗口: {time_window}\n"
                advice = item.get("advice")
                if advice is not None:
                    markdown_content += f"  - 操作建议: {advice}\n"
                markdown_content += "\n"

        heat = predictions.get("heat", {})
        hottest = heat.get("hottest", [])
        heating = heat.get("heating", [])
        cooling = heat.get("cooling", [])

        markdown_content += "### 板块热度排行\n\n"

        if hottest:
            markdown_content += "#### 最热板块\n\n| 排名 | 板块 | 热度评分 | 趋势 | 持续性 |\n|------|------|----------|------|--------|\n"
            for idx, item in enumerate(hottest[:10], 1):
                sector = item.get("sector")
                score = item.get("score")
                trend = item.get("trend")
                sustainability = item.get("sustainability")
                if None in (sector, score, trend, sustainability):
                    continue
                markdown_content += f"| {idx} | {sector} | {score} | {trend} | {sustainability} |\n"
            markdown_content += "\n"

        if heating:
            markdown_content += "#### 升温板块\n\n"
            for idx, item in enumerate(heating[:5], 1):
                sector = item.get("sector")
                score = item.get("score")
                if sector is None or score is None:
                    continue
                markdown_content += f"{idx}. {sector} (评分: {score})\n"
            markdown_content += "\n"

        if cooling:
            markdown_content += "#### 降温板块\n\n"
            for idx, item in enumerate(cooling[:5], 1):
                sector = item.get("sector")
                score = item.get("score")
                if sector is None or score is None:
                    continue
                markdown_content += f"{idx}. {sector} (评分: {score})\n"
            markdown_content += "\n"

        summary = predictions.get("summary", {})
        if summary:
            markdown_content += "### 策略总结\n\n"
            if summary.get("market_view"):
                markdown_content += f"**市场观点:** {summary.get('market_view', '')}\n\n"
            if summary.get("key_opportunity"):
                markdown_content += f"**核心机会:** {summary.get('key_opportunity', '')}\n\n"
            if summary.get("major_risk"):
                markdown_content += f"**主要风险:** {summary.get('major_risk', '')}\n\n"
            if summary.get("strategy"):
                markdown_content += f"**整体策略:** {summary.get('strategy', '')}\n\n"

    agents_analysis = result_data.get("agents_analysis", {})
    if agents_analysis:
        markdown_content += "## AI智能体分析\n\n"
        for _key, agent_data in agents_analysis.items():
            agent_name = agent_data.get("agent_name")
            if agent_name is None:
                continue
            agent_role = agent_data.get("agent_role")
            focus_areas_list = agent_data.get("focus_areas")
            focus_areas = ", ".join(focus_areas_list) if focus_areas_list else ""
            analysis = agent_data.get("analysis")
            if analysis is None:
                continue
            markdown_content += f"### {agent_name}\n\n"
            if agent_role:
                markdown_content += f"- **职责**: {agent_role}\n"
            if focus_areas:
                markdown_content += f"- **关注领域**: {focus_areas}\n\n"
            markdown_content += f"{analysis}\n\n"
            markdown_content += "---\n\n"

    comprehensive_report = result_data.get("comprehensive_report", "")
    if comprehensive_report:
        markdown_content += "## 综合研判\n\n"
        markdown_content += f"{comprehensive_report}\n\n"

    markdown_content += """
---

*报告由智策AI系统自动生成*
"""
    return markdown_content


def generate_longhubang_markdown_report(result_data: dict) -> str:
    """Generate longhubang markdown report."""

    current_time = local_now_str("%Y年%m月%d日 %H:%M:%S")
    markdown_content = f"""# 智瞰龙虎榜分析报告

**AI驱动的龙虎榜多维度分析系统**

---

## 📊 报告概览

- **生成时间**: {current_time}
- **数据记录**: {result_data.get('data_info', {}).get('total_records')} 条
- **涉及股票**: {result_data.get('data_info', {}).get('total_stocks')} 只
- **涉及游资**: {result_data.get('data_info', {}).get('total_youzi')} 个
- **AI分析师**: 5位专业分析师团队
- **分析模型**: DeepSeek AI Multi-Agent System

> ⚠️ 本报告由AI系统基于龙虎榜公开数据自动生成，仅供参考，不构成投资建议。市场有风险，投资需谨慎。

---

## 📈 数据概况

本次分析共涵盖 **{result_data.get('data_info', {}).get('total_records')}** 条龙虎榜记录，
涉及 **{result_data.get('data_info', {}).get('total_stocks')}** 只股票和 
**{result_data.get('data_info', {}).get('total_youzi')}** 个游资席位。

"""

    summary = result_data.get("data_info", {}).get("summary", {})
    markdown_content += "\n### 💰 资金概况\n\n"
    total_buy_amount = _fmt_number(summary.get("total_buy_amount"), 2)
    total_sell_amount = _fmt_number(summary.get("total_sell_amount"), 2)
    total_net_inflow = _fmt_number(summary.get("total_net_inflow"), 2)
    if total_buy_amount is not None:
        markdown_content += f"- **总买入金额**: {total_buy_amount} 元\n"
    if total_sell_amount is not None:
        markdown_content += f"- **总卖出金额**: {total_sell_amount} 元\n"
    if total_net_inflow is not None:
        markdown_content += f"- **净流入金额**: {total_net_inflow} 元\n"
    markdown_content += "\n"

    if summary.get("top_youzi"):
        markdown_content += "### 🏆 活跃游资 TOP10\n\n| 排名 | 游资名称 | 净流入金额(元) |\n|------|----------|---------------|\n"
        for idx, (name, amount) in enumerate(list(summary["top_youzi"].items())[:10], 1):
            markdown_content += f"| {idx} | {name} | {amount:,.2f} |\n"
        markdown_content += "\n"

    if summary.get("top_stocks"):
        markdown_content += "### 📈 资金净流入 TOP20 股票\n\n| 排名 | 股票代码 | 股票名称 | 净流入金额(元) |\n|------|----------|----------|---------------|\n"
        for idx, stock in enumerate(summary["top_stocks"][:20], 1):
            code = stock.get("code")
            name = stock.get("name")
            net_inflow = _fmt_number(stock.get("net_inflow"), 2)
            if None in (code, name, net_inflow):
                continue
            markdown_content += f"| {idx} | {code} | {name} | {net_inflow} |\n"
        markdown_content += "\n"

    if summary.get("hot_concepts"):
        markdown_content += "### 🔥 热门概念 TOP15\n\n"
        for idx, (concept, count) in enumerate(list(summary["hot_concepts"].items())[:15], 1):
            markdown_content += f"{idx}. {concept} ({count}次)  \n"
        markdown_content += "\n"

    recommended = result_data.get("recommended_stocks", [])
    if recommended:
        markdown_content += f"""
## 🎯 AI推荐股票

基于5位AI分析师的综合分析，系统识别出以下 **{len(recommended)}** 只潜力股票，
这些股票在资金流向、游资关注度、题材热度等多个维度表现突出。

### 推荐股票清单

| 排名 | 股票代码 | 股票名称 | 净流入金额 | 确定性 | 持有周期 |
|------|----------|----------|------------|--------|----------|
"""
        for stock in recommended[:10]:
            rank = stock.get("rank")
            code = stock.get("code")
            name = stock.get("name")
            net_inflow = _fmt_number(stock.get("net_inflow"), 0)
            confidence = stock.get("confidence")
            hold_period = stock.get("hold_period")
            if None in (rank, code, name, net_inflow, confidence, hold_period):
                continue
            markdown_content += f"| {rank} | {code} | {name} | {net_inflow} | {confidence} | {hold_period} |\n"

        markdown_content += "\n### 推荐理由详解\n\n"
        for stock in recommended[:5]:
            rank = stock.get("rank")
            name = stock.get("name")
            code = stock.get("code")
            if None in (rank, name, code):
                continue
            markdown_content += f"**{rank}. {name} ({code})**\n\n"
            reason = stock.get("reason")
            if reason is not None:
                markdown_content += f"- 推荐理由: {reason}\n"
            confidence = stock.get("confidence")
            if confidence is not None:
                markdown_content += f"- 确定性: {confidence}\n"
            hold_period = stock.get("hold_period")
            if hold_period is not None:
                markdown_content += f"- 持有周期: {hold_period}\n"
            markdown_content += "\n"

    agents_analysis = result_data.get("agents_analysis", {})
    if agents_analysis:
        markdown_content += "## 🤖 AI分析师报告\n\n"
        markdown_content += "本报告由5位AI专业分析师从不同维度进行分析，综合形成投资建议：\n\n"
        markdown_content += "- **游资行为分析师** - 分析游资操作特征和意图\n"
        markdown_content += "- **个股潜力分析师** - 挖掘次日大概率上涨的股票\n"
        markdown_content += "- **题材追踪分析师** - 识别热点题材和轮动机会\n"
        markdown_content += "- **风险控制专家** - 识别高风险股票和市场陷阱\n"
        markdown_content += "- **首席策略师** - 综合研判并给出最终建议\n\n"

        agent_titles = {
            "youzi": "游资行为分析师",
            "stock": "个股潜力分析师",
            "theme": "题材追踪分析师",
            "risk": "风险控制专家",
            "chief": "首席策略师综合研判",
        }
        for agent_key, agent_title in agent_titles.items():
            agent_data = agents_analysis.get(agent_key, {})
            if not agent_data:
                continue
            markdown_content += f"### {agent_title}\n\n"
            analysis_text = agent_data.get("analysis")
            if analysis_text is None:
                continue
            analysis_text = analysis_text.replace("\n", "\n\n")
            markdown_content += f"{analysis_text}\n\n"

    markdown_content += """
---

*报告由智瞰龙虎AI系统自动生成*
"""
    return markdown_content
