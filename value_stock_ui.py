#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
低估值策略UI模块
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from value_stock_selector import ValueStockSelector
from value_stock_strategy import ValueStockStrategy
from ui_shared import get_dataframe_height
from ui_analysis_task_utils import (
    consume_finished_ui_analysis_task,
    get_ui_analysis_button_state,
    render_ui_analysis_task_live_card,
    start_ui_analysis_task,
)


VALUE_STOCK_TASK_TYPE = "value_stock_selection"
VALUE_STOCK_TASK_DONE_KEY = "value_stock_selection_last_handled_task"


@st.fragment(run_every=1.0)
def _render_value_stock_task_fragment():
    render_ui_analysis_task_live_card(
        task_type=VALUE_STOCK_TASK_TYPE,
        title="低估值选股任务状态",
        state_prefix="value_stock_selection_live",
    )


def _run_value_stock_selection_task(
    *,
    top_n: int,
    max_pe: float,
    max_pb: float,
    min_dividend_yield: float,
    max_debt_ratio: float,
    min_float_cap_yi: float,
    max_float_cap_yi: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
    filter_summary: str,
    report_progress,
):
    report_progress(current=0, total=2, message="正在拉取低估值候选数据...")
    selector = ValueStockSelector()
    success, stocks_df, message = selector.get_value_stocks(
        top_n=top_n,
        max_pe=max_pe,
        max_pb=max_pb,
        min_dividend_yield=min_dividend_yield,
        max_debt_ratio=max_debt_ratio,
        min_float_cap_yi=min_float_cap_yi or None,
        max_float_cap_yi=max_float_cap_yi or None,
        sort_by=sort_by,
        exclude_st=exclude_st,
        exclude_kcb=exclude_kcb,
        exclude_cyb=exclude_cyb,
    )
    if not success or stocks_df is None:
        raise RuntimeError(message or "低估值选股失败")

    report_progress(current=2, total=2, message="低估值选股完成，正在同步结果...")
    return {
        "stocks_df": stocks_df,
        "message": message,
        "filter_summary": filter_summary,
        "selected_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def build_value_stock_filter_summary(
    *,
    max_pe: float,
    max_pb: float,
    min_dividend_yield: float,
    max_debt_ratio: float,
    min_float_cap_yi: float,
    max_float_cap_yi: float,
    sort_by: str,
    exclude_st: bool,
    exclude_kcb: bool,
    exclude_cyb: bool,
) -> str:
    """Build a compact filter summary for the UI."""
    parts = [
        f"PE≤{max_pe:.1f}",
        f"PB≤{max_pb:.1f}",
        f"股息率≥{min_dividend_yield:.1f}%",
        f"资产负债率≤{max_debt_ratio:.1f}%",
        sort_by,
    ]
    if min_float_cap_yi > 0:
        parts.append(f"流通市值≥{min_float_cap_yi:.0f}亿")
    if max_float_cap_yi > 0:
        parts.append(f"流通市值≤{max_float_cap_yi:.0f}亿")
    if exclude_st:
        parts.append("剔除ST")
    if exclude_kcb:
        parts.append("剔除科创板")
    if exclude_cyb:
        parts.append("剔除创业板")
    return "，".join(parts)


def display_value_stock():
    """显示低估值选股界面"""

    with st.expander("选股策略说明", expanded=False):
        st.markdown("""
        基于视频 [头号投资法则](https://www.bilibili.com/video/BV1eJfxBrEjZ)

        低PE + 低PB + 高股息 + 低负债 — 寻找被市场低估的优质标的

        **筛选条件**：
        - 市盈率（PE）≤ 20
        - 市净率（PB）≤ 1.5
        - 股息率 ≥ 1%
        - 资产负债率 ≤ 30%
        - 非ST股票
        - 非科创板
        - 非创业板
        - 按流通市值由小到大排名

        **量化交易策略**：
        - 资金量：100万元
        - 买入时机：开盘买入
        - 单股最大仓位：30%
        - 最大持股数：4只
        - 每日最多买入：2只
        - 卖出条件①：持股满30天到期卖出
        - 卖出条件②：RSI超买（>70）卖出
        """)

    col_top_n, col_hint = st.columns([2, 1])
    with col_top_n:
        top_n = st.slider(
            "筛选数量",
            min_value=5,
            max_value=20,
            value=10,
            step=1,
            help="选择展示的股票数量",
            key="value_stock_top_n",
        )
    with col_hint:
        st.caption(f"默认返回前 {top_n} 只股票。")

    with st.expander("高级筛选参数", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            max_pe = st.number_input("最高PE", min_value=1.0, max_value=200.0, value=20.0, step=1.0)
        with col2:
            max_pb = st.number_input("最高PB", min_value=0.1, max_value=20.0, value=1.5, step=0.1)
        with col3:
            min_dividend_yield = st.number_input("最低股息率(%)", min_value=0.0, max_value=20.0, value=1.0, step=0.1)
        with col4:
            max_debt_ratio = st.number_input("最高资产负债率(%)", min_value=0.0, max_value=100.0, value=30.0, step=1.0)

        col5, col6, col7, col8 = st.columns(4)
        with col5:
            min_float_cap_yi = st.number_input(
                "最低流通市值(亿)",
                min_value=0.0,
                max_value=100000.0,
                value=0.0,
                step=10.0,
                help="0 表示不限制",
            )
        with col6:
            max_float_cap_yi = st.number_input(
                "最高流通市值(亿)",
                min_value=0.0,
                max_value=100000.0,
                value=0.0,
                step=10.0,
                help="0 表示不限制",
            )
        with col7:
            sort_by = st.selectbox(
                "排序方式",
                ["流通市值升序", "PE升序", "PB升序", "股息率降序", "资产负债率升序"],
            )
        with col8:
            exclude_st = st.checkbox("剔除ST", value=True)

        col9, col10 = st.columns(2)
        with col9:
            exclude_kcb = st.checkbox("剔除科创板", value=True)
        with col10:
            exclude_cyb = st.checkbox("剔除创业板", value=True)

    filter_summary = build_value_stock_filter_summary(
        max_pe=max_pe,
        max_pb=max_pb,
        min_dividend_yield=min_dividend_yield,
        max_debt_ratio=max_debt_ratio,
        min_float_cap_yi=min_float_cap_yi,
        max_float_cap_yi=max_float_cap_yi,
        sort_by=sort_by,
        exclude_st=exclude_st,
        exclude_kcb=exclude_kcb,
        exclude_cyb=exclude_cyb,
    )
    st.caption(f"当前筛选：{filter_summary}")
    _render_value_stock_task_fragment()

    finished_task = consume_finished_ui_analysis_task(VALUE_STOCK_TASK_TYPE, VALUE_STOCK_TASK_DONE_KEY)
    if finished_task:
        if finished_task.get("status") == "success":
            payload = finished_task.get("result") or {}
            st.session_state.value_stocks = payload.get("stocks_df")
            st.session_state.value_stock_selector = None
            st.session_state.value_stock_selected_time = payload.get("selected_time")
            st.session_state.value_stock_filter_summary = payload.get("filter_summary")
            st.success(payload.get("message") or "低估值选股完成。")
        else:
            st.error(f"低估值选股失败：{finished_task.get('error', '未知错误')}")

    action_label, action_disabled, action_help = get_ui_analysis_button_state(
        VALUE_STOCK_TASK_TYPE,
        "开始选股",
    )
    if st.button(
        action_label,
        type="primary",
        width='content',
        key="value_stock_start",
        disabled=action_disabled,
        help=action_help,
    ):
        try:
            start_ui_analysis_task(
                task_type=VALUE_STOCK_TASK_TYPE,
                label="低估值选股",
                runner=lambda _task_id, report_progress: _run_value_stock_selection_task(
                    top_n=top_n,
                    max_pe=max_pe,
                    max_pb=max_pb,
                    min_dividend_yield=min_dividend_yield,
                    max_debt_ratio=max_debt_ratio,
                    min_float_cap_yi=min_float_cap_yi,
                    max_float_cap_yi=max_float_cap_yi,
                    sort_by=sort_by,
                    exclude_st=exclude_st,
                    exclude_kcb=exclude_kcb,
                    exclude_cyb=exclude_cyb,
                    filter_summary=filter_summary,
                    report_progress=report_progress,
                ),
                metadata={"top_n": top_n, "filter_summary": filter_summary},
            )
            st.info("已提交后台分析任务，可切换页面，返回后会自动同步进度和结果。")
            st.rerun()
        except RuntimeError as exc:
            st.warning(str(exc))

    # 显示选股结果
    if 'value_stocks' in st.session_state:
        display_stock_results(
            st.session_state.value_stocks,
            st.session_state.get('value_stock_selector')
        )


def display_stock_results(stocks_df: pd.DataFrame, selector):
    """显示选股结果"""

    selection_time = st.session_state.get("value_stock_selected_time")
    filter_summary = st.session_state.get("value_stock_filter_summary")
    if selection_time or filter_summary:
        summary_parts = []
        if selection_time:
            summary_parts.append(f"选股时间: {selection_time}")
        if filter_summary:
            summary_parts.append(f"条件: {filter_summary}")
        st.caption(" | ".join(summary_parts))

    st.markdown("---")
    st.markdown("## 选股结果")

    # 统计信息
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("筛选数量", f"{len(stocks_df)} 只")

    with col2:
        pe_col = None
        for pattern in ['市盈率', '市盈率(动态)']:
            matching = [col for col in stocks_df.columns if pattern in col]
            if matching:
                pe_col = matching[0]
                break
        if pe_col:
            valid = pd.to_numeric(stocks_df[pe_col], errors='coerce').dropna()
            if len(valid) > 0:
                st.metric("平均PE", f"{valid.mean():.1f}")
            else:
                st.metric("平均PE", "-")
        else:
            st.metric("平均PE", "-")

    with col3:
        pb_col = None
        matching = [col for col in stocks_df.columns if '市净率' in col]
        if matching:
            pb_col = matching[0]
            valid = pd.to_numeric(stocks_df[pb_col], errors='coerce').dropna()
            if len(valid) > 0:
                st.metric("平均PB", f"{valid.mean():.2f}")
            else:
                st.metric("平均PB", "-")
        else:
            st.metric("平均PB", "-")

    with col4:
        div_col = None
        matching = [col for col in stocks_df.columns if '股息率' in col]
        if matching:
            div_col = matching[0]
            valid = pd.to_numeric(stocks_df[div_col], errors='coerce').dropna()
            if len(valid) > 0:
                st.metric("平均股息率", f"{valid.mean():.2f}%")
            else:
                st.metric("平均股息率", "-")
        else:
            st.metric("平均股息率", "-")

    st.markdown("---")

    # 显示股票列表
    st.markdown("### 精选低估值股票")

    for idx, row in stocks_df.iterrows():
        code = row.get('股票代码', 'N/A')
        name = row.get('股票简称', 'N/A')

        # 获取关键指标用于标题
        pe_val = ''
        for pattern in ['市盈率', '市盈率(动态)']:
            matching = [col for col in stocks_df.columns if pattern in col]
            if matching:
                v = row.get(matching[0])
                if v is not None and not pd.isna(v):
                    try:
                        pe_val = f" PE:{float(v):.1f}"
                    except:
                        pass
                break

        pb_val = ''
        matching = [col for col in stocks_df.columns if '市净率' in col]
        if matching:
            v = row.get(matching[0])
            if v is not None and not pd.isna(v):
                try:
                    pb_val = f" PB:{float(v):.2f}"
                except:
                    pass

        with st.expander(
            f"【第{idx+1}名】{code} - {name}{pe_val}{pb_val}",
            expanded=(idx < 3)
        ):
            display_stock_detail(row, stocks_df)

    # 完整数据表格
    st.markdown("---")
    st.markdown("### 完整数据表格")

    # 选择关键列
    display_cols = ['股票代码', '股票简称']
    for pattern in ['最新价', '股价']:
        matching = [col for col in stocks_df.columns if pattern in col]
        if matching:
            display_cols.append(matching[0])
            break
    for pattern in ['市盈率', '市净率', '股息率', '资产负债率', '流通市值', '所属行业']:
        matching = [col for col in stocks_df.columns if pattern in col]
        if matching:
            display_cols.append(matching[0])

    final_cols = [col for col in display_cols if col in stocks_df.columns]

    if final_cols:
        st.dataframe(
            stocks_df[final_cols],
            width='content',
            height=get_dataframe_height(len(stocks_df[final_cols]), max_rows=40),
        )

        csv = stocks_df[final_cols].to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="下载股票列表CSV",
            data=csv,
            file_name=f"value_stock_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            key="value_csv_download"
        )

    # 量化交易模拟
    st.markdown("---")
    display_strategy_simulation(stocks_df, selector)


def display_stock_detail(row: pd.Series, df: pd.DataFrame):
    """显示单个股票详情"""

    def is_valid(value):
        if value is None:
            return False
        if isinstance(value, float) and pd.isna(value):
            return False
        if isinstance(value, str) and value.strip() in ('', 'N/A', 'nan', 'None'):
            return False
        return True

    def fmt(value, suffix=''):
        if not is_valid(value):
            return "-"
        try:
            return f"{float(value):.2f}{suffix}"
        except:
            return str(value) + suffix

    # 基本估值数据
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        for p in ['市盈率', '市盈率(动态)']:
            m = [c for c in df.columns if p in c]
            if m:
                st.metric("市盈率(PE)", fmt(row.get(m[0])))
                break

    with col2:
        m = [c for c in df.columns if '市净率' in c]
        if m:
            st.metric("市净率(PB)", fmt(row.get(m[0])))

    with col3:
        m = [c for c in df.columns if '股息率' in c]
        if m:
            st.metric("股息率", fmt(row.get(m[0]), '%'))

    with col4:
        m = [c for c in df.columns if '资产负债率' in c]
        if m:
            st.metric("资产负债率", fmt(row.get(m[0]), '%'))

    # 补充信息
    st.markdown("**其他指标**：")
    info_parts = []
    for pattern in ['最新价', '股价', '流通市值', '总市值', '所属行业', '涨跌幅']:
        m = [c for c in df.columns if pattern in c]
        if m:
            val = row.get(m[0])
            if is_valid(val):
                info_parts.append(f"**{pattern}**: {val}")
    if info_parts:
        st.markdown(" | ".join(info_parts))


def display_strategy_simulation(stocks_df: pd.DataFrame, selector):
    """显示量化交易策略模拟"""

    st.markdown("## 策略模拟")

    st.info("""
    **策略规则**：
    - **买入**：开盘价买入，单股最大仓位30%，每日最多买2只
    - **卖出条件①**：持股满30天，到期自动卖出
    - **卖出条件②**：RSI(14) > 70 超买，触发卖出
    - **最大持股**：4只
    - **初始资金**：100万元
    """)

    col1, col2 = st.columns(2)

    with col1:
        if st.button("开始策略模拟", type="primary", width='content', key="value_sim_start"):
            st.session_state.show_value_strategy_sim = True

    with col2:
        pass

    if st.session_state.get('show_value_strategy_sim'):
        run_strategy_simulation(stocks_df)


def run_strategy_simulation(stocks_df: pd.DataFrame):
    """运行策略模拟"""

    st.markdown("---")
    st.markdown("### 策略模拟执行")

    strategy = ValueStockStrategy(initial_capital=1000000.0)

    # 模拟买入
    st.markdown("#### 1️⃣ 模拟买入信号")

    buy_results = []
    current_date = datetime.now().strftime("%Y-%m-%d")

    for idx, row in stocks_df.head(strategy.max_daily_buy).iterrows():
        code = str(row.get('股票代码', '')).split('.')[0]
        name = row.get('股票简称', 'N/A')

        # 尝试获取价格
        price = 0
        for p in ['最新价', '股价']:
            m = [c for c in stocks_df.columns if p in c]
            if m:
                try:
                    price = float(row.get(m[0], 0))
                except:
                    pass
                if price > 0:
                    break

        if price > 0:
            success, message, trade = strategy.buy(code, name, price, current_date)
            buy_results.append({
                'success': success,
                'message': message,
                'trade': trade
            })

    for result in buy_results:
        if result['success']:
            st.success(result['message'])
        else:
            st.warning(result['message'])

    # RSI检查
    st.markdown("---")
    st.markdown("#### 2️⃣ RSI卖出信号检测")

    with st.spinner("正在计算RSI指标..."):
        for code, pos in list(strategy.positions.items()):
            rsi = strategy.calculate_rsi(code)
            if rsi is not None:
                if rsi > strategy.rsi_overbought:
                    st.warning(f"{code} {pos['name']} RSI={rsi} > {strategy.rsi_overbought}，触发超买卖出信号。")
                else:
                    st.info(f"ℹ️ {code} {pos['name']} RSI={rsi}，正常范围")
            else:
                st.info(f"ℹ️ {code} {pos['name']} RSI计算中...")

    # 显示持仓
    st.markdown("---")
    st.markdown("#### 3️⃣ 当前持仓")

    positions = strategy.get_positions()
    if positions:
        positions_df = pd.DataFrame(positions)
        st.dataframe(positions_df, width='content')
    else:
        st.info("暂无持仓")

    # 显示账户摘要
    st.markdown("---")
    st.markdown("#### 4️⃣ 账户摘要")

    summary = strategy.get_portfolio_summary()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("初始资金", f"{summary['initial_capital']:,.0f} 元")
    with col2:
        st.metric("可用资金", f"{summary['available_cash']:,.0f} 元")
    with col3:
        st.metric("持仓市值", f"{summary['position_value']:,.0f} 元")
    with col4:
        st.metric("总资产", f"{summary['total_assets']:,.0f} 元")

    st.markdown("---")
    st.markdown("#### 策略说明")
    st.markdown("""
    **后续操作**：
    1. **持有期管理**：系统跟踪每只股票的持有天数（30天到期）
    2. **RSI监测**：每日收盘后计算RSI(14)
       - RSI > 70：超买信号，提示卖出
       - RSI < 30：超卖信号（可作为加仓参考）
    3. **轮动买入**：卖出后释放资金，继续买入新的低估值股票

    **风险提示**：
    - 本策略为模拟演示，实际交易存在滑点、手续费等成本。
    - 低估值不代表没有风险，价值陷阱仍需警惕。
    - 请谨慎评估风险，理性投资。
    """)


# 主入口
if __name__ == "__main__":
    display_value_stock()
