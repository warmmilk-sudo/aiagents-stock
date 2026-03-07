import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json
from datetime import datetime
import time
import base64
import os
from typing import Optional
import config
import streamlit.components.v1 as components
from model_config import get_lightweight_model_options, get_reasoning_model_options

from stock_data import StockDataFetcher
from ai_agents import StockAnalysisAgents
from pdf_generator import display_pdf_export_section
from database import db
from monitor_manager import display_monitor_manager, get_monitor_summary
from monitor_service import monitor_service
from notification_service import notification_service
from config_manager import config_manager
from main_force_ui import display_main_force_selector
from sector_strategy_ui import display_sector_strategy
from longhubang_ui import display_longhubang
from smart_monitor_ui import smart_monitor_ui
from news_flow_ui import display_news_flow_monitor

# 页面配置
st.set_page_config(
    page_title="复合多AI智能体股票团队分析系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 注入JS用于隐藏侧边栏
def collapse_sidebar():
    """使用JS注入自动折叠侧边栏"""
    components.html("""
        <script>
        const doc = window.parent.document;
        const sidebar = doc.querySelector('[data-testid="stSidebar"]');
        if (sidebar && sidebar.getAttribute("aria-expanded") === "true") {
            const btn = doc.querySelector('[data-testid="stSidebarCollapseButton"]');
            if (btn) btn.click();
        }
        </script>
    """, height=0, width=0)

# 自定义CSS样式 - 极简专业版 (Dark Mode Adapted)
st.markdown("""
<style>
    /* 全局极简风格 */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* 弱化 Streamlit 默认头部 */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* 干净的标签页 */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
        background: transparent;
        padding: 0;
        border-bottom: 1px solid var(--secondary-background-color);
    }
    .stTabs [data-baseweb="tab"] {
        height: 3rem;
        font-weight: 500;
        color: rgba(255,255,255,0.6);
        background: transparent;
        border: none;
        padding: 0 1rem;
    }
    .stTabs [aria-selected="true"] {
        color: var(--text-color) !important;
        border-bottom: 2px solid var(--primary-color) !important;
        background: transparent !important;
        box-shadow: none !important;
    }
    
    /* 专业卡片样式 */
    .agent-card, .metric-card, .decision-card, .warning-card {
        background: var(--secondary-background-color);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 1.25rem;
        margin: 0.75rem 0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.2);
    }
    .agent-card { border-left: 4px solid #3b82f6; }
    .decision-card { border-left: 4px solid #10b981; }
    .warning-card { border-left: 4px solid #f59e0b; }
    
    /* 表单控件极简设计 */
    .stTextInput>div>div>input, .stSelectbox>div>div>div {
        border-radius: 6px;
        border: 1px solid rgba(255,255,255,0.2);
    }
    
    /* 隐藏输入框的回车提示 */
    div[data-testid="InputInstructions"] {
        display: none !important;
    }
    
    /* 按钮样式 */
    .stButton>button {
        background: var(--secondary-background-color);
        color: var(--text-color);
        border: 1px solid rgba(255,255,255,0.2);
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.2s;
    }
    .stButton>button:hover {
        border-color: var(--primary-color);
    }
    
    .streamlit-expanderHeader {
        background: transparent;
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 6px;
        color: var(--text-color);
        font-weight: 500;
    }
    
    .dataframe {
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 6px;
    }
    
    .js-plotly-plot {
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 0.5rem;
    }
    
    .site-filing {
        padding-top: 1.25rem;
        text-align: center;
        font-size: 0.72rem;
        line-height: 1.2;
        color: rgba(255,255,255,0.42);
    }
    .site-filing a {
        color: rgba(255,255,255,0.4);
        text-decoration: none;
    }
    .site-filing a:hover {
        color: rgba(255,255,255,0.8);
        text-decoration: underline;
    }
        color: #64748b;
        text-decoration: underline;
    }
    
    @media (max-width: 768px) {
        .block-container { padding-top: 1rem; }
    }
</style>
""", unsafe_allow_html=True)

def render_site_filing() -> None:
    """Render ICP filing info as a small line of text."""
    import html
    icp_number = (config.ICP_NUMBER or "").strip()
    if not icp_number:
        return

    icp_link = (getattr(config, "ICP_LINK", "") or "").strip()
    safe_icp_number = html.escape(icp_number)

    if icp_link:
        safe_icp_link = html.escape(icp_link, quote=True)
        content = (
            f'<a href="{safe_icp_link}" target="_blank" '
            f'rel="noopener noreferrer">{safe_icp_number}</a>'
        )
    else:
        content = f"<span>{safe_icp_number}</span>"

    st.markdown(f'<div class="site-filing">{content}</div>', unsafe_allow_html=True)


def ensure_model_session_state() -> None:
    """初始化当前会话的模型选择。"""
    default_lightweight_model = config.LIGHTWEIGHT_MODEL_NAME or "deepseek-chat"
    default_reasoning_model = config.REASONING_MODEL_NAME or "deepseek-reasoner"

    if "selected_lightweight_model" not in st.session_state:
        st.session_state.selected_lightweight_model = default_lightweight_model
    elif not str(st.session_state.selected_lightweight_model).strip():
        st.session_state.selected_lightweight_model = default_lightweight_model
    if "selected_reasoning_model" not in st.session_state:
        st.session_state.selected_reasoning_model = default_reasoning_model
    elif not str(st.session_state.selected_reasoning_model).strip():
        st.session_state.selected_reasoning_model = default_reasoning_model


def get_selected_models():
    """获取当前会话生效的轻量/推理模型。"""
    ensure_model_session_state()
    return (
        st.session_state.selected_lightweight_model,
        st.session_state.selected_reasoning_model,
    )

NAV_VIEW_KEYS = [
    "show_history",
    "show_monitor",
    "show_main_force",
    "show_low_price_bull",
    "show_small_cap",
    "show_profit_growth",
    "show_value_stock",
    "show_sector_strategy",
    "show_longhubang",
    "show_smart_monitor",
    "show_portfolio",
    "show_news_flow",
    "show_macro_cycle",
    "show_config",
]


def activate_view(view_key: Optional[str] = None) -> None:
    """Activate one main view and clear the others."""
    for key in NAV_VIEW_KEYS:
        if key == view_key:
            st.session_state[key] = True
        else:
            st.session_state.pop(key, None)


def main():
    # 顶部标题栏
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem;">
        <h2 style="font-weight: 600; font-size: 1.6rem; margin-bottom: 0.2rem; color: var(--text-color);">复合多AI智能体股票分析系统</h2>
        <p style="color: rgba(255,255,255,0.6); font-size: 0.9rem;">基于专业量化策略的多Agent智能分析平台</p>
    </div>
    """, unsafe_allow_html=True)

    # 侧边栏
    with st.sidebar:
        # 快捷导航 - 移到顶部
        st.markdown("### 功能导航")

        # 单股分析（首页）
        if st.button("股票分析", width='stretch', key="nav_home", help="返回首页，进行单只股票的深度分析"):
            activate_view()

        if st.button("历史记录", width='stretch', key="nav_history", help="查看历史分析记录"):
            activate_view("show_history")

        with st.expander("使用帮助"):
            st.markdown("""
            **输入格式**
            - A股：6位数字，如 `600519`
            - 港股：数字或 `HK` 前缀，如 `00700`、`HK00700`
            - 美股：字母代码，如 `AAPL`

            **AI 分析流程**
            数据获取 -> 技术面/基本面/资金面分析 -> AI 整理 -> 团队决策
            """)

        st.markdown("---")

        # 选股板块
        with st.expander("选股板块", expanded=True):
            st.markdown("**根据不同策略筛选优质股票**")

            if st.button("主力选股", width='stretch', key="nav_main_force", help="基于主力资金流向的选股策略"):
                activate_view("show_main_force")
            
            if st.button("低价擒牛", width='stretch', key="nav_low_price_bull", help="低价高成长股票筛选策略"):
                activate_view("show_low_price_bull")
            
            if st.button("小市值策略", width='stretch', key="nav_small_cap", help="小盘高成长股票筛选策略"):
                activate_view("show_small_cap")
            
            if st.button("净利增长", width='stretch', key="nav_profit_growth", help="净利润增长稳健股票筛选策略"):
                activate_view("show_profit_growth")

            if st.button("低估值策略", width='stretch', key="nav_value_stock", help="低PE+低PB+高股息+低负债 价值投资筛选"):
                activate_view("show_value_stock")

        # 策略分析
        with st.expander("策略分析", expanded=True):
            st.markdown("**AI驱动的板块和龙虎榜策略**")

            if st.button("智策板块", width='stretch', key="nav_sector_strategy", help="AI板块策略分析"):
                activate_view("show_sector_strategy")

            if st.button("智瞰龙虎", width='stretch', key="nav_longhubang", help="龙虎榜深度分析"):
                activate_view("show_longhubang")
            
            if st.button("新闻流量", width='stretch', key="nav_news_flow", help="新闻流量监测与短线指导"):
                activate_view("show_news_flow")

            if st.button("宏观周期", width='stretch', key="nav_macro_cycle", help="康波周期 × 美林投资时钟 × 政策分析"):
                activate_view("show_macro_cycle")

        # 投资管理
        with st.expander("投资管理", expanded=True):
            st.markdown("**持仓跟踪与实时监测**")

            if st.button("持仓分析", width='stretch', key="nav_portfolio", help="投资组合分析与定时跟踪"):
                activate_view("show_portfolio")

            if st.button("AI盯盘", width='stretch', key="nav_smart_monitor", help="DeepSeek AI自动盯盘决策交易（支持A股T+1）"):
                activate_view("show_smart_monitor")

            if st.button("实时监测", width='stretch', key="nav_monitor", help="价格监控与预警提醒"):
                activate_view("show_monitor")

        st.markdown("---")

        # 环境配置
        if st.button("环境配置", width='stretch', key="nav_config", help="系统设置与API配置"):
            activate_view("show_config")

        st.markdown("---")

        # 系统配置
        st.markdown("### 系统配置")
        ensure_model_session_state()

        # API密钥检查
        api_key_status = check_api_key()
        if api_key_status:
            st.success("API已连接")
        else:
            st.error("API未配置")
            st.caption("请在.env中配置API密钥")

        st.markdown("---")

        lightweight_model_options = get_lightweight_model_options(
            st.session_state.selected_lightweight_model,
        )
        reasoning_model_options = get_reasoning_model_options(
            st.session_state.selected_reasoning_model,
        )
        lightweight_model_keys = list(lightweight_model_options.keys())
        reasoning_model_keys = list(reasoning_model_options.keys())

        st.selectbox(
            "轻量模型",
            options=lightweight_model_keys,
            index=lightweight_model_keys.index(st.session_state.selected_lightweight_model),
            format_func=lambda model_name: lightweight_model_options.get(model_name, model_name),
            key="selected_lightweight_model",
            help="候选项完全来自 .env 中的轻量模型配置",
        )

        st.selectbox(
            "推理模型",
            options=reasoning_model_keys,
            index=reasoning_model_keys.index(st.session_state.selected_reasoning_model),
            format_func=lambda model_name: reasoning_model_options.get(model_name, model_name),
            key="selected_reasoning_model",
            help="候选项完全来自 .env 中的推理模型配置",
        )

        st.caption(f"轻量任务当前使用: {st.session_state.selected_lightweight_model}")
        st.caption(f"推理任务当前使用: {st.session_state.selected_reasoning_model}")
        st.caption("仅影响当前会话中新发起的分析")


        if config.ADMIN_PASSWORD or getattr(config, "ADMIN_PASSWORD_HASH", ""):
            if st.button("退出登录", width='stretch', key="nav_logout"):
                st.session_state.authenticated = False
                st.session_state.pop("authenticated_at", None)
                st.session_state.pop("login_password_input", None)
                st.rerun()

        st.markdown("---")

        # 系统状态面板
        st.markdown("### 系统状态")

        monitor_status = "🟢 运行中" if monitor_service.running else "🔴 已停止"
        st.markdown(f"**监测服务**: {monitor_status}")

        try:
            from monitor_db import monitor_db
            stocks = monitor_db.get_monitored_stocks()
            notifications = monitor_db.get_pending_notifications()
            record_count = db.get_record_count()

            st.markdown(f"**分析记录**: {record_count}条")
            st.markdown(f"**监测股票**: {len(stocks)}只")
            st.markdown(f"**待处理**: {len(notifications)}条")
        except:
            pass

        render_site_filing()

    # 从配置获取数据获取周期
    period = getattr(config, "DATA_PERIOD", "1y")
    selected_lightweight_model, selected_reasoning_model = get_selected_models()
    
    # 检测是否进入任意子页面，以折叠侧边栏
    if any(st.session_state.get(v, False) for v in NAV_VIEW_KEYS):
        collapse_sidebar()

    # 检查是否显示历史记录
    if 'show_history' in st.session_state and st.session_state.show_history:
        display_history_records()
        return

    # 检查是否显示监测面板
    if 'show_monitor' in st.session_state and st.session_state.show_monitor:
        display_monitor_manager()
        return

    # 检查是否显示主力选股
    if 'show_main_force' in st.session_state and st.session_state.show_main_force:
        display_main_force_selector(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        return
    
    # 检查是否显示低价擒牛
    if 'show_low_price_bull' in st.session_state and st.session_state.show_low_price_bull:
        from low_price_bull_ui import display_low_price_bull
        display_low_price_bull()
        return
    
    # 检查是否显示小市值策略
    if 'show_small_cap' in st.session_state and st.session_state.show_small_cap:
        from small_cap_ui import display_small_cap
        display_small_cap()
        return
    
    # 检查是否显示净利增长策略
    if 'show_profit_growth' in st.session_state and st.session_state.show_profit_growth:
        from profit_growth_ui import display_profit_growth
        display_profit_growth()
        return

    # 检查是否显示低估值策略
    if 'show_value_stock' in st.session_state and st.session_state.show_value_stock:
        from value_stock_ui import display_value_stock
        display_value_stock()
        return

    # 检查是否显示智策板块
    if 'show_sector_strategy' in st.session_state and st.session_state.show_sector_strategy:
        display_sector_strategy(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        return

    # 检查是否显示智瞰龙虎
    if 'show_longhubang' in st.session_state and st.session_state.show_longhubang:
        display_longhubang(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        return

    # 检查是否显示AI盯盘
    if 'show_smart_monitor' in st.session_state and st.session_state.show_smart_monitor:
        smart_monitor_ui(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        return

    # 检查是否显示持仓分析
    if 'show_portfolio' in st.session_state and st.session_state.show_portfolio:
        from portfolio_ui import display_portfolio_manager
        display_portfolio_manager(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        return

    # 检查是否显示新闻流量监测
    if 'show_news_flow' in st.session_state and st.session_state.show_news_flow:
        display_news_flow_monitor(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        return

    # 检查是否显示宏观周期分析
    if 'show_macro_cycle' in st.session_state and st.session_state.show_macro_cycle:
        from macro_cycle_ui import display_macro_cycle
        display_macro_cycle(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        return
    
    # 检查是否显示环境配置
    if 'show_config' in st.session_state and st.session_state.show_config:
        display_config_manager()
        return

    # 主界面
    # 添加单个/批量分析切换
    col_mode1, col_mode2 = st.columns([1, 3])
    with col_mode1:
        analysis_mode = st.radio(
            "分析模式",
            ["单个分析", "批量分析"],
            horizontal=True,
            help="单个分析：分析单只股票；批量分析：同时分析多只股票"
        )

    with col_mode2:
        if analysis_mode == "批量分析":
            batch_mode = st.radio(
                "批量模式",
                ["顺序分析", "多线程并行"],
                horizontal=True,
                help="顺序分析：按次序分析，稳定但较慢；多线程并行：同时分析多只，快速但消耗资源"
            )
            st.session_state.batch_mode = batch_mode

    st.markdown("---")

    if analysis_mode == "单个分析":
        # 单个股票分析界面
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            stock_input = st.text_input(
                "请输入股票代码或名称",
                placeholder="例如: AAPL, 000001, 00700",
                help="支持A股(如000001)、港股(如00700)和美股(如AAPL)"
            )

        with col2:
            analyze_button = st.button("开始分析", type="primary", width='stretch')

        with col3:
            if st.button("清除缓存", width='stretch'):
                st.cache_data.clear()
                st.success("缓存已清除")

    else:
        # 批量股票分析界面
        stock_input = st.text_area(
            "请输入多个股票代码（每行一个或用逗号分隔）",
            placeholder="例如:\n000001\n600036\n00700\n\n或者: 000001, 600036, 00700, AAPL",
            height=120,
            help="支持多种格式：每行一个代码或用逗号分隔。支持A股、港股、美股"
        )

        col1, col2, col3 = st.columns(3)
        with col1:
            analyze_button = st.button("开始批量分析", type="primary", width='stretch')
        with col2:
            if st.button("清除缓存", width='stretch'):
                st.cache_data.clear()
                st.success("缓存已清除")
        with col3:
            if st.button("清除结果", width='stretch'):
                if 'batch_analysis_results' in st.session_state:
                    del st.session_state.batch_analysis_results
                st.success("已清除批量分析结果")

    # 分析师团队选择
    st.markdown("---")
    st.subheader("选择分析师团队")

    col1, col2, col3 = st.columns(3)

    with col1:
        enable_technical = st.checkbox("技术分析师", value=True,
                                       help="负责技术指标分析、图表形态识别、趋势判断")
        enable_fundamental = st.checkbox("基本面分析师", value=True,
                                        help="负责公司财务分析、行业研究、估值分析")

    with col2:
        enable_fund_flow = st.checkbox("资金面分析师", value=True,
                                      help="负责资金流向分析、主力行为研究")
        enable_risk = st.checkbox("风险管理师", value=True,
                                 help="负责风险识别、风险评估、风险控制策略制定")

    with col3:
        enable_sentiment = st.checkbox("市场情绪分析师", value=True,
                                      help="负责市场情绪研究、ARBR指标分析（仅A股）")
        enable_news = st.checkbox("新闻分析师", value=True,
                                 help="负责新闻事件分析、舆情研究（仅A股，qstock数据源）")

    # 显示已选择的分析师
    selected_analysts = []
    if enable_technical:
        selected_analysts.append("技术分析师")
    if enable_fundamental:
        selected_analysts.append("基本面分析师")
    if enable_fund_flow:
        selected_analysts.append("资金面分析师")
    if enable_risk:
        selected_analysts.append("风险管理师")
    if enable_sentiment:
        selected_analysts.append("市场情绪分析师")
    if enable_news:
        selected_analysts.append("新闻分析师")

    if selected_analysts:
        st.info(f"已选择 {len(selected_analysts)} 位分析师: {', '.join(selected_analysts)}")
    else:
        st.warning("请至少选择一位分析师")

    # 保存选择到session_state
    st.session_state.enable_technical = enable_technical
    st.session_state.enable_fundamental = enable_fundamental
    st.session_state.enable_fund_flow = enable_fund_flow
    st.session_state.enable_risk = enable_risk
    st.session_state.enable_sentiment = enable_sentiment
    st.session_state.enable_news = enable_news

    st.markdown("---")

    if analyze_button and stock_input:
        if not api_key_status:
            st.error("请先配置 DeepSeek API Key")
            return

        # 检查是否至少选择了一位分析师
        if not selected_analysts:
            st.error("请至少选择一位分析师参与分析")
            return

        if analysis_mode == "单个分析":
            # 单个股票分析
            # 清除之前的分析结果
            if 'analysis_completed' in st.session_state:
                del st.session_state.analysis_completed
            if 'stock_info' in st.session_state:
                del st.session_state.stock_info
            if 'agents_results' in st.session_state:
                del st.session_state.agents_results
            if 'discussion_result' in st.session_state:
                del st.session_state.discussion_result
            if 'final_decision' in st.session_state:
                del st.session_state.final_decision
            if 'just_completed' in st.session_state:
                del st.session_state.just_completed

            run_stock_analysis(stock_input, period)

        else:
            # 批量股票分析
            # 解析股票代码列表
            stock_list = parse_stock_list(stock_input)

            if not stock_list:
                st.error("请输入有效的股票代码")
                return

            if len(stock_list) > 20:
                st.warning(f"检测到 {len(stock_list)} 只股票，建议一次分析不超过20只")

            st.info(f"准备分析 {len(stock_list)} 只股票: {', '.join(stock_list)}")

            # 清除之前的分析结果（包括单个和批量）
            if 'batch_analysis_results' in st.session_state:
                del st.session_state.batch_analysis_results
            if 'analysis_completed' in st.session_state:
                del st.session_state.analysis_completed
            if 'stock_info' in st.session_state:
                del st.session_state.stock_info
            if 'agents_results' in st.session_state:
                del st.session_state.agents_results
            if 'discussion_result' in st.session_state:
                del st.session_state.discussion_result
            if 'final_decision' in st.session_state:
                del st.session_state.final_decision
            if 'just_completed' in st.session_state:
                del st.session_state.just_completed

            # 获取批量模式
            batch_mode = st.session_state.get('batch_mode', '顺序分析')

            # 运行批量分析
            run_batch_analysis(stock_list, period, batch_mode)

    # 检查是否有已完成的批量分析结果（优先显示批量结果）
    if 'batch_analysis_results' in st.session_state and st.session_state.batch_analysis_results:
        display_batch_analysis_results(st.session_state.batch_analysis_results, period)

    # 检查是否有已完成的单个分析结果（但不是刚刚完成的，避免重复显示）
    elif 'analysis_completed' in st.session_state and st.session_state.analysis_completed:
        # 如果是刚刚完成的分析，清除标志，避免重复显示
        if st.session_state.get('just_completed', False):
            st.session_state.just_completed = False
        else:
            # 重新显示之前的分析结果（页面刷新后）
            stock_info = st.session_state.stock_info
            agents_results = st.session_state.agents_results
            discussion_result = st.session_state.discussion_result
            final_decision = st.session_state.final_decision

            # 重新获取股票数据用于显示图表
            stock_info_current, stock_data, indicators = get_stock_data(stock_info['symbol'], period)

            # 显示股票基本信息
            display_stock_info(stock_info, indicators)

            # 显示股票图表
            if stock_data is not None:
                display_stock_chart(stock_data, stock_info)

            # 显示各分析师报告
            display_agents_analysis(agents_results)

            # 显示团队讨论
            display_team_discussion(discussion_result)

            # 显示最终决策
            display_final_decision(final_decision, stock_info, agents_results, discussion_result)

    # 示例和说明
    elif not stock_input:
        show_example_interface()

def check_api_key():
    """检查API密钥是否配置"""
    try:
        import config
        return bool(config.DEEPSEEK_API_KEY and config.DEEPSEEK_API_KEY.strip())
    except:
        return False

@st.cache_data(ttl=300)  # 缓存5分钟
def get_stock_data(symbol, period):
    """获取股票数据（带缓存）"""
    fetcher = StockDataFetcher()
    stock_info = fetcher.get_stock_info(symbol)
    stock_data = fetcher.get_stock_data(symbol, period)

    if isinstance(stock_data, dict) and "error" in stock_data:
        return stock_info, None, None

    stock_data_with_indicators = fetcher.calculate_technical_indicators(stock_data)
    indicators = fetcher.get_latest_indicators(stock_data_with_indicators)

    return stock_info, stock_data_with_indicators, indicators

def parse_stock_list(stock_input):
    """解析股票代码列表

    支持的格式：
    - 每行一个代码
    - 逗号分隔
    - 空格分隔
    """
    if not stock_input or not stock_input.strip():
        return []

    # 先按换行符分割
    lines = stock_input.strip().split('\n')

    # 处理每一行
    stock_list = []
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # 检查是否包含逗号
        if ',' in line:
            codes = [code.strip() for code in line.split(',')]
            stock_list.extend([code for code in codes if code])
        # 检查是否包含空格
        elif ' ' in line:
            codes = [code.strip() for code in line.split()]
            stock_list.extend([code for code in codes if code])
        else:
            stock_list.append(line)

    # 去重并保持顺序
    seen = set()
    unique_list = []
    for code in stock_list:
        if code not in seen:
            seen.add(code)
            unique_list.append(code)

    return unique_list

def analyze_single_stock_for_batch(symbol, period, enabled_analysts_config=None,
                                   selected_model=None,
                                   selected_lightweight_model=None,
                                   selected_reasoning_model=None,
                                   save_to_global_history=True):
    """单个股票分析（用于批量分析）

    Args:
        symbol: 股票代码
        period: 数据周期
        enabled_analysts_config: 分析师配置字典
        selected_model: 兼容旧接口，强制所有任务统一使用同一个模型
        selected_lightweight_model: 当前会话的轻量模型
        selected_reasoning_model: 当前会话的推理模型

    返回分析结果或错误信息
    """
    try:
        forced_model = selected_model
        if selected_lightweight_model or selected_reasoning_model:
            forced_model = None

        # 使用默认配置
        if enabled_analysts_config is None:
            enabled_analysts_config = {
                'technical': True,
                'fundamental': True,
                'fund_flow': True,
                'risk': True,
                'sentiment': False,
                'news': False
            }

        # 1. 获取股票数据
        stock_info, stock_data, indicators = get_stock_data(symbol, period)

        if "error" in stock_info:
            return {"symbol": symbol, "error": stock_info['error'], "success": False}

        if stock_data is None:
            return {"symbol": symbol, "error": "无法获取股票历史数据", "success": False}

        # 2. 获取财务数据
        fetcher = StockDataFetcher()
        financial_data = fetcher.get_financial_data(symbol)

        # 2.5 获取季报数据（仅A股）
        quarterly_data = None
        enable_fundamental = enabled_analysts_config.get('fundamental', True)
        if enable_fundamental and fetcher._is_chinese_stock(symbol):
            try:
                from quarterly_report_data import QuarterlyReportDataFetcher
                quarterly_fetcher = QuarterlyReportDataFetcher()
                quarterly_data = quarterly_fetcher.get_quarterly_reports(symbol)
            except:
                pass

        # 获取分析师选择状态（从参数而不是session_state）
        enable_fund_flow = enabled_analysts_config.get('fund_flow', True)
        enable_sentiment = enabled_analysts_config.get('sentiment', False)
        enable_news = enabled_analysts_config.get('news', False)

        # 3. 获取资金流向数据（akshare数据源，可选）
        fund_flow_data = None
        if enable_fund_flow and fetcher._is_chinese_stock(symbol):
            try:
                from fund_flow_akshare import FundFlowAkshareDataFetcher
                fund_flow_fetcher = FundFlowAkshareDataFetcher()
                fund_flow_data = fund_flow_fetcher.get_fund_flow_data(symbol)
            except:
                pass

        # 4. 获取市场情绪数据（可选）
        sentiment_data = None
        if enable_sentiment and fetcher._is_chinese_stock(symbol):
            try:
                from market_sentiment_data import MarketSentimentDataFetcher
                sentiment_fetcher = MarketSentimentDataFetcher()
                sentiment_data = sentiment_fetcher.get_market_sentiment_data(symbol, stock_data)
            except:
                pass

        # 5. 获取新闻数据（qstock数据源，可选）
        news_data = None
        if enable_news and fetcher._is_chinese_stock(symbol):
            try:
                from qstock_news_data import QStockNewsDataFetcher
                news_fetcher = QStockNewsDataFetcher()
                news_data = news_fetcher.get_stock_news(symbol)
            except:
                pass

        # 5.5 获取风险数据（限售解禁、大股东减持、重要事件，可选）
        risk_data = None
        enable_risk = enabled_analysts_config.get('risk', True)
        if enable_risk and fetcher._is_chinese_stock(symbol):
            try:
                risk_data = fetcher.get_risk_data(symbol)
            except:
                pass

        # 6. 初始化AI分析系统
        agents = StockAnalysisAgents(
            model=forced_model,
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )

        # 使用传入的分析师配置
        enabled_analysts = enabled_analysts_config

        # 7. 运行多智能体分析
        agents_results = agents.run_multi_agent_analysis(
            stock_info, stock_data, indicators, financial_data,
            fund_flow_data, sentiment_data, news_data, quarterly_data, risk_data,
            enabled_analysts=enabled_analysts_config
        )

        # 8. 团队讨论
        discussion_result = agents.conduct_team_discussion(agents_results, stock_info)

        # 9. 最终决策
        final_decision = agents.make_final_decision(discussion_result, stock_info, indicators)

        saved_to_db = False
        db_error = None
        if save_to_global_history:
            try:
                record_id = db.save_analysis(
                    symbol=stock_info.get('symbol', ''),
                    stock_name=stock_info.get('name', ''),
                    period=period,
                    stock_info=stock_info,
                    agents_results=agents_results,
                    discussion_result=discussion_result,
                    final_decision=final_decision
                )
                saved_to_db = True
                print(f"{symbol} 成功保存到数据库，记录ID: {record_id}")
            except Exception as e:
                db_error = str(e)
                print(f"{symbol} 保存到数据库失败: {db_error}")

        return {
            "symbol": symbol,
            "success": True,
            "stock_info": stock_info,
            "indicators": indicators,
            "agents_results": agents_results,
            "discussion_result": discussion_result,
            "final_decision": final_decision,
            "saved_to_db": saved_to_db,
            "db_error": db_error
        }

    except Exception as e:
        return {"symbol": symbol, "error": str(e), "success": False}

def run_batch_analysis(stock_list, period, batch_mode="顺序分析"):
    """运行批量股票分析"""
    import concurrent.futures
    import threading

    # 在开始分析前获取配置（从session_state）
    enabled_analysts_config = {
        'technical': st.session_state.get('enable_technical', True),
        'fundamental': st.session_state.get('enable_fundamental', True),
        'fund_flow': st.session_state.get('enable_fund_flow', True),
        'risk': st.session_state.get('enable_risk', True),
        'sentiment': st.session_state.get('enable_sentiment', False),
        'news': st.session_state.get('enable_news', False)
    }
    selected_lightweight_model, selected_reasoning_model = get_selected_models()

    # 创建进度显示
    st.subheader(f"批量分析进行中 ({batch_mode})")

    progress_bar = st.progress(0)
    status_text = st.empty()

    # 存储结果
    results = []
    total = len(stock_list)

    if batch_mode == "多线程并行":
        # 多线程并行分析
        status_text.text(f"使用多线程并行分析 {total} 只股票...")

        # 创建线程锁用于更新进度
        lock = threading.Lock()
        completed = [0]  # 使用列表以便在闭包中修改
        progress_status = [{}]  # 存储进度状态

        def analyze_with_progress(symbol):
            """包装分析函数，不在线程中访问Streamlit上下文"""
            try:
                result = analyze_single_stock_for_batch(
                    symbol,
                    period,
                    enabled_analysts_config,
                    selected_lightweight_model=selected_lightweight_model,
                    selected_reasoning_model=selected_reasoning_model,
                )
                with lock:
                    completed[0] += 1
                    progress_status[0][symbol] = result
                return result
            except Exception as e:
                with lock:
                    completed[0] += 1
                    error_result = {"symbol": symbol, "error": str(e), "success": False}
                    progress_status[0][symbol] = error_result
                return error_result

        # 使用线程池执行，限制最大并发数为3以避免API限流
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_symbol = {executor.submit(analyze_with_progress, symbol): symbol
                              for symbol in stock_list}

            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result(timeout=300)  # 5分钟超时
                    results.append(result)

                    # 在主线程中更新UI
                    progress = len(results) / total
                    progress_bar.progress(progress)

                    if result['success']:
                        status_text.text(f"[{len(results)}/{total}] {symbol} 分析完成")
                    else:
                        status_text.text(f"[{len(results)}/{total}] {symbol} 分析失败: {result.get('error', '未知错误')}")

                except concurrent.futures.TimeoutError:
                    results.append({"symbol": symbol, "error": "分析超时（5分钟）", "success": False})
                    progress_bar.progress(len(results) / total)
                    status_text.text(f"⏱️ [{len(results)}/{total}] {symbol} 分析超时")
                except Exception as e:
                    results.append({"symbol": symbol, "error": str(e), "success": False})
                    progress_bar.progress(len(results) / total)
                    status_text.text(f"[{len(results)}/{total}] {symbol} 出现错误")

    else:
        # 顺序分析
        status_text.text(f"📝 按顺序分析 {total} 只股票...")

        for i, symbol in enumerate(stock_list, 1):
            status_text.text(f"[{i}/{total}] 正在分析 {symbol}...")

            try:
                result = analyze_single_stock_for_batch(
                    symbol,
                    period,
                    enabled_analysts_config,
                    selected_lightweight_model=selected_lightweight_model,
                    selected_reasoning_model=selected_reasoning_model,
                )
            except Exception as e:
                result = {"symbol": symbol, "error": str(e), "success": False}

            results.append(result)

            # 更新进度
            progress = i / total
            progress_bar.progress(progress)

            if result['success']:
                status_text.text(f"[{i}/{total}] {symbol} 分析完成")
            else:
                status_text.text(f"[{i}/{total}] {symbol} 分析失败: {result.get('error', '未知错误')}")

    # 完成
    progress_bar.progress(1.0)

    # 统计结果
    success_count = sum(1 for r in results if r['success'])
    failed_count = total - success_count
    saved_count = sum(1 for r in results if r.get('saved_to_db', False))

    # 显示完成信息
    if success_count > 0:
        status_text.success(f"批量分析完成！成功 {success_count} 只，失败 {failed_count} 只，已保存 {saved_count} 只到历史记录")

        # 显示保存失败的股票
        save_failed = [r['symbol'] for r in results if r.get('success') and not r.get('saved_to_db', False)]
        if save_failed:
            st.warning(f"以下股票分析成功但保存失败: {', '.join(save_failed)}")
    else:
        status_text.error(f"批量分析完成，但所有股票都分析失败")

    # 保存结果到session_state
    st.session_state.batch_analysis_results = results
    st.session_state.batch_analysis_mode = batch_mode

    time.sleep(1)
    progress_bar.empty()

    # 自动显示结果
    st.rerun()

def run_stock_analysis(symbol, period):
    """运行股票分析"""

    # 进度条
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # 1. 获取股票数据
        status_text.text("正在获取股票数据...")
        progress_bar.progress(10)

        stock_info, stock_data, indicators = get_stock_data(symbol, period)

        if "error" in stock_info:
            st.error(f"{stock_info['error']}")
            return

        if stock_data is None:
            st.error("无法获取股票历史数据")
            return

        # 显示股票基本信息
        display_stock_info(stock_info, indicators)
        progress_bar.progress(20)

        # 显示股票图表
        display_stock_chart(stock_data, stock_info)
        progress_bar.progress(30)

        # 2. 获取财务数据
        status_text.text("正在获取财务数据...")
        fetcher = StockDataFetcher()  # 创建fetcher实例
        financial_data = fetcher.get_financial_data(symbol)
        progress_bar.progress(35)

        # 2.5 获取季报数据（仅在选择了基本面分析师且为A股时）
        enable_fundamental = st.session_state.get('enable_fundamental', True)
        quarterly_data = None
        if enable_fundamental and fetcher._is_chinese_stock(symbol):
            status_text.text("正在获取季报数据（akshare数据源）...")
            try:
                from quarterly_report_data import QuarterlyReportDataFetcher
                quarterly_fetcher = QuarterlyReportDataFetcher()
                quarterly_data = quarterly_fetcher.get_quarterly_reports(symbol)
                if quarterly_data and quarterly_data.get('data_success'):
                    income_count = quarterly_data.get('income_statement', {}).get('periods', 0) if quarterly_data.get('income_statement') else 0
                    balance_count = quarterly_data.get('balance_sheet', {}).get('periods', 0) if quarterly_data.get('balance_sheet') else 0
                    cash_flow_count = quarterly_data.get('cash_flow', {}).get('periods', 0) if quarterly_data.get('cash_flow') else 0
                    st.info(f"成功获取季报数据：利润表{income_count}期，资产负债表{balance_count}期，现金流量表{cash_flow_count}期")
                else:
                    st.warning("未能获取季报数据，将基于基本财务数据分析")
            except Exception as e:
                st.warning(f"获取季报数据时出错: {str(e)}")
                quarterly_data = None
        elif enable_fundamental and not fetcher._is_chinese_stock(symbol):
            st.info("ℹ️ 美股暂不支持季报数据")
        progress_bar.progress(37)

        # 获取分析师选择状态
        enable_fund_flow = st.session_state.get('enable_fund_flow', True)
        enable_sentiment = st.session_state.get('enable_sentiment', False)
        enable_news = st.session_state.get('enable_news', False)

        # 3. 获取资金流向数据（仅在选择了资金面分析师时，使用akshare数据源）
        fund_flow_data = None
        if enable_fund_flow and fetcher._is_chinese_stock(symbol):
            status_text.text("正在获取资金流向数据（akshare数据源）...")
            try:
                from fund_flow_akshare import FundFlowAkshareDataFetcher
                fund_flow_fetcher = FundFlowAkshareDataFetcher()
                fund_flow_data = fund_flow_fetcher.get_fund_flow_data(symbol)
                if fund_flow_data and fund_flow_data.get('data_success'):
                    days = fund_flow_data.get('fund_flow_data', {}).get('days', 0) if fund_flow_data.get('fund_flow_data') else 0
                    st.info(f"成功获取 {days} 个交易日的资金流向数据")
                else:
                    st.warning("未能获取资金流向数据，将基于技术指标进行资金面分析")
            except Exception as e:
                st.warning(f"获取资金流向数据时出错: {str(e)}")
                fund_flow_data = None
        elif enable_fund_flow and not fetcher._is_chinese_stock(symbol):
            st.info("ℹ️ 美股暂不支持资金流向数据")
        progress_bar.progress(40)

        # 4. 获取市场情绪数据（仅在选择了市场情绪分析师时）
        sentiment_data = None
        if enable_sentiment and fetcher._is_chinese_stock(symbol):
            status_text.text("正在获取市场情绪数据（ARBR等指标）...")
            try:
                from market_sentiment_data import MarketSentimentDataFetcher
                sentiment_fetcher = MarketSentimentDataFetcher()
                sentiment_data = sentiment_fetcher.get_market_sentiment_data(symbol, stock_data)
                if sentiment_data and sentiment_data.get('data_success'):
                    st.info("成功获取市场情绪数据（ARBR、换手率、涨跌停等）")
                else:
                    st.warning("未能获取完整的市场情绪数据，将基于基本信息进行分析")
            except Exception as e:
                st.warning(f"获取市场情绪数据时出错: {str(e)}")
                sentiment_data = None
        elif enable_sentiment and not fetcher._is_chinese_stock(symbol):
            st.info("ℹ️ 美股暂不支持市场情绪数据（ARBR等指标）")
        progress_bar.progress(45)

        # 5. 获取新闻数据（仅在选择了新闻分析师时，使用qstock数据源）
        news_data = None
        if enable_news and fetcher._is_chinese_stock(symbol):
            status_text.text("正在获取新闻数据...")
            try:
                from qstock_news_data import QStockNewsDataFetcher
                news_fetcher = QStockNewsDataFetcher()
                news_data = news_fetcher.get_stock_news(symbol)
                if news_data and news_data.get('data_success'):
                    news_count = news_data.get('news_data', {}).get('count', 0) if news_data.get('news_data') else 0
                    st.info(f"成功从东方财富获取个股 {news_count} 条新闻")
                else:
                    st.warning("未能获取新闻数据，将基于基本信息进行分析")
            except Exception as e:
                st.warning(f"获取新闻数据时出错: {str(e)}")
                news_data = None
        elif enable_news and not fetcher._is_chinese_stock(symbol):
            st.info("ℹ️ 美股暂不支持新闻数据")
        progress_bar.progress(45)

        # 5.5 获取风险数据（仅在选择了风险管理师时，使用问财数据源）
        enable_risk = st.session_state.get('enable_risk', True)
        risk_data = None
        if enable_risk and fetcher._is_chinese_stock(symbol):
            status_text.text("正在获取风险数据（限售解禁、大股东减持、重要事件）...")
            try:
                risk_data = fetcher.get_risk_data(symbol)
                if risk_data and risk_data.get('data_success'):
                    # 统计获取到的风险数据类型
                    risk_types = []
                    if risk_data.get('lifting_ban') and risk_data['lifting_ban'].get('has_data'):
                        risk_types.append("限售解禁")
                    if risk_data.get('shareholder_reduction') and risk_data['shareholder_reduction'].get('has_data'):
                        risk_types.append("大股东减持")
                    if risk_data.get('important_events') and risk_data['important_events'].get('has_data'):
                        risk_types.append("重要事件")

                    if risk_types:
                        st.info(f"成功获取风险数据：{', '.join(risk_types)}")
                    else:
                        st.info("ℹ️ 暂无风险相关数据")
                elif risk_data and risk_data.get('error'):
                    st.warning(f"风险数据获取超时或失败，已跳过风险数据抓取：{risk_data['error']}")
                else:
                    st.info("ℹ️ 暂无风险相关数据，将基于基本信息进行风险分析")
            except Exception as e:
                st.warning(f"获取风险数据时出错: {str(e)}")
                risk_data = None
        elif enable_risk and not fetcher._is_chinese_stock(symbol):
            st.info("ℹ️ 美股暂不支持风险数据（限售解禁、大股东减持等）")
        progress_bar.progress(50)

        # 6. 初始化AI分析系统
        status_text.text("正在初始化AI分析系统...")
        # 使用选择的模型
        selected_lightweight_model, selected_reasoning_model = get_selected_models()
        agents = StockAnalysisAgents(
            lightweight_model=selected_lightweight_model,
            reasoning_model=selected_reasoning_model,
        )
        progress_bar.progress(55)

        # 获取所有分析师选择状态
        enable_technical = st.session_state.get('enable_technical', True)
        enable_fundamental = st.session_state.get('enable_fundamental', True)
        enable_risk = st.session_state.get('enable_risk', True)

        # 创建分析师启用字典
        enabled_analysts = {
            'technical': enable_technical,
            'fundamental': enable_fundamental,
            'fund_flow': enable_fund_flow,
            'risk': enable_risk,
            'sentiment': enable_sentiment,
            'news': enable_news
        }

        # 7. 运行多智能体分析（传入所有数据和分析师选择）
        status_text.text("AI分析师团队正在分析,请耐心等待几分钟...")
        agents_results = agents.run_multi_agent_analysis(
            stock_info, stock_data, indicators, financial_data,
            fund_flow_data, sentiment_data, news_data, quarterly_data, risk_data,
            enabled_analysts=enabled_analysts
        )
        progress_bar.progress(75)

        # 显示各分析师报告
        display_agents_analysis(agents_results)

        # 8. 团队讨论
        status_text.text("🤝 分析团队正在讨论...")
        discussion_result = agents.conduct_team_discussion(agents_results, stock_info)
        progress_bar.progress(88)

        # 显示团队讨论
        display_team_discussion(discussion_result)

        # 9. 最终决策
        status_text.text("📋 正在制定最终投资决策...")
        final_decision = agents.make_final_decision(discussion_result, stock_info, indicators)
        progress_bar.progress(100)

        # 显示最终决策
        display_final_decision(final_decision, stock_info, agents_results, discussion_result)

        # 保存分析结果到session_state（用于页面刷新后显示）
        st.session_state.analysis_completed = True
        st.session_state.stock_info = stock_info
        st.session_state.agents_results = agents_results
        st.session_state.discussion_result = discussion_result
        st.session_state.final_decision = final_decision
        st.session_state.just_completed = True  # 标记刚刚完成分析

        # 保存到数据库
        try:
            db.save_analysis(
                symbol=stock_info.get('symbol', ''),
                stock_name=stock_info.get('name', ''),
                period=period,
                stock_info=stock_info,
                agents_results=agents_results,
                discussion_result=discussion_result,
                final_decision=final_decision
            )
            st.success("分析记录已保存到数据库")
        except Exception as e:
            st.warning(f"保存到数据库时出现错误: {str(e)}")

        status_text.text("分析完成！")
        time.sleep(1)
        status_text.empty()
        progress_bar.empty()

    except Exception as e:
        st.error(f"分析过程中出现错误: {str(e)}")
        progress_bar.empty()
        status_text.empty()

def display_stock_info(stock_info, indicators):
    """显示股票基本信息"""
    st.subheader(f"{stock_info.get('name', 'N/A')} ({stock_info.get('symbol', 'N/A')})")

    # 基本信息卡片
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        current_price = stock_info.get('current_price', 'N/A')
        st.metric("当前价格", f"{current_price}")

    with col2:
        change_percent = stock_info.get('change_percent', 'N/A')
        if isinstance(change_percent, (int, float)):
            st.metric("涨跌幅", f"{change_percent:.2f}%", f"{change_percent:.2f}%")
        else:
            st.metric("涨跌幅", f"{change_percent}")

    with col3:
        pe_ratio = stock_info.get('pe_ratio', 'N/A')
        st.metric("市盈率", f"{pe_ratio}")

    with col4:
        pb_ratio = stock_info.get('pb_ratio', 'N/A')
        st.metric("市净率", f"{pb_ratio}")

    with col5:
        market_cap = stock_info.get('market_cap', 'N/A')
        if isinstance(market_cap, (int, float)):
            market_cap_str = f"{market_cap/1e9:.2f}B" if market_cap > 1e9 else f"{market_cap/1e6:.2f}M"
            st.metric("市值", market_cap_str)
        else:
            st.metric("市值", f"{market_cap}")

    # 技术指标
    if indicators and not isinstance(indicators, dict) or "error" not in indicators:
        st.subheader("关键技术指标")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            rsi = indicators.get('rsi', 'N/A')
            if isinstance(rsi, (int, float)):
                rsi_color = "normal"
                if rsi > 70:
                    rsi_color = "inverse"
                elif rsi < 30:
                    rsi_color = "off"
                st.metric("RSI", f"{rsi:.2f}")
            else:
                st.metric("RSI", f"{rsi}")

        with col2:
            ma20 = indicators.get('ma20', 'N/A')
            if isinstance(ma20, (int, float)):
                st.metric("MA20", f"{ma20:.2f}")
            else:
                st.metric("MA20", f"{ma20}")

        with col3:
            volume_ratio = indicators.get('volume_ratio', 'N/A')
            if isinstance(volume_ratio, (int, float)):
                st.metric("量比", f"{volume_ratio:.2f}")
            else:
                st.metric("量比", f"{volume_ratio}")

        with col4:
            macd = indicators.get('macd', 'N/A')
            if isinstance(macd, (int, float)):
                st.metric("MACD", f"{macd:.4f}")
            else:
                st.metric("MACD", f"{macd}")

def display_stock_chart(stock_data, stock_info):
    """显示股票图表"""
    st.subheader("股价走势图")

    # 创建蜡烛图
    fig = go.Figure()

    # 添加蜡烛图
    fig.add_trace(go.Candlestick(
        x=stock_data.index,
        open=stock_data['Open'],
        high=stock_data['High'],
        low=stock_data['Low'],
        close=stock_data['Close'],
        name="K线"
    ))

    # 添加移动平均线
    if 'MA5' in stock_data.columns:
        fig.add_trace(go.Scatter(
            x=stock_data.index,
            y=stock_data['MA5'],
            name="MA5",
            line=dict(color='orange', width=1)
        ))

    if 'MA20' in stock_data.columns:
        fig.add_trace(go.Scatter(
            x=stock_data.index,
            y=stock_data['MA20'],
            name="MA20",
            line=dict(color='blue', width=1)
        ))

    if 'MA60' in stock_data.columns:
        fig.add_trace(go.Scatter(
            x=stock_data.index,
            y=stock_data['MA60'],
            name="MA60",
            line=dict(color='purple', width=1)
        ))

    # 布林带
    if 'BB_upper' in stock_data.columns and 'BB_lower' in stock_data.columns:
        fig.add_trace(go.Scatter(
            x=stock_data.index,
            y=stock_data['BB_upper'],
            name="布林上轨",
            line=dict(color='red', width=1, dash='dash')
        ))
        fig.add_trace(go.Scatter(
            x=stock_data.index,
            y=stock_data['BB_lower'],
            name="布林下轨",
            line=dict(color='green', width=1, dash='dash'),
            fill='tonexty',
            fillcolor='rgba(0,100,80,0.1)'
        ))

    fig.update_layout(
        title=f"{stock_info.get('name', 'N/A')} 股价走势",
        xaxis_title="日期",
        yaxis_title="价格",
        height=500,
        showlegend=True
    )

    # 生成唯一的key
    chart_key = f"main_stock_chart_{stock_info.get('symbol', 'unknown')}_{int(time.time())}"
    st.plotly_chart(fig, width='stretch', config={'responsive': True}, key=chart_key)

    # 成交量图
    if 'Volume' in stock_data.columns:
        fig_volume = go.Figure()
        fig_volume.add_trace(go.Bar(
            x=stock_data.index,
            y=stock_data['Volume'],
            name="成交量",
            marker_color='lightblue'
        ))

        fig_volume.update_layout(
            title="成交量",
            xaxis_title="日期",
            yaxis_title="成交量",
            height=200
        )

        # 生成唯一的key
        volume_key = f"volume_chart_{stock_info.get('symbol', 'unknown')}_{int(time.time())}"
        st.plotly_chart(fig_volume, width='stretch', config={'responsive': True}, key=volume_key)

def display_agents_analysis(agents_results):
    """显示各分析师报告"""
    st.subheader("AI分析师团队报告")

    # 创建标签页
    tab_names = []
    tab_contents = []

    for agent_key, agent_result in agents_results.items():
        agent_name = agent_result.get('agent_name', '未知分析师')
        tab_names.append(agent_name)
        tab_contents.append(agent_result)

    tabs = st.tabs(tab_names)

    for i, tab in enumerate(tabs):
        with tab:
            agent_result = tab_contents[i]

            # 分析师信息
            st.markdown(f"""
            <div class="agent-card">
                <h4>👨‍{agent_result.get('agent_name', '未知')}</h4>
                <p><strong>职责：</strong>{agent_result.get('agent_role', '未知')}</p>
                <p><strong>关注领域：</strong>{', '.join(agent_result.get('focus_areas', []))}</p>
                <p><strong>分析时间：</strong>{agent_result.get('timestamp', '未知')}</p>
            </div>
            """, unsafe_allow_html=True)

            # 分析报告
            st.markdown("**📄 分析报告:**")
            st.write(agent_result.get('analysis', '暂无分析'))

def display_team_discussion(discussion_result):
    """显示团队讨论"""
    st.subheader("🤝 分析团队讨论")

    st.markdown("""
    <div class="agent-card">
        <h4>💭 团队综合讨论</h4>
        <p>各位分析师正在就该股票进行深入讨论，整合不同维度的分析观点...</p>
    </div>
    """, unsafe_allow_html=True)

    st.write(discussion_result)

def display_final_decision(final_decision, stock_info, agents_results=None, discussion_result=None):
    """显示最终投资决策"""
    st.subheader("📋 最终投资决策")

    if isinstance(final_decision, dict) and "decision_text" not in final_decision:
        # JSON格式的决策
        col1, col2 = st.columns([1, 2])

        with col1:
            # 投资评级
            rating = final_decision.get('rating', '未知')
            rating_color = {"买入": "🟢", "持有": "🟡", "卖出": "🔴"}.get(rating, "⚪")

            st.markdown(f"""
            <div class="decision-card">
                <h3 style="text-align: center;">{rating_color} {rating}</h3>
                <h4 style="text-align: center;">投资评级</h4>
            </div>
            """, unsafe_allow_html=True)

            # 关键指标
            confidence = final_decision.get('confidence_level', 'N/A')
            st.metric("信心度", f"{confidence}/10")

            target_price = final_decision.get('target_price', 'N/A')
            st.metric("目标价格", f"{target_price}")

            position_size = final_decision.get('position_size', 'N/A')
            st.metric("建议仓位", f"{position_size}")

        with col2:
            # 详细建议
            st.markdown("**操作建议:**")
            st.write(final_decision.get('operation_advice', '暂无建议'))

            st.markdown("**📍 关键位置:**")
            col2_1, col2_2 = st.columns(2)

            with col2_1:
                st.write(f"**进场区间:** {final_decision.get('entry_range', 'N/A')}")
                st.write(f"**止盈位:** {final_decision.get('take_profit', 'N/A')}")

            with col2_2:
                st.write(f"**止损位:** {final_decision.get('stop_loss', 'N/A')}")
                st.write(f"**持有周期:** {final_decision.get('holding_period', 'N/A')}")

        # 风险提示
        risk_warning = final_decision.get('risk_warning', '')
        if risk_warning:
            st.markdown(f"""
            <div class="warning-card">
                <h4>风险提示</h4>
                <p>{risk_warning}</p>
            </div>
            """, unsafe_allow_html=True)

    else:
        # 文本格式的决策
        decision_text = final_decision.get('decision_text', str(final_decision))
        st.write(decision_text)

    # 添加PDF导出功能
    st.markdown("---")
    if agents_results and discussion_result:
        display_pdf_export_section(stock_info, agents_results, discussion_result, final_decision)
    else:
        st.warning("PDF导出功能需要完整的分析数据")

def show_example_interface():
    """显示示例界面"""
    st.subheader("使用说明")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        ### 如何使用
        1. **输入股票代码**：支持A股(如000001)、港股(如00700)和美股(如AAPL)
        2. **点击开始分析**：系统将启动AI分析师团队
        3. **查看分析报告**：多位专业分析师将从不同角度分析
        4. **获得投资建议**：获得最终的投资评级和操作建议
        
        ### 分析维度
        - **技术面**：趋势、指标、支撑阻力
        - **基本面**：财务、估值、行业分析
        - **资金面**：资金流向、主力行为
        - **风险管理**：风险识别与控制
        - **市场情绪**：情绪指标、热点分析
        """)

    with col2:
        st.markdown("""
        ### 示例股票代码
        
        **A股热门**
        - 000001 (平安银行)
        - 600036 (招商银行)
        - 600519 (贵州茅台)
        
        **港股热门**
        - 00700 或 700 (腾讯控股)
        - 09988 或 9988 (阿里巴巴-SW)
        - 01810 或 1810 (小米集团-W)
        
        **美股热门**
        - AAPL (苹果)
        - MSFT (微软)
        - NVDA (英伟达)
        """)

    st.markdown("---")
    st.markdown("""
    ### 🌏 市场支持说明
    - **A股**：完整支持（技术分析、财务数据、资金流向、市场情绪、新闻数据qstock）
    - **港股**：部分支持（技术分析、21项财务指标）⭐️ 
    - **美股**：完整支持（技术分析、财务数据）
    
    ### 港股支持的财务指标
    盈利能力（6项）、营运能力（3项）、偿债能力（2项）、市场表现（4项）、分红指标（3项）、股本结构（3项）
    """)

def display_history_records():
    """显示历史分析记录"""
    st.subheader("历史分析记录")

    # 获取所有记录
    records = db.get_all_records()

    if not records:
        st.info("📭 暂无历史分析记录")
        return

    st.write(f"共找到 {len(records)} 条分析记录")

    # 搜索和筛选
    col1, col2 = st.columns([3, 1])
    with col1:
        search_term = st.text_input("搜索股票代码或名称", placeholder="输入股票代码或名称进行搜索")
    with col2:
        st.write("")
        st.write("")
        if st.button("刷新列表"):
            st.rerun()

    # 筛选记录
    filtered_records = records
    if search_term:
        filtered_records = [
            record for record in records
            if search_term.lower() in record['symbol'].lower() or
               search_term.lower() in record['stock_name'].lower()
        ]

    if not filtered_records:
        st.warning("未找到匹配的记录")
        return

    # 显示记录列表
    for record in filtered_records:
        # 根据评级设置颜色和图标
        rating = record.get('rating', '未知')
        rating_color = {
            "买入": "🟢",
            "持有": "🟡",
            "卖出": "🔴",
            "强烈买入": "🟢",
            "强烈卖出": "🔴"
        }.get(rating, "⚪")

        with st.expander(f"{rating_color} {record['stock_name']} ({record['symbol']}) - {record['analysis_date']}"):
            col1, col2, col3, col4 = st.columns([2, 2, 1, 1])

            with col1:
                st.write(f"**股票代码:** {record['symbol']}")
                st.write(f"**股票名称:** {record['stock_name']}")

            with col2:
                st.write(f"**分析时间:** {record['analysis_date']}")
                st.write(f"**数据周期:** {record['period']}")
                st.write(f"**投资评级:** **{rating}**")

            with col3:
                if st.button("👀 查看详情", key=f"view_{record['id']}"):
                    st.session_state.viewing_record_id = record['id']

            with col4:
                if st.button("➕ 监测", key=f"add_monitor_{record['id']}"):
                    st.session_state.add_to_monitor_id = record['id']
                    st.session_state.viewing_record_id = record['id']

            # 删除按钮（新增一行）
            col5, _, _, _ = st.columns(4)
            with col5:
                if st.button("删除", key=f"delete_{record['id']}"):
                    if db.delete_record(record['id']):
                        st.success("记录已删除")
                        st.rerun()
                    else:
                        st.error("删除失败")

    # 查看详细记录
    if 'viewing_record_id' in st.session_state:
        display_record_detail(st.session_state.viewing_record_id)

def display_add_to_monitor_dialog(record):
    """显示加入监测的对话框"""
    st.markdown("---")
    st.subheader("➕ 加入监测")

    final_decision = record['final_decision']

    # 从final_decision中提取关键数据
    if isinstance(final_decision, dict):
        # 解析进场区间
        entry_range_str = final_decision.get('entry_range', 'N/A')
        entry_min = 0.0
        entry_max = 0.0

        # 尝试解析进场区间字符串，支持多种格式
        if entry_range_str and entry_range_str != 'N/A':
            try:
                import re
                # 移除常见的前缀和单位
                clean_str = str(entry_range_str).replace('¥', '').replace('元', '').replace('$', '')
                # 使用正则表达式提取数字
                # 支持格式：10.5-12.0, 10.5 - 12.0, 10.5~12.0, 10.5至12.0 等
                numbers = re.findall(r'\d+\.?\d*', clean_str)
                if len(numbers) >= 2:
                    entry_min = float(numbers[0])
                    entry_max = float(numbers[1])
            except:
                # 如果解析失败，尝试用分隔符split
                try:
                    clean_str = str(entry_range_str).replace('¥', '').replace('元', '').replace('$', '')
                    # 尝试多种分隔符
                    for sep in ['-', '~', '至', '到']:
                        if sep in clean_str:
                            parts = clean_str.split(sep)
                            if len(parts) == 2:
                                entry_min = float(parts[0].strip())
                                entry_max = float(parts[1].strip())
                                break
                except:
                    pass

        # 提取止盈和止损
        take_profit_str = final_decision.get('take_profit', 'N/A')
        stop_loss_str = final_decision.get('stop_loss', 'N/A')

        take_profit = 0.0
        stop_loss = 0.0

        # 解析止盈位
        if take_profit_str and take_profit_str != 'N/A':
            try:
                import re
                # 移除单位和符号
                clean_str = str(take_profit_str).replace('¥', '').replace('元', '').replace('$', '').strip()
                # 提取第一个数字
                numbers = re.findall(r'\d+\.?\d*', clean_str)
                if numbers:
                    take_profit = float(numbers[0])
            except:
                pass

        # 解析止损位
        if stop_loss_str and stop_loss_str != 'N/A':
            try:
                import re
                # 移除单位和符号
                clean_str = str(stop_loss_str).replace('¥', '').replace('元', '').replace('$', '').strip()
                # 提取第一个数字
                numbers = re.findall(r'\d+\.?\d*', clean_str)
                if numbers:
                    stop_loss = float(numbers[0])
            except:
                pass

        # 获取评级
        rating = final_decision.get('rating', '买入')

        # 检查是否已经在监测列表中
        from monitor_db import monitor_db
        existing_stocks = monitor_db.get_monitored_stocks()
        is_duplicate = any(stock['symbol'] == record['symbol'] for stock in existing_stocks)

        if is_duplicate:
            st.warning(f"{record['symbol']} 已经在监测列表中。继续添加将创建重复监测项。")

        st.info(f"""
        **从分析结果中提取的数据：**
        - 进场区间: {entry_min} - {entry_max}
        - 止盈位: {take_profit if take_profit > 0 else '未设置'}
        - 止损位: {stop_loss if stop_loss > 0 else '未设置'}
        - 投资评级: {rating}
        """)

        # 显示表单供用户确认或修改
        with st.form(key=f"monitor_form_{record['id']}"):
            st.markdown("**请确认或修改监测参数：**")

            col1, col2 = st.columns([1, 1])

            with col1:
                st.subheader("关键位置")
                new_entry_min = st.number_input("进场区间最低价", value=float(entry_min), step=0.01, format="%.2f")
                new_entry_max = st.number_input("进场区间最高价", value=float(entry_max), step=0.01, format="%.2f")
                new_take_profit = st.number_input("止盈价位", value=float(take_profit), step=0.01, format="%.2f")
                new_stop_loss = st.number_input("止损价位", value=float(stop_loss), step=0.01, format="%.2f")

            with col2:
                st.subheader("监测设置")
                check_interval = st.slider("监测间隔(分钟)", 5, 120, 30)
                notification_enabled = st.checkbox("启用通知", value=True)
                new_rating = st.selectbox("投资评级", ["买入", "持有", "卖出"],
                                         index=["买入", "持有", "卖出"].index(rating) if rating in ["买入", "持有", "卖出"] else 0)

            col_a, col_b, col_c = st.columns(3)

            with col_a:
                submit = st.form_submit_button("确认加入监测", type="primary", width='stretch')

            with col_b:
                cancel = st.form_submit_button("取消", width='stretch')

            if submit:
                if new_entry_min > 0 and new_entry_max > 0 and new_entry_max > new_entry_min:
                    try:
                        # 添加到监测数据库
                        entry_range = {"min": new_entry_min, "max": new_entry_max}

                        stock_id = monitor_db.add_monitored_stock(
                            symbol=record['symbol'],
                            name=record['stock_name'],
                            rating=new_rating,
                            entry_range=entry_range,
                            take_profit=new_take_profit if new_take_profit > 0 else None,
                            stop_loss=new_stop_loss if new_stop_loss > 0 else None,
                            check_interval=check_interval,
                            notification_enabled=notification_enabled
                        )

                        st.success(f"已成功将 {record['symbol']} 加入监测列表！")
                        st.balloons()

                        # 立即更新一次价格
                        from monitor_service import monitor_service
                        monitor_service.manual_update_stock(stock_id)

                        # 清理session state并跳转到监测页面
                        if 'add_to_monitor_id' in st.session_state:
                            del st.session_state.add_to_monitor_id
                        if 'viewing_record_id' in st.session_state:
                            del st.session_state.viewing_record_id
                        if 'show_history' in st.session_state:
                            del st.session_state.show_history

                        # 设置跳转到监测页面
                        st.session_state.show_monitor = True
                        st.session_state.monitor_jump_highlight = record['symbol']  # 标记要高亮显示的股票

                        time.sleep(1.5)
                        st.rerun()

                    except Exception as e:
                        st.error(f"加入监测失败: {str(e)}")
                else:
                    st.error("请输入有效的进场区间（最低价应小于最高价，且都大于0）")

            if cancel:
                if 'add_to_monitor_id' in st.session_state:
                    del st.session_state.add_to_monitor_id
                st.rerun()
    else:
        st.warning("无法从分析结果中提取关键数据")
        if st.button("取消"):
            if 'add_to_monitor_id' in st.session_state:
                del st.session_state.add_to_monitor_id
            st.rerun()

def display_record_detail(record_id):
    """显示单条记录的详细信息"""
    st.markdown("---")
    st.subheader("📋 详细分析记录")

    record = db.get_record_by_id(record_id)
    if not record:
        st.error("记录不存在")
        return

    # 基本信息
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("股票代码", record['symbol'])
    with col2:
        st.metric("股票名称", record['stock_name'])
    with col3:
        st.metric("分析时间", record['analysis_date'])

    # 股票基本信息
    st.subheader("股票基本信息")
    stock_info = record['stock_info']
    if stock_info:
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            current_price = stock_info.get('current_price', 'N/A')
            st.metric("当前价格", f"{current_price}")

        with col2:
            change_percent = stock_info.get('change_percent', 'N/A')
            if isinstance(change_percent, (int, float)):
                st.metric("涨跌幅", f"{change_percent:.2f}%", f"{change_percent:.2f}%")
            else:
                st.metric("涨跌幅", f"{change_percent}")

        with col3:
            pe_ratio = stock_info.get('pe_ratio', 'N/A')
            st.metric("市盈率", f"{pe_ratio}")

        with col4:
            pb_ratio = stock_info.get('pb_ratio', 'N/A')
            st.metric("市净率", f"{pb_ratio}")

        with col5:
            market_cap = stock_info.get('market_cap', 'N/A')
            if isinstance(market_cap, (int, float)):
                market_cap_str = f"{market_cap/1e9:.2f}B" if market_cap > 1e9 else f"{market_cap/1e6:.2f}M"
                st.metric("市值", market_cap_str)
            else:
                st.metric("市值", f"{market_cap}")

    # 各分析师报告
    st.subheader("AI分析师团队报告")
    agents_results = record['agents_results']
    if agents_results:
        tab_names = []
        tab_contents = []

        for agent_key, agent_result in agents_results.items():
            agent_name = agent_result.get('agent_name', '未知分析师')
            tab_names.append(agent_name)
            tab_contents.append(agent_result)

        tabs = st.tabs(tab_names)

        for i, tab in enumerate(tabs):
            with tab:
                agent_result = tab_contents[i]

                st.markdown(f"""
                <div class="agent-card">
                    <h4>👨‍{agent_result.get('agent_name', '未知')}</h4>
                    <p><strong>职责：</strong>{agent_result.get('agent_role', '未知')}</p>
                    <p><strong>关注领域：</strong>{', '.join(agent_result.get('focus_areas', []))}</p>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("**📄 分析报告:**")
                st.write(agent_result.get('analysis', '暂无分析'))

    # 团队讨论
    st.subheader("🤝 分析团队讨论")
    discussion_result = record['discussion_result']
    if discussion_result:
        st.markdown("""
        <div class="agent-card">
            <h4>💭 团队综合讨论</h4>
        </div>
        """, unsafe_allow_html=True)
        st.write(discussion_result)

    # 最终决策
    st.subheader("📋 最终投资决策")
    final_decision = record['final_decision']
    if final_decision:
        if isinstance(final_decision, dict) and "decision_text" not in final_decision:
            col1, col2 = st.columns([1, 2])

            with col1:
                rating = final_decision.get('rating', '未知')
                rating_color = {"买入": "🟢", "持有": "🟡", "卖出": "🔴"}.get(rating, "⚪")

                st.markdown(f"""
                <div class="decision-card">
                    <h3 style="text-align: center;">{rating_color} {rating}</h3>
                    <h4 style="text-align: center;">投资评级</h4>
                </div>
                """, unsafe_allow_html=True)

                confidence = final_decision.get('confidence_level', 'N/A')
                st.metric("信心度", f"{confidence}/10")

                target_price = final_decision.get('target_price', 'N/A')
                st.metric("目标价格", f"{target_price}")

                position_size = final_decision.get('position_size', 'N/A')
                st.metric("建议仓位", f"{position_size}")

            with col2:
                st.markdown("**操作建议:**")
                st.write(final_decision.get('operation_advice', '暂无建议'))

                st.markdown("**📍 关键位置:**")
                col2_1, col2_2 = st.columns(2)

                with col2_1:
                    st.write(f"**进场区间:** {final_decision.get('entry_range', 'N/A')}")
                    st.write(f"**止盈位:** {final_decision.get('take_profit', 'N/A')}")

                with col2_2:
                    st.write(f"**止损位:** {final_decision.get('stop_loss', 'N/A')}")
                    st.write(f"**持有周期:** {final_decision.get('holding_period', 'N/A')}")
        else:
            decision_text = final_decision.get('decision_text', str(final_decision))
            st.write(decision_text)

    # 加入监测功能
    st.markdown("---")
    st.subheader("操作")

    # 检查是否需要显示加入监测的对话框
    if 'add_to_monitor_id' in st.session_state and st.session_state.add_to_monitor_id == record_id:
        display_add_to_monitor_dialog(record)
    else:
        # 只有在不显示对话框时才显示按钮
        col1, col2 = st.columns([1, 3])

        with col1:
            if st.button("➕ 加入监测", type="primary", width='stretch'):
                st.session_state.add_to_monitor_id = record_id
                st.rerun()

    # 返回按钮
    st.markdown("---")
    if st.button("⬅️ 返回历史记录列表"):
        if 'viewing_record_id' in st.session_state:
            del st.session_state.viewing_record_id
        if 'add_to_monitor_id' in st.session_state:
            del st.session_state.add_to_monitor_id
        st.rerun()

def display_config_manager():
    """显示环境配置管理界面"""
    st.subheader("环境配置管理")



    # 获取当前配置
    config_info = config_manager.get_config_info()

    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs(["📝 基本配置", "数据源配置", "量化交易配置", "通知配置"])

    # 使用session_state保存临时配置
    if 'temp_config' not in st.session_state:
        st.session_state.temp_config = {key: info["value"] for key, info in config_info.items()}

    with tab1:
        st.markdown("### DeepSeek API配置")


    # DeepSeek API Key
        api_key_info = config_info["DEEPSEEK_API_KEY"]
        current_api_key = st.session_state.temp_config.get("DEEPSEEK_API_KEY", "")

        new_api_key = st.text_input(
            f"🔑 {api_key_info['description']} {'*' if api_key_info['required'] else ''}",
            value=current_api_key,
            type="password",
            help="从 https://platform.deepseek.com 获取API密钥",
            key="input_deepseek_api_key"
        )
        st.session_state.temp_config["DEEPSEEK_API_KEY"] = new_api_key



        st.markdown("---")

        # DeepSeek Base URL
        base_url_info = config_info["DEEPSEEK_BASE_URL"]
        current_base_url = st.session_state.temp_config.get("DEEPSEEK_BASE_URL", "")

        new_base_url = st.text_input(
            f"🌐 {base_url_info['description']}",
            value=current_base_url,
            help="一般无需修改，保持默认即可",
            key="input_deepseek_base_url"
        )
        st.session_state.temp_config["DEEPSEEK_BASE_URL"] = new_base_url

        st.markdown("---")

        st.caption("这里配置的是应用重启后的默认模型；侧边栏下拉框可临时覆盖当前会话中的模型选择。")

        lightweight_model_info = config_info["LIGHTWEIGHT_MODEL_NAME"]
        current_lightweight_model = st.session_state.temp_config.get(
            "LIGHTWEIGHT_MODEL_NAME",
            "deepseek-chat",
        )

        new_lightweight_model = st.text_input(
            f"{lightweight_model_info['description']}",
            value=current_lightweight_model,
            help="轻量任务默认使用的 OpenAI 兼容模型名称，保存后重启生效",
            key="input_lightweight_model_name"
        )
        st.session_state.temp_config["LIGHTWEIGHT_MODEL_NAME"] = new_lightweight_model.strip()

        lightweight_model_options_info = config_info.get(
            "LIGHTWEIGHT_MODEL_OPTIONS",
            {"description": "轻量模型下拉候选（逗号或换行分隔）"}
        )
        current_lightweight_model_options = st.session_state.temp_config.get(
            "LIGHTWEIGHT_MODEL_OPTIONS",
            ""
        )
        new_lightweight_model_options = st.text_area(
            f"{lightweight_model_options_info['description']}",
            value=current_lightweight_model_options,
            height=80,
            help="侧边栏轻量模型下拉候选，留空时仅保留当前轻量模型",
            key="input_lightweight_model_options"
        )
        st.session_state.temp_config["LIGHTWEIGHT_MODEL_OPTIONS"] = new_lightweight_model_options.strip()

        reasoning_model_info = config_info["REASONING_MODEL_NAME"]
        current_reasoning_model = st.session_state.temp_config.get(
            "REASONING_MODEL_NAME",
            "deepseek-reasoner",
        )

        new_reasoning_model = st.text_input(
            f"{reasoning_model_info['description']}",
            value=current_reasoning_model,
            help="强推理任务默认使用的 OpenAI 兼容模型名称，保存后重启生效",
            key="input_reasoning_model_name"
        )
        st.session_state.temp_config["REASONING_MODEL_NAME"] = new_reasoning_model.strip()

        reasoning_model_options_info = config_info.get(
            "REASONING_MODEL_OPTIONS",
            {"description": "推理模型下拉候选（逗号或换行分隔）"}
        )
        current_reasoning_model_options = st.session_state.temp_config.get(
            "REASONING_MODEL_OPTIONS",
            ""
        )
        new_reasoning_model_options = st.text_area(
            f"{reasoning_model_options_info['description']}",
            value=current_reasoning_model_options,
            height=80,
            help="侧边栏推理模型下拉候选，留空时仅保留当前推理模型",
            key="input_reasoning_model_options"
        )
        st.session_state.temp_config["REASONING_MODEL_OPTIONS"] = new_reasoning_model_options.strip()





        st.markdown("---")
        st.markdown("### 网站备案配置")

        icp_number_info = config_info["ICP_NUMBER"]
        current_icp_number = st.session_state.temp_config.get("ICP_NUMBER", "")
        new_icp_number = st.text_input(
            f"📝 {icp_number_info['description']}",
            value=current_icp_number,
            placeholder="例如：京ICP备12345678号",
            key="input_icp_number"
        )
        st.session_state.temp_config["ICP_NUMBER"] = new_icp_number.strip()

        icp_link_info = config_info.get(
            "ICP_LINK",
            {"description": "备案号跳转地址（留空则仅显示文本）"}
        )
        current_icp_link = st.session_state.temp_config.get(
            "ICP_LINK",
            "https://beian.miit.gov.cn/"
        )
        new_icp_link = st.text_input(
            f"🔗 {icp_link_info['description']}",
            value=current_icp_link,
            placeholder="https://beian.miit.gov.cn/",
            key="input_icp_link"
        )
        st.session_state.temp_config["ICP_LINK"] = new_icp_link.strip()



        st.markdown("---")
        st.markdown("### 管理员登录配置")

        admin_password_info = config_info["ADMIN_PASSWORD"]
        current_admin_password = st.session_state.temp_config.get("ADMIN_PASSWORD", "")
        new_admin_password = st.text_input(
            f"🔑 {admin_password_info['description']}",
            value=current_admin_password,
            type="password",
            key="input_admin_password"
        )
        st.session_state.temp_config["ADMIN_PASSWORD"] = new_admin_password

        admin_password_hash_info = config_info.get(
            "ADMIN_PASSWORD_HASH",
            {"description": "管理员密码哈希（推荐，优先于明文密码）"}
        )
        current_admin_password_hash = st.session_state.temp_config.get("ADMIN_PASSWORD_HASH", "")
        new_admin_password_hash = st.text_input(
            f"{admin_password_hash_info['description']}",
            value=current_admin_password_hash,
            type="password",
            placeholder="pbkdf2_sha256$迭代次数$salt_hex$hash_hex",
            key="input_admin_password_hash"
        )
        st.session_state.temp_config["ADMIN_PASSWORD_HASH"] = new_admin_password_hash.strip()


        login_max_attempts = st.text_input(
            "登录最大失败次数",
            value=st.session_state.temp_config.get("LOGIN_MAX_ATTEMPTS", "5"),
            key="input_login_max_attempts"
        )
        st.session_state.temp_config["LOGIN_MAX_ATTEMPTS"] = login_max_attempts.strip()

        login_lockout_seconds = st.text_input(
            "登录锁定时长（秒）",
            value=st.session_state.temp_config.get("LOGIN_LOCKOUT_SECONDS", "300"),
            key="input_login_lockout_seconds"
        )
        st.session_state.temp_config["LOGIN_LOCKOUT_SECONDS"] = login_lockout_seconds.strip()

        admin_session_ttl_seconds = st.text_input(
            "管理员会话有效期（秒）",
            value=st.session_state.temp_config.get("ADMIN_SESSION_TTL_SECONDS", "28800"),
            key="input_admin_session_ttl_seconds"
        )
        st.session_state.temp_config["ADMIN_SESSION_TTL_SECONDS"] = admin_session_ttl_seconds.strip()

    with tab2:
        st.markdown("### Tushare数据接口（可选）")



        tushare_info = config_info["TUSHARE_TOKEN"]
        current_tushare = st.session_state.temp_config.get("TUSHARE_TOKEN", "")

        new_tushare = st.text_input(
            f"🎫 {tushare_info['description']}",
            value=current_tushare,
            type="password",
            help="从 https://tushare.pro 获取Token",
            key="input_tushare_token"
        )
        st.session_state.temp_config["TUSHARE_TOKEN"] = new_tushare

        tushare_url_info = config_info.get("TUSHARE_URL", {"description": "Tushare API地址"})
        current_tushare_url = st.session_state.temp_config.get("TUSHARE_URL", "https://api.tushare.pro")
        new_tushare_url = st.text_input(
            f"🌐 {tushare_url_info['description']}",
            value=current_tushare_url,
            help="一般无需修改",
            key="input_tushare_url"
        )
        st.session_state.temp_config["TUSHARE_URL"] = new_tushare_url

        st.markdown("---")
        st.markdown("### TDX 数据源配置（推荐）")
        
        # 启用开关
        tdx_enabled_info = config_info["TDX_ENABLED"]
        current_tdx_enabled = st.session_state.temp_config.get("TDX_ENABLED", "false") == "true"
        
        new_tdx_enabled = st.checkbox(
            f"✅ {tdx_enabled_info['description']}",
            value=current_tdx_enabled,
            key="input_tdx_enabled"
        )
        st.session_state.temp_config["TDX_ENABLED"] = "true" if new_tdx_enabled else "false"
        
        # 接口地址
        tdx_url_info = config_info["TDX_BASE_URL"]
        current_tdx_url = st.session_state.temp_config.get("TDX_BASE_URL", "http://127.0.0.1:8181")
        
        new_tdx_url = st.text_input(
            f"🌐 {tdx_url_info['description']}",
            value=current_tdx_url,
            disabled=not new_tdx_enabled,
            key="input_tdx_base_url"
        )
        st.session_state.temp_config["TDX_BASE_URL"] = new_tdx_url






    with tab3:
        st.markdown("### MiniQMT量化交易配置（可选）")


        # 启用开关
        miniqmt_enabled_info = config_info["MINIQMT_ENABLED"]
        current_enabled = st.session_state.temp_config.get("MINIQMT_ENABLED", "false") == "true"

        new_enabled = st.checkbox(
            "启用MiniQMT量化交易",
            value=current_enabled,
            help="开启后可以使用量化交易功能",
            key="input_miniqmt_enabled"
        )
        st.session_state.temp_config["MINIQMT_ENABLED"] = "true" if new_enabled else "false"

        # 其他配置
        col1, col2 = st.columns(2)

        with col1:
            account_id_info = config_info["MINIQMT_ACCOUNT_ID"]
            current_account_id = st.session_state.temp_config.get("MINIQMT_ACCOUNT_ID", "")

            new_account_id = st.text_input(
                f"🆔 {account_id_info['description']}",
                value=current_account_id,
                disabled=not new_enabled,
                key="input_miniqmt_account_id"
            )
            st.session_state.temp_config["MINIQMT_ACCOUNT_ID"] = new_account_id

            host_info = config_info["MINIQMT_HOST"]
            current_host = st.session_state.temp_config.get("MINIQMT_HOST", "")

            new_host = st.text_input(
                f"🖥️ {host_info['description']}",
                value=current_host,
                disabled=not new_enabled,
                key="input_miniqmt_host"
            )
            st.session_state.temp_config["MINIQMT_HOST"] = new_host

        with col2:
            port_info = config_info["MINIQMT_PORT"]
            current_port = st.session_state.temp_config.get("MINIQMT_PORT", "")

            new_port = st.text_input(
                f"🔌 {port_info['description']}",
                value=current_port,
                disabled=not new_enabled,
                key="input_miniqmt_port"
            )
            st.session_state.temp_config["MINIQMT_PORT"] = new_port



        st.warning("警告：量化交易涉及真实资金操作，请谨慎配置和使用！")

    with tab4:
        st.markdown("### 通知配置")


        # 创建两列布局
        col_email, col_webhook = st.columns(2)

        with col_email:
            st.markdown("#### 📧 邮件通知")

            # 邮件启用开关
            email_enabled_info = config_info.get("EMAIL_ENABLED", {"value": "false"})
            current_email_enabled = st.session_state.temp_config.get("EMAIL_ENABLED", "false") == "true"

            new_email_enabled = st.checkbox(
                "启用邮件通知",
                value=current_email_enabled,
                help="开启后可以接收邮件提醒",
                key="input_email_enabled"
            )
            st.session_state.temp_config["EMAIL_ENABLED"] = "true" if new_email_enabled else "false"

            # SMTP服务器
            smtp_server_info = config_info.get("SMTP_SERVER", {"description": "SMTP服务器地址", "value": ""})
            current_smtp_server = st.session_state.temp_config.get("SMTP_SERVER", "")

            new_smtp_server = st.text_input(
                f"📮 {smtp_server_info['description']}",
                value=current_smtp_server,
                disabled=not new_email_enabled,
                placeholder="smtp.qq.com",
                key="input_smtp_server"
            )
            st.session_state.temp_config["SMTP_SERVER"] = new_smtp_server

            # SMTP端口
            smtp_port_info = config_info.get("SMTP_PORT", {"description": "SMTP端口", "value": "587"})
            current_smtp_port = st.session_state.temp_config.get("SMTP_PORT", "587")

            new_smtp_port = st.text_input(
                f"🔌 {smtp_port_info['description']}",
                value=current_smtp_port,
                disabled=not new_email_enabled,
                placeholder="587 (TLS) 或 465 (SSL)",
                key="input_smtp_port"
            )
            st.session_state.temp_config["SMTP_PORT"] = new_smtp_port

            # 发件人邮箱
            email_from_info = config_info.get("EMAIL_FROM", {"description": "发件人邮箱", "value": ""})
            current_email_from = st.session_state.temp_config.get("EMAIL_FROM", "")

            new_email_from = st.text_input(
                f"📤 {email_from_info['description']}",
                value=current_email_from,
                disabled=not new_email_enabled,
                placeholder="your-email@qq.com",
                key="input_email_from"
            )
            st.session_state.temp_config["EMAIL_FROM"] = new_email_from

            # 邮箱授权码
            email_password_info = config_info.get("EMAIL_PASSWORD", {"description": "邮箱授权码", "value": ""})
            current_email_password = st.session_state.temp_config.get("EMAIL_PASSWORD", "")

            new_email_password = st.text_input(
                f"{email_password_info['description']}",
                value=current_email_password,
                type="password",
                disabled=not new_email_enabled,
                help="不是邮箱登录密码，而是SMTP授权码",
                key="input_email_password"
            )
            st.session_state.temp_config["EMAIL_PASSWORD"] = new_email_password

            # 收件人邮箱
            email_to_info = config_info.get("EMAIL_TO", {"description": "收件人邮箱", "value": ""})
            current_email_to = st.session_state.temp_config.get("EMAIL_TO", "")

            new_email_to = st.text_input(
                f"📥 {email_to_info['description']}",
                value=current_email_to,
                disabled=not new_email_enabled,
                placeholder="receiver@qq.com",
                key="input_email_to"
            )
            st.session_state.temp_config["EMAIL_TO"] = new_email_to





        with col_webhook:
            st.markdown("#### 📱 Webhook通知")

            # Webhook启用开关
            webhook_enabled_info = config_info.get("WEBHOOK_ENABLED", {"value": "false"})
            current_webhook_enabled = st.session_state.temp_config.get("WEBHOOK_ENABLED", "false") == "true"

            new_webhook_enabled = st.checkbox(
                "启用Webhook通知",
                value=current_webhook_enabled,
                help="开启后可以发送到钉钉或飞书群",
                key="input_webhook_enabled"
            )
            st.session_state.temp_config["WEBHOOK_ENABLED"] = "true" if new_webhook_enabled else "false"

            # Webhook类型选择
            webhook_type_info = config_info.get("WEBHOOK_TYPE", {"description": "Webhook类型", "value": "dingtalk", "options": ["dingtalk", "feishu"]})
            current_webhook_type = st.session_state.temp_config.get("WEBHOOK_TYPE", "dingtalk")

            new_webhook_type = st.selectbox(
                f"📲 {webhook_type_info['description']}",
                options=webhook_type_info.get('options', ["dingtalk", "feishu"]),
                index=0 if current_webhook_type == "dingtalk" else 1,
                disabled=not new_webhook_enabled,
                key="input_webhook_type"
            )
            st.session_state.temp_config["WEBHOOK_TYPE"] = new_webhook_type

            # Webhook URL
            webhook_url_info = config_info.get("WEBHOOK_URL", {"description": "Webhook地址", "value": ""})
            current_webhook_url = st.session_state.temp_config.get("WEBHOOK_URL", "")

            new_webhook_url = st.text_input(
                f"🔗 {webhook_url_info['description']}",
                value=current_webhook_url,
                disabled=not new_webhook_enabled,
                placeholder="https://oapi.dingtalk.com/robot/send?access_token=...",
                key="input_webhook_url"
            )
            st.session_state.temp_config["WEBHOOK_URL"] = new_webhook_url

            # Webhook自定义关键词（钉钉安全验证）
            webhook_keyword_info = config_info.get("WEBHOOK_KEYWORD", {"description": "自定义关键词（钉钉安全验证）", "value": "aiagents通知"})
            current_webhook_keyword = st.session_state.temp_config.get("WEBHOOK_KEYWORD", "aiagents通知")

            new_webhook_keyword = st.text_input(
                f"🔑 {webhook_keyword_info['description']}",
                value=current_webhook_keyword,
                disabled=not new_webhook_enabled or new_webhook_type != "dingtalk",
                placeholder="aiagents通知",
                help="钉钉机器人安全设置中的自定义关键词，飞书不需要此设置",
                key="input_webhook_keyword"
            )
            st.session_state.temp_config["WEBHOOK_KEYWORD"] = new_webhook_keyword

            # 测试连通按钮
            if new_webhook_enabled and new_webhook_url:
                if st.button("🧪 测试Webhook连通", width='stretch', key="test_webhook_btn"):
                    with st.spinner("正在发送测试消息..."):
                        # 临时更新配置
                        temp_env_backup = {}
                        for key in ["WEBHOOK_ENABLED", "WEBHOOK_TYPE", "WEBHOOK_URL", "WEBHOOK_KEYWORD"]:
                            temp_env_backup[key] = os.getenv(key)
                            os.environ[key] = st.session_state.temp_config.get(key, "")

                        try:
                            # 创建临时通知服务实例
                            from notification_service import NotificationService
                            temp_notification_service = NotificationService()
                            success, message = temp_notification_service.send_test_webhook()

                            if success:
                                st.success(f"{message}")
                            else:
                                st.error(f"{message}")
                        except Exception as e:
                            st.error(f"测试失败: {str(e)}")
                        finally:
                            # 恢复环境变量
                            for key, value in temp_env_backup.items():
                                if value is not None:
                                    os.environ[key] = value
                                elif key in os.environ:
                                    del os.environ[key]



            # 显示帮助信息


    # 操作按钮
    st.markdown("---")
    col1, col2, col3, col4 = st.columns([1, 1, 1, 2])

    with col1:
        if st.button("💾 保存配置", type="primary", width='stretch'):
            # 验证配置
            is_valid, message = config_manager.validate_config(st.session_state.temp_config)

            if is_valid:
                # 保存配置
                if config_manager.write_env(st.session_state.temp_config):
                    st.success("配置已保存到 .env 文件")
                    st.info("ℹ️ 请重启应用使配置生效")

                    # 尝试重新加载配置
                    try:
                        config_manager.reload_config()
                        st.success("配置已重新加载")
                    except Exception as e:
                        st.warning(f"配置重新加载失败: {e}")

                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("保存配置失败")
            else:
                st.error(f"配置验证失败: {message}")

    with col2:
        if st.button("重置", width='stretch'):
            # 重置为当前文件中的值
            st.session_state.temp_config = {key: info["value"] for key, info in config_info.items()}
            st.success("已重置为当前配置")
            st.rerun()

    with col3:
        if st.button("⬅️ 返回", width='stretch'):
            if 'show_config' in st.session_state:
                del st.session_state.show_config
            if 'temp_config' in st.session_state:
                del st.session_state.temp_config
            st.rerun()



def display_batch_analysis_results(results, period):
    """显示批量分析结果（对比视图）"""

    st.subheader("批量分析结果对比")

    # 统计信息
    total = len(results)
    success_results = [r for r in results if r['success']]
    failed_results = [r for r in results if not r['success']]
    saved_count = sum(1 for r in results if r.get('saved_to_db', False))

    # 显示统计
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("总数", total)
    with col2:
        st.metric("成功", len(success_results), delta=None, delta_color="normal")
    with col3:
        st.metric("失败", len(failed_results), delta=None, delta_color="inverse")
    with col4:
        st.metric("已保存", saved_count, delta=None, delta_color="normal")

    # 提示信息
    if saved_count > 0:
        st.info(f"💾 已有 {saved_count} 只股票的分析结果保存到历史记录，可在侧边栏点击「历史记录」查看")

    st.markdown("---")

    # 失败的股票列表
    if failed_results:
        with st.expander(f"查看失败的 {len(failed_results)} 只股票", expanded=False):
            for result in failed_results:
                st.error(f"**{result['symbol']}**: {result.get('error', '未知错误')}")

    # 保存失败的股票列表
    save_failed_results = [r for r in success_results if not r.get('saved_to_db', False)]
    if save_failed_results:
        with st.expander(f"查看分析成功但保存失败的 {len(save_failed_results)} 只股票", expanded=False):
            for result in save_failed_results:
                db_error = result.get('db_error', '未知错误')
                st.warning(f"**{result['symbol']} - {result['stock_info'].get('name', 'N/A')}**: {db_error}")

    # 成功的股票分析结果
    if not success_results:
        st.warning("没有成功分析的股票")
        return

    # 创建对比视图选项
    view_mode = st.radio(
        "显示模式",
        ["对比表格", "详细卡片"],
        horizontal=True,
        help="对比表格：横向对比多只股票；详细卡片：逐个查看详细分析"
    )

    if view_mode == "对比表格":
        # 表格对比视图
        display_comparison_table(success_results)
    else:
        # 详细卡片视图
        display_detailed_cards(success_results, period)

def display_comparison_table(results):
    """显示对比表格"""
    import pandas as pd

    st.subheader("📋 股票对比表格")

    # 构建对比数据
    comparison_data = []
    for result in results:
        stock_info = result['stock_info']
        indicators = result.get('indicators', {})
        final_decision = result['final_decision']

        # 解析评级
        if isinstance(final_decision, dict):
            rating = final_decision.get('rating', 'N/A')
            confidence = final_decision.get('confidence_level', 'N/A')
            target_price = final_decision.get('target_price', 'N/A')
        else:
            rating = 'N/A'
            confidence = 'N/A'
            target_price = 'N/A'

        # 确保信心度为字符串类型，避免类型混合导致的序列化错误
        if isinstance(confidence, (int, float)):
            confidence = str(confidence)

        row = {
            '股票代码': stock_info.get('symbol', 'N/A'),
            '股票名称': stock_info.get('name', 'N/A'),
            '当前价格': stock_info.get('current_price', 'N/A'),
            '涨跌幅(%)': stock_info.get('change_percent', 'N/A'),
            '市盈率': stock_info.get('pe_ratio', 'N/A'),
            '市净率': stock_info.get('pb_ratio', 'N/A'),
            'RSI': indicators.get('rsi', 'N/A'),
            'MACD': indicators.get('macd', 'N/A'),
            '投资评级': rating,
            '信心度': confidence,
            '目标价格': target_price
        }
        comparison_data.append(row)

    # 创建DataFrame
    df = pd.DataFrame(comparison_data)

    # 应用样式
    # 显示表格（不使用样式，避免matplotlib导入问题）
    st.dataframe(
        df,
        width='stretch',
        height=400
    )

    # 添加评级说明
    st.caption("投资评级说明：强烈买入 > 买入 > 持有 > 卖出 > 强烈卖出")

    # 添加筛选功能
    st.markdown("---")
    st.subheader("快速筛选")

    col1, col2 = st.columns(2)
    with col1:
        rating_filter = st.multiselect(
            "按评级筛选",
            options=df['投资评级'].unique().tolist(),
            default=df['投资评级'].unique().tolist()
        )

    with col2:
        # 按涨跌幅排序
        sort_by = st.selectbox(
            "排序方式",
            ["默认", "涨跌幅降序", "涨跌幅升序", "信心度降序", "RSI降序"]
        )

    # 应用筛选
    filtered_df = df[df['投资评级'].isin(rating_filter)]

    # 应用排序
    if sort_by == "涨跌幅降序":
        filtered_df = filtered_df.sort_values('涨跌幅(%)', ascending=False)
    elif sort_by == "涨跌幅升序":
        filtered_df = filtered_df.sort_values('涨跌幅(%)', ascending=True)
    elif sort_by == "信心度降序":
        filtered_df = filtered_df.sort_values('信心度', ascending=False)
    elif sort_by == "RSI降序":
        filtered_df = filtered_df.sort_values('RSI', ascending=False)

    if not filtered_df.empty:
        st.dataframe(filtered_df, width='stretch')
    else:
        st.info("没有符合条件的股票")

def display_detailed_cards(results, period):
    """显示详细卡片视图"""

    st.subheader("📇 详细分析卡片")

    # 选择要查看的股票
    stock_options = [f"{r['stock_info']['symbol']} - {r['stock_info']['name']}" for r in results]
    selected_stock = st.selectbox("选择股票", options=stock_options)

    # 找到对应的结果
    selected_index = stock_options.index(selected_stock)
    result = results[selected_index]

    # 显示详细分析
    stock_info = result['stock_info']
    indicators = result['indicators']
    agents_results = result['agents_results']
    discussion_result = result['discussion_result']
    final_decision = result['final_decision']

    # 获取股票数据用于显示图表
    try:
        stock_info_current, stock_data, _ = get_stock_data(stock_info['symbol'], period)

        # 显示股票基本信息
        display_stock_info(stock_info, indicators)

        # 显示股票图表
        if stock_data is not None:
            display_stock_chart(stock_data, stock_info)

        # 显示各分析师报告
        display_agents_analysis(agents_results)

        # 显示团队讨论
        display_team_discussion(discussion_result)

        # 显示最终决策
        display_final_decision(final_decision, stock_info, agents_results, discussion_result)

    except Exception as e:
        st.error(f"显示详细信息时出错: {str(e)}")

def _show_login_page():
    """管理员密码登录页面"""
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col2:
        with st.container(border=True):
            st.markdown('<p style="text-align: center; font-size: 1.4rem; font-weight: 600; margin-bottom: 0.5rem;">系统登录</p>', unsafe_allow_html=True)
            st.markdown('<p style="text-align: center; color: rgba(255,255,255,0.6); font-size: 0.85rem; margin-bottom: 2rem;">复合多AI智能体股票团队分析系统</p>', unsafe_allow_html=True)

            now_ts = int(time.time())
            lock_until = int(st.session_state.get("login_lock_until", 0))
            is_locked = now_ts < lock_until
            if is_locked:
                remain = lock_until - now_ts
                st.error(f"尝试次数过多，请 {remain} 秒后重试")

            password = st.text_input(
                "管理员密码",
                type="password",
                placeholder="请输入管理员密码",
                key="login_password_input",
                disabled=is_locked,
                label_visibility="collapsed"
            )

            if st.button("登 录", type="primary", width='stretch', disabled=is_locked):
                if _verify_admin_password(password):
                    st.session_state.authenticated = True
                    st.session_state.authenticated_at = int(time.time())
                    st.session_state.login_fail_count = 0
                    st.session_state.login_lock_until = 0
                    st.rerun()
                else:
                    fail_count = int(st.session_state.get("login_fail_count", 0)) + 1
                    st.session_state.login_fail_count = fail_count
                    if fail_count >= max(config.LOGIN_MAX_ATTEMPTS, 1):
                        st.session_state.login_lock_until = int(time.time()) + max(config.LOGIN_LOCKOUT_SECONDS, 1)
                        st.session_state.login_fail_count = 0
                    st.error("密码错误，请重试")


def _verify_admin_password(input_password: str) -> bool:
    """Verify admin password using hash (preferred) or plain text (compatible)."""
    pwd = input_password or ""
    hash_value = (getattr(config, "ADMIN_PASSWORD_HASH", "") or "").strip()
    if hash_value:
        # Format: pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>
        try:
            algo, iter_text, salt_hex, digest_hex = hash_value.split("$", 3)
            if algo != "pbkdf2_sha256":
                return False
            iterations = int(iter_text)
            salt = bytes.fromhex(salt_hex)
            expected = bytes.fromhex(digest_hex)
            import hashlib
            import hmac
            computed = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt, iterations)
            return hmac.compare_digest(computed, expected)
        except Exception:
            return False

    plain = (config.ADMIN_PASSWORD or "").strip()
    import hmac
    return bool(plain) and hmac.compare_digest(pwd, plain)

if __name__ == "__main__":
    # 管理员密码门控
    if config.ADMIN_PASSWORD or getattr(config, "ADMIN_PASSWORD_HASH", ""):
        if st.session_state.get("authenticated", False):
            authed_at = int(st.session_state.get("authenticated_at", 0))
            ttl = max(getattr(config, "ADMIN_SESSION_TTL_SECONDS", 28800), 60)
            if authed_at <= 0 or int(time.time()) - authed_at > ttl:
                st.session_state.authenticated = False
                st.session_state.pop("authenticated_at", None)
        if not st.session_state.get("authenticated", False):
            _show_login_page()
            st.stop()
    main()
