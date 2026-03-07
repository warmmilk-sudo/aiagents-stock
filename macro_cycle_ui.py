"""
宏观周期分析 - UI界面模块
展示康波周期 + 美林投资时钟 + 中国政策分析的综合结果
"""

import streamlit as st
import time
from datetime import datetime
from macro_cycle_engine import MacroCycleEngine
from macro_cycle_pdf import MacroCyclePDFGenerator, generate_macro_cycle_markdown


def display_macro_cycle(lightweight_model=None, reasoning_model=None):
    """显示宏观周期分析主界面"""
    restore_suppressed = st.session_state.pop("suppress_macro_cycle_restore_once", False)
    if not restore_suppressed and 'macro_cycle_result' not in st.session_state:
        try:
            engine = MacroCycleEngine(
                lightweight_model=lightweight_model,
                reasoning_model=reasoning_model,
            )
            latest_report = engine.get_latest_report()
            if latest_report and latest_report.get("result_parsed"):
                restored_result = dict(latest_report["result_parsed"])
                restored_result.setdefault("success", True)
                restored_result["saved_report"] = latest_report
                restored_result["report_id"] = latest_report.get("id")
                st.session_state.macro_cycle_result = restored_result
                st.session_state.macro_cycle_result_source = "history_restore"
        except Exception:
            pass

    tab1, tab2, tab3 = st.tabs(["周期分析", "历史报告", "理论介绍"])

    with tab1:
        display_analysis_tab(lightweight_model, reasoning_model)

    with tab2:
        display_history_tab(lightweight_model, reasoning_model)

    with tab3:
        display_theory_tab()


def display_analysis_tab(lightweight_model=None, reasoning_model=None):
    """显示分析标签页"""
    with st.expander("分析说明", expanded=False):
        st.caption("康波周期 × 美林投资时钟 × 中国政策分析 — AI驱动的宏观经济周期研判")
        st.markdown("""
        > **分析说明**：本模块基于视频[康波周期理论](https://www.bilibili.com/video/BV1QNcEzREzY)（50-60年长周期）和视频[美林投资时钟](https://www.bilibili.com/video/BV1Zuf5BUEhH)（3-5年中短周期），
        > 结合中国政策环境（第三维度），由4位AI分析师协同研判当前宏观经济所处的周期位置，并给出资产配置建议。
        """)

        st.markdown("""
        **AI分析师团队：**
        - **康波周期分析师** — 60年长周期战略定位（回升→繁荣→衰退→萧条）
        - **美林时钟分析师** — 3-5年中短周期战术定位（复苏→过热→滞胀→衰退）
        - **中国政策分析师** — 政策第三维度（货币/财政/产业/房地产）
        - **首席宏观策略师** — 三维综合研判，最终资产配置建议
        """)

    # 操作按钮
    col1, col2 = st.columns([2, 2])

    with col1:
        analyze_button = st.button("开始宏观周期分析", type="primary", key="macro_analyze")

    with col2:
        if st.button("清除结果", key="macro_clear"):
            if 'macro_cycle_result' in st.session_state:
                del st.session_state.macro_cycle_result
            st.session_state["suppress_macro_cycle_restore_once"] = True
            st.success("已清除分析结果")
            st.rerun()

    st.markdown("---")

    # 开始分析
    if analyze_button:
        if 'macro_cycle_result' in st.session_state:
            del st.session_state.macro_cycle_result

        run_macro_cycle_analysis(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )

    # 显示结果
    if 'macro_cycle_result' in st.session_state:
        result = st.session_state.macro_cycle_result
        if result.get("success"):
            display_analysis_results(result)
        else:
            st.error(f"分析失败: {result.get('error', '未知错误')}")


def display_history_tab(lightweight_model=None, reasoning_model=None):
    """显示历史报告标签页"""
    st.markdown("### 历史报告")

    try:
        engine = MacroCycleEngine(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        reports = engine.get_historical_reports(limit=20)
    except Exception as e:
        st.error(f"加载历史报告失败: {e}")
        return

    if reports.empty:
        st.info("暂无历史报告。")
        return

    st.caption(f"共 {len(reports)} 份已保存报告。")

    for _, report in reports.iterrows():
        report_id = report.get("id")
        analysis_date = report.get("analysis_date", "")
        summary = report.get("summary") or "宏观周期分析报告"
        with st.expander(f"{analysis_date} | 报告 #{report_id}", expanded=False):
            st.info(summary)

            col1, col2 = st.columns([3, 1])
            with col1:
                chief_summary = report.get("chief_summary")
                if chief_summary:
                    st.caption(chief_summary)
            with col2:
                if st.button("删除", key=f"macro_delete_{report_id}", width='content'):
                    if engine.delete_report(report_id):
                        st.success("历史报告已删除。")
                        st.rerun()
                    st.error("删除历史报告失败。")

            detail = engine.get_report_detail(report_id)
            if not detail or not detail.get("result_parsed"):
                st.warning("报告详情缺失。")
                continue

            inline_result = dict(detail["result_parsed"])
            inline_result.setdefault("success", True)
            inline_result["saved_report"] = detail
            inline_result["report_id"] = detail.get("id")
            display_analysis_results(inline_result, show_export=False, key_prefix=f"macro_history_{report_id}")


def run_macro_cycle_analysis(lightweight_model=None, reasoning_model=None):
    """运行宏观周期分析"""

    progress_bar = st.progress(0)
    status_text = st.empty()

    def progress_callback(pct, text):
        progress_bar.progress(pct)
        status_text.text(text)

    try:
        engine = MacroCycleEngine(
            lightweight_model=lightweight_model,
            reasoning_model=reasoning_model,
        )
        result = engine.run_full_analysis(progress_callback=progress_callback)

        if result.get("success"):
            st.session_state.macro_cycle_result = result
            time.sleep(1)
            status_text.empty()
            progress_bar.empty()
            st.rerun()
        else:
            st.error(f"分析失败: {result.get('error', '未知错误')}")

    except Exception as e:
        st.error(f"分析过程出错: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
    finally:
        progress_bar.empty()
        status_text.empty()


def display_analysis_results(result, show_export=True, key_prefix="macro"):
    """显示分析结果"""
    agents = result.get("agents_analysis", {})
    timestamp = result.get("timestamp", "")

    st.success(f"分析完成于 {timestamp}")

    # 数据采集状态
    data_errors = result.get("data_errors", [])
    if data_errors:
        with st.expander("部分数据采集失败（不影响分析）"):
            for err in data_errors:
                st.warning(f"• {err}")

    # 四个分析师报告
    report_tabs = st.tabs([
        "综合策略",
        "康波周期",
        "美林时钟",
        "政策分析"
    ])

    # Tab 1: 首席宏观策略师（综合）
    with report_tabs[0]:
        chief = agents.get("chief", {})
        if chief:
            st.markdown("""
                <div class="metric-card macro-hero-card" style="background: linear-gradient(135deg, #2563eb 0%, #4f46e5 100%); 
                        padding: 1.2rem; border-radius: 12px; margin-bottom: 1rem;
                        color: white;">
                <h3 style="margin: 0; color: white;">首席宏观策略师 — 综合研判</h3>
                <p class="ui-body-text" style="margin: 0.3rem 0 0 0; opacity: 0.9;">
                    整合康波周期 + 美林投资时钟 + 中国政策三维分析，给出最终投资策略
                </p>
            </div>
            """, unsafe_allow_html=True)
            st.markdown(chief.get("analysis", "暂无分析结果"))
        else:
            st.info("暂无综合策略分析结果")

    # Tab 2: 康波周期分析师
    with report_tabs[1]:
        kondratieff = agents.get("kondratieff", {})
        if kondratieff:
            st.markdown("""
                <div class="metric-card macro-hero-card" style="background: linear-gradient(135deg, #0f766e 0%, #1d4ed8 100%); 
                        padding: 1.2rem; border-radius: 12px; margin-bottom: 1rem;
                        color: white;">
                <h3 style="margin: 0; color: white;">康波周期分析师 — 60年长周期定位</h3>
                <p class="ui-body-text" style="margin: 0.3rem 0 0 0; opacity: 0.9;">
                    基于康德拉季耶夫长波理论，判断当前处于第五轮信息技术康波的哪个阶段
                </p>
            </div>
            """, unsafe_allow_html=True)
            st.markdown(kondratieff.get("analysis", "暂无分析结果"))
        else:
            st.info("暂无康波周期分析结果")

    # Tab 3: 美林时钟分析师
    with report_tabs[2]:
        merrill = agents.get("merrill", {})
        if merrill:
            st.markdown("""
                <div class="metric-card macro-hero-card" style="background: linear-gradient(135deg, #d97706 0%, #92400e 100%); 
                        padding: 1.2rem; border-radius: 12px; margin-bottom: 1rem;
                        color: white;">
                <h3 style="margin: 0; color: white;">美林投资时钟分析师 — 中短周期定位</h3>
                <p class="ui-body-text" style="margin: 0.3rem 0 0 0; opacity: 0.9;">
                    基于经济增长+通胀+政策三维度，判断当前处于美林时钟的哪个象限
                </p>
            </div>
            """, unsafe_allow_html=True)
            st.markdown(merrill.get("analysis", "暂无分析结果"))
        else:
            st.info("暂无美林时钟分析结果")

    # Tab 4: 中国政策分析师
    with report_tabs[3]:
        policy = agents.get("policy", {})
        if policy:
            st.markdown("""
                <div class="metric-card macro-hero-card" style="background: linear-gradient(135deg, #475569 0%, #1e293b 100%); 
                        padding: 1.2rem; border-radius: 12px; margin-bottom: 1rem;
                        color: white;">
                <h3 style="margin: 0; color: white;">中国政策分析师 — 政策第三维度</h3>
                <p class="ui-body-text" style="margin: 0.3rem 0 0 0; opacity: 0.9;">
                    深度分析货币政策、财政政策、产业政策、房地产政策对周期和投资的影响
                </p>
            </div>
            """, unsafe_allow_html=True)
            st.markdown(policy.get("analysis", "暂无分析结果"))
        else:
            st.info("暂无政策分析结果")

    # 导出报告
    if show_export:
        st.markdown("---")
        display_pdf_export_section(result, key_prefix=key_prefix)


def display_pdf_export_section(result, key_prefix="macro"):
    """显示PDF/Markdown导出部分"""
    st.subheader("导出报告")

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

    with col1:
        st.write("将宏观周期分析报告导出为PDF或Markdown文件，方便保存和分享")

    with col2:
        if st.button("生成PDF报告", type="primary", width='content', key=f"{key_prefix}_pdf_gen"):
            with st.spinner("正在生成PDF报告..."):
                try:
                    generator = MacroCyclePDFGenerator()
                    pdf_path = generator.generate_pdf(result)

                    with open(pdf_path, "rb") as f:
                        pdf_bytes = f.read()

                    st.session_state[f"{key_prefix}_pdf_data"] = pdf_bytes
                    ts = result.get('timestamp', datetime.now().strftime('%Y%m%d_%H%M%S')).replace(':', '').replace(' ', '_')
                    st.session_state[f"{key_prefix}_pdf_filename"] = f"宏观周期报告_{ts}.pdf"

                    st.success("PDF报告生成成功。")
                    st.rerun()

                except Exception as e:
                    st.error(f"PDF生成失败: {str(e)}")

    with col3:
        if st.button("生成Markdown", type="secondary", width='content', key=f"{key_prefix}_md_gen"):
            with st.spinner("正在生成Markdown报告..."):
                try:
                    markdown_content = generate_macro_cycle_markdown(result)

                    st.session_state[f"{key_prefix}_md_data"] = markdown_content
                    ts = result.get('timestamp', datetime.now().strftime('%Y%m%d_%H%M%S')).replace(':', '').replace(' ', '_')
                    st.session_state[f"{key_prefix}_md_filename"] = f"宏观周期报告_{ts}.md"

                    st.success("Markdown报告生成成功。")
                    st.rerun()

                except Exception as e:
                    st.error(f"Markdown生成失败: {str(e)}")

    with col4:
        if f'{key_prefix}_pdf_data' in st.session_state:
            st.download_button(
                label="下载PDF",
                data=st.session_state[f"{key_prefix}_pdf_data"],
                file_name=st.session_state[f"{key_prefix}_pdf_filename"],
                mime="application/pdf",
                width='content',
                key=f"{key_prefix}_pdf_dl"
            )

        if f'{key_prefix}_md_data' in st.session_state:
            st.download_button(
                label="下载Markdown",
                data=st.session_state[f"{key_prefix}_md_data"],
                file_name=st.session_state[f"{key_prefix}_md_filename"],
                mime="text/markdown",
                width='content',
                key=f"{key_prefix}_md_dl"
            )

def display_theory_tab():
    """显示理论介绍标签页"""
    st.markdown("""
    ## 两大周期理论简介

    ---

    ### 康德拉季耶夫长波（康波周期）

    **创始人**：苏联经济学家尼古拉·康德拉季耶夫（1920s）  
    **中国推广者**：周金涛（"周期天王"，中信建投首席经济学家）

    **核心思想**：资本主义经济存在约 **50-60年** 的超长周期，由重大技术革命驱动。

    **四个阶段**：

    | 阶段 | 持续时间 | 特征 | 最优资产 |
    |------|---------|------|---------|
    | **回升期** | ~15年 | 新技术商业化，经济从底部爬起 | 股票、新兴产业 |
    | **繁荣期** | ~15年 | 技术全面铺开，高速增长 | 几乎所有资产 |
    | **衰退期** | ~10年 | 泡沫破裂，增速放缓 | 大宗商品→现金 |
    | **萧条期** | ~10年 | 全面收缩，资产便宜 | 现金→布局未来 |

    **历史五轮康波**：
    1. **1780s-1840s**：蒸汽机革命
    2. **1840s-1890s**：铁路与钢铁
    3. **1890s-1940s**：电力与化工
    4. **1940s-1990s**：汽车与计算机
    5. **1990s-2050s?**：信息技术革命（当前）

    > *"人生发财靠康波。每个人的财富积累，一定不要以为是你多有本事，财富积累完全来源于经济周期运动的时间给你的机会。"* — 周金涛

    ---

    ### ⏰ 美林投资时钟

    **创始人**：美林证券分析师（2004年）  
    **核心指标**：经济增长 × 通货膨胀

    **四个象限**：

    | 象限 | 经济 | 通胀 | 最优资产 | 典型特征 |
    |------|------|------|---------|---------|
    | **复苏期** | ↑ | ↓ | **股票** | 盈利改善，利率低 |
    | **过热期** | ↑ | ↑ | **大宗商品** | 需求旺盛，加息 |
    | **滞胀期** | ↓ | ↑ | **现金** | 成本上升，利润缩水 |
    | **衰退期** | ↓ | ↓ | **债券** | 降息，避险需求 |

    **中国化改造**：
    - 增加 **政策方向** 作为第三维度
    - 缩短时钟转动周期（中国约1-3年一轮，美国3-5年）
    - 增加 **房地产** 作为第五类资产
    - 重视 **政策友好型** 资产

    ---

    ### 两大理论的结合使用

    | 维度 | 康波周期 | 美林时钟 |
    |------|---------|---------|
    | **时间尺度** | 50-60年 | 3-5年 |
    | **驱动力** | 技术革命（供给侧） | 增长+通胀（需求侧） |
    | **用途** | 人生战略决策 | 投资组合调整 |
    | **比喻** | 望远镜 | 显微镜 |
    | **角色** | 罗盘（大方向） | 航海图（风浪变化） |

    **结合方法**：
    - 康波定 **战略方向**（进攻/防守）
    - 美林定 **战术节奏**（具体配什么）
    - 政策作为 **催化剂**（加速/扭曲周期）

    > *"双指针一致时信心更强，矛盾时要谨慎。"*

    ---

    ### 免责声明

    本分析仅供学习研究参考，不构成任何投资建议。周期理论是认知框架而非精确预测工具。
    投资有风险，入市需谨慎。
    """)


# 主入口
if __name__ == "__main__":
    display_macro_cycle()
