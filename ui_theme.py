from __future__ import annotations

import streamlit as st
import plotly.graph_objects as go
import plotly.io as pio

BG_COLOR = "#0B1220"
CARD_COLOR = "#111827"
SURFACE_COLOR = "#1F2937"
BORDER_COLOR = "#2B3545"
TEXT_COLOR = "#E5E7EB"
MUTED_TEXT_COLOR = "#9CA3AF"
ACCENT_COLOR = "#22D3EE"
SUCCESS_COLOR = "#10B981"
WARNING_COLOR = "#F59E0B"
DANGER_COLOR = "#EF4444"

FONT_STACK = (
    '"PingFang SC", "Microsoft YaHei", "Noto Sans CJK SC", '
    '"Source Han Sans SC", sans-serif'
)

PLOTLY_TEMPLATE_NAME = "tra_dark_minimal"


def inject_global_theme() -> None:
    """Inject a shared Streamlit theme for all pages."""
    if st.session_state.get("_tra_theme_injected"):
        return

    st.session_state["_tra_theme_injected"] = True

    st.markdown(
        f"""
<style>
:root {{
    --tra-bg: {BG_COLOR};
    --tra-card: {CARD_COLOR};
    --tra-surface: {SURFACE_COLOR};
    --tra-border: {BORDER_COLOR};
    --tra-text: {TEXT_COLOR};
    --tra-muted: {MUTED_TEXT_COLOR};
    --tra-accent: {ACCENT_COLOR};
    --tra-success: {SUCCESS_COLOR};
    --tra-warning: {WARNING_COLOR};
    --tra-danger: {DANGER_COLOR};
}}

html, body, [class*="css"] {{
    font-family: {FONT_STACK};
}}

.stApp {{
    background: radial-gradient(1200px 800px at 100% -10%, rgba(34, 211, 238, 0.10), transparent 55%),
                radial-gradient(900px 600px at -10% 0%, rgba(59, 130, 246, 0.10), transparent 60%),
                var(--tra-bg);
    color: var(--tra-text);
}}

[data-testid="stAppViewContainer"] {{
    background: transparent;
}}

.block-container {{
    max-width: 1320px;
    padding-top: 1.2rem;
    padding-bottom: 2rem;
    animation: fadeInUp 240ms ease-out;
}}

.page-header,
.top-nav {{
    background: linear-gradient(140deg, rgba(31, 41, 55, 0.96), rgba(17, 24, 39, 0.96));
    border: 1px solid var(--tra-border);
    border-radius: 14px;
    padding: 1rem 1.2rem;
    margin-bottom: 1rem;
}}

.nav-title {{
    margin: 0;
    font-size: 1.45rem;
    line-height: 1.25;
    color: var(--tra-text);
    font-weight: 700;
}}

.nav-subtitle {{
    margin: 0.35rem 0 0;
    font-size: 0.92rem;
    color: var(--tra-muted);
}}

[data-testid="stSidebar"] {{
    background: linear-gradient(180deg, rgba(17, 24, 39, 0.98), rgba(11, 18, 32, 0.98));
    border-right: 1px solid var(--tra-border);
}}

h1, h2, h3, h4, h5, h6,
p,
span,
label,
[data-testid="stMarkdownContainer"],
[data-testid="stMetricLabel"],
[data-testid="stMetricValue"],
[data-testid="stMetricDelta"] {{
    color: var(--tra-text) !important;
}}

small,
caption,
[data-testid="stCaptionContainer"] {{
    color: var(--tra-muted) !important;
}}

[data-testid="stExpander"] {{
    border: 1px solid var(--tra-border);
    border-radius: 12px;
    background: rgba(17, 24, 39, 0.7);
}}

div[data-baseweb="select"] > div,
.stTextInput > div > div > input,
.stTextArea > div > div > textarea,
.stDateInput input,
.stNumberInput input,
.stSlider {{
    background-color: rgba(17, 24, 39, 0.92) !important;
    color: var(--tra-text) !important;
    border: 1px solid var(--tra-border) !important;
    border-radius: 10px !important;
}}

.stButton > button,
.stDownloadButton > button {{
    background: linear-gradient(140deg, rgba(34, 211, 238, 0.18), rgba(34, 211, 238, 0.08));
    color: var(--tra-text);
    border: 1px solid rgba(34, 211, 238, 0.45);
    border-radius: 10px;
    transition: all 180ms ease;
}}

.stButton > button:hover,
.stDownloadButton > button:hover {{
    border-color: var(--tra-accent);
    box-shadow: 0 0 0 2px rgba(34, 211, 238, 0.18);
    transform: translateY(-1px);
}}

.stTabs [data-baseweb="tab-list"] {{
    gap: 0.5rem;
    background: rgba(17, 24, 39, 0.75);
    border: 1px solid var(--tra-border);
    border-radius: 12px;
    padding: 0.45rem;
    overflow-x: auto;
    flex-wrap: nowrap;
}}

.stTabs [data-baseweb="tab"] {{
    background: transparent;
    color: var(--tra-muted);
    border-radius: 8px;
    border: 1px solid transparent;
    padding: 0.55rem 0.9rem;
    white-space: nowrap;
}}

.stTabs [aria-selected="true"] {{
    background: rgba(34, 211, 238, 0.14) !important;
    border-color: rgba(34, 211, 238, 0.45) !important;
    color: var(--tra-text) !important;
}}

.agent-card,
.decision-card,
.warning-card,
.metric-card {{
    background: rgba(17, 24, 39, 0.86);
    border: 1px solid var(--tra-border);
    border-left: 3px solid var(--tra-accent);
    border-radius: 12px;
    padding: 0.9rem 1rem;
    margin: 0.8rem 0;
}}

.decision-card {{
    border-left-color: var(--tra-success);
}}

.warning-card {{
    border-left-color: var(--tra-warning);
}}

.stAlert,
[data-testid="stMetric"] {{
    background: rgba(17, 24, 39, 0.82);
    border: 1px solid var(--tra-border);
    border-radius: 12px;
}}

.stDataFrame,
[data-testid="stTable"] {{
    border: 1px solid var(--tra-border);
    border-radius: 12px;
    overflow: hidden;
}}

.js-plotly-plot,
[data-testid="stPlotlyChart"] {{
    border: 1px solid var(--tra-border);
    border-radius: 12px;
    overflow: hidden;
    background: rgba(17, 24, 39, 0.85);
}}

[data-testid="stVerticalBlock"] {{
    gap: 0.8rem;
}}

#MainMenu, footer {{
    visibility: hidden;
}}

@media (max-width: 768px) {{
    .block-container {{
        padding-top: 0.8rem;
        padding-left: 0.7rem;
        padding-right: 0.7rem;
        padding-bottom: 1.4rem;
    }}

    .nav-title {{
        font-size: 1.2rem;
    }}

    .nav-subtitle {{
        font-size: 0.82rem;
    }}

    div[data-testid="stHorizontalBlock"] {{
        flex-direction: column !important;
        gap: 0.65rem !important;
    }}

    div[data-testid="column"] {{
        width: 100% !important;
        min-width: 100% !important;
        flex: 1 1 100% !important;
    }}

    .stButton > button,
    .stDownloadButton > button,
    .stTextInput,
    .stTextArea,
    .stSelectbox,
    .stDateInput,
    .stNumberInput {{
        width: 100% !important;
    }}

    .stTabs [data-baseweb="tab-list"] {{
        padding: 0.35rem;
    }}

    .stTabs [data-baseweb="tab"] {{
        padding: 0.45rem 0.75rem;
    }}
}}

@media (prefers-reduced-motion: reduce) {{
    * {{
        animation: none !important;
        transition: none !important;
    }}
}}

@keyframes fadeInUp {{
    from {{
        opacity: 0;
        transform: translateY(8px);
    }}
    to {{
        opacity: 1;
        transform: translateY(0);
    }}
}}
</style>
        """,
        unsafe_allow_html=True,
    )


def configure_plotly_template() -> None:
    """Register and apply a shared Plotly template."""
    if PLOTLY_TEMPLATE_NAME not in pio.templates:
        pio.templates[PLOTLY_TEMPLATE_NAME] = go.layout.Template(
            layout=go.Layout(
                font=dict(family=FONT_STACK, color=TEXT_COLOR),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(31,41,55,0.55)",
                colorway=[
                    "#22D3EE",
                    "#60A5FA",
                    "#10B981",
                    "#F59E0B",
                    "#EF4444",
                    "#A78BFA",
                ],
                xaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(156,163,175,0.16)",
                    zeroline=False,
                    linecolor="rgba(156,163,175,0.28)",
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor="rgba(156,163,175,0.16)",
                    zeroline=False,
                    linecolor="rgba(156,163,175,0.28)",
                ),
                legend=dict(
                    bgcolor="rgba(17,24,39,0.82)",
                    bordercolor="rgba(43,53,69,1)",
                    borderwidth=1,
                ),
                title=dict(font=dict(color=TEXT_COLOR, size=18)),
                margin=dict(l=36, r=24, t=52, b=34),
            )
        )

    pio.templates.default = PLOTLY_TEMPLATE_NAME


def render_page_header(title: str, subtitle: str | None = None) -> None:
    """Render consistent page header."""
    subtitle_markup = f'<p class="nav-subtitle">{subtitle}</p>' if subtitle else ""
    st.markdown(
        f"""
<div class="page-header">
    <h1 class="nav-title">{title}</h1>
    {subtitle_markup}
</div>
        """,
        unsafe_allow_html=True,
    )
