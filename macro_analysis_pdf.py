"""
宏观分析板块 - Markdown 与 PDF 导出
"""

from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime
from typing import Any

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    return str(value)


def _num(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "-"


def generate_macro_analysis_markdown(result_data: dict[str, Any]) -> str:
    raw_data = result_data.get("raw_data", {})
    snapshot = raw_data.get("macro_snapshot", {})
    sector_view = result_data.get("sector_view", {})
    stock_view = result_data.get("stock_view", {})
    agents = result_data.get("agents_analysis", {})

    lines = [
        "# 宏观分析报告",
        "",
        "## 报告信息",
        "",
        f"- 生成时间: {_text(result_data.get('timestamp'), '-')}",
        f"- 数据异常数量: {len(result_data.get('data_errors', []) or [])}",
        "",
        "## 综合结论",
        "",
        _text(agents.get("chief", {}).get("analysis"), "暂无综合结论"),
        "",
        "## 宏观指标快照",
        "",
    ]

    for item in snapshot.values():
        lines.append(
            f"- {_text(item.get('label'))}: {_text(item.get('value'))}{_text(item.get('unit'))} "
            f"({ _text(item.get('period_label')) })"
        )

    lines.extend(
        [
            "",
            "## 行业映射",
            "",
            f"- 市场判断: {_text(sector_view.get('market_view'), '暂无')}",
            "",
            "### 利好行业",
            "",
        ]
    )
    for item in sector_view.get("bullish_sectors", []):
        lines.append(f"- {_text(item.get('sector'))}: {_text(item.get('logic'))}")
    lines.extend(["", "### 利空行业", ""])
    for item in sector_view.get("bearish_sectors", []):
        lines.append(f"- {_text(item.get('sector'))}: {_text(item.get('logic'))}")

    lines.extend(["", "## 优质标的", ""])
    for item in stock_view.get("recommended_stocks", []):
        lines.append(
            f"- {_text(item.get('name'))} ({_text(item.get('code'))}) / {_text(item.get('sector'))}: {_text(item.get('reason'))}"
        )

    lines.extend(["", "## 分析过程", ""])
    for key in ["macro", "policy", "sector", "stock"]:
        agent = agents.get(key, {})
        lines.append(f"### {_text(agent.get('agent_name'), key)}")
        lines.append("")
        lines.append(_text(agent.get("analysis"), "暂无"))
        lines.append("")

    return "\n".join(lines).strip() + "\n"


class MacroAnalysisPDFGenerator:
    """宏观分析PDF生成器"""

    def __init__(self) -> None:
        self.font_name = "Helvetica"
        self._setup_fonts()

    def _setup_fonts(self) -> None:
        font_paths = [
            "C:/Windows/Fonts/msyh.ttc",
            "C:/Windows/Fonts/simsun.ttc",
            "C:/Windows/Fonts/simhei.ttf",
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
            "/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf",
        ]
        for font_path in font_paths:
            if not os.path.exists(font_path):
                continue
            try:
                pdfmetrics.registerFont(TTFont("ChineseFont", font_path))
                self.font_name = "ChineseFont"
                return
            except Exception:
                continue

    def _styles(self) -> dict[str, ParagraphStyle]:
        base = getSampleStyleSheet()
        return {
            "title": ParagraphStyle("MacroTitle", parent=base["Title"], fontName=self.font_name, leading=26),
            "h1": ParagraphStyle("MacroH1", parent=base["Heading1"], fontName=self.font_name, leading=20),
            "h2": ParagraphStyle("MacroH2", parent=base["Heading2"], fontName=self.font_name, leading=18),
            "body": ParagraphStyle("MacroBody", parent=base["BodyText"], fontName=self.font_name, leading=16),
            "small": ParagraphStyle("MacroSmall", parent=base["BodyText"], fontName=self.font_name, fontSize=9, leading=12),
        }

    @staticmethod
    def _clean(value: Any) -> str:
        text = str(value or "").strip()
        text = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
        text = re.sub(r"^#{1,6}\s+", "", text, flags=re.M)
        text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
        return text.replace("\n", "<br/>")

    def _add_paragraph_block(self, story: list[Any], title: str, content: str, styles: dict[str, ParagraphStyle]) -> None:
        story.append(Paragraph(title, styles["h1"]))
        story.append(Spacer(1, 0.12 * inch))
        for paragraph in [part.strip() for part in str(content or "").split("\n\n") if part.strip()]:
            story.append(Paragraph(self._clean(paragraph), styles["body"]))
            story.append(Spacer(1, 0.08 * inch))

    def generate_pdf(self, result_data: dict[str, Any], output_path: str | None = None) -> str:
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(tempfile.gettempdir(), f"宏观分析报告_{timestamp}.pdf")

        styles = self._styles()
        story: list[Any] = []
        raw_data = result_data.get("raw_data", {})
        snapshot = raw_data.get("macro_snapshot", {})
        sector_view = result_data.get("sector_view", {})
        stock_view = result_data.get("stock_view", {})
        agents = result_data.get("agents_analysis", {})

        story.append(Spacer(1, 0.5 * inch))
        story.append(Paragraph("宏观分析报告", styles["title"]))
        story.append(Spacer(1, 0.2 * inch))
        story.append(Paragraph(f"生成时间：{_text(result_data.get('timestamp'), '-')}", styles["small"]))
        story.append(Spacer(1, 0.3 * inch))

        snapshot_lines = [
            f"{_text(item.get('label'))}: {_text(item.get('value'))}{_text(item.get('unit'))} ({_text(item.get('period_label'))})"
            for item in snapshot.values()
        ]
        self._add_paragraph_block(story, "一、综合结论", _text(agents.get("chief", {}).get("analysis"), "暂无综合结论"), styles)
        story.append(PageBreak())
        self._add_paragraph_block(story, "二、宏观指标快照", "<br/>".join(snapshot_lines) or "暂无宏观指标", styles)
        story.append(Spacer(1, 0.12 * inch))
        self._add_paragraph_block(
            story,
            "三、行业映射",
            "\n\n".join(
                [
                    f"市场判断：{_text(sector_view.get('market_view'), '暂无')}",
                    "利好行业：" + "；".join(f"{_text(item.get('sector'))}({_text(item.get('logic'))})" for item in sector_view.get("bullish_sectors", [])),
                    "利空行业：" + "；".join(f"{_text(item.get('sector'))}({_text(item.get('logic'))})" for item in sector_view.get("bearish_sectors", [])),
                ]
            ),
            styles,
        )
        story.append(PageBreak())
        self._add_paragraph_block(
            story,
            "四、优质标的",
            "；".join(
                f"{_text(item.get('name'))}({_text(item.get('code'))}) {_text(item.get('sector'))} 现价{_num(item.get('price'))} 推荐理由：{_text(item.get('reason'))}"
                for item in stock_view.get("recommended_stocks", [])
            )
            or "暂无推荐标的",
            styles,
        )
        for key, title in [("macro", "五、宏观总量分析"), ("policy", "六、政策流动性分析"), ("sector", "七、行业映射分析"), ("stock", "八、优质标的分析")]:
            story.append(PageBreak())
            self._add_paragraph_block(story, title, _text(agents.get(key, {}).get("analysis"), "暂无"), styles)

        document = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=0.5 * inch,
            leftMargin=0.5 * inch,
            topMargin=0.5 * inch,
            bottomMargin=0.5 * inch,
        )
        document.build(story)
        return output_path
