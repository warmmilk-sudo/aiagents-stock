"""Main-force report exporters (pure backend, no UI dependencies)."""

from __future__ import annotations

import os
import re
import tempfile
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

from time_utils import local_now_str


class MainForcePDFGenerator:
    """主力选股 PDF 报告生成器。"""

    def __init__(self):
        self.chinese_font = "Helvetica"
        self._setup_fonts()

    def _setup_fonts(self):
        for font_path in (
            "C:/Windows/Fonts/msyh.ttc",
            "C:/Windows/Fonts/simsun.ttc",
            "C:/Windows/Fonts/simhei.ttf",
        ):
            if not os.path.exists(font_path):
                continue
            try:
                pdfmetrics.registerFont(TTFont("MainForceChineseFont", font_path))
                self.chinese_font = "MainForceChineseFont"
                return
            except Exception:
                continue

    @staticmethod
    def _clean_markdown_line(text: str) -> str:
        cleaned = str(text or "").strip()
        if not cleaned:
            return ""
        if re.fullmatch(r"[\|\-\s:]+", cleaned):
            return ""
        cleaned = re.sub(r"\*\*(.*?)\*\*", r"\1", cleaned)
        cleaned = re.sub(r"\*(.*?)\*", r"\1", cleaned)
        cleaned = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", cleaned)
        cleaned = cleaned.replace("`", "")
        if "|" in cleaned:
            cells = [cell.strip() for cell in cleaned.strip("|").split("|") if cell.strip()]
            cleaned = "  |  ".join(cells)
        return cleaned.strip()

    def generate_pdf(self, analyzer, result, output_path: str | None = None) -> str:
        markdown_content = generate_main_force_markdown_report(analyzer, result)
        if output_path is None:
            timestamp = local_now_str("%Y%m%d_%H%M%S")
            output_path = os.path.join(tempfile.gettempdir(), f"主力选股分析报告_{timestamp}.pdf")

        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=0.5 * inch,
            leftMargin=0.5 * inch,
            topMargin=0.5 * inch,
            bottomMargin=0.5 * inch,
        )
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "MainForceTitle",
            parent=styles["Title"],
            fontName=self.chinese_font,
            fontSize=18,
            leading=24,
            alignment=TA_CENTER,
            textColor=colors.HexColor("#0f172a"),
            spaceAfter=10,
        )
        heading_style = ParagraphStyle(
            "MainForceHeading",
            parent=styles["Heading2"],
            fontName=self.chinese_font,
            fontSize=13,
            leading=18,
            textColor=colors.HexColor("#1d4ed8"),
            spaceBefore=8,
            spaceAfter=6,
        )
        body_style = ParagraphStyle(
            "MainForceBody",
            parent=styles["BodyText"],
            fontName=self.chinese_font,
            fontSize=9.5,
            leading=14,
            textColor=colors.HexColor("#111827"),
            spaceAfter=4,
        )

        story = [
            Paragraph("主力选股分析报告", title_style),
            Spacer(1, 0.12 * inch),
        ]
        for raw_line in markdown_content.splitlines():
            line = self._clean_markdown_line(raw_line)
            if not line:
                continue
            if raw_line.startswith("# "):
                story.append(Paragraph(line, title_style))
            elif raw_line.startswith("## ") or raw_line.startswith("### "):
                story.append(Paragraph(line, heading_style))
            else:
                story.append(Paragraph(line, body_style))
            story.append(Spacer(1, 0.04 * inch))

        doc.build(story)
        return output_path


def generate_main_force_markdown_report(analyzer, result):
    """生成主力选股Markdown格式的分析报告。"""

    current_time = local_now_str("%Y年%m月%d日 %H:%M:%S")
    params = result.get("params", {})
    start_date = params.get("start_date", "N/A")
    min_cap = params.get("min_market_cap", 50)
    max_cap = params.get("max_market_cap", 5000)
    max_change = params.get("max_range_change", 50)

    markdown_content = f"""
# 主力选股AI分析报告

**生成时间**: {current_time}

---

## 📊 选股参数

| 项目 | 值 |
|------|-----|
| **起始日期** | {start_date} |
| **市值范围** | {min_cap}亿 - {max_cap}亿 |
| **最大涨跌幅** | {max_change}% |
| **初始数据量** | {result.get('total_fetched', 0)}只 |
| **筛选后数量** | {result.get('filtered_count', 0)}只 |
| **最终推荐** | {len(result.get('final_recommendations', []))}只 |

---

## 🤖 AI分析师团队报告

"""

    if hasattr(analyzer, "fund_flow_analysis") and analyzer.fund_flow_analysis:
        markdown_content += f"""
### 💰 资金流向分析师

{analyzer.fund_flow_analysis}

---

"""

    if hasattr(analyzer, "industry_analysis") and analyzer.industry_analysis:
        markdown_content += f"""
### 📊 行业板块及市场热点分析师

{analyzer.industry_analysis}

---

"""

    if hasattr(analyzer, "fundamental_analysis") and analyzer.fundamental_analysis:
        markdown_content += f"""
### 📈 财务基本面分析师

{analyzer.fundamental_analysis}

---

"""

    markdown_content += """
## ⭐ 精选推荐股票

"""

    final_recommendations = result.get("final_recommendations", [])
    if final_recommendations:
        for rec in final_recommendations:
            markdown_content += f"""
### 【第{rec['rank']}名】{rec['symbol']} - {rec['name']}

**推荐理由**:
{rec.get('reason', '暂无')}

**关键指标**:
"""
            if "stock_data" in rec:
                stock_data = rec["stock_data"]
                markdown_content += f"""
- **所属行业**: {stock_data.get('industry', 'N/A')}
- **市值**: {stock_data.get('market_cap', 'N/A')}
- **主力资金流向**: {stock_data.get('main_fund_inflow', 'N/A')}
- **区间涨跌幅**: {stock_data.get('range_change', 'N/A')}%
- **市盈率**: {stock_data.get('pe_ratio', 'N/A')}
- **市净率**: {stock_data.get('pb_ratio', 'N/A')}

"""

            if "scores" in rec.get("stock_data", {}):
                scores = rec["stock_data"]["scores"]
                if scores:
                    markdown_content += "**能力评分**:\n"
                    for score_name, score_value in scores.items():
                        markdown_content += f"- {score_name}: {score_value}\n"
                    markdown_content += "\n"

            markdown_content += "---\n\n"
    else:
        markdown_content += "暂无推荐股票\n\n---\n\n"

    markdown_content += f"""
---

## 📝 免责声明

本报告由AI系统生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。请在做出投资决策前咨询专业的投资顾问。

---

*报告生成时间: {current_time}*  
*主力选股AI分析系统 v1.0*
"""

    return markdown_content
