"""
新闻流量分析PDF报告生成器
生成包含AI分析结果的PDF格式报告
"""

import io
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Optional

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

import logging

logger = logging.getLogger(__name__)


class NewsFlowPDFGenerator:
    """新闻流量分析PDF报告生成器"""
    
    def __init__(self):
        self.chinese_font = self._register_chinese_fonts()
        self.styles = self._create_styles()
    
    def _register_chinese_fonts(self) -> str:
        """注册中文字体"""
        try:
            if 'ChineseFont' in pdfmetrics.getRegisteredFontNames():
                return 'ChineseFont'
            
            # 字体路径列表
            font_paths = [
                'C:/Windows/Fonts/simsun.ttc',
                'C:/Windows/Fonts/simhei.ttf',
                'C:/Windows/Fonts/msyh.ttc',
                '/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc',
                '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
            ]
            
            for font_path in font_paths:
                if os.path.exists(font_path):
                    try:
                        pdfmetrics.registerFont(TTFont('ChineseFont', font_path))
                        return 'ChineseFont'
                    except Exception:
                        continue
            
            return 'Helvetica'
        except Exception:
            return 'Helvetica'
    
    def _create_styles(self) -> Dict:
        """创建PDF样式"""
        styles = getSampleStyleSheet()
        
        # 标题样式
        styles.add(ParagraphStyle(
            name='ChineseTitle',
            fontName=self.chinese_font,
            fontSize=24,
            alignment=TA_CENTER,
            spaceAfter=30,
            textColor=colors.HexColor('#1a1a2e')
        ))
        
        # 副标题样式
        styles.add(ParagraphStyle(
            name='ChineseSubtitle',
            fontName=self.chinese_font,
            fontSize=14,
            alignment=TA_CENTER,
            spaceAfter=20,
            textColor=colors.HexColor('#666666')
        ))
        
        # 章节标题
        styles.add(ParagraphStyle(
            name='ChineseHeading',
            fontName=self.chinese_font,
            fontSize=16,
            spaceBefore=20,
            spaceAfter=10,
            textColor=colors.HexColor('#2d3436'),
            borderPadding=5,
        ))
        
        # 正文样式
        styles.add(ParagraphStyle(
            name='ChineseBody',
            fontName=self.chinese_font,
            fontSize=11,
            leading=18,
            alignment=TA_JUSTIFY,
            spaceBefore=6,
            spaceAfter=6,
        ))
        
        # 小标题
        styles.add(ParagraphStyle(
            name='ChineseSmallHeading',
            fontName=self.chinese_font,
            fontSize=12,
            spaceBefore=10,
            spaceAfter=5,
            textColor=colors.HexColor('#0984e3'),
        ))
        
        # 重点提示
        styles.add(ParagraphStyle(
            name='ChineseHighlight',
            fontName=self.chinese_font,
            fontSize=11,
            leading=16,
            backColor=colors.HexColor('#fff3cd'),
            borderPadding=8,
            spaceBefore=10,
            spaceAfter=10,
        ))
        
        # 风险警告
        styles.add(ParagraphStyle(
            name='ChineseWarning',
            fontName=self.chinese_font,
            fontSize=10,
            leading=14,
            backColor=colors.HexColor('#f8d7da'),
            borderPadding=8,
            spaceBefore=10,
            spaceAfter=10,
            textColor=colors.HexColor('#721c24'),
        ))
        
        return styles
    
    def generate_report(self, analysis_result: Dict) -> Optional[str]:
        """
        生成PDF分析报告
        
        Args:
            analysis_result: 完整分析结果
            
        Returns:
            PDF文件路径
        """
        try:
            # 创建临时文件
            temp_dir = tempfile.gettempdir()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pdf_path = os.path.join(temp_dir, f'news_flow_report_{timestamp}.pdf')
            
            # 创建PDF文档
            doc = SimpleDocTemplate(
                pdf_path,
                pagesize=A4,
                rightMargin=50,
                leftMargin=50,
                topMargin=50,
                bottomMargin=50
            )
            
            # 构建内容
            content = []
            
            # 封面
            content.extend(self._build_cover(analysis_result))
            content.append(PageBreak())
            
            # 概要
            content.extend(self._build_summary(analysis_result))
            content.append(Spacer(1, 20))
            
            # 流量分析
            content.extend(self._build_flow_analysis(analysis_result))
            content.append(Spacer(1, 20))
            
            # AI分析结果
            content.extend(self._build_ai_analysis(analysis_result))
            content.append(PageBreak())
            
            # 板块深度分析
            content.extend(self._build_sector_analysis(analysis_result))
            content.append(Spacer(1, 20))
            
            # 股票推荐
            content.extend(self._build_stock_recommendations(analysis_result))
            content.append(Spacer(1, 20))
            
            # 风险提示
            content.extend(self._build_risk_warning(analysis_result))
            
            # 生成PDF
            doc.build(content)
            
            logger.info(f"✅ PDF报告生成成功: {pdf_path}")
            return pdf_path
            
        except Exception as e:
            logger.error(f"❌ PDF生成失败: {e}")
            return None
    
    def _build_cover(self, result: Dict) -> List:
        """构建封面"""
        content = []
        
        content.append(Spacer(1, 100))
        content.append(Paragraph("新闻流量分析报告", self.styles['ChineseTitle']))
        content.append(Spacer(1, 30))
        
        # 生成时间
        fetch_time = result.get('fetch_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        content.append(Paragraph(f"生成时间：{fetch_time}", self.styles['ChineseSubtitle']))
        
        # 分析耗时
        duration = result.get('duration', 0)
        content.append(Paragraph(f"分析耗时：{duration} 秒", self.styles['ChineseSubtitle']))
        
        content.append(Spacer(1, 50))
        
        # 核心指标概览
        flow_data = result.get('flow_data', {})
        sentiment_data = result.get('sentiment_data', {})
        
        overview_text = f"""
        流量得分：{flow_data.get('total_score', 0)}/1000 ({flow_data.get('level', '中')})<br/>
        情绪指数：{sentiment_data.get('sentiment', {}).get('sentiment_index', 50)}/100<br/>
        流量阶段：{sentiment_data.get('flow_stage', {}).get('stage_name', '未知')}
        """
        content.append(Paragraph(overview_text, self.styles['ChineseBody']))
        
        return content
    
    def _build_summary(self, result: Dict) -> List:
        """构建分析概要"""
        content = []
        
        content.append(Paragraph("📊 分析概要", self.styles['ChineseHeading']))
        
        trading_signals = result.get('trading_signals', {})
        ai_analysis = result.get('ai_analysis', {})
        
        # 交易信号
        signal = trading_signals.get('overall_signal', '观望')
        confidence = trading_signals.get('confidence', 50)
        
        content.append(Paragraph(
            f"【AI建议】{signal}（置信度：{confidence}%）",
            self.styles['ChineseHighlight']
        ))
        
        # 核心提示
        key_message = trading_signals.get('key_message', '')
        if key_message:
            content.append(Paragraph(f"核心提示：{key_message}", self.styles['ChineseBody']))
        
        # AI总结
        advice = ai_analysis.get('investment_advice', {})
        summary = advice.get('summary', '')
        if summary:
            content.append(Paragraph(f"AI总结：{summary}", self.styles['ChineseBody']))
        
        return content
    
    def _build_flow_analysis(self, result: Dict) -> List:
        """构建流量分析"""
        content = []
        
        content.append(Paragraph("📈 流量分析", self.styles['ChineseHeading']))
        
        flow_data = result.get('flow_data', {})
        model_data = result.get('model_data', {})
        
        # 流量得分表格
        flow_table_data = [
            ['指标', '数值', '等级'],
            ['总流量得分', str(flow_data.get('total_score', 0)), flow_data.get('level', '中')],
            ['社交媒体', str(flow_data.get('social_score', 0)), '-'],
            ['财经平台', str(flow_data.get('finance_score', 0)), '-'],
            ['新闻媒体', str(flow_data.get('news_score', 0)), '-'],
        ]
        
        table = Table(flow_table_data, colWidths=[150, 100, 100])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4a69bd')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, -1), self.chinese_font),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ]))
        content.append(table)
        
        # K值分析
        viral_k = model_data.get('viral_k', {})
        if viral_k:
            k_value = viral_k.get('k_value', 1.0)
            trend = viral_k.get('trend', '稳定')
            content.append(Spacer(1, 10))
            content.append(Paragraph(
                f"K值（病毒系数）：{k_value:.2f} - {trend}",
                self.styles['ChineseBody']
            ))
        
        return content
    
    def _build_ai_analysis(self, result: Dict) -> List:
        """构建AI分析结果"""
        content = []
        
        content.append(Paragraph("🤖 AI智能分析", self.styles['ChineseHeading']))
        
        ai_analysis = result.get('ai_analysis', {})
        if not ai_analysis:
            content.append(Paragraph("暂无AI分析数据", self.styles['ChineseBody']))
            return content
        
        # 受益板块
        sector_analysis = ai_analysis.get('sector_analysis', {})
        benefited_sectors = sector_analysis.get('benefited_sectors', [])
        
        if benefited_sectors:
            content.append(Paragraph("受益板块分析", self.styles['ChineseSmallHeading']))
            
            sector_table_data = [['板块', '置信度', '分析理由']]
            for sector in benefited_sectors[:5]:
                sector_table_data.append([
                    sector.get('name', ''),
                    f"{sector.get('confidence', 0)}%",
                    sector.get('reason', '')[:40] + '...' if len(sector.get('reason', '')) > 40 else sector.get('reason', '')
                ])
            
            table = Table(sector_table_data, colWidths=[100, 60, 280])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#00b894')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, -1), self.chinese_font),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('ALIGN', (0, 0), (1, -1), 'CENTER'),
                ('ALIGN', (2, 0), (2, -1), 'LEFT'),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
            ]))
            content.append(table)
        
        # 热门题材
        hot_themes = sector_analysis.get('hot_themes', [])
        if hot_themes:
            content.append(Spacer(1, 15))
            content.append(Paragraph("今日热门题材", self.styles['ChineseSmallHeading']))
            
            themes_text = []
            for theme in hot_themes[:5]:
                themes_text.append(f"• {theme.get('theme', '')} ({theme.get('heat_level', '中')})")
            
            content.append(Paragraph('<br/>'.join(themes_text), self.styles['ChineseBody']))
        
        return content
    
    def _build_sector_analysis(self, result: Dict) -> List:
        """构建板块深度分析"""
        content = []
        
        ai_analysis = result.get('ai_analysis', {})
        multi_sector = ai_analysis.get('multi_sector', {})
        sector_analyses = multi_sector.get('sector_analyses', [])
        
        if not sector_analyses:
            return content
        
        content.append(Paragraph("🎯 板块深度分析", self.styles['ChineseHeading']))
        content.append(Paragraph(
            f"共分析 {len(sector_analyses)} 个热门板块",
            self.styles['ChineseBody']
        ))
        
        for sector in sector_analyses[:5]:
            sector_name = sector.get('sector_name', '未知')
            heat_level = sector.get('heat_level', '中')
            outlook = sector.get('short_term_outlook', '震荡')
            
            content.append(Spacer(1, 10))
            content.append(Paragraph(
                f"【{sector_name}】热度：{heat_level} | 短期预判：{outlook}",
                self.styles['ChineseSmallHeading']
            ))
            
            # 预判理由
            outlook_reason = sector.get('outlook_reason', '')
            if outlook_reason:
                content.append(Paragraph(f"预判理由：{outlook_reason}", self.styles['ChineseBody']))
            
            # 龙头股
            leaders = sector.get('leader_stocks', [])
            if leaders:
                leader_names = [f"{s.get('code', '')}{s.get('name', '')}" for s in leaders[:3]]
                content.append(Paragraph(f"龙头股：{', '.join(leader_names)}", self.styles['ChineseBody']))
            
            # 投资建议
            advice = sector.get('investment_advice', '')
            if advice:
                content.append(Paragraph(f"建议：{advice}", self.styles['ChineseBody']))
        
        return content
    
    def _build_stock_recommendations(self, result: Dict) -> List:
        """构建股票推荐"""
        content = []
        
        ai_analysis = result.get('ai_analysis', {})
        stock_recommend = ai_analysis.get('stock_recommend', {})
        recommended_stocks = stock_recommend.get('recommended_stocks', [])
        
        if not recommended_stocks:
            return content
        
        content.append(Paragraph("💰 AI选股推荐", self.styles['ChineseHeading']))
        
        # 股票推荐表格
        stock_table_data = [['代码', '名称', '板块', '风险', '推荐理由']]
        for stock in recommended_stocks[:8]:
            stock_table_data.append([
                stock.get('code', ''),
                stock.get('name', ''),
                stock.get('sector', ''),
                stock.get('risk_level', '中'),
                stock.get('reason', '')[:30] + '...' if len(stock.get('reason', '')) > 30 else stock.get('reason', '')
            ])
        
        table = Table(stock_table_data, colWidths=[60, 70, 70, 40, 200])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e17055')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, -1), self.chinese_font),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 0), (3, -1), 'CENTER'),
            ('ALIGN', (4, 0), (4, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ]))
        content.append(table)
        
        # 整体策略
        overall_strategy = stock_recommend.get('overall_strategy', '')
        if overall_strategy:
            content.append(Spacer(1, 10))
            content.append(Paragraph(f"整体策略：{overall_strategy}", self.styles['ChineseBody']))
        
        return content
    
    def _build_risk_warning(self, result: Dict) -> List:
        """构建风险提示"""
        content = []
        
        content.append(Paragraph("⚠️ 风险提示", self.styles['ChineseHeading']))
        
        ai_analysis = result.get('ai_analysis', {})
        risk_assess = ai_analysis.get('risk_assess', {})
        
        risk_level = risk_assess.get('risk_level', '中等')
        risk_score = risk_assess.get('risk_score', 50)
        risk_factors = risk_assess.get('risk_factors', [])
        
        content.append(Paragraph(
            f"风险等级：{risk_level}（分数：{risk_score}/100）",
            self.styles['ChineseBody']
        ))
        
        if risk_factors:
            factors_text = '<br/>'.join([f"• {f}" for f in risk_factors[:5]])
            content.append(Paragraph(f"风险因素：<br/>{factors_text}", self.styles['ChineseBody']))
        
        # 免责声明
        disclaimer = """
        【免责声明】
        本报告由AI自动生成，仅供参考，不构成任何投资建议。
        股市有风险，投资需谨慎。请投资者根据自身情况独立判断，
        理性投资，自负盈亏。本报告作者及生成系统不对投资决策
        产生的任何损失承担责任。
        """
        content.append(Spacer(1, 20))
        content.append(Paragraph(disclaimer, self.styles['ChineseWarning']))
        
        return content
