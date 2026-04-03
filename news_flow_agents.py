"""
新闻流量智能分析代理模块
使用DeepSeek进行AI驱动的分析
包含：板块影响分析、股票推荐、风险评估、投资建议
"""
import json
import logging
import re
import time
from datetime import datetime
from typing import Dict, List, Optional
from deepseek_client import DeepSeekClient
from model_routing import ModelTier
from prompt_registry import build_messages

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NewsFlowAgents:
    """新闻流量智能分析代理"""
    
    def __init__(self, model: str = None, lightweight_model: str = None, reasoning_model: str = None):
        """
        初始化代理
        
        Args:
            model: 强制所有任务统一使用同一个模型
        """
        self.model = model
        self.lightweight_model = lightweight_model
        self.reasoning_model = reasoning_model
        self.deepseek_client = None
        self._init_client()
    
    def _init_client(self):
        """初始化DeepSeek客户端"""
        try:
            self.deepseek_client = DeepSeekClient(
                model=self.model,
                lightweight_model=self.lightweight_model,
                reasoning_model=self.reasoning_model,
            )
            logger.info(f"✅ DeepSeek客户端初始化成功，模型配置: {self.deepseek_client.model_selection}")
        except Exception as e:
            logger.error(f"❌ DeepSeek客户端初始化失败: {e}")
            self.deepseek_client = None
    
    def is_available(self) -> bool:
        """检查AI是否可用"""
        return self.deepseek_client is not None
    
    def sector_impact_agent(self, hot_topics: List[Dict], 
                            stock_news: List[Dict],
                            flow_data: Dict = None) -> Dict:
        """
        板块影响分析代理
        
        分析热点可能影响的板块
        
        Returns:
            {
                'affected_sectors': List[Dict],
                'analysis': str,
                'success': bool,
            }
        """
        if not self.is_available():
            return self._fallback_sector_analysis(hot_topics, stock_news)
        
        # 准备数据
        topics_text = '\n'.join([
            f"- {t['topic']} (热度:{t.get('heat', 0)}, 跨{t.get('cross_platform', 0)}平台)"
            for t in hot_topics[:20]
        ])
        
        news_text = '\n'.join([
            f"- [{n.get('platform_name', '')}] {n.get('title', '')}"
            for n in stock_news[:15]
        ])
        
        flow_info = ""
        if flow_data:
            flow_info = f"""
当前流量状态:
- 流量得分: {flow_data.get('total_score', 'N/A')}/1000
- 流量等级: {flow_data.get('level', 'N/A')}
- 社交媒体热度: {flow_data.get('social_score', 'N/A')}
- 财经平台热度: {flow_data.get('finance_score', 'N/A')}
"""
        
        try:
            messages = build_messages(
                "news_flow/sector_impact.system.txt",
                "news_flow/sector_impact.user.txt",
                topics_text=topics_text,
                news_text=news_text,
                flow_info=flow_info,
            )
            
            response = self.deepseek_client.call_api(
                messages,
                temperature=0.5,
                max_tokens=2000,
                tier=ModelTier.REASONING,
            )
            
            # 解析JSON
            result = self._parse_json_response(response)
            
            if result:
                return {
                    'hot_themes': result.get('hot_themes', []),
                    'affected_sectors': result.get('benefited_sectors', []) + result.get('damaged_sectors', []),
                    'benefited_sectors': result.get('benefited_sectors', []),
                    'damaged_sectors': result.get('damaged_sectors', []),
                    'opportunity_assessment': result.get('opportunity_assessment', ''),
                    'trading_suggestion': result.get('trading_suggestion', ''),
                    'key_points': result.get('key_points', []),
                    'success': True,
                    'raw_response': response,
                }
            else:
                return self._fallback_sector_analysis(hot_topics, stock_news)
                
        except Exception as e:
            logger.error(f"板块分析失败: {e}")
            return self._fallback_sector_analysis(hot_topics, stock_news)
    
    def stock_recommend_agent(self, hot_sectors: List[Dict],
                               flow_stage: str,
                               sentiment_class: str) -> Dict:
        """
        股票推荐代理
        
        基于热门板块和市场状态推荐股票
        
        Returns:
            {
                'recommended_stocks': List[Dict],
                'strategy': str,
                'success': bool,
            }
        """
        if not self.is_available():
            return self._fallback_stock_recommend(hot_sectors)
        
        sectors_text = '\n'.join([
            f"- {s.get('name', '')}：{s.get('impact', '利好')}，置信度{s.get('confidence', 50)}%\n  原因：{s.get('reason', '')}\n  龙头特征：{s.get('leader_characteristics', 'N/A')}"
            for s in hot_sectors[:5]
        ])
        
        related_concepts = []
        for s in hot_sectors[:5]:
            related_concepts.extend(s.get('related_concepts', []))
        concepts_text = ', '.join(list(set(related_concepts))[:10]) if related_concepts else '无'
        
        try:
            messages = build_messages(
                "news_flow/stock_recommend.system.txt",
                "news_flow/stock_recommend.user.txt",
                flow_stage=flow_stage,
                sentiment_class=sentiment_class,
                concepts_text=concepts_text,
                sectors_text=sectors_text,
            )
            
            response = self.deepseek_client.call_api(
                messages,
                temperature=0.6,
                max_tokens=2000,
                tier=ModelTier.REASONING,
            )
            result = self._parse_json_response(response)
            
            if result:
                return {
                    'recommended_stocks': result.get('recommended_stocks', []),
                    'overall_strategy': result.get('overall_strategy', ''),
                    'timing_advice': result.get('timing_advice', ''),
                    'risk_warning': result.get('risk_warning', ''),
                    'success': True,
                    'raw_response': response,
                }
            else:
                return self._fallback_stock_recommend(hot_sectors)
                
        except Exception as e:
            logger.error(f"股票推荐失败: {e}")
            return self._fallback_stock_recommend(hot_sectors)
    
    def risk_assess_agent(self, flow_stage: str, 
                          sentiment_data: Dict,
                          viral_k: float,
                          flow_type: str) -> Dict:
        """
        风险评估代理
        
        评估当前市场风险
        
        Returns:
            {
                'risk_level': str,
                'risk_factors': List[str],
                'risk_score': int,
                'analysis': str,
                'success': bool,
            }
        """
        if not self.is_available():
            return self._fallback_risk_assess(flow_stage, sentiment_data, viral_k)
        
        try:
            messages = build_messages(
                "news_flow/risk_assess.system.txt",
                "news_flow/risk_assess.user.txt",
                flow_stage=flow_stage,
                sentiment_index=sentiment_data.get("sentiment_index", 50),
                sentiment_class=sentiment_data.get("sentiment_class", "中性"),
                viral_k=viral_k,
                flow_type=flow_type,
            )
            
            response = self.deepseek_client.call_api(
                messages,
                temperature=0.4,
                max_tokens=1500,
                tier=ModelTier.REASONING,
            )
            result = self._parse_json_response(response)
            
            if result:
                return {
                    'risk_level': result.get('risk_level', '中等'),
                    'risk_score': result.get('risk_score', 50),
                    'risk_factors': result.get('risk_factors', []),
                    'opportunities': result.get('opportunities', []),
                    'analysis': result.get('analysis', ''),
                    'key_warning': result.get('key_warning', ''),
                    'success': True,
                    'raw_response': response,
                }
            else:
                return self._fallback_risk_assess(flow_stage, sentiment_data, viral_k)
                
        except Exception as e:
            logger.error(f"风险评估失败: {e}")
            return self._fallback_risk_assess(flow_stage, sentiment_data, viral_k)
    
    def investment_advisor_agent(self, sector_analysis: Dict,
                                   stock_recommend: Dict,
                                   risk_assess: Dict,
                                   flow_data: Dict,
                                   sentiment_data: Dict) -> Dict:
        """
        投资建议代理（综合）
        
        综合所有分析给出最终投资建议
        
        Returns:
            {
                'advice': str,  # 买入/持有/观望/回避
                'confidence': int,
                'summary': str,
                'action_plan': List[str],
                'success': bool,
            }
        """
        if not self.is_available():
            return self._fallback_investment_advice(risk_assess, flow_data)
        
        # 构建综合信息
        sectors_text = ', '.join([s.get('name', '') for s in sector_analysis.get('benefited_sectors', [])[:3]])
        stocks_text = ', '.join([f"{s.get('name', '')}({s.get('code', '')})" 
                                 for s in stock_recommend.get('recommended_stocks', [])[:3]])
        
        try:
            start_time = time.time()
            
            messages = build_messages(
                "news_flow/investment_advisor.system.txt",
                "news_flow/investment_advisor.user.txt",
                total_score=flow_data.get("total_score", "N/A"),
                flow_level=flow_data.get("level", "N/A"),
                sentiment_index=sentiment_data.get("sentiment_index", 50),
                sentiment_class=sentiment_data.get("sentiment_class", "中性"),
                flow_stage=sentiment_data.get("flow_stage", "未知"),
                sectors_text=sectors_text,
                opportunity_assessment=sector_analysis.get("opportunity_assessment", "N/A"),
                stocks_text=stocks_text,
                risk_level=risk_assess.get("risk_level", "中等"),
                risk_score=risk_assess.get("risk_score", 50),
                risk_factors_text=", ".join(risk_assess.get("risk_factors", [])[:3]),
            )
            
            response = self.deepseek_client.call_api(
                messages,
                temperature=0.5,
                max_tokens=2000,
                tier=ModelTier.REASONING,
            )
            result = self._parse_json_response(response)
            
            analysis_time = time.time() - start_time
            
            if result:
                return {
                    'advice': result.get('advice', '观望'),
                    'confidence': result.get('confidence', 50),
                    'summary': result.get('summary', ''),
                    'action_plan': result.get('action_plan', []),
                    'position_suggestion': result.get('position_suggestion', ''),
                    'timing': result.get('timing', ''),
                    'key_message': result.get('key_message', ''),
                    'success': True,
                    'analysis_time': round(analysis_time, 2),
                    'raw_response': response,
                }
            else:
                return self._fallback_investment_advice(risk_assess, flow_data)
                
        except Exception as e:
            logger.error(f"投资建议生成失败: {e}")
            return self._fallback_investment_advice(risk_assess, flow_data)
    
    def run_full_analysis(self, hot_topics: List[Dict],
                           stock_news: List[Dict],
                           flow_data: Dict,
                           sentiment_data: Dict,
                           viral_k: float = 1.0,
                           flow_type: str = "未知") -> Dict:
        """
        运行完整的AI分析
        
        Returns:
            {
                'sector_analysis': Dict,
                'stock_recommend': Dict,
                'risk_assess': Dict,
                'investment_advice': Dict,
                'success': bool,
                'analysis_time': float,
            }
        """
        start_time = time.time()
        
        logger.info("🤖 开始AI分析...")
        
        # 1. 板块影响分析
        logger.info("  📊 分析板块影响...")
        sector_analysis = self.sector_impact_agent(hot_topics, stock_news, flow_data)
        
        # 2. 股票推荐
        logger.info("  📈 生成股票推荐...")
        flow_stage = sentiment_data.get('flow_stage', {}).get('stage_name', '未知')
        sentiment_class = sentiment_data.get('sentiment', {}).get('sentiment_class', '中性')
        stock_recommend = self.stock_recommend_agent(
            sector_analysis.get('benefited_sectors', []),
            flow_stage,
            sentiment_class
        )
        
        # 3. 风险评估
        logger.info("  ⚠️ 评估风险...")
        risk_assess = self.risk_assess_agent(
            flow_stage,
            sentiment_data.get('sentiment', {}),
            viral_k,
            flow_type
        )
        
        # 4. 综合投资建议
        logger.info("  💡 生成投资建议...")
        investment_advice = self.investment_advisor_agent(
            sector_analysis,
            stock_recommend,
            risk_assess,
            flow_data,
            sentiment_data.get('sentiment', {})
        )
        
        total_time = time.time() - start_time
        logger.info(f"✅ AI分析完成，耗时 {total_time:.2f} 秒")
        
        # 汇总结果
        return {
            'sector_analysis': sector_analysis,
            'stock_recommend': stock_recommend,
            'risk_assess': risk_assess,
            'investment_advice': investment_advice,
            'success': all([
                sector_analysis.get('success', False),
                stock_recommend.get('success', False),
                risk_assess.get('success', False),
                investment_advice.get('success', False),
            ]),
            'analysis_time': round(total_time, 2),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
    
    def analyze_sector_deep(self, sector_name: str, related_news: List[Dict], 
                            hot_topics: List[Dict]) -> Dict:
        """
        深度分析单个板块
        
        为每个热门板块单独调用DeepSeek进行深度分析
        """
        if not self.is_available():
            return {'success': False, 'error': 'AI不可用'}
        
        news_text = '\n'.join([
            f"- [{n.get('platform_name', '')}] {n.get('title', '')}"
            for n in related_news[:20]
        ])
        
        topics_text = '\n'.join([
            f"- {t['topic']} (热度:{t.get('heat', 0)})"
            for t in hot_topics[:10]
        ])
        
        try:
            messages = build_messages(
                "news_flow/sector_deep.system.txt",
                "news_flow/sector_deep.user.txt",
                sector_name=sector_name,
                news_text=news_text,
                topics_text=topics_text,
            )
            
            response = self.deepseek_client.call_api(
                messages,
                temperature=0.5,
                max_tokens=2000,
                tier=ModelTier.REASONING,
            )
            result = self._parse_json_response(response)
            
            if result:
                result['success'] = True
                return result
            else:
                return {'success': False, 'sector_name': sector_name}
                
        except Exception as e:
            logger.error(f"{sector_name}板块分析失败: {e}")
            return {'success': False, 'sector_name': sector_name, 'error': str(e)}
    
    def run_multi_sector_analysis(self, hot_topics: List[Dict], 
                                   stock_news: List[Dict],
                                   target_sectors: List[str] = None) -> Dict:
        """
        多板块并行分析
        
        对多个热门板块分别调用DeepSeek进行深度分析
        
        Args:
            hot_topics: 热门话题列表
            stock_news: 股票相关新闻
            target_sectors: 指定分析的板块列表，为None则自动识别
            
        Returns:
            {
                'sector_analyses': List[Dict],  # 各板块分析结果
                'summary': str,  # 综合总结
                'top_sectors': List[str],  # 最热门板块
                'success': bool
            }
        """
        if not self.is_available():
            return {'success': False, 'error': 'AI不可用', 'sector_analyses': []}
        
        start_time = time.time()
        
        # 如果没有指定板块，先识别热门板块
        if not target_sectors:
            target_sectors = self._identify_hot_sectors(hot_topics, stock_news)
        
        logger.info(f"🔍 开始分析 {len(target_sectors)} 个热门板块: {target_sectors}")
        
        # 对每个板块进行深度分析
        sector_analyses = []
        for sector in target_sectors[:5]:  # 最多分析5个板块
            logger.info(f"  📊 分析板块: {sector}")
            
            # 筛选与该板块相关的新闻
            related_news = self._filter_news_by_sector(stock_news, sector)
            related_topics = self._filter_topics_by_sector(hot_topics, sector)
            
            analysis = self.analyze_sector_deep(sector, related_news, related_topics)
            if analysis.get('success'):
                sector_analyses.append(analysis)
        
        # 生成综合总结
        summary = self._generate_multi_sector_summary(sector_analyses)
        
        total_time = time.time() - start_time
        logger.info(f"✅ 多板块分析完成，耗时 {total_time:.2f} 秒")
        
        return {
            'sector_analyses': sector_analyses,
            'summary': summary,
            'top_sectors': target_sectors[:5],
            'analysis_count': len(sector_analyses),
            'analysis_time': round(total_time, 2),
            'success': len(sector_analyses) > 0
        }
    
    def _identify_hot_sectors(self, hot_topics: List[Dict], stock_news: List[Dict]) -> List[str]:
        """识别热门板块"""
        # 板块关键词映射
        sector_keywords = {
            'AI人工智能': ['AI', '人工智能', '大模型', 'ChatGPT', '算力', '智能', 'DeepSeek', '机器人'],
            '新能源': ['新能源', '光伏', '锂电', '储能', '电池', '充电桩', '风电'],
            '半导体芯片': ['芯片', '半导体', '光刻', '封装', '晶圆', '国产替代'],
            '医药生物': ['医药', '生物', '疫苗', '创新药', '医疗', 'CXO'],
            '消费': ['消费', '白酒', '食品', '零售', '餐饮', '旅游'],
            '金融': ['银行', '保险', '券商', '证券', '金融'],
            '房地产': ['房地产', '地产', '楼市', '房价'],
            '军工': ['军工', '国防', '航空', '航天', '武器'],
            '汽车': ['汽车', '新能源车', '智能驾驶', '无人驾驶'],
            '低空经济': ['低空', '无人机', '飞行汽车', 'eVTOL'],
            '机器人': ['机器人', '人形机器人', '工业机器人', '减速器'],
            '数据要素': ['数据', '数据要素', '数据交易', '数字经济'],
        }
        
        # 统计各板块的热度
        sector_scores = {}
        
        # 从话题中统计
        for topic in hot_topics:
            topic_text = topic.get('topic', '')
            for sector, keywords in sector_keywords.items():
                for kw in keywords:
                    if kw in topic_text:
                        sector_scores[sector] = sector_scores.get(sector, 0) + topic.get('heat', 1)
                        break
        
        # 从新闻中统计
        for news in stock_news:
            news_text = (news.get('title') or '') + (news.get('content') or '')
            for sector, keywords in sector_keywords.items():
                for kw in keywords:
                    if kw in news_text:
                        sector_scores[sector] = sector_scores.get(sector, 0) + news.get('weight', 1)
                        break
        
        # 按热度排序
        sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
        return [s[0] for s in sorted_sectors[:5]]
    
    def _filter_news_by_sector(self, news_list: List[Dict], sector: str) -> List[Dict]:
        """筛选与板块相关的新闻"""
        sector_keywords = {
            'AI人工智能': ['AI', '人工智能', '大模型', 'ChatGPT', '算力', '智能', 'DeepSeek', '机器人'],
            '新能源': ['新能源', '光伏', '锂电', '储能', '电池', '充电桩', '风电'],
            '半导体芯片': ['芯片', '半导体', '光刻', '封装', '晶圆'],
            '医药生物': ['医药', '生物', '疫苗', '创新药', '医疗'],
            '消费': ['消费', '白酒', '食品', '零售', '餐饮'],
            '金融': ['银行', '保险', '券商', '证券', '金融'],
            '房地产': ['房地产', '地产', '楼市'],
            '军工': ['军工', '国防', '航空', '航天'],
            '汽车': ['汽车', '新能源车', '智能驾驶'],
            '低空经济': ['低空', '无人机', '飞行汽车'],
            '机器人': ['机器人', '人形机器人', '减速器'],
            '数据要素': ['数据', '数据要素', '数字经济'],
        }
        
        keywords = sector_keywords.get(sector, [sector])
        related = []
        
        for news in news_list:
            text = (news.get('title') or '') + (news.get('content') or '')
            for kw in keywords:
                if kw in text:
                    related.append(news)
                    break
        
        return related[:20]
    
    def _filter_topics_by_sector(self, topics: List[Dict], sector: str) -> List[Dict]:
        """筛选与板块相关的话题"""
        sector_keywords = {
            'AI人工智能': ['AI', '人工智能', '大模型', 'ChatGPT', '算力', '智能'],
            '新能源': ['新能源', '光伏', '锂电', '储能', '电池'],
            '半导体芯片': ['芯片', '半导体', '光刻'],
            '医药生物': ['医药', '生物', '疫苗', '医疗'],
            '消费': ['消费', '白酒', '食品', '餐饮'],
        }
        
        keywords = sector_keywords.get(sector, [sector])
        related = []
        
        for topic in topics:
            text = topic.get('topic', '')
            for kw in keywords:
                if kw in text:
                    related.append(topic)
                    break
        
        return related[:10]
    
    def _generate_multi_sector_summary(self, sector_analyses: List[Dict]) -> str:
        """生成多板块分析总结"""
        if not sector_analyses:
            return "暂无板块分析数据"
        
        # 按热度排序
        sorted_analyses = sorted(
            sector_analyses, 
            key=lambda x: x.get('heat_score', 0), 
            reverse=True
        )
        
        summary_parts = []
        summary_parts.append(f"共分析{len(sector_analyses)}个热门板块：")
        
        for i, analysis in enumerate(sorted_analyses[:3], 1):
            sector = analysis.get('sector_name', '未知')
            heat = analysis.get('heat_level', '中')
            outlook = analysis.get('short_term_outlook', '震荡')
            summary_parts.append(f"{i}. {sector}（热度{heat}，{outlook}）")
        
        return ' '.join(summary_parts)
    
    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """解析JSON响应"""
        decoder = json.JSONDecoder()
        try:
            text = str(response or "").strip()
            if not text:
                return None

            if '【推理过程】' in text:
                parts = text.split('【推理过程】')
                text = parts[-1] if len(parts) > 1 else parts[0]

            candidates = []
            fenced_blocks = re.findall(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
            for block in fenced_blocks:
                block_text = block.strip()
                if block_text:
                    candidates.append(block_text)
            candidates.append(text)

            for candidate in candidates:
                parsed = self._decode_first_json_object(candidate, decoder)
                if parsed is not None:
                    return parsed

            logger.error("JSON解析失败: 未找到可解析的JSON对象")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
            return None

    def _decode_first_json_object(self, text: str, decoder: json.JSONDecoder) -> Optional[Dict]:
        """从文本中提取第一个完整JSON对象。"""
        normalized = text.lstrip("\ufeff").strip()
        if not normalized:
            return None

        candidate_positions = []
        for marker in ("{", "["):
            search_from = 0
            while True:
                index = normalized.find(marker, search_from)
                if index < 0:
                    break
                candidate_positions.append(index)
                search_from = index + 1

        for start in sorted(set(candidate_positions)):
            snippet = normalized[start:].strip()
            if not snippet:
                continue
            try:
                parsed, _ = decoder.raw_decode(snippet)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
            if isinstance(parsed, list):
                first_dict = next((item for item in parsed if isinstance(item, dict)), None)
                if first_dict is not None:
                    return first_dict
        return None
    
    # ==================== 降级方法 ====================
    
    def _fallback_sector_analysis(self, hot_topics: List[Dict], 
                                   stock_news: List[Dict]) -> Dict:
        """板块分析降级方法"""
        # 基于关键词的简单分析
        sector_keywords = {
            'AI人工智能': ['AI', '人工智能', 'ChatGPT', '大模型', '算力', 'GPT'],
            '新能源': ['新能源', '锂电', '光伏', '风电', '储能', '充电桩'],
            '半导体': ['芯片', '半导体', '光刻机', '集成电路', '封测'],
            '医药生物': ['医药', '疫苗', '创新药', '医疗', '生物'],
            '消费': ['消费', '白酒', '食品', '零售', '餐饮'],
            '金融': ['银行', '保险', '券商', '金融', '信托'],
        }
        
        sector_hits = {}
        for topic in hot_topics:
            topic_text = topic.get('topic', '')
            heat = topic.get('heat', 0)
            for sector, keywords in sector_keywords.items():
                if any(kw in topic_text for kw in keywords):
                    if sector not in sector_hits:
                        sector_hits[sector] = 0
                    sector_hits[sector] += heat
        
        # 排序获取TOP板块
        sorted_sectors = sorted(sector_hits.items(), key=lambda x: x[1], reverse=True)
        
        benefited_sectors = [
            {
                'name': sector,
                'impact': '利好',
                'confidence': min(60, score // 2),
                'reason': f'热点话题关联度较高，热度得分{score}'
            }
            for sector, score in sorted_sectors[:5]
        ]
        
        return {
            'affected_sectors': benefited_sectors,
            'benefited_sectors': benefited_sectors,
            'damaged_sectors': [],
            'opportunity_assessment': '基于关键词匹配的简单分析，建议参考AI深度分析结果。',
            'key_points': ['AI分析不可用，使用降级方法'],
            'success': True,
            'fallback': True,
        }
    
    def _fallback_stock_recommend(self, hot_sectors: List[Dict]) -> Dict:
        """股票推荐降级方法"""
        return {
            'recommended_stocks': [],
            'overall_strategy': 'AI分析不可用，建议自行研究热门板块龙头股。',
            'risk_warning': '此为降级结果，请谨慎参考。',
            'success': True,
            'fallback': True,
        }
    
    def _fallback_risk_assess(self, flow_stage: str, 
                               sentiment_data: Dict, 
                               viral_k: float) -> Dict:
        """风险评估降级方法"""
        risk_score = 50
        risk_factors = []
        
        # 基于规则的简单风险评估
        if flow_stage in ['一致', 'consensus']:
            risk_score += 30
            risk_factors.append('流量处于一致阶段，可能是顶部')
        elif flow_stage in ['退潮', 'decline']:
            risk_score += 20
            risk_factors.append('流量正在退潮')
        
        sentiment_index = sentiment_data.get('sentiment_index', 50)
        if sentiment_index > 85:
            risk_score += 15
            risk_factors.append('情绪过度乐观')
        elif sentiment_index < 20:
            risk_score += 10
            risk_factors.append('情绪过度悲观')
        
        if viral_k > 1.5:
            risk_score += 15
            risk_factors.append(f'K值={viral_k}，流量指数型增长')
        
        risk_score = min(100, risk_score)
        
        if risk_score >= 80:
            risk_level = '极高'
        elif risk_score >= 60:
            risk_level = '高'
        elif risk_score >= 40:
            risk_level = '中等'
        elif risk_score >= 20:
            risk_level = '低'
        else:
            risk_level = '极低'
        
        return {
            'risk_level': risk_level,
            'risk_score': risk_score,
            'risk_factors': risk_factors,
            'opportunities': [],
            'analysis': '基于规则的简单风险评估，AI分析不可用。',
            'key_warning': '请谨慎参考，建议开启AI分析获取更准确的评估。',
            'success': True,
            'fallback': True,
        }
    
    def _fallback_investment_advice(self, risk_assess: Dict, 
                                     flow_data: Dict) -> Dict:
        """投资建议降级方法"""
        risk_level = risk_assess.get('risk_level', '中等')
        
        if risk_level in ['极高', '高']:
            advice = '回避'
            confidence = 70
            summary = '当前风险较高，建议保持观望或减仓。'
        elif risk_level == '中等':
            advice = '观望'
            confidence = 60
            summary = '市场状态中性，建议观望等待更明确的信号。'
        else:
            advice = '关注'
            confidence = 55
            summary = '风险较低，可关注热点板块机会。'
        
        return {
            'advice': advice,
            'confidence': confidence,
            'summary': summary,
            'action_plan': ['AI分析不可用，请自行判断'],
            'position_suggestion': '建议仓位不超过30%',
            'timing': '等待确认信号',
            'key_message': '此为降级结果，请谨慎参考。',
            'success': True,
            'fallback': True,
        }


# 全局实例
news_flow_agents = NewsFlowAgents()


# 测试代码
if __name__ == "__main__":
    print("=== 测试新闻流量智能分析代理 ===")
    
    # 检查AI是否可用
    if news_flow_agents.is_available():
        print("✅ AI客户端可用")
    else:
        print("⚠️ AI客户端不可用，将使用降级方法")
    
    # 模拟数据
    hot_topics = [
        {'topic': 'AI芯片', 'heat': 95, 'cross_platform': 5},
        {'topic': '新能源汽车', 'heat': 80, 'cross_platform': 4},
        {'topic': '涨停板', 'heat': 75, 'cross_platform': 3},
    ]
    
    stock_news = [
        {'platform_name': '东方财富', 'title': 'AI概念股集体大涨，龙头股涨停'},
        {'platform_name': '雪球', 'title': '新能源板块反弹，锂电池领涨'},
    ]
    
    flow_data = {
        'total_score': 650,
        'level': '高',
    }
    
    sentiment_data = {
        'sentiment': {'sentiment_index': 72, 'sentiment_class': '乐观'},
        'flow_stage': {'stage_name': '加速'},
    }
    
    # 运行板块分析
    print("\n--- 板块影响分析 ---")
    sector_result = news_flow_agents.sector_impact_agent(hot_topics, stock_news, flow_data)
    print(f"受益板块: {[s.get('name', '') for s in sector_result.get('benefited_sectors', [])]}")
    print(f"是否降级: {sector_result.get('fallback', False)}")
