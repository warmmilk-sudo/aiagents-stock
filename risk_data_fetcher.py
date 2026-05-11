"""
风险数据获取模块
使用pywencai获取股票风险相关信息：
1. 限售解禁数据
2. 大股东减持公告
3. 近期重要事件
"""

import pandas as pd
from typing import Dict, Any
import time
import warnings
import os
import re
import threading
from pywencai_runtime import setup_pywencai_runtime_env

setup_pywencai_runtime_env()
import pywencai

try:
    from config import RISK_QUERY_TIMEOUT_SECONDS
except Exception:
    RISK_QUERY_TIMEOUT_SECONDS = 10

# 屏蔽pywencai的Node.js警告信息（不影响功能）
warnings.filterwarnings('ignore', category=DeprecationWarning)
os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning'


class RiskDataFetcher:
    """风险数据获取类"""
    
    def __init__(self, query_timeout: int | None = None, pause_seconds: float = 0.5):
        """初始化"""
        configured_timeout = query_timeout or RISK_QUERY_TIMEOUT_SECONDS
        self.query_timeout = max(1, int(configured_timeout))
        self.pause_seconds = max(0.0, float(pause_seconds))
        self.ai_row_limit = max(1, int(os.getenv("RISK_PROMPT_ROW_LIMIT", "3") or 3))
        self.ai_value_char_limit = max(40, int(os.getenv("RISK_PROMPT_VALUE_CHAR_LIMIT", "90") or 90))
    
    def get_risk_data(self, symbol: str) -> Dict[str, Any]:
        """
        获取股票风险相关数据
        
        Args:
            symbol: 股票代码（如：600000）
            
        Returns:
            包含风险数据的字典
        """
        print(f"\n正在获取 {symbol} 的风险数据...")
        
        risk_data = {
            'symbol': symbol,
            'data_success': False,
            'lifting_ban': None,  # 限售解禁数据
            'shareholder_reduction': None,  # 大股东减持数据
            'important_events': None,  # 重要事件数据
            'error': None
        }
        
        try:
            # 1. 获取限售解禁数据
            print("   查询限售解禁数据...")
            lifting_ban = self._get_lifting_ban_data(symbol)
            risk_data['lifting_ban'] = lifting_ban
            if lifting_ban and lifting_ban.get('has_data'):
                print(f"   获取到限售解禁数据")
            else:
                print(f"   暂无限售解禁数据")
            
            self._sleep_between_queries()
            
            # 2. 获取大股东减持公告
            print("   查询大股东减持公告...")
            reduction = self._get_shareholder_reduction_data(symbol)
            risk_data['shareholder_reduction'] = reduction
            if reduction and reduction.get('has_data'):
                print(f"   获取到大股东减持数据")
            else:
                print(f"   暂无大股东减持数据")
            
            self._sleep_between_queries()
            
            # 3. 获取近期重要事件
            print("   查询近期重要事件...")
            events = self._get_important_events_data(symbol)
            risk_data['important_events'] = events
            if events and events.get('has_data'):
                print(f"   获取到重要事件数据")
            else:
                print(f"   暂无重要事件数据")

            section_errors = []
            for label, section_data in (
                ("限售解禁", lifting_ban),
                ("大股东减持", reduction),
                ("重要事件", events),
            ):
                if section_data and section_data.get('error'):
                    section_errors.append(f"{label}: {section_data['error']}")
            if section_errors:
                risk_data['error'] = "；".join(section_errors)
            
            # 如果至少有一个数据源成功，则认为获取成功
            if (lifting_ban and lifting_ban.get('has_data')) or \
               (reduction and reduction.get('has_data')) or \
               (events and events.get('has_data')):
                risk_data['data_success'] = True
                print(f"风险数据获取完成")
            else:
                print(f"未获取到风险相关数据")
                
        except Exception as e:
            print(f"风险数据获取失败: {str(e)}")
            risk_data['error'] = str(e)
        
        return risk_data

    def _sleep_between_queries(self) -> None:
        """在查询之间短暂暂停，避免连续请求过快。"""
        if self.pause_seconds > 0:
            time.sleep(self.pause_seconds)

    @staticmethod
    def _format_yyyymmdd(value: Any) -> str:
        text = str(value or "").strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:]}"
        return text

    def _call_tushare_risk_api(self, api_name: str, symbol: str):
        try:
            from data_source_manager import data_source_manager

            if not getattr(data_source_manager, "tushare_available", False):
                return None
            return data_source_manager.call_tushare_api(
                api_name,
                ts_code=data_source_manager._convert_to_ts_code(symbol),
                empty_ok=True,
            )
        except Exception:
            return None

    def _get_tushare_lifting_ban_data(self, symbol: str) -> Dict[str, Any]:
        result = {
            'has_data': False,
            'query': f"{symbol} Tushare限售解禁",
            'data': None,
            'summary': None,
            'source': 'tushare.share_float',
            'checked': False,
            'error': None,
        }
        df = self._call_tushare_risk_api("share_float", symbol)
        if df is None:
            return result
        result['checked'] = True
        if df.empty or 'float_date' not in df.columns:
            result['summary'] = "Tushare未返回限售解禁记录"
            return result

        view = df.copy()
        view['float_date'] = view['float_date'].astype(str)
        today = pd.Timestamp.today().strftime("%Y%m%d")
        view = view.loc[view['float_date'] >= today].sort_values('float_date')
        if view.empty:
            result['summary'] = "Tushare未发现未来限售解禁记录"
            return result

        normalized = pd.DataFrame({
            "解禁日期": view.get("float_date", "").map(self._format_yyyymmdd),
            "公告日期": view.get("ann_date", "").map(self._format_yyyymmdd) if "ann_date" in view.columns else "",
            "股东名称": view.get("holder_name", ""),
            "解禁股数": view.get("float_share", ""),
            "解禁比例": view.get("float_ratio", ""),
            "股份类型": view.get("share_type", ""),
        })
        result['has_data'] = True
        result['data'] = normalized
        result['summary'] = f"Tushare发现 {len(normalized)} 条未来限售解禁记录"
        return result

    def _get_tushare_holder_trade_data(self, symbol: str) -> Dict[str, Any]:
        result = {
            'has_data': False,
            'query': f"{symbol} Tushare股东增减持",
            'data': None,
            'summary': None,
            'source': 'tushare.stk_holdertrade',
            'checked': False,
            'error': None,
        }
        df = self._call_tushare_risk_api("stk_holdertrade", symbol)
        if df is None:
            return result
        result['checked'] = True
        if df.empty or 'ann_date' not in df.columns:
            result['summary'] = "Tushare未返回股东增减持记录"
            return result

        view = df.copy()
        view['ann_date'] = view['ann_date'].astype(str)
        since = (pd.Timestamp.today() - pd.Timedelta(days=180)).strftime("%Y%m%d")
        view = view.loc[view['ann_date'] >= since].sort_values('ann_date', ascending=False)
        if view.empty:
            result['summary'] = "Tushare近180日未发现股东减持记录"
            return result

        if "in_de" not in view.columns:
            result['summary'] = "Tushare股东增减持数据缺少方向字段，无法确认减持记录"
            return result

        view = view.loc[view["in_de"].astype(str).str.upper() == "DE"]
        if view.empty:
            result['summary'] = "Tushare近180日未发现股东减持记录"
            return result

        normalized = pd.DataFrame({
            "公告日期": view.get("ann_date", "").map(self._format_yyyymmdd),
            "股东名称": view.get("holder_name", ""),
            "股东类型": view.get("holder_type", ""),
            "方式": "减持",
            "减持股数": view.get("change_vol", ""),
            "减持比例": view.get("change_ratio", ""),
            "减持均价": view.get("avg_price", ""),
            "变动后持股": view.get("after_share", ""),
            "变动后持股比例": view.get("after_ratio", ""),
        })
        normalized = normalized.drop_duplicates().reset_index(drop=True)
        result['has_data'] = True
        result['data'] = normalized
        result['summary'] = f"Tushare发现近180日 {len(normalized)} 条股东减持记录"
        return result

    def _query_wencai(self, query: str):
        """在后台线程中执行问财查询，防止单次请求长期阻塞主流程。"""
        result_holder: Dict[str, Any] = {}

        def _runner() -> None:
            try:
                result_holder['result'] = pywencai.get(query=query, loop=True)
            except Exception as exc:
                result_holder['error'] = exc

        worker = threading.Thread(target=_runner, daemon=True, name="pywencai-risk-query")
        worker.start()
        worker.join(self.query_timeout)

        if worker.is_alive():
            raise TimeoutError(f"问财查询超时（>{self.query_timeout}s）: {query}")
        if 'error' in result_holder:
            raise result_holder['error']

        return result_holder.get('result')
    
    def _get_lifting_ban_data(self, symbol: str) -> Dict[str, Any]:
        """获取限售解禁数据"""
        tushare_result = self._get_tushare_lifting_ban_data(symbol)
        if tushare_result.get('checked'):
            return tushare_result

        result = {
            'has_data': False,
            'query': f"{symbol}限售解禁",
            'data': None,
            'summary': None,
            'error': None
        }
        
        try:
            # 构建问句
            query = f"{symbol}限售解禁"
            
            # 使用pywencai查询
            response = self._query_wencai(query)
            
            if response is None:
                return result
            
            # 处理返回结果
            df_result = self._convert_to_dataframe(response)
            
            if df_result is None or df_result.empty:
                return result

            # 提取有用的信息
            result['has_data'] = True
            result['data'] = df_result
            
            # 生成摘要
            summary = []
            
            # 尝试提取关键字段
            if '解禁时间' in df_result.columns or '限售解禁日' in df_result.columns:
                time_col = '解禁时间' if '解禁时间' in df_result.columns else '限售解禁日'
                summary.append(f"发现 {len(df_result)} 条解禁记录")
                
                # 提取最近的解禁记录
                recent_records = df_result.head(5)
                for idx, row in recent_records.iterrows():
                    record_info = []
                    if time_col in row.index:
                        record_info.append(f"日期: {row[time_col]}")
                    if '解禁股数' in row.index:
                        record_info.append(f"解禁股数: {row['解禁股数']}")
                    if '解禁市值' in row.index:
                        record_info.append(f"解禁市值: {row['解禁市值']}")
                    if '股东名称' in row.index:
                        record_info.append(f"股东: {row['股东名称']}")
                    
                    if record_info:
                        summary.append(" | ".join(record_info))
            else:
                # 如果没有标准字段，只记录有数据
                summary.append(f"获取到 {len(df_result)} 条相关记录")
            
            result['summary'] = "\n".join(summary) if summary else "有限售解禁数据"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _get_shareholder_reduction_data(self, symbol: str) -> Dict[str, Any]:
        """获取大股东减持公告数据"""
        tushare_result = self._get_tushare_holder_trade_data(symbol)
        if tushare_result.get('checked'):
            return tushare_result

        result = {
            'has_data': False,
            'query': f"{symbol}大股东减持公告",
            'data': None,
            'summary': None,
            'error': None
        }
        
        try:
            # 构建问句
            query = f"{symbol}大股东减持公告"
            
            # 使用pywencai查询
            response = self._query_wencai(query)
            
            if response is None:
                return result
            
            # 处理返回结果
            df_result = self._convert_to_dataframe(response)
            
            if df_result is None or df_result.empty:
                return result

            # 提取有用的信息
            result['has_data'] = True
            result['data'] = df_result
            
            # 生成摘要
            summary = []
            
            # 尝试提取关键字段
            if '公告日期' in df_result.columns or '减持日期' in df_result.columns:
                date_col = '公告日期' if '公告日期' in df_result.columns else '减持日期'
                summary.append(f"发现 {len(df_result)} 条减持公告")
                
                # 提取最近的减持记录
                recent_records = df_result.head(5)
                for idx, row in recent_records.iterrows():
                    record_info = []
                    if date_col in row.index:
                        record_info.append(f"日期: {row[date_col]}")
                    if '股东名称' in row.index:
                        record_info.append(f"股东: {row['股东名称']}")
                    if '减持股数' in row.index:
                        record_info.append(f"减持股数: {row['减持股数']}")
                    if '减持比例' in row.index:
                        record_info.append(f"减持比例: {row['减持比例']}")
                    
                    if record_info:
                        summary.append(" | ".join(record_info))
            else:
                # 如果没有标准字段，只记录有数据
                summary.append(f"获取到 {len(df_result)} 条相关记录")
            
            result['summary'] = "\n".join(summary) if summary else "有大股东减持数据"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _get_important_events_data(self, symbol: str) -> Dict[str, Any]:
        """获取近期重要事件数据"""
        result = {
            'has_data': False,
            'query': f"{symbol}近期重要事件",
            'data': None,
            'summary': None,
            'error': None
        }
        
        try:
            # 构建问句
            query = f"{symbol}近期重要事件"
            
            # 使用pywencai查询
            response = self._query_wencai(query)
            
            if response is None:
                return result
            
            # 处理返回结果
            df_result = self._convert_to_dataframe(response)
            
            if df_result is None or df_result.empty:
                return result

            if "近期重要事件" in df_result.columns:
                expanded_events = []
                for value in df_result["近期重要事件"]:
                    if isinstance(value, list):
                        for event in value:
                            if not isinstance(event, dict):
                                continue
                            expanded_events.append({
                                "日期": self._format_yyyymmdd(event.get("日期")),
                                "事件类型": event.get("事件"),
                                "事件内容": event.get("概要"),
                                "股票简称": event.get("股票简称"),
                                "股票代码": event.get("股票代码"),
                            })
                if expanded_events:
                    df_result = pd.DataFrame(expanded_events)
            
            # 提取有用的信息
            result['has_data'] = True
            result['data'] = df_result
            
            # 生成摘要
            summary = []
            
            # 尝试提取关键字段
            if '事件时间' in df_result.columns or '公告日期' in df_result.columns or '日期' in df_result.columns:
                time_col = '事件时间' if '事件时间' in df_result.columns else ('公告日期' if '公告日期' in df_result.columns else '日期')
                summary.append(f"发现 {len(df_result)} 条重要事件")
                
                # 提取最近的事件
                recent_events = df_result.head(10)
                for idx, row in recent_events.iterrows():
                    event_info = []
                    if time_col in row.index:
                        event_info.append(f"时间: {row[time_col]}")
                    if '事件类型' in row.index:
                        event_info.append(f"类型: {row['事件类型']}")
                    if '事件内容' in row.index:
                        content = str(row['事件内容'])[:100]  # 限制长度
                        event_info.append(f"内容: {content}")
                    elif '标题' in row.index:
                        title = str(row['标题'])[:100]
                        event_info.append(f"标题: {title}")
                    
                    if event_info:
                        summary.append(" | ".join(event_info))
            else:
                # 如果没有标准字段，只记录有数据
                summary.append(f"获取到 {len(df_result)} 条相关记录")
            
            result['summary'] = "\n".join(summary) if summary else "有重要事件数据"
            
        except Exception as e:
            result['error'] = str(e)
        
        return result
    
    def _convert_to_dataframe(self, result) -> pd.DataFrame:
        """将pywencai返回结果转换为DataFrame"""
        try:
            if result is None:
                return None
            
            df_result = None
            
            if isinstance(result, dict):
                try:
                    df_result = pd.DataFrame([result])
                except Exception:
                    return None
            elif isinstance(result, pd.DataFrame):
                df_result = result
            else:
                return None
            
            if df_result is None or df_result.empty:
                return None
            
            # 处理嵌套结构（tableV1）
            if 'tableV1' in df_result.columns and len(df_result.columns) == 1:
                table_v1_data = df_result.iloc[0]['tableV1']
                if isinstance(table_v1_data, pd.DataFrame):
                    df_result = table_v1_data
                elif isinstance(table_v1_data, list) and len(table_v1_data) > 0:
                    df_result = pd.DataFrame(table_v1_data)
                else:
                    return None
            
            # 处理嵌套结构（title_content等单列嵌套）
            # 如果只有一列，且该列的值是DataFrame，则展开
            if len(df_result.columns) == 1:
                col_name = df_result.columns[0]
                first_value = df_result.iloc[0][col_name]
                if isinstance(first_value, pd.DataFrame):
                    print(f"   检测到嵌套DataFrame（列名: {col_name}），正在展开...")
                    df_result = first_value
            
            return df_result if not df_result.empty else None
            
        except Exception as e:
            print(f"   转换DataFrame时出错: {str(e)}")
            return None
    
    def format_risk_data_for_ai(self, risk_data: Dict[str, Any]) -> str:
        """格式化风险数据供AI分析使用，避免将原始 DataFrame 整表塞入提示词。"""
        if not risk_data:
            return "未获取到风险数据"

        formatted_text = []

        try:
            sections = [
                (
                    "限售解禁",
                    risk_data.get('lifting_ban'),
                    (
                        ("日期", "时间", "解禁日"),
                        ("股东", "名称", "类型"),
                        ("解禁股数", "股份", "数量"),
                        ("解禁市值", "市值", "金额"),
                        ("比例", "占比"),
                    ),
                ),
                (
                    "大股东减持",
                    risk_data.get('shareholder_reduction'),
                    (
                        ("公告日期", "减持日期", "日期", "时间"),
                        ("股东", "名称"),
                        ("减持股数", "减持数量", "股份"),
                        ("减持比例", "比例", "占比"),
                        ("减持均价", "均价", "金额"),
                        ("方式", "进度", "状态"),
                    ),
                ),
                (
                    "重要事件",
                    risk_data.get('important_events'),
                    (
                        ("日期", "时间"),
                        ("事件", "标题", "事项", "摘要"),
                        ("类型", "分类"),
                        ("影响", "风险", "进展", "状态"),
                    ),
                ),
            ]

            for section_name, section_data, keyword_groups in sections:
                section_text = self._format_risk_section_for_ai(section_name, section_data, keyword_groups)
                if section_text:
                    formatted_text.append(section_text)

            if formatted_text:
                return "\n".join(formatted_text)
            return "暂无风险数据" if risk_data.get('data_success') else "未获取到风险数据"

        except Exception as e:
            print(f"格式化风险数据时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return f"格式化风险数据时出错: {str(e)}"

    @staticmethod
    def _is_empty_ai_value(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str) and not value.strip():
            return True
        try:
            return bool(pd.isna(value))
        except Exception:
            return False

    def _normalize_ai_value(self, value: Any, *, max_length: int | None = None) -> str:
        if self._is_empty_ai_value(value):
            return ""

        if isinstance(value, pd.Timestamp):
            text = value.strftime("%Y-%m-%d")
        else:
            text = str(value)

        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            return ""

        limit = max_length or self.ai_value_char_limit
        if len(text) > limit:
            text = text[:limit].rstrip() + "..."
        return text

    @staticmethod
    def _is_noise_column(column_name: str) -> bool:
        text = str(column_name or "").strip().lower()
        if not text:
            return True
        noise_keywords = (
            "id",
            "url",
            "image",
            "附件",
            "pdf",
            "html",
            "链接",
            "来源id",
            "source_id",
            "序号",
            "index",
        )
        return any(keyword in text for keyword in noise_keywords)

    @staticmethod
    def _looks_like_date_column(column_name: str) -> bool:
        text = str(column_name or "")
        return any(keyword in text for keyword in ("日期", "时间", "日"))

    def _normalize_ai_date(self, value: Any) -> str:
        text = self._normalize_ai_value(value, max_length=40)
        if not text:
            return ""
        try:
            parsed = pd.to_datetime(text, errors="coerce")
            if not pd.isna(parsed):
                return parsed.strftime("%Y-%m-%d")
        except Exception:
            pass
        return text

    def _select_relevant_columns(self, df: pd.DataFrame, keyword_groups: tuple[tuple[str, ...], ...]) -> list[str]:
        selected: list[str] = []
        selected_set = set()

        for keywords in keyword_groups:
            for column in df.columns:
                column_name = str(column or "").strip()
                if column_name in selected_set or self._is_noise_column(column_name):
                    continue
                if any(keyword in column_name for keyword in keywords):
                    selected.append(column_name)
                    selected_set.add(column_name)

        if selected:
            return selected[:6]

        for column in df.columns:
            column_name = str(column or "").strip()
            if column_name in selected_set or self._is_noise_column(column_name):
                continue
            selected.append(column_name)
            selected_set.add(column_name)
            if len(selected) >= 5:
                break
        return selected

    def _format_risk_section_for_ai(
        self,
        section_name: str,
        section_data: Dict[str, Any] | None,
        keyword_groups: tuple[tuple[str, ...], ...],
    ) -> str:
        if not section_data or not section_data.get('has_data'):
            if section_data and section_data.get("checked") and section_data.get("summary"):
                summary = self._normalize_ai_value(section_data.get("summary"), max_length=self.ai_value_char_limit * 2)
                return f"【{section_name}】\n- 摘要：{summary}" if summary else ""
            return ""

        df = section_data.get('data')
        if df is None or getattr(df, "empty", True):
            return ""

        lines = [f"【{section_name}】"]
        lines.append(f"- 记录数：{len(df)}，仅保留前 {min(len(df), self.ai_row_limit)} 条关键记录")

        summary = self._normalize_ai_value(section_data.get("summary"), max_length=self.ai_value_char_limit * 2)
        if summary:
            lines.append(f"- 摘要：{summary}")

        columns = self._select_relevant_columns(df, keyword_groups)
        for index, (_, row) in enumerate(df.head(self.ai_row_limit).iterrows(), 1):
            parts = []
            for column in columns:
                value = row.get(column)
                if self._is_empty_ai_value(value):
                    continue
                value_text = self._normalize_ai_date(value) if self._looks_like_date_column(column) else self._normalize_ai_value(value)
                if not value_text:
                    continue
                parts.append(f"{column}={value_text}")
            if parts:
                lines.append(f"- {index}. " + "；".join(parts))

        if len(lines) == 2:
            lines.append("- 无可用关键字段")
        return "\n".join(lines)
    
    def _format_dataframe_for_ai(self, df: pd.DataFrame, data_type: str) -> str:
        """将DataFrame格式化为AI易读的文本格式"""
        lines = []
        
        # 显示数据总数
        lines.append(f"共 {len(df)} 条{data_type}记录")
        lines.append("")
        
        # 显示列名
        lines.append(f"数据字段：{', '.join(df.columns.tolist())}")
        lines.append("")
        
        # 逐行显示数据（最多显示50条，避免数据过大）
        max_rows = min(50, len(df))
        
        for idx, row in df.head(max_rows).iterrows():
            lines.append(f"【记录 {idx + 1}】")
            
            # 显示每个字段的值
            for col in df.columns:
                value = row[col]
                
                # 处理不同类型的值
                if pd.isna(value):
                    value_str = "无数据"
                elif isinstance(value, (int, float)):
                    value_str = str(value)
                else:
                    value_str = str(value)
                    # 限制过长的字符串
                    if len(value_str) > 200:
                        value_str = value_str[:200] + "..."
                
                lines.append(f"  {col}: {value_str}")
            
            lines.append("")
        
        if len(df) > max_rows:
            lines.append(f"... 还有 {len(df) - max_rows} 条记录（已省略）")
            lines.append("")
        
        return "\n".join(lines)


# 测试代码
if __name__ == "__main__":
    fetcher = RiskDataFetcher()
    
    # 测试获取风险数据
    test_symbol = "600000"
    print(f"测试获取 {test_symbol} 的风险数据...")
    
    risk_data = fetcher.get_risk_data(test_symbol)
    
    print("\n" + "=" * 60)
    print("获取结果:")
    print("=" * 60)
    print(f"数据获取成功: {risk_data['data_success']}")
    
    if risk_data['data_success']:
        print("\n格式化的风险数据:")
        print(fetcher.format_risk_data_for_ai(risk_data))
