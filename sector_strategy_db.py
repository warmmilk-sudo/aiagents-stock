"""
智策板块数据库模块
用于存储板块策略历史数据和分析报告
"""

import sqlite3
from datetime import datetime, date
import json
import os
import re
import pandas as pd
import logging

from time_utils import local_now_str


class SectorStrategyDatabase:
    """智策板块数据库管理类"""

    LIFECYCLE_STAGE_STARTUP = "startup"
    LIFECYCLE_STAGE_EXPLOSIVE = "explosive"
    LIFECYCLE_STAGE_DECAY = "decay"
    LIFECYCLE_STAGE_NEUTRAL = "neutral"
    LIFECYCLE_LOOKBACK_DAYS = 15
    LIFECYCLE_WINDOWS = (3, 5, 10, 15)
    LIFECYCLE_MIN_OBSERVATIONS = {3: 3, 5: 4, 10: 7, 15: 10}
    LIFECYCLE_CONFIG_KEY = "lifecycle_config_v1"
    # 生命周期阈值固定在代码中，避免通过 UI / 数据库在线调参影响回放与选股口径。
    DEFAULT_LIFECYCLE_CONFIG = {
        "startup_current_min": 60.0,
        "startup_change_3d_min": 12.0,
        "startup_slope_3d_min": 5.0,
        "startup_current_vs_avg_3d_min": 8.0,
        "startup_acceleration_min": -3.0,
        "startup_change_5d_min": 18.0,
        "startup_drawdown_5d_max": 4.0,
        "startup_rising_5d_min": 3,
        "startup_falling_5d_max": 1,
        "startup_current_max": 88.0,
        "explosive_current_min": 83.5,
        "explosive_avg_10d_min": 68.0,
        "explosive_slope_10d_min": 1.3,
        "explosive_drawdown_10d_max": 8.0,
        "explosive_high_heat_days_min": 2,
        "explosive_rising_5d_min": 2,
        "explosive_falling_5d_max": 1,
        "explosive_current_vs_avg_5d_min": -1.0,
        "decay_peak_min": 88.0,
        "decay_drawdown_long_min": 14.0,
        "decay_change_5d_max": -8.0,
        "decay_change_3d_max": -10.0,
        "decay_falling_5d_min": 2,
        "decay_current_below_avg_min": 4.0,
    }
    
    def __init__(self, db_path='sector_strategy.db'):
        """
        初始化数据库
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path
        # 初始化日志
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(name)s: %(message)s')
        self.init_database()
        self._lifecycle_config_cache = self._load_lifecycle_config()
    
    def get_connection(self):
        """获取数据库连接"""
        return sqlite3.connect(self.db_path)

    def _make_json_serializable(self, value):
        """递归转换 pandas / numpy 对象，确保可被 JSON 序列化。"""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if isinstance(value, dict):
            return {
                str(key): self._make_json_serializable(item)
                for key, item in value.items()
            }

        if isinstance(value, (list, tuple, set)):
            return [self._make_json_serializable(item) for item in value]

        if isinstance(value, pd.DataFrame):
            return self._make_json_serializable(value.to_dict(orient='records'))

        if isinstance(value, pd.Series):
            return self._make_json_serializable(value.to_dict())

        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass

        if hasattr(value, 'item'):
            try:
                return self._make_json_serializable(value.item())
            except (TypeError, ValueError):
                pass

        if hasattr(value, 'tolist'):
            try:
                return self._make_json_serializable(value.tolist())
            except TypeError:
                pass

        return str(value)

    def _to_json(self, value, *, indent=None):
        """统一的 JSON 序列化入口。"""
        return json.dumps(
            self._make_json_serializable(value),
            ensure_ascii=False,
            indent=indent
        )

    @staticmethod
    def _coerce_config_value(default_value, raw_value):
        if isinstance(default_value, int) and not isinstance(default_value, bool):
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                return int(default_value)
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            return float(default_value)

    def _normalize_lifecycle_config(self, payload):
        normalized = dict(self.DEFAULT_LIFECYCLE_CONFIG)
        if not isinstance(payload, dict):
            return normalized
        for key, default_value in self.DEFAULT_LIFECYCLE_CONFIG.items():
            if key in payload:
                normalized[key] = self._coerce_config_value(default_value, payload.get(key))
        return normalized

    def _load_lifecycle_config(self):
        return dict(self.DEFAULT_LIFECYCLE_CONFIG)

    def get_lifecycle_config(self):
        self._lifecycle_config_cache = dict(self.DEFAULT_LIFECYCLE_CONFIG)
        return dict(self._lifecycle_config_cache)

    def update_lifecycle_config(self, payload):
        raise ValueError("生命周期阈值已固定在代码配置中，不支持在线修改")
    
    def init_database(self):
        """初始化数据库表"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 板块原始数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_raw_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_date TEXT NOT NULL,
            sector_code TEXT NOT NULL,
            sector_name TEXT,
            price REAL,
            change_pct REAL,
            volume REAL,
            turnover REAL,
            market_cap REAL,
            pe_ratio REAL,
            pb_ratio REAL,
            data_type TEXT,
            data_version INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(data_date, sector_code, data_type)
        )
        ''')
        
        # 创建索引
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sector_data_date ON sector_raw_data(data_date)
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sector_code ON sector_raw_data(sector_code)
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_data_type ON sector_raw_data(data_type)
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_data_version ON sector_raw_data(data_version)
        ''')
        
        # 板块新闻数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_news_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_date TEXT NOT NULL,
            title TEXT,
            content TEXT,
            source TEXT,
            url TEXT,
            related_sectors TEXT,
            sentiment_score REAL,
            importance_score REAL,
            data_version INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # AI分析报告表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_analysis_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_date TEXT NOT NULL,
            data_date_range TEXT,
            analysis_content TEXT,
            recommended_sectors TEXT,
            summary TEXT,
            confidence_score REAL,
            risk_level TEXT,
            investment_horizon TEXT,
            market_outlook TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 板块追踪表（记录推荐板块的后续表现）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id INTEGER,
            sector_code TEXT NOT NULL,
            sector_name TEXT,
            recommended_date TEXT,
            recommended_price REAL,
            target_price REAL,
            stop_loss_price REAL,
            current_price REAL,
            profit_loss_pct REAL,
            status TEXT,
            notes TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (analysis_id) REFERENCES sector_analysis_reports (id)
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_heat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id INTEGER NOT NULL,
            board_date TEXT,
            analysis_date TEXT NOT NULL,
            sector_name TEXT NOT NULL,
            normalized_sector_name TEXT NOT NULL,
            source_type TEXT,
            heat_score REAL NOT NULL DEFAULT 0,
            heat_group TEXT,
            heat_rank INTEGER DEFAULT 0,
            trend_text TEXT,
            sustainability TEXT,
            lifecycle_stage TEXT NOT NULL DEFAULT 'neutral',
            delta_1 REAL,
            delta_2 REAL,
            observation_count INTEGER NOT NULL DEFAULT 0,
            window_size_used INTEGER NOT NULL DEFAULT 0,
            trajectory_json TEXT,
            lifecycle_details_json TEXT,
            action_hint TEXT,
            defense_line_type TEXT NOT NULL DEFAULT 'NONE',
            selection_veto INTEGER NOT NULL DEFAULT 0,
            quant_stage TEXT,
            quant_confidence REAL,
            llm_stage TEXT,
            llm_confidence REAL,
            final_stage TEXT,
            stage_review_reason TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (analysis_id) REFERENCES sector_analysis_reports (id)
        )
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sector_heat_history_analysis
        ON sector_heat_history(analysis_id, heat_group, heat_rank)
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sector_heat_history_sector_date
        ON sector_heat_history(normalized_sector_name, datetime(analysis_date) DESC, id DESC)
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_heat_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            board_date TEXT NOT NULL,
            sector_name TEXT NOT NULL,
            normalized_sector_name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            heat_score REAL NOT NULL DEFAULT 0,
            change_pct REAL NOT NULL DEFAULT 0,
            turnover REAL NOT NULL DEFAULT 0,
            market_cap REAL NOT NULL DEFAULT 0,
            fund_flow_pct REAL NOT NULL DEFAULT 0,
            rank_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(board_date, normalized_sector_name)
        )
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sector_heat_daily_date
        ON sector_heat_daily(board_date, rank_order)
        ''')
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_sector_heat_daily_sector
        ON sector_heat_daily(normalized_sector_name, board_date DESC)
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sector_strategy_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        self._ensure_table_column(cursor, "sector_heat_history", "board_date", "TEXT")
        self._ensure_table_column(cursor, "sector_heat_history", "source_type", "TEXT")
        self._ensure_table_column(cursor, "sector_heat_history", "observation_count", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_table_column(cursor, "sector_heat_history", "window_size_used", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_table_column(cursor, "sector_heat_history", "lifecycle_details_json", "TEXT")
        self._ensure_table_column(cursor, "sector_heat_history", "quant_stage", "TEXT")
        self._ensure_table_column(cursor, "sector_heat_history", "quant_confidence", "REAL")
        self._ensure_table_column(cursor, "sector_heat_history", "llm_stage", "TEXT")
        self._ensure_table_column(cursor, "sector_heat_history", "llm_confidence", "REAL")
        self._ensure_table_column(cursor, "sector_heat_history", "final_stage", "TEXT")
        self._ensure_table_column(cursor, "sector_heat_history", "stage_review_reason", "TEXT")

        # 数据版本管理表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_type TEXT NOT NULL,
            data_date TEXT NOT NULL,
            version INTEGER NOT NULL,
            status TEXT DEFAULT 'active',
            fetch_success BOOLEAN DEFAULT 1,
            error_message TEXT,
            record_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(data_type, data_date, version)
        )
        ''')
        
        conn.commit()
        conn.close()
        
        self.logger.info("[智策板块] 数据库初始化完成")

    @staticmethod
    def _row_to_dict(cursor: sqlite3.Cursor, row):
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return dict(zip(columns, row)) if row else None

    @staticmethod
    def _ensure_table_column(cursor: sqlite3.Cursor, table_name: str, column_name: str, column_type: str):
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in cursor.fetchall()}
        if column_name not in existing_columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    @staticmethod
    def _extract_board_date(analysis_date, data_date_range=None):
        for value in (data_date_range, analysis_date):
            match = re.search(r"(\d{4}-\d{2}-\d{2})", str(value or ""))
            if match:
                return match.group(1)
        return str(analysis_date or "")[:10]

    @staticmethod
    def _normalize_sector_name(value):
        text = re.sub(r"\s+", "", str(value or "").strip())
        return text.replace("概念", "").replace("板块", "")

    @staticmethod
    def _to_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _extract_heat_payload(self, analysis_content):
        payload = analysis_content if isinstance(analysis_content, dict) else {}
        final_predictions = payload.get("final_predictions") if isinstance(payload.get("final_predictions"), dict) else {}
        heat = final_predictions.get("heat") if isinstance(final_predictions.get("heat"), dict) else {}
        return heat if isinstance(heat, dict) else {}

    def _extract_heat_items(self, analysis_content):
        heat_payload = self._extract_heat_payload(analysis_content)
        items = []
        for group_name in ("hottest", "heating", "cooling"):
            group_items = heat_payload.get(group_name)
            if not isinstance(group_items, list):
                continue
            for index, raw_item in enumerate(group_items, 1):
                if not isinstance(raw_item, dict):
                    continue
                sector_name = str(raw_item.get("sector") or raw_item.get("name") or "").strip()
                normalized_name = self._normalize_sector_name(sector_name)
                if not sector_name or not normalized_name:
                    continue
                items.append(
                    {
                        "sector_name": sector_name,
                        "normalized_sector_name": normalized_name,
                        "heat_score": self._to_float(raw_item.get("score"), 0.0),
                        "heat_group": group_name,
                        "heat_rank": index,
                        "trend_text": str(raw_item.get("trend") or "").strip(),
                        "sustainability": str(raw_item.get("sustainability") or "").strip(),
                    }
                )
        store = {}
        for item in items:
            existing = store.get(item["normalized_sector_name"])
            if existing is None or item["heat_score"] > existing["heat_score"]:
                store[item["normalized_sector_name"]] = item
        return list(store.values())

    @staticmethod
    def _percentile_rank(values):
        ordered = sorted(enumerate(values), key=lambda item: item[1], reverse=True)
        total = len(ordered)
        result = [50.0] * total
        if total <= 1:
            return [100.0] * total if total == 1 else []
        for position, (index, _value) in enumerate(ordered):
            result[index] = round((1 - (position / (total - 1))) * 100, 2)
        return result

    def _build_panel_rows_from_payload(self, board_date, analysis_content):
        payload = analysis_content if isinstance(analysis_content, dict) else {}
        data_summary = payload.get("data_summary") if isinstance(payload.get("data_summary"), dict) else {}
        sectors_payload = data_summary.get("sectors") if isinstance(data_summary.get("sectors"), dict) else {}
        concepts_payload = data_summary.get("concepts") if isinstance(data_summary.get("concepts"), dict) else {}
        raw_entries = []
        for source_type, boards in (("industry", sectors_payload), ("concept", concepts_payload)):
            for sector_name, board in boards.items():
                if not isinstance(board, dict):
                    continue
                normalized_name = self._normalize_sector_name(sector_name)
                if not normalized_name:
                    continue
                raw_entries.append(
                    {
                        "board_date": board_date,
                        "sector_name": str(sector_name or "").strip(),
                        "normalized_sector_name": normalized_name,
                        "source_type": source_type,
                        "change_pct": self._to_float(board.get("change_pct"), 0.0),
                        "turnover": self._to_float(board.get("turnover"), 0.0),
                        "market_cap": self._to_float(board.get("market_cap"), 0.0),
                        "fund_flow_pct": 0.0,
                    }
                )
        return self._score_daily_panel_rows(raw_entries)

    def _build_panel_rows_from_heat_payload(self, board_date, analysis_content):
        heat_items = self._extract_heat_items(analysis_content)
        if not heat_items:
            return []
        scored_rows = []
        for index, item in enumerate(
            sorted(heat_items, key=lambda row: self._to_float(row.get("heat_score"), 0.0), reverse=True),
            1,
        ):
            scored_rows.append(
                {
                    "board_date": board_date,
                    "sector_name": item.get("sector_name"),
                    "normalized_sector_name": item.get("normalized_sector_name"),
                    "source_type": "report_heat",
                    "change_pct": 0.0,
                    "turnover": 0.0,
                    "market_cap": 0.0,
                    "fund_flow_pct": 0.0,
                    "heat_score": round(self._to_float(item.get("heat_score"), 0.0), 2),
                    "rank_order": index,
                }
            )
        return scored_rows

    def _load_latest_raw_rows(self, cursor, board_date, data_type):
        cursor.execute(
            '''
            SELECT MAX(data_version)
            FROM sector_raw_data
            WHERE data_date = ? AND data_type = ?
            ''',
            (board_date, data_type),
        )
        version_row = cursor.fetchone()
        version = version_row[0] if version_row else None
        if version is None:
            return []
        cursor.execute(
            '''
            SELECT sector_name, sector_code, change_pct, turnover, market_cap
            FROM sector_raw_data
            WHERE data_date = ? AND data_type = ? AND data_version = ?
            ''',
            (board_date, data_type, int(version)),
        )
        return cursor.fetchall()

    def _load_fund_flow_ranks(self, cursor, board_date):
        cursor.execute(
            '''
            SELECT MAX(data_version)
            FROM sector_raw_data
            WHERE data_date = ? AND data_type = 'fund_flow'
            ''',
            (board_date,),
        )
        version_row = cursor.fetchone()
        version = version_row[0] if version_row else None
        if version is None:
            return {}
        cursor.execute(
            '''
            SELECT sector_name, change_pct
            FROM sector_raw_data
            WHERE data_date = ? AND data_type = 'fund_flow' AND data_version = ?
            ''',
            (board_date, int(version)),
        )
        rows = cursor.fetchall()
        if not rows:
            return {}
        normalized_names = [self._normalize_sector_name(row[0]) for row in rows]
        values = [self._to_float(row[1], 0.0) for row in rows]
        ranks = self._percentile_rank(values)
        return {
            normalized_name: {"rank": ranks[index], "value": values[index]}
            for index, normalized_name in enumerate(normalized_names)
            if normalized_name
        }

    def _build_panel_rows_from_raw(self, cursor, board_date):
        fund_flow_map = self._load_fund_flow_ranks(cursor, board_date)
        raw_entries = []
        for source_type in ("industry", "concept"):
            rows = self._load_latest_raw_rows(cursor, board_date, source_type)
            for sector_name, _sector_code, change_pct, turnover, market_cap in rows:
                normalized_name = self._normalize_sector_name(sector_name)
                if not normalized_name:
                    continue
                raw_entries.append(
                    {
                        "board_date": board_date,
                        "sector_name": str(sector_name or "").strip(),
                        "normalized_sector_name": normalized_name,
                        "source_type": source_type,
                        "change_pct": self._to_float(change_pct, 0.0),
                        "turnover": self._to_float(turnover, 0.0),
                        "market_cap": self._to_float(market_cap, 0.0),
                        "fund_flow_pct": self._to_float((fund_flow_map.get(normalized_name) or {}).get("value"), 0.0),
                    }
                )
        return self._score_daily_panel_rows(raw_entries)

    def _score_daily_panel_rows(self, raw_entries):
        if not raw_entries:
            return []

        grouped = {"industry": [], "concept": []}
        for entry in raw_entries:
            grouped.setdefault(entry.get("source_type") or "industry", []).append(entry)

        scored_entries = []
        for source_type, entries in grouped.items():
            if not entries:
                continue
            change_scores = self._percentile_rank([entry["change_pct"] for entry in entries])
            turnover_scores = self._percentile_rank([entry["turnover"] for entry in entries])
            fund_flow_scores = self._percentile_rank([entry["fund_flow_pct"] for entry in entries])
            for index, entry in enumerate(entries):
                penalty = max(0.0, min(25.0, abs(entry["change_pct"]) * 2.5)) if entry["change_pct"] < 0 else 0.0
                heat_score = max(
                    0.0,
                    min(
                        100.0,
                        0.65 * change_scores[index] + 0.25 * turnover_scores[index] + 0.10 * fund_flow_scores[index] - penalty,
                    ),
                )
                scored_entries.append(
                    {
                        **entry,
                        "heat_score": round(heat_score, 2),
                    }
                )

        merged = {}
        for entry in scored_entries:
            existing = merged.get(entry["normalized_sector_name"])
            if existing is None:
                merged[entry["normalized_sector_name"]] = dict(entry)
                continue
            if entry["heat_score"] > existing["heat_score"]:
                source_types = set(str(existing.get("source_type") or "").split("|")) | {entry["source_type"]}
                merged[entry["normalized_sector_name"]] = {
                    **entry,
                    "source_type": "|".join(sorted(filter(None, source_types))),
                }
            else:
                source_types = set(str(existing.get("source_type") or "").split("|")) | {entry["source_type"]}
                existing["source_type"] = "|".join(sorted(filter(None, source_types)))

        ranked = sorted(
            merged.values(),
            key=lambda item: (self._to_float(item.get("heat_score"), 0.0), self._to_float(item.get("change_pct"), 0.0)),
            reverse=True,
        )
        for index, item in enumerate(ranked, 1):
            item["rank_order"] = index
        return ranked

    def _upsert_daily_heat_panel(self, cursor, board_date, rows):
        cursor.execute("DELETE FROM sector_heat_daily WHERE board_date = ?", (board_date,))
        for row in rows:
            cursor.execute(
                '''
                INSERT INTO sector_heat_daily (
                    board_date, sector_name, normalized_sector_name, source_type,
                    heat_score, change_pct, turnover, market_cap, fund_flow_pct, rank_order
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    board_date,
                    row.get("sector_name"),
                    row.get("normalized_sector_name"),
                    row.get("source_type") or "industry",
                    self._to_float(row.get("heat_score"), 0.0),
                    self._to_float(row.get("change_pct"), 0.0),
                    self._to_float(row.get("turnover"), 0.0),
                    self._to_float(row.get("market_cap"), 0.0),
                    self._to_float(row.get("fund_flow_pct"), 0.0),
                    int(row.get("rank_order") or 0),
                ),
            )

    def _ensure_daily_heat_panel(self, cursor, board_date, analysis_content=None, *, prefer_payload=True):
        rows = []
        if prefer_payload and isinstance(analysis_content, dict):
            rows = self._build_panel_rows_from_payload(board_date, analysis_content)
            if not rows:
                rows = self._build_panel_rows_from_heat_payload(board_date, analysis_content)
        if not rows:
            rows = self._build_panel_rows_from_raw(cursor, board_date)
        if rows:
            self._upsert_daily_heat_panel(cursor, board_date, rows)
        return rows

    def _get_recent_heat_scores(self, cursor, normalized_sector_name, board_date, limit=2):
        cursor.execute(
            '''
            SELECT heat_score
            FROM sector_heat_daily
            WHERE normalized_sector_name = ? AND board_date < ?
            ORDER BY board_date DESC, id DESC
            LIMIT ?
            ''',
            (normalized_sector_name, board_date, int(limit)),
        )
        return [self._to_float(row[0], 0.0) for row in cursor.fetchall()]

    def _window_metrics(self, ordered_scores, window_size):
        scores = ordered_scores[-min(window_size, len(ordered_scores)) :]
        min_observations = self.LIFECYCLE_MIN_OBSERVATIONS.get(window_size, min(window_size, 3))
        if len(scores) < min_observations:
            return None
        deltas = [right - left for left, right in zip(scores, scores[1:])]
        peak = max(scores)
        return {
            "scores": scores,
            "count": len(scores),
            "current": scores[-1],
            "change": scores[-1] - scores[0],
            "avg": sum(scores) / len(scores),
            "slope": (scores[-1] - scores[0]) / max(1, len(scores) - 1),
            "rising": sum(1 for delta in deltas if delta > 0),
            "falling": sum(1 for delta in deltas if delta < 0),
            "peak": peak,
            "drawdown": peak - scores[-1],
            "acceleration": (deltas[-1] - deltas[-2]) if len(deltas) >= 2 else 0.0,
            "high_heat_days": sum(1 for score in scores if score >= 80),
        }

    @staticmethod
    def _clip_confidence(value):
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return None
        return round(max(0.0, min(0.99, numeric_value)), 2)

    def _compute_quant_confidence(self, stage, *, short_metrics, startup_metrics, explosive_metrics, decay_metrics):
        if stage == self.LIFECYCLE_STAGE_EXPLOSIVE and explosive_metrics:
            confidence = (
                0.72
                + max(0.0, explosive_metrics["current"] - self.DEFAULT_LIFECYCLE_CONFIG["explosive_current_min"]) / 80.0
                + max(0.0, explosive_metrics["avg"] - self.DEFAULT_LIFECYCLE_CONFIG["explosive_avg_10d_min"]) / 120.0
                + explosive_metrics["high_heat_days"] * 0.03
            )
            return self._clip_confidence(confidence)
        if stage == self.LIFECYCLE_STAGE_STARTUP and startup_metrics and short_metrics:
            confidence = (
                0.68
                + max(0.0, short_metrics["current"] - self.DEFAULT_LIFECYCLE_CONFIG["startup_current_min"]) / 120.0
                + max(0.0, startup_metrics["change"] - self.DEFAULT_LIFECYCLE_CONFIG["startup_change_5d_min"]) / 120.0
                + max(0.0, short_metrics["slope"] - self.DEFAULT_LIFECYCLE_CONFIG["startup_slope_3d_min"]) / 40.0
            )
            return self._clip_confidence(confidence)
        if stage == self.LIFECYCLE_STAGE_DECAY and decay_metrics and startup_metrics:
            confidence = (
                0.78
                + max(0.0, decay_metrics["drawdown"] - self.DEFAULT_LIFECYCLE_CONFIG["decay_drawdown_long_min"]) / 120.0
                + max(0.0, abs(startup_metrics["change"])) / 120.0
            )
            return self._clip_confidence(confidence)
        return 0.56

    def _extract_report_sector_bias(self, analysis_content, sector_name):
        payload = analysis_content if isinstance(analysis_content, dict) else {}
        final_predictions = payload.get("final_predictions") if isinstance(payload.get("final_predictions"), dict) else {}
        normalized_target = self._normalize_sector_name(sector_name)
        hot_signals = []
        cooling_signals = []

        def _match(raw_name):
            normalized_name = self._normalize_sector_name(raw_name)
            if not normalized_name or not normalized_target:
                return False
            return (
                normalized_name == normalized_target
                or normalized_name in normalized_target
                or normalized_target in normalized_name
            )

        heat = final_predictions.get("heat") if isinstance(final_predictions.get("heat"), dict) else {}
        for group_name in ("hottest", "heating"):
            for item in heat.get(group_name) or []:
                if isinstance(item, dict) and _match(item.get("sector") or item.get("name")):
                    hot_signals.append(f"heat.{group_name}")
        for group_name in ("cooling",):
            for item in heat.get(group_name) or []:
                if isinstance(item, dict) and _match(item.get("sector") or item.get("name")):
                    cooling_signals.append(f"heat.{group_name}")

        long_short = final_predictions.get("long_short") if isinstance(final_predictions.get("long_short"), dict) else {}
        for item in long_short.get("bullish") or []:
            if isinstance(item, dict) and _match(item.get("sector") or item.get("name")):
                hot_signals.append("long_short.bullish")
        for item in long_short.get("bearish") or []:
            if isinstance(item, dict) and _match(item.get("sector") or item.get("name")):
                cooling_signals.append("long_short.bearish")

        rotation = final_predictions.get("rotation") if isinstance(final_predictions.get("rotation"), dict) else {}
        for group_name in ("current_strong", "potential"):
            for item in rotation.get(group_name) or []:
                if isinstance(item, dict) and _match(item.get("sector") or item.get("name")):
                    hot_signals.append(f"rotation.{group_name}")
        for item in rotation.get("declining") or []:
            if isinstance(item, dict) and _match(item.get("sector") or item.get("name")):
                cooling_signals.append("rotation.declining")

        return {
            "hot_signals": hot_signals,
            "cooling_signals": cooling_signals,
            "is_hot": bool(hot_signals),
            "is_cooling": bool(cooling_signals),
        }

    def _should_review_lifecycle_stage(self, *, quant_stage, quant_confidence, report_bias, rank_order):
        return bool(
            quant_confidence < 0.82
            or (quant_stage == self.LIFECYCLE_STAGE_NEUTRAL and int(rank_order or 0) <= 10)
            or (quant_stage in {self.LIFECYCLE_STAGE_NEUTRAL, self.LIFECYCLE_STAGE_DECAY} and report_bias.get("is_hot"))
            or (quant_stage in {self.LIFECYCLE_STAGE_STARTUP, self.LIFECYCLE_STAGE_EXPLOSIVE} and report_bias.get("is_cooling"))
        )

    def _call_stage_review_llm(self, *, sector_name, quant_stage, quant_confidence, trajectory, lifecycle_details, report_bias):
        if str(os.getenv("SMART_SELECTION_ENABLE_STAGE_REVIEW_LLM") or "").strip() != "1":
            return {}
        try:
            from llm_client import LLMClient
            from model_routing import ModelTier
            from prompt_registry import build_messages
        except Exception:
            return {}
        try:
            client = LLMClient()
            messages = build_messages(
                "smart_selection/sector_stage_review.system.txt",
                "smart_selection/sector_stage_review.user.txt",
                sector_name=sector_name,
                aliases_payload=self._to_json([sector_name]),
                source_type="sector_heat_history",
                quant_stage=quant_stage,
                quant_confidence=quant_confidence,
                trajectory_payload=self._to_json(trajectory or []),
                lifecycle_details_payload=self._to_json(lifecycle_details or {}),
                report_bias_payload=self._to_json(report_bias or {}),
            )
            response = client.call_api(
                messages,
                max_tokens=500,
                tier=ModelTier.LIGHTWEIGHT,
            )
            parsed = json.loads(re.search(r"\{[\s\S]*\}", str(response or "")).group(0))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _review_lifecycle_stage(self, *, sector_name, quant_stage, quant_confidence, rank_order, analysis_content, trajectory, lifecycle_details, current_score):
        report_bias = self._extract_report_sector_bias(analysis_content, sector_name)
        final_stage = quant_stage
        llm_stage = ""
        llm_confidence = None
        review_reason = "量化阶段直接采用"

        if quant_stage == self.LIFECYCLE_STAGE_DECAY and quant_confidence >= 0.82:
            return {
                "llm_stage": llm_stage,
                "llm_confidence": llm_confidence,
                "final_stage": final_stage,
                "stage_review_reason": "高置信衰退期，保持量化一票否决",
            }
        if quant_stage in {self.LIFECYCLE_STAGE_STARTUP, self.LIFECYCLE_STAGE_EXPLOSIVE} and quant_confidence >= 0.82:
            return {
                "llm_stage": llm_stage,
                "llm_confidence": llm_confidence,
                "final_stage": final_stage,
                "stage_review_reason": "高置信启动/爆发期，直接采用量化结果",
            }

        if self._should_review_lifecycle_stage(
            quant_stage=quant_stage,
            quant_confidence=quant_confidence,
            report_bias=report_bias,
            rank_order=rank_order,
        ):
            llm_payload = self._call_stage_review_llm(
                sector_name=sector_name,
                quant_stage=quant_stage,
                quant_confidence=quant_confidence,
                trajectory=trajectory,
                lifecycle_details=lifecycle_details,
                report_bias=report_bias,
            )
            llm_stage = str(llm_payload.get("final_stage") or "").strip()
            llm_confidence = self._clip_confidence(llm_payload.get("confidence")) if llm_payload.get("confidence") not in (None, "") else None
            review_reason = str(llm_payload.get("reason") or "").strip() or "边界样本复核"

            if quant_stage == self.LIFECYCLE_STAGE_NEUTRAL and report_bias.get("is_hot") and current_score >= 75:
                final_stage = self.LIFECYCLE_STAGE_EXPLOSIVE if current_score >= 85 else self.LIFECYCLE_STAGE_STARTUP
                review_reason = review_reason or "热点板块与量化中性冲突，提升为可观察阶段"
            elif quant_stage in {self.LIFECYCLE_STAGE_STARTUP, self.LIFECYCLE_STAGE_EXPLOSIVE} and report_bias.get("is_cooling") and quant_confidence < 0.82:
                final_stage = self.LIFECYCLE_STAGE_NEUTRAL
                review_reason = review_reason or "量化偏强但报告显示降温，降级为中性"

            if llm_stage in {
                self.LIFECYCLE_STAGE_STARTUP,
                self.LIFECYCLE_STAGE_EXPLOSIVE,
                self.LIFECYCLE_STAGE_DECAY,
                self.LIFECYCLE_STAGE_NEUTRAL,
            } and (llm_confidence or 0) >= 0.55:
                final_stage = llm_stage
        return {
            "llm_stage": llm_stage,
            "llm_confidence": llm_confidence,
            "final_stage": final_stage,
            "stage_review_reason": review_reason,
        }

    def _build_lifecycle_payload(self, current_score, previous_scores, *, sector_name="", rank_order=0, analysis_content=None):
        config = self.get_lifecycle_config()
        ordered_scores = list(reversed(previous_scores)) + [current_score]
        delta_1 = None
        delta_2 = None
        if len(ordered_scores) >= 2:
            delta_1 = ordered_scores[-1] - ordered_scores[-2]
        if len(ordered_scores) >= 3:
            delta_2 = (ordered_scores[-1] - ordered_scores[-2]) - (ordered_scores[-2] - ordered_scores[-3])
        metrics = {
            window_size: self._window_metrics(ordered_scores, window_size)
            for window_size in self.LIFECYCLE_WINDOWS
        }
        short_metrics = metrics.get(3)
        startup_metrics = metrics.get(5) or metrics.get(3)
        explosive_metrics = metrics.get(10) or metrics.get(5) or metrics.get(3)
        decay_metrics = metrics.get(15) or metrics.get(10) or metrics.get(5) or metrics.get(3)

        quant_stage = self.LIFECYCLE_STAGE_NEUTRAL
        defense_line_type = "NONE"
        selection_veto = False
        action_hint = "仅展示，不推动动作"
        window_size_used = max((window_size for window_size, item in metrics.items() if item), default=len(ordered_scores))

        if short_metrics and startup_metrics:
            startup_signal = (
                short_metrics["current"] >= config["startup_current_min"]
                and short_metrics["change"] >= config["startup_change_3d_min"]
                and short_metrics["slope"] >= config["startup_slope_3d_min"]
                and short_metrics["rising"] >= 2
                and short_metrics["falling"] == 0
                and short_metrics["current"] >= short_metrics["avg"] + config["startup_current_vs_avg_3d_min"]
                and short_metrics["acceleration"] >= config["startup_acceleration_min"]
                and startup_metrics["change"] >= config["startup_change_5d_min"]
                and startup_metrics["drawdown"] <= config["startup_drawdown_5d_max"]
                and startup_metrics["rising"] >= min(startup_metrics["count"] - 1, int(config["startup_rising_5d_min"]))
                and startup_metrics["falling"] <= int(config["startup_falling_5d_max"])
                and current_score < config["startup_current_max"]
            )
        else:
            startup_signal = False

        explosive_required_high_days = int(config["explosive_high_heat_days_min"])
        explosive_signal = bool(
            explosive_metrics
            and startup_metrics
            and explosive_metrics["current"] >= config["explosive_current_min"]
            and explosive_metrics["avg"] >= config["explosive_avg_10d_min"]
            and explosive_metrics["slope"] >= config["explosive_slope_10d_min"]
            and explosive_metrics["drawdown"] <= config["explosive_drawdown_10d_max"]
            and explosive_metrics["high_heat_days"] >= explosive_required_high_days
            and startup_metrics["rising"] >= int(config["explosive_rising_5d_min"])
            and startup_metrics["falling"] <= int(config["explosive_falling_5d_max"])
            and startup_metrics["current"] >= startup_metrics["avg"] + config["explosive_current_vs_avg_5d_min"]
        )

        decay_required_drawdown = (
            config["decay_drawdown_long_min"]
            if decay_metrics and decay_metrics["count"] >= 10
            else min(float(config["decay_drawdown_long_min"]), 10.0)
        )
        decay_required_falling = (
            int(config["decay_falling_5d_min"])
            if startup_metrics and startup_metrics["count"] >= 4
            else 1
        )
        decay_signal = bool(
            decay_metrics
            and startup_metrics
            and decay_metrics["peak"] >= config["decay_peak_min"]
            and decay_metrics["drawdown"] >= decay_required_drawdown
            and (
                startup_metrics["change"] <= config["decay_change_5d_max"]
                or (short_metrics and short_metrics["change"] <= config["decay_change_3d_max"])
            )
            and startup_metrics["falling"] >= decay_required_falling
            and current_score <= (decay_metrics["avg"] - config["decay_current_below_avg_min"])
        )

        if decay_signal:
            quant_stage = self.LIFECYCLE_STAGE_DECAY
            defense_line_type = "NONE"
            selection_veto = True
            action_hint = "衰退期，板块级一票否决"
        elif explosive_signal:
            quant_stage = self.LIFECYCLE_STAGE_EXPLOSIVE
            defense_line_type = "MA5"
            action_hint = "爆发期，允许进入尾盘执行候选"
        elif startup_signal:
            quant_stage = self.LIFECYCLE_STAGE_STARTUP
            defense_line_type = "MA10"
            action_hint = "启动期，进入 MA10 重点观察池"
        trajectory = [{"day_offset": offset - len(ordered_scores) + 1, "score": round(score, 2)} for offset, score in enumerate(ordered_scores)]
        quant_confidence = self._compute_quant_confidence(
            quant_stage,
            short_metrics=short_metrics,
            startup_metrics=startup_metrics,
            explosive_metrics=explosive_metrics,
            decay_metrics=decay_metrics,
        )
        lifecycle_details = {
            str(window_size): {
                key: round(value, 2) if isinstance(value, float) else value
                for key, value in (window_metrics or {}).items()
                if key != "scores"
            }
            for window_size, window_metrics in metrics.items()
            if window_metrics
        }
        review_payload = self._review_lifecycle_stage(
            sector_name=sector_name,
            quant_stage=quant_stage,
            quant_confidence=quant_confidence,
            rank_order=rank_order,
            analysis_content=analysis_content if isinstance(analysis_content, dict) else {},
            trajectory=trajectory,
            lifecycle_details=lifecycle_details,
            current_score=current_score,
        )
        lifecycle_stage = review_payload.get("final_stage") or quant_stage
        if quant_stage == self.LIFECYCLE_STAGE_DECAY and quant_confidence >= 0.82:
            lifecycle_stage = quant_stage
            review_payload["stage_review_reason"] = "高置信衰退期，保持量化一票否决"
        elif quant_stage in {self.LIFECYCLE_STAGE_STARTUP, self.LIFECYCLE_STAGE_EXPLOSIVE} and quant_confidence >= 0.82:
            lifecycle_stage = quant_stage
            review_payload["stage_review_reason"] = "高置信启动/爆发期，直接采用量化结果"
        if lifecycle_stage == self.LIFECYCLE_STAGE_DECAY:
            defense_line_type = "NONE"
            selection_veto = True
            action_hint = "衰退期，板块级一票否决"
        elif lifecycle_stage == self.LIFECYCLE_STAGE_EXPLOSIVE:
            defense_line_type = "MA5"
            selection_veto = False
            action_hint = "爆发期，允许进入尾盘执行候选"
        elif lifecycle_stage == self.LIFECYCLE_STAGE_STARTUP:
            defense_line_type = "MA10"
            selection_veto = False
            action_hint = "启动期，进入 MA10 重点观察池"
        else:
            defense_line_type = "NONE"
            selection_veto = False
            action_hint = "仅展示，不推动动作"
        return {
            "lifecycle_stage": lifecycle_stage,
            "quant_stage": quant_stage,
            "quant_confidence": quant_confidence,
            "llm_stage": review_payload.get("llm_stage") or "",
            "llm_confidence": review_payload.get("llm_confidence"),
            "final_stage": lifecycle_stage,
            "stage_review_reason": review_payload.get("stage_review_reason") or "",
            "delta_1": round(delta_1, 2) if delta_1 is not None else None,
            "delta_2": round(delta_2, 2) if delta_2 is not None else None,
            "trajectory": trajectory,
            "action_hint": action_hint,
            "defense_line_type": defense_line_type,
            "selection_veto": selection_veto,
            "observation_count": len(ordered_scores),
            "window_size_used": int(window_size_used or 0),
            "lifecycle_details": lifecycle_details,
            "config_snapshot": {
                key: self._coerce_config_value(default_value, config.get(key))
                for key, default_value in self.DEFAULT_LIFECYCLE_CONFIG.items()
            },
        }

    def _save_heat_history(self, cursor, analysis_id, analysis_date, analysis_content, data_date_range=None):
        board_date = self._extract_board_date(analysis_date, data_date_range)
        cursor.execute(
            '''
            SELECT sector_name, normalized_sector_name, source_type, heat_score, rank_order
            FROM sector_heat_daily
            WHERE board_date = ?
            ORDER BY rank_order ASC, id ASC
            ''',
            (board_date,),
        )
        panel_rows = cursor.fetchall()
        if not panel_rows:
            return
        cursor.execute("DELETE FROM sector_heat_history WHERE analysis_id = ?", (analysis_id,))
        for sector_name, normalized_sector_name, source_type, heat_score, rank_order in panel_rows:
            previous_scores = self._get_recent_heat_scores(
                cursor,
                normalized_sector_name,
                board_date,
                limit=max(0, int(self.LIFECYCLE_LOOKBACK_DAYS) - 1),
            )
            lifecycle = self._build_lifecycle_payload(
                self._to_float(heat_score, 0.0),
                previous_scores,
                sector_name=sector_name,
                rank_order=rank_order,
                analysis_content=analysis_content,
            )
            cursor.execute(
                '''
                INSERT INTO sector_heat_history (
                    analysis_id, board_date, analysis_date, sector_name, normalized_sector_name,
                    source_type, heat_score, heat_group, heat_rank, trend_text, sustainability,
                    lifecycle_stage, delta_1, delta_2, observation_count, window_size_used,
                    trajectory_json, lifecycle_details_json, action_hint, defense_line_type, selection_veto,
                    quant_stage, quant_confidence, llm_stage, llm_confidence, final_stage, stage_review_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    int(analysis_id),
                    board_date,
                    analysis_date,
                    sector_name,
                    normalized_sector_name,
                    source_type,
                    self._to_float(heat_score, 0.0),
                    None,
                    int(rank_order or 0),
                    "",
                    "",
                    lifecycle["lifecycle_stage"],
                    lifecycle["delta_1"],
                    lifecycle["delta_2"],
                    int(lifecycle["observation_count"]),
                    int(lifecycle["window_size_used"]),
                    self._to_json(lifecycle["trajectory"]),
                    self._to_json(lifecycle["lifecycle_details"]),
                    lifecycle["action_hint"],
                    lifecycle["defense_line_type"],
                    1 if lifecycle["selection_veto"] else 0,
                    lifecycle.get("quant_stage"),
                    self._to_float(lifecycle.get("quant_confidence"), 0.0),
                    lifecycle.get("llm_stage"),
                    self._to_float(lifecycle.get("llm_confidence"), 0.0) if lifecycle.get("llm_confidence") is not None else None,
                    lifecycle.get("final_stage"),
                    lifecycle.get("stage_review_reason"),
                ),
            )
    
    def save_raw_data(self, data_date, data_type, data_df, version=None):
        """
        保存原始数据
        
        Args:
            data_date: 数据日期
            data_type: 数据类型 (sector_data, news_data等)
            data_df: 数据DataFrame
            version: 数据版本号，如果为None则自动生成
            
        Returns:
            int: 数据版本号
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 获取或生成版本号
            if version is None:
                cursor.execute('''
                SELECT COALESCE(MAX(version), 0) + 1 
                FROM data_versions 
                WHERE data_type = ? AND data_date = ?
                ''', (data_type, data_date))
                version = cursor.fetchone()[0]
            
            # 保存数据
            if data_type == 'sector_data':
                self._save_sector_data(cursor, data_date, data_df, version)
            elif data_type == 'news_data':
                self._save_news_data(cursor, data_date, data_df, version)
            
            # 记录版本信息
            cursor.execute('''
            INSERT OR REPLACE INTO data_versions 
            (data_type, data_date, version, status, fetch_success, record_count)
            VALUES (?, ?, ?, 'active', 1, ?)
            ''', (data_type, data_date, version, len(data_df)))
            
            conn.commit()
            self.logger.info(f"[智策板块] 保存{data_type}数据成功 (日期: {data_date}, 版本: {version}, 记录数: {len(data_df)})")
            return version
            
        except Exception as e:
            conn.rollback()
            # 记录失败版本
            cursor.execute('''
            INSERT OR REPLACE INTO data_versions 
            (data_type, data_date, version, status, fetch_success, error_message, record_count)
            VALUES (?, ?, ?, 'failed', 0, ?, 0)
            ''', (data_type, data_date, version or 1, str(e)))
            conn.commit()
            self.logger.error(f"[智策板块] 保存{data_type}数据失败: {e}")
            raise
        finally:
            conn.close()
    
    def _save_sector_data(self, cursor, data_date, data_df, version):
        """保存板块数据"""
        for _, row in data_df.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO sector_raw_data 
            (data_date, sector_code, sector_name, price, change_pct, volume, 
             turnover, market_cap, pe_ratio, pb_ratio, data_type, data_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'sector_data', ?)
            ''', (
                data_date,
                row.get('sector_code', ''),
                row.get('sector_name', ''),
                row.get('price', 0),
                row.get('change_pct', 0),
                row.get('volume', 0),
                row.get('turnover', 0),
                row.get('market_cap', 0),
                row.get('pe_ratio', 0),
                row.get('pb_ratio', 0),
                version
            ))
    
    def _save_news_data(self, cursor, data_date, data_df, version):
        """保存新闻数据"""
        for _, row in data_df.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO sector_news_data 
            (news_date, title, content, source, url, related_sectors, 
             sentiment_score, importance_score, data_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data_date,
                row.get('title', ''),
                row.get('content', ''),
                row.get('source', ''),
                row.get('url', ''),
                json.dumps(row.get('related_sectors', []), ensure_ascii=False),
                row.get('sentiment_score', 0),
                row.get('importance_score', 0),
                version
            ))
    
    def get_latest_data(self, data_type, data_date=None):
        """
        获取最新的成功数据
        
        Args:
            data_type: 数据类型
            data_date: 指定日期，如果为None则获取最新日期的数据
            
        Returns:
            pd.DataFrame: 数据DataFrame
        """
        conn = self.get_connection()
        
        try:
            # 获取最新成功的数据版本
            if data_date:
                query = '''
                SELECT version FROM data_versions 
                WHERE data_type = ? AND data_date = ? AND fetch_success = 1
                ORDER BY version DESC LIMIT 1
                '''
                params = [data_type, data_date]
            else:
                query = '''
                SELECT data_date, version FROM data_versions 
                WHERE data_type = ? AND fetch_success = 1
                ORDER BY data_date DESC, version DESC LIMIT 1
                '''
                params = [data_type]
            
            version_df = pd.read_sql_query(query, conn, params=params)
            
            if version_df.empty:
                self.logger.warning(f"[智策板块] 未找到{data_type}的成功数据")
                return pd.DataFrame()
            
            if data_date is None:
                data_date = version_df.iloc[0]['data_date']
            version = version_df.iloc[0]['version']
            
            # 获取具体数据
            if data_type == 'sector_data':
                data_query = '''
                SELECT * FROM sector_raw_data 
                WHERE data_date = ? AND data_version = ?
                ORDER BY sector_code
                '''
            elif data_type == 'news_data':
                data_query = '''
                SELECT * FROM sector_news_data 
                WHERE news_date = ? AND data_version = ?
                ORDER BY importance_score DESC
                '''
            else:
                return pd.DataFrame()
            
            data_df = pd.read_sql_query(data_query, conn, params=[data_date, version])
            self.logger.info(f"[智策板块] 获取{data_type}数据成功 (日期: {data_date}, 版本: {version}, 记录数: {len(data_df)})")
            return data_df
            
        except Exception as e:
            self.logger.error(f"[智策板块] 获取{data_type}数据失败: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def save_analysis_report(self, data_date_range, analysis_content, 
                           recommended_sectors, summary, confidence_score=None,
                           risk_level=None, investment_horizon=None, market_outlook=None):
        """
        保存AI分析报告
        
        Args:
            data_date_range: 数据日期范围
            analysis_content: 分析内容（JSON字符串或字典）
            recommended_sectors: 推荐板块列表
            summary: 摘要
            confidence_score: 置信度分数
            risk_level: 风险等级
            investment_horizon: 投资周期
            market_outlook: 市场展望
            
        Returns:
            int: 报告ID
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        analysis_payload = analysis_content
        
        if not isinstance(analysis_content, str):
            analysis_content = self._to_json(analysis_content, indent=2)
        
        analysis_date = local_now_str()
        cursor.execute('''
        INSERT INTO sector_analysis_reports 
        (analysis_date, data_date_range, analysis_content, recommended_sectors, 
         summary, confidence_score, risk_level, investment_horizon, market_outlook)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            analysis_date,
            data_date_range,
            analysis_content,
            self._to_json(recommended_sectors),
            summary,
            confidence_score,
            risk_level,
            investment_horizon,
            market_outlook
        ))
        
        report_id = cursor.lastrowid
        try:
            if isinstance(analysis_payload, str):
                analysis_payload = json.loads(analysis_payload)
            normalized_payload = analysis_payload if isinstance(analysis_payload, dict) else {}
            board_date = self._extract_board_date(analysis_date, data_date_range)
            self._ensure_daily_heat_panel(cursor, board_date, normalized_payload, prefer_payload=True)
            cursor.execute(
                '''
                SELECT id, analysis_date, data_date_range, analysis_content
                FROM sector_analysis_reports
                WHERE substr(COALESCE(data_date_range, analysis_date), 1, 10) = ?
                   OR substr(analysis_date, 1, 10) = ?
                ORDER BY datetime(COALESCE(created_at, analysis_date)) ASC, id ASC
                ''',
                (board_date, board_date),
            )
            same_day_reports = cursor.fetchall()
            for same_day_report_id, same_day_analysis_date, same_day_data_date_range, same_day_content in same_day_reports:
                payload = same_day_content
                if isinstance(payload, str):
                    payload = json.loads(payload)
                self._save_heat_history(
                    cursor,
                    int(same_day_report_id),
                    same_day_analysis_date,
                    payload if isinstance(payload, dict) else {},
                    same_day_data_date_range,
                )
        except Exception as exc:
            self.logger.warning(f"[智策板块] 保存板块热度历史失败: {exc}")
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"[智策板块] 分析报告已保存 (ID: {report_id})")
        return report_id
    
    def get_analysis_reports(self, limit=10):
        """
        获取历史分析报告
        
        Args:
            limit: 返回数量
            
        Returns:
            pd.DataFrame: 报告列表
        """
        conn = self.get_connection()
        
        query = '''
        SELECT * FROM sector_analysis_reports
        ORDER BY created_at DESC
        LIMIT ?
        '''
        
        df = pd.read_sql_query(query, conn, params=[limit])
        conn.close()
        
        return df
    
    def get_analysis_report(self, report_id, include_lifecycle=True):
        """
        获取单个分析报告详情
        
        Args:
            report_id: 报告ID
            include_lifecycle: 是否同时加载生命周期明细
            
        Returns:
            dict: 报告详情
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT * FROM sector_analysis_reports WHERE id = ?
        ''', (report_id,))
        
        row = cursor.fetchone()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        conn.close()
        
        if row:
            report = dict(zip(columns, row))
            
            # 解析JSON字段
            try:
                if report.get('analysis_content'):
                    report['analysis_content_parsed'] = json.loads(report['analysis_content'])
                if report.get('recommended_sectors'):
                    report['recommended_sectors_parsed'] = json.loads(report['recommended_sectors'])
                if include_lifecycle:
                    report['lifecycle_items'] = self.get_lifecycle_items_for_analysis(report_id)
                    report['lifecycle_summary'] = self.build_lifecycle_summary(report['lifecycle_items'])
            except json.JSONDecodeError as e:
                self.logger.warning(f"[智策板块] JSON解析失败: {e}")
            
            return report
        
        return None
    
    def delete_analysis_report(self, report_id):
        """
        删除分析报告
        
        Args:
            report_id: 报告ID
            
        Returns:
            bool: 删除是否成功
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 删除相关的追踪记录
            cursor.execute('DELETE FROM sector_tracking WHERE analysis_id = ?', (report_id,))
            
            # 删除报告
            cursor.execute('DELETE FROM sector_analysis_reports WHERE id = ?', (report_id,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                self.logger.info(f"[智策板块] 报告删除成功 (ID: {report_id})")
                return True
            else:
                self.logger.warning(f"[智策板块] 未找到要删除的报告 (ID: {report_id})")
                return False
                
        except Exception as e:
            conn.rollback()
            self.logger.error(f"[智策板块] 删除报告失败: {e}")
            return False
        finally:
            conn.close()

    def get_lifecycle_items_for_analysis(self, analysis_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT *
                FROM sector_heat_history
                WHERE analysis_id = ?
                ORDER BY
                    CASE COALESCE(final_stage, lifecycle_stage)
                        WHEN 'explosive' THEN 0
                        WHEN 'startup' THEN 1
                        WHEN 'decay' THEN 2
                        ELSE 3
                    END,
                    heat_score DESC,
                    heat_rank ASC,
                    id ASC
                ''',
                (int(analysis_id),),
            )
            rows = []
            for row in cursor.fetchall():
                item = self._row_to_dict(cursor, row)
                item['selection_veto'] = bool(item.get('selection_veto', 0))
                if item.get('final_stage'):
                    item['lifecycle_stage'] = item.get('final_stage')
                try:
                    item['trajectory'] = json.loads(item.get('trajectory_json') or '[]')
                except Exception:
                    item['trajectory'] = []
                try:
                    item['lifecycle_details'] = json.loads(item.get('lifecycle_details_json') or '{}')
                except Exception:
                    item['lifecycle_details'] = {}
                rows.append(item)
            return rows
        finally:
            conn.close()

    def build_lifecycle_summary(self, items):
        stage_groups = {
            self.LIFECYCLE_STAGE_STARTUP: [],
            self.LIFECYCLE_STAGE_EXPLOSIVE: [],
            self.LIFECYCLE_STAGE_DECAY: [],
            self.LIFECYCLE_STAGE_NEUTRAL: [],
        }
        for item in items or []:
            stage = str(item.get('lifecycle_stage') or self.LIFECYCLE_STAGE_NEUTRAL)
            stage_groups.setdefault(stage, []).append(item)
        return {
            "counts": {
                "startup": len(stage_groups.get(self.LIFECYCLE_STAGE_STARTUP, [])),
                "explosive": len(stage_groups.get(self.LIFECYCLE_STAGE_EXPLOSIVE, [])),
                "decay": len(stage_groups.get(self.LIFECYCLE_STAGE_DECAY, [])),
                "neutral": len(stage_groups.get(self.LIFECYCLE_STAGE_NEUTRAL, [])),
            },
            "startup": stage_groups.get(self.LIFECYCLE_STAGE_STARTUP, [])[:5],
            "explosive": stage_groups.get(self.LIFECYCLE_STAGE_EXPLOSIVE, [])[:5],
            "decay": stage_groups.get(self.LIFECYCLE_STAGE_DECAY, [])[:5],
        }

    def get_daily_heat_panel(self, board_date=None, limit=30):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            resolved_date = board_date
            if not resolved_date:
                cursor.execute(
                    '''
                    SELECT board_date
                    FROM sector_heat_daily
                    ORDER BY board_date DESC, id DESC
                    LIMIT 1
                    '''
                )
                row = cursor.fetchone()
                resolved_date = row[0] if row else None
            if not resolved_date:
                return {"available": False, "board_date": None, "total_count": 0, "items": []}
            cursor.execute(
                '''
                SELECT COUNT(*)
                FROM sector_heat_daily
                WHERE board_date = ?
                ''',
                (resolved_date,),
            )
            total_count = int((cursor.fetchone() or [0])[0] or 0)
            cursor.execute(
                '''
                SELECT board_date, sector_name, normalized_sector_name, source_type,
                       heat_score, change_pct, turnover, market_cap, fund_flow_pct, rank_order
                FROM sector_heat_daily
                WHERE board_date = ?
                ORDER BY rank_order ASC, id ASC
                LIMIT ?
                ''',
                (resolved_date, max(1, int(limit))),
            )
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            items = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return {
                "available": True,
                "board_date": resolved_date,
                "total_count": total_count,
                "items": items,
            }
        finally:
            conn.close()

    def get_latest_lifecycle_snapshot(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT id, analysis_date
                FROM sector_analysis_reports
                ORDER BY datetime(COALESCE(created_at, analysis_date)) DESC, id DESC
                LIMIT 1
                '''
            )
            row = cursor.fetchone()
            if not row:
                return {"available": False, "items": [], "summary": self.build_lifecycle_summary([])}
            analysis_id = int(row[0])
            analysis_date = row[1]
            items = self.get_lifecycle_items_for_analysis(analysis_id)
            return {
                "available": True,
                "analysis_id": analysis_id,
                "analysis_date": analysis_date,
                "items": items,
                "summary": self.build_lifecycle_summary(items),
                "daily_heat_panel": self.get_daily_heat_panel(board_date=self._extract_board_date(analysis_date), limit=30),
            }
        finally:
            conn.close()

    def list_lifecycle_snapshots(self, days=20):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT id, analysis_date
                FROM sector_analysis_reports
                ORDER BY datetime(COALESCE(created_at, analysis_date)) DESC, id DESC
                LIMIT ?
                ''',
                (max(1, int(days)),),
            )
            snapshots = []
            for report_id, analysis_date in cursor.fetchall():
                items = self.get_lifecycle_items_for_analysis(int(report_id))
                snapshots.append(
                    {
                        "analysis_id": int(report_id),
                        "analysis_date": analysis_date,
                        "summary": self.build_lifecycle_summary(items),
                        "items": items,
                    }
                )
            return snapshots
        finally:
            conn.close()

    def rebuild_heat_history(self, progress_callback=None):
        """按历史报告时间顺序重建板块热度生命周期表。"""
        conn = self.get_connection()
        cursor = conn.cursor()
        report_count = 0
        rebuilt_items = 0
        failed_reports = []
        try:
            self._rebuild_daily_heat_panels(cursor)
            cursor.execute("DELETE FROM sector_heat_history")
            cursor.execute(
                '''
                SELECT id, analysis_date, data_date_range, analysis_content
                FROM sector_analysis_reports
                ORDER BY datetime(COALESCE(created_at, analysis_date)) ASC, id ASC
                '''
            )
            reports = cursor.fetchall()
            report_count = len(reports)
            if callable(progress_callback):
                progress_callback(0, max(1, report_count), f"准备重建 {report_count} 份历史报告...")
            canonical_payload_by_date = {}
            for report_id, analysis_date, data_date_range, analysis_content in reports:
                try:
                    payload = analysis_content
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if not isinstance(payload, dict):
                        payload = {}
                    board_date = self._extract_board_date(analysis_date, data_date_range)
                    canonical_payload_by_date[board_date] = payload
                except Exception:
                    continue

            for board_date, payload in canonical_payload_by_date.items():
                self._ensure_daily_heat_panel(cursor, board_date, payload, prefer_payload=True)

            for index, (report_id, analysis_date, data_date_range, analysis_content) in enumerate(reports, start=1):
                try:
                    payload = analysis_content
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    if not isinstance(payload, dict):
                        payload = {}
                    self._save_heat_history(cursor, int(report_id), analysis_date, payload, data_date_range)
                    cursor.execute(
                        "SELECT COUNT(*) FROM sector_heat_history WHERE analysis_id = ?",
                        (int(report_id),),
                    )
                    rebuilt_items += int(cursor.fetchone()[0] or 0)
                    if callable(progress_callback):
                        progress_callback(index, max(1, report_count), f"正在回放生命周期: {index}/{report_count}")
                except Exception as exc:
                    failed_reports.append(
                        {
                            "report_id": int(report_id),
                            "analysis_date": analysis_date,
                            "error": str(exc),
                        }
                    )
            conn.commit()
            latest_snapshot = self.get_latest_lifecycle_snapshot()
            return {
                "reports_processed": report_count,
                "heat_rows_rebuilt": rebuilt_items,
                "failed_reports": failed_reports,
                "latest_summary": latest_snapshot.get("summary", self.build_lifecycle_summary([])),
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _rebuild_daily_heat_panels(self, cursor, start_date=None, end_date=None, dates=None):
        if dates:
            board_dates = sorted({str(item) for item in dates if str(item).strip()})
        else:
            conditions = ["data_type IN ('industry', 'concept')"]
            params = []
            if start_date:
                conditions.append("data_date >= ?")
                params.append(str(start_date))
            if end_date:
                conditions.append("data_date <= ?")
                params.append(str(end_date))
            cursor.execute(
                f'''
                SELECT DISTINCT data_date
                FROM sector_raw_data
                WHERE {' AND '.join(conditions)}
                ORDER BY data_date ASC
                ''',
                tuple(params),
            )
            board_dates = [row[0] for row in cursor.fetchall()]
        for board_date in board_dates:
            self._ensure_daily_heat_panel(cursor, board_date, None, prefer_payload=False)
        return board_dates

    def rebuild_daily_heat_panels(self, start_date=None, end_date=None, dates=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            board_dates = self._rebuild_daily_heat_panels(cursor, start_date=start_date, end_date=end_date, dates=dates)
            conn.commit()
            return {"rebuilt_dates": board_dates, "rebuilt_count": len(board_dates)}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_data_versions(self, data_type, limit=10):
        """
        获取数据版本历史
        
        Args:
            data_type: 数据类型
            limit: 返回数量
            
        Returns:
            pd.DataFrame: 版本历史
        """
        conn = self.get_connection()
        
        query = '''
        SELECT * FROM data_versions 
        WHERE data_type = ?
        ORDER BY data_date DESC, version DESC
        LIMIT ?
        '''
        
        df = pd.read_sql_query(query, conn, params=[data_type, limit])
        conn.close()
        
        return df
    
    def save_sector_raw_data(self, data_date, data_type, data_df):
        """
        保存板块原始数据
        
        Args:
            data_date: 数据日期
            data_type: 数据类型 ('industry', 'concept', 'fund_flow', 'market_overview', 'north_fund', 'news')
            data_df: 数据DataFrame
        """
        # 兼容不同数据结构的空值判断
        is_empty = False
        if data_df is None:
            is_empty = True
        elif hasattr(data_df, 'empty'):
            is_empty = data_df.empty
        elif isinstance(data_df, (list, tuple, set, dict)):
            is_empty = len(data_df) == 0
        if is_empty:
            self.logger.warning(f"[智策板块] {data_type}数据为空，跳过保存")
            return
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # 获取下一个版本号
            version = self._get_next_version(data_date, data_type)
            
            # 根据数据类型保存数据
            if data_type in ['industry', 'concept']:
                self._save_sector_data_raw(cursor, data_date, data_df, data_type, version)
            elif data_type == 'fund_flow':
                self._save_fund_flow_data(cursor, data_date, data_df, version)
            elif data_type == 'market_overview':
                self._save_market_overview_data(cursor, data_date, data_df, version)
            elif data_type == 'north_fund':
                self._save_north_fund_data(cursor, data_date, data_df, version)
            elif data_type == 'news':
                self._save_news_data_raw(cursor, data_date, data_df, version)
            
            # 记录版本信息
            cursor.execute('''
            INSERT OR REPLACE INTO data_versions 
            (data_date, data_type, version, fetch_success, record_count)
            VALUES (?, ?, ?, 1, ?)
            ''', (data_date, data_type, version, len(data_df)))
            
            conn.commit()
            self.logger.info(f"[智策板块] {data_type}数据保存成功 (日期: {data_date}, 版本: {version}, 记录数: {len(data_df)})")
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"[智策板块] 保存{data_type}数据失败: {e}")
            raise
        finally:
            conn.close()
    
    def _save_sector_data_raw(self, cursor, data_date, data_df, data_type, version):
        """保存板块原始数据"""
        for _, row in data_df.iterrows():
            sector_name = str(row.get('板块名称', row.get('sector_name', '')))
            sector_code = str(row.get('板块代码', row.get('sector_code', sector_name)))
            cursor.execute('''
            INSERT OR REPLACE INTO sector_raw_data 
            (data_date, sector_code, sector_name, price, change_pct, volume, 
             turnover, market_cap, pe_ratio, pb_ratio, data_type, data_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data_date,
                sector_code,
                sector_name,
                float(row.get('最新价', row.get('price', 0))) if pd.notna(row.get('最新价', row.get('price', 0))) else 0,
                float(row.get('涨跌幅', row.get('change_pct', 0))) if pd.notna(row.get('涨跌幅', row.get('change_pct', 0))) else 0,
                float(row.get('成交量', row.get('volume', 0))) if pd.notna(row.get('成交量', row.get('volume', 0))) else 0,
                float(row.get('成交额', row.get('turnover', 0))) if pd.notna(row.get('成交额', row.get('turnover', 0))) else 0,
                float(row.get('总市值', row.get('market_cap', 0))) if pd.notna(row.get('总市值', row.get('market_cap', 0))) else 0,
                float(row.get('市盈率', row.get('pe_ratio', 0))) if pd.notna(row.get('市盈率', row.get('pe_ratio', 0))) else 0,
                float(row.get('市净率', row.get('pb_ratio', 0))) if pd.notna(row.get('市净率', row.get('pb_ratio', 0))) else 0,
                data_type,
                version
            ))
    
    def _save_fund_flow_data(self, cursor, data_date, data_df, version):
        """保存资金流向数据"""
        for _, row in data_df.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO sector_raw_data 
            (data_date, sector_code, sector_name, price, change_pct, volume, 
             turnover, market_cap, pe_ratio, pb_ratio, data_type, data_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'fund_flow', ?)
            ''', (
                data_date,
                str(row.get('行业', '')),
                str(row.get('行业', '')),
                float(row.get('主力净流入-净额', 0)) if pd.notna(row.get('主力净流入-净额', 0)) else 0,
                float(row.get('主力净流入-净占比', 0)) if pd.notna(row.get('主力净流入-净占比', 0)) else 0,
                float(row.get('超大单净流入-净额', 0)) if pd.notna(row.get('超大单净流入-净额', 0)) else 0,
                float(row.get('超大单净流入-净占比', 0)) if pd.notna(row.get('超大单净流入-净占比', 0)) else 0,
                float(row.get('大单净流入-净额', 0)) if pd.notna(row.get('大单净流入-净额', 0)) else 0,
                float(row.get('大单净流入-净占比', 0)) if pd.notna(row.get('大单净流入-净占比', 0)) else 0,
                0,
                version
            ))
    
    def _save_market_overview_data(self, cursor, data_date, data_df, version):
        """保存市场概况数据"""
        for _, row in data_df.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO sector_raw_data 
            (data_date, sector_code, sector_name, price, change_pct, volume, 
             turnover, market_cap, pe_ratio, pb_ratio, data_type, data_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'market_overview', ?)
            ''', (
                data_date,
                str(row.get('代码', row.get('名称', ''))),
                str(row.get('名称', '')),
                float(row.get('最新价', 0)) if pd.notna(row.get('最新价', 0)) else 0,
                float(row.get('涨跌幅', 0)) if pd.notna(row.get('涨跌幅', 0)) else 0,
                float(row.get('成交量', 0)) if pd.notna(row.get('成交量', 0)) else 0,
                float(row.get('成交额', 0)) if pd.notna(row.get('成交额', 0)) else 0,
                float(row.get('总市值', 0)) if pd.notna(row.get('总市值', 0)) else 0,
                float(row.get('市盈率', 0)) if pd.notna(row.get('市盈率', 0)) else 0,
                float(row.get('市净率', 0)) if pd.notna(row.get('市净率', 0)) else 0,
                version
            ))
    
    def _save_north_fund_data(self, cursor, data_date, data_df, version):
        """保存北向资金数据"""
        for _, row in data_df.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO sector_raw_data 
            (data_date, sector_code, sector_name, price, change_pct, volume, 
             turnover, market_cap, pe_ratio, pb_ratio, data_type, data_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'north_fund', ?)
            ''', (
                data_date,
                str(row.get('代码', '')),
                str(row.get('名称', '')),
                float(row.get('收盘价', 0)) if pd.notna(row.get('收盘价', 0)) else 0,
                float(row.get('涨跌幅', 0)) if pd.notna(row.get('涨跌幅', 0)) else 0,
                float(row.get('持股数量', 0)) if pd.notna(row.get('持股数量', 0)) else 0,
                float(row.get('持股市值', 0)) if pd.notna(row.get('持股市值', 0)) else 0,
                float(row.get('持股变化', 0)) if pd.notna(row.get('持股变化', 0)) else 0,
                0, 0,
                version
            ))
    
    def _save_news_data_raw(self, cursor, data_date, data_df, version):
        """保存新闻数据"""
        for _, row in data_df.iterrows():
            cursor.execute('''
            INSERT OR REPLACE INTO sector_news_data 
            (news_date, title, content, source, url, related_sectors, 
             sentiment_score, importance_score, data_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data_date,
                str(row.get('新闻标题', row.get('title', ''))),
                str(row.get('新闻内容', row.get('content', ''))),
                str(row.get('新闻来源', row.get('source', ''))),
                str(row.get('新闻链接', row.get('url', ''))),
                json.dumps([], ensure_ascii=False),  # 暂时为空
                0,  # 暂时为0
                0,  # 暂时为0
                version
            ))

    def cleanup_old_data(self, data_type, keep_days=30):
        """
        清理旧数据，保留指定天数的数据
        
        Args:
            data_type: 数据类型
            keep_days: 保留天数
            
        Returns:
            int: 删除的记录数
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cutoff_date = (datetime.now() - pd.Timedelta(days=keep_days)).strftime('%Y-%m-%d')
            
            if data_type == 'sector_data':
                cursor.execute('''
                DELETE FROM sector_raw_data 
                WHERE data_date < ?
                ''', (cutoff_date,))
            elif data_type == 'news_data':
                cursor.execute('''
                DELETE FROM sector_news_data 
                WHERE news_date < ?
                ''', (cutoff_date,))
            
            deleted_count = cursor.rowcount
            
            # 同时清理版本记录
            cursor.execute('''
            DELETE FROM data_versions 
            WHERE data_type = ? AND data_date < ?
            ''', (data_type, cutoff_date))
            
            conn.commit()
            self.logger.info(f"[智策板块] 清理{data_type}旧数据完成，删除{deleted_count}条记录")
            return deleted_count
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"[智策板块] 清理{data_type}旧数据失败: {e}")
            return 0
        finally:
            conn.close()

    # =====================
    # 缓存与最近数据读取接口
    # =====================
    def save_news_data(self, news_list, news_date, source="rsshub_tushare"):
        """
        保存新闻列表（字典列表）到数据库，用于非DataFrame场景
        Args:
            news_list: [{title, content, url, related_sectors, sentiment_score, importance_score}]
            news_date: 新闻日期字符串
            source: 新闻来源
        """
        if not news_list:
            self.logger.warning("[智策板块] 新闻列表为空，跳过保存")
            return 0

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # 版本号按日期累加
            version = self._get_next_version(news_date, 'news')
            inserted = 0
            for item in news_list:
                cursor.execute('''
                INSERT OR REPLACE INTO sector_news_data 
                (news_date, title, content, source, url, related_sectors, 
                 sentiment_score, importance_score, data_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(news_date),
                    str(item.get('title', '')),
                    str(item.get('content', '')),
                    str(item.get('source', source)),
                    str(item.get('url', '')),
                    json.dumps(item.get('related_sectors', []), ensure_ascii=False),
                    float(item.get('sentiment_score', 0) or 0),
                    float(item.get('importance_score', 0) or 0),
                    version
                ))
                inserted += 1

            # 记录版本信息
            cursor.execute('''
            INSERT OR REPLACE INTO data_versions 
            (data_date, data_type, version, fetch_success, record_count)
            VALUES (?, ?, ?, 1, ?)
            ''', (str(news_date), 'news', version, inserted))

            conn.commit()
            self.logger.info(f"[智策板块] 保存新闻数据成功 (日期: {news_date}, 版本: {version}, 记录数: {inserted})")
            return inserted
        except Exception as e:
            conn.rollback()
            self.logger.error(f"[智策板块] 保存新闻数据失败: {e}")
            return 0
        finally:
            conn.close()

    def _get_next_version(self, data_date: str, data_type: str) -> int:
        """获取指定日期与类型的下一个版本号"""
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
            SELECT COALESCE(MAX(version), 0) + 1 FROM data_versions 
            WHERE data_type = ? AND data_date = ?
            ''', (data_type, data_date))
            next_version = cursor.fetchone()[0] or 1
            return int(next_version)
        finally:
            conn.close()

    def _build_raw_data_payload(self, key: str, data_date: str, raw_df: pd.DataFrame):
        if raw_df.empty:
            return None

        if key in ['sectors', 'concepts']:
            result = {}
            for _, row in raw_df.iterrows():
                name = str(row.get('sector_name', ''))
                result[name] = {
                    'name': name,
                    'change_pct': float(row.get('change_pct', 0) or 0),
                    'price': float(row.get('price', 0) or 0),
                    'volume': float(row.get('volume', 0) or 0),
                    'turnover': float(row.get('turnover', 0) or 0),
                    'market_cap': float(row.get('market_cap', 0) or 0),
                    'pe_ratio': float(row.get('pe_ratio', 0) or 0),
                    'pb_ratio': float(row.get('pb_ratio', 0) or 0),
                }
            return {
                'data_date': data_date,
                'data_content': result
            }

        if key == 'fund_flow':
            today = []
            for _, row in raw_df.iterrows():
                name = str(row.get('sector_name', ''))
                today.append({
                    'sector': name,
                    'main_net_inflow': float(row.get('price', 0) or 0),
                    'main_net_inflow_pct': float(row.get('change_pct', 0) or 0),
                    'super_large_net_inflow': float(row.get('volume', 0) or 0),
                    'super_large_net_inflow_pct': float(row.get('turnover', 0) or 0),
                    'large_net_inflow': float(row.get('market_cap', 0) or 0),
                    'large_net_inflow_pct': float(row.get('pe_ratio', 0) or 0),
                    'medium_net_inflow': 0,
                    'small_net_inflow': 0
                })
            return {
                'data_date': data_date,
                'data_content': {
                    'today': today
                }
            }

        if key == 'market_overview':
            overview = {}
            for _, row in raw_df.iterrows():
                name = str(row.get('sector_name', ''))
                if name == '__MARKET_BREADTH__':
                    overview['total_stocks'] = int(row.get('price', 0) or 0)
                    overview['up_ratio'] = float(row.get('change_pct', 0) or 0)
                    overview['up_count'] = int(row.get('volume', 0) or 0)
                    overview['down_count'] = int(row.get('turnover', 0) or 0)
                    overview['flat_count'] = int(row.get('market_cap', 0) or 0)
                    overview['limit_up'] = int(row.get('pe_ratio', 0) or 0)
                    overview['limit_down'] = int(row.get('pb_ratio', 0) or 0)
                    continue

                entry = {
                    'price': float(row.get('price', 0) or 0),
                    'close': float(row.get('price', 0) or 0),
                    'change_pct': float(row.get('change_pct', 0) or 0),
                    'turnover': float(row.get('turnover', 0) or 0),
                    'volume': float(row.get('volume', 0) or 0)
                }
                if '上证' in name or '沪指' in name or 'SH' in name:
                    overview['sh_index'] = entry
                elif '深证' in name or 'SZ' in name:
                    overview['sz_index'] = entry
                elif '创业' in name or 'CYB' in name:
                    overview['cyb_index'] = entry
            return {
                'data_date': data_date,
                'data_content': overview
            }

        if key == 'north_flow':
            total_value = float(raw_df['turnover'].sum()) if not raw_df.empty else 0
            return {
                'data_date': data_date,
                'data_content': {
                    'north_total_amount': total_value,
                    'history': []
                }
            }

        return None

    def _get_raw_data_snapshot(self, key: str, *, data_date: str | None = None, within_hours: int | None = 24):
        """
        获取原始数据并组装为分析所需结构
        Args:
            key: 'sectors' | 'concepts' | 'fund_flow' | 'market_overview' | 'north_flow'
            data_date: 指定日期（YYYY-MM-DD）
            within_hours: 有效缓存时长（小时）
        Returns:
            dict 或 None
        """
        key_map = {
            'sectors': 'industry',
            'concepts': 'concept',
            'fund_flow': 'fund_flow',
            'market_overview': 'market_overview',
            'north_flow': 'north_fund'
        }
        data_type = key_map.get(key)
        if not data_type:
            return None

        conn = self.get_connection()
        try:
            if data_date:
                version_df = pd.read_sql_query('''
                    SELECT data_date, version FROM data_versions
                    WHERE data_type = ? AND fetch_success = 1 AND data_date = ?
                    ORDER BY version DESC LIMIT 1
                ''', conn, params=[data_type, data_date])
            else:
                cutoff = (pd.Timestamp.now() - pd.Timedelta(hours=within_hours or 24)).strftime('%Y-%m-%d %H:%M:%S')
                version_df = pd.read_sql_query('''
                    SELECT data_date, version FROM data_versions
                    WHERE data_type = ? AND fetch_success = 1 
                    AND datetime(created_at) >= datetime(?)
                    ORDER BY data_date DESC, version DESC LIMIT 1
                ''', conn, params=[data_type, cutoff])

            if version_df.empty:
                return None

            resolved_date = version_df.iloc[0]['data_date']
            version = int(version_df.iloc[0]['version'])

            raw_df = pd.read_sql_query('''
                SELECT * FROM sector_raw_data 
                WHERE data_type = ? AND data_date = ? AND data_version = ?
            ''', conn, params=[data_type, resolved_date, version])

            return self._build_raw_data_payload(key, resolved_date, raw_df)
        except Exception as e:
            self.logger.error(f"[智策板块] 获取原始数据失败: {e}")
            return None
        finally:
            conn.close()

    def get_latest_raw_data(self, key: str, within_hours: int = 24):
        return self._get_raw_data_snapshot(key, within_hours=within_hours)

    def get_raw_data_by_date(self, key: str, data_date: str):
        return self._get_raw_data_snapshot(key, data_date=data_date)

    def build_data_summary(self, *, data_date: str | None = None, within_hours: int = 24):
        loader = (
            (lambda key: self.get_raw_data_by_date(key, data_date))
            if data_date
            else (lambda key: self.get_latest_raw_data(key, within_hours=within_hours))
        )
        sectors_data = loader('sectors')
        concepts_data = loader('concepts')
        market_data = loader('market_overview')

        summary = {
            'from_cache': True,
            'cache_warning': f"市场快照由{'历史' if data_date else '缓存'}原始数据重建，缺失字段以可恢复内容为准。",
            'data_timestamp': data_date or '',
            'market_overview': (market_data or {}).get('data_content', {}) or {},
            'sectors': (sectors_data or {}).get('data_content', {}) or {},
            'concepts': (concepts_data or {}).get('data_content', {}) or {},
        }
        if not summary['market_overview'] and not summary['sectors'] and not summary['concepts']:
            return {}
        return summary

    def get_latest_news_data(self, within_hours: int = 24):
        """获取最近within_hours小时的新闻列表"""
        conn = self.get_connection()
        try:
            cutoff = (pd.Timestamp.now() - pd.Timedelta(hours=within_hours)).strftime('%Y-%m-%d %H:%M:%S')
            df = pd.read_sql_query('''
                SELECT * FROM sector_news_data 
                WHERE datetime(created_at) >= datetime(?)
                ORDER BY importance_score DESC, created_at DESC
            ''', conn, params=[cutoff])
            if df.empty:
                return None
            news = []
            for _, row in df.iterrows():
                try:
                    related = json.loads(row.get('related_sectors', '[]'))
                except Exception:
                    related = []
                news.append({
                    'title': row.get('title', ''),
                    'content': row.get('content', ''),
                    'source': row.get('source', ''),
                    'url': row.get('url', ''),
                    'related_sectors': related,
                    'sentiment_score': float(row.get('sentiment_score', 0) or 0),
                    'importance_score': float(row.get('importance_score', 0) or 0),
                    'news_date': row.get('news_date', '')
                })
            return {
                'data_date': df.iloc[0]['news_date'] if not df.empty else None,
                'data_content': news
            }
        except Exception as e:
            self.logger.error(f"[智策板块] 获取最近新闻数据失败: {e}")
            return None
        finally:
            conn.close()
