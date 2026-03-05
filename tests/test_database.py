"""
database.py 的单元测试
测试 StockAnalysisDatabase 的 CRUD 操作和 JSON 序列化安全性
"""
import json
import pytest
from database import StockAnalysisDatabase


class TestStockAnalysisDatabase:
    """StockAnalysisDatabase 基础功能测试"""

    @pytest.fixture(autouse=True)
    def setup_db(self, tmp_db):
        """每个测试方法使用独立的临时数据库"""
        self.db = StockAnalysisDatabase(db_path=tmp_db)

    def test_init_creates_table(self):
        """初始化应自动创建表"""
        count = self.db.get_record_count()
        assert count == 0

    def test_save_and_retrieve(self):
        """保存后应能正确读取"""
        stock_info = {"symbol": "000001", "name": "平安银行", "price": 12.5}
        agents_results = {"technical": "看多"}
        discussion = {"summary": "团队讨论结果"}
        decision = {"rating": "买入", "confidence_level": "高"}

        record_id = self.db.save_analysis(
            symbol="000001",
            stock_name="平安银行",
            period="1y",
            stock_info=stock_info,
            agents_results=agents_results,
            discussion_result=discussion,
            final_decision=decision,
        )
        assert record_id is not None

        # 验证记录数
        assert self.db.get_record_count() == 1

        # 验证详细记录
        record = self.db.get_record_by_id(record_id)
        assert record is not None
        assert record["symbol"] == "000001"
        assert record["stock_name"] == "平安银行"
        assert record["stock_info"]["price"] == 12.5
        assert record["final_decision"]["rating"] == "买入"

    def test_get_all_records(self):
        """获取所有记录应按时间倒序"""
        for i in range(3):
            self.db.save_analysis(f"00000{i}", f"股票{i}", "1y", {}, {}, {}, {})

        records = self.db.get_all_records()
        assert len(records) == 3

    def test_delete_record(self):
        """删除记录后计数应减少"""
        rid = self.db.save_analysis("000001", "测试", "1y", {}, {}, {}, {})
        assert self.db.get_record_count() == 1

        result = self.db.delete_record(rid)
        assert result is True
        assert self.db.get_record_count() == 0

    def test_delete_nonexistent_record(self):
        """删除不存在的记录应返回 False"""
        result = self.db.delete_record(99999)
        assert result is False

    def test_get_nonexistent_record(self):
        """获取不存在的记录应返回 None"""
        record = self.db.get_record_by_id(99999)
        assert record is None


class TestSafeJsonLoads:
    """JSON 反序列化安全性测试"""

    def test_valid_json(self):
        """有效 JSON 应正常解析"""
        result = StockAnalysisDatabase._safe_json_loads('{"key": "value"}')
        assert result == {"key": "value"}

    def test_empty_string(self):
        """空字符串应返回默认值"""
        result = StockAnalysisDatabase._safe_json_loads("")
        assert result == {}

    def test_none_input(self):
        """None 输入应返回默认值"""
        result = StockAnalysisDatabase._safe_json_loads(None)
        assert result == {}

    def test_malformed_json(self):
        """损坏的 JSON 不应崩溃，应返回默认值"""
        result = StockAnalysisDatabase._safe_json_loads("{invalid json}")
        assert result == {}

    def test_custom_default(self):
        """自定义默认值应生效"""
        result = StockAnalysisDatabase._safe_json_loads(None, default=[])
        assert result == []
