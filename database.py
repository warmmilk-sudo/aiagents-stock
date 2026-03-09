from analysis_repository import AnalysisRepository


class StockAnalysisDatabase:
    def __init__(self, db_path: str = "investment.db"):
        self.repository = AnalysisRepository(db_path)

    def save_analysis(self, symbol, stock_name, period, stock_info, agents_results, discussion_result, final_decision):
        return self.repository.save_record(
            symbol=symbol,
            stock_name=stock_name,
            period=period,
            stock_info=stock_info,
            agents_results=agents_results,
            discussion_result=discussion_result,
            final_decision=final_decision,
            analysis_scope="research",
            analysis_source="home_single_analysis",
            has_full_report=True,
        )

    def get_all_records(self):
        records = self.repository.list_records(analysis_scope="research")
        result = []
        for record in records:
            result.append(
                {
                    "id": record["id"],
                    "symbol": record["symbol"],
                    "stock_name": record.get("stock_name"),
                    "analysis_date": record.get("analysis_date"),
                    "period": record.get("period"),
                    "rating": record.get("rating") or "未知",
                    "created_at": record.get("created_at"),
                }
            )
        return result

    def get_record_count(self):
        return len(self.repository.list_records(analysis_scope="research"))

    def get_record_by_id(self, record_id):
        record = self.repository.get_record(record_id)
        if not record or record.get("analysis_scope") != "research":
            return None
        return record

    def delete_record(self, record_id):
        record = self.repository.get_record(record_id)
        if not record or record.get("analysis_scope") != "research":
            return False
        return self.repository.delete_record(record_id)


db = StockAnalysisDatabase()
