from datetime import datetime

from analysis_repository import AnalysisRepository


class StockAnalysisDatabase:
    def __init__(self, db_path: str = "investment.db"):
        self.repository = AnalysisRepository(db_path)

    @staticmethod
    def _format_analysis_date(value):
        text = str(value or "").strip()
        if not text:
            return ""
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, fmt)
                return parsed.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        return text

    def _build_record_dedupe_key(self, record):
        return (
            record.get("symbol") or "",
            record.get("stock_name") or "",
            record.get("period") or "",
            record.get("analysis_date") or "",
            record.get("rating") or "",
            str(record.get("summary") or "").strip(),
            self.repository._sort_json_text(record.get("final_decision") or {}),
        )

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
        seen_keys = set()
        for record in records:
            dedupe_key = self._build_record_dedupe_key(record)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            result.append(
                {
                    "id": record["id"],
                    "symbol": record["symbol"],
                    "stock_name": record.get("stock_name"),
                    "analysis_date": self._format_analysis_date(record.get("analysis_date")),
                    "period": record.get("period"),
                    "rating": record.get("rating") or "未知",
                    "created_at": record.get("created_at"),
                }
            )
        return result

    def get_record_count(self):
        return len(self.get_all_records())

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
