import ast
import unittest
from pathlib import Path


class ModelTierAssignmentTests(unittest.TestCase):
    def _assert_function_uses_reasoning_tier(self, file_path: str, class_name: str, function_name: str) -> None:
        tree = ast.parse(Path(file_path).read_text(encoding="utf-8"))

        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or node.name != class_name:
                continue

            for item in node.body:
                if not isinstance(item, ast.FunctionDef) or item.name != function_name:
                    continue

                for call in ast.walk(item):
                    if not isinstance(call, ast.Call):
                        continue
                    if not isinstance(call.func, ast.Attribute) or call.func.attr != "call_api":
                        continue

                    for keyword in call.keywords:
                        if keyword.arg == "tier":
                            self.assertEqual(ast.unparse(keyword.value), "ModelTier.REASONING")
                            return

                    self.fail(f"{file_path}:{class_name}.{function_name} missing tier keyword")

        self.fail(f"{file_path}:{class_name}.{function_name} not found")

    def test_news_flow_risk_assess_uses_reasoning(self):
        self._assert_function_uses_reasoning_tier(
            "news_flow_agents.py",
            "NewsFlowAgents",
            "risk_assess_agent",
        )

    def test_news_flow_investment_advisor_uses_reasoning(self):
        self._assert_function_uses_reasoning_tier(
            "news_flow_agents.py",
            "NewsFlowAgents",
            "investment_advisor_agent",
        )

    def test_sector_strategy_final_predictions_uses_reasoning(self):
        self._assert_function_uses_reasoning_tier(
            "sector_strategy_engine.py",
            "SectorStrategyEngine",
            "_generate_final_predictions",
        )

    def test_main_force_final_selection_uses_reasoning(self):
        self._assert_function_uses_reasoning_tier(
            "main_force_analysis.py",
            "MainForceAnalyzer",
            "_select_best_stocks",
        )


if __name__ == "__main__":
    unittest.main()
