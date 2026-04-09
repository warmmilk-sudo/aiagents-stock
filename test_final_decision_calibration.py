import unittest

from final_decision_calibration import calibrate_final_decision


class FinalDecisionCalibrationTests(unittest.TestCase):
    def test_calibrate_final_decision_preserves_buy_with_good_reward_risk(self):
        result = calibrate_final_decision(
            {
                "rating": "buy",
                "confidence_level": "9",
                "target_price": "120",
                "take_profit": "120",
                "stop_loss": "92",
                "operation_advice": "回踩后可分批买入。",
                "position_size": "中等仓位",
            },
            stock_info={"current_price": 100},
        )

        self.assertEqual(result["rating"], "买入")
        self.assertGreaterEqual(result["confidence_level"], 8.0)
        self.assertEqual(result["raw_model_rating"], "buy")
        self.assertEqual(result["raw_model_confidence_level"], "9")
        self.assertEqual(result["calibration_version"], "rule_v1")

    def test_calibrate_final_decision_downgrades_buy_when_risk_reward_is_weak(self):
        result = calibrate_final_decision(
            {
                "rating": "买入",
                "confidence_level": "9",
                "target_price": "102",
                "take_profit": "102",
                "stop_loss": "88",
                "risk_warning": "跌破关键支撑需立即止损，整体高风险，建议规避追高。",
                "operation_advice": "建议等待观察。",
                "position_size": "轻仓",
            },
            stock_info={"current_price": 100},
        )

        self.assertEqual(result["rating"], "持有")
        self.assertLessEqual(result["confidence_level"], 6.5)
        self.assertTrue(any("校准" in note for note in result["calibration_notes"]))


if __name__ == "__main__":
    unittest.main()
