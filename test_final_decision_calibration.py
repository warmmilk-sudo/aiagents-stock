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

    def test_calibrate_final_decision_maps_legacy_position_labels_to_new_labels(self):
        result = calibrate_final_decision(
            {
                "rating": "增持",
                "confidence_level": "9",
                "target_price": "120",
                "take_profit": "120",
                "stop_loss": "92",
                "operation_advice": "回踩后可以继续加仓。",
                "position_size": "中等仓位",
            },
            stock_info={"current_price": 100, "has_position": True},
            has_position=True,
        )

        self.assertEqual(result["rating"], "加仓")
        self.assertEqual(result["raw_model_rating"], "增持")
        self.assertTrue(any("原始信心度" in note for note in result["calibration_notes"]))


if __name__ == "__main__":
    unittest.main()
