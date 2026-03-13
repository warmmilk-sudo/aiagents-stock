import os
import unittest

from time_utils import format_display_timestamp


class TimeUtilsTests(unittest.TestCase):
    def setUp(self):
        self.original_tz = os.environ.get("TZ")
        os.environ["TZ"] = "Asia/Shanghai"

    def tearDown(self):
        if self.original_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self.original_tz

    def test_format_display_timestamp_converts_utc_created_at_to_shanghai(self):
        self.assertEqual(
            format_display_timestamp("2026-03-12 01:02:03", assume_utc=True),
            "2026-03-12 09:02:03",
        )

    def test_format_display_timestamp_preserves_local_naive_time(self):
        self.assertEqual(
            format_display_timestamp("2026-03-12 09:02:03"),
            "2026-03-12 09:02:03",
        )


if __name__ == "__main__":
    unittest.main()
