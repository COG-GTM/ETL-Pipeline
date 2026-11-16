import csv
import os
import unittest

import pandas as pd

from src.profiler import SourceProfiler

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CUSTOMERS_CSV = os.path.join(REPO_ROOT, "demo", "sample_data", "csv", "customers.csv")
PII_COLUMNS = ["first_name", "last_name", "email", "phone", "address"]


def _customer_pii_values():
    with open(CUSTOMERS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    return {row[col] for row in rows for col in PII_COLUMNS if row[col]}


class ProfilerTopValuesPiiTest(unittest.TestCase):
    def setUp(self):
        self.pii_values = _customer_pii_values()

    def _assert_no_pii(self, text):
        leaked = sorted(v for v in self.pii_values if v in text)
        self.assertEqual(leaked, [], f"raw customer values leaked: {leaked}")

    def test_default_profile_and_report_contain_no_customer_pii(self):
        profiler = SourceProfiler()
        profile = profiler.profile_auto(CUSTOMERS_CSV)
        for col in ("email", "phone"):
            self.assertNotIn("top_values", profile["columns"][col])
        self._assert_no_pii(repr(profile))
        self._assert_no_pii(profiler.generate_report(profile))

    def test_default_profile_stores_no_top_values_at_all(self):
        profile = SourceProfiler().profile_auto(CUSTOMERS_CSV)
        self.assertFalse(any("top_values" in c for c in profile["columns"].values()))

    def test_opt_in_never_stores_sensitive_columns(self):
        profiler = SourceProfiler(include_top_values=True)
        profile = profiler.profile_auto(CUSTOMERS_CSV)
        for col in ("email", "phone", "registration_date"):
            self.assertNotIn("top_values", profile["columns"][col])
        self._assert_no_pii(repr(profile))
        self._assert_no_pii(profiler.generate_report(profile, reveal_values=True))
        self.assertEqual(
            profile["columns"]["loyalty_tier"]["top_values"],
            {"Gold": 6, "Silver": 4, "Bronze": 3, "Platinum": 2},
        )

    def test_opt_in_skips_columns_with_personal_data_names(self):
        df = pd.DataFrame({
            "contact_name": ["Ann Lee", "Bo Kim", "Ann Lee"] * 4,
            "home_address": ["1 A St", "2 B St", "1 A St"] * 4,
            "zip": ["94102", "60601", "94102"] * 4,
            "tier": ["Gold", "Silver", "Gold"] * 4,
        })
        profile = SourceProfiler(include_top_values=True)._profile_dataframe(df, "inline", "csv")
        for col in ("contact_name", "home_address", "zip"):
            self.assertNotIn("top_values", profile["columns"][col])
        self.assertEqual(profile["columns"]["tier"]["top_values"], {"Gold": 8, "Silver": 4})

    def test_report_masks_opted_in_values_by_default(self):
        profiler = SourceProfiler(include_top_values=True)
        profile = profiler.profile_auto(CUSTOMERS_CSV)
        report = profiler.generate_report(profile)
        self.assertNotIn("Gold", report)
        self.assertIn("Top Values: 4 distinct values (masked)", report)
        self.assertIn("'Gold': 6", profiler.generate_report(profile, reveal_values=True))


if __name__ == "__main__":
    unittest.main()
