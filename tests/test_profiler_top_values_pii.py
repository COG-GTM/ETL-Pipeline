"""Regression tests: the profiler must not leak raw PII values via top_values."""
import csv
import os

import pandas as pd

from src.profiler import SourceProfiler

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CUSTOMERS_CSV = os.path.join(REPO_ROOT, "demo", "sample_data", "csv", "customers.csv")
PII_COLUMNS = ["first_name", "last_name", "email", "phone", "address"]


def _customer_pii_values():
    """Return every non-empty PII value from the bundled customers.csv."""
    with open(CUSTOMERS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    return {row[col] for row in rows for col in PII_COLUMNS if row[col]}


def _assert_no_pii(text):
    """Fail if any raw customer PII value appears in text."""
    leaked = sorted(v for v in _customer_pii_values() if v in text)
    assert leaked == [], f"raw customer values leaked: {leaked}"


def test_default_profile_and_report_contain_no_customer_pii():
    """Default profile and report must not contain customer emails, phones or names."""
    profiler = SourceProfiler()
    profile = profiler.profile_auto(CUSTOMERS_CSV)
    for col in ("email", "phone"):
        assert "top_values" not in profile["columns"][col]
    _assert_no_pii(repr(profile))
    _assert_no_pii(profiler.generate_report(profile))


def test_default_profile_stores_no_top_values_at_all():
    """top_values is opt-in, so no column carries it by default."""
    profile = SourceProfiler().profile_auto(CUSTOMERS_CSV)
    assert not any("top_values" in c for c in profile["columns"].values())


def test_opt_in_never_stores_sensitive_columns():
    """Even with include_top_values, email/phone/date columns are never stored."""
    profiler = SourceProfiler(include_top_values=True)
    profile = profiler.profile_auto(CUSTOMERS_CSV)
    for col in ("email", "phone", "registration_date"):
        assert "top_values" not in profile["columns"][col]
    _assert_no_pii(repr(profile))
    _assert_no_pii(profiler.generate_report(profile, reveal_values=True))
    assert profile["columns"]["loyalty_tier"]["top_values"] == {
        "Gold": 6, "Silver": 4, "Bronze": 3, "Platinum": 2,
    }


def test_opt_in_skips_columns_with_personal_data_names():
    """Columns named like personal data are skipped regardless of their content."""
    df = pd.DataFrame({
        "contact_name": ["Ann Lee", "Bo Kim", "Ann Lee"] * 4,
        "home_address": ["1 A St", "2 B St", "1 A St"] * 4,
        "zip": ["94102", "60601", "94102"] * 4,
        "tier": ["Gold", "Silver", "Gold"] * 4,
    })
    profile = SourceProfiler(include_top_values=True)._profile_dataframe(df, "inline", "csv")
    for col in ("contact_name", "home_address", "zip"):
        assert "top_values" not in profile["columns"][col]
    assert profile["columns"]["tier"]["top_values"] == {"Gold": 8, "Silver": 4}


def test_report_masks_opted_in_values_by_default():
    """Opted-in values are masked in the report unless reveal_values=True."""
    profiler = SourceProfiler(include_top_values=True)
    profile = profiler.profile_auto(CUSTOMERS_CSV)
    report = profiler.generate_report(profile)
    assert "Gold" not in report
    assert "Top Values: 4 distinct values (masked)" in report
    assert "'Gold': 6" in profiler.generate_report(profile, reveal_values=True)


def test_masked_report_shows_full_distinct_count_beyond_ten():
    """Masked count reflects all distinct values, not just the ten stored ones."""
    df = pd.DataFrame({"tier": [f"level_{chr(97 + i)}" for i in range(15)] * 2})
    profiler = SourceProfiler(include_top_values=True)
    profile = profiler._profile_dataframe(df, "inline", "csv")
    assert len(profile["columns"]["tier"]["top_values"]) == 10
    assert "Top Values: 15 distinct values (masked)" in profiler.generate_report(profile)
