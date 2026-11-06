"""Tests for the timeliness check in DataQualityEngine."""

import pandas as pd

from demo.playbooks.quality_gate import QualityGatePlaybook
from src.quality import DataQualityEngine


def _rules(report, prefix):
    """Return the rule dicts whose name starts with ``prefix``."""
    return [r for r in report["rules"] if r["rule"].startswith(prefix)]


def test_timezone_aware_future_dates_are_flagged():
    """tz-aware dates are compared to now instead of raising and passing."""
    df = pd.DataFrame(
        {"created_at": ["2020-01-01T00:00:00+00:00", "2099-01-01T00:00:00+00:00"]}
    )
    report = DataQualityEngine().run_all_checks(df, "tz_dates")
    (rule,) = _rules(report, "no_future_dates_created_at")
    assert rule["passed"] is False
    assert rule["details"]["future_date_count"] == 1


def test_naive_future_dates_are_flagged():
    """Naive future dates still produce a failed warning rule."""
    df = pd.DataFrame({"sale_date": ["2020-01-01", "2099-01-01"]})
    report = DataQualityEngine().run_all_checks(df, "naive_dates")
    (rule,) = _rules(report, "no_future_dates_sale_date")
    assert rule["passed"] is False


def test_unevaluable_date_column_records_failed_rule():
    """A date column that cannot be compared records a failure, not a pass."""
    df = pd.DataFrame(
        {
            "updated_at": [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T00:00:00+05:00",
                "2020-01-01",
            ]
        }
    )
    report = DataQualityEngine().run_all_checks(df, "mixed_dates")
    rules = _rules(report, "timeliness_updated_at") + _rules(
        report, "no_future_dates_updated_at"
    )
    assert _rules(report, "timeliness_updated_at"), report["rules"]
    assert all(r["passed"] is False for r in rules)


def test_past_dates_add_no_failed_timeliness_rule():
    """Valid past dates do not create a failed timeliness rule."""
    df = pd.DataFrame({"created_at": ["2020-01-01", "2021-06-30"]})
    report = DataQualityEngine().run_all_checks(df, "past_dates")
    assert not _rules(report, "timeliness_")
    assert not _rules(report, "no_future_dates_")


def test_duplicate_transaction_id_blocks_gate():
    """Repeated transaction_id values are a critical failure that blocks."""
    df = pd.DataFrame(
        {
            "transaction_id": ["T1", "T1", "T1", "T2"],
            "amount": [10.0, 20.0, 30.0, 40.0],
        }
    )
    report = DataQualityEngine().run_all_checks(df, "transactions")
    assert report["critical_failures"] >= 1
    result = QualityGatePlaybook(min_score=0, block_on_critical=True).execute(
        df, "transactions"
    )
    assert result["gate_passed"] is False
    assert result["decision"] == "REJECTED"
