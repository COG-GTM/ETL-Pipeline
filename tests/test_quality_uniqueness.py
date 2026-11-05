"""Tests for primary key uniqueness checks in DataQualityEngine."""

import pandas as pd

from demo.playbooks.quality_gate import QualityGatePlaybook
from src.quality import DataQualityEngine


def _rule(report, name):
    """Return the rule dict named ``name`` from a quality report."""
    return next(r for r in report["rules"] if r["rule"] == name)


def _customers(ids):
    """Build a small customer frame keyed by ``customer_id``."""
    return pd.DataFrame(
        {
            "customer_id": ids,
            "first_name": [f"name{i}" for i in range(len(ids))],
        }
    )


def test_duplicate_id_column_is_critical_failure():
    """A repeated value in ``id`` must fail as a critical rule."""
    df = pd.DataFrame({"id": [1, 2, 2, 3], "value": ["a", "b", "c", "d"]})
    report = DataQualityEngine().run_all_checks(df, "dup_id")
    rule = _rule(report, "uniqueness_id")
    assert rule["severity"] == "critical"
    assert rule["passed"] is False
    assert rule["details"]["duplicate_values"] == 1
    assert report["critical_failures"] == 1


def test_leading_key_column_with_duplicates_blocks_gate():
    """Duplicated leading ``customer_id`` is rejected by the gate."""
    df = _customers(["C1", "C1", "C2", "C3", "C3", "C3"])
    gate = QualityGatePlaybook(min_score=0, block_on_critical=True)
    result = gate.execute(df, "customers")
    assert result["critical_failures"] == 1
    assert result["gate_passed"] is False
    assert result["decision"] == "REJECTED"


def test_duplicate_primary_key_recommendation_is_emitted():
    """The duplicate ID recommendation is reachable."""
    df = _customers(["C1", "C1", "C2"])
    report = DataQualityEngine().run_all_checks(df, "customers")
    assert any(
        "Fix duplicate IDs in 'customer_id'" in rec
        for rec in report["recommendations"]
    )


def test_unique_primary_key_passes_gate():
    """Unique leading ``customer_id`` keeps the gate approved."""
    df = _customers(["C1", "C2", "C3"])
    result = QualityGatePlaybook(min_score=0).execute(df, "customers")
    assert result["critical_failures"] == 0
    assert result["gate_passed"] is True


def test_foreign_keys_may_repeat():
    """Non-leading ``*_id`` columns stay informational foreign keys."""
    df = pd.DataFrame(
        {
            "transaction_id": ["T1", "T2", "T3"],
            "customer_id": ["C1", "C1", "C2"],
        }
    )
    report = DataQualityEngine().run_all_checks(df, "transactions")
    fk = _rule(report, "uniqueness_customer_id")
    assert fk["severity"] == "info"
    assert fk["passed"] is True
    assert _rule(report, "uniqueness_transaction_id")["passed"] is True
    assert report["critical_failures"] == 0


def test_explicit_primary_keys_override_inference():
    """Explicit ``primary_keys`` are checked even when not leading."""
    df = pd.DataFrame(
        {
            "order_line": [1, 2, 3],
            "order_id": ["O1", "O1", "O2"],
        }
    )
    gate = QualityGatePlaybook(min_score=0, primary_keys=["order_id"])
    result = gate.execute(df, "orders")
    assert result["critical_failures"] == 1
    assert result["gate_passed"] is False


def test_missing_explicit_primary_key_fails():
    """A declared primary key absent from the data fails critically."""
    df = pd.DataFrame({"name": ["a", "b"]})
    report = DataQualityEngine().run_all_checks(
        df, "missing", primary_keys=["id"]
    )
    assert _rule(report, "uniqueness_id")["passed"] is False
    assert report["critical_failures"] == 1


def test_null_primary_key_fails():
    """Null primary key values fail the uniqueness rule."""
    df = pd.DataFrame({"id": [1, None, 3], "value": ["a", "b", "c"]})
    report = DataQualityEngine().run_all_checks(df, "null_id")
    rule = _rule(report, "uniqueness_id")
    assert rule["passed"] is False
    assert rule["details"]["null_values"] == 1


if __name__ == "__main__":
    import sys

    failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {_name}: {exc!r}")
    sys.exit(1 if failures else 0)
