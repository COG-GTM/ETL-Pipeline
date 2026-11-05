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
    rule = _rule(report, "uniqueness_id")
    assert rule["passed"] is False
    assert rule["details"]["missing_columns"] == ["id"]
    assert report["critical_failures"] == 1
    recs = " ".join(report["recommendations"])
    assert "missing from dataset" in recs
    assert "Fix duplicate IDs" not in recs


def test_null_primary_key_fails():
    """Null primary key values fail the uniqueness rule."""
    df = pd.DataFrame({"id": [1, None, 3], "value": ["a", "b", "c"]})
    report = DataQualityEngine().run_all_checks(df, "null_id")
    rule = _rule(report, "uniqueness_id")
    assert rule["passed"] is False
    assert rule["details"]["null_values"] == 1
    recs = " ".join(report["recommendations"])
    assert "null IDs in 'id'" in recs
    assert "Fix duplicate IDs" not in recs


def _order_lines(line_numbers):
    """Build order lines keyed by ``order_id`` + ``line_number``."""
    return pd.DataFrame(
        {
            "order_id": ["O1", "O1", "O2"],
            "line_number": line_numbers,
            "sku": ["a", "b", "c"],
        }
    )


def test_composite_primary_key_allows_repeated_components():
    """Unique ``(order_id, line_number)`` pairs pass the gate."""
    gate = QualityGatePlaybook(
        min_score=0, primary_keys=["order_id", "line_number"]
    )
    result = gate.execute(_order_lines([1, 2, 1]), "order_lines")
    assert result["critical_failures"] == 0
    assert result["gate_passed"] is True


def test_composite_primary_key_duplicate_pair_fails():
    """A repeated ``(order_id, line_number)`` pair fails critically."""
    report = DataQualityEngine().run_all_checks(
        _order_lines([1, 1, 1]),
        "order_lines",
        primary_keys=["order_id", "line_number"],
    )
    rule = _rule(report, "uniqueness_composite[order_id, line_number]")
    assert rule["severity"] == "critical"
    assert rule["passed"] is False
    assert rule["details"]["duplicate_values"] == 1
    assert report["critical_failures"] == 1


def test_composite_rule_name_does_not_clash_with_column():
    """A column named like the joined key keeps its own rule."""
    df = pd.DataFrame(
        {
            "order": ["O1", "O1"],
            "item_id": [1, 2],
            "order_item_id": ["X", "X"],
        }
    )
    report = DataQualityEngine().run_all_checks(
        df, "items", primary_keys=["order", "item_id"]
    )
    names = [r["rule"] for r in report["rules"]]
    assert len(names) == len(set(names))
    assert _rule(report, "uniqueness_order_item_id")["severity"] == "info"
    pk = _rule(report, "uniqueness_composite[order, item_id]")
    assert pk["passed"] is True


def test_gate_reuses_generator_primary_keys():
    """A generator of key names is honoured on every gate run."""
    df = pd.DataFrame({"line": [1, 2], "order_id": ["O1", "O1"]})
    gate = QualityGatePlaybook(
        min_score=0, primary_keys=(c for c in ["order_id"])
    )
    for _ in range(2):
        assert gate.execute(df, "orders")["gate_passed"] is False


def test_string_primary_key_is_one_column():
    """A plain string names a single primary key column."""
    df = pd.DataFrame({"line": [1, 2], "order_id": ["O1", "O1"]})
    gate = QualityGatePlaybook(min_score=0, primary_keys="order_id")
    assert gate.primary_keys == ("order_id",)
    assert gate.execute(df, "orders")["critical_failures"] == 1


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
