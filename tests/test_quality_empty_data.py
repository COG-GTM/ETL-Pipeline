"""Quality scoring must fail closed when a source yields no rows."""

import contextlib
import io
import os
import tempfile

import pandas as pd

from demo.playbooks import DataOnboardingPlaybook, QualityGatePlaybook
from src.quality import DataQualityEngine

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHIPPING_XML = os.path.join(
    REPO_ROOT, "demo", "sample_data", "xml", "shipping_records.xml"
)


def _run_onboarding(path):
    """Run the onboarding playbook quietly and return its quality report."""
    playbook = DataOnboardingPlaybook()
    with contextlib.redirect_stdout(io.StringIO()):
        result = playbook.execute(path)
    return playbook, result["steps"]["quality"]


def test_empty_dataframe_is_not_a_perfect_score():
    """An empty frame scores 0 / F and is flagged as not evaluated."""
    report = DataQualityEngine().run_all_checks(pd.DataFrame(), "empty")
    assert report["overall_score"] == 0.0
    assert report["grade"] == "F"
    assert report["evaluated"] is False
    rules = {r["rule"]: r for r in report["rules"]}
    assert rules["non_empty_dataset"]["passed"] is False
    assert rules["non_empty_dataset"]["severity"] == "critical"
    assert rules["completeness_overall"]["passed"] is False
    assert report["critical_failures"] == 1


def test_header_only_frame_fails_closed():
    """Columns without rows are still treated as no data."""
    df = pd.DataFrame(columns=["id", "name", "email"])
    report = DataQualityEngine().run_all_checks(df, "header-only")
    assert report["overall_score"] == 0.0
    assert report["grade"] == "F"
    assert report["evaluated"] is False


def test_empty_recommendations_name_the_real_problem():
    """Empty data gets one 'no rows' hint, not a missing-values hint."""
    engine = DataQualityEngine()
    report = engine.run_all_checks(pd.DataFrame(), "empty")
    recs = report["recommendations"]
    assert len(recs) == 1
    assert "no rows" in recs[0]
    assert "NOT EVALUATED" in engine.generate_report(report)


def test_non_empty_data_scores_unchanged():
    """A clean frame still scores 100 and gets no emptiness rule."""
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    report = DataQualityEngine().run_all_checks(df, "clean")
    assert report["overall_score"] == 100.0
    assert report["grade"] == "A"
    assert report["evaluated"] is True
    assert "non_empty_dataset" not in {r["rule"] for r in report["rules"]}


def test_quality_gate_rejects_empty_data():
    """The quality gate does not approve a feed with no rows."""
    gate = QualityGatePlaybook(min_score=75)
    with contextlib.redirect_stdout(io.StringIO()):
        result = gate.execute(pd.DataFrame(), "empty_feed")
    assert result["gate_passed"] is False
    assert result["decision"] == "REJECTED"
    assert result["evaluated"] is False


def test_onboarding_checks_xml_rows():
    """XML sources are parsed and actually checked during onboarding."""
    _, quality = _run_onboarding(SHIPPING_XML)
    assert quality["evaluated"] is True
    assert quality["total_checks"] > 2
    rule_names = {r["rule"] for r in quality["rules"]}
    assert "uniqueness_shipment_id" in rule_names


def test_onboarding_empty_xml_is_flagged():
    """An XML feed with no records is reported as not evaluated."""
    with tempfile.NamedTemporaryFile(
        "w", suffix=".xml", delete=False
    ) as handle:
        handle.write("<shipments></shipments>")
    try:
        playbook, quality = _run_onboarding(handle.name)
    finally:
        os.unlink(handle.name)
    assert quality["overall_score"] == 0.0
    assert quality["grade"] == "F"
    assert "NOT EVALUATED" in playbook.generate_report()


if __name__ == "__main__":
    for name, func in sorted(globals().items()):
        if name.startswith("test_") and callable(func):
            func()
            print(f"PASS {name}")
