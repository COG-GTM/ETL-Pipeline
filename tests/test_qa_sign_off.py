"""Regression tests for the QA agent sign-off in MultiAgentOrchestrator."""

import pytest

from src.orchestrator import MultiAgentOrchestrator


def _report(source, score, critical=0):
    """Build a minimal DataQualityEngine-style report."""
    return {
        "source": source,
        "overall_score": score,
        "grade": "A" if score >= 90 else "F",
        "total_checks": 10,
        "passed": 10,
        "failed": 0,
        "critical_failures": critical,
        "recommendations": [],
    }


def _profile(path):
    """Build a minimal SourceProfiler-style profile for a file path."""
    return {"source": path, "format": path.rsplit(".", 1)[-1], "row_count": 10, "column_count": 3}


def _run(reports, profiles):
    """Run the workflow and return the result plus the QA agent's actions keyed by name."""
    orchestrator = MultiAgentOrchestrator()
    result = orchestrator.run_orchestrated_workflow(profiles, reports, {}, {"steps": []})
    qa_actions = {a["action"]: a["details"] for a in result["agents"]["qa"]["actions_taken"]}
    return result, qa_actions


@pytest.fixture
def profiles():
    """Three profiled CSV sources."""
    return [
        _profile("sample_data/csv/customers.csv"),
        _profile("sample_data/csv/transactions.csv"),
        _profile("sample_data/csv/products.csv"),
    ]


def test_one_failing_source_is_not_hidden_by_average(profiles):
    """A 0/100/100 batch (average 66.7) must not be approved."""
    reports = [_report("customers", 0), _report("transactions", 100), _report("products", 100)]
    result, qa = _run(reports, profiles)

    assert result["qa_sign_off"]["approved"] is False
    assert qa["sign_off"]["approved"] is False
    assert qa["quality_summary"]["failing_sources"] == ["customers"]
    assert result["status"] == "failed"


def test_critical_failure_blocks_sign_off(profiles):
    """A high-scoring source with a critical failure blocks approval."""
    reports = [_report("customers", 95, critical=1), _report("transactions", 100), _report("products", 100)]
    result, _ = _run(reports, profiles)

    assert result["qa_sign_off"]["approved"] is False
    assert any("critical" in c for c in result["qa_sign_off"]["conditions"])


def test_profiled_source_without_report_blocks_sign_off(profiles):
    """A profiled source with no quality report blocks approval."""
    profiles = profiles + [_profile("sample_data/json/orders.json")]
    reports = [_report("customers", 100), _report("transactions", 100), _report("products", 100)]
    result, qa = _run(reports, profiles)

    assert result["qa_sign_off"]["approved"] is False
    assert qa["quality_summary"]["unassessed_sources"] == ["sample_data/json/orders.json"]
    assert result["status"] == "failed"


@pytest.mark.parametrize("report_source", ["sales.v2", "sales.v2.csv", "data/SALES.v2.csv"])
def test_dotted_source_name_matches_profile(report_source):
    """Reports match profiles by name with or without extension, including dotted names."""
    result, qa = _run([_report(report_source, 100)], [_profile("data/sales.v2.csv")])

    assert qa["quality_summary"]["unassessed_sources"] == []
    assert result["qa_sign_off"]["approved"] is True


def test_report_stem_does_not_match_different_profile():
    """A report for 'sales.v2' does not cover a profiled 'sales.csv'."""
    result, qa = _run([_report("sales.v2", 100)], [_profile("data/sales.csv")])

    assert qa["quality_summary"]["unassessed_sources"] == ["data/sales.csv"]
    assert result["qa_sign_off"]["approved"] is False


def test_no_reports_is_not_approved(profiles):
    """No quality reports means no approval."""
    result, _ = _run([], profiles)

    assert result["qa_sign_off"]["approved"] is False


def test_no_unverified_attestations_recorded(profiles):
    """The summary does not claim contract or regression checks that never ran."""
    _, qa = _run([_report("customers", 100)], profiles[:1])

    assert "data_contracts_validated" not in qa["quality_summary"]
    assert "regression_tests_passed" not in qa["quality_summary"]


def test_workflow_does_not_log_success_when_rejected(profiles):
    """A rejected sign-off logs workflow_failed and the PM status is blocked."""
    reports = [_report("customers", 0), _report("transactions", 100), _report("products", 100)]
    result, _ = _run(reports, profiles)

    events = [e["event"] for e in result["workflow_log"]]
    assert "workflow_failed" in events
    assert "workflow_completed" not in events
    pm = {a["action"]: a["details"] for a in result["agents"]["pm"]["actions_taken"]}
    assert pm["progress_tracking"]["overall_status"] == "blocked"


def test_all_sources_pass_is_approved(profiles):
    """Every source passing with no critical failures is approved."""
    reports = [_report("customers", 100), _report("transactions", 90), _report("products", 85)]
    result, _ = _run(reports, profiles)

    assert result["qa_sign_off"]["approved"] is True
    assert result["qa_sign_off"]["conditions"] == []
    assert result["status"] == "completed"
    assert "workflow_completed" in [e["event"] for e in result["workflow_log"]]
