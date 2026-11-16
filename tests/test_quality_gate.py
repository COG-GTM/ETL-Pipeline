import pytest

from demo.playbooks.quality_gate import QualityGatePlaybook


def _run_gate(score, critical=0, min_score=75, block_on_critical=True):
    gate = QualityGatePlaybook(min_score=min_score, block_on_critical=block_on_critical)
    report = {
        "overall_score": score,
        "critical_failures": critical,
        "grade": "D",
        "total_checks": 10,
        "passed": 6,
        "failed": 4,
        "recommendations": [],
    }
    gate.quality_engine.run_all_checks = lambda df, source_name: report
    return gate.execute(df=None, source_name="partner_feed")


def _says_proceed(result):
    return any(
        "proceed" in str(result[key]).lower() or "cleared" in str(result[key]).lower()
        for key in ("decision", "action")
    )


@pytest.mark.parametrize("score", [60.0, 62.0, 65.0, 74.9])
def test_score_below_min_in_remediation_band_is_blocked(score):
    result = _run_gate(score)

    assert result["gate_passed"] is False
    assert result["decision"] == "NEEDS_REMEDIATION"
    assert "blocked" in result["action"].lower()
    assert not _says_proceed(result)


@pytest.mark.parametrize("score", [0.0, 30.0, 59.9])
def test_score_far_below_min_is_rejected(score):
    result = _run_gate(score)

    assert result["gate_passed"] is False
    assert result["decision"] == "REJECTED"
    assert not _says_proceed(result)


@pytest.mark.parametrize("score", [75.0, 90.0, 100.0])
def test_score_at_or_above_min_is_approved(score):
    result = _run_gate(score)

    assert result["gate_passed"] is True
    assert result["decision"] == "APPROVED"


def test_critical_failure_rejects_even_with_passing_score():
    result = _run_gate(95.0, critical=1)

    assert result["gate_passed"] is False
    assert result["decision"] == "REJECTED"
    assert "critical" in result["action"].lower()


@pytest.mark.parametrize("score", [0.0, 30.0, 62.0, 74.9, 75.0, 100.0])
@pytest.mark.parametrize("critical", [0, 1])
def test_decision_never_disagrees_with_gate_passed(score, critical):
    result = _run_gate(score, critical=critical)

    assert (result["decision"] == "APPROVED") is result["gate_passed"]
    if not result["gate_passed"]:
        assert not _says_proceed(result)
