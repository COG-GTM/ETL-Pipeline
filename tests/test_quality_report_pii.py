import pandas as pd
import pytest

from src.quality.quality_engine import DataQualityEngine, mask_email, mask_phone, mask_text

RAW_EMAIL = "jane.doe@examplecorp"
RAW_EMAIL_LOCAL = "jane.doe"
RAW_PHONE = "415-555-01XX"
RAW_PHONE_DIGITS = "415-555"


@pytest.fixture
def customers():
    return pd.DataFrame(
        {
            "customer_id": ["C1", "C2", "C3"],
            "email": [RAW_EMAIL, "ok.user@example.com", "bob.smith@example.com"],
            "phone": [RAW_PHONE, "555-0101", "555-0102"],
            "first_name": ["Jonathan", "Jonathans", "Mary"],
        }
    )


def _rule(report, name):
    return next(r for r in report["rules"] if r["rule"] == name)


def test_invalid_email_sample_is_masked(customers):
    report = DataQualityEngine().run_all_checks(customers, "customers")
    rule = _rule(report, "valid_email_email")
    assert rule["details"]["invalid_count"] == 1
    assert rule["details"]["sample_invalid"] == ["j***@e***"]


def test_invalid_phone_sample_keeps_only_last_two_characters(customers):
    report = DataQualityEngine().run_all_checks(customers, "customers")
    rule = _rule(report, "valid_phone_phone")
    assert rule["details"]["invalid_count"] == 1
    assert rule["details"]["sample_invalid"] == ["***-***-**XX"]


def test_consistency_pairs_are_masked(customers):
    report = DataQualityEngine().run_all_checks(customers, "customers")
    rule = _rule(report, "consistency_first_name")
    pairs = rule["details"]["potential_inconsistencies"]
    assert pairs
    for a, b in pairs:
        assert a == "j***" and b == "j***"


def test_raw_values_absent_from_report_and_text(customers):
    engine = DataQualityEngine()
    report = engine.run_all_checks(customers, "customers")
    text = engine.generate_report(report)
    serialized = repr(report)
    for raw in (RAW_EMAIL, RAW_EMAIL_LOCAL, "examplecorp", RAW_PHONE, RAW_PHONE_DIGITS, "jonathan", "jonathans"):
        assert raw not in serialized
        assert raw not in text


def test_numeric_phone_values_are_masked():
    df = pd.DataFrame({"phone": [4155550199, 5550101]})
    report = DataQualityEngine().run_all_checks(df, "numeric_phones")
    assert _rule(report, "valid_phone_phone")["details"]["sample_invalid"] == ["********99", "*****01"]


@pytest.mark.parametrize(
    "value,expected",
    [
        ("alice@email.com", "a***@e***.com"),
        ("alice@mail.example.co.uk", "a***@m***.uk"),
        ("no-at-sign.example.com", "n***"),
        ("a@b@c.org", "a***@c***.org"),
        ("@example.com", "@e***.com"),
        ("", ""),
    ],
)
def test_mask_email(value, expected):
    assert mask_email(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("555-XXXX", "***-**XX"),
        ("(415) 555-0199", "(***) ***-**99"),
        ("12", "**"),
        ("123", "***"),
    ],
)
def test_mask_phone(value, expected):
    assert mask_phone(value) == expected


def test_mask_text():
    assert mask_text("Gold") == "G***"
    assert mask_text("") == ""
