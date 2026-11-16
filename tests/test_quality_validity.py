"""Tests for format and money-column validity checks in DataQualityEngine."""

import pandas as pd

from src.quality import DataQualityEngine


def _rules(report):
    """Index the report's rules by name."""
    return {r["rule"]: r for r in report["rules"]}


def test_email_with_trailing_newline_is_invalid():
    df = pd.DataFrame({"email": ["a@example.com", "b@example.com\n"]})
    rule = _rules(DataQualityEngine().run_all_checks(df))["valid_email_email"]
    assert rule["passed"] is False
    assert rule["details"]["invalid_count"] == 1


def test_phone_with_trailing_newline_is_invalid():
    df = pd.DataFrame({"phone": ["555-0101", "555-0102\n", "(555) 123-4567\n"]})
    rule = _rules(DataQualityEngine().run_all_checks(df))["valid_phone_phone"]
    assert rule["passed"] is False
    assert rule["details"]["invalid_count"] == 2


def test_valid_email_and_phone_still_pass():
    df = pd.DataFrame(
        {
            "email": ["a@example.com", "first.last+tag@sub.example.org"],
            "phone": ["555-0101", "(555) 123-4567"],
        }
    )
    rules = _rules(DataQualityEngine().run_all_checks(df))
    assert rules["valid_email_email"]["passed"] is True
    assert rules["valid_phone_phone"]["passed"] is True


def test_money_column_with_text_cell_still_checks_negatives():
    df = pd.DataFrame({"total_amount": [10.0, "N/A", -5.0, 20.0, -1.5]})
    rules = _rules(DataQualityEngine().run_all_checks(df))

    type_rule = rules["numeric_total_amount"]
    assert type_rule["passed"] is False
    assert type_rule["severity"] == "error"
    assert type_rule["details"]["non_numeric_count"] == 1

    neg_rule = rules["non_negative_total_amount"]
    assert neg_rule["passed"] is False
    assert neg_rule["details"]["negative_count"] == 2


def test_money_column_with_text_cell_still_checks_outliers():
    values = [10.0, 11.0, 12.0, 10.5, 11.5, 12.5, 10000.0, "N/A"]
    df = pd.DataFrame({"unit_price": values})
    rules = _rules(DataQualityEngine().run_all_checks(df))
    assert rules["outliers_unit_price"]["details"]["outlier_count"] == 1


def test_numeric_strings_in_money_column_are_accepted():
    df = pd.DataFrame({"cost": ["1.50", "2", "-3"]})
    rules = _rules(DataQualityEngine().run_all_checks(df))
    assert "numeric_cost" not in rules
    assert rules["non_negative_cost"]["details"]["negative_count"] == 1


def test_clean_numeric_money_column_adds_no_rules():
    df = pd.DataFrame({"sale_price": [1.0, 2.0, 3.0]})
    rules = _rules(DataQualityEngine().run_all_checks(df))
    assert "numeric_sale_price" not in rules
    assert "non_negative_sale_price" not in rules


def test_money_column_text_lowers_score():
    clean = pd.DataFrame({"total_amount": [10.0, 20.0, 30.0]})
    tampered = pd.DataFrame({"total_amount": [10.0, "N/A", -30.0]})
    engine = DataQualityEngine()
    clean_score = engine.run_all_checks(clean)["overall_score"]
    tampered_report = engine.run_all_checks(tampered)
    assert tampered_report["error_failures"] >= 2
    assert tampered_report["overall_score"] < clean_score
