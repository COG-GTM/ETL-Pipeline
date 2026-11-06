import pandas as pd
import pytest

from src.quality.quality_engine import DataQualityEngine, QualityRule


class TestQualityRule:
    """Tests for the QualityRule class."""

    def test_default_values(self):
        rule = QualityRule("test_rule", "A test rule")
        assert rule.name == "test_rule"
        assert rule.description == "A test rule"
        assert rule.severity == "warning"
        assert rule.passed is False
        assert rule.details == {}

    def test_custom_severity(self):
        rule = QualityRule("test", "desc", severity="critical")
        assert rule.severity == "critical"

    def test_to_dict(self):
        rule = QualityRule("name", "desc", "error")
        rule.passed = True
        rule.details = {"column": "col1"}
        d = rule.to_dict()
        assert d["rule"] == "name"
        assert d["description"] == "desc"
        assert d["severity"] == "error"
        assert d["passed"] is True
        assert d["details"]["column"] == "col1"


class TestDataQualityEngine:
    """Tests for the DataQualityEngine class."""

    def setup_method(self):
        self.engine = DataQualityEngine()

    def _make_clean_df(self):
        """Create a clean DataFrame that should pass most quality checks."""
        return pd.DataFrame({
            "customer_id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
            "sale_date": ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01", "2023-05-01"],
        })

    def test_run_all_checks_returns_report(self):
        """Should return a report dict with expected keys."""
        df = self._make_clean_df()
        report = self.engine.run_all_checks(df, "test_source")

        assert "source" in report
        assert report["source"] == "test_source"
        assert "overall_score" in report
        assert "grade" in report
        assert "total_checks" in report
        assert "passed" in report
        assert "failed" in report
        assert "rules" in report
        assert "recommendations" in report

    def test_clean_data_high_score(self):
        """Clean data should get a high quality score."""
        df = self._make_clean_df()
        report = self.engine.run_all_checks(df)
        assert report["overall_score"] >= 70

    def test_completeness_check_with_nulls(self):
        """Should detect missing values."""
        df = pd.DataFrame({
            "a": [1, None, None, None, None],
            "b": [1, 2, 3, 4, 5],
        })
        self.engine._check_completeness(df)
        completeness_rules = [r for r in self.engine.rules if "completeness" in r.name]
        assert len(completeness_rules) > 0

    def test_completeness_fails_with_high_nulls(self):
        """Should fail completeness when >5% cells are null."""
        df = pd.DataFrame({
            "a": [None] * 50 + [1] * 50,
            "b": list(range(100)),
        })
        self.engine._check_completeness(df)
        overall = [r for r in self.engine.rules if r.name == "completeness_overall"][0]
        # 50 nulls out of 200 cells = 25% null -> completeness 75% -> fails (threshold 95%)
        assert overall.passed is False

    def test_uniqueness_check_no_duplicates(self):
        """Should pass uniqueness check when no duplicates."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        self.engine._check_uniqueness(df)
        dup_rule = [r for r in self.engine.rules if r.name == "no_duplicate_rows"][0]
        assert dup_rule.passed is True

    def test_uniqueness_check_with_duplicates(self):
        """Should detect duplicate rows."""
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        self.engine._check_uniqueness(df)
        dup_rule = [r for r in self.engine.rules if r.name == "no_duplicate_rows"][0]
        assert dup_rule.passed is False
        assert dup_rule.details["duplicate_rows"] == 1

    def test_uniqueness_id_column(self):
        """Should check uniqueness for _id columns."""
        df = pd.DataFrame({"user_id": [1, 2, 3], "name": ["a", "b", "c"]})
        self.engine._check_uniqueness(df)
        id_rules = [r for r in self.engine.rules if "uniqueness_user_id" in r.name]
        assert len(id_rules) == 1
        assert id_rules[0].passed is True

    def test_validity_email_check(self):
        """Should validate email format."""
        df = pd.DataFrame({
            "email": ["valid@example.com", "invalid-email", "also@valid.org"],
        })
        self.engine._check_validity(df)
        email_rules = [r for r in self.engine.rules if "email" in r.name]
        assert len(email_rules) == 1
        assert email_rules[0].passed is False
        assert email_rules[0].details["invalid_count"] == 1

    def test_validity_negative_prices(self):
        """Should detect negative values in price columns."""
        df = pd.DataFrame({"sale_price": [100.0, -50.0, 200.0]})
        self.engine._check_validity(df)
        neg_rules = [r for r in self.engine.rules if "non_negative" in r.name]
        assert len(neg_rules) == 1
        assert neg_rules[0].passed is False

    def test_timeliness_future_dates(self):
        """Should detect future dates."""
        df = pd.DataFrame({
            "created_date": ["2020-01-01", "2099-12-31"],
        })
        self.engine._check_timeliness(df)
        future_rules = [r for r in self.engine.rules if "future_dates" in r.name]
        assert len(future_rules) == 1
        assert future_rules[0].passed is False

    def test_accuracy_outliers(self):
        """Should detect statistical outliers."""
        values = [10, 11, 12, 13, 14, 15, 16, 17, 18, 1000]
        df = pd.DataFrame({"metric": values})
        self.engine._check_accuracy(df)
        outlier_rules = [r for r in self.engine.rules if "outliers" in r.name]
        assert len(outlier_rules) == 1

    def test_score_to_grade(self):
        """Should map scores to correct letter grades."""
        assert self.engine._score_to_grade(100) == "A"
        assert self.engine._score_to_grade(95) == "A"
        assert self.engine._score_to_grade(90) == "B"
        assert self.engine._score_to_grade(85) == "B"
        assert self.engine._score_to_grade(75) == "C"
        assert self.engine._score_to_grade(70) == "C"
        assert self.engine._score_to_grade(60) == "D"
        assert self.engine._score_to_grade(50) == "D"
        assert self.engine._score_to_grade(40) == "F"
        assert self.engine._score_to_grade(0) == "F"

    def test_generate_recommendations(self):
        """Should generate recommendations for failed rules."""
        rule = QualityRule("completeness_col1", "Check col1", "error")
        rule.passed = False
        rule.details = {"column": "col1"}
        self.engine.rules = [rule]

        recs = self.engine._generate_recommendations()
        assert len(recs) > 0
        assert any("col1" in r for r in recs)

    def test_generate_report_output(self):
        """Should generate a formatted string report."""
        df = self._make_clean_df()
        result = self.engine.run_all_checks(df, "test")
        report = self.engine.generate_report(result)

        assert "DATA QUALITY REPORT" in report
        assert "test" in report
        assert "QUALITY SCORE" in report

    def test_consistency_check_whitespace(self):
        """Should detect leading/trailing whitespace."""
        df = pd.DataFrame({"name": [" Alice", "Bob ", "Charlie"]})
        self.engine._check_consistency(df)
        ws_rules = [r for r in self.engine.rules if "whitespace" in r.name]
        assert len(ws_rules) == 1
        assert ws_rules[0].passed is False

    def test_severity_weights(self):
        """Should have correct severity weights."""
        assert DataQualityEngine.SEVERITY_WEIGHTS["critical"] == 0
        assert DataQualityEngine.SEVERITY_WEIGHTS["error"] == 25
        assert DataQualityEngine.SEVERITY_WEIGHTS["warning"] == 50
        assert DataQualityEngine.SEVERITY_WEIGHTS["info"] == 75

    def test_phone_validation(self):
        """Should validate phone format."""
        df = pd.DataFrame({
            "phone": ["123-456-7890", "not-a-phone", "(123) 456-7890"],
        })
        self.engine._check_validity(df)
        phone_rules = [r for r in self.engine.rules if "phone" in r.name]
        assert len(phone_rules) == 1
