import json
import os
import tempfile

import pandas as pd
import pytest

from src.profiler.source_profiler import SourceProfiler


class TestSourceProfiler:
    """Tests for the SourceProfiler class."""

    def setup_method(self):
        self.profiler = SourceProfiler()

    def _write_csv(self, data: dict, tmpdir: str) -> str:
        path = os.path.join(tmpdir, "test.csv")
        pd.DataFrame(data).to_csv(path, index=False)
        return path

    def _write_json(self, data, tmpdir: str) -> str:
        path = os.path.join(tmpdir, "test.json")
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def _write_xml(self, xml_str: str, tmpdir: str) -> str:
        path = os.path.join(tmpdir, "test.xml")
        with open(path, "w") as f:
            f.write(xml_str)
        return path

    def test_profile_csv_basic(self):
        """Should profile a CSV file and return expected structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": [1, 2, 3], "b": ["x", "y", "z"]}, tmpdir)
            result = self.profiler.profile_csv(path)

            assert result["format"] == "csv"
            assert result["row_count"] == 3
            assert result["column_count"] == 2
            assert "a" in result["columns"]
            assert "b" in result["columns"]

    def test_profile_csv_detects_nulls(self):
        """Should detect null values in the profile."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": [1, None, 3], "b": ["x", "y", None]}, tmpdir)
            result = self.profiler.profile_csv(path)

            assert result["summary"]["total_null_cells"] > 0
            assert result["summary"]["null_percentage"] > 0

    def test_profile_csv_detects_duplicates(self):
        """Should detect duplicate rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": [1, 1, 2], "b": ["x", "x", "y"]}, tmpdir)
            result = self.profiler.profile_csv(path)

            assert result["summary"]["duplicate_rows"] == 1

    def test_profile_json_list(self):
        """Should profile a JSON file with a list of records."""
        with tempfile.TemporaryDirectory() as tmpdir:
            data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
            path = self._write_json(data, tmpdir)
            result = self.profiler.profile_json(path)

            assert result["format"] == "json"
            assert result["row_count"] == 2
            assert "name" in result["columns"]

    def test_profile_json_dict_with_data_key(self):
        """Should extract records from known keys like 'data'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            data = {"data": [{"id": 1}, {"id": 2}]}
            path = self._write_json(data, tmpdir)
            result = self.profiler.profile_json(path)

            assert result["row_count"] == 2

    def test_profile_json_dict_with_items_key(self):
        """Should extract records from 'items' key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            data = {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}
            path = self._write_json(data, tmpdir)
            result = self.profiler.profile_json(path)

            assert result["row_count"] == 3

    def test_profile_auto_csv(self):
        """Should auto-detect CSV format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": [1]}, tmpdir)
            result = self.profiler.profile_auto(path)
            assert result["format"] == "csv"

    def test_profile_auto_json(self):
        """Should auto-detect JSON format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_json([{"a": 1}], tmpdir)
            result = self.profiler.profile_auto(path)
            assert result["format"] == "json"

    def test_profile_auto_unsupported_format(self):
        """Should raise ValueError for unsupported formats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.xyz")
            with open(path, "w") as f:
                f.write("data")
            with pytest.raises(ValueError, match="Unsupported"):
                self.profiler.profile_auto(path)

    def test_profile_column_numeric_stats(self):
        """Should compute numeric statistics for numeric columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"price": [10.0, 20.0, 30.0, 40.0, 50.0]}, tmpdir)
            result = self.profiler.profile_csv(path)

            stats = result["columns"]["price"]["stats"]
            assert stats["mean"] == 30.0
            assert stats["min"] == 10.0
            assert stats["max"] == 50.0
            assert stats["zeros"] == 0
            assert stats["negatives"] == 0

    def test_profile_column_string_stats(self):
        """Should compute string length statistics."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"name": ["Al", "Bob", "Charlie"]}, tmpdir)
            result = self.profiler.profile_csv(path)

            stats = result["columns"]["name"]["stats"]
            assert stats["min_length"] == 2
            assert stats["max_length"] == 7

    def test_profile_column_email_detection(self):
        """Should detect email semantic type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({
                "contact": ["a@b.com", "c@d.org", "e@f.net", "g@h.io", "i@j.com"]
            }, tmpdir)
            result = self.profiler.profile_csv(path)

            assert result["columns"]["contact"].get("semantic_type") == "email"

    def test_profile_column_categorical_role(self):
        """Should detect categorical role for low-cardinality columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({
                "status": ["active", "inactive", "active", "pending", "active"] * 10
            }, tmpdir)
            result = self.profiler.profile_csv(path)

            assert result["columns"]["status"].get("inferred_role") == "categorical"

    def test_profile_stores_results(self):
        """Should store profile results in profile_results dict."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": [1]}, tmpdir)
            self.profiler.profile_csv(path)

            assert path in self.profiler.profile_results

    def test_generate_report(self):
        """Should produce a formatted report string."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": [1, 2], "b": ["x", "y"]}, tmpdir)
            profile = self.profiler.profile_csv(path)
            report = self.profiler.generate_report(profile)

            assert "SOURCE DATA PROFILE REPORT" in report
            assert "Rows:" in report
            assert "Columns:" in report

    def test_profile_xml_basic(self):
        """Should profile an XML file."""
        xml_data = """<?xml version="1.0"?>
<root>
  <record>
    <name>Alice</name>
    <age>30</age>
    <city>NYC</city>
  </record>
  <record>
    <name>Bob</name>
    <age>25</age>
    <city>LA</city>
  </record>
</root>"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_xml(xml_data, tmpdir)
            result = self.profiler.profile_xml(path)

            assert result["format"] == "xml"
            assert result["row_count"] == 2

    def test_memory_usage_tracked(self):
        """Should track memory usage in summary."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": list(range(100))}, tmpdir)
            result = self.profiler.profile_csv(path)

            assert result["summary"]["memory_usage_bytes"] > 0

    def test_unique_percentage_calculation(self):
        """Should compute correct unique percentage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv({"a": [1, 1, 2, 2, 3]}, tmpdir)
            result = self.profiler.profile_csv(path)

            assert result["columns"]["a"]["unique_count"] == 3
            assert result["columns"]["a"]["unique_percentage"] == 60.0
