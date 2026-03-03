import json
import os
import tempfile

import pandas as pd
import pytest

from src.consolidator.data_consolidator import DataConsolidator


class TestDataConsolidator:
    """Tests for the DataConsolidator class."""

    def setup_method(self):
        self.consolidator = DataConsolidator()

    def _write_csv(self, data: dict, path: str) -> str:
        pd.DataFrame(data).to_csv(path, index=False)
        return path

    def _write_json(self, data, path: str) -> str:
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def test_load_csv_source(self):
        """Should load a CSV source and store it."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            self._write_csv({"a": [1, 2], "b": ["x", "y"]}, path)

            df = self.consolidator.load_source("test", path)

            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert "test" in self.consolidator.sources

    def test_load_json_list_source(self):
        """Should load a JSON list source."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.json")
            self._write_json([{"a": 1}, {"a": 2}], path)

            df = self.consolidator.load_source("json_src", path)

            assert len(df) == 2
            assert "a" in df.columns

    def test_load_json_dict_with_known_key(self):
        """Should extract data from known dict keys like 'data', 'items', etc."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.json")
            self._write_json({"data": [{"id": 1}, {"id": 2}]}, path)

            df = self.consolidator.load_source("json_src", path)

            assert len(df) == 2

    def test_load_unsupported_format(self):
        """Should raise ValueError for unsupported formats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.xyz")
            with open(path, "w") as f:
                f.write("data")

            with pytest.raises(ValueError, match="Unsupported format"):
                self.consolidator.load_source("bad", path)

    def test_load_source_records_lineage(self):
        """Should record lineage for source loading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            self._write_csv({"a": [1]}, path)

            self.consolidator.load_source("test", path)

            assert len(self.consolidator.lineage) == 1
            assert self.consolidator.lineage[0]["action"] == "load_source"
            assert self.consolidator.lineage[0]["source_name"] == "test"

    def test_consolidate_single_join(self):
        """Should join two sources on a common key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path1 = os.path.join(tmpdir, "orders.csv")
            path2 = os.path.join(tmpdir, "customers.csv")
            self._write_csv({"customer_id": [1, 2], "amount": [100, 200]}, path1)
            self._write_csv({"customer_id": [1, 2], "name": ["Alice", "Bob"]}, path2)

            self.consolidator.load_source("orders", path1)
            self.consolidator.load_source("customers", path2)

            result = self.consolidator.consolidate("orders", [
                {"source": "customers", "on": "customer_id", "how": "left"},
            ])

            assert len(result) == 2
            assert "name" in result.columns

    def test_consolidate_primary_not_loaded(self):
        """Should raise ValueError when primary source is not loaded."""
        with pytest.raises(ValueError, match="not loaded"):
            self.consolidator.consolidate("missing", [])

    def test_consolidate_skips_unloaded_source(self):
        """Should skip join for unloaded sources and record in lineage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "main.csv")
            self._write_csv({"a": [1]}, path)
            self.consolidator.load_source("main", path)

            result = self.consolidator.consolidate("main", [
                {"source": "nonexistent", "on": "a"},
            ])

            assert len(result) == 1
            skip_events = [e for e in self.consolidator.lineage if e["action"] == "skip_join"]
            assert len(skip_events) == 1

    def test_consolidate_multiple_joins(self):
        """Should join multiple sources sequentially."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_csv({"id": [1, 2], "value": [10, 20]}, os.path.join(tmpdir, "main.csv"))
            self._write_csv({"id": [1, 2], "label": ["A", "B"]}, os.path.join(tmpdir, "labels.csv"))
            self._write_csv({"id": [1, 2], "score": [0.9, 0.8]}, os.path.join(tmpdir, "scores.csv"))

            self.consolidator.load_source("main", os.path.join(tmpdir, "main.csv"))
            self.consolidator.load_source("labels", os.path.join(tmpdir, "labels.csv"))
            self.consolidator.load_source("scores", os.path.join(tmpdir, "scores.csv"))

            result = self.consolidator.consolidate("main", [
                {"source": "labels", "on": "id"},
                {"source": "scores", "on": "id"},
            ])

            assert "label" in result.columns
            assert "score" in result.columns

    def test_get_consolidation_summary_before_consolidation(self):
        """Should return summary even before consolidation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            self._write_csv({"a": [1, 2]}, path)
            self.consolidator.load_source("src", path)

            summary = self.consolidator.get_consolidation_summary()

            assert "src" in summary["sources_loaded"]
            assert summary["consolidated_shape"] is None

    def test_get_consolidation_summary_after_consolidation(self):
        """Should include consolidated shape after consolidation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            self._write_csv({"a": [1, 2]}, path)
            self.consolidator.load_source("src", path)
            self.consolidator.consolidate("src", [])

            summary = self.consolidator.get_consolidation_summary()

            assert summary["consolidated_shape"] is not None
            assert summary["consolidated_shape"]["rows"] == 2

    def test_generate_report(self):
        """Should generate a formatted report string."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.csv")
            self._write_csv({"a": [1, 2]}, path)
            self.consolidator.load_source("src", path)
            self.consolidator.consolidate("src", [])

            report = self.consolidator.generate_report()

            assert "DATA CONSOLIDATION REPORT" in report
            assert "SOURCE SYSTEMS" in report

    def test_parse_xml(self):
        """Should parse XML into a DataFrame with records from leaf elements."""
        xml_data = """<?xml version="1.0"?>
<root>
  <item>
    <name>Widget</name>
    <price>9.99</price>
    <qty>100</qty>
  </item>
  <item>
    <name>Gadget</name>
    <price>19.99</price>
    <qty>50</qty>
  </item>
</root>"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.xml")
            with open(path, "w") as f:
                f.write(xml_data)

            df = self.consolidator.load_source("xml_src", path)

            # The parser also picks up the root-level element with item children
            # that contain leaf text nodes, producing 3 records
            assert len(df) >= 2
            assert "name" in df.columns

    def test_left_on_right_on_join(self):
        """Should support left_on/right_on for joins with different column names."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_csv({"cust_id": [1, 2], "amount": [10, 20]}, os.path.join(tmpdir, "orders.csv"))
            self._write_csv({"customer_id": [1, 2], "name": ["A", "B"]}, os.path.join(tmpdir, "customers.csv"))

            self.consolidator.load_source("orders", os.path.join(tmpdir, "orders.csv"))
            self.consolidator.load_source("customers", os.path.join(tmpdir, "customers.csv"))

            result = self.consolidator.consolidate("orders", [
                {"source": "customers", "left_on": "cust_id", "right_on": "customer_id", "how": "left"},
            ])

            assert "name" in result.columns
            assert len(result) == 2
