import pytest

from src.profiler.schema_detector import SchemaDetector


class TestSchemaDetector:
    """Tests for the SchemaDetector class."""

    def setup_method(self):
        self.detector = SchemaDetector()

    def _make_profile(self, columns: dict, source: str = "test_data.csv"):
        """Helper to create a profile dict."""
        return {
            "source": source,
            "columns": columns,
        }

    def test_detect_schema_basic(self):
        """Should detect schema with correct structure."""
        profile = self._make_profile({
            "id": {"dtype": "int64", "null_count": 0, "unique_percentage": 100.0},
            "name": {"dtype": "object", "null_count": 0, "unique_percentage": 80.0, "stats": {"max_length": 50}},
        })
        schema = self.detector.detect_schema(profile)

        assert schema["source"] == "test_data.csv"
        assert schema["detected_table_name"] == "test_data"
        assert len(schema["columns"]) == 2

    def test_infer_table_name(self):
        """Should infer table name from file path."""
        assert self.detector._infer_table_name("data/my_table.csv") == "my_table"
        assert self.detector._infer_table_name("path/to/Sales-Data.json") == "sales_data"
        assert self.detector._infer_table_name("records.xml") == "records"

    def test_type_map_integer(self):
        """Should map int64 to INTEGER."""
        profile = self._make_profile({
            "count": {"dtype": "int64", "null_count": 0, "unique_percentage": 50.0},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "INTEGER"

    def test_type_map_float(self):
        """Should map float64 to DECIMAL."""
        profile = self._make_profile({
            "price": {"dtype": "float64", "null_count": 0, "unique_percentage": 90.0},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "DECIMAL"

    def test_type_map_boolean(self):
        """Should map bool to BOOLEAN."""
        profile = self._make_profile({
            "active": {"dtype": "bool", "null_count": 0, "unique_percentage": 50.0},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "BOOLEAN"

    def test_type_map_datetime(self):
        """Should map datetime64[ns] to TIMESTAMP."""
        profile = self._make_profile({
            "created": {"dtype": "datetime64[ns]", "null_count": 0, "unique_percentage": 100.0},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "TIMESTAMP"

    def test_type_map_string_with_length(self):
        """Should map object to VARCHAR with appropriate length."""
        profile = self._make_profile({
            "description": {"dtype": "object", "null_count": 0, "unique_percentage": 80.0, "stats": {"max_length": 100}},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert "VARCHAR" in col["target_dtype"]
        assert "150" in col["target_dtype"]  # 100 * 1.5

    def test_email_semantic_type(self):
        """Should set VARCHAR(255) and email validation for email columns."""
        profile = self._make_profile({
            "email": {"dtype": "object", "null_count": 0, "unique_percentage": 100.0, "semantic_type": "email", "stats": {"max_length": 30}},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "VARCHAR(255)"
        assert col["validation"] == "email_format"

    def test_phone_semantic_type(self):
        """Should set VARCHAR(20) and phone validation for phone columns."""
        profile = self._make_profile({
            "phone": {"dtype": "object", "null_count": 0, "unique_percentage": 100.0, "semantic_type": "phone", "stats": {"max_length": 15}},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "VARCHAR(20)"
        assert col["validation"] == "phone_format"

    def test_date_semantic_type(self):
        """Should set DATE type for date semantic columns."""
        profile = self._make_profile({
            "created_at": {"dtype": "object", "null_count": 0, "unique_percentage": 80.0, "semantic_type": "date", "stats": {"max_length": 10}},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "DATE"

    def test_primary_key_candidate(self):
        """Should detect primary key candidates."""
        profile = self._make_profile({
            "user_id": {"dtype": "int64", "null_count": 0, "unique_percentage": 100.0, "inferred_role": "identifier"},
        })
        schema = self.detector.detect_schema(profile)
        assert "user_id" in schema["primary_key_candidates"]

    def test_foreign_key_candidate(self):
        """Should detect foreign key candidates."""
        profile = self._make_profile({
            "order_id": {"dtype": "int64", "null_count": 5, "unique_percentage": 50.0},
        })
        schema = self.detector.detect_schema(profile)
        assert "order_id" in schema["foreign_key_candidates"]

    def test_index_recommendation(self):
        """Should recommend indexes for categorical columns."""
        profile = self._make_profile({
            "status": {"dtype": "object", "null_count": 0, "unique_percentage": 10.0, "inferred_role": "categorical", "stats": {"max_length": 10}},
        })
        schema = self.detector.detect_schema(profile)
        assert "status" in schema["indexes_recommended"]

    def test_nullable_detection(self):
        """Should correctly detect nullable columns."""
        profile = self._make_profile({
            "required": {"dtype": "object", "null_count": 0, "unique_percentage": 50.0, "stats": {"max_length": 10}},
            "optional": {"dtype": "object", "null_count": 5, "unique_percentage": 50.0, "stats": {"max_length": 10}},
        })
        schema = self.detector.detect_schema(profile)
        cols = {c["name"]: c for c in schema["columns"]}
        assert cols["required"]["nullable"] is False
        assert cols["optional"]["nullable"] is True

    def test_generate_ddl(self):
        """Should generate valid DDL."""
        profile = self._make_profile({
            "id": {"dtype": "int64", "null_count": 0, "unique_percentage": 100.0, "inferred_role": "identifier"},
            "name": {"dtype": "object", "null_count": 0, "unique_percentage": 80.0, "stats": {"max_length": 50}},
        })
        schema = self.detector.detect_schema(profile)
        ddl = self.detector.generate_ddl(schema)

        assert "CREATE TABLE test_data" in ddl
        assert "id" in ddl
        assert "name" in ddl
        assert "PRIMARY KEY" in ddl

    def test_generate_schema_report(self):
        """Should generate a formatted schema report."""
        profile = self._make_profile({
            "id": {"dtype": "int64", "null_count": 0, "unique_percentage": 100.0, "inferred_role": "identifier"},
        })
        schema = self.detector.detect_schema(profile)
        report = self.detector.generate_schema_report(schema)

        assert "DETECTED SCHEMA" in report
        assert "test_data" in report

    def test_unknown_dtype_defaults_varchar(self):
        """Should default to VARCHAR(255) for unknown dtypes."""
        profile = self._make_profile({
            "weird": {"dtype": "category", "null_count": 0, "unique_percentage": 50.0},
        })
        schema = self.detector.detect_schema(profile)
        col = schema["columns"][0]
        assert col["target_dtype"] == "VARCHAR(255)"
