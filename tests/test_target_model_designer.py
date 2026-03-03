import pytest

from src.model_designer.target_model_designer import TargetModelDesigner


class TestTargetModelDesigner:
    """Tests for the TargetModelDesigner class."""

    def setup_method(self):
        self.designer = TargetModelDesigner()

    def _make_schema(self, table_name, columns):
        return {
            "detected_table_name": table_name,
            "columns": columns,
        }

    def _make_column(self, name, target_dtype="VARCHAR(255)", nullable=True, is_unique=False):
        return {
            "name": name,
            "target_dtype": target_dtype,
            "nullable": nullable,
            "is_unique": is_unique,
        }

    def test_add_source_schema(self):
        """Should add source schemas to the list."""
        schema = self._make_schema("test", [])
        self.designer.add_source_schema(schema)
        assert len(self.designer.source_schemas) == 1

    def test_design_star_schema_with_hint(self):
        """Should use hint to identify the fact table."""
        self.designer.add_source_schema(self._make_schema("transactions", [
            self._make_column("transaction_id", "INTEGER", False, True),
            self._make_column("amount", "DECIMAL"),
            self._make_column("customer_id", "INTEGER"),
        ]))
        self.designer.add_source_schema(self._make_schema("customers", [
            self._make_column("customer_id", "INTEGER", False, True),
            self._make_column("name", "VARCHAR(100)"),
        ]))

        model = self.designer.design_star_schema(fact_table_hint="transactions")

        assert model["schema_type"] == "star_schema"
        assert model["fact_table"]["name"] == "fact_transactions"
        assert len(model["dimension_tables"]) == 1
        assert model["dimension_tables"][0]["name"] == "dim_customers"

    def test_design_star_schema_fallback_largest_table(self):
        """Should use the table with most columns as fact if hint doesn't match."""
        self.designer.add_source_schema(self._make_schema("small_table", [
            self._make_column("id", "INTEGER"),
        ]))
        self.designer.add_source_schema(self._make_schema("big_table", [
            self._make_column("id", "INTEGER"),
            self._make_column("col1", "VARCHAR(255)"),
            self._make_column("col2", "VARCHAR(255)"),
            self._make_column("col3", "VARCHAR(255)"),
        ]))

        model = self.designer.design_star_schema(fact_table_hint="nonexistent")

        assert model["fact_table"]["name"] == "fact_big_table"

    def test_infer_relationships(self):
        """Should infer relationships between fact and dimension tables."""
        self.designer.add_source_schema(self._make_schema("sales", [
            self._make_column("sale_id", "INTEGER", False, True),
            self._make_column("product_id", "INTEGER"),
            self._make_column("customer_id", "INTEGER"),
        ]))
        self.designer.add_source_schema(self._make_schema("products", [
            self._make_column("product_id", "INTEGER", False, True),
            self._make_column("name", "VARCHAR(100)"),
        ]))

        model = self.designer.design_star_schema(fact_table_hint="sales")

        rels = model["relationships"]
        assert len(rels) > 0
        fk_rels = [r for r in rels if r["from_column"] == "product_id"]
        assert len(fk_rels) > 0

    def test_infer_relationships_no_fact_table(self):
        """Should return empty relationships when fact table is None."""
        result = self.designer._infer_relationships(None, [])
        assert result == []

    def test_design_aggregations(self):
        """Should design aggregation tables for numeric + date columns."""
        self.designer.add_source_schema(self._make_schema("transactions", [
            self._make_column("transaction_id", "INTEGER", False, True),
            self._make_column("sale_date", "DATE"),
            self._make_column("amount", "DECIMAL"),
            self._make_column("customer_id", "INTEGER"),
        ]))

        model = self.designer.design_star_schema(fact_table_hint="transactions")

        aggs = model["aggregation_tables"]
        assert len(aggs) > 0

    def test_design_aggregations_no_fact(self):
        """Should return empty aggregations when no fact table."""
        result = self.designer._design_aggregations(None)
        assert result == []

    def test_generate_target_ddl_no_model(self):
        """Should return placeholder when no model designed."""
        ddl = self.designer.generate_target_ddl()
        assert "No target model designed yet" in ddl

    def test_generate_target_ddl(self):
        """Should generate DDL with CREATE TABLE statements."""
        self.designer.add_source_schema(self._make_schema("transactions", [
            self._make_column("id", "INTEGER", False, True),
            self._make_column("amount", "DECIMAL"),
        ]))
        self.designer.add_source_schema(self._make_schema("customers", [
            self._make_column("customer_id", "INTEGER", False, True),
            self._make_column("name", "VARCHAR(100)"),
        ]))
        self.designer.design_star_schema(fact_table_hint="transactions")

        ddl = self.designer.generate_target_ddl()

        assert "CREATE TABLE" in ddl
        assert "fact_transactions" in ddl
        assert "dim_customers" in ddl
        assert "effective_date" in ddl  # SCD Type 2 columns
        assert "etl_load_timestamp" in ddl  # ETL audit columns

    def test_generate_report_no_model(self):
        """Should handle report generation when no model exists."""
        report = self.designer.generate_report()
        assert "No target model designed yet" in report

    def test_generate_report(self):
        """Should generate a formatted model report."""
        self.designer.add_source_schema(self._make_schema("transactions", [
            self._make_column("id", "INTEGER", False, True),
            self._make_column("amount", "DECIMAL"),
        ]))
        self.designer.design_star_schema(fact_table_hint="transactions")

        report = self.designer.generate_report()

        assert "TARGET DATA MODEL DESIGN" in report
        assert "star_schema" in report
        assert "FACT TABLE" in report

    def test_design_star_schema_sets_designed_at(self):
        """Should set the designed_at timestamp."""
        self.designer.add_source_schema(self._make_schema("test", [
            self._make_column("id", "INTEGER"),
        ]))
        model = self.designer.design_star_schema()
        assert "designed_at" in model

    def test_multiple_dimension_tables(self):
        """Should create multiple dimension tables."""
        self.designer.add_source_schema(self._make_schema("sales", [
            self._make_column("sale_id", "INTEGER"),
            self._make_column("amount", "DECIMAL"),
        ]))
        self.designer.add_source_schema(self._make_schema("products", [
            self._make_column("product_id", "INTEGER"),
        ]))
        self.designer.add_source_schema(self._make_schema("customers", [
            self._make_column("customer_id", "INTEGER"),
        ]))

        model = self.designer.design_star_schema(fact_table_hint="sales")

        assert len(model["dimension_tables"]) == 2
