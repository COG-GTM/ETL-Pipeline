import pytest

from src.pipeline_generator.pipeline_builder import PipelineBuilder, PipelineStep


class TestPipelineStep:
    """Tests for the PipelineStep class."""

    def test_init(self):
        step = PipelineStep("extract_data", "extract", {"source": "file.csv"})
        assert step.name == "extract_data"
        assert step.step_type == "extract"
        assert step.config == {"source": "file.csv"}
        assert step.status == "pending"
        assert step.output is None
        assert step.error is None

    def test_to_dict(self):
        step = PipelineStep("test_step", "transform", {"ops": []})
        d = step.to_dict()
        assert d["name"] == "test_step"
        assert d["type"] == "transform"
        assert d["status"] == "pending"
        assert d["duration_ms"] == 0
        assert d["error"] is None


class TestPipelineBuilder:
    """Tests for the PipelineBuilder class."""

    def setup_method(self):
        self.builder = PipelineBuilder()

    def _make_profile(self, source, row_count=100, fmt="csv", columns=None):
        return {
            "source": source,
            "row_count": row_count,
            "format": fmt,
            "columns": columns or {},
        }

    def _make_quality_report(self, source, score=85, grade="B", recommendations=None):
        return {
            "source": source,
            "overall_score": score,
            "grade": grade,
            "recommendations": recommendations or [],
        }

    def _make_target_model(self):
        return {
            "schema_type": "star_schema",
            "fact_table": {"name": "fact_sales"},
            "dimension_tables": [{"name": "dim_customers"}, {"name": "dim_products"}],
        }

    def test_generate_pipeline_single_source(self):
        """Should generate pipeline for a single source."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]
        model = self._make_target_model()

        config = self.builder.generate_pipeline_from_sources(profiles, qr, model)

        assert config["pipeline_name"] == "zero_touch_etl_pipeline"
        assert config["total_steps"] > 0
        assert "steps" in config
        assert "execution_config" in config

    def test_generate_pipeline_has_extract_step(self):
        """Should include an extract step for each source."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        extract_steps = [s for s in config["steps"] if s["type"] == "extract"]
        assert len(extract_steps) == 1

    def test_generate_pipeline_has_validate_step(self):
        """Should include a validate step for each quality report."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        validate_steps = [s for s in config["steps"] if s["type"] == "validate"]
        assert len(validate_steps) >= 1

    def test_generate_pipeline_has_load_step(self):
        """Should always include a load step."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        load_steps = [s for s in config["steps"] if s["type"] == "load"]
        assert len(load_steps) == 1

    def test_multiple_sources_adds_consolidation(self):
        """Should add consolidation step when multiple sources exist."""
        profiles = [
            self._make_profile("source1.csv", columns={"customer_id": {}}),
            self._make_profile("source2.csv", columns={"customer_id": {}}),
        ]
        qr = [
            self._make_quality_report("source1.csv"),
            self._make_quality_report("source2.csv"),
        ]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        consolidate_steps = [s for s in config["steps"] if s["type"] == "consolidate"]
        assert len(consolidate_steps) == 1

    def test_single_source_no_consolidation(self):
        """Should not add consolidation for a single source."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        consolidate_steps = [s for s in config["steps"] if s["type"] == "consolidate"]
        assert len(consolidate_steps) == 0

    def test_target_model_adds_transform(self):
        """Should add transform step when target model is provided."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]
        model = self._make_target_model()

        config = self.builder.generate_pipeline_from_sources(profiles, qr, model)

        transform_steps = [s for s in config["steps"] if s["type"] == "transform" and "target" in s["name"]]
        assert len(transform_steps) == 1

    def test_chunked_strategy_for_large_datasets(self):
        """Should use chunked strategy for large datasets (>100k rows)."""
        profiles = [self._make_profile("big.csv", row_count=500000)]
        qr = [self._make_quality_report("big.csv")]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        extract_step = [s for s in config["steps"] if s["type"] == "extract"][0]
        assert extract_step["config"]["strategy"] == "chunked"
        assert extract_step["config"]["chunk_size"] == 10000

    def test_full_load_strategy_for_small_datasets(self):
        """Should use full_load strategy for small datasets."""
        profiles = [self._make_profile("small.csv", row_count=100)]
        qr = [self._make_quality_report("small.csv")]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        extract_step = [s for s in config["steps"] if s["type"] == "extract"][0]
        assert extract_step["config"]["strategy"] == "full_load"

    def test_recommendations_to_transforms(self):
        """Should convert quality recommendations to transform operations."""
        recs = [
            "Address missing values in 'col1'",
            "Remove duplicate rows",
            "Add email validation",
            "Standardize phone number format",
            "Investigate negative values",
            "Review outliers in 'metric'",
        ]
        transforms = self.builder._recommendations_to_transforms(recs, {})

        assert "fill_missing_values" in transforms
        assert "remove_duplicates" in transforms
        assert "validate_email_format" in transforms
        assert "standardize_phone_format" in transforms
        assert "handle_negative_values" in transforms
        assert "cap_outliers" in transforms

    def test_recommendations_to_transforms_empty(self):
        """Should return empty list for no recommendations."""
        transforms = self.builder._recommendations_to_transforms([], {})
        assert transforms == []

    def test_detect_join_keys(self):
        """Should detect shared _id columns across profiles."""
        profiles = [
            self._make_profile("a.csv", columns={"customer_id": {}, "name": {}}),
            self._make_profile("b.csv", columns={"customer_id": {}, "amount": {}}),
        ]
        keys = self.builder._detect_join_keys(profiles)
        assert "customer_id" in keys

    def test_detect_join_keys_no_shared(self):
        """Should return empty when no shared keys exist."""
        profiles = [
            self._make_profile("a.csv", columns={"user_id": {}}),
            self._make_profile("b.csv", columns={"product_id": {}}),
        ]
        keys = self.builder._detect_join_keys(profiles)
        assert len(keys) == 0

    def test_safe_name(self):
        """Should normalize file names for safe identifiers."""
        assert self.builder._safe_name("path/to/My File.csv") == "my_file"
        assert self.builder._safe_name("data-source.json") == "data_source"
        assert self.builder._safe_name("simple.csv") == "simple"

    def test_execution_config(self):
        """Should set execution config with retries and timeout."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        exec_config = config["execution_config"]
        assert exec_config["retry_policy"]["max_retries"] == 3
        assert exec_config["timeout_minutes"] == 60

    def test_generate_pipeline_code(self):
        """Should generate Python pipeline code."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]
        self.builder.generate_pipeline_from_sources(profiles, qr, {})

        code = self.builder.generate_pipeline_code()

        assert "class GeneratedPipeline:" in code
        assert "def run(self):" in code
        assert "import pandas as pd" in code

    def test_generate_report(self):
        """Should generate a formatted pipeline report."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv")]
        self.builder.generate_pipeline_from_sources(profiles, qr, {})

        report = self.builder.generate_report()

        assert "AUTOMATED PIPELINE DESIGN" in report
        assert "zero_touch_etl_pipeline" in report

    def test_generate_report_empty(self):
        """Should handle report generation when no pipeline exists."""
        report = self.builder.generate_report()
        assert "No pipeline generated yet" in report

    def test_quality_clean_step_with_recommendations(self):
        """Should add clean step when quality report has actionable recommendations."""
        profiles = [self._make_profile("data.csv")]
        qr = [self._make_quality_report("data.csv", recommendations=["Address missing values in 'col1'"])]

        config = self.builder.generate_pipeline_from_sources(profiles, qr, {})

        clean_steps = [s for s in config["steps"] if "clean" in s["name"]]
        assert len(clean_steps) == 1
