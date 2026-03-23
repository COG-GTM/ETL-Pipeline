"""Configuration for the consumption indicators analysis module.

Defines data source schemas, configurable parameters, and consumption
metric categories used throughout the analysis pipeline.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class DataSourceConfig:
    """Configuration for consumption data sources."""

    source_type: str = "csv"  # "csv", "database", or "api"

    # CSV source settings
    csv_path: Optional[str] = None

    # Database source settings
    db_host: Optional[str] = None
    db_port: Optional[int] = None
    db_name: Optional[str] = None
    db_user: Optional[str] = None
    db_password: Optional[str] = None
    db_query: Optional[str] = None

    # API source settings
    api_url: Optional[str] = None
    api_key: Optional[str] = None
    api_headers: Optional[dict[str, str]] = None

    # Required columns in the consumption data
    account_id_col: str = "account_id"
    timestamp_col: str = "timestamp"
    metric_name_col: str = "metric_name"
    metric_value_col: str = "metric_value"


@dataclass
class TimeWindowConfig:
    """Configuration for time-based analysis windows."""

    rolling_windows_days: list[int] = field(
        default_factory=lambda: [7, 30, 60, 90]
    )
    growth_evaluation_period_days: int = 90
    growth_threshold_pct: float = 10.0  # >10% consumption increase = "growth"
    min_history_days: int = 90  # Minimum history required per account
    seasonality_period_days: int = 7  # Weekly seasonality by default


@dataclass
class AnalysisConfig:
    """Configuration for indicator analysis methods."""

    top_n_indicators: int = 10
    test_size: float = 0.25
    random_state: int = 42
    n_estimators: int = 200
    max_depth: int = 5
    n_permutation_repeats: int = 10
    correlation_method: str = "spearman"
    significance_level: float = 0.05


# Consumption metric categories to analyze
METRIC_CATEGORIES: dict[str, str] = {
    "usage_volume": "Total consumption volume (API calls, data processed, etc.)",
    "usage_frequency": "How often consumption occurs (daily active sessions, etc.)",
    "usage_breadth": "Number of distinct features/products/endpoints consumed",
    "usage_intensity": "Peak vs. average consumption patterns (burstiness)",
    "usage_trends": "Acceleration or deceleration of consumption over time",
    "usage_concentration": "How spread consumption is across users/teams (Gini/HHI)",
}


@dataclass
class PipelineConfig:
    """Top-level configuration for the full analysis pipeline."""

    data_source: DataSourceConfig = field(default_factory=DataSourceConfig)
    time_windows: TimeWindowConfig = field(default_factory=TimeWindowConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    output_dir: str = "output"
    use_synthetic_data: bool = True
    synthetic_n_accounts: int = 500
    synthetic_months: int = 12
    verbose: bool = True
