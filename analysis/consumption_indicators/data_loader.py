"""Data loader for account-level consumption data.

Supports loading from CSV files and includes a synthetic data generator
for demonstration purposes that creates realistic consumption patterns.
"""

import os
from typing import Optional

import numpy as np
import pandas as pd

from .config import DataSourceConfig, PipelineConfig


def load_consumption_data(config: PipelineConfig) -> pd.DataFrame:
    """Load account-level consumption data from the configured source.

    Expects columns: account_id, timestamp, metric_name, metric_value
    (column names are configurable via DataSourceConfig).

    Args:
        config: Pipeline configuration with data source settings.

    Returns:
        DataFrame with consumption data.
    """
    if config.use_synthetic_data:
        return generate_synthetic_data(
            n_accounts=config.synthetic_n_accounts,
            n_months=config.synthetic_months,
            random_state=config.analysis.random_state,
        )

    src = config.data_source

    if src.source_type == "csv":
        return _load_from_csv(src)
    elif src.source_type == "database":
        return _load_from_database(src)
    elif src.source_type == "api":
        raise NotImplementedError(
            "API data source loading is not yet implemented. "
            "Please use CSV or database sources."
        )
    else:
        raise ValueError(f"Unsupported source type: {src.source_type}")


def _load_from_csv(src: DataSourceConfig) -> pd.DataFrame:
    """Load consumption data from a CSV file."""
    if src.csv_path is None:
        raise ValueError("csv_path must be set for CSV data source.")
    if not os.path.exists(src.csv_path):
        raise FileNotFoundError(f"CSV file not found: {src.csv_path}")

    df = pd.read_csv(src.csv_path)

    required_cols = [
        src.account_id_col,
        src.timestamp_col,
        src.metric_name_col,
        src.metric_value_col,
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in CSV: {missing}. "
            f"Expected: {required_cols}"
        )

    df[src.timestamp_col] = pd.to_datetime(df[src.timestamp_col])
    return df


def _load_from_database(src: DataSourceConfig) -> pd.DataFrame:
    """Load consumption data from a database connection."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 is required for database connections. "
            "Install with: pip install psycopg2-binary"
        )

    if not all([src.db_host, src.db_name, src.db_user, src.db_password]):
        raise ValueError(
            "Database connection requires db_host, db_name, db_user, and db_password."
        )

    query = src.db_query or (
        f"SELECT {src.account_id_col}, {src.timestamp_col}, "
        f"{src.metric_name_col}, {src.metric_value_col} "
        f"FROM consumption_data ORDER BY {src.timestamp_col}"
    )

    conn = psycopg2.connect(
        host=src.db_host,
        port=src.db_port or 5432,
        dbname=src.db_name,
        user=src.db_user,
        password=src.db_password,
    )
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    df[src.timestamp_col] = pd.to_datetime(df[src.timestamp_col])
    return df


def generate_synthetic_data(
    n_accounts: int = 500,
    n_months: int = 12,
    random_state: int = 42,
    growth_fraction: float = 0.35,
) -> pd.DataFrame:
    """Generate synthetic consumption data with realistic patterns.

    Creates data for multiple accounts over a specified period with known
    growth outcomes. Growth accounts exhibit increasing consumption trends,
    while non-growth accounts show flat or declining patterns.

    Args:
        n_accounts: Number of accounts to generate.
        n_months: Duration of data in months.
        random_state: Random seed for reproducibility.
        growth_fraction: Fraction of accounts that will be growth accounts.

    Returns:
        DataFrame with columns: account_id, timestamp, metric_name, metric_value.
    """
    rng = np.random.RandomState(random_state)

    n_growth = int(n_accounts * growth_fraction)
    n_no_growth = n_accounts - n_growth

    account_ids = [f"ACCT-{i:04d}" for i in range(n_accounts)]
    growth_labels = [True] * n_growth + [False] * n_no_growth
    rng.shuffle(growth_labels)

    metric_names = [
        "api_calls",
        "data_processed_gb",
        "active_users",
        "feature_a_usage",
        "feature_b_usage",
        "feature_c_usage",
        "feature_d_usage",
        "storage_used_gb",
        "compute_hours",
        "report_generations",
    ]

    end_date = pd.Timestamp("2025-12-31")
    start_date = end_date - pd.DateOffset(months=n_months)
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    records: list[dict[str, object]] = []

    for acct_idx, (acct_id, is_growth) in enumerate(
        zip(account_ids, growth_labels)
    ):
        base_volume = rng.uniform(50, 500)
        n_metrics_used = rng.randint(4, len(metric_names) + 1)
        used_metrics = list(
            rng.choice(metric_names, size=n_metrics_used, replace=False)
        )

        if is_growth:
            daily_trend = rng.uniform(0.001, 0.005)
            late_boost = rng.uniform(1.2, 2.0)
        else:
            daily_trend = rng.uniform(-0.002, 0.001)
            late_boost = rng.uniform(0.7, 1.1)

        for metric in used_metrics:
            metric_base = base_volume * rng.uniform(0.3, 1.5)

            if metric == "active_users":
                metric_base = rng.uniform(5, 100)
            elif metric == "storage_used_gb":
                metric_base = rng.uniform(10, 200)
            elif metric == "compute_hours":
                metric_base = rng.uniform(20, 300)

            for day_idx, date in enumerate(dates):
                day_of_week = date.dayofweek
                weekend_factor = 0.3 if day_of_week >= 5 else 1.0

                trend_factor = 1.0 + daily_trend * day_idx

                fraction_through = day_idx / len(dates)
                if fraction_through > 0.7:
                    trend_factor *= late_boost

                seasonal = 1.0 + 0.1 * np.sin(2 * np.pi * day_idx / 7)

                noise = rng.normal(1.0, 0.15)

                spike = 1.0
                if rng.random() < 0.02:
                    spike = rng.uniform(2.0, 5.0)

                value = (
                    metric_base
                    * trend_factor
                    * seasonal
                    * weekend_factor
                    * noise
                    * spike
                )
                value = max(0, value)

                if metric == "active_users":
                    value = int(round(value))

                records.append(
                    {
                        "account_id": acct_id,
                        "timestamp": date,
                        "metric_name": metric,
                        "metric_value": round(value, 2),
                    }
                )

    df = pd.DataFrame(records)
    df = df.sort_values(["account_id", "metric_name", "timestamp"]).reset_index(
        drop=True
    )

    return df


def get_account_growth_labels(
    df: pd.DataFrame,
    evaluation_period_days: int = 90,
    growth_threshold_pct: float = 10.0,
    account_id_col: str = "account_id",
    timestamp_col: str = "timestamp",
    metric_value_col: str = "metric_value",
) -> pd.DataFrame:
    """Label accounts as growth or no-growth based on consumption changes.

    Compares total consumption in the last `evaluation_period_days` to the
    preceding period of equal length.

    Args:
        df: Consumption data.
        evaluation_period_days: Number of days for the evaluation window.
        growth_threshold_pct: Minimum percentage increase to label as growth.
        account_id_col: Column name for account identifiers.
        timestamp_col: Column name for timestamps.
        metric_value_col: Column name for metric values.

    Returns:
        DataFrame with columns: account_id, growth_label (1 = growth, 0 = no growth),
        pct_change.
    """
    max_date = df[timestamp_col].max()
    future_start = max_date - pd.Timedelta(days=evaluation_period_days)
    baseline_start = future_start - pd.Timedelta(days=evaluation_period_days)

    baseline_mask = (df[timestamp_col] >= baseline_start) & (
        df[timestamp_col] < future_start
    )
    future_mask = df[timestamp_col] >= future_start

    baseline_totals = (
        df[baseline_mask]
        .groupby(account_id_col)[metric_value_col]
        .sum()
        .rename("baseline_total")
    )
    future_totals = (
        df[future_mask]
        .groupby(account_id_col)[metric_value_col]
        .sum()
        .rename("future_total")
    )

    labels = pd.concat([baseline_totals, future_totals], axis=1).fillna(0)
    labels["pct_change"] = np.where(
        labels["baseline_total"] > 0,
        (labels["future_total"] - labels["baseline_total"])
        / labels["baseline_total"]
        * 100,
        0.0,
    )
    labels["growth_label"] = (
        labels["pct_change"] >= growth_threshold_pct
    ).astype(int)

    return labels[["growth_label", "pct_change"]].reset_index()
