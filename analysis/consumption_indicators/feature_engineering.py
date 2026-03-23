"""Feature engineering for account-level consumption time series.

Computes a rich set of consumption features per account, including
rolling statistics, growth rates, velocity, breadth, concentration,
burstiness, trend coefficients, and cohort percentile ranks.
"""

from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

from .config import PipelineConfig


def engineer_features(
    df: pd.DataFrame,
    config: PipelineConfig,
) -> pd.DataFrame:
    """Compute all consumption features per account.

    Args:
        df: Raw consumption data with columns defined in config.
        config: Pipeline configuration.

    Returns:
        DataFrame indexed by account_id with engineered feature columns.
    """
    tc = config.time_windows
    src = config.data_source

    acct_col = src.account_id_col
    ts_col = src.timestamp_col
    metric_col = src.metric_name_col
    value_col = src.metric_value_col

    # Exclude the future evaluation period so features don't leak the label
    max_date = df[ts_col].max()
    cutoff = max_date - pd.Timedelta(days=tc.growth_evaluation_period_days)
    hist = df[df[ts_col] < cutoff].copy()

    accounts = hist[acct_col].unique()
    feature_frames: list[pd.DataFrame] = []

    for acct_id in accounts:
        acct_data = hist[hist[acct_col] == acct_id]
        features = _compute_account_features(
            acct_data,
            acct_id,
            ts_col=ts_col,
            metric_col=metric_col,
            value_col=value_col,
            rolling_windows=tc.rolling_windows_days,
            seasonality_period=tc.seasonality_period_days,
        )
        feature_frames.append(features)

    if not feature_frames:
        return pd.DataFrame()

    feature_df = pd.DataFrame(feature_frames)
    feature_df = feature_df.set_index(acct_col)

    # Percentile rank of total consumption vs. cohort
    if "total_consumption" in feature_df.columns:
        feature_df["consumption_percentile_rank"] = feature_df[
            "total_consumption"
        ].rank(pct=True)

    # Fill any remaining NaN with 0 for modeling
    feature_df = feature_df.fillna(0.0)

    return feature_df


def _compute_account_features(
    acct_data: pd.DataFrame,
    acct_id: str,
    ts_col: str,
    metric_col: str,
    value_col: str,
    rolling_windows: list[int],
    seasonality_period: int,
) -> dict[str, object]:
    """Compute all features for a single account.

    Args:
        acct_data: Consumption records for one account.
        acct_id: The account identifier.
        ts_col: Timestamp column name.
        metric_col: Metric name column name.
        value_col: Metric value column name.
        rolling_windows: List of window sizes in days for rolling calculations.
        seasonality_period: Period in days for seasonality adjustment.

    Returns:
        Dictionary of feature name to value.
    """
    features: dict[str, object] = {"account_id": acct_id}

    # Aggregate daily total consumption across all metrics
    daily = (
        acct_data.groupby(pd.Grouper(key=ts_col, freq="D"))[value_col]
        .sum()
        .fillna(0)
    )
    if daily.empty:
        return features

    # Ensure continuous date range
    full_range = pd.date_range(start=daily.index.min(), end=daily.index.max(), freq="D")
    daily = daily.reindex(full_range, fill_value=0.0)

    # --- Usage volume features ---
    features["total_consumption"] = float(daily.sum())
    features["mean_daily_consumption"] = float(daily.mean())
    features["median_daily_consumption"] = float(daily.median())
    features["std_daily_consumption"] = float(daily.std()) if len(daily) > 1 else 0.0
    features["max_daily_consumption"] = float(daily.max())
    features["min_daily_consumption"] = float(daily.min())

    # --- Rolling averages ---
    for window in rolling_windows:
        if len(daily) >= window:
            rolling_mean = daily.rolling(window=window).mean()
            features[f"rolling_avg_{window}d"] = float(rolling_mean.iloc[-1])
            features[f"rolling_std_{window}d"] = float(
                daily.rolling(window=window).std().iloc[-1]
            )
        else:
            features[f"rolling_avg_{window}d"] = float(daily.mean())
            features[f"rolling_std_{window}d"] = float(daily.std()) if len(daily) > 1 else 0.0

    # --- Growth rates ---
    weekly = daily.resample("W").sum()
    if len(weekly) >= 2:
        features["wow_growth_rate"] = _safe_pct_change(
            float(weekly.iloc[-2]), float(weekly.iloc[-1])
        )
    else:
        features["wow_growth_rate"] = 0.0

    monthly = daily.resample("ME").sum()
    if len(monthly) >= 2:
        features["mom_growth_rate"] = _safe_pct_change(
            float(monthly.iloc[-2]), float(monthly.iloc[-1])
        )
    else:
        features["mom_growth_rate"] = 0.0

    # Average growth over multiple periods
    if len(weekly) >= 4:
        weekly_changes = weekly.pct_change().dropna()
        weekly_changes = weekly_changes.replace([np.inf, -np.inf], np.nan).dropna()
        features["avg_weekly_growth_rate"] = (
            float(weekly_changes.mean()) if len(weekly_changes) > 0 else 0.0
        )
    else:
        features["avg_weekly_growth_rate"] = 0.0

    # --- Consumption velocity and acceleration ---
    if len(daily) >= 7:
        velocity = daily.diff().dropna()
        features["consumption_velocity"] = float(velocity.mean())
        features["consumption_velocity_std"] = float(velocity.std()) if len(velocity) > 1 else 0.0

        if len(velocity) >= 7:
            acceleration = velocity.diff().dropna()
            features["consumption_acceleration"] = float(acceleration.mean())
        else:
            features["consumption_acceleration"] = 0.0
    else:
        features["consumption_velocity"] = 0.0
        features["consumption_velocity_std"] = 0.0
        features["consumption_acceleration"] = 0.0

    # --- Usage breadth ---
    n_distinct_metrics = acct_data[metric_col].nunique()
    features["usage_breadth"] = n_distinct_metrics

    # Breadth over recent 30 days
    recent_30d = acct_data[
        acct_data[ts_col] >= (acct_data[ts_col].max() - pd.Timedelta(days=30))
    ]
    features["usage_breadth_recent_30d"] = recent_30d[metric_col].nunique()

    # --- Usage concentration (Herfindahl-Hirschman Index and Gini) ---
    metric_totals = acct_data.groupby(metric_col)[value_col].sum()
    if metric_totals.sum() > 0:
        shares = metric_totals / metric_totals.sum()
        features["hhi_concentration"] = float((shares**2).sum())
        features["gini_concentration"] = _gini_coefficient(metric_totals.values)
    else:
        features["hhi_concentration"] = 1.0
        features["gini_concentration"] = 0.0

    # --- Peak-to-average ratio (burstiness) ---
    mean_val = daily.mean()
    if mean_val > 0:
        features["peak_to_avg_ratio"] = float(daily.max() / mean_val)
    else:
        features["peak_to_avg_ratio"] = 0.0

    # --- Trend coefficients (linear regression slope over rolling windows) ---
    for window in rolling_windows:
        if len(daily) >= window:
            recent = daily.iloc[-window:]
            x = np.arange(len(recent), dtype=float)
            y = recent.values.astype(float)
            if np.std(y) > 0:
                slope, _, r_value, _, _ = scipy_stats.linregress(x, y)
                features[f"trend_slope_{window}d"] = float(slope)
                features[f"trend_r2_{window}d"] = float(r_value**2)
            else:
                features[f"trend_slope_{window}d"] = 0.0
                features[f"trend_r2_{window}d"] = 0.0
        else:
            features[f"trend_slope_{window}d"] = 0.0
            features[f"trend_r2_{window}d"] = 0.0

    # --- Seasonality-adjusted consumption ---
    if len(daily) >= seasonality_period * 4:
        seasonal_means = pd.Series(
            [
                daily.iloc[i::seasonality_period].mean()
                for i in range(seasonality_period)
            ]
        )
        overall_mean = daily.mean()
        if overall_mean > 0:
            seasonal_factors = seasonal_means / overall_mean
            seasonal_strength = float(seasonal_factors.std())
            features["seasonality_strength"] = seasonal_strength

            deseasonalized = daily.copy()
            for i in range(len(deseasonalized)):
                factor = seasonal_factors.iloc[i % seasonality_period]
                if factor > 0:
                    deseasonalized.iloc[i] = deseasonalized.iloc[i] / factor
            features["deseasonalized_trend_slope"] = _compute_slope(deseasonalized)
        else:
            features["seasonality_strength"] = 0.0
            features["deseasonalized_trend_slope"] = 0.0
    else:
        features["seasonality_strength"] = 0.0
        features["deseasonalized_trend_slope"] = 0.0

    # --- Days since last consumption spike ---
    if len(daily) > 0 and daily.std() > 0:
        threshold = daily.mean() + 2 * daily.std()
        spikes = daily[daily > threshold]
        if len(spikes) > 0:
            days_since_spike = (daily.index[-1] - spikes.index[-1]).days
            features["days_since_last_spike"] = days_since_spike
        else:
            features["days_since_last_spike"] = len(daily)
    else:
        features["days_since_last_spike"] = len(daily) if len(daily) > 0 else 0

    # --- Usage frequency ---
    active_days = (daily > 0).sum()
    total_days = len(daily)
    features["active_day_ratio"] = float(active_days / total_days) if total_days > 0 else 0.0

    # Consecutive active days (streak)
    if len(daily) > 0:
        is_active = (daily > 0).astype(int)
        streaks = is_active.groupby((is_active != is_active.shift()).cumsum()).sum()
        active_streaks = streaks[streaks > 0]
        features["max_active_streak"] = int(active_streaks.max()) if len(active_streaks) > 0 else 0
        features["current_streak"] = (
            int(active_streaks.iloc[-1])
            if len(active_streaks) > 0 and is_active.iloc[-1] == 1
            else 0
        )
    else:
        features["max_active_streak"] = 0
        features["current_streak"] = 0

    # --- Coefficient of variation ---
    if mean_val > 0:
        features["coefficient_of_variation"] = float(daily.std() / mean_val)
    else:
        features["coefficient_of_variation"] = 0.0

    # --- Recent vs. historical ratio ---
    if len(daily) >= 60:
        recent_30 = daily.iloc[-30:].mean()
        historical = daily.iloc[:-30].mean()
        if historical > 0:
            features["recent_to_historical_ratio"] = float(recent_30 / historical)
        else:
            features["recent_to_historical_ratio"] = 0.0
    else:
        features["recent_to_historical_ratio"] = 0.0

    return features


def _safe_pct_change(old: float, new: float) -> float:
    """Compute percentage change safely, avoiding division by zero."""
    if old == 0:
        return 0.0
    return (new - old) / abs(old) * 100


def _gini_coefficient(values: np.ndarray) -> float:
    """Compute the Gini coefficient for a set of values."""
    values = np.sort(np.abs(values.astype(float)))
    n = len(values)
    if n == 0 or values.sum() == 0:
        return 0.0
    index = np.arange(1, n + 1)
    return float((2 * np.sum(index * values) - (n + 1) * np.sum(values)) / (n * np.sum(values)))


def _compute_slope(series: pd.Series) -> float:
    """Compute the linear regression slope of a time series."""
    if len(series) < 2:
        return 0.0
    x = np.arange(len(series), dtype=float)
    y = series.values.astype(float)
    if np.std(y) == 0:
        return 0.0
    slope, _, _, _, _ = scipy_stats.linregress(x, y)
    return float(slope)
