"""Visualizations for consumption indicator analysis results.

Generates bar charts, SHAP summary plots, time-series comparisons,
and correlation heatmaps. Saves all plots as PNG files.
"""

import os
from typing import Any, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


def generate_all_visualizations(
    results: dict[str, Any],
    features: pd.DataFrame,
    labels: pd.Series,
    raw_data: pd.DataFrame,
    output_dir: str,
    top_n: int = 10,
    account_id_col: str = "account_id",
    timestamp_col: str = "timestamp",
    metric_value_col: str = "metric_value",
) -> list[str]:
    """Generate all visualizations and save to output directory.

    Args:
        results: Output from run_indicator_analysis.
        features: Feature matrix.
        labels: Growth labels.
        raw_data: Original consumption data for time-series plots.
        output_dir: Directory to save PNG files.
        top_n: Number of top indicators to show in plots.
        account_id_col: Account ID column name.
        timestamp_col: Timestamp column name.
        metric_value_col: Metric value column name.

    Returns:
        List of file paths for generated plots.
    """
    os.makedirs(output_dir, exist_ok=True)
    saved_files: list[str] = []

    consolidated = results["consolidated"]
    top_indicators = consolidated.head(top_n)

    # 1. Bar chart of top indicators
    path = _plot_top_indicators_bar(top_indicators, output_dir)
    saved_files.append(path)

    # 2. SHAP summary plot
    shap_values = results.get("shap_values")
    X_test = results.get("X_test")
    if shap_values is not None and X_test is not None:
        path = _plot_shap_summary(
            shap_values, X_test, results["feature_names"], output_dir
        )
        if path:
            saved_files.append(path)

    # 3. Time-series comparison of top 3 indicators
    top_3_features = list(top_indicators["feature"].head(3))
    path = _plot_timeseries_comparison(
        top_3_features,
        features,
        labels,
        raw_data,
        output_dir,
        account_id_col=account_id_col,
        timestamp_col=timestamp_col,
        metric_value_col=metric_value_col,
    )
    saved_files.append(path)

    # 4. Correlation heatmap of top indicators
    top_feature_names = list(top_indicators["feature"].head(top_n))
    path = _plot_correlation_heatmap(features, top_feature_names, output_dir)
    saved_files.append(path)

    return saved_files


def _plot_top_indicators_bar(
    top_indicators: pd.DataFrame, output_dir: str
) -> str:
    """Bar chart of top indicators ranked by composite importance score."""
    fig, ax = plt.subplots(figsize=(12, 7))

    colors = [
        "#2ecc71" if d == "positive" else "#e74c3c" if d == "negative" else "#95a5a6"
        for d in top_indicators["direction"]
    ]

    bars = ax.barh(
        range(len(top_indicators)),
        top_indicators["composite_score"],
        color=colors,
        edgecolor="white",
        linewidth=0.5,
    )

    ax.set_yticks(range(len(top_indicators)))
    ax.set_yticklabels(top_indicators["feature"], fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Composite Importance Score", fontsize=12)
    ax.set_title(
        "Top Leading Indicators of Account Consumption Growth",
        fontsize=14,
        fontweight="bold",
    )

    # Add significance markers
    for i, (_, row) in enumerate(top_indicators.iterrows()):
        marker = " *" if row["significant"] else ""
        ax.text(
            row["composite_score"] + 0.01,
            i,
            f'{row["composite_score"]:.3f}{marker}',
            va="center",
            fontsize=9,
        )

    legend_elements = [
        plt.Rectangle((0, 0), 1, 1, fc="#2ecc71", label="Positive direction"),
        plt.Rectangle((0, 0), 1, 1, fc="#e74c3c", label="Negative direction"),
        plt.Rectangle((0, 0), 1, 1, fc="#95a5a6", label="Neutral"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=9)

    plt.tight_layout()
    path = os.path.join(output_dir, "top_indicators_bar.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _plot_shap_summary(
    shap_values: np.ndarray,
    X_test: pd.DataFrame,
    feature_names: list[str],
    output_dir: str,
) -> Optional[str]:
    """SHAP beeswarm summary plot showing feature impact distribution."""
    try:
        import shap

        fig, ax = plt.subplots(figsize=(12, 8))
        shap.summary_plot(
            shap_values,
            X_test,
            feature_names=feature_names,
            show=False,
            max_display=20,
        )
        plt.title(
            "SHAP Feature Impact on Growth Prediction",
            fontsize=14,
            fontweight="bold",
        )
        plt.tight_layout()
        path = os.path.join(output_dir, "shap_summary.png")
        plt.savefig(path, dpi=150, bbox_inches="tight")
        plt.close("all")
        return path
    except ImportError:
        print("  WARNING: shap not available, skipping SHAP summary plot.")
        return None
    except Exception as e:
        print(f"  WARNING: SHAP summary plot failed: {e}")
        return None


def _plot_timeseries_comparison(
    top_features: list[str],
    features: pd.DataFrame,
    labels: pd.Series,
    raw_data: pd.DataFrame,
    output_dir: str,
    account_id_col: str = "account_id",
    timestamp_col: str = "timestamp",
    metric_value_col: str = "metric_value",
) -> str:
    """Time-series plots of top indicators comparing growth vs non-growth cohorts."""
    common_idx = features.index.intersection(labels.index)
    growth_accounts = labels.loc[common_idx][labels.loc[common_idx] == 1].index
    no_growth_accounts = labels.loc[common_idx][labels.loc[common_idx] == 0].index

    n_features = len(top_features)
    fig, axes = plt.subplots(n_features, 1, figsize=(14, 5 * n_features))
    if n_features == 1:
        axes = [axes]

    # For time-series comparison, aggregate daily consumption by cohort
    raw_data = raw_data.copy()
    raw_data["cohort"] = "unknown"
    raw_data.loc[
        raw_data[account_id_col].isin(growth_accounts), "cohort"
    ] = "growth"
    raw_data.loc[
        raw_data[account_id_col].isin(no_growth_accounts), "cohort"
    ] = "no_growth"
    raw_data = raw_data[raw_data["cohort"] != "unknown"]

    for idx, feature_name in enumerate(top_features):
        ax = axes[idx]

        # Plot weekly average consumption per cohort
        for cohort, color, label in [
            ("growth", "#2ecc71", "Growth Accounts"),
            ("no_growth", "#e74c3c", "Non-Growth Accounts"),
        ]:
            cohort_data = raw_data[raw_data["cohort"] == cohort]
            weekly = (
                cohort_data.groupby(
                    pd.Grouper(key=timestamp_col, freq="W")
                )[metric_value_col]
                .mean()
            )
            ax.plot(weekly.index, weekly.values, color=color, label=label, alpha=0.8)

        ax.set_title(
            f"Feature: {feature_name}",
            fontsize=12,
            fontweight="bold",
        )
        ax.set_xlabel("Date")
        ax.set_ylabel("Avg Weekly Consumption")
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

        # Add feature value distributions as inset text
        growth_vals = features.loc[
            features.index.isin(growth_accounts), feature_name
        ]
        no_growth_vals = features.loc[
            features.index.isin(no_growth_accounts), feature_name
        ]
        if len(growth_vals) > 0 and len(no_growth_vals) > 0:
            text = (
                f"Growth median: {growth_vals.median():.2f}\n"
                f"Non-growth median: {no_growth_vals.median():.2f}"
            )
            ax.text(
                0.02, 0.95, text, transform=ax.transAxes,
                fontsize=8, verticalalignment="top",
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
            )

    fig.suptitle(
        "Weekly Consumption: Growth vs Non-Growth Cohorts",
        fontsize=14,
        fontweight="bold",
        y=1.01,
    )
    plt.tight_layout()
    path = os.path.join(output_dir, "timeseries_comparison.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _plot_correlation_heatmap(
    features: pd.DataFrame,
    top_feature_names: list[str],
    output_dir: str,
) -> str:
    """Correlation heatmap of top indicators to identify redundancy."""
    available = [f for f in top_feature_names if f in features.columns]
    subset = features[available]
    corr_matrix = subset.corr(method="spearman")

    fig, ax = plt.subplots(figsize=(12, 10))

    mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)

    sns.heatmap(
        corr_matrix,
        mask=mask,
        annot=True,
        fmt=".2f",
        cmap="RdBu_r",
        center=0,
        vmin=-1,
        vmax=1,
        square=True,
        linewidths=0.5,
        ax=ax,
        cbar_kws={"shrink": 0.8},
    )

    ax.set_title(
        "Correlation Between Top Indicators\n(identifying redundancy)",
        fontsize=14,
        fontweight="bold",
    )

    plt.tight_layout()
    path = os.path.join(output_dir, "correlation_heatmap.png")
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path
