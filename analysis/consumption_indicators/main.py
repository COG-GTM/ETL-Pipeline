"""Main orchestrator for the consumption indicators analysis pipeline.

Runs the full pipeline: load data -> engineer features -> run indicator
analysis -> generate visualizations -> output summary report.
"""

import os
import sys
import time

import pandas as pd

from .config import PipelineConfig
from .data_loader import get_account_growth_labels, load_consumption_data
from .feature_engineering import engineer_features
from .indicator_analysis import run_indicator_analysis
from .visualizations import generate_all_visualizations


def run_pipeline(config: PipelineConfig | None = None) -> dict:
    """Execute the full consumption indicators analysis pipeline.

    Args:
        config: Pipeline configuration. Uses defaults if not provided.

    Returns:
        Dictionary containing all results from the analysis.
    """
    if config is None:
        config = PipelineConfig()

    output_dir = config.output_dir
    os.makedirs(output_dir, exist_ok=True)

    start_time = time.time()

    # Step 1: Load data
    _log("Step 1: Loading consumption data...", config.verbose)
    raw_data = load_consumption_data(config)
    _log(
        f"  Loaded {len(raw_data):,} records for "
        f"{raw_data[config.data_source.account_id_col].nunique()} accounts",
        config.verbose,
    )

    # Step 2: Engineer features
    _log("Step 2: Engineering time-series features...", config.verbose)
    features = engineer_features(raw_data, config)
    _log(
        f"  Generated {len(features.columns)} features for "
        f"{len(features)} accounts",
        config.verbose,
    )

    # Step 3: Compute growth labels
    _log("Step 3: Computing growth labels...", config.verbose)
    label_df = get_account_growth_labels(
        raw_data,
        evaluation_period_days=config.time_windows.growth_evaluation_period_days,
        growth_threshold_pct=config.time_windows.growth_threshold_pct,
        account_id_col=config.data_source.account_id_col,
        timestamp_col=config.data_source.timestamp_col,
        metric_value_col=config.data_source.metric_value_col,
    )
    label_series = label_df.set_index(config.data_source.account_id_col)[
        "growth_label"
    ]

    n_growth = int(label_series.sum())
    n_total = len(label_series)
    _log(
        f"  Growth accounts: {n_growth}/{n_total} "
        f"({n_growth / n_total * 100:.1f}%)",
        config.verbose,
    )

    # Step 4: Run indicator analysis
    _log("Step 4: Running indicator analysis...", config.verbose)
    results = run_indicator_analysis(
        features, label_series, config.analysis, verbose=config.verbose
    )

    model_metrics = results.get("model_metrics", {})
    _log(
        f"  Model accuracy: {model_metrics.get('accuracy', 0):.3f}, "
        f"ROC AUC: {model_metrics.get('roc_auc', 0):.3f}",
        config.verbose,
    )

    # Step 5: Generate visualizations
    _log("Step 5: Generating visualizations...", config.verbose)
    plot_files = generate_all_visualizations(
        results=results,
        features=features,
        labels=label_series,
        raw_data=raw_data,
        output_dir=output_dir,
        top_n=config.analysis.top_n_indicators,
        account_id_col=config.data_source.account_id_col,
        timestamp_col=config.data_source.timestamp_col,
        metric_value_col=config.data_source.metric_value_col,
    )
    _log(f"  Saved {len(plot_files)} visualizations to {output_dir}/", config.verbose)

    # Step 6: Output summary report
    _log("Step 6: Generating summary report...", config.verbose)
    consolidated = results["consolidated"]
    top_n = config.analysis.top_n_indicators
    report_df = consolidated.head(top_n)[
        [
            "rank",
            "feature",
            "composite_score",
            "direction",
            "p_value",
            "significant",
            "correlation_raw",
            "mutual_info_raw",
            "gbi_importance_raw",
            "shap_importance_raw",
            "permutation_importance_raw",
        ]
    ].copy()

    report_path = os.path.join(output_dir, "consumption_indicators_report.csv")
    report_df.to_csv(report_path, index=False)
    _log(f"  Saved report to {report_path}", config.verbose)

    # Print summary table
    elapsed = time.time() - start_time
    _print_summary(report_df, model_metrics, elapsed, config.verbose)

    return {
        "raw_data": raw_data,
        "features": features,
        "labels": label_series,
        "results": results,
        "report": report_df,
        "plot_files": plot_files,
    }


def _print_summary(
    report_df: pd.DataFrame,
    model_metrics: dict,
    elapsed: float,
    verbose: bool,
) -> None:
    """Print a formatted summary table to the console."""
    if not verbose:
        return

    print("\n" + "=" * 80)
    print("  CONSUMPTION GROWTH LEADING INDICATORS - SUMMARY")
    print("=" * 80)
    print(
        f"  Model Performance: Accuracy={model_metrics.get('accuracy', 0):.3f}, "
        f"ROC AUC={model_metrics.get('roc_auc', 0):.3f}"
    )
    print(f"  Analysis completed in {elapsed:.1f}s")
    print("-" * 80)
    print(
        f"  {'Rank':<6}{'Feature':<40}{'Score':<10}{'Direction':<12}{'Sig.':<6}"
    )
    print("-" * 80)

    for _, row in report_df.iterrows():
        sig = "*" if row["significant"] else ""
        print(
            f"  {int(row['rank']):<6}"
            f"{row['feature']:<40}"
            f"{row['composite_score']:<10.4f}"
            f"{row['direction']:<12}"
            f"{sig:<6}"
        )

    print("-" * 80)
    print("  * = statistically significant (p < 0.05)")
    print("=" * 80 + "\n")


def _log(message: str, verbose: bool) -> None:
    """Print a log message if verbose mode is enabled."""
    if verbose:
        print(message)


if __name__ == "__main__":
    config = PipelineConfig()

    # Allow overriding output dir via command line
    if len(sys.argv) > 1:
        config.output_dir = sys.argv[1]

    run_pipeline(config)
