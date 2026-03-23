"""Indicator analysis for identifying leading indicators of account growth.

Implements multiple ranking methods: correlation analysis, mutual information,
gradient boosting feature importance, SHAP values, and permutation importance.
Produces a consolidated ranking that aggregates across all methods.
"""

from typing import Any, Optional

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import StandardScaler

from .config import AnalysisConfig


def run_indicator_analysis(
    features: pd.DataFrame,
    labels: pd.Series,
    config: AnalysisConfig,
    verbose: bool = True,
) -> dict[str, Any]:
    """Run all indicator analysis methods and produce consolidated rankings.

    Args:
        features: Feature matrix (accounts x features).
        labels: Binary growth labels (1 = growth, 0 = no growth).
        config: Analysis configuration.
        verbose: Whether to print progress messages.

    Returns:
        Dictionary containing:
        - rankings: DataFrame with feature rankings from each method
        - consolidated: DataFrame with composite scores and top indicators
        - model: Trained GradientBoostingClassifier
        - model_metrics: Dictionary of model performance metrics
        - shap_values: SHAP values array (if available)
        - feature_names: List of feature names
    """
    # Align features and labels
    common_idx = features.index.intersection(labels.index)
    X = features.loc[common_idx]
    y = labels.loc[common_idx]

    feature_names = list(X.columns)

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=config.test_size, random_state=config.random_state,
        stratify=y,
    )

    results: dict[str, Any] = {"feature_names": feature_names}

    # 1. Correlation analysis
    if verbose:
        print("  Running correlation analysis...")
    corr_scores = _correlation_analysis(
        X, y, method=config.correlation_method
    )

    # 2. Mutual information
    if verbose:
        print("  Running mutual information analysis...")
    mi_scores = _mutual_information(X, y, random_state=config.random_state)

    # 3. Gradient Boosting feature importance
    if verbose:
        print("  Training gradient boosting model...")
    model, model_metrics, gbi_scores = _gradient_boosting_importance(
        X_train, X_test, y_train, y_test, config
    )
    results["model"] = model
    results["model_metrics"] = model_metrics

    # 4. SHAP values
    if verbose:
        print("  Computing SHAP values...")
    shap_scores, shap_values = _shap_analysis(model, X_test, feature_names)
    results["shap_values"] = shap_values
    results["X_test"] = X_test

    # 5. Permutation importance
    if verbose:
        print("  Computing permutation importance...")
    perm_scores = _permutation_importance(
        model, X_test, y_test, config, feature_names
    )

    # Build rankings DataFrame
    rankings = pd.DataFrame(
        {
            "feature": feature_names,
            "correlation": [corr_scores.get(f, 0.0) for f in feature_names],
            "mutual_info": [mi_scores.get(f, 0.0) for f in feature_names],
            "gbi_importance": [gbi_scores.get(f, 0.0) for f in feature_names],
            "shap_importance": [shap_scores.get(f, 0.0) for f in feature_names],
            "permutation_importance": [
                perm_scores.get(f, 0.0) for f in feature_names
            ],
        }
    )

    # Compute directionality from correlation sign
    rankings["direction"] = rankings["correlation"].apply(
        lambda x: "positive" if x > 0 else ("negative" if x < 0 else "neutral")
    )

    # Statistical significance from correlation p-values
    p_values = _correlation_p_values(X, y, method=config.correlation_method)
    rankings["p_value"] = [p_values.get(f, 1.0) for f in feature_names]
    rankings["significant"] = rankings["p_value"] < config.significance_level

    results["rankings"] = rankings

    # Consolidated ranking: normalize each method to [0, 1] then average
    consolidated = _consolidate_rankings(rankings, config.top_n_indicators)
    results["consolidated"] = consolidated

    return results


def _correlation_analysis(
    X: pd.DataFrame, y: pd.Series, method: str = "spearman"
) -> dict[str, float]:
    """Compute correlation of each feature with the growth label."""
    scores: dict[str, float] = {}
    for col in X.columns:
        if X[col].std() == 0:
            scores[col] = 0.0
            continue
        if method == "spearman":
            corr, _ = scipy_stats.spearmanr(X[col], y)
        else:
            corr, _ = scipy_stats.pearsonr(X[col], y)
        scores[col] = float(corr) if not np.isnan(corr) else 0.0
    return scores


def _correlation_p_values(
    X: pd.DataFrame, y: pd.Series, method: str = "spearman"
) -> dict[str, float]:
    """Compute p-values for correlation of each feature with the growth label."""
    p_values: dict[str, float] = {}
    for col in X.columns:
        if X[col].std() == 0:
            p_values[col] = 1.0
            continue
        if method == "spearman":
            _, p = scipy_stats.spearmanr(X[col], y)
        else:
            _, p = scipy_stats.pearsonr(X[col], y)
        p_values[col] = float(p) if not np.isnan(p) else 1.0
    return p_values


def _mutual_information(
    X: pd.DataFrame, y: pd.Series, random_state: int = 42
) -> dict[str, float]:
    """Compute mutual information between each feature and the growth label."""
    mi = mutual_info_classif(
        X, y, discrete_features=False, random_state=random_state
    )
    return dict(zip(X.columns, mi.astype(float)))


def _gradient_boosting_importance(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
    config: AnalysisConfig,
) -> tuple[GradientBoostingClassifier, dict[str, float], dict[str, float]]:
    """Train a gradient boosting classifier and extract feature importances."""
    model = GradientBoostingClassifier(
        n_estimators=config.n_estimators,
        max_depth=config.max_depth,
        random_state=config.random_state,
        subsample=0.8,
        learning_rate=0.1,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "roc_auc": float(roc_auc_score(y_test, y_proba)),
        "n_train": len(X_train),
        "n_test": len(X_test),
    }

    importances = dict(
        zip(X_train.columns, model.feature_importances_.astype(float))
    )
    return model, metrics, importances


def _shap_analysis(
    model: GradientBoostingClassifier,
    X_test: pd.DataFrame,
    feature_names: list[str],
) -> tuple[dict[str, float], Optional[np.ndarray]]:
    """Compute SHAP values for the trained model."""
    try:
        import shap

        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)

        # For binary classification, shap_values may be a list of two arrays
        if isinstance(shap_values, list):
            shap_vals = np.abs(shap_values[1]).mean(axis=0)
            shap_raw = shap_values[1]
        else:
            shap_vals = np.abs(shap_values).mean(axis=0)
            shap_raw = shap_values

        scores = dict(zip(feature_names, shap_vals.astype(float)))
        return scores, shap_raw
    except ImportError:
        print(
            "  WARNING: shap library not available. "
            "Skipping SHAP analysis. Install with: pip install shap"
        )
        return {f: 0.0 for f in feature_names}, None
    except Exception as e:
        print(f"  WARNING: SHAP analysis failed: {e}")
        return {f: 0.0 for f in feature_names}, None


def _permutation_importance(
    model: GradientBoostingClassifier,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    config: AnalysisConfig,
    feature_names: list[str],
) -> dict[str, float]:
    """Compute permutation importance as a robustness check."""
    result = permutation_importance(
        model,
        X_test,
        y_test,
        n_repeats=config.n_permutation_repeats,
        random_state=config.random_state,
        scoring="roc_auc",
    )
    return dict(zip(feature_names, result.importances_mean.astype(float)))


def _consolidate_rankings(
    rankings: pd.DataFrame, top_n: int
) -> pd.DataFrame:
    """Normalize scores across methods and compute a composite ranking.

    Each method's scores are min-max normalized to [0, 1], and the absolute
    values of correlation are used. The composite score is the mean across
    all normalized method scores.

    Args:
        rankings: DataFrame with per-method scores.
        top_n: Number of top indicators to highlight.

    Returns:
        Sorted DataFrame with composite scores and rank.
    """
    methods = [
        "correlation",
        "mutual_info",
        "gbi_importance",
        "shap_importance",
        "permutation_importance",
    ]

    consolidated = rankings[["feature", "direction", "p_value", "significant"]].copy()

    for method in methods:
        values = rankings[method].copy()
        if method == "correlation":
            values = values.abs()
        vmin, vmax = values.min(), values.max()
        if vmax > vmin:
            consolidated[f"{method}_norm"] = (values - vmin) / (vmax - vmin)
        else:
            consolidated[f"{method}_norm"] = 0.0

    norm_cols = [f"{m}_norm" for m in methods]
    consolidated["composite_score"] = consolidated[norm_cols].mean(axis=1)
    consolidated = consolidated.sort_values(
        "composite_score", ascending=False
    ).reset_index(drop=True)
    consolidated["rank"] = range(1, len(consolidated) + 1)

    # Copy raw scores for reference
    for method in methods:
        consolidated[f"{method}_raw"] = rankings.set_index("feature").loc[
            consolidated["feature"].values, method
        ].values

    return consolidated
