# Consumption Growth Leading Indicators

Identifies the top leading indicators of account consumption growth using multi-method feature importance ranking.

## Purpose

This module analyzes account-level consumption data (e.g., API calls, seat utilization, resource usage) to answer: **which consumption behaviors best predict future account growth?**

It engineers time-series features from raw consumption data, then ranks those features using five complementary methods to produce a robust, consolidated ranking of the top leading indicators.

## Methodology

### Feature Engineering

From raw consumption records (`account_id, timestamp, metric_name, metric_value`), the module computes per-account features across six categories:

| Category | Examples |
|---|---|
| **Usage Volume** | Total consumption, rolling averages (7d/30d/60d/90d), daily mean/median/max |
| **Usage Frequency** | Active day ratio, max active streak, current streak |
| **Usage Breadth** | Number of distinct metrics/features used, recent 30d breadth |
| **Usage Intensity** | Peak-to-average ratio, coefficient of variation |
| **Usage Trends** | Week-over-week/month-over-month growth, velocity, acceleration, trend slopes |
| **Usage Concentration** | Herfindahl-Hirschman Index (HHI), Gini coefficient across product areas |

Additional features: seasonality strength, deseasonalized trend, days since last spike, recent-to-historical ratio, percentile rank vs. cohort.

### Indicator Ranking Methods

1. **Spearman Rank Correlation** - Linear monotonic relationship with growth label
2. **Mutual Information** - Captures non-linear dependencies (sklearn `mutual_info_classif`)
3. **Gradient Boosting Feature Importance** - Tree-based importance from a trained `GradientBoostingClassifier`
4. **SHAP Values** - Model-agnostic feature importance with directionality (positive/negative impact)
5. **Permutation Importance** - Robustness check measuring accuracy drop when feature is shuffled

The composite score is the mean of min-max normalized scores across all five methods.

### Growth Labeling

Accounts are labeled as "growth" if their total consumption increased by more than a configurable threshold (default: >10%) when comparing the most recent evaluation period (default: 90 days) to the preceding period of equal length.

## Usage

### Quick Start with Synthetic Data

```bash
cd ETL-Pipeline
pip install -r analysis/consumption_indicators/requirements.txt
python -m analysis.consumption_indicators.main
```

This generates synthetic data for ~500 accounts over 12 months, runs the full analysis, and saves results to the `output/` directory.

### Using Real Data (CSV)

```python
from analysis.consumption_indicators.config import PipelineConfig, DataSourceConfig
from analysis.consumption_indicators.main import run_pipeline

config = PipelineConfig(
    use_synthetic_data=False,
    data_source=DataSourceConfig(
        source_type="csv",
        csv_path="path/to/your/consumption_data.csv",
        account_id_col="account_id",
        timestamp_col="timestamp",
        metric_name_col="metric_name",
        metric_value_col="metric_value",
    ),
)

results = run_pipeline(config)
```

Your CSV must contain at minimum these columns (names are configurable):
- `account_id` - Unique account identifier
- `timestamp` - Date/datetime of the consumption event
- `metric_name` - Name of the consumption metric (e.g., "api_calls", "storage_gb")
- `metric_value` - Numeric value of consumption

### Using Real Data (Database)

```python
config = PipelineConfig(
    use_synthetic_data=False,
    data_source=DataSourceConfig(
        source_type="database",
        db_host="your-host",
        db_port=5432,
        db_name="your-db",
        db_user="your-user",
        db_password="your-password",
    ),
)
results = run_pipeline(config)
```

### Customizing Parameters

```python
from analysis.consumption_indicators.config import (
    PipelineConfig, TimeWindowConfig, AnalysisConfig
)

config = PipelineConfig(
    time_windows=TimeWindowConfig(
        rolling_windows_days=[7, 14, 30, 60, 90],
        growth_evaluation_period_days=60,
        growth_threshold_pct=15.0,
    ),
    analysis=AnalysisConfig(
        top_n_indicators=15,
        n_estimators=300,
        max_depth=6,
    ),
    output_dir="my_output",
)
```

## Expected Output

### Console Output

A summary table showing the top N indicators:

```
================================================================================
  CONSUMPTION GROWTH LEADING INDICATORS - SUMMARY
================================================================================
  Model Performance: Accuracy=0.850, ROC AUC=0.920
  Analysis completed in 45.2s
--------------------------------------------------------------------------------
  Rank  Feature                                 Score     Direction   Sig.
--------------------------------------------------------------------------------
  1     trend_slope_90d                         0.8721    positive    *
  2     mom_growth_rate                         0.8234    positive    *
  3     recent_to_historical_ratio              0.7891    positive    *
  ...
--------------------------------------------------------------------------------
  * = statistically significant (p < 0.05)
================================================================================
```

### Files Generated

| File | Description |
|---|---|
| `output/consumption_indicators_report.csv` | Full ranking table with scores from each method |
| `output/top_indicators_bar.png` | Bar chart of top indicators by composite score |
| `output/shap_summary.png` | SHAP beeswarm plot showing feature impact distribution |
| `output/timeseries_comparison.png` | Weekly consumption trends: growth vs. non-growth cohorts |
| `output/correlation_heatmap.png` | Correlation matrix of top indicators (to spot redundancy) |

### Interpreting Results

- **Composite Score**: Higher = stronger indicator of growth. Ranges from 0 to 1.
- **Direction**: "positive" means higher values of the feature are associated with growth; "negative" means lower values are associated with growth.
- **Significant**: Marked with `*` if the Spearman correlation p-value < 0.05.
- **Correlation Heatmap**: Highly correlated indicators (|r| > 0.8) may be redundant; consider keeping only the highest-ranked one from each cluster.

## Module Structure

```
analysis/consumption_indicators/
├── __init__.py              # Package init
├── config.py                # Configuration dataclasses
├── data_loader.py           # Data loading and synthetic data generation
├── feature_engineering.py   # Time-series feature computation
├── indicator_analysis.py    # Multi-method indicator ranking
├── visualizations.py        # Plot generation
├── main.py                  # Pipeline orchestration
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

## Dependencies

- pandas, numpy, scipy - Data manipulation and statistics
- scikit-learn - Machine learning (GradientBoosting, mutual info, permutation importance)
- shap - SHAP value computation for model interpretability
- matplotlib, seaborn - Visualization
- xgboost - Optional, for future XGBoost model support
