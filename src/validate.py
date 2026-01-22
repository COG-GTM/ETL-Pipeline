import re
from datetime import datetime
import pandas as pd
import numpy as np
from scipy import stats


def validate_data_completeness(df):
    """
    Check for missing values and completeness metrics.
    
    Returns a dictionary with:
    - null_counts: NULL counts per column
    - missing_vins: count of records with missing VINs
    - orphaned_records: count of records with missing critical relationships
    - completeness_score: percentage of complete records
    """
    results = {
        'null_counts': {},
        'missing_vins': 0,
        'orphaned_records': 0,
        'completeness_score': 0.0,
        'issues': []
    }
    
    results['null_counts'] = df.isnull().sum().to_dict()
    
    results['missing_vins'] = df['vin'].isnull().sum()
    if results['missing_vins'] > 0:
        results['issues'].append(f"Found {results['missing_vins']} records with missing VINs")
    
    orphaned_count = 0
    if 'dealership_name' in df.columns:
        orphaned_count += df['dealership_name'].isnull().sum()
    if 'sale_date' in df.columns:
        orphaned_count += df['sale_date'].isnull().sum()
    results['orphaned_records'] = orphaned_count
    if orphaned_count > 0:
        results['issues'].append(f"Found {orphaned_count} orphaned records (missing dealership or sale data)")
    
    total_cells = df.size
    null_cells = df.isnull().sum().sum()
    results['completeness_score'] = round((1 - null_cells / total_cells) * 100, 2) if total_cells > 0 else 0.0
    
    return results


def validate_referential_integrity(df):
    """
    Verify foreign key relationships and data consistency.
    
    Returns a dictionary with:
    - invalid_vins: list of VINs with invalid format
    - invalid_dealership_refs: count of invalid dealership references
    - invalid_service_refs: count of service records without valid vehicle references
    - integrity_violations: total count of violations
    """
    results = {
        'invalid_vins': [],
        'invalid_dealership_refs': 0,
        'invalid_service_refs': 0,
        'integrity_violations': 0,
        'issues': []
    }
    
    vin_pattern = re.compile(r'^[A-HJ-NPR-Z0-9]{17}$', re.IGNORECASE)
    
    if 'vin' in df.columns:
        for vin in df['vin'].dropna().unique():
            if not vin_pattern.match(str(vin)):
                results['invalid_vins'].append(str(vin))
        
        if results['invalid_vins']:
            results['issues'].append(f"Found {len(results['invalid_vins'])} VINs with invalid format (should be 17 alphanumeric characters)")
    
    if 'dealership_name' in df.columns:
        results['invalid_dealership_refs'] = df['dealership_name'].isnull().sum()
        if results['invalid_dealership_refs'] > 0:
            results['issues'].append(f"Found {results['invalid_dealership_refs']} records with missing dealership references")
    
    if 'service_date' in df.columns and 'sale_date' in df.columns:
        service_before_sale = df[
            (df['service_date'].notna()) & 
            (df['sale_date'].notna()) & 
            (df['service_date'] < df['sale_date'])
        ]
        results['invalid_service_refs'] = len(service_before_sale)
        if results['invalid_service_refs'] > 0:
            results['issues'].append(f"Found {results['invalid_service_refs']} service records dated before sale date")
    
    results['integrity_violations'] = (
        len(results['invalid_vins']) + 
        results['invalid_dealership_refs'] + 
        results['invalid_service_refs']
    )
    
    return results


def detect_anomalies(df):
    """
    Identify unusual patterns in sales prices, service costs, dates.
    
    Uses IQR method for statistical outlier detection.
    
    Returns a dictionary with:
    - price_outliers: count of sales price outliers
    - service_cost_outliers: count of service cost outliers
    - duplicate_vins_different_specs: VINs with inconsistent specifications
    - future_dates: count of records with future dates
    - anomaly_count: total count of anomalies
    """
    results = {
        'price_outliers': 0,
        'service_cost_outliers': 0,
        'duplicate_vins_different_specs': [],
        'future_dates': 0,
        'regional_anomalies': [],
        'anomaly_count': 0,
        'issues': []
    }
    
    current_date = pd.Timestamp.now()
    
    if 'sale_price' in df.columns:
        prices = df['sale_price'].dropna()
        if len(prices) > 0:
            Q1 = prices.quantile(0.25)
            Q3 = prices.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = prices[(prices < lower_bound) | (prices > upper_bound)]
            results['price_outliers'] = len(outliers)
            if results['price_outliers'] > 0:
                results['issues'].append(f"Found {results['price_outliers']} sales price outliers (outside IQR bounds)")
    
    if 'service_cost' in df.columns:
        costs = df['service_cost'].dropna()
        costs = costs[costs > 0]
        if len(costs) > 0:
            Q1 = costs.quantile(0.25)
            Q3 = costs.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = costs[(costs < lower_bound) | (costs > upper_bound)]
            results['service_cost_outliers'] = len(outliers)
            if results['service_cost_outliers'] > 0:
                results['issues'].append(f"Found {results['service_cost_outliers']} service cost outliers")
    
    if 'vin' in df.columns and 'model' in df.columns and 'year' in df.columns:
        vin_specs = df.groupby('vin').agg({
            'model': 'nunique',
            'year': 'nunique'
        }).reset_index()
        inconsistent_vins = vin_specs[
            (vin_specs['model'] > 1) | (vin_specs['year'] > 1)
        ]['vin'].tolist()
        results['duplicate_vins_different_specs'] = inconsistent_vins
        if inconsistent_vins:
            results['issues'].append(f"Found {len(inconsistent_vins)} VINs with inconsistent model/year specifications")
    
    future_count = 0
    if 'sale_date' in df.columns:
        future_sales = df[df['sale_date'] > current_date]
        future_count += len(future_sales)
    if 'service_date' in df.columns:
        future_services = df[df['service_date'] > current_date]
        future_count += len(future_services)
    results['future_dates'] = future_count
    if future_count > 0:
        results['issues'].append(f"Found {future_count} records with future dates")
    
    if 'region' in df.columns and 'sale_price' in df.columns:
        regional_stats = df.groupby('region')['sale_price'].agg(['mean', 'std', 'count']).reset_index()
        overall_mean = df['sale_price'].mean()
        overall_std = df['sale_price'].std()
        
        if overall_std > 0:
            for _, row in regional_stats.iterrows():
                z_score = abs(row['mean'] - overall_mean) / overall_std
                if z_score > 2 and row['count'] >= 5:
                    results['regional_anomalies'].append({
                        'region': row['region'],
                        'mean_price': round(row['mean'], 2),
                        'z_score': round(z_score, 2)
                    })
            if results['regional_anomalies']:
                results['issues'].append(f"Found {len(results['regional_anomalies'])} regions with unusual pricing patterns")
    
    results['anomaly_count'] = (
        results['price_outliers'] + 
        results['service_cost_outliers'] + 
        len(results['duplicate_vins_different_specs']) + 
        results['future_dates']
    )
    
    return results


def validate_data_quality(df):
    """
    Run all validation checks on the DataFrame.
    
    Returns a dictionary containing results from all validation functions.
    """
    completeness = validate_data_completeness(df)
    integrity = validate_referential_integrity(df)
    anomalies = detect_anomalies(df)
    
    return {
        'completeness': completeness,
        'integrity': integrity,
        'anomalies': anomalies
    }


def generate_quality_report(df, validation_results):
    """
    Create comprehensive quality report DataFrame.
    
    Parameters:
    - df: the validated DataFrame
    - validation_results: dictionary from validate_data_quality()
    
    Returns:
    - DataFrame with quality report metrics
    """
    completeness = validation_results.get('completeness', {})
    integrity = validation_results.get('integrity', {})
    anomalies = validation_results.get('anomalies', {})
    
    all_issues = []
    all_issues.extend(completeness.get('issues', []))
    all_issues.extend(integrity.get('issues', []))
    all_issues.extend(anomalies.get('issues', []))
    
    completeness_score = completeness.get('completeness_score', 0)
    integrity_violations = integrity.get('integrity_violations', 0)
    anomaly_count = anomalies.get('anomaly_count', 0)
    
    total_records = len(df)
    max_violations = total_records * 3 if total_records > 0 else 1
    violation_penalty = min((integrity_violations + anomaly_count) / max_violations * 50, 50)
    quality_score = round(max(0, completeness_score - violation_penalty), 2)
    
    report_data = {
        'validation_timestamp': [datetime.now().isoformat()],
        'total_records': [total_records],
        'completeness_score': [completeness_score],
        'integrity_violations': [integrity_violations],
        'anomaly_count': [anomaly_count],
        'quality_score': [quality_score],
        'detailed_issues': ['; '.join(all_issues) if all_issues else 'No issues found']
    }
    
    report_df = pd.DataFrame(report_data)
    
    return report_df
