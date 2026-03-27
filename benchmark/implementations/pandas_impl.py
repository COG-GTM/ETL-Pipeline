"""
Pandas implementation wrapper for ETL operations.

This module wraps the existing Pandas code from the current pipeline
for use in the benchmark framework.
"""

import pandas as pd


def extract_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate extraction by converting date columns to datetime.

    This wraps the date conversion logic from src/extract.py:
    - df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    - df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

    Parameters:
    - df: Input DataFrame with raw data

    Returns:
    - DataFrame with date columns converted to datetime
    """
    result = df.copy()

    result['sale_date'] = pd.to_datetime(result['sale_date'], errors='coerce')
    result['service_date'] = pd.to_datetime(result['service_date'], errors='coerce')

    return result


def transform_data(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    This wraps the deduplication logic from src/transform.py:
    - df.drop_duplicates(subset=subset, keep='first')

    Parameters:
    - df: Input DataFrame
    - subset: List of column names to consider for duplicate detection

    Returns:
    - DataFrame with duplicates removed
    """
    return df.drop_duplicates(subset=subset, keep='first').reset_index(drop=True)


def extract_from_connection(connection, query: str) -> pd.DataFrame:
    """
    Extract data from a database connection using pd.read_sql.

    This wraps the SQL reading logic from src/extract.py:
    - pd.read_sql(query, conn)

    Parameters:
    - connection: Database connection object
    - query: SQL query string

    Returns:
    - DataFrame with query results
    """
    df = pd.read_sql(query, connection)

    if 'sale_date' in df.columns:
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    if 'service_date' in df.columns:
        df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

    return df


def get_implementation_name() -> str:
    """Return the name of this implementation."""
    return "pandas"
