"""
Polars implementation for ETL operations.

This module provides equivalent functions using Polars:
- Uses pl.read_database() for SQL reading
- Uses pl.col().str.strptime() for date conversion
- Uses df.unique() for deduplication
"""

import polars as pl
import pandas as pd


def extract_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate extraction by converting date columns to datetime using Polars.

    Equivalent to the Pandas implementation:
    - pd.to_datetime(df['sale_date'], errors='coerce')
    - pd.to_datetime(df['service_date'], errors='coerce')

    Parameters:
    - df: Input DataFrame with raw data

    Returns:
    - DataFrame with date columns converted to datetime (as Pandas DataFrame)
    """
    pl_df = pl.from_pandas(df)

    if pl_df.schema['sale_date'] != pl.Datetime:
        pl_df = pl_df.with_columns(
            pl.col('sale_date').cast(pl.Datetime).alias('sale_date')
        )

    if pl_df.schema['service_date'] != pl.Datetime:
        pl_df = pl_df.with_columns(
            pl.col('service_date').cast(pl.Datetime).alias('service_date')
        )

    return pl_df.to_pandas()


def transform_data(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    """
    Remove duplicate rows using Polars.

    Equivalent to the Pandas implementation:
    - df.drop_duplicates(subset=subset, keep='first')

    Parameters:
    - df: Input DataFrame
    - subset: List of column names to consider for duplicate detection

    Returns:
    - DataFrame with duplicates removed (as Pandas DataFrame)
    """
    pl_df = pl.from_pandas(df)

    if subset is None:
        result = pl_df.unique(maintain_order=True)
    else:
        result = pl_df.unique(subset=subset, keep='first', maintain_order=True)

    return result.to_pandas().reset_index(drop=True)


def extract_from_connection(connection, query: str) -> pd.DataFrame:
    """
    Extract data from a database connection using Polars.

    Uses Polars' read_database for SQL reading.

    Parameters:
    - connection: Database connection object
    - query: SQL query string

    Returns:
    - DataFrame with query results (as Pandas DataFrame)
    """
    pl_df = pl.read_database(query, connection)

    if 'sale_date' in pl_df.columns:
        if pl_df.schema['sale_date'] != pl.Datetime:
            pl_df = pl_df.with_columns(
                pl.col('sale_date').cast(pl.Datetime).alias('sale_date')
            )

    if 'service_date' in pl_df.columns:
        if pl_df.schema['service_date'] != pl.Datetime:
            pl_df = pl_df.with_columns(
                pl.col('service_date').cast(pl.Datetime).alias('service_date')
            )

    return pl_df.to_pandas()


def get_implementation_name() -> str:
    """Return the name of this implementation."""
    return "polars"
