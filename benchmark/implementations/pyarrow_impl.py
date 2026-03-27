"""
PyArrow implementation for ETL operations.

This module provides equivalent functions using PyArrow:
- Uses pyarrow for data manipulation
- Uses pyarrow.compute for transformations
"""

import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd


def extract_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate extraction by converting date columns to datetime using PyArrow.

    Equivalent to the Pandas implementation:
    - pd.to_datetime(df['sale_date'], errors='coerce')
    - pd.to_datetime(df['service_date'], errors='coerce')

    Parameters:
    - df: Input DataFrame with raw data

    Returns:
    - DataFrame with date columns converted to datetime
    """
    table = pa.Table.from_pandas(df)

    sale_date_col = table.column('sale_date')
    if not pa.types.is_timestamp(sale_date_col.type):
        sale_date_col = pc.cast(sale_date_col, pa.timestamp('ns'))

    service_date_col = table.column('service_date')
    if not pa.types.is_timestamp(service_date_col.type):
        service_date_col = pc.cast(service_date_col, pa.timestamp('ns'))

    columns = []
    for i, name in enumerate(table.column_names):
        if name == 'sale_date':
            columns.append(sale_date_col)
        elif name == 'service_date':
            columns.append(service_date_col)
        else:
            columns.append(table.column(i))

    result_table = pa.Table.from_arrays(columns, names=table.column_names)

    return result_table.to_pandas()


def transform_data(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    """
    Remove duplicate rows using PyArrow.

    Equivalent to the Pandas implementation:
    - df.drop_duplicates(subset=subset, keep='first')

    Parameters:
    - df: Input DataFrame
    - subset: List of column names to consider for duplicate detection

    Returns:
    - DataFrame with duplicates removed
    """
    table = pa.Table.from_pandas(df)

    if subset is None:
        subset = table.column_names

    indices_to_keep = []
    seen_keys = set()

    for i in range(table.num_rows):
        key_values = []
        for col_name in subset:
            col = table.column(col_name)
            val = col[i].as_py()
            key_values.append(val)
        key = tuple(key_values)

        if key not in seen_keys:
            seen_keys.add(key)
            indices_to_keep.append(i)

    indices_array = pa.array(indices_to_keep)
    result_table = table.take(indices_array)

    return result_table.to_pandas().reset_index(drop=True)


def extract_from_connection(connection, query: str) -> pd.DataFrame:
    """
    Extract data from a database connection using PyArrow.

    Note: PyArrow doesn't have direct SQL reading like pd.read_sql,
    so we use pandas for the SQL read and then convert to PyArrow
    for the date transformations.

    Parameters:
    - connection: Database connection object
    - query: SQL query string

    Returns:
    - DataFrame with query results
    """
    df = pd.read_sql(query, connection)

    table = pa.Table.from_pandas(df)

    if 'sale_date' in table.column_names:
        sale_date_col = table.column('sale_date')
        if not pa.types.is_timestamp(sale_date_col.type):
            sale_date_col = pc.cast(sale_date_col, pa.timestamp('ns'))
            col_idx = table.column_names.index('sale_date')
            table = table.set_column(col_idx, 'sale_date', sale_date_col)

    if 'service_date' in table.column_names:
        service_date_col = table.column('service_date')
        if not pa.types.is_timestamp(service_date_col.type):
            service_date_col = pc.cast(service_date_col, pa.timestamp('ns'))
            col_idx = table.column_names.index('service_date')
            table = table.set_column(col_idx, 'service_date', service_date_col)

    return table.to_pandas()


def get_implementation_name() -> str:
    """Return the name of this implementation."""
    return "pyarrow"
