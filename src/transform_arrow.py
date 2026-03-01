import pyarrow as pa
import pyarrow.compute as pc


def identify_and_remove_duplicated_data_arrow(table, subset=None):
    """
    Identifies and removes duplicated rows from the Arrow Table.

    Parameters:
    - table: the input Arrow Table
    - subset: list of column names to consider for duplicate detection (default: all columns)

    Returns:
    - A cleaned Arrow Table with duplicates removed
    """
    if subset is None:
        subset = table.column_names

    df_pandas = table.to_pandas()
    original_count = len(df_pandas)
    duplicate_mask = df_pandas.duplicated(subset=subset, keep='first')
    duplicate_count = duplicate_mask.sum()

    if duplicate_count > 0:
        print("-" * 50)
        print(f"Found {duplicate_count} duplicate rows (Arrow)")
        print("Shape before:", table.num_rows)

        df_cleaned = df_pandas[~duplicate_mask]
        cleaned_table = pa.Table.from_pandas(df_cleaned, preserve_index=False)

        print("Shape after:", cleaned_table.num_rows)
        print("-" * 50)
        return cleaned_table
    else:
        print("✅ No duplicate rows found (Arrow)")
        return table
