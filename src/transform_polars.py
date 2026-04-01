import polars as pl


def identify_and_remove_duplicated_data_polars(df, subset=None):
    """
    Identifies and removes duplicated rows from a Polars DataFrame.

    Parameters:
    - df: the input Polars DataFrame
    - subset: list of column names to consider for duplicate detection (default: all columns)

    Returns:
    - A cleaned Polars DataFrame with duplicates removed
    """
    if subset is None:
        subset = df.columns

    original_count = df.height

    df_cleaned = df.unique(subset=subset, keep='first')
    duplicate_count = original_count - df_cleaned.height

    if duplicate_count > 0:
        print("-" * 50)
        print(f"Found {duplicate_count} duplicate rows (Polars)")
        print("Shape before:", original_count)
        print("Shape after:", df_cleaned.height)
        print("-" * 50)
        return df_cleaned
    else:
        print("No duplicate rows found (Polars)")
        return df
