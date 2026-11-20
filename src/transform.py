def identify_and_remove_duplicated_data(df, subset=None, inplace=False):
    """
    Identifies and removes duplicated rows from the DataFrame.

    Parameters:
    - df: the input DataFrame
    - subset: list of column names to consider for duplicate detection (default: all columns)
    - inplace: if True, modifies the original DataFrame (default: False)

    Returns:
    - A cleaned DataFrame with duplicates removed
    """
    shape_before = df.shape
    if inplace:
        df.drop_duplicates(subset=subset, keep='first', inplace=True)
        df_cleaned = df
    else:
        df_cleaned = df.drop_duplicates(subset=subset, keep='first')
    duplicate_count = shape_before[0] - df_cleaned.shape[0]

    if duplicate_count > 0:
        print("-" * 50)
        print(f"Found {duplicate_count} duplicate rows")
        print("Shape before:", shape_before)
        print("Shape after:", df_cleaned.shape)
        print("-" * 50)
    else:
        print("✅ No duplicate rows found")
    return df_cleaned
