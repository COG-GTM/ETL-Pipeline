from datetime import datetime


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
    duplicate_count = df.duplicated(subset=subset).sum()

    if duplicate_count > 0:
        print("-" * 50)
        print(f"Found {duplicate_count} duplicate rows")
        print("Shape before:", df.shape)

        if inplace:
            df.drop_duplicates(subset=subset, keep='first', inplace=True)
            print("Shape after:", df.shape)
            print("-" * 50)
            return df
        else:
            df_cleaned = df.drop_duplicates(subset=subset, keep='first')
            print("Shape after:", df_cleaned.shape)
            print("-" * 50)
            return df_cleaned
    else:
        print("✅ No duplicate rows found")
        return df if inplace else df.copy()


def validate_schema(df):
    """
    Validates that all rows in the DataFrame match the expected schema:
    - vin: 17-character string
    - year: between 1990 and current year
    - sale_price: non-negative number
    
    Raises ValueError if any row fails validation.
    """
    current_year = datetime.now().year
    
    required_columns = ['vin', 'year', 'sale_price']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    invalid_vins = df[~df['vin'].astype(str).str.len().eq(17)]
    if not invalid_vins.empty:
        raise ValueError(f"Invalid VIN found (must be 17 characters): {invalid_vins['vin'].iloc[0]}")
    
    invalid_years = df[(df['year'] < 1990) | (df['year'] > current_year)]
    if not invalid_years.empty:
        raise ValueError(f"Invalid year found (must be between 1990 and {current_year}): {invalid_years['year'].iloc[0]}")
    
    invalid_prices = df[df['sale_price'] < 0]
    if not invalid_prices.empty:
        raise ValueError(f"Invalid sale_price found (must be non-negative): {invalid_prices['sale_price'].iloc[0]}")
    
    print(f"✅ Schema validation passed for {len(df)} rows")
    return df
