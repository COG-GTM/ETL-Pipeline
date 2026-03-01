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


def validate_vehicle_sales_schema(df):
    """
    Validates that all rows match the expected schema:
    - vin is a 17-character string
    - year is between 1990 and current year
    - sale_price is non-negative
    
    Raises:
        ValueError: If any row fails validation
    """
    from datetime import datetime
    
    current_year = datetime.now().year
    errors = []
    
    # Check VIN length and type
    invalid_vins = df[~df['vin'].astype(str).str.len().eq(17)]
    if not invalid_vins.empty:
        errors.append(f"VIN validation failed: {len(invalid_vins)} rows have VINs not equal to 17 characters")
    
    # Check year range
    invalid_years = df[(df['year'] < 1990) | (df['year'] > current_year)]
    if not invalid_years.empty:
        errors.append(f"Year validation failed: {len(invalid_years)} rows have years outside 1990-{current_year}")
    
    # Check sale_price is non-negative
    invalid_prices = df[df['sale_price'] < 0]
    if not invalid_prices.empty:
        errors.append(f"Sale price validation failed: {len(invalid_prices)} rows have negative sale prices")
    
    if errors:
        raise ValueError("Schema validation failed:\n" + "\n".join(errors))
    
    print("✅ Schema validation passed")
    return df
