from datetime import datetime


class SchemaValidationError(Exception):
    """Exception raised when data fails schema validation."""
    pass


def validate_schema(df):
    """
    Validates that all rows in the DataFrame match the expected schema.

    Validation rules:
    - vin: must be a 17-character string
    - year: must be between 1990 and current year (inclusive)
    - sale_price: must be non-negative (NULL values are allowed)

    Parameters:
    - df: the input DataFrame

    Raises:
    - SchemaValidationError: if any row fails validation

    Returns:
    - The validated DataFrame (unchanged)
    """
    errors = []
    current_year = datetime.now().year

    for idx, row in df.iterrows():
        vin = row.get('vin')
        year = row.get('year')
        sale_price = row.get('sale_price')

        if not isinstance(vin, str) or len(vin) != 17:
            errors.append(
                f"Row {idx}: Invalid VIN '{vin}' - must be a 17-character string"
            )

        if year is not None:
            try:
                year_int = int(year)
                if year_int < 1990 or year_int > current_year:
                    errors.append(
                        f"Row {idx}: Invalid year {year} - must be between 1990 and {current_year}"
                    )
            except (ValueError, TypeError):
                errors.append(
                    f"Row {idx}: Invalid year '{year}' - must be a valid integer"
                )

        if sale_price is not None:
            try:
                price_float = float(sale_price)
                if price_float < 0:
                    errors.append(
                        f"Row {idx}: Invalid sale_price {sale_price} - must be non-negative"
                    )
            except (ValueError, TypeError):
                errors.append(
                    f"Row {idx}: Invalid sale_price '{sale_price}' - must be a valid number"
                )

    if errors:
        error_message = "Schema validation failed:\n" + "\n".join(errors)
        raise SchemaValidationError(error_message)

    print(f"✅ Schema validation passed for {len(df)} rows")
    return df


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
