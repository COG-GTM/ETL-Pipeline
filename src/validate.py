from datetime import datetime
import pandas as pd


def validate_vehicle_sales_schema(df):
    """
    Validates DataFrame against expected schema:
    - vin: 17-character string
    - year: between 1990 and current year
    - sale_price: non-negative
    
    Raises ValueError if validation fails
    """
    current_year = datetime.now().year
    errors = []
    
    # Check VIN length
    invalid_vins = df[df['vin'].str.len() != 17]
    if not invalid_vins.empty:
        errors.append(f"Found {len(invalid_vins)} rows with invalid VIN length (not 17 characters)")
    
    # Check year range
    invalid_years = df[(df['year'] < 1990) | (df['year'] > current_year)]
    if not invalid_years.empty:
        errors.append(f"Found {len(invalid_years)} rows with year outside 1990-{current_year}")
    
    # Check non-negative sale_price
    negative_prices = df[df['sale_price'] < 0]
    if not negative_prices.empty:
        errors.append(f"Found {len(negative_prices)} rows with negative sale_price")
    
    if errors:
        raise ValueError("Schema validation failed:\n" + "\n".join(errors))
    
    print("Schema validation passed")
    return df
