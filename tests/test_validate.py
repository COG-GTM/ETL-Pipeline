import pytest
import pandas as pd
from datetime import datetime
from src.validate import validate_vehicle_sales_schema


def test_valid_schema():
    """Test that valid data passes validation"""
    current_year = datetime.now().year
    df = pd.DataFrame({
        'vin': ['1HGCM82633A004352', '2HGCM82633A004353'],
        'year': [2020, current_year],
        'sale_price': [25000, 30000]
    })
    # Should not raise exception
    validate_vehicle_sales_schema(df)


def test_invalid_vin_length():
    """Test that invalid VIN length raises exception"""
    df = pd.DataFrame({
        'vin': ['SHORT', '1HGCM82633A004352'],
        'year': [2020, 2021],
        'sale_price': [25000, 30000]
    })
    with pytest.raises(ValueError, match="invalid VIN length"):
        validate_vehicle_sales_schema(df)


def test_invalid_year_range():
    """Test that years outside 1990-current raise exception"""
    df = pd.DataFrame({
        'vin': ['1HGCM82633A004352', '2HGCM82633A004353'],
        'year': [1989, datetime.now().year + 1],
        'sale_price': [25000, 30000]
    })
    with pytest.raises(ValueError, match="year outside"):
        validate_vehicle_sales_schema(df)


def test_negative_sale_price():
    """Test that negative sale prices raise exception"""
    df = pd.DataFrame({
        'vin': ['1HGCM82633A004352', '2HGCM82633A004353'],
        'year': [2020, 2021],
        'sale_price': [-1000, 30000]
    })
    with pytest.raises(ValueError, match="negative sale_price"):
        validate_vehicle_sales_schema(df)
