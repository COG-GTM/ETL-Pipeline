import pytest
from unittest.mock import patch
import pandas as pd
from main import *
from src.validate import validate_vehicle_sales_schema


def test_pipeline_validation_failure():
    """Test that pipeline fails on invalid data"""
    # Create invalid test data
    invalid_df = pd.DataFrame({
        'vin': ['INVALID_VIN_LENGTH'],
        'year': [1985],
        'sale_price': [-1000],
        'model': ['Test'],
        'dealership_name': ['Test'],
        'region': ['Test'],
        'sale_date': [pd.Timestamp('2023-01-01')],
        'buyer_name': ['Test'],
        'service_date': [None],
        'service_type': ['Unknown'],
        'service_cost': [0]
    })
    
    with patch('src.extract.extract_vehicle_sales_data', return_value=invalid_df):
        with pytest.raises(ValueError, match="Schema validation failed"):
            # Run pipeline - should fail at validation step
            vehicle_sales_df = extract_vehicle_sales_data(dbname, host, port, user, password)
            vehicle_sales_deduped = identify_and_remove_duplicated_data(vehicle_sales_df)
            validate_vehicle_sales_schema(vehicle_sales_deduped)
