import pytest
import pandas as pd
from unittest.mock import patch, Mock
from io import StringIO

from src.extract import extract_vehicle_sales_data
from src.transform import identify_and_remove_duplicated_data
from src.load_data_to_s3 import df_to_s3_partitioned

@pytest.fixture
def mock_extract_data():
    """Mock extracted vehicle sales data"""
    return pd.DataFrame({
        'vin': ['1HGCM82633A004352', '1HGCM82633A004352', '1HGCM82633A004353'],
        'model': ['Camry', 'Camry', 'Corolla'],
        'year': [2021, 2021, 2022],
        'dealership_name': ['Bay Area Motors', 'Bay Area Motors', 'Bay Area Motors'],
        'region': ['West', 'West', 'West'],
        'sale_date': pd.to_datetime(['2022-01-15', '2022-01-15', '2023-03-20']),
        'sale_price': [28000, 28000, 22000],
        'buyer_name': ['Alice Johnson', 'Alice Johnson', 'Bob Smith'],
        'service_date': pd.to_datetime(['2023-02-01', '2023-02-01', None]),
        'service_type': ['Oil Change', 'Oil Change', 'Unknown'],
        'service_cost': [100, 100, 0]
    })

@patch('src.load_data_to_s3.connect_to_s3')
def test_end_to_end_pipeline(mock_s3_connect, mock_extract_data):
    """Test complete pipeline with Parquet output"""
    # Mock S3 client
    mock_client = Mock()
    mock_client.put_object = Mock()
    mock_s3_connect.return_value = mock_client
    
    # Transform data (remove duplicates)
    deduped_data = identify_and_remove_duplicated_data(mock_extract_data)
    
    # Upload to S3 as partitioned Parquet
    df_to_s3_partitioned(
        deduped_data,
        'test-bucket',
        'test-key',
        'test-secret'
    )
    
    # Verify upload was called
    assert mock_client.put_object.called
    
    # Verify deduplication worked
    assert len(deduped_data) == 2  # Should remove 1 duplicate
