import pytest
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from unittest.mock import Mock, patch
from datetime import datetime

from src.load_data_to_s3 import df_to_s3_partitioned, connect_to_s3

@pytest.fixture
def sample_dataframe():
    """Create sample DataFrame with region column for testing"""
    return pd.DataFrame({
        'vin': ['1HGCM82633A004352', '1HGCM82633A004353', '1HGCM82633A004354'],
        'model': ['Camry', 'Corolla', 'F-150'],
        'year': [2021, 2022, 2023],
        'region': ['West', 'West', 'Central'],
        'sale_price': [28000, 22000, 45000]
    })

@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing"""
    client = Mock()
    client.put_object = Mock()
    return client

def test_parquet_compression_format(sample_dataframe):
    """Test that Parquet files are written with Snappy compression"""
    # Convert to Parquet
    table = pa.Table.from_pandas(sample_dataframe)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression='snappy')
    
    # Verify compression
    parquet_file = pq.ParquetFile(BytesIO(buf.getvalue().to_pybytes()))
    assert parquet_file.metadata.row_group(0).column(0).compression == 'SNAPPY'

def test_partitioning_by_region(sample_dataframe, mock_s3_client):
    """Test that data is correctly partitioned by region"""
    with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
        df_to_s3_partitioned(
            sample_dataframe, 
            'test-bucket', 
            'test-key', 
            'test-secret'
        )
    
    # Should have 2 calls: one for West, one for Central
    assert mock_s3_client.put_object.call_count == 2
    
    # Check partition keys
    calls = mock_s3_client.put_object.call_args_list
    keys = [call[1]['Key'] for call in calls]
    
    assert any('region=West' in key for key in keys)
    assert any('region=Central' in key for key in keys)

def test_timestamp_formatting(sample_dataframe, mock_s3_client):
    """Test that filenames include correct timestamp format"""
    with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
        with patch('src.load_data_to_s3.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = '20241203_143022'
            
            df_to_s3_partitioned(
                sample_dataframe, 
                'test-bucket', 
                'test-key', 
                'test-secret'
            )
    
    # Verify timestamp in filename
    call = mock_s3_client.put_object.call_args
    key = call[1]['Key']
    assert '20241203_143022.parquet' in key

def test_s3_upload_error_handling(sample_dataframe):
    """Test error handling for S3 upload failures"""
    with patch('src.load_data_to_s3.connect_to_s3') as mock_connect:
        mock_client = Mock()
        mock_client.put_object.side_effect = Exception("S3 Error")
        mock_connect.return_value = mock_client
        
        # Should not raise exception, but handle gracefully
        df_to_s3_partitioned(
            sample_dataframe, 
            'test-bucket', 
            'test-key', 
            'test-secret'
        )

def test_parquet_data_integrity(sample_dataframe):
    """Test that data integrity is maintained in Parquet conversion"""
    # Convert to Parquet and back
    table = pa.Table.from_pandas(sample_dataframe)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression='snappy')
    
    # Read back and verify
    result_df = pq.read_table(BytesIO(buf.getvalue().to_pybytes())).to_pandas()
    
    # Compare data (ignoring index)
    pd.testing.assert_frame_equal(
        sample_dataframe.reset_index(drop=True),
        result_df.reset_index(drop=True)
    )
