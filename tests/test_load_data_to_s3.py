import pytest
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from unittest.mock import MagicMock, patch
from datetime import datetime

import sys
sys.path.insert(0, '..')

from src.load_data_to_s3 import df_to_s3_parquet, connect_to_s3


class TestParquetOutput:
    """Tests to verify Parquet output format, Snappy compression, and region partitioning."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample DataFrame with region column for testing."""
        return pd.DataFrame({
            'vin': ['VIN001', 'VIN002', 'VIN003', 'VIN004'],
            'model': ['Camry', 'Corolla', 'F-150', 'Civic'],
            'year': [2021, 2022, 2023, 2022],
            'dealership_name': ['Bay Area Motors', 'Bay Area Motors', 'Midwest Auto Hub', 'Atlantic Car Group'],
            'region': ['West', 'West', 'Central', 'East'],
            'sale_price': [28000, 22000, 45000, 25000],
            'buyer_name': ['Alice', 'Bob', 'Carlos', 'Diana']
        })

    @pytest.fixture
    def mock_s3_client(self):
        """Create a mock S3 client that captures uploaded data."""
        mock_client = MagicMock()
        mock_client.uploaded_objects = {}
        
        def capture_put_object(Bucket, Key, Body):
            mock_client.uploaded_objects[Key] = Body
        
        mock_client.put_object.side_effect = capture_put_object
        return mock_client

    def test_parquet_format_output(self, sample_dataframe, mock_s3_client):
        """Test that output is in valid Parquet format."""
        with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
            df_to_s3_parquet(
                sample_dataframe,
                'test/output',
                'test-bucket',
                'fake_key',
                'fake_secret'
            )
        
        # Verify files were uploaded
        assert len(mock_s3_client.uploaded_objects) > 0, "No files were uploaded"
        
        # Verify each uploaded file is valid Parquet
        for key, body in mock_s3_client.uploaded_objects.items():
            assert key.endswith('.parquet'), f"File {key} does not have .parquet extension"
            
            # Read the Parquet data to verify it's valid
            buffer = BytesIO(body)
            table = pq.read_table(buffer)
            df = table.to_pandas()
            
            # Verify data integrity
            assert len(df) > 0, f"Parquet file {key} is empty"
            assert 'vin' in df.columns, "Missing 'vin' column in Parquet output"
            assert 'region' in df.columns, "Missing 'region' column in Parquet output"

    def test_snappy_compression(self, sample_dataframe, mock_s3_client):
        """Test that Parquet files use Snappy compression."""
        with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
            df_to_s3_parquet(
                sample_dataframe,
                'test/output',
                'test-bucket',
                'fake_key',
                'fake_secret'
            )
        
        # Verify Snappy compression is applied
        for key, body in mock_s3_client.uploaded_objects.items():
            buffer = BytesIO(body)
            parquet_file = pq.ParquetFile(buffer)
            metadata = parquet_file.metadata
            
            # Check compression for each row group
            for i in range(metadata.num_row_groups):
                row_group = metadata.row_group(i)
                for j in range(row_group.num_columns):
                    column = row_group.column(j)
                    assert column.compression == 'SNAPPY', \
                        f"Column {j} in row group {i} uses {column.compression} instead of SNAPPY"

    def test_region_partitioning(self, sample_dataframe, mock_s3_client):
        """Test that data is partitioned by region."""
        with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
            df_to_s3_parquet(
                sample_dataframe,
                'test/output',
                'test-bucket',
                'fake_key',
                'fake_secret'
            )
        
        # Get unique regions from sample data
        expected_regions = set(sample_dataframe['region'].unique())
        
        # Verify we have one file per region
        assert len(mock_s3_client.uploaded_objects) == len(expected_regions), \
            f"Expected {len(expected_regions)} partitions, got {len(mock_s3_client.uploaded_objects)}"
        
        # Verify partition paths contain region=<value>
        found_regions = set()
        for key in mock_s3_client.uploaded_objects.keys():
            assert 'region=' in key, f"Key {key} does not contain region partition"
            # Extract region value from path like 'test/output/region=West/timestamp.parquet'
            for part in key.split('/'):
                if part.startswith('region='):
                    found_regions.add(part.split('=')[1])
        
        assert found_regions == expected_regions, \
            f"Expected regions {expected_regions}, found {found_regions}"

    def test_partition_data_correctness(self, sample_dataframe, mock_s3_client):
        """Test that each partition contains only data for that region."""
        with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
            df_to_s3_parquet(
                sample_dataframe,
                'test/output',
                'test-bucket',
                'fake_key',
                'fake_secret'
            )
        
        for key, body in mock_s3_client.uploaded_objects.items():
            # Extract expected region from key
            expected_region = None
            for part in key.split('/'):
                if part.startswith('region='):
                    expected_region = part.split('=')[1]
                    break
            
            assert expected_region is not None, f"Could not extract region from key {key}"
            
            # Read Parquet and verify all rows have the expected region
            buffer = BytesIO(body)
            df = pq.read_table(buffer).to_pandas()
            
            unique_regions = df['region'].unique()
            assert len(unique_regions) == 1, \
                f"Partition {key} contains multiple regions: {unique_regions}"
            assert unique_regions[0] == expected_region, \
                f"Partition {key} contains region {unique_regions[0]}, expected {expected_region}"

    def test_timestamp_filename(self, sample_dataframe, mock_s3_client):
        """Test that filenames contain timestamp in expected format."""
        with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
            df_to_s3_parquet(
                sample_dataframe,
                'test/output',
                'test-bucket',
                'fake_key',
                'fake_secret'
            )
        
        for key in mock_s3_client.uploaded_objects.keys():
            # Extract filename from path
            filename = key.split('/')[-1]
            # Remove .parquet extension
            timestamp_str = filename.replace('.parquet', '')
            
            # Verify timestamp format YYYYMMDD_HHMMSS
            try:
                datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
            except ValueError:
                pytest.fail(f"Filename {filename} does not match expected timestamp format YYYYMMDD_HHMMSS")

    def test_all_data_preserved(self, sample_dataframe, mock_s3_client):
        """Test that all rows from input DataFrame are preserved across partitions."""
        with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_s3_client):
            df_to_s3_parquet(
                sample_dataframe,
                'test/output',
                'test-bucket',
                'fake_key',
                'fake_secret'
            )
        
        # Collect all rows from all partitions
        all_rows = []
        for body in mock_s3_client.uploaded_objects.values():
            buffer = BytesIO(body)
            df = pq.read_table(buffer).to_pandas()
            all_rows.append(df)
        
        combined_df = pd.concat(all_rows, ignore_index=True)
        
        # Verify row count matches
        assert len(combined_df) == len(sample_dataframe), \
            f"Expected {len(sample_dataframe)} rows, got {len(combined_df)}"
        
        # Verify all VINs are present
        expected_vins = set(sample_dataframe['vin'])
        actual_vins = set(combined_df['vin'])
        assert expected_vins == actual_vins, \
            f"Missing VINs: {expected_vins - actual_vins}"


class TestS3Connection:
    """Tests for S3 connection functionality."""

    def test_connect_to_s3_returns_client(self):
        """Test that connect_to_s3 returns a boto3 S3 client."""
        with patch('src.load_data_to_s3.boto3.client') as mock_boto_client:
            mock_boto_client.return_value = MagicMock()
            
            client = connect_to_s3('fake_key', 'fake_secret', 'us-west-2')
            
            mock_boto_client.assert_called_once_with(
                's3',
                region_name='us-west-2',
                aws_access_key_id='fake_key',
                aws_secret_access_key='fake_secret'
            )
            assert client is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
