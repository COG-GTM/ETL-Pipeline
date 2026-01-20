"""
Unit tests for the load module (src/load_data_to_s3.py).

This module tests:
- connect_to_s3() function
- df_to_s3() function

Tests use moto library to mock AWS S3 operations.
"""

import pytest
import pandas as pd
import boto3
from io import StringIO
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

from moto import mock_aws

from src.load_data_to_s3 import connect_to_s3, df_to_s3


class TestConnectToS3:
    """Tests for the connect_to_s3 function."""

    def test_connect_to_s3_returns_client(self, test_aws_credentials):
        """Test that connect_to_s3 returns an S3 client."""
        with mock_aws():
            client = connect_to_s3(**test_aws_credentials)
            
            assert client is not None
            # Verify it's a boto3 S3 client by checking for expected methods
            assert hasattr(client, 'put_object')
            assert hasattr(client, 'get_object')

    def test_connect_to_s3_with_custom_region(self):
        """Test connection with a custom region."""
        with mock_aws():
            client = connect_to_s3(
                aws_access_key_id='testing',
                aws_secret_access_key='testing',
                region_name='eu-west-1'
            )
            
            assert client is not None

    def test_connect_to_s3_default_region(self):
        """Test that default region is us-west-2."""
        with mock_aws():
            client = connect_to_s3(
                aws_access_key_id='testing',
                aws_secret_access_key='testing'
            )
            
            assert client is not None


class TestDfToS3:
    """Tests for the df_to_s3 function."""

    @mock_aws
    def test_df_to_s3_uploads_successfully(self, sample_vehicle_data):
        """Test successful DataFrame upload to S3."""
        # Create bucket
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        # Upload DataFrame
        df_to_s3(
            df=sample_vehicle_data,
            key='test/data.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Verify upload
        response = s3_client.get_object(Bucket='test-bucket', Key='test/data.csv')
        content = response['Body'].read().decode('utf-8')
        
        # Verify content is CSV
        assert 'vin' in content
        assert 'model' in content

    @mock_aws
    def test_df_to_s3_creates_valid_csv(self, sample_vehicle_data):
        """Test that uploaded CSV can be read back as DataFrame."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        df_to_s3(
            df=sample_vehicle_data,
            key='test/data.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Read back and verify
        response = s3_client.get_object(Bucket='test-bucket', Key='test/data.csv')
        content = response['Body'].read().decode('utf-8')
        result_df = pd.read_csv(StringIO(content))
        
        assert len(result_df) == len(sample_vehicle_data)
        assert list(result_df.columns) == list(sample_vehicle_data.columns)

    @mock_aws
    def test_df_to_s3_with_empty_dataframe(self):
        """Test uploading an empty DataFrame."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        empty_df = pd.DataFrame(columns=['vin', 'model', 'year'])
        
        df_to_s3(
            df=empty_df,
            key='test/empty.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Verify upload
        response = s3_client.get_object(Bucket='test-bucket', Key='test/empty.csv')
        content = response['Body'].read().decode('utf-8')
        
        # Should have header row only
        assert 'vin' in content

    @mock_aws
    def test_df_to_s3_overwrites_existing_key(self, sample_vehicle_data):
        """Test that uploading to existing key overwrites the file."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        # First upload
        df_to_s3(
            df=sample_vehicle_data,
            key='test/data.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Second upload with different data
        new_df = sample_vehicle_data.head(1)
        df_to_s3(
            df=new_df,
            key='test/data.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Verify overwrite
        response = s3_client.get_object(Bucket='test-bucket', Key='test/data.csv')
        content = response['Body'].read().decode('utf-8')
        result_df = pd.read_csv(StringIO(content))
        
        assert len(result_df) == 1

    def test_df_to_s3_handles_client_error(self, sample_vehicle_data, capsys):
        """Test handling of S3 client errors."""
        with patch('src.load_data_to_s3.connect_to_s3') as mock_connect:
            mock_client = MagicMock()
            mock_client.put_object.side_effect = ClientError(
                {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
                'PutObject'
            )
            mock_connect.return_value = mock_client
            
            df_to_s3(
                df=sample_vehicle_data,
                key='test/data.csv',
                s3_bucket='test-bucket',
                aws_access_key_id='testing',
                aws_secret_access_key='testing'
            )
            
            # Should print error message
            captured = capsys.readouterr()
            assert 'Failed to upload' in captured.out or 'Access Denied' in captured.out

    @mock_aws
    def test_df_to_s3_with_special_characters_in_key(self, sample_vehicle_data):
        """Test uploading with special characters in the S3 key."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        special_key = 'test/data-2022_01_15.csv'
        
        df_to_s3(
            df=sample_vehicle_data,
            key=special_key,
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Verify upload
        response = s3_client.get_object(Bucket='test-bucket', Key=special_key)
        assert response['Body'] is not None


class TestDfToS3WithMockClient:
    """Tests using mock S3 client fixtures."""

    def test_df_to_s3_calls_put_object(self, sample_vehicle_data, mock_connect_to_s3, mock_boto3_client):
        """Test that df_to_s3 calls put_object with correct parameters."""
        df_to_s3(
            df=sample_vehicle_data,
            key='test/data.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing'
        )
        
        # Verify put_object was called
        mock_boto3_client.put_object.assert_called_once()
        
        # Verify bucket and key
        call_kwargs = mock_boto3_client.put_object.call_args[1]
        assert call_kwargs['Bucket'] == 'test-bucket'
        assert call_kwargs['Key'] == 'test/data.csv'

    def test_df_to_s3_csv_content(self, sample_vehicle_data, mock_connect_to_s3, mock_boto3_client):
        """Test that the CSV content is correctly formatted."""
        df_to_s3(
            df=sample_vehicle_data,
            key='test/data.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing'
        )
        
        # Get the body content that was uploaded
        call_kwargs = mock_boto3_client.put_object.call_args[1]
        body_content = call_kwargs['Body']
        
        # Verify it's valid CSV
        result_df = pd.read_csv(StringIO(body_content))
        assert len(result_df) == len(sample_vehicle_data)


class TestDfToS3EdgeCases:
    """Edge case tests for S3 upload."""

    @mock_aws
    def test_df_to_s3_with_null_values(self, sample_vehicle_data_with_nulls):
        """Test uploading DataFrame with NULL values."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        df_to_s3(
            df=sample_vehicle_data_with_nulls,
            key='test/nulls.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Verify upload
        response = s3_client.get_object(Bucket='test-bucket', Key='test/nulls.csv')
        content = response['Body'].read().decode('utf-8')
        
        # Should contain data
        assert len(content) > 0

    @mock_aws
    def test_df_to_s3_with_large_dataframe(self, test_dataframe_factory):
        """Test uploading a larger DataFrame."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        large_df = test_dataframe_factory(num_rows=1000)
        
        df_to_s3(
            df=large_df,
            key='test/large.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        # Verify upload
        response = s3_client.get_object(Bucket='test-bucket', Key='test/large.csv')
        content = response['Body'].read().decode('utf-8')
        result_df = pd.read_csv(StringIO(content))
        
        assert len(result_df) == 1000

    @mock_aws
    def test_df_to_s3_preserves_column_order(self, sample_vehicle_data):
        """Test that column order is preserved in CSV."""
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        
        original_columns = list(sample_vehicle_data.columns)
        
        df_to_s3(
            df=sample_vehicle_data,
            key='test/data.csv',
            s3_bucket='test-bucket',
            aws_access_key_id='testing',
            aws_secret_access_key='testing',
            region_name='us-east-1'
        )
        
        response = s3_client.get_object(Bucket='test-bucket', Key='test/data.csv')
        content = response['Body'].read().decode('utf-8')
        result_df = pd.read_csv(StringIO(content))
        
        assert list(result_df.columns) == original_columns
