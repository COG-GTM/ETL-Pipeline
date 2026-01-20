"""
Integration tests for the ETL Pipeline.

This module tests the complete ETL pipeline flow:
- Extract -> Transform -> Load

Tests verify that all components work together correctly.
"""

import pytest
import pandas as pd
import boto3
from io import StringIO
from unittest.mock import patch, MagicMock

from moto import mock_aws

from src.extract import extract_vehicle_sales_data
from src.transform import identify_and_remove_duplicated_data
from src.load_data_to_s3 import df_to_s3


class TestETLPipelineIntegration:
    """Integration tests for the complete ETL pipeline."""

    def test_full_pipeline_with_mocks(
        self, mock_psycopg2_connect, sample_vehicle_data, test_db_credentials
    ):
        """Test the complete ETL pipeline with mocked dependencies."""
        with mock_aws():
            # Setup S3
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            # Mock the database read
            with patch('pandas.read_sql', return_value=sample_vehicle_data):
                # Step 1: Extract
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                assert len(extracted_df) > 0
                
                # Step 2: Transform
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                assert len(transformed_df) <= len(extracted_df)
                
                # Step 3: Load
                df_to_s3(
                    df=transformed_df,
                    key='test/pipeline_output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )
                
                # Verify the data was uploaded
                response = s3_client.get_object(Bucket='test-bucket', Key='test/pipeline_output.csv')
                content = response['Body'].read().decode('utf-8')
                result_df = pd.read_csv(StringIO(content))
                
                assert len(result_df) == len(transformed_df)

    def test_pipeline_with_duplicates(
        self, mock_psycopg2_connect, sample_vehicle_data_with_duplicates, test_db_credentials
    ):
        """Test pipeline correctly removes duplicates."""
        with mock_aws():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            with patch('pandas.read_sql', return_value=sample_vehicle_data_with_duplicates):
                # Extract
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                original_count = len(extracted_df)
                
                # Transform - should remove duplicates
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                
                # Verify duplicates were removed
                assert len(transformed_df) < original_count
                assert transformed_df.duplicated().sum() == 0
                
                # Load
                df_to_s3(
                    df=transformed_df,
                    key='test/deduped_output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )
                
                # Verify uploaded data has no duplicates
                response = s3_client.get_object(Bucket='test-bucket', Key='test/deduped_output.csv')
                content = response['Body'].read().decode('utf-8')
                result_df = pd.read_csv(StringIO(content))
                
                assert result_df.duplicated().sum() == 0

    def test_pipeline_with_null_values(
        self, mock_psycopg2_connect, sample_vehicle_data_with_nulls, test_db_credentials
    ):
        """Test pipeline handles NULL values correctly."""
        with mock_aws():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            with patch('pandas.read_sql', return_value=sample_vehicle_data_with_nulls):
                # Extract
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                
                # Transform
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                
                # Load
                df_to_s3(
                    df=transformed_df,
                    key='test/nulls_output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )
                
                # Verify data was uploaded
                response = s3_client.get_object(Bucket='test-bucket', Key='test/nulls_output.csv')
                content = response['Body'].read().decode('utf-8')
                
                assert len(content) > 0

    def test_pipeline_empty_data(self, mock_psycopg2_connect, test_db_credentials):
        """Test pipeline handles empty data gracefully."""
        empty_df = pd.DataFrame({
            'vin': [], 'model': [], 'year': [], 'dealership_name': [],
            'region': [], 'sale_date': [], 'sale_price': [], 'buyer_name': [],
            'service_date': [], 'service_type': [], 'service_cost': []
        })
        
        with mock_aws():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            with patch('pandas.read_sql', return_value=empty_df):
                # Extract
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                assert len(extracted_df) == 0
                
                # Transform
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                assert len(transformed_df) == 0
                
                # Load
                df_to_s3(
                    df=transformed_df,
                    key='test/empty_output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )
                
                # Verify empty CSV was uploaded (header only)
                response = s3_client.get_object(Bucket='test-bucket', Key='test/empty_output.csv')
                content = response['Body'].read().decode('utf-8')
                result_df = pd.read_csv(StringIO(content))
                
                assert len(result_df) == 0


class TestETLPipelineDataIntegrity:
    """Tests for data integrity throughout the pipeline."""

    def test_data_columns_preserved(
        self, mock_psycopg2_connect, sample_vehicle_data, test_db_credentials
    ):
        """Test that all columns are preserved through the pipeline."""
        with mock_aws():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            with patch('pandas.read_sql', return_value=sample_vehicle_data):
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                original_columns = set(extracted_df.columns)
                
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                
                df_to_s3(
                    df=transformed_df,
                    key='test/output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )
                
                response = s3_client.get_object(Bucket='test-bucket', Key='test/output.csv')
                content = response['Body'].read().decode('utf-8')
                result_df = pd.read_csv(StringIO(content))
                
                assert set(result_df.columns) == original_columns

    def test_data_values_preserved(
        self, mock_psycopg2_connect, sample_vehicle_data, test_db_credentials
    ):
        """Test that data values are preserved through the pipeline."""
        with mock_aws():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            with patch('pandas.read_sql', return_value=sample_vehicle_data):
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                
                # Get first VIN before transformation
                first_vin = extracted_df.iloc[0]['vin']
                
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                
                df_to_s3(
                    df=transformed_df,
                    key='test/output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )
                
                response = s3_client.get_object(Bucket='test-bucket', Key='test/output.csv')
                content = response['Body'].read().decode('utf-8')
                result_df = pd.read_csv(StringIO(content))
                
                # Verify the first VIN is still present
                assert first_vin in result_df['vin'].values


class TestETLPipelinePerformance:
    """Performance tests for the ETL pipeline."""

    def test_pipeline_with_large_dataset(
        self, mock_psycopg2_connect, test_dataframe_factory, test_db_credentials
    ):
        """Test pipeline performance with larger dataset."""
        large_df = test_dataframe_factory(num_rows=500)
        # Add some duplicates
        duplicates = large_df.head(50).copy()
        large_df_with_dups = pd.concat([large_df, duplicates], ignore_index=True)
        
        with mock_aws():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            with patch('pandas.read_sql', return_value=large_df_with_dups):
                # Extract
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                assert len(extracted_df) == 550
                
                # Transform
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                assert len(transformed_df) == 500  # Duplicates removed
                
                # Load
                df_to_s3(
                    df=transformed_df,
                    key='test/large_output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )
                
                # Verify
                response = s3_client.get_object(Bucket='test-bucket', Key='test/large_output.csv')
                content = response['Body'].read().decode('utf-8')
                result_df = pd.read_csv(StringIO(content))
                
                assert len(result_df) == 500


class TestETLPipelineErrorHandling:
    """Tests for error handling in the pipeline."""

    def test_pipeline_continues_after_transform_with_no_duplicates(
        self, mock_psycopg2_connect, sample_vehicle_data, test_db_credentials
    ):
        """Test pipeline continues when no duplicates are found."""
        with mock_aws():
            s3_client = boto3.client('s3', region_name='us-east-1')
            s3_client.create_bucket(Bucket='test-bucket')
            
            with patch('pandas.read_sql', return_value=sample_vehicle_data):
                extracted_df = extract_vehicle_sales_data(**test_db_credentials)
                
                # Transform should not fail when no duplicates
                transformed_df = identify_and_remove_duplicated_data(extracted_df)
                assert len(transformed_df) == len(extracted_df)
                
                # Load should succeed
                df_to_s3(
                    df=transformed_df,
                    key='test/output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing',
                    region_name='us-east-1'
                )

    def test_pipeline_with_s3_error(
        self, mock_psycopg2_connect, sample_vehicle_data, test_db_credentials, capsys
    ):
        """Test pipeline handles S3 errors gracefully."""
        with patch('pandas.read_sql', return_value=sample_vehicle_data):
            extracted_df = extract_vehicle_sales_data(**test_db_credentials)
            transformed_df = identify_and_remove_duplicated_data(extracted_df)
            
            # Mock S3 to raise an error
            with patch('src.load_data_to_s3.connect_to_s3') as mock_connect:
                mock_client = MagicMock()
                mock_client.put_object.side_effect = Exception("S3 Error")
                mock_connect.return_value = mock_client
                
                # Should not raise, but print error
                df_to_s3(
                    df=transformed_df,
                    key='test/output.csv',
                    s3_bucket='test-bucket',
                    aws_access_key_id='testing',
                    aws_secret_access_key='testing'
                )
                
                captured = capsys.readouterr()
                assert 'Failed' in captured.out or 'Error' in captured.out
