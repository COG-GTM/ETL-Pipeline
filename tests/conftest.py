"""
Pytest configuration and fixtures for ETL Pipeline tests.

This module provides:
- Database connection mocks for unit testing the extract module
- S3 client mocks using the moto library
- Test lifecycle management (setup/teardown)
- Common test utilities and fixtures
"""

import os
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
from io import StringIO

import boto3
from moto import mock_aws

# Load test environment variables
from dotenv import load_dotenv

# Path to test fixtures
FIXTURES_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')


# =============================================================================
# Test Environment Setup
# =============================================================================

@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    """Load test environment variables at the start of the test session."""
    env_test_path = os.path.join(os.path.dirname(__file__), '..', '.env.test')
    if os.path.exists(env_test_path):
        load_dotenv(env_test_path)
    yield


@pytest.fixture
def test_db_credentials():
    """Provide test database credentials."""
    return {
        'dbname': os.getenv('DB_NAME', 'etl_db'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'mysecretpassword')
    }


@pytest.fixture
def test_aws_credentials():
    """Provide test AWS credentials for mocked S3."""
    return {
        'aws_access_key_id': os.getenv('aws_access_key_id', 'testing'),
        'aws_secret_access_key': os.getenv('aws_secret_access_key_id', 'testing'),
        'region_name': os.getenv('TEST_S3_REGION', 'us-east-1')
    }


@pytest.fixture
def test_s3_bucket():
    """Provide test S3 bucket name."""
    return os.getenv('TEST_S3_BUCKET', 'test-bucket')


# =============================================================================
# Database Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_db_connection():
    """
    Create a mock database connection for unit testing.
    
    This fixture mocks psycopg2.connect to return a mock connection
    that can be used to test extract_vehicle_sales_data() without
    requiring a real database connection.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def mock_psycopg2_connect(mock_db_connection):
    """
    Patch psycopg2.connect to return a mock connection.
    
    Usage:
        def test_extract(mock_psycopg2_connect):
            # psycopg2.connect is now mocked
            from src.extract import connect_to_postgres
            conn = connect_to_postgres(...)
    """
    with patch('psycopg2.connect', return_value=mock_db_connection) as mock_connect:
        yield mock_connect


@pytest.fixture
def sample_vehicle_data():
    """
    Provide sample vehicle sales data as a DataFrame.
    
    This fixture returns a DataFrame that matches the structure
    returned by extract_vehicle_sales_data().
    """
    data = {
        'vin': ['1HGCM82633A100001', '1HGCM82633A100002', '1HGCM82633A100003'],
        'model': ['Camry', 'Corolla', 'F-150'],
        'year': [2021, 2022, 2023],
        'dealership_name': ['Pacific Coast Motors', 'Pacific Coast Motors', 'Great Lakes Auto'],
        'region': ['West', 'West', 'Central'],
        'sale_date': pd.to_datetime(['2022-01-15', '2022-03-20', '2023-06-10']),
        'sale_price': [28000.00, 22000.00, 45000.00],
        'buyer_name': ['Alice Johnson', 'Bob Smith', 'Carlos Vega'],
        'service_date': pd.to_datetime(['2022-07-15', '2022-09-10', '2023-12-05']),
        'service_type': ['Oil Change', 'Tire Rotation', 'Transmission Service'],
        'service_cost': [75.00, 50.00, 800.00]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_vehicle_data_with_duplicates():
    """
    Provide sample vehicle sales data with duplicate rows.
    
    This fixture is used to test the identify_and_remove_duplicated_data() function.
    """
    data = {
        'vin': ['DUP0000000000001', 'DUP0000000000001', 'DUP0000000000001', 
                'DUP0000000000002', 'DUP0000000000002', 'DUP0000000000003'],
        'model': ['Camry', 'Camry', 'Camry', 'Corolla', 'Corolla', 'F-150'],
        'year': [2021, 2021, 2021, 2022, 2022, 2023],
        'dealership_name': ['Duplicate Test Motors'] * 6,
        'region': ['West'] * 6,
        'sale_date': pd.to_datetime(['2022-01-15'] * 6),
        'sale_price': [28000.00] * 3 + [22000.00] * 2 + [45000.00],
        'buyer_name': ['Alice Johnson'] * 3 + ['Bob Smith'] * 2 + ['Carlos Vega'],
        'service_date': pd.to_datetime(['2022-07-15'] * 3 + ['2022-09-10'] * 2 + ['2023-12-05']),
        'service_type': ['Oil Change'] * 3 + ['Tire Rotation'] * 2 + ['Transmission Service'],
        'service_cost': [75.00] * 3 + [50.00] * 2 + [800.00]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_vehicle_data_with_nulls():
    """
    Provide sample vehicle sales data with NULL values for edge case testing.
    """
    data = {
        'vin': ['EDGE000000000001', 'EDGE000000000002', 'EDGE000000000003'],
        'model': ['Camry', 'Corolla', None],
        'year': [2021, 2022, 2023],
        'dealership_name': ['Edge Case Motors', None, 'Boundary Dealership'],
        'region': ['West', 'Central', None],
        'sale_date': pd.to_datetime(['2022-01-15', None, '2023-06-10']),
        'sale_price': [28000.00, None, 0.00],
        'buyer_name': ['Alice Johnson', 'Bob Smith', None],
        'service_date': pd.to_datetime(['2022-07-15', None, '2023-12-05']),
        'service_type': ['Oil Change', 'Unknown', None],
        'service_cost': [75.00, 0.00, None]
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_pandas_read_sql(sample_vehicle_data):
    """
    Patch pandas.read_sql to return sample vehicle data.
    
    This allows testing extract_vehicle_sales_data() without a real database.
    """
    with patch('pandas.read_sql', return_value=sample_vehicle_data.copy()) as mock_read:
        yield mock_read


# =============================================================================
# S3 Mock Fixtures (using moto)
# =============================================================================

@pytest.fixture
def aws_credentials():
    """Set up mock AWS credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    yield
    # Cleanup is handled by moto


@pytest.fixture
def mock_s3(aws_credentials):
    """
    Create a mocked S3 environment using moto.
    
    This fixture provides a fully functional mock S3 service that can be used
    to test df_to_s3() without connecting to real AWS.
    
    Usage:
        def test_upload(mock_s3, test_s3_bucket):
            # S3 is now mocked
            from src.load_data_to_s3 import df_to_s3
            df_to_s3(df, key, test_s3_bucket, ...)
    """
    with mock_aws():
        # Create S3 client and bucket
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-bucket')
        yield s3_client


@pytest.fixture
def mock_s3_with_bucket(mock_s3, test_s3_bucket):
    """
    Create a mocked S3 environment with the test bucket already created.
    
    Returns the S3 client for verification purposes.
    """
    # Bucket is already created in mock_s3, but ensure it exists
    try:
        mock_s3.head_bucket(Bucket=test_s3_bucket)
    except Exception:
        mock_s3.create_bucket(Bucket=test_s3_bucket)
    return mock_s3


@pytest.fixture
def mock_boto3_client():
    """
    Create a mock boto3 S3 client for unit testing.
    
    This is a simpler mock that doesn't use moto, useful for
    testing error handling and edge cases.
    """
    mock_client = MagicMock()
    mock_client.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
    return mock_client


@pytest.fixture
def mock_connect_to_s3(mock_boto3_client):
    """
    Patch connect_to_s3 to return a mock S3 client.
    """
    with patch('src.load_data_to_s3.connect_to_s3', return_value=mock_boto3_client) as mock_connect:
        yield mock_connect


# =============================================================================
# Test Lifecycle Management
# =============================================================================

@pytest.fixture(autouse=True)
def reset_environment():
    """
    Reset environment state before and after each test.
    
    This ensures tests are isolated and don't affect each other.
    """
    # Store original environment
    original_env = os.environ.copy()
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def capture_stdout(capsys):
    """
    Fixture to capture stdout for testing print statements.
    
    Usage:
        def test_output(capture_stdout):
            print("Hello")
            captured = capture_stdout.readouterr()
            assert "Hello" in captured.out
    """
    return capsys


# =============================================================================
# SQL Fixture Loaders
# =============================================================================

@pytest.fixture
def load_sample_data_sql():
    """Load the sample_data.sql fixture content."""
    sql_path = os.path.join(FIXTURES_DIR, 'sample_data.sql')
    with open(sql_path, 'r') as f:
        return f.read()


@pytest.fixture
def load_duplicate_data_sql():
    """Load the duplicate_data.sql fixture content."""
    sql_path = os.path.join(FIXTURES_DIR, 'duplicate_data.sql')
    with open(sql_path, 'r') as f:
        return f.read()


@pytest.fixture
def load_edge_cases_sql():
    """Load the edge_cases.sql fixture content."""
    sql_path = os.path.join(FIXTURES_DIR, 'edge_cases.sql')
    with open(sql_path, 'r') as f:
        return f.read()


@pytest.fixture
def load_volume_data_sql():
    """Load the volume_data.sql fixture content."""
    sql_path = os.path.join(FIXTURES_DIR, 'volume_data.sql')
    with open(sql_path, 'r') as f:
        return f.read()


# =============================================================================
# Integration Test Fixtures
# =============================================================================

@pytest.fixture
def full_etl_mocks(mock_psycopg2_connect, mock_pandas_read_sql, mock_s3):
    """
    Provide all mocks needed for full ETL pipeline integration testing.
    
    This fixture combines database and S3 mocks for testing the complete
    pipeline from extraction to loading.
    """
    return {
        'db_connect': mock_psycopg2_connect,
        'read_sql': mock_pandas_read_sql,
        's3_client': mock_s3
    }


# =============================================================================
# Utility Functions
# =============================================================================

def create_test_dataframe(num_rows=10):
    """
    Create a test DataFrame with the expected schema.
    
    Args:
        num_rows: Number of rows to generate
        
    Returns:
        DataFrame with vehicle sales data schema
    """
    data = {
        'vin': [f'TEST{str(i).zfill(13)}' for i in range(num_rows)],
        'model': ['Camry', 'Corolla', 'F-150', 'Civic', 'Accord'] * (num_rows // 5 + 1),
        'year': [2020 + (i % 5) for i in range(num_rows)],
        'dealership_name': [f'Dealer {i % 3}' for i in range(num_rows)],
        'region': ['West', 'Central', 'East'] * (num_rows // 3 + 1),
        'sale_date': pd.to_datetime(['2022-01-01'] * num_rows),
        'sale_price': [25000 + (i * 1000) for i in range(num_rows)],
        'buyer_name': [f'Buyer {i}' for i in range(num_rows)],
        'service_date': pd.to_datetime(['2022-06-01'] * num_rows),
        'service_type': ['Oil Change'] * num_rows,
        'service_cost': [75.0] * num_rows
    }
    # Trim lists to exact num_rows
    for key in data:
        data[key] = data[key][:num_rows]
    return pd.DataFrame(data)


@pytest.fixture
def test_dataframe_factory():
    """
    Provide a factory function for creating test DataFrames.
    
    Usage:
        def test_something(test_dataframe_factory):
            df = test_dataframe_factory(num_rows=100)
    """
    return create_test_dataframe
