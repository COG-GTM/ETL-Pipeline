"""
Unit tests for the extract module (src/extract.py).

This module tests:
- connect_to_postgres() function
- extract_vehicle_sales_data() function

Tests use mocked database connections to avoid requiring a real PostgreSQL instance.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.extract import connect_to_postgres, extract_vehicle_sales_data


class TestConnectToPostgres:
    """Tests for the connect_to_postgres function."""

    def test_connect_to_postgres_success(self, mock_psycopg2_connect):
        """Test successful database connection."""
        conn = connect_to_postgres(
            dbname='test_db',
            host='localhost',
            port='5432',
            user='test_user',
            password='test_password'
        )
        
        # Verify psycopg2.connect was called with correct parameters
        mock_psycopg2_connect.assert_called_once_with(
            dbname='test_db',
            host='localhost',
            port='5432',
            user='test_user',
            password='test_password'
        )
        
        # Verify a connection object was returned
        assert conn is not None

    def test_connect_to_postgres_with_credentials(self, mock_psycopg2_connect, test_db_credentials):
        """Test connection using test credentials fixture."""
        conn = connect_to_postgres(**test_db_credentials)
        
        mock_psycopg2_connect.assert_called_once()
        assert conn is not None

    def test_connect_to_postgres_connection_error(self):
        """Test handling of connection errors."""
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection refused")
            
            with pytest.raises(Exception) as exc_info:
                connect_to_postgres(
                    dbname='test_db',
                    host='invalid_host',
                    port='5432',
                    user='test_user',
                    password='test_password'
                )
            
            assert "Connection refused" in str(exc_info.value)


class TestExtractVehicleSalesData:
    """Tests for the extract_vehicle_sales_data function."""

    def test_extract_vehicle_sales_data_returns_dataframe(
        self, mock_psycopg2_connect, mock_pandas_read_sql, test_db_credentials
    ):
        """Test that extract_vehicle_sales_data returns a DataFrame."""
        df = extract_vehicle_sales_data(**test_db_credentials)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_extract_vehicle_sales_data_has_expected_columns(
        self, mock_psycopg2_connect, mock_pandas_read_sql, test_db_credentials
    ):
        """Test that the returned DataFrame has all expected columns."""
        df = extract_vehicle_sales_data(**test_db_credentials)
        
        expected_columns = [
            'vin', 'model', 'year', 'dealership_name', 'region',
            'sale_date', 'sale_price', 'buyer_name',
            'service_date', 'service_type', 'service_cost'
        ]
        
        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

    def test_extract_vehicle_sales_data_date_conversion(
        self, mock_psycopg2_connect, mock_pandas_read_sql, test_db_credentials
    ):
        """Test that date columns are converted to datetime."""
        df = extract_vehicle_sales_data(**test_db_credentials)
        
        # Check that sale_date and service_date are datetime types
        assert pd.api.types.is_datetime64_any_dtype(df['sale_date'])
        assert pd.api.types.is_datetime64_any_dtype(df['service_date'])

    def test_extract_vehicle_sales_data_sql_query_structure(
        self, mock_psycopg2_connect, test_db_credentials
    ):
        """Test that the SQL query joins the correct tables."""
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame({
                'vin': [], 'model': [], 'year': [], 'dealership_name': [],
                'region': [], 'sale_date': [], 'sale_price': [], 'buyer_name': [],
                'service_date': [], 'service_type': [], 'service_cost': []
            })
            
            extract_vehicle_sales_data(**test_db_credentials)
            
            # Get the SQL query that was passed to read_sql
            call_args = mock_read_sql.call_args
            sql_query = call_args[0][0]
            
            # Verify the query joins the expected tables
            assert 'vehicles' in sql_query.lower()
            assert 'dealerships' in sql_query.lower()
            assert 'sales_transactions' in sql_query.lower()
            assert 'service_records' in sql_query.lower()
            assert 'join' in sql_query.lower()

    def test_extract_vehicle_sales_data_handles_null_service_type(
        self, mock_psycopg2_connect, test_db_credentials
    ):
        """Test that NULL service_type is handled with COALESCE."""
        with patch('pandas.read_sql') as mock_read_sql:
            # Simulate data with NULL service_type replaced by 'Unknown'
            mock_read_sql.return_value = pd.DataFrame({
                'vin': ['TEST1'],
                'model': ['Camry'],
                'year': [2021],
                'dealership_name': ['Test Dealer'],
                'region': ['West'],
                'sale_date': ['2022-01-15'],
                'sale_price': [28000.00],
                'buyer_name': ['Test Buyer'],
                'service_date': ['2022-07-15'],
                'service_type': ['Unknown'],  # COALESCE result
                'service_cost': [0]  # COALESCE result
            })
            
            df = extract_vehicle_sales_data(**test_db_credentials)
            
            # Verify the query uses COALESCE for service_type
            call_args = mock_read_sql.call_args
            sql_query = call_args[0][0]
            assert 'coalesce' in sql_query.lower()

    def test_extract_vehicle_sales_data_empty_result(
        self, mock_psycopg2_connect, test_db_credentials
    ):
        """Test handling of empty query results."""
        with patch('pandas.read_sql') as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame({
                'vin': [], 'model': [], 'year': [], 'dealership_name': [],
                'region': [], 'sale_date': [], 'sale_price': [], 'buyer_name': [],
                'service_date': [], 'service_type': [], 'service_cost': []
            })
            
            df = extract_vehicle_sales_data(**test_db_credentials)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 0


class TestExtractWithSampleData:
    """Tests using sample data fixtures."""

    def test_extract_with_sample_vehicle_data(
        self, mock_psycopg2_connect, sample_vehicle_data, test_db_credentials
    ):
        """Test extraction with sample vehicle data fixture."""
        with patch('pandas.read_sql', return_value=sample_vehicle_data):
            df = extract_vehicle_sales_data(**test_db_credentials)
            
            assert len(df) == len(sample_vehicle_data)
            assert list(df.columns) == list(sample_vehicle_data.columns)

    def test_extract_with_null_data(
        self, mock_psycopg2_connect, sample_vehicle_data_with_nulls, test_db_credentials
    ):
        """Test extraction handles NULL values correctly."""
        with patch('pandas.read_sql', return_value=sample_vehicle_data_with_nulls):
            df = extract_vehicle_sales_data(**test_db_credentials)
            
            assert isinstance(df, pd.DataFrame)
            # Verify NULL handling doesn't cause errors
            assert len(df) == len(sample_vehicle_data_with_nulls)
