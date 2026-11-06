from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.extract import connect_to_postgres, extract_vehicle_sales_data


class TestConnectToPostgres:
    """Tests for the connect_to_postgres function."""

    @patch("src.extract.psycopg2.connect")
    def test_successful_connection(self, mock_connect):
        """Should call psycopg2.connect with the correct parameters."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        result = connect_to_postgres("mydb", "localhost", "5432", "user", "pass")

        mock_connect.assert_called_once_with(
            dbname="mydb",
            host="localhost",
            port="5432",
            user="user",
            password="pass",
        )
        assert result is mock_conn

    @patch("src.extract.psycopg2.connect")
    def test_connection_failure_raises(self, mock_connect):
        """Should propagate exceptions when connection fails."""
        mock_connect.side_effect = Exception("Connection refused")

        with pytest.raises(Exception, match="Connection refused"):
            connect_to_postgres("mydb", "localhost", "5432", "user", "pass")


class TestExtractVehicleSalesData:
    """Tests for the extract_vehicle_sales_data function."""

    @patch("src.extract.pd.read_sql")
    @patch("src.extract.connect_to_postgres")
    def test_returns_dataframe(self, mock_connect, mock_read_sql):
        """Should return a DataFrame with the expected columns."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_df = pd.DataFrame({
            "vin": ["VIN001", "VIN002"],
            "model": ["Model A", "Model B"],
            "year": [2020, 2021],
            "dealership_name": ["Dealer 1", "Dealer 2"],
            "region": ["North", "South"],
            "sale_date": ["2023-01-01", "2023-02-01"],
            "sale_price": [25000, 30000],
            "buyer_name": ["John", "Jane"],
            "service_date": ["2023-06-01", None],
            "service_type": ["Oil Change", "Unknown"],
            "service_cost": [50, 0],
        })
        mock_read_sql.return_value = mock_df

        result = extract_vehicle_sales_data("db", "host", "5432", "user", "pass")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        mock_connect.assert_called_once_with("db", "host", "5432", "user", "pass")

    @patch("src.extract.pd.read_sql")
    @patch("src.extract.connect_to_postgres")
    def test_converts_dates_to_datetime(self, mock_connect, mock_read_sql):
        """Should convert sale_date and service_date to datetime objects."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_df = pd.DataFrame({
            "vin": ["VIN001"],
            "model": ["Model A"],
            "year": [2020],
            "dealership_name": ["Dealer 1"],
            "region": ["North"],
            "sale_date": ["2023-01-15"],
            "sale_price": [25000],
            "buyer_name": ["John"],
            "service_date": ["2023-06-01"],
            "service_type": ["Oil Change"],
            "service_cost": [50],
        })
        mock_read_sql.return_value = mock_df

        result = extract_vehicle_sales_data("db", "host", "5432", "user", "pass")

        assert pd.api.types.is_datetime64_any_dtype(result["sale_date"])
        assert pd.api.types.is_datetime64_any_dtype(result["service_date"])

    @patch("src.extract.pd.read_sql")
    @patch("src.extract.connect_to_postgres")
    def test_handles_null_dates(self, mock_connect, mock_read_sql):
        """Should handle null dates with coerce (NaT)."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        mock_df = pd.DataFrame({
            "vin": ["VIN001"],
            "model": ["Model A"],
            "year": [2020],
            "dealership_name": ["Dealer 1"],
            "region": ["North"],
            "sale_date": [None],
            "sale_price": [25000],
            "buyer_name": ["John"],
            "service_date": [None],
            "service_type": ["Unknown"],
            "service_cost": [0],
        })
        mock_read_sql.return_value = mock_df

        result = extract_vehicle_sales_data("db", "host", "5432", "user", "pass")

        assert pd.isna(result["sale_date"].iloc[0])
        assert pd.isna(result["service_date"].iloc[0])

    @patch("src.extract.pd.read_sql")
    @patch("src.extract.connect_to_postgres")
    def test_passes_query_to_read_sql(self, mock_connect, mock_read_sql):
        """Should execute the SQL query via pd.read_sql."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_read_sql.return_value = pd.DataFrame({
            "vin": [], "model": [], "year": [],
            "dealership_name": [], "region": [],
            "sale_date": [], "sale_price": [], "buyer_name": [],
            "service_date": [], "service_type": [], "service_cost": [],
        })

        extract_vehicle_sales_data("db", "host", "5432", "user", "pass")

        mock_read_sql.assert_called_once()
        call_args = mock_read_sql.call_args
        query = call_args[0][0]
        assert "vehicles" in query
        assert "dealerships" in query
        assert "sales_transactions" in query
        assert "service_records" in query

    @patch("src.extract.connect_to_postgres")
    def test_connection_error_propagates(self, mock_connect):
        """Should propagate connection errors."""
        mock_connect.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            extract_vehicle_sales_data("db", "host", "5432", "user", "pass")
