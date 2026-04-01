from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.load_data_to_s3 import connect_to_s3, df_to_s3


class TestConnectToS3:
    """Tests for the connect_to_s3 function."""

    @patch("src.load_data_to_s3.boto3.client")
    def test_creates_s3_client(self, mock_boto3_client):
        """Should create an S3 client with the correct parameters."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        result = connect_to_s3("access_key", "secret_key")

        mock_boto3_client.assert_called_once_with(
            "s3",
            region_name="us-west-2",
            aws_access_key_id="access_key",
            aws_secret_access_key="secret_key",
        )
        assert result is mock_client

    @patch("src.load_data_to_s3.boto3.client")
    def test_custom_region(self, mock_boto3_client):
        """Should pass custom region_name to boto3 client."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client

        connect_to_s3("access_key", "secret_key", region_name="eu-west-1")

        mock_boto3_client.assert_called_once_with(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id="access_key",
            aws_secret_access_key="secret_key",
        )


class TestDfToS3:
    """Tests for the df_to_s3 function."""

    @patch("src.load_data_to_s3.connect_to_s3")
    def test_uploads_csv_to_s3(self, mock_connect):
        """Should convert DataFrame to CSV and upload to S3."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df_to_s3(df, "test/key.csv", "my-bucket", "access", "secret")

        mock_connect.assert_called_once_with("access", "secret", "us-west-2")
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "my-bucket"
        assert call_kwargs["Key"] == "test/key.csv"
        assert "a,b" in call_kwargs["Body"]

    @patch("src.load_data_to_s3.connect_to_s3")
    def test_custom_region(self, mock_connect):
        """Should pass region_name to connect_to_s3."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        df = pd.DataFrame({"a": [1]})
        df_to_s3(df, "key.csv", "bucket", "access", "secret", region_name="eu-west-1")

        mock_connect.assert_called_once_with("access", "secret", "eu-west-1")

    @patch("src.load_data_to_s3.connect_to_s3")
    def test_handles_client_error(self, mock_connect):
        """Should handle ClientError gracefully (print error, not raise)."""
        import botocore.exceptions

        mock_client = MagicMock()
        mock_client.put_object.side_effect = botocore.exceptions.ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "PutObject"
        )
        mock_connect.return_value = mock_client

        df = pd.DataFrame({"a": [1, 2]})
        # Should not raise, just print error
        df_to_s3(df, "key.csv", "bucket", "access", "secret")

    @patch("src.load_data_to_s3.connect_to_s3")
    def test_csv_content_correctness(self, mock_connect):
        """Should produce valid CSV content without index."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        df = pd.DataFrame({"col1": [10, 20], "col2": ["a", "b"]})
        df_to_s3(df, "key.csv", "bucket", "access", "secret")

        body = mock_client.put_object.call_args[1]["Body"]
        lines = body.strip().split("\n")
        assert lines[0] == "col1,col2"
        assert lines[1] == "10,a"
        assert lines[2] == "20,b"

    @patch("src.load_data_to_s3.connect_to_s3")
    def test_empty_dataframe(self, mock_connect):
        """Should upload an empty CSV (header only) for empty DataFrame."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        df = pd.DataFrame({"a": [], "b": []})
        df_to_s3(df, "key.csv", "bucket", "access", "secret")

        body = mock_client.put_object.call_args[1]["Body"]
        lines = body.strip().split("\n")
        assert lines[0] == "a,b"
