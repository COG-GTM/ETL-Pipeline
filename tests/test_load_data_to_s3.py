from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd

from src.load_data_to_s3 import df_to_s3


def sample_df():
    return pd.DataFrame({
        "vin": ["1HGCM82633A004352", "2HGCM82633A004353"],
        "sale_price": [23500.50, 31200.00],
        "sale_date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
    })


def test_parquet_round_trip():
    df = sample_df()
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    result = pd.read_parquet(buffer)
    pd.testing.assert_frame_equal(df, result)


def test_df_to_s3_uploads_parquet_bytes():
    df = sample_df()
    mock_client = MagicMock()

    with patch("src.load_data_to_s3.connect_to_s3", return_value=mock_client):
        df_to_s3(df, "auto_oem/etl/vehicle_sales_deduped.parquet", "cognition-devin", "key", "secret")

    mock_client.put_object.assert_called_once()
    kwargs = mock_client.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "cognition-devin"
    assert kwargs["Key"] == "auto_oem/etl/vehicle_sales_deduped.parquet"
    assert kwargs["ContentType"] == "application/octet-stream"
    body = kwargs["Body"]
    assert isinstance(body, bytes)
    result = pd.read_parquet(BytesIO(body))
    pd.testing.assert_frame_equal(df, result)
