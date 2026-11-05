import boto3
import botocore.exceptions
import pandas as pd
from io import StringIO

# Cells starting with these characters are evaluated as formulas by spreadsheet apps (OWASP CSV injection).
FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def neutralize_csv_formulas(df):
    """Return a copy of df where text cells that a spreadsheet would treat as a formula are prefixed with a single quote."""
    def escape(value):
        if isinstance(value, str) and value.startswith(FORMULA_PREFIXES):
            return "'" + value
        return value

    safe_df = df.copy()
    for column in safe_df.columns:
        series = safe_df[column]
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            safe_df[column] = series.map(escape)
    return safe_df


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    csv_buffer = StringIO()
    # CRLF line terminator makes the csv writer quote fields with embedded CR/LF, so they cannot start a new row.
    neutralize_csv_formulas(df).to_csv(csv_buffer, index=False, lineterminator="\r\n")

    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"✅ Uploaded {len(df)} rows to s3://{s3_bucket}/{key}")
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
