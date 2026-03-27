import boto3
import botocore.exceptions
from io import StringIO, BytesIO
import os


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2', file_format=None):
    if file_format is None:
        file_extension = os.path.splitext(key)[1].lower()
        if file_extension == '.parquet':
            file_format = 'parquet'
        else:
            file_format = 'csv'  # Default to CSV for backward compatibility
    
    if file_format.lower() == 'parquet':
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        body = buffer.getvalue()
    else:  # CSV format
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        body = buffer.getvalue()

    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=body)
        print(f"✅ Uploaded {len(df)} rows to s3://{s3_bucket}/{key} in {file_format.upper()} format")
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
