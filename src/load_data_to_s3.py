import boto3
import botocore.exceptions
from io import StringIO, BytesIO


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2', file_format='parquet'):
    if file_format == 'parquet':
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, compression='snappy')
        buffer.seek(0)
        content_type = 'application/octet-stream'
        body = buffer.getvalue()
    else:
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        content_type = 'text/csv'
        body = buffer.getvalue()

    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=body, ContentType=content_type)
        print(f"✅ Uploaded {len(df)} rows to s3://{s3_bucket}/{key}")
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
