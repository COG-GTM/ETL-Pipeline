import boto3
import botocore.exceptions
from io import StringIO
from datetime import datetime


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    """Connect to S3 using provided AWS credentials."""
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def upload_quality_report(report_df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    """
    Upload quality report to S3 with timestamp.
    
    Parameters:
    - report_df: DataFrame containing the quality report
    - key: S3 key (path) for the report
    - s3_bucket: S3 bucket name
    - aws_access_key_id: AWS access key ID
    - aws_secret_access_key: AWS secret access key
    - region_name: AWS region (default: us-west-2)
    
    Raises:
    - botocore.exceptions.ClientError: If upload fails
    """
    csv_buffer = StringIO()
    report_df.to_csv(csv_buffer, index=False)

    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"✅ Uploaded quality report to s3://{s3_bucket}/{key}")
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload quality report to S3: {e}")
        raise
