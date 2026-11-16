import re

import boto3
import botocore.exceptions
from io import StringIO

_AWS_ACCOUNT_ID = re.compile(r"\d{12}")


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key, s3_bucket, aws_access_key_id, aws_secret_access_key,
             region_name='us-west-2', *, expected_bucket_owner):
    """Upload df as CSV to s3://s3_bucket/key.

    S3 rejects the request with 403 unless s3_bucket is owned by the AWS
    account expected_bucket_owner. Upload errors are raised, not swallowed.
    """
    if not s3_bucket:
        raise ValueError("s3_bucket is required")
    if not _AWS_ACCOUNT_ID.fullmatch(str(expected_bucket_owner or "")):
        raise ValueError("expected_bucket_owner must be a 12-digit AWS account ID")

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=csv_buffer.getvalue(),
            ExpectedBucketOwner=expected_bucket_owner,
        )
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
        raise
    print(f"✅ Uploaded {len(df)} rows to s3://{s3_bucket}/{key}")
