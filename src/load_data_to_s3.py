import os
import math

import boto3
import botocore.exceptions
from io import StringIO


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key, s3_bucket, aws_access_key_id, aws_secret_access_key,
             region_name='us-west-2', batch_size=None):
    """
    Upload a DataFrame to S3 as CSV.

    Parameters:
    - df: DataFrame to upload
    - key: S3 object key (e.g., 'path/to/file.csv')
    - s3_bucket: S3 bucket name
    - aws_access_key_id, aws_secret_access_key: AWS credentials
    - region_name: AWS region (default: 'us-west-2')
    - batch_size: if set, split the DataFrame into multiple part files of
      this many rows each (e.g., 'file_part001.csv', 'file_part002.csv').
      If None, upload as a single file (original behavior).
    """
    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)

        if batch_size is None or len(df) <= batch_size:
            # Upload as a single file (original behavior)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())
            print(f"✅ Uploaded {len(df)} rows to s3://{s3_bucket}/{key}")
        else:
            # Split into multiple part files
            total_parts = math.ceil(len(df) / batch_size)
            base, ext = os.path.splitext(key)

            for part_num in range(total_parts):
                start_idx = part_num * batch_size
                end_idx = min(start_idx + batch_size, len(df))
                chunk = df.iloc[start_idx:end_idx]

                part_key = f"{base}_part{part_num + 1:03d}{ext}"
                csv_buffer = StringIO()
                chunk.to_csv(csv_buffer, index=False)
                s3_client.put_object(Bucket=s3_bucket, Key=part_key, Body=csv_buffer.getvalue())
                print(f"  ✅ Uploaded part {part_num + 1}/{total_parts} "
                      f"({len(chunk)} rows) to s3://{s3_bucket}/{part_key}")

            print(f"✅ Uploaded {len(df)} rows in {total_parts} parts to s3://{s3_bucket}/{base}*{ext}")

    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
