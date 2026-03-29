import boto3
import botocore.exceptions
from io import StringIO
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"✅ Uploaded {len(df)} rows to s3://{s3_bucket}/{key}")
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")


def df_to_s3_partitioned(df, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    """
    Upload DataFrame to S3 as partitioned Parquet files by region
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
    
    # Group by region and upload each partition
    for region_value, region_df in df.groupby('region'):
        # Convert DataFrame to Parquet with Snappy compression
        table = pa.Table.from_pandas(region_df)
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, compression='snappy')
        
        # Create partitioned path: auto_oem/etl/region=West/20241203_143022.parquet
        partition_key = f"auto_oem/etl/region={region_value}/{timestamp}.parquet"
        
        try:
            s3_client.put_object(
                Bucket=s3_bucket, 
                Key=partition_key, 
                Body=buf.getvalue().to_pybytes()
            )
            print(f"✅ Uploaded {len(region_df)} rows to s3://{s3_bucket}/{partition_key}")
        except (botocore.exceptions.ClientError, Exception) as e:
            print(f"❌ Failed to upload partition {region_value}: {e}")
