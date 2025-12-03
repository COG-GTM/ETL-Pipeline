import boto3
import botocore.exceptions
from io import BytesIO
from datetime import datetime


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3_parquet(df, base_key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    """
    Upload DataFrame to S3 as Parquet files with Snappy compression.
    Data is partitioned by region and files are named with current timestamp.
    
    Args:
        df: pandas DataFrame to upload
        base_key: Base S3 key path (e.g., 'auto_oem/etl/vehicle_sales')
        s3_bucket: S3 bucket name
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        region_name: AWS region (default: us-west-2)
    """
    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        total_rows = 0
        
        # Get unique regions for partitioning
        regions = df['region'].unique()
        
        for region in regions:
            # Filter data for this region
            region_df = df[df['region'] == region].copy()
            
            # Create Parquet buffer with Snappy compression
            parquet_buffer = BytesIO()
            region_df.to_parquet(parquet_buffer, index=False, compression='snappy')
            parquet_buffer.seek(0)
            
            # Construct S3 key with partition and timestamp
            s3_key = f"{base_key}/region={region}/{timestamp}.parquet"
            
            # Upload to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue()
            )
            
            total_rows += len(region_df)
            print(f"Uploaded {len(region_df)} rows to s3://{s3_bucket}/{s3_key}")
        
        print(f"Total: Uploaded {total_rows} rows across {len(regions)} region partitions")
        
    except botocore.exceptions.ClientError as e:
        print(f"Failed to upload to S3: {e}")
