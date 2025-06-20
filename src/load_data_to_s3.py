import boto3
import botocore.exceptions
from io import BytesIO
import pandas as pd
from datetime import datetime


def connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    return boto3.client(
        "s3",
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


def df_to_s3(df, key_prefix, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
    """
    Upload DataFrame to S3 as Parquet files, partitioned by region with timestamp-based naming
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        
        regions = df['region'].unique()
        total_uploaded = 0
        
        for region in regions:
            region_df = df[df['region'] == region]
            
            parquet_buffer = BytesIO()
            region_df.to_parquet(
                parquet_buffer, 
                engine='pyarrow', 
                compression='snappy',
                index=False
            )
            
            filename = f"vehicle_sales_{region.lower()}_{timestamp}.parquet"
            full_key = f"{key_prefix.rstrip('/')}/{filename}"
            
            s3_client.put_object(
                Bucket=s3_bucket, 
                Key=full_key, 
                Body=parquet_buffer.getvalue()
            )
            
            total_uploaded += len(region_df)
            print(f"✅ Uploaded {len(region_df)} rows for region '{region}' to s3://{s3_bucket}/{full_key}")
        
        print(f"✅ Total uploaded: {total_uploaded} rows across {len(regions)} regions")
        
    except botocore.exceptions.ClientError as e:
        print(f"❌ Failed to upload to S3: {e}")
    except Exception as e:
        print(f"❌ Failed to create Parquet files: {e}")
