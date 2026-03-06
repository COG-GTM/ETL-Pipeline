import os
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

from src.extract import extract_vehicle_sales_data_chunked
from src.transform import identify_and_remove_duplicated_data
from src.load_data_to_s3 import df_to_s3

# Load environment variables (only needed for local/dev testing)
load_dotenv()

# Read database and AWS credentials from .env
dbname = os.getenv("DB_NAME")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key_id")

# Track start time
start_time = datetime.now()

# Step 1: Extract and process data in batches
print("\n🚗 Extracting and transforming vehicle sales + service data in batches...")
processed_chunks = []
chunk_size = 10000

for i, chunk in enumerate(extract_vehicle_sales_data_chunked(
        dbname, host, port, user, password, chunk_size=chunk_size)):
    print(f"  Processing batch {i + 1} ({len(chunk)} rows)...")
    deduped_chunk = identify_and_remove_duplicated_data(chunk)
    processed_chunks.append(deduped_chunk)

vehicle_sales_deduped = pd.concat(processed_chunks, ignore_index=True)
print(f"✅ Batch processing complete. Total rows: {len(vehicle_sales_deduped)}")

# Step 2: Final cross-batch deduplication (records could duplicate across chunks)
print("\n🧹 Running cross-batch deduplication...")
vehicle_sales_deduped = identify_and_remove_duplicated_data(vehicle_sales_deduped)
print("✅ Cross-batch deduplication complete")

# Step 3: Upload to S3
print("\n☁️ Uploading cleaned data to S3...")
s3_bucket = 'cognition-devin'
key = 'auto_oem/etl/vehicle_sales_deduped.csv'

df_to_s3(vehicle_sales_deduped, key, s3_bucket, aws_access_key_id, aws_secret_access_key)
print("✅ Data successfully uploaded to S3")

# Step 4: Execution time
execution_time = datetime.now() - start_time
print(f"\n⏱️ Total execution time: {execution_time}")
