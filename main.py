import os
import sys
from datetime import datetime
from dotenv import load_dotenv

from src.extract import extract_vehicle_sales_data
from src.transform import identify_and_remove_duplicated_data
from src.load_data_to_s3 import df_to_s3

DEFAULT_S3_KEY = 'auto_oem/etl/vehicle_sales_deduped.csv'


def load_s3_config():
    """Read the S3 destination from the environment; exit if it is incomplete."""
    s3_bucket = os.getenv("S3_BUCKET")
    expected_bucket_owner = os.getenv("S3_EXPECTED_BUCKET_OWNER")
    key = os.getenv("S3_KEY") or DEFAULT_S3_KEY
    missing = [name for name, value in (("S3_BUCKET", s3_bucket),
                                        ("S3_EXPECTED_BUCKET_OWNER", expected_bucket_owner))
               if not value]
    if missing:
        sys.exit(f"❌ Missing required configuration: {', '.join(missing)}")
    if not (expected_bucket_owner.isdigit() and len(expected_bucket_owner) == 12):
        sys.exit("❌ S3_EXPECTED_BUCKET_OWNER must be a 12-digit AWS account ID")
    return s3_bucket, key, expected_bucket_owner


def main():
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
    s3_bucket, key, expected_bucket_owner = load_s3_config()

    # Track start time
    start_time = datetime.now()

    # Step 1: Extract data
    print("\n🚗 Extracting and transforming vehicle sales + service data...")
    vehicle_sales_df = extract_vehicle_sales_data(dbname, host, port, user, password)
    print("✅ Extraction complete")

    # Step 2: Remove duplicates
    print("\n🧹 Removing duplicated rows...")
    vehicle_sales_deduped = identify_and_remove_duplicated_data(vehicle_sales_df)
    print("✅ Deduplication complete")

    # Step 3: Upload to S3
    print("\n☁️ Uploading cleaned data to S3...")
    df_to_s3(vehicle_sales_deduped, key, s3_bucket, aws_access_key_id, aws_secret_access_key,
             expected_bucket_owner=expected_bucket_owner)
    print("✅ Data successfully uploaded to S3")

    # Step 4: Execution time
    execution_time = datetime.now() - start_time
    print(f"\n⏱️ Total execution time: {execution_time}")


if __name__ == "__main__":
    main()
