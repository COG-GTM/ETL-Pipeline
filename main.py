import os
from datetime import datetime
from dotenv import load_dotenv

from src.extract import extract_vehicle_sales_data
from src.transform import identify_and_remove_duplicated_data
from src.load_data_to_s3 import df_to_s3
from src.validate import validate_data_quality, generate_quality_report
from src.load_quality_report import upload_quality_report

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

# Step 1: Extract data
print("\n🚗 Extracting and transforming vehicle sales + service data...")
vehicle_sales_df = extract_vehicle_sales_data(dbname, host, port, user, password)
print("✅ Extraction complete")

# Step 1.5: Data Quality Validation
print("\n🔍 Running data quality validation...")
validation_results = validate_data_quality(vehicle_sales_df)
quality_report_df = generate_quality_report(vehicle_sales_df, validation_results)

completeness_score = validation_results['completeness']['completeness_score']
integrity_violations = validation_results['integrity']['integrity_violations']
anomaly_count = validation_results['anomalies']['anomaly_count']

if integrity_violations > 0 or anomaly_count > 0:
    print(f"⚠️ Data quality warnings: {integrity_violations} integrity violations, {anomaly_count} anomalies detected")
print(f"✅ Validation complete (Completeness: {completeness_score}%)")

# Step 2: Remove duplicates
print("\n🧹 Removing duplicated rows...")
vehicle_sales_deduped = identify_and_remove_duplicated_data(vehicle_sales_df)
print("✅ Deduplication complete")

# Step 3: Upload to S3
print("\n☁️ Uploading cleaned data to S3...")
s3_bucket = 'cognition-devin'
key = 'auto_oem/etl/vehicle_sales_deduped.csv'

df_to_s3(vehicle_sales_deduped, key, s3_bucket, aws_access_key_id, aws_secret_access_key)
print("✅ Data successfully uploaded to S3")

# Step 4: Upload quality report to S3
print("\n📊 Uploading quality report to S3...")
quality_report_key = 'auto_oem/etl/quality_reports/quality_report.csv'
upload_quality_report(quality_report_df, quality_report_key, s3_bucket, aws_access_key_id, aws_secret_access_key)
print("✅ Quality report uploaded to S3")

# Step 5: Execution time
execution_time = datetime.now() - start_time
print(f"\n⏱️ Total execution time: {execution_time}")
