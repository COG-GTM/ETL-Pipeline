import pandas as pd
from io import StringIO

from src.extract import connect_to_postgres
from src.load_data_to_s3 import connect_to_s3


class PandasImpl:
    """Pandas implementation wrapper for the ETL pipeline operations."""

    name = "pandas"

    def extract_data(self, dbname, host, port, user, password):
        """
        Extract vehicle sales data from PostgreSQL using pandas.
        Uses pd.read_sql() for data extraction.
        """
        conn = connect_to_postgres(dbname, host, port, user, password)

        query = """
        SELECT
            v.vin,
            v.model,
            v.year,
            d.name AS dealership_name,
            d.region,
            s.sale_date,
            s.sale_price,
            s.buyer_name,
            COALESCE(sr.service_date, NULL) AS service_date,
            COALESCE(sr.service_type, 'Unknown') AS service_type,
            COALESCE(sr.service_cost, 0) AS service_cost
        FROM vehicles v
        JOIN dealerships d ON v.dealership_id = d.id
        LEFT JOIN sales_transactions s ON v.vin = s.vin
        LEFT JOIN service_records sr ON v.vin = sr.vin
        """

        df = pd.read_sql(query, conn)

        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
        df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

        conn.close()
        return df

    def transform_data(self, df, subset=None):
        """
        Remove duplicate rows from the DataFrame using pandas.
        Uses df.duplicated() and df.drop_duplicates().
        """
        duplicate_count = df.duplicated(subset=subset).sum()

        if duplicate_count > 0:
            df_cleaned = df.drop_duplicates(subset=subset, keep='first')
            return df_cleaned
        else:
            return df.copy()

    def load_data(self, df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
        """
        Write DataFrame to CSV and upload to S3 using pandas.
        Uses df.to_csv() for CSV conversion.
        """
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_buffer.getvalue())

        return csv_buffer.getvalue()

    def get_row_count(self, df):
        """Return the number of rows in the DataFrame."""
        return len(df)

    def get_csv_size(self, csv_data):
        """Return the size of the CSV data in bytes."""
        return len(csv_data.encode('utf-8'))
