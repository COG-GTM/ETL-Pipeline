import pandas as pd
import polars as pl
from io import StringIO

from src.extract import connect_to_postgres
from src.load_data_to_s3 import connect_to_s3


class PolarsImpl:
    """Polars implementation for the ETL pipeline operations."""

    name = "polars"

    def extract_data(self, dbname, host, port, user, password):
        """
        Extract vehicle sales data from PostgreSQL.
        Uses pandas for SQL extraction, then converts to Polars DataFrame.
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

        df_pandas = pd.read_sql(query, conn)

        df_pandas['sale_date'] = pd.to_datetime(df_pandas['sale_date'], errors='coerce')
        df_pandas['service_date'] = pd.to_datetime(df_pandas['service_date'], errors='coerce')

        conn.close()

        df_polars = pl.from_pandas(df_pandas)
        return df_polars

    def transform_data(self, df, subset=None):
        """
        Remove duplicate rows from the Polars DataFrame.
        Uses pl.DataFrame.unique() for deduplication.
        """
        if subset is None:
            return df.unique()
        else:
            return df.unique(subset=subset, keep='first')

    def load_data(self, df, key, s3_bucket, aws_access_key_id, aws_secret_access_key, region_name='us-west-2'):
        """
        Write Polars DataFrame to CSV and upload to S3.
        Uses df.write_csv() for CSV conversion.
        """
        csv_buffer = StringIO()
        df.write_csv(csv_buffer)

        csv_data = csv_buffer.getvalue()

        s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body=csv_data)

        return csv_data

    def get_row_count(self, df):
        """Return the number of rows in the Polars DataFrame."""
        return df.height

    def get_csv_size(self, csv_data):
        """Return the size of the CSV data in bytes."""
        return len(csv_data.encode('utf-8'))
