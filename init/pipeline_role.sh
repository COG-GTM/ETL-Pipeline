#!/bin/bash
# Runs after init.sql (docker-entrypoint-initdb.d is processed in name order).
# Creates a read-only login for main.py so the pipeline does not use the superuser.
# The entrypoint may source this file, so it must not call exit.
if [ -n "${ETL_READER_PASSWORD:-}" ]; then
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v db="$POSTGRES_DB" -v pw="$ETL_READER_PASSWORD" <<'SQL'
CREATE ROLE etl_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'pw';
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO etl_reader;
GRANT USAGE ON SCHEMA public TO etl_reader;
GRANT SELECT ON dealerships, vehicles, sales_transactions, service_records TO etl_reader;
SQL
else
  echo "ETL_READER_PASSWORD not set; skipping etl_reader role"
fi
