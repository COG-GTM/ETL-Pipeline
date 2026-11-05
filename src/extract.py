import ipaddress
import os

import psycopg2
import pandas as pd

DEFAULT_SSLMODE = "verify-full"
SECURE_SSLMODES = ("require", "verify-ca", "verify-full")
LOOPBACK_ONLY_SSLMODES = ("disable", "allow", "prefer")
LOOPBACK_HOSTNAMES = ("localhost",)


def _is_loopback_host(host):
    """Return True when ``host`` is a Unix socket or a loopback address."""
    host = host or os.getenv("PGHOST")
    if not host:
        return True
    hosts = [h.strip() for h in str(host).split(",")]
    for name in hosts:
        if name.startswith("/") or name.lower() in LOOPBACK_HOSTNAMES:
            continue
        try:
            if ipaddress.ip_address(name).is_loopback:
                continue
        except ValueError:
            pass
        return False
    return True


def resolve_ssl_options(host, sslmode=None, sslrootcert=None):
    """
    Build the libpq TLS options for a connection.

    ``sslmode`` defaults to ``DB_SSLMODE`` and then ``verify-full`` and
    ``sslrootcert`` defaults to ``DB_SSLROOTCERT``. Modes that allow
    cleartext or skip certificate checks are only accepted for loopback
    hosts and Unix sockets.
    """
    mode = (sslmode or os.getenv("DB_SSLMODE") or DEFAULT_SSLMODE)
    mode = mode.strip().lower()
    rootcert = sslrootcert or os.getenv("DB_SSLROOTCERT") or None

    if mode in LOOPBACK_ONLY_SSLMODES:
        if not _is_loopback_host(host):
            raise ValueError(
                f"DB_SSLMODE={mode} is only allowed for local databases; "
                f"use one of {', '.join(SECURE_SSLMODES)} for host {host!r}"
            )
    elif mode not in SECURE_SSLMODES:
        raise ValueError(f"Unsupported DB_SSLMODE: {mode!r}")

    options = {"sslmode": mode}
    if rootcert:
        options["sslrootcert"] = rootcert
    return options


def connect_to_postgres(dbname, host, port, user, password,
                        sslmode=None, sslrootcert=None):
    """Connects to a local or remote PostgreSQL database over TLS"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password,
        **resolve_ssl_options(host, sslmode, sslrootcert)
    )
    print("✅ Connected to PostgreSQL")
    return conn


def extract_vehicle_sales_data(dbname, host, port, user, password):
    """
    Extract and transform vehicle sales and service data.
    - Joins vehicles, dealerships, sales_transactions, and service_records
    - Replaces null service type/cost with defaults
    - Computes total sales revenue per transaction
    - Formats dates as datetime
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

    # Convert dates to datetime objects
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

    print("🔍 Extracted rows:", df.shape[0])
    return df
