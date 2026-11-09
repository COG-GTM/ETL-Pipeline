import ipaddress
import os

import psycopg2
import pandas as pd

DEFAULT_SSLMODE = "verify-full"
REMOTE_SSLMODES = ("verify-full",)
LOCAL_ONLY_SSLMODES = ("disable", "allow", "prefer", "require", "verify-ca")
LOOPBACK_HOSTNAMES = ("localhost",)
DEFAULT_CONNECT_TIMEOUT = 10


def _all_loopback(value, allow_names):
    """Return True when every entry of a libpq host/hostaddr list is local."""
    for name in (h.strip() for h in str(value).split(",")):
        if not name:
            continue
        if allow_names and (name.startswith("/")
                            or name.lower() in LOOPBACK_HOSTNAMES):
            continue
        try:
            if ipaddress.ip_address(name).is_loopback:
                continue
        except ValueError:
            pass
        return False
    return True


def _is_loopback_host(host):
    """
    Return True when libpq will only reach a Unix socket or loopback address.

    ``PGHOSTADDR`` and ``PGSERVICE`` can redirect libpq to another address
    regardless of ``host``, so they must be local (or unset) as well.
    """
    if os.getenv("PGSERVICE"):
        return False
    hostaddr = os.getenv("PGHOSTADDR")
    if hostaddr and not _all_loopback(hostaddr, allow_names=False):
        return False
    host = host or os.getenv("PGHOST")
    if not host:
        return True
    return _all_loopback(host, allow_names=True)


def resolve_ssl_options(host, sslmode=None, sslrootcert=None):
    """
    Build the libpq TLS options for a connection.

    ``sslmode`` defaults to ``DB_SSLMODE`` and then ``verify-full`` and
    ``sslrootcert`` defaults to ``DB_SSLROOTCERT``. Remote databases must
    use ``verify-full``; modes that allow cleartext or skip the certificate
    or hostname check are only accepted for loopback hosts and Unix sockets.
    """
    mode = (sslmode or os.getenv("DB_SSLMODE") or DEFAULT_SSLMODE)
    mode = mode.strip().lower()
    rootcert = sslrootcert or os.getenv("DB_SSLROOTCERT") or None

    if mode in LOCAL_ONLY_SSLMODES:
        if not _is_loopback_host(host):
            raise ValueError(
                f"DB_SSLMODE={mode} is only allowed for local databases; "
                f"use {', '.join(REMOTE_SSLMODES)} for host {host!r}"
            )
    elif mode not in REMOTE_SSLMODES:
        raise ValueError(f"Unsupported DB_SSLMODE: {mode!r}")

    options = {"sslmode": mode}
    if rootcert:
        options["sslrootcert"] = rootcert
    return options


def resolve_connect_timeout(connect_timeout=None):
    """
    Return the libpq ``connect_timeout`` in seconds.

    Defaults to ``DB_CONNECT_TIMEOUT`` and then ``DEFAULT_CONNECT_TIMEOUT``.
    Zero or negative values would make libpq wait forever, so they are rejected.
    """
    raw = connect_timeout
    if raw is None:
        raw = os.getenv("DB_CONNECT_TIMEOUT") or DEFAULT_CONNECT_TIMEOUT
    try:
        timeout = int(str(raw).strip())
    except ValueError:
        raise ValueError(f"Invalid DB_CONNECT_TIMEOUT: {raw!r}") from None
    if timeout <= 0:
        raise ValueError(f"DB_CONNECT_TIMEOUT must be positive, got {raw!r}")
    return timeout


def connect_to_postgres(dbname, host, port, user, password,
                        sslmode=None, sslrootcert=None, connect_timeout=None):
    """Connects to a local or remote PostgreSQL database over TLS"""
    conn = psycopg2.connect(
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        password=password,
        connect_timeout=resolve_connect_timeout(connect_timeout),
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
    - Closes the database connection even if the query fails
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

    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    # Convert dates to datetime objects
    df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
    df['service_date'] = pd.to_datetime(df['service_date'], errors='coerce')

    print("🔍 Extracted rows:", df.shape[0])
    return df
