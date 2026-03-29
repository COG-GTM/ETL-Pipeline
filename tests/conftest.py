import os
import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


@pytest.fixture(scope="session")
def test_db():
    """
    Creates an isolated test database for testing.
    
    This fixture:
    - Creates a test database named 'test_etl_db'
    - Executes the SQL schema from init/init.sql to set up tables
    - Yields the database name for use in tests
    - Drops the test database after tests complete
    """
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "password")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    test_db_name = "test_etl_db"
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    init_sql_path = os.path.join(project_root, "init", "init.sql")
    
    conn = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
    cursor.execute(f"CREATE DATABASE {test_db_name}")
    cursor.close()
    conn.close()
    
    conn = psycopg2.connect(
        dbname=test_db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    with conn.cursor() as cursor:
        with open(init_sql_path, "r") as f:
            cursor.execute(f.read())
    conn.commit()
    conn.close()
    
    yield test_db_name
    
    conn = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
    cursor.close()
    conn.close()
