import pytest
import os
import sys
import pandas as pd

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment variables"""
    os.environ['DB_NAME'] = 'test_db'
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_USER'] = 'test_user'
    os.environ['DB_PASSWORD'] = 'test_password'
    os.environ['aws_access_key_id'] = 'test_key'
    os.environ['aws_secret_access_key_id'] = 'test_secret'

@pytest.fixture
def sample_vehicle_data():
    """Sample vehicle data for testing"""
    return pd.DataFrame({
        'vin': ['1HGCM82633A004352', '1HGCM82633A004353'],
        'model': ['Camry', 'Corolla'],
        'year': [2021, 2022],
        'region': ['West', 'Central'],
        'sale_price': [28000, 22000]
    })
