import random
import string
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


def generate_vin() -> str:
    """Generate a realistic VIN (Vehicle Identification Number)."""
    chars = string.ascii_uppercase.replace('I', '').replace('O', '').replace('Q', '') + string.digits
    return ''.join(random.choices(chars, k=17))


def generate_test_data(num_records: int, seed: Optional[int] = 42) -> pd.DataFrame:
    """
    Generate realistic test data matching the current database schema.

    Parameters:
    - num_records: Number of records to generate (e.g., 1000, 10000, 100000)
    - seed: Random seed for reproducibility (default: 42)

    Returns:
    - DataFrame with columns: vin, model, year, dealership_name, region,
      sale_date, sale_price, buyer_name, service_date, service_type, service_cost
    """
    if seed is not None:
        random.seed(seed)

    models = ['Camry', 'Corolla', 'F-150', 'Civic', 'Accord', 'Mustang', 'Explorer',
              'Silverado', 'RAV4', 'CR-V', 'Highlander', 'Tacoma', 'Wrangler', 'Cherokee']

    dealership_names = ['Bay Area Motors', 'Midwest Auto Hub', 'Atlantic Car Group',
                        'Pacific Coast Cars', 'Mountain View Auto', 'Sunshine Motors',
                        'Northern Lights Auto', 'Desert Valley Cars', 'Coastal Auto Group']

    regions = ['West', 'Central', 'East', 'North', 'South']

    first_names = ['Alice', 'Bob', 'Carlos', 'Diana', 'Edward', 'Fiona', 'George',
                   'Hannah', 'Ivan', 'Julia', 'Kevin', 'Laura', 'Michael', 'Nancy']

    last_names = ['Johnson', 'Smith', 'Vega', 'Williams', 'Brown', 'Davis', 'Miller',
                  'Wilson', 'Moore', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White']

    service_types = ['Oil Change', 'Brake Pads', 'Transmission Flush', 'Tire Rotation',
                     'Battery Replacement', 'Air Filter', 'Spark Plugs', 'Coolant Flush',
                     'Wheel Alignment', 'Unknown']

    base_date = datetime(2020, 1, 1)
    date_range_days = 1825

    data = {
        'vin': [],
        'model': [],
        'year': [],
        'dealership_name': [],
        'region': [],
        'sale_date': [],
        'sale_price': [],
        'buyer_name': [],
        'service_date': [],
        'service_type': [],
        'service_cost': []
    }

    for _ in range(num_records):
        vin = generate_vin()
        model = random.choice(models)
        year = random.randint(2018, 2024)
        dealership_name = random.choice(dealership_names)
        region = random.choice(regions)

        sale_date = base_date + timedelta(days=random.randint(0, date_range_days))
        sale_price = round(random.uniform(15000, 75000), 2)
        buyer_name = f"{random.choice(first_names)} {random.choice(last_names)}"

        if random.random() > 0.2:
            service_date = sale_date + timedelta(days=random.randint(30, 365))
            service_type = random.choice(service_types[:-1])
            service_cost = round(random.uniform(50, 2000), 2)
        else:
            service_date = None
            service_type = 'Unknown'
            service_cost = 0.0

        data['vin'].append(vin)
        data['model'].append(model)
        data['year'].append(year)
        data['dealership_name'].append(dealership_name)
        data['region'].append(region)
        data['sale_date'].append(sale_date)
        data['sale_price'].append(sale_price)
        data['buyer_name'].append(buyer_name)
        data['service_date'].append(service_date)
        data['service_type'].append(service_type)
        data['service_cost'].append(service_cost)

    df = pd.DataFrame(data)

    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df['service_date'] = pd.to_datetime(df['service_date'])

    return df


def generate_test_data_with_duplicates(
    num_records: int,
    duplicate_ratio: float = 0.1,
    seed: Optional[int] = 42
) -> pd.DataFrame:
    """
    Generate test data with intentional duplicates for testing deduplication.

    Parameters:
    - num_records: Total number of records to generate
    - duplicate_ratio: Ratio of duplicate records (default: 0.1 = 10%)
    - seed: Random seed for reproducibility

    Returns:
    - DataFrame with some duplicate rows
    """
    unique_records = int(num_records * (1 - duplicate_ratio))
    df = generate_test_data(unique_records, seed=seed)

    num_duplicates = num_records - unique_records
    if num_duplicates > 0:
        duplicate_indices = random.choices(range(len(df)), k=num_duplicates)
        duplicates = df.iloc[duplicate_indices].copy()
        df = pd.concat([df, duplicates], ignore_index=True)

    return df.sample(frac=1, random_state=seed).reset_index(drop=True)
