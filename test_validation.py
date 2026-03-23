import pandas as pd
from src.transform import validate_schema

print("Testing with valid data...")
valid_data = pd.DataFrame({
    'vin': ['1HGCM82633A004352', '1HGCM82633A004353'],
    'year': [2021, 2022],
    'sale_price': [28000, 22000]
})

try:
    validate_schema(valid_data)
    print("✅ Valid data test passed")
except ValueError as e:
    print(f"❌ Valid data test failed: {e}")

print("\nTesting with invalid VIN...")
invalid_vin_data = pd.DataFrame({
    'vin': ['SHORT', '1HGCM82633A004353'],
    'year': [2021, 2022],
    'sale_price': [28000, 22000]
})

try:
    validate_schema(invalid_vin_data)
    print("❌ Invalid VIN test failed - should have raised exception")
except ValueError as e:
    print(f"✅ Invalid VIN test passed: {e}")

print("\nTesting with invalid year...")
invalid_year_data = pd.DataFrame({
    'vin': ['1HGCM82633A004352', '1HGCM82633A004353'],
    'year': [1980, 2030],
    'sale_price': [28000, 22000]
})

try:
    validate_schema(invalid_year_data)
    print("❌ Invalid year test failed - should have raised exception")
except ValueError as e:
    print(f"✅ Invalid year test passed: {e}")

print("\nTesting with negative price...")
invalid_price_data = pd.DataFrame({
    'vin': ['1HGCM82633A004352', '1HGCM82633A004353'],
    'year': [2021, 2022],
    'sale_price': [-1000, 22000]
})

try:
    validate_schema(invalid_price_data)
    print("❌ Invalid price test failed - should have raised exception")
except ValueError as e:
    print(f"✅ Invalid price test passed: {e}")
