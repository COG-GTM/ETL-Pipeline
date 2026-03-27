-- tests/fixtures/volume_data.sql
-- Larger dataset for performance testing
-- This fixture generates a substantial amount of data to test ETL performance

-- Clear existing data (in reverse order of dependencies)
TRUNCATE TABLE service_records CASCADE;
TRUNCATE TABLE sales_transactions CASCADE;
TRUNCATE TABLE vehicles CASCADE;
TRUNCATE TABLE dealerships CASCADE;

-- Reset sequences
ALTER SEQUENCE dealerships_id_seq RESTART WITH 1;
ALTER SEQUENCE sales_transactions_id_seq RESTART WITH 1;
ALTER SEQUENCE service_records_id_seq RESTART WITH 1;

-- Insert 20 dealerships across different regions
INSERT INTO dealerships (name, region) VALUES
('Volume Motors West 1', 'West'),
('Volume Motors West 2', 'West'),
('Volume Motors West 3', 'West'),
('Volume Motors West 4', 'West'),
('Volume Motors West 5', 'West'),
('Volume Motors Central 1', 'Central'),
('Volume Motors Central 2', 'Central'),
('Volume Motors Central 3', 'Central'),
('Volume Motors Central 4', 'Central'),
('Volume Motors Central 5', 'Central'),
('Volume Motors East 1', 'East'),
('Volume Motors East 2', 'East'),
('Volume Motors East 3', 'East'),
('Volume Motors East 4', 'East'),
('Volume Motors East 5', 'East'),
('Volume Motors South 1', 'South'),
('Volume Motors South 2', 'South'),
('Volume Motors South 3', 'South'),
('Volume Motors South 4', 'South'),
('Volume Motors South 5', 'South');

-- Generate 100 vehicles using a series
-- Using generate_series to create bulk data
INSERT INTO vehicles (vin, model, year, dealership_id)
SELECT 
    'VOL' || LPAD(n::text, 14, '0') as vin,
    (ARRAY['Camry', 'Corolla', 'F-150', 'Civic', 'Accord', 'Mustang', 'Explorer', 'Highlander', 'RAV4', 'Tacoma'])[1 + (n % 10)] as model,
    2018 + (n % 7) as year,
    1 + (n % 20) as dealership_id
FROM generate_series(1, 100) as n;

-- Generate 100 sales transactions (one per vehicle)
INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name)
SELECT 
    'VOL' || LPAD(n::text, 14, '0') as vin,
    DATE '2020-01-01' + (n * 3) as sale_date,
    20000 + (n * 500) + (random() * 10000)::int as sale_price,
    'Buyer ' || n as buyer_name
FROM generate_series(1, 100) as n;

-- Generate 300 service records (average 3 per vehicle)
INSERT INTO service_records (vin, service_date, service_type, service_cost)
SELECT 
    'VOL' || LPAD(((n - 1) / 3 + 1)::text, 14, '0') as vin,
    DATE '2020-06-01' + (n * 7) as service_date,
    (ARRAY['Oil Change', 'Tire Rotation', 'Brake Inspection', 'Battery Check', 'Transmission Service', 'Air Filter', 'Coolant Flush', 'Wheel Alignment', 'Spark Plugs', 'Fuel Filter'])[1 + (n % 10)] as service_type,
    50 + (n % 20) * 25 + (random() * 100)::int as service_cost
FROM generate_series(1, 300) as n;

-- Add some duplicate records for testing deduplication at scale
INSERT INTO service_records (vin, service_date, service_type, service_cost)
SELECT 
    'VOL' || LPAD(n::text, 14, '0') as vin,
    DATE '2021-01-15' as service_date,
    'Oil Change' as service_type,
    75.00 as service_cost
FROM generate_series(1, 50) as n;

-- Add another batch of duplicates
INSERT INTO service_records (vin, service_date, service_type, service_cost)
SELECT 
    'VOL' || LPAD(n::text, 14, '0') as vin,
    DATE '2021-01-15' as service_date,
    'Oil Change' as service_type,
    75.00 as service_cost
FROM generate_series(1, 50) as n;

-- Summary:
-- - 20 dealerships
-- - 100 vehicles
-- - 100 sales transactions
-- - 400 service records (300 unique + 100 duplicates)
-- - Expected rows after JOIN: ~400 (with duplicates)
-- - Expected rows after deduplication: ~350 (duplicates removed)
