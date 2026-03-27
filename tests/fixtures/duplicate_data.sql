-- tests/fixtures/duplicate_data.sql
-- Test data with intentional duplicates for testing identify_and_remove_duplicated_data()
-- This fixture creates scenarios where the deduplication logic should remove duplicate rows

-- Clear existing data (in reverse order of dependencies)
TRUNCATE TABLE service_records CASCADE;
TRUNCATE TABLE sales_transactions CASCADE;
TRUNCATE TABLE vehicles CASCADE;
TRUNCATE TABLE dealerships CASCADE;

-- Reset sequences
ALTER SEQUENCE dealerships_id_seq RESTART WITH 1;
ALTER SEQUENCE sales_transactions_id_seq RESTART WITH 1;
ALTER SEQUENCE service_records_id_seq RESTART WITH 1;

-- Insert dealerships
INSERT INTO dealerships (name, region) VALUES
('Duplicate Test Motors', 'West'),
('Twin City Auto', 'Central');

-- Insert vehicles
INSERT INTO vehicles (vin, model, year, dealership_id) VALUES
('DUP0000000000001', 'Camry', 2021, 1),
('DUP0000000000002', 'Corolla', 2022, 1),
('DUP0000000000003', 'F-150', 2023, 2),
('DUP0000000000004', 'Civic', 2022, 2);

-- Insert sales transactions (some vehicles have multiple sales - edge case)
INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name) VALUES
('DUP0000000000001', '2022-01-15', 28000.00, 'Alice Johnson'),
('DUP0000000000002', '2022-03-20', 22000.00, 'Bob Smith'),
('DUP0000000000003', '2023-06-10', 45000.00, 'Carlos Vega'),
('DUP0000000000004', '2022-08-05', 24000.00, 'Diana Prince');

-- Insert service records with DUPLICATES
-- These will create duplicate rows when joined with vehicles and sales
-- Vehicle DUP0000000000001 has multiple identical service records (exact duplicates)
INSERT INTO service_records (vin, service_date, service_type, service_cost) VALUES
('DUP0000000000001', '2022-07-15', 'Oil Change', 75.00),
('DUP0000000000001', '2022-07-15', 'Oil Change', 75.00),  -- Exact duplicate
('DUP0000000000001', '2022-07-15', 'Oil Change', 75.00),  -- Exact duplicate

-- Vehicle DUP0000000000002 has multiple service records on same date (different types)
('DUP0000000000002', '2022-09-10', 'Tire Rotation', 50.00),
('DUP0000000000002', '2022-09-10', 'Oil Change', 65.00),
('DUP0000000000002', '2022-09-10', 'Tire Rotation', 50.00),  -- Duplicate

-- Vehicle DUP0000000000003 has duplicate service records
('DUP0000000000003', '2023-12-05', 'Transmission Service', 800.00),
('DUP0000000000003', '2023-12-05', 'Transmission Service', 800.00),  -- Exact duplicate

-- Vehicle DUP0000000000004 has multiple unique service records (no duplicates)
('DUP0000000000004', '2023-02-28', 'Oil Change', 65.00),
('DUP0000000000004', '2023-05-15', 'Brake Inspection', 100.00),
('DUP0000000000004', '2023-08-20', 'Tire Rotation', 50.00);

-- Expected behavior after deduplication:
-- - DUP0000000000001 should have 1 service record (3 duplicates -> 1)
-- - DUP0000000000002 should have 2 service records (1 duplicate removed)
-- - DUP0000000000003 should have 1 service record (1 duplicate removed)
-- - DUP0000000000004 should have 3 service records (no duplicates)
-- Total: 11 records -> 7 unique records after deduplication
