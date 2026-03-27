-- tests/fixtures/edge_cases.sql
-- Test data with NULL values, invalid data, and boundary conditions
-- This fixture tests how the ETL pipeline handles edge cases

-- Clear existing data (in reverse order of dependencies)
TRUNCATE TABLE service_records CASCADE;
TRUNCATE TABLE sales_transactions CASCADE;
TRUNCATE TABLE vehicles CASCADE;
TRUNCATE TABLE dealerships CASCADE;

-- Reset sequences
ALTER SEQUENCE dealerships_id_seq RESTART WITH 1;
ALTER SEQUENCE sales_transactions_id_seq RESTART WITH 1;
ALTER SEQUENCE service_records_id_seq RESTART WITH 1;

-- Insert dealerships with various edge cases
INSERT INTO dealerships (name, region) VALUES
('Edge Case Motors', 'West'),
('Null Test Auto', 'Central'),
('Boundary Dealership', 'East'),
('Special Chars & Co.', 'South'),  -- Special characters in name
('Unicode Dealer 日本語', 'West');  -- Unicode characters

-- Insert vehicles with edge cases
INSERT INTO vehicles (vin, model, year, dealership_id) VALUES
-- Standard vehicles
('EDGE000000000001', 'Camry', 2021, 1),
('EDGE000000000002', 'Corolla', 2022, 2),
-- Boundary year values
('EDGE000000000003', 'Model T', 1908, 3),  -- Very old year
('EDGE000000000004', 'Future Car', 2030, 3),  -- Future year
-- Minimum/maximum length model names
('EDGE000000000005', 'X', 2023, 4),  -- Single character model
('EDGE000000000006', 'Super Long Model Name That Tests Maximum Length Handling', 2023, 4),
-- Special characters in VIN (edge case)
('EDGE000000000007', 'Civic', 2022, 5),
-- Vehicle with no sales (tests LEFT JOIN)
('EDGE000000000008', 'Unsold Vehicle', 2023, 1),
-- Vehicle with no service records (tests LEFT JOIN)
('EDGE000000000009', 'No Service Vehicle', 2022, 2);

-- Insert sales transactions with edge cases
INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name) VALUES
-- Standard sale
('EDGE000000000001', '2022-01-15', 28000.00, 'Alice Johnson'),
-- NULL sale_date
('EDGE000000000002', NULL, 22000.00, 'Bob Smith'),
-- Zero sale price
('EDGE000000000003', '2023-06-10', 0.00, 'Carlos Vega'),
-- Very high sale price (boundary)
('EDGE000000000004', '2023-08-05', 9999999.99, 'Diana Prince'),
-- Negative sale price (invalid but should handle gracefully)
('EDGE000000000005', '2023-02-14', -1000.00, 'Edward Norton'),
-- NULL buyer_name
('EDGE000000000006', '2023-09-22', 55000.00, NULL),
-- Empty string buyer_name
('EDGE000000000007', '2023-12-01', 24000.00, ''),
-- Special characters in buyer_name
('EDGE000000000009', '2023-04-18', 35000.00, 'José García-López');
-- Note: EDGE000000000008 has no sale (tests vehicle without sale)

-- Insert service records with edge cases
INSERT INTO service_records (vin, service_date, service_type, service_cost) VALUES
-- Standard service record
('EDGE000000000001', '2022-07-15', 'Oil Change', 75.00),
-- NULL service_date (should be handled by COALESCE in query)
('EDGE000000000002', NULL, 'Tire Rotation', 50.00),
-- NULL service_type (should become 'Unknown' via COALESCE)
('EDGE000000000003', '2023-12-05', NULL, 800.00),
-- NULL service_cost (should become 0 via COALESCE)
('EDGE000000000004', '2023-02-28', 'Battery Check', NULL),
-- Zero service cost
('EDGE000000000005', '2023-08-20', 'Free Inspection', 0.00),
-- Very high service cost (boundary)
('EDGE000000000006', '2023-03-10', 'Engine Rebuild', 99999.99),
-- Empty string service_type
('EDGE000000000007', '2023-04-25', '', 120.00),
-- All NULL values except VIN
('EDGE000000000001', NULL, NULL, NULL);
-- Note: EDGE000000000008 and EDGE000000000009 have no service records

-- Expected behavior:
-- - NULL service_type should become 'Unknown'
-- - NULL service_cost should become 0
-- - NULL service_date should remain NULL (handled by COALESCE returning NULL)
-- - Vehicles without sales should still appear (LEFT JOIN)
-- - Vehicles without service records should still appear (LEFT JOIN)
