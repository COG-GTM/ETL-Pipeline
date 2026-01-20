-- tests/fixtures/sample_data.sql
-- Baseline test data with valid vehicle sales records
-- This fixture provides a clean dataset for basic functionality testing

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
('Pacific Coast Motors', 'West'),
('Great Lakes Auto', 'Central'),
('Eastern Seaboard Cars', 'East'),
('Southern Sun Dealership', 'South'),
('Mountain View Motors', 'West');

-- Insert vehicles with various models and years
INSERT INTO vehicles (vin, model, year, dealership_id) VALUES
('1HGCM82633A100001', 'Camry', 2021, 1),
('1HGCM82633A100002', 'Corolla', 2022, 1),
('1HGCM82633A100003', 'F-150', 2023, 2),
('1HGCM82633A100004', 'Civic', 2022, 3),
('1HGCM82633A100005', 'Accord', 2021, 4),
('1HGCM82633A100006', 'Mustang', 2023, 5),
('1HGCM82633A100007', 'Explorer', 2022, 2),
('1HGCM82633A100008', 'Highlander', 2021, 1),
('1HGCM82633A100009', 'RAV4', 2023, 3),
('1HGCM82633A100010', 'Tacoma', 2022, 4);

-- Insert sales transactions
INSERT INTO sales_transactions (vin, sale_date, sale_price, buyer_name) VALUES
('1HGCM82633A100001', '2022-01-15', 28000.00, 'Alice Johnson'),
('1HGCM82633A100002', '2022-03-20', 22000.00, 'Bob Smith'),
('1HGCM82633A100003', '2023-06-10', 45000.00, 'Carlos Vega'),
('1HGCM82633A100004', '2022-08-05', 24000.00, 'Diana Prince'),
('1HGCM82633A100005', '2021-11-30', 32000.00, 'Edward Norton'),
('1HGCM82633A100006', '2023-02-14', 55000.00, 'Fiona Apple'),
('1HGCM82633A100007', '2022-09-22', 42000.00, 'George Lucas'),
('1HGCM82633A100008', '2021-12-01', 38000.00, 'Hannah Montana'),
('1HGCM82633A100009', '2023-04-18', 35000.00, 'Ivan Petrov'),
('1HGCM82633A100010', '2022-07-04', 40000.00, 'Julia Roberts');

-- Insert service records
INSERT INTO service_records (vin, service_date, service_type, service_cost) VALUES
('1HGCM82633A100001', '2022-07-15', 'Oil Change', 75.00),
('1HGCM82633A100001', '2023-01-20', 'Brake Inspection', 150.00),
('1HGCM82633A100002', '2022-09-10', 'Tire Rotation', 50.00),
('1HGCM82633A100003', '2023-12-05', 'Transmission Service', 800.00),
('1HGCM82633A100004', '2023-02-28', 'Oil Change', 65.00),
('1HGCM82633A100005', '2022-06-15', 'Battery Replacement', 200.00),
('1HGCM82633A100006', '2023-08-20', 'Brake Pads', 350.00),
('1HGCM82633A100007', '2023-03-10', 'Air Filter', 45.00),
('1HGCM82633A100008', '2022-04-25', 'Coolant Flush', 120.00),
('1HGCM82633A100009', '2023-10-12', 'Wheel Alignment', 100.00);
