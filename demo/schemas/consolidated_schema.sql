-- Consolidated schema for the three sample data sources.
--
-- Sources:
--   1. shipping_records  (XML  - demo/sample_data/xml/shipping_records.xml)   [primary]
--   2. returns           (JSON - demo/sample_data/api_mock/returns_api.json)  joined on transaction_id
--   3. supplier_orders   (JSON - demo/sample_data/json/supplier_orders.json)  items exploded, joined on product_id
--
-- Column names match the rename mapping applied in demo/run_consolidation.py so
-- that columns are unambiguous after merging sources that share `status`.

CREATE TABLE consolidated_orders (
    -- ----------------------------------------------------------------
    -- Shipping records (primary source)
    -- ----------------------------------------------------------------
    shipment_id             VARCHAR(50),
    transaction_id          VARCHAR(50),
    customer_id             VARCHAR(50),
    origin                  VARCHAR(50),
    destination_address     VARCHAR(255),
    destination_city        VARCHAR(100),
    destination_state       VARCHAR(10),
    destination_zip         VARCHAR(20),
    carrier                 VARCHAR(100),
    tracking_number         VARCHAR(100),
    ship_date               DATE,
    delivery_date           DATE,
    shipment_status         VARCHAR(50),
    weight_kg               NUMERIC(10, 2),
    shipping_cost           NUMERIC(12, 2),

    -- ----------------------------------------------------------------
    -- Returns (joined on transaction_id)
    -- ----------------------------------------------------------------
    return_id               VARCHAR(50),
    return_date             DATE,
    return_reason           VARCHAR(100),
    return_reason_detail    TEXT,
    refund_amount           NUMERIC(12, 2),
    refund_status           VARCHAR(50),
    item_condition          VARCHAR(50),
    restockable             BOOLEAN,

    -- ----------------------------------------------------------------
    -- Supplier orders with exploded items (joined on product_id)
    -- ----------------------------------------------------------------
    product_id              VARCHAR(50),
    quantity                INTEGER,
    unit_cost               NUMERIC(12, 2),
    total                   NUMERIC(14, 2),
    order_id                VARCHAR(50),
    order_date              DATE,
    expected_delivery       DATE,
    supplier_order_status   VARCHAR(50),
    supplier_id             VARCHAR(50),
    supplier_name           VARCHAR(255),
    supplier_country        VARCHAR(10),
    supplier_carrier        VARCHAR(100),
    supplier_tracking       VARCHAR(100),
    supplier_shipping_cost  NUMERIC(12, 2),

    -- ----------------------------------------------------------------
    -- ETL audit columns
    -- ----------------------------------------------------------------
    source_system           VARCHAR(100),
    etl_load_timestamp      TIMESTAMP,
    etl_batch_id            VARCHAR(100)
);
