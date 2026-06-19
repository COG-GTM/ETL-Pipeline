#!/usr/bin/env python3
"""
Data Consolidation Demo
=======================
Consolidates the three sample data sources into a single unified dataset that
matches demo/schemas/consolidated_schema.sql:

1. shipping_records  (XML)  - primary source
2. returns           (JSON) - joined on transaction_id
3. supplier_orders   (JSON) - items exploded, joined on product_id

Run:
    python -m demo.run_consolidation
"""

import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.consolidator import DataConsolidator

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DATA_DIR = os.path.join(DEMO_DIR, "sample_data")

# The XML parser walks every element in shipping_records.xml, so it also emits
# noise rows (metadata, the <shipments> container, destination-only rows) and a
# duplicate `shipment_*` prefixed column set. Restrict the primary source to the
# real per-shipment columns before consolidating. This also avoids the `status`
# -> `shipment_status` rename colliding with the container-derived column.
SHIPPING_COLUMNS = [
    "shipment_id",
    "transaction_id",
    "customer_id",
    "origin",
    "destination_address",
    "destination_city",
    "destination_state",
    "destination_zip",
    "carrier",
    "tracking_number",
    "ship_date",
    "delivery_date",
    "status",
    "weight_kg",
    "shipping_cost",
]

# Final column order of consolidated_orders (see demo/schemas/consolidated_schema.sql).
# Output is reindexed to this so it matches the schema exactly, dropping any
# incidental columns from the joins (e.g. the returns `customer_id_returns`).
SCHEMA_COLUMNS = [
    "shipment_id", "transaction_id", "customer_id", "origin",
    "destination_address", "destination_city", "destination_state",
    "destination_zip", "carrier", "tracking_number", "ship_date",
    "delivery_date", "shipment_status", "weight_kg", "shipping_cost",
    "return_id", "return_date", "return_reason", "return_reason_detail",
    "refund_amount", "refund_status", "item_condition", "restockable",
    "product_id", "quantity", "unit_cost", "total", "order_id", "order_date",
    "expected_delivery", "supplier_order_status", "supplier_id",
    "supplier_name", "supplier_country", "supplier_carrier",
    "supplier_tracking", "supplier_shipping_cost",
    "source_system", "etl_load_timestamp", "etl_batch_id",
]

# Supplier orders nest a per-order "items" array; explode it to one row per
# item while carrying the order-level fields down via the meta paths.
SUPPLIER_META = [
    "order_id",
    "order_date",
    "expected_delivery",
    "status",
    ["supplier", "id"],
    ["supplier", "name"],
    ["supplier", "country"],
    ["shipping", "carrier"],
    ["shipping", "tracking"],
    ["shipping", "cost"],
]

# Join the secondary sources onto the shipping records. The supplier join uses a
# "_sup" suffix so its `status` meta field lands as `status_sup` instead of
# colliding with the shipping `status` column.
JOIN_CONFIGS = [
    {"source": "returns", "on": "transaction_id", "how": "left"},
    {"source": "supplier_order_items", "on": "product_id", "how": "left", "suffix": "_sup"},
]

# Rename columns so multiple sources that share a `status` column (and the
# nested supplier fields) become unambiguous in the consolidated output.
COLUMN_RENAMES = {
    # shipping
    "status": "shipment_status",
    # returns
    "reason": "return_reason",
    "reason_detail": "return_reason_detail",
    "condition": "item_condition",
    # supplier
    "status_sup": "supplier_order_status",
    "shipping.carrier": "supplier_carrier",
    "shipping.tracking": "supplier_tracking",
    "shipping.cost": "supplier_shipping_cost",
    "supplier.id": "supplier_id",
    "supplier.name": "supplier_name",
    "supplier.country": "supplier_country",
}


def main() -> None:
    """Load the sample sources, consolidate them, and print the report."""
    consolidator = DataConsolidator()

    shipping = consolidator.load_source(
        "shipping_records",
        os.path.join(SAMPLE_DATA_DIR, "xml", "shipping_records.xml"),
    )
    # Keep only real shipment rows/columns (drop XML parser noise).
    shipping = shipping[shipping["shipment_id"].notna()]
    consolidator.sources["shipping_records"] = (
        shipping[[c for c in SHIPPING_COLUMNS if c in shipping.columns]]
        .reset_index(drop=True)
    )

    consolidator.load_source(
        "returns",
        os.path.join(SAMPLE_DATA_DIR, "api_mock", "returns_api.json"),
    )
    consolidator.load_json_with_explode(
        "supplier_order_items",
        os.path.join(SAMPLE_DATA_DIR, "json", "supplier_orders.json"),
        record_path="items",
        meta=SUPPLIER_META,
    )

    consolidated = consolidator.consolidate("shipping_records", JOIN_CONFIGS)
    consolidated.rename(columns=COLUMN_RENAMES, inplace=True)

    # Populate ETL audit columns and align the output to the schema.
    consolidated["source_system"] = "shipping_records+returns+supplier_orders"
    consolidated["etl_load_timestamp"] = datetime.now().isoformat()
    consolidated["etl_batch_id"] = f"BATCH-{datetime.now():%Y%m%d-%H%M%S}"
    consolidated = consolidated.reindex(columns=SCHEMA_COLUMNS)

    consolidator.consolidated = consolidated
    print(consolidator.generate_report())


if __name__ == "__main__":
    main()
