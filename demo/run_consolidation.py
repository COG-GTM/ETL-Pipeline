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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.consolidator import DataConsolidator

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DATA_DIR = os.path.join(DEMO_DIR, "sample_data")

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
    consolidator = DataConsolidator()

    consolidator.load_source(
        "shipping_records",
        os.path.join(SAMPLE_DATA_DIR, "xml", "shipping_records.xml"),
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

    print(consolidator.generate_report())


if __name__ == "__main__":
    main()
