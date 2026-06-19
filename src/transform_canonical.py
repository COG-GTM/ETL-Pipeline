"""Canonical schema transform.

Reads the three demo data sources (supplier orders JSON, shipping records XML,
and the returns API mock JSON), normalizes/flattens each into a tabular form,
joins them via cross-source keys, and emits records conforming to the canonical
JSON schema defined in :data:`CANONICAL_SCHEMA`.
"""

import argparse
import json
import os
import xml.etree.ElementTree as ET
from datetime import date

import pandas as pd

# ---------------------------------------------------------------------------
# Canonical schema
# ---------------------------------------------------------------------------
CANONICAL_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "CanonicalOrderRecord",
    "type": "object",
    "properties": {
        "transaction_id": {"type": ["string", "null"]},
        "customer_id": {"type": ["string", "null"]},
        "product_id": {"type": "string"},
        "order_id": {"type": "string"},
        "supplier_id": {"type": "string"},
        "supplier_name": {"type": "string"},
        "supplier_country": {"type": "string"},
        "order_date": {"type": "string", "format": "date-time"},
        "expected_delivery": {"type": "string", "format": "date-time"},
        "order_status": {"type": "string"},
        "quantity": {"type": "integer"},
        "unit_cost": {"type": "number"},
        "line_total": {"type": "number"},
        "inbound_carrier": {"type": ["string", "null"]},
        "inbound_tracking": {"type": ["string", "null"]},
        "inbound_shipping_cost": {"type": ["number", "null"]},
        "shipment_id": {"type": ["string", "null"]},
        "origin_warehouse": {"type": ["string", "null"]},
        "dest_address": {"type": ["string", "null"]},
        "dest_city": {"type": ["string", "null"]},
        "dest_state": {"type": ["string", "null"]},
        "dest_zip": {"type": ["string", "null"]},
        "outbound_carrier": {"type": ["string", "null"]},
        "outbound_tracking": {"type": ["string", "null"]},
        "ship_date": {"type": ["string", "null"], "format": "date"},
        "delivery_date": {"type": ["string", "null"], "format": "date"},
        "shipment_status": {"type": ["string", "null"]},
        "weight_kg": {"type": ["number", "null"]},
        "outbound_shipping_cost": {"type": ["number", "null"]},
        "return_id": {"type": ["string", "null"]},
        "return_date": {"type": ["string", "null"]},
        "return_reason": {"type": ["string", "null"]},
        "return_reason_detail": {"type": ["string", "null"]},
        "refund_amount": {"type": ["number", "null"]},
        "refund_status": {"type": ["string", "null"]},
        "item_condition": {"type": ["string", "null"]},
        "restockable": {"type": ["boolean", "null"]},
    },
    "required": [
        "product_id",
        "order_id",
        "supplier_id",
        "supplier_name",
        "supplier_country",
        "order_date",
        "expected_delivery",
        "order_status",
        "quantity",
        "unit_cost",
        "line_total",
    ],
}

# Canonical column order, derived from the schema's property declaration order.
CANONICAL_COLUMNS = list(CANONICAL_SCHEMA["properties"].keys())
REQUIRED_COLUMNS = list(CANONICAL_SCHEMA["required"])


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------
def _opt(value):
    """Return a stripped string or ``None`` for empty/whitespace-only values."""
    if value is None:
        return None
    value = value.strip()
    return value or None


def _parse_date(value):
    """Parse an ISO date string into a ``date``; empty values become ``None``."""
    value = _opt(value)
    if value is None:
        return None
    return date.fromisoformat(value)


def load_supplier_orders(path: str) -> pd.DataFrame:
    """Load and flatten the supplier orders JSON into one row per line item."""
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)

    df["supplier_id"] = df["supplier"].apply(lambda s: s["id"])
    df["supplier_name"] = df["supplier"].apply(lambda s: s["name"])
    df["supplier_country"] = df["supplier"].apply(lambda s: s["country"])

    df = df.explode("items", ignore_index=True)
    df["product_id"] = df["items"].apply(lambda i: i["product_id"])
    df["quantity"] = df["items"].apply(lambda i: i["quantity"])
    df["unit_cost"] = df["items"].apply(lambda i: i["unit_cost"])
    df["line_total"] = df["items"].apply(lambda i: i["total"])

    def _ship(field):
        return df["shipping"].apply(
            lambda s: s[field] if isinstance(s, dict) else None
        )

    df["inbound_carrier"] = _ship("carrier")
    df["inbound_tracking"] = _ship("tracking")
    df["inbound_shipping_cost"] = _ship("cost")

    df = df.rename(columns={"status": "order_status"})
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["expected_delivery"] = pd.to_datetime(df["expected_delivery"])

    df = df.drop(columns=["supplier", "items", "shipping"])

    columns = [
        "order_id",
        "supplier_id",
        "supplier_name",
        "supplier_country",
        "order_date",
        "expected_delivery",
        "order_status",
        "product_id",
        "quantity",
        "unit_cost",
        "line_total",
        "inbound_carrier",
        "inbound_tracking",
        "inbound_shipping_cost",
    ]
    return df[columns]


def load_shipping_records(path: str) -> pd.DataFrame:
    """Load and flatten the shipping records XML into one row per shipment."""
    root = ET.parse(path).getroot()

    rows = []
    for shipment in root.find("shipments").findall("shipment"):
        destination = shipment.find("destination")
        rows.append(
            {
                "shipment_id": shipment.findtext("shipment_id"),
                "transaction_id": shipment.findtext("transaction_id"),
                "customer_id": shipment.findtext("customer_id"),
                "origin_warehouse": shipment.findtext("origin"),
                "dest_address": destination.findtext("address"),
                "dest_city": destination.findtext("city"),
                "dest_state": destination.findtext("state"),
                "dest_zip": destination.findtext("zip"),
                "outbound_carrier": shipment.findtext("carrier"),
                "outbound_tracking": shipment.findtext("tracking_number"),
                "ship_date": _parse_date(shipment.findtext("ship_date")),
                "delivery_date": _parse_date(shipment.findtext("delivery_date")),
                "shipment_status": shipment.findtext("status"),
                "weight_kg": float(shipment.findtext("weight_kg")),
                "outbound_shipping_cost": float(
                    shipment.findtext("shipping_cost")
                ),
            }
        )

    columns = [
        "shipment_id",
        "transaction_id",
        "customer_id",
        "origin_warehouse",
        "dest_address",
        "dest_city",
        "dest_state",
        "dest_zip",
        "outbound_carrier",
        "outbound_tracking",
        "ship_date",
        "delivery_date",
        "shipment_status",
        "weight_kg",
        "outbound_shipping_cost",
    ]
    return pd.DataFrame(rows, columns=columns)


def load_returns(path: str) -> pd.DataFrame:
    """Load the returns API mock, stripping the pagination envelope."""
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    df = pd.DataFrame(payload["data"])
    df = df.rename(
        columns={
            "condition": "item_condition",
            "reason": "return_reason",
            "reason_detail": "return_reason_detail",
        }
    )

    columns = [
        "return_id",
        "transaction_id",
        "customer_id",
        "product_id",
        "return_date",
        "return_reason",
        "return_reason_detail",
        "refund_amount",
        "refund_status",
        "item_condition",
        "restockable",
    ]
    return df[columns]


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------
def merge_to_canonical(
    supplier_orders_df: pd.DataFrame,
    shipping_df: pd.DataFrame,
    returns_df: pd.DataFrame,
) -> pd.DataFrame:
    """Join the three sources into the canonical schema.

    Supplier order line items are the grain. Returns are joined on ``product_id``
    (supplying ``transaction_id``/``customer_id``), then shipping is joined on
    ``transaction_id`` and ``customer_id``.
    """
    merged = supplier_orders_df.merge(returns_df, on="product_id", how="left")
    merged = merged.merge(
        shipping_df, on=["transaction_id", "customer_id"], how="left"
    )

    merged = merged.reindex(columns=CANONICAL_COLUMNS)

    nulls = [col for col in REQUIRED_COLUMNS if merged[col].isna().any()]
    if nulls:
        raise ValueError(
            f"Required canonical fields contain null values: {', '.join(nulls)}"
        )

    return merged


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def transform_all(data_dir: str, output_path: str) -> pd.DataFrame:
    """Run the full transform and write canonical records to ``output_path``."""
    supplier_orders_df = load_supplier_orders(
        os.path.join(data_dir, "json", "supplier_orders.json")
    )
    shipping_df = load_shipping_records(
        os.path.join(data_dir, "xml", "shipping_records.xml")
    )
    returns_df = load_returns(
        os.path.join(data_dir, "api_mock", "returns_api.json")
    )

    canonical_df = merge_to_canonical(supplier_orders_df, shipping_df, returns_df)

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    canonical_df.to_json(
        output_path,
        orient="records",
        indent=2,
        date_format="iso",
        default_handler=str,
    )

    print(f"Canonical transform: {canonical_df.shape[0]} rows x "
          f"{canonical_df.shape[1]} columns")
    print("Null counts per column:")
    for col, count in canonical_df.isna().sum().items():
        print(f"  {col}: {count}")

    return canonical_df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transform demo data sources into canonical JSON records."
    )
    parser.add_argument(
        "--data-dir",
        default=os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "demo",
            "sample_data",
        ),
        help="Directory containing json/, xml/, and api_mock/ subdirectories.",
    )
    parser.add_argument(
        "--output",
        default="canonical_records.json",
        help="Path to write the canonical JSON records.",
    )
    args = parser.parse_args()
    transform_all(args.data_dir, args.output)


if __name__ == "__main__":
    main()
