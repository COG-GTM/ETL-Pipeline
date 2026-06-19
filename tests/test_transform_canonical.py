import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.transform_canonical import (
    CANONICAL_COLUMNS,
    REQUIRED_COLUMNS,
    load_returns,
    load_shipping_records,
    load_supplier_orders,
    merge_to_canonical,
    transform_all,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE_DATA_DIR = os.path.join(REPO_ROOT, "demo", "sample_data")
SUPPLIER_ORDERS = os.path.join(SAMPLE_DATA_DIR, "json", "supplier_orders.json")
SHIPPING_RECORDS = os.path.join(SAMPLE_DATA_DIR, "xml", "shipping_records.xml")
RETURNS_API = os.path.join(SAMPLE_DATA_DIR, "api_mock", "returns_api.json")


def test_load_supplier_orders():
    df = load_supplier_orders(SUPPLIER_ORDERS)

    assert len(df) == 7
    assert list(df.columns) == [
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

    so_003 = df[df["order_id"] == "SO-2024-003"]
    assert len(so_003) == 2
    assert so_003["inbound_carrier"].isna().all()
    assert so_003["inbound_tracking"].isna().all()
    assert so_003["inbound_shipping_cost"].isna().all()


def test_load_shipping_records():
    df = load_shipping_records(SHIPPING_RECORDS)

    assert len(df) == 5
    assert "generated_date" not in df.columns
    assert "system" not in df.columns
    assert "region" not in df.columns

    sh_004 = df[df["shipment_id"] == "SH-2024-0004"].iloc[0]
    assert sh_004["delivery_date"] is None

    assert df["weight_kg"].dtype == float
    assert isinstance(df["weight_kg"].iloc[0], float)


def test_load_returns():
    df = load_returns(RETURNS_API)

    assert len(df) == 5
    for envelope_col in ("api_version", "endpoint", "total_records", "page", "per_page"):
        assert envelope_col not in df.columns

    assert "item_condition" in df.columns
    assert "return_reason" in df.columns
    assert "return_reason_detail" in df.columns
    assert "condition" not in df.columns
    assert "reason" not in df.columns


def test_merge_to_canonical():
    supplier_orders_df = load_supplier_orders(SUPPLIER_ORDERS)
    shipping_df = load_shipping_records(SHIPPING_RECORDS)
    returns_df = load_returns(RETURNS_API)

    merged = merge_to_canonical(supplier_orders_df, shipping_df, returns_df)

    assert list(merged.columns) == CANONICAL_COLUMNS
    assert len(merged.columns) == 37
    assert len(merged) == 7

    for col in REQUIRED_COLUMNS:
        assert merged[col].notna().all()

    non_returned = merged[merged["product_id"] == "P109"]
    assert len(non_returned) == 1
    assert non_returned["return_id"].isna().all()
    assert non_returned["refund_amount"].isna().all()


def test_transform_all(tmp_path):
    output_path = os.path.join(tmp_path, "canonical_records.json")
    df = transform_all(SAMPLE_DATA_DIR, output_path)

    assert os.path.exists(output_path)
    assert list(df.columns) == CANONICAL_COLUMNS

    with open(output_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    assert isinstance(records, list)
    assert len(records) == len(df)
    assert set(records[0].keys()) == set(CANONICAL_COLUMNS)
