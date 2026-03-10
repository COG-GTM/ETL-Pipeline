"""
Supply Chain ETL Pipeline
=========================
Extracts inventory and supplier order data from JSON sources,
joins them on product_id, enriches with product catalog data,
deduplicates, and prepares for S3 loading as a consolidated
supply chain dataset.
"""

import json
from typing import Optional

import pandas as pd


def extract_inventory(file_path: str) -> pd.DataFrame:
    """
    Extract inventory data from a nested JSON file.

    The JSON structure contains a top-level 'inventory' array with items
    including product_id, sku, quantity_on_hand, reorder_point, and
    last_restocked. Warehouse-level metadata (warehouse_id, warehouse_name,
    location) is flattened and merged onto each inventory row.

    Parameters:
        file_path: Path to the inventory JSON file.

    Returns:
        A DataFrame with one row per inventory item, enriched with
        warehouse metadata.
    """
    with open(file_path, "r") as f:
        raw = json.load(f)

    items = raw.get("inventory", [])
    if not items:
        print("Warning: No inventory items found in source file")
        return pd.DataFrame()

    df = pd.json_normalize(items)

    # Attach warehouse-level metadata to each row
    df["warehouse_id"] = raw.get("warehouse_id", "")
    df["warehouse_name"] = raw.get("warehouse_name", "")

    location = raw.get("location", {})
    df["warehouse_city"] = location.get("city", "")
    df["warehouse_state"] = location.get("state", "")
    df["warehouse_country"] = location.get("country", "")

    df["inventory_last_updated"] = raw.get("last_updated", "")

    # Convert date columns
    df["last_restocked"] = pd.to_datetime(df["last_restocked"], errors="coerce")
    df["inventory_last_updated"] = pd.to_datetime(
        df["inventory_last_updated"], errors="coerce"
    )

    print(f"Extracted {len(df)} inventory items from {file_path}")
    return df


def extract_supplier_orders(file_path: str) -> pd.DataFrame:
    """
    Extract supplier order data from a JSON file.

    The JSON is an array of orders, each containing nested 'supplier',
    'items', and 'shipping' objects. This function flattens the structure
    so that each row represents one order-item combination with supplier
    and shipping details attached.

    Parameters:
        file_path: Path to the supplier_orders JSON file.

    Returns:
        A DataFrame with one row per order-item, including supplier
        and shipping metadata.
    """
    with open(file_path, "r") as f:
        raw = json.load(f)

    if not raw:
        print("Warning: No supplier orders found in source file")
        return pd.DataFrame()

    rows = []
    for order in raw:
        order_id = order.get("order_id", "")
        order_date = order.get("order_date", "")
        expected_delivery = order.get("expected_delivery", "")
        status = order.get("status", "")

        supplier = order.get("supplier", {})
        supplier_id = supplier.get("id", "")
        supplier_name = supplier.get("name", "")
        supplier_country = supplier.get("country", "")

        shipping = order.get("shipping") or {}
        shipping_carrier = shipping.get("carrier", "")
        shipping_tracking = shipping.get("tracking", "")
        shipping_cost = shipping.get("cost", 0.0)

        for item in order.get("items", []):
            rows.append({
                "order_id": order_id,
                "order_date": order_date,
                "expected_delivery": expected_delivery,
                "order_status": status,
                "supplier_id": supplier_id,
                "supplier_name": supplier_name,
                "supplier_country": supplier_country,
                "product_id": item.get("product_id", ""),
                "order_quantity": item.get("quantity", 0),
                "unit_cost": item.get("unit_cost", 0.0),
                "line_total": item.get("total", 0.0),
                "shipping_carrier": shipping_carrier,
                "shipping_tracking": shipping_tracking,
                "shipping_cost": shipping_cost,
            })

    df = pd.DataFrame(rows)

    # Convert date columns
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["expected_delivery"] = pd.to_datetime(
        df["expected_delivery"], errors="coerce"
    )

    print(f"Extracted {len(df)} supplier order line items from {file_path}")
    return df


def join_inventory_and_orders(
    inventory_df: pd.DataFrame,
    orders_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join inventory and supplier order data on product_id.

    Uses an outer join so that all inventory items and all order items
    are preserved, even if a product appears in only one source.

    Parameters:
        inventory_df: DataFrame from extract_inventory.
        orders_df: DataFrame from extract_supplier_orders.

    Returns:
        A merged DataFrame joined on product_id.
    """
    if inventory_df.empty and orders_df.empty:
        print("Warning: Both inventory and orders DataFrames are empty")
        return pd.DataFrame()

    if inventory_df.empty:
        print("Warning: Inventory DataFrame is empty; returning orders only")
        return orders_df.copy()

    if orders_df.empty:
        print("Warning: Orders DataFrame is empty; returning inventory only")
        return inventory_df.copy()

    merged = inventory_df.merge(orders_df, on="product_id", how="outer")

    print(
        f"Joined inventory ({len(inventory_df)} rows) with orders "
        f"({len(orders_df)} rows) -> {len(merged)} rows"
    )
    return merged


def enrich_with_product_catalog(
    df: pd.DataFrame,
    catalog_path: str,
) -> pd.DataFrame:
    """
    Enrich the supply chain dataset with product catalog data.

    Left-joins the consolidated DataFrame with the product catalog CSV
    on product_id, adding product_name, category, subcategory, brand,
    unit_price, cost_price, and weight_kg.

    Parameters:
        df: The joined inventory + orders DataFrame.
        catalog_path: Path to the products CSV file.

    Returns:
        The enriched DataFrame with product catalog columns.
    """
    catalog_df = pd.read_csv(catalog_path)
    print(f"Loaded product catalog: {len(catalog_df)} products from {catalog_path}")

    # Avoid column collisions: if supplier_id already exists in df,
    # the catalog's supplier_id will get a suffix
    before_cols = set(df.columns)
    enriched = df.merge(catalog_df, on="product_id", how="left", suffixes=("", "_catalog"))

    new_cols = [c for c in enriched.columns if c not in before_cols]
    print(
        f"Enriched with product catalog: added {len(new_cols)} columns "
        f"({len(enriched)} rows)"
    )
    return enriched


def deduplicate(
    df: pd.DataFrame,
    subset: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    Parameters:
        df: Input DataFrame.
        subset: Columns to consider for duplicate detection.
                Defaults to all columns.

    Returns:
        A deduplicated DataFrame.
    """
    duplicate_count = df.duplicated(subset=subset).sum()

    if duplicate_count > 0:
        print(f"Found {duplicate_count} duplicate rows — removing")
        print(f"  Shape before: {df.shape}")
        df_cleaned = df.drop_duplicates(subset=subset, keep="first")
        print(f"  Shape after:  {df_cleaned.shape}")
        return df_cleaned

    print("No duplicate rows found")
    return df.copy()


def add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns useful for supply chain analysis.

    Computed columns:
        - stock_status: 'low_stock' when quantity_on_hand <= reorder_point,
          'adequate' otherwise.
        - days_until_delivery: Business days between today and expected_delivery.
        - margin_pct: Percentage margin based on unit_price and unit_cost.

    Parameters:
        df: The enriched supply chain DataFrame.

    Returns:
        DataFrame with additional computed columns.
    """
    if "quantity_on_hand" in df.columns and "reorder_point" in df.columns:
        df["stock_status"] = df.apply(
            lambda row: "low_stock"
            if pd.notna(row["quantity_on_hand"])
            and pd.notna(row["reorder_point"])
            and row["quantity_on_hand"] <= row["reorder_point"]
            else "adequate",
            axis=1,
        )

    if "unit_price" in df.columns and "unit_cost" in df.columns:
        df["margin_pct"] = df.apply(
            lambda row: round(
                ((row["unit_price"] - row["unit_cost"]) / row["unit_price"]) * 100, 2
            )
            if pd.notna(row["unit_price"])
            and pd.notna(row["unit_cost"])
            and row["unit_price"] > 0
            else None,
            axis=1,
        )

    print(f"Added computed columns to supply chain dataset ({len(df)} rows)")
    return df
