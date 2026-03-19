"""
Supply Chain ETL Pipeline - Main Orchestration
================================================
Reads from inventory JSON and supplier_orders JSON sources,
joins them on product_id, enriches with product catalog data,
and loads to S3 as a consolidated supply chain dataset.

Usage:
    python supply_chain_main.py
"""

import os
from datetime import datetime

from dotenv import load_dotenv

from src.supply_chain_etl import (
    add_computed_columns,
    deduplicate,
    enrich_with_product_catalog,
    extract_inventory,
    extract_supplier_orders,
    join_inventory_and_orders,
)
from src.load_data_to_s3 import df_to_s3

# Load environment variables (only needed for local/dev testing)
load_dotenv()

# AWS credentials from environment
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key_id")

# Data source paths
DATA_DIR = os.path.join(os.path.dirname(__file__), "demo", "sample_data")
INVENTORY_PATH = os.path.join(DATA_DIR, "json", "inventory.json")
SUPPLIER_ORDERS_PATH = os.path.join(DATA_DIR, "json", "supplier_orders.json")
PRODUCT_CATALOG_PATH = os.path.join(DATA_DIR, "csv", "products.csv")

# S3 target
S3_BUCKET = "cognition-devin"
S3_KEY = "supply_chain/consolidated_supply_chain.csv"

# Track start time
start_time = datetime.now()

# ── Step 1: Extract ──────────────────────────────────────────────────────────
print("\n--- Step 1: Extracting source data ---")

print("\n  Extracting inventory data...")
inventory_df = extract_inventory(INVENTORY_PATH)

print("\n  Extracting supplier order data...")
orders_df = extract_supplier_orders(SUPPLIER_ORDERS_PATH)

# ── Step 2: Join on product_id ───────────────────────────────────────────────
print("\n--- Step 2: Joining inventory and supplier orders on product_id ---")
joined_df = join_inventory_and_orders(inventory_df, orders_df)

# ── Step 3: Enrich with product catalog ──────────────────────────────────────
print("\n--- Step 3: Enriching with product catalog ---")
enriched_df = enrich_with_product_catalog(joined_df, PRODUCT_CATALOG_PATH)

# ── Step 4: Add computed columns ─────────────────────────────────────────────
print("\n--- Step 4: Adding computed supply chain columns ---")
enriched_df = add_computed_columns(enriched_df)

# ── Step 5: Deduplicate ─────────────────────────────────────────────────────
print("\n--- Step 5: Removing duplicate rows ---")
final_df = deduplicate(enriched_df)

# ── Step 6: Preview ─────────────────────────────────────────────────────────
print("\n--- Consolidated Supply Chain Dataset ---")
print(f"  Rows: {final_df.shape[0]}  |  Columns: {final_df.shape[1]}")
print(f"  Columns: {', '.join(final_df.columns.tolist())}")
print("\n  Preview (first 5 rows):")
print(final_df.head().to_string(index=False))

# ── Step 7: Load to S3 ──────────────────────────────────────────────────────
print("\n--- Step 7: Uploading to S3 ---")
df_to_s3(final_df, S3_KEY, S3_BUCKET, aws_access_key_id, aws_secret_access_key)

# ── Done ─────────────────────────────────────────────────────────────────────
execution_time = datetime.now() - start_time
print(f"\nSupply chain ETL pipeline completed in {execution_time}")
