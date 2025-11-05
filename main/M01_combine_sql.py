# ====================================================================================================
# M01_run_order_level.py
# ----------------------------------------------------------------------------------------------------
# Executes both SQL queries to produce consolidated DWH export files for all delivery providers.
# ----------------------------------------------------------------------------------------------------
# Process Summary:
#   • S01_order_level.sql → Produces the order-level data (core financial + operational metrics)
#   • S02_item_level.sql  → Produces the item-level data (line-item + VAT breakdown)
#   • Combines both datasets into one DataFrame per order, pivots item-level details, and
#     exports provider-specific CSVs to their respective folders.
# ----------------------------------------------------------------------------------------------------
# This script forms the foundation for the "Orders-to-Cash DWH extraction" layer of the pipeline.
# ====================================================================================================

import sys
from pathlib import Path

# ----------------------------------------------------------------------------------------------------
# SYSTEM PATH ADJUSTMENT
# ----------------------------------------------------------------------------------------------------
# Ensures Python can import shared modules from the parent folder (`processes/` directory).
# This allows consistent referencing across all scripts in the project.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True  # Prevents creation of __pycache__ folders

# ----------------------------------------------------------------------------------------------------
# PROJECT IMPORTS
# ----------------------------------------------------------------------------------------------------
# All project-wide packages and dependencies are declared in P00_set_packages.py.
# This guarantees consistent package usage and avoids duplicate imports.
from processes.P00_set_packages import *
from processes.P01_set_file_paths import braintree_path, paypal_path, uber_path, deliveroo_path, justeat_path, amazon_path
from processes.P02_system_processes import user_download_folder
from processes.P03_shared_functions import normalize_columns, read_sql_clean
from processes.P04_static_lists import FINAL_DF_ORDER
from processes.P07_module_configs import get_reporting_period
from processes.P08_snowflake_connector import connect_to_snowflake, set_snowflake_context

# ----------------------------------------------------------------------------------------------------
# run_order_level_query()
# ----------------------------------------------------------------------------------------------------
def run_order_level_query(conn):
    """
    Executes the order-level SQL query (S01_order_level.sql) against Snowflake.

    Steps:
    1. Replaces {{start_date}} and {{end_date}} placeholders in SQL with current reporting period.
    2. Executes the query using the shared read_sql_clean() helper (silent + normalized columns).
    3. Returns the resulting DataFrame containing order-level financial and operational data.

    Args:
        conn (snowflake.connector.connection.SnowflakeConnection):
            Active Snowflake connection object.

    Returns:
        pandas.DataFrame: Normalized DataFrame containing order-level data for the reporting period.
    """
    start_date, end_date = get_reporting_period()
    sql_query = (
        Path("sql/S01_order_level.sql").read_text()
        .replace("{{start_date}}", start_date)
        .replace("{{end_date}}", end_date)
    )

    print(f"⏳ Executing order-level query for {start_date} → {end_date} ...", end="", flush=True)
    t0 = time.time()

    # Execute SQL quietly and normalize columns (lowercase, underscore)
    df_orders = read_sql_clean(conn, sql_query)

    print(f"\r✅ Order-level query complete in {time.time() - t0:,.1f}s — {len(df_orders):,} rows.")
    return df_orders


# ----------------------------------------------------------------------------------------------------
# run_item_level_query()
# ----------------------------------------------------------------------------------------------------
def run_item_level_query(conn, df_orders):
    """
    Executes the item-level SQL query (S02_item_level.sql) using gp_order_id values from df_orders.

    Steps:
    1. Extracts all unique gp_order_id values from the order-level DataFrame.
    2. Uploads them in chunks to a Snowflake TEMP TABLE (`temp_order_ids`).
    3. Replaces {{order_id_list}} in SQL with SELECT from that temp table.
    4. Executes the item-level query and normalizes results.
    5. Returns the resulting DataFrame with per-item financial breakdown by VAT band.

    Args:
        conn (snowflake.connector.connection.SnowflakeConnection):
            Active Snowflake connection object.
        df_orders (pandas.DataFrame):
            Output from run_order_level_query().

    Returns:
        pandas.DataFrame: Normalized DataFrame with per-item metrics for each gp_order_id.
    """
    # Validate input
    gp_order_ids = df_orders["gp_order_id"].dropna().unique().tolist()
    if not gp_order_ids:
        raise ValueError("❌ No valid gp_order_id values found.")

    print(f"⏳ Uploading {len(gp_order_ids):,} order IDs to Snowflake ...", end="", flush=True)
    cur = conn.cursor()
    cur.execute("CREATE OR REPLACE TEMP TABLE temp_order_ids (gp_order_id STRING);")

    # Upload order IDs in chunks to avoid memory limits
    chunk_size = 25000
    start_time = time.time()
    for i in range(0, len(gp_order_ids), chunk_size):
        chunk = [(oid,) for oid in gp_order_ids[i:i + chunk_size]]
        cur.executemany("INSERT INTO temp_order_ids (gp_order_id) VALUES (%s);", chunk)
        done = min(i + chunk_size, len(gp_order_ids))
        pct = (done / len(gp_order_ids)) * 100
        print(f"\r   ⏳ Inserted {done:,}/{len(gp_order_ids):,} IDs ({pct:,.1f}%)", end="", flush=True)
    elapsed = time.time() - start_time
    print(f"\r✅ Uploaded {len(gp_order_ids):,} order IDs via chunked insert in {elapsed:,.1f}s. Running item-level query ...", end="", flush=True)

    cur.close()

    # Load item-level SQL and link it to the temporary ID list
    sql_query = Path("sql/S02_item_level.sql").read_text()
    sql_query = sql_query.replace("{{order_id_list}}", "SELECT gp_order_id FROM temp_order_ids")

    t0 = time.time()
    df_items = read_sql_clean(conn, sql_query)

    print(f"\n✅ Item-level query complete in {time.time() - t0:,.1f}s — {len(df_items):,} rows.")
    return df_items


# ----------------------------------------------------------------------------------------------------
# transform_item_data()
# ----------------------------------------------------------------------------------------------------
def transform_item_data(df_orders, df_items):
    """
    Merges item-level data into order-level dataset.

    Steps:
    1. Converts VAT band text values into simplified numeric codes (0, 5, 20, other).
    2. Pivots item-level rows into columns by VAT band (e.g., item_quantity_count_0, total_price_exc_vat_5, etc.)
    3. Merges the pivoted item data into the main df_orders DataFrame using gp_order_id.
    4. Clears duplicated item metrics for rows with multiple Braintree transactions (index >= 2).
    5. Sorts and aligns the final DataFrame column order to FINAL_DF_ORDER.

    Args:
        df_orders (pandas.DataFrame):
            The main order-level dataset.
        df_items (pandas.DataFrame):
            The VAT-band-level item data.

    Returns:
        pandas.DataFrame: Fully combined dataset with both order and item-level information.
    """
    # Normalize VAT band codes to simpler labels
    df_items["vat_band"] = df_items["vat_band"].replace({
        "0% VAT Band": "0",
        "5% VAT Band": "5",
        "20% VAT Band": "20",
        "Other / Unknown VAT Band": "other"
    })

    # Pivot VAT band rows into columns (aggregating by gp_order_id)
    df_pivot = (
        df_items.pivot_table(
            index="gp_order_id",
            columns="vat_band",
            values=["item_quantity_count", "total_price_inc_vat", "total_price_exc_vat"],
            aggfunc="sum",
            fill_value=0
        )
    )

    # Flatten multi-level column index (metric_band)
    df_pivot.columns = [f"{metric}_{band}" for metric, band in df_pivot.columns]

    # Calculate total number of products per order (sum across all VAT bands)
    df_pivot["total_products"] = df_pivot.filter(like="item_quantity_count_").sum(axis=1)

    # Merge back into main order-level data
    df_final = df_orders.merge(df_pivot, how="left", left_on="gp_order_id", right_index=True)

    # Clear duplicated item metrics for multi-transaction orders
    item_cols = [c for c in df_final.columns if any(x in c for x in [
        "item_quantity_count", "total_price_inc_vat", "total_price_exc_vat", "total_products"
    ])]
    mask = (df_final["braintree_tx_index"].notna()) & (df_final["braintree_tx_index"] >= 2)
    df_final.loc[mask, item_cols] = np.nan

    # Sort and align columns
    df_final = df_final.sort_values(by=["gp_order_id", "braintree_tx_index"], ascending=True)
    df_final = df_final[FINAL_DF_ORDER]

    print(f"✅ Combined order + item data: {len(df_final):,} rows, {len(df_final.columns):,} columns.")
    return df_final


# ----------------------------------------------------------------------------------------------------
# main()
# ----------------------------------------------------------------------------------------------------
def main():
    """
    Main function to orchestrate the DWH export workflow.

    Process overview:
      1. Connects to Snowflake and sets warehouse/database/schema context.
      2. Executes both order-level and item-level SQL extractions.
      3. Combines and standardizes the data into a final master DataFrame.
      4. Exports filtered subsets for each delivery provider to CSV.

    Returns:
        pandas.DataFrame: The combined order + item dataset (df_final).
    """
    # Connect to Snowflake
    conn = connect_to_snowflake()
    set_snowflake_context(conn)

    # Run SQL queries
    df_orders = run_order_level_query(conn)
    df_items = run_item_level_query(conn, df_orders)

    # Combine and finalize
    df_final = transform_item_data(df_orders, df_items)

    # Cleanly close connection
    conn.close()
    print("\n🔒 Connection closed cleanly.\n")

    # ------------------------------------------------------------------------------------------------
    # EXPORT TO PROVIDER CSVs
    # ------------------------------------------------------------------------------------------------
    start_date, end_date = get_reporting_period()
    period_label = pd.to_datetime(start_date).strftime("%y.%m")

    # Define subsets by provider type
    df_braintree = df_final.loc[(df_final["vendor_group"].str.lower() == "dtc") & (df_final["payment_system"].str.lower() != "paypal")]
    df_paypal = df_final.loc[(df_final["vendor_group"].str.lower() == "dtc") & (df_final["payment_system"].str.lower() == "paypal")]
    df_uber = df_final.loc[df_final["order_vendor"].str.lower() == "uber"]
    df_deliveroo = df_final.loc[df_final["order_vendor"].str.lower() == "deliveroo"]
    df_justeat = df_final.loc[df_final["order_vendor"].str.lower().isin(["just eat", "justeat"])]
    df_amazon = df_final.loc[df_final["order_vendor"].str.lower() == "amazon uk"]

    # Map providers to their output folders and filtered DataFrames
    provider_map = {
        "Braintree": (braintree_path, df_braintree),
        "PayPal": (paypal_path, df_paypal),
        "Uber": (uber_path, df_uber),
        "Deliveroo": (deliveroo_path, df_deliveroo),
        "Just Eat": (justeat_path, df_justeat),
        "Amazon": (amazon_path, df_amazon),
    }

    # Iterate over each provider and save its subset to CSV
    for provider, (path, df_subset) in provider_map.items():
        if df_subset.empty:
            print(f"⚠️ No rows found for {provider}, skipping.")
            continue

        path.mkdir(parents=True, exist_ok=True)
        filename = f"{period_label} - {provider} DWH data.csv"
        file_path = path / filename

        df_subset.to_csv(file_path, index=False)
        print(f"💾 Saved {len(df_subset):,} rows for {provider} → {file_path}")

    return df_final


# ----------------------------------------------------------------------------------------------------
# STANDALONE EXECUTION
# ----------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Allows running this module independently (not just imported by GUI or orchestrator)
    df_final = main()