# ====================================================================================================
# M01_run_order_level.py
# ----------------------------------------------------------------------------------------------------
# Executes both:
#   • S01_order_level.sql → produces list of gp_order_id
#   • S02_item_level.sql  → uses those IDs to fetch detailed line-item data
#   • Combines order + item data into one DataFrame
# ====================================================================================================

import sys
from pathlib import Path
import time

# Adjust sys.path so we can import modules from the parent folder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True

# ----------------------------------------------------------------------------------------------------
# Import Project Libraries
# ----------------------------------------------------------------------------------------------------
from processes.P00_set_packages import *
from processes.P01_set_file_paths import braintree_path, paypal_path, uber_path, deliveroo_path, justeat_path, amazon_path
from processes.P02_system_processes import user_download_folder
from processes.P04_static_lists import FINAL_DF_ORDER
from processes.P07_module_configs import get_reporting_period
from processes.P08_snowflake_connector import connect_to_snowflake, set_snowflake_context

# ----------------------------------------------------------------------------------------------------
# run_order_level_query()
# ----------------------------------------------------------------------------------------------------
def run_order_level_query(conn):
    """Runs S01_order_level.sql and returns a pandas DataFrame."""
    start_date, end_date = get_reporting_period()
    sql_query = (
        Path("sql/S01_order_level.sql").read_text()
        .replace("{{start_date}}", start_date)
        .replace("{{end_date}}", end_date)
    )

    print(f"⏳ Executing order-level query for {start_date} → {end_date} ...", end="", flush=True)
    t0 = time.time()

    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        df_orders = pd.read_sql(sql_query, conn)

    print(f"\r✅ Order-level query complete in {time.time() - t0:,.1f}s — {len(df_orders):,} rows.")
    return df_orders

# ----------------------------------------------------------------------------------------------------
# run_item_level_query()
# ----------------------------------------------------------------------------------------------------
def run_item_level_query(conn, df_orders):
    """Runs S02_item_level.sql using gp_order_id values from df_orders."""
    gp_order_ids = df_orders["GP_ORDER_ID"].dropna().unique().tolist()
    if not gp_order_ids:
        raise ValueError("❌ No valid gp_order_id values found.")

    print(f"⏳ Uploading {len(gp_order_ids):,} order IDs to Snowflake ...", end="", flush=True)
    cur = conn.cursor()
    cur.execute("CREATE OR REPLACE TEMP TABLE temp_order_ids (gp_order_id STRING);")

    # Only use chunked insert (bulk upload removed)
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

    # Load item-level SQL
    sql_query = Path("sql/S02_item_level.sql").read_text()
    sql_query = sql_query.replace("{{order_id_list}}", "SELECT gp_order_id FROM temp_order_ids")

    t0 = time.time()
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        df_items = pd.read_sql(sql_query, conn)

    print(f"\r✅ Item-level query complete in {time.time() - t0:,.1f}s — {len(df_items):,} rows.")
    return df_items

# ----------------------------------------------------------------------------------------------------
# transform_item_data()
# ----------------------------------------------------------------------------------------------------
def transform_item_data(df_orders, df_items):
    """Pivot VAT band rows (0%, 5%, 20%, Other) into columns and merge into df_orders."""
    df_items["vat_band"] = df_items["VAT_BAND"].replace({
        "0% VAT Band": "0",
        "5% VAT Band": "5",
        "20% VAT Band": "20",
        "Other / Unknown VAT Band": "Other"
    })

    df_pivot = (
        df_items.pivot_table(
            index="GP_ORDER_ID",
            columns="vat_band",
            values=["ITEM_QUANTITY_COUNT", "TOTAL_PRICE_INC_VAT", "TOTAL_PRICE_EXC_VAT"],
            aggfunc="sum",
            fill_value=0
        )
    )

    # Capitalize all new column names
    df_pivot.columns = [f"{metric.upper()}_{band.upper()}" for metric, band in df_pivot.columns]
    df_pivot["TOTAL_PRODUCTS"] = df_pivot.filter(like="ITEM_QUANTITY_COUNT_").sum(axis=1)

    # Merge back into df_orders
    df_final = df_orders.merge(df_pivot, how="left", left_on="GP_ORDER_ID", right_index=True)

    # ✅ Clear merged item columns for BRAINTREE_TX_INDEX >= 2
    item_cols = [c for c in df_final.columns if any(x in c for x in ["ITEM_QUANTITY_COUNT", "TOTAL_PRICE_INC_VAT", "TOTAL_PRICE_EXC_VAT", "TOTAL_PRODUCTS"])]
    mask = (df_final["BRAINTREE_TX_INDEX"].notna()) & (df_final["BRAINTREE_TX_INDEX"] >= 2)
    df_final.loc[mask, item_cols] = np.nan

    # Sort by order_id + braintree_tx_index
    df_final = df_final.sort_values(by=["GP_ORDER_ID", "BRAINTREE_TX_INDEX"], ascending=True)
    df_final = df_final[FINAL_DF_ORDER]

    print(f"✅ Combined order + item data: {len(df_final):,} rows, {len(df_final.columns):,} columns.")
    return df_final

# ----------------------------------------------------------------------------------------------------
# main()
# ----------------------------------------------------------------------------------------------------
def main():
    conn = connect_to_snowflake()
    set_snowflake_context(conn)

    df_orders = run_order_level_query(conn)
    df_items = run_item_level_query(conn, df_orders)

    df_final = transform_item_data(df_orders, df_items)

    conn.close()
    print("\n🔒 Connection closed cleanly.\n")

    # ------------------------------------------------------------------------------------------------
    # Save individual provider files
    # ------------------------------------------------------------------------------------------------
    start_date, end_date = get_reporting_period()
    period_label = pd.to_datetime(start_date).strftime("%y.%m")

    # Define subsets
    df_braintree = df_final.loc[(df_final["VENDOR_GROUP"] == "DTC") & (df_final["PAYMENT_SYSTEM"] != "Paypal")]
    df_paypal = df_final.loc[(df_final["VENDOR_GROUP"] == "DTC") & (df_final["PAYMENT_SYSTEM"] == "Paypal")]
    df_uber = df_final.loc[df_final["ORDER_VENDOR"].str.lower() == "uber"]
    df_deliveroo = df_final.loc[df_final["ORDER_VENDOR"].str.lower() == "deliveroo"]
    df_justeat = df_final.loc[df_final["ORDER_VENDOR"].str.lower().isin(["just eat", "justeat"])]
    df_amazon = df_final.loc[df_final["ORDER_VENDOR"].str.lower() == "amazon uk"]

    # Map providers to paths and DataFrames
    provider_map = {
        "Braintree": (braintree_path, df_braintree),
        "PayPal": (paypal_path, df_paypal),
        "Uber": (uber_path, df_uber),
        "Deliveroo": (deliveroo_path, df_deliveroo),
        "Just Eat": (justeat_path, df_justeat),
        "Amazon": (amazon_path, df_amazon),
    }

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
# Standalone execution
# ----------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    df_final = main()
