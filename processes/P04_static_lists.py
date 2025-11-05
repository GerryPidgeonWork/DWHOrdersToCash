# ====================================================================================================
# P04_static_lists.py
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   Stores global static lists and constants used throughout the Orders-to-Cash workflow.
#   These lists act as configuration-like reference structures, ensuring consistent ordering
#   of columns, standardized labels, or fixed mappings across multiple scripts.
# ----------------------------------------------------------------------------------------------------
# Update Policy:
#   • Keep naming conventions consistent with the normalized DataFrame schema (lowercase, underscores).
#   • Add or remove fields only when schema changes are confirmed in both DWH and downstream processes.
#   • Any modification should be followed by testing in all dependent modules.
# ----------------------------------------------------------------------------------------------------
# Example Usage:
#   from processes.P04_static_lists import FINAL_DF_ORDER
#   df = df[FINAL_DF_ORDER]      # Ensures consistent column order before CSV export
# ====================================================================================================

# ----------------------------------------------------------------------------------------------------
# Import Libraries required to adjust sys path
# ----------------------------------------------------------------------------------------------------
import sys                      # Provides access to system-level parameters and functions
from pathlib import Path        # Provides object-oriented filesystem path handling

# Add parent directory to system path to allow imports from `processes/`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True  # Prevents creation of __pycache__ directories

# Import shared project packages (declared centrally in P00_set_packages.py)
from processes.P00_set_packages import *

# ----------------------------------------------------------------------------------------------------
# FINAL_DF_ORDER
# ----------------------------------------------------------------------------------------------------
# Defines the canonical column order for the final combined DataFrame
# produced by M01_run_order_level.py (after joining order- and item-level data).
#
# This ensures:
#   • Consistent CSV exports across providers (Braintree, Uber, Deliveroo, etc.)
#   • Logical column grouping: identifiers → timestamps → financials → item metrics.
#   • Simplified downstream processing and validation.
#
# Note:
#   - All names are lowercase, following normalize_columns() output.
#   - The list length and order must align with the SELECT statements in the SQL templates.
# ----------------------------------------------------------------------------------------------------

FINAL_DF_ORDER = [
    # ---- Identifiers ----
    'gp_order_id', 'gp_order_id_obfuscated', 'mp_order_id',
    'payment_system', 'braintree_tx_index', 'braintree_tx_id',
    'location_name', 'order_vendor', 'vendor_group',

    # ---- Status and timestamps ----
    'order_completed', 'created_at_timestamp', 'delivered_at_timestamp',
    'created_at_day', 'created_at_week', 'created_at_month',
    'delivered_at_day', 'delivered_at_week', 'delivered_at_month',
    'ops_date_day', 'ops_date_week', 'ops_date_month',

    # ---- Financials: VAT and Revenue ----
    'blended_vat_rate', 'post_promo_sales_inc_vat',
    'delivery_fee_inc_vat', 'priority_fee_inc_vat',
    'small_order_fee_inc_vat', 'mp_bag_fee_inc_vat',
    'total_payment_inc_vat', 'tips_amount',
    'total_payment_with_tips_inc_vat',

    'post_promo_sales_exc_vat', 'delivery_fee_exc_vat',
    'priority_fee_exc_vat', 'small_order_fee_exc_vat',
    'mp_bag_fee_exc_vat', 'total_revenue_exc_vat',
    'cost_of_goods_inc_vat', 'cost_of_goods_exc_vat',

    # ---- Alternate metrics (for validation/reconciliation) ----
    'alt_post_promo_sales_inc_vat', 'alt_delivery_fee_exc_vat',
    'alt_priority_fee_exc_vat', 'alt_small_order_fee_exc_vat',
    'alt_total_payment_with_tips_inc_vat',

    # ---- Item-level breakdown ----
    'total_products',
    'item_quantity_count_0', 'item_quantity_count_5', 'item_quantity_count_20',
    'total_price_exc_vat_0', 'total_price_exc_vat_5', 'total_price_exc_vat_20',
    'total_price_inc_vat_0', 'total_price_inc_vat_5', 'total_price_inc_vat_20'
]
