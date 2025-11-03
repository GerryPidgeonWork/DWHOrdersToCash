# ====================================================================================================
# P04_static_lists.py
# ====================================================================================================

# ====================================================================================================
# Import Libraries that are required to adjust sys path
# ====================================================================================================
import sys                      # Provides access to system-specific parameters and functions
from pathlib import Path        # Offers an object-oriented interface for filesystem paths

# Adjust sys.path so we can import modules from the parent folder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True  # Prevents _pycache_ creation

# Import Project Libraries
from processes.P00_set_packages import *

# ====================================================================================================
# Import shared functions and file paths from other folders
# ====================================================================================================


# ====================================================================================================
# Static Lists
# ====================================================================================================

FINAL_DF_ORDER = ['GP_ORDER_ID', 'GP_ORDER_ID_OBFUSCATED', 'MP_ORDER_ID', 'PAYMENT_SYSTEM', 'BRAINTREE_TX_INDEX', 'BRAINTREE_TX_ID', 'LOCATION_NAME', 'ORDER_VENDOR', 
                  'VENDOR_GROUP', 'ORDER_COMPLETED', 'CREATED_AT_TIMESTAMP', 'DELIVERED_AT_TIMESTAMP', 'CREATED_AT_DAY', 'CREATED_AT_WEEK', 'CREATED_AT_MONTH', 'DELIVERED_AT_DAY', 
                   'DELIVERED_AT_WEEK', 'DELIVERED_AT_MONTH', 'OPS_DATE_DAY', 'OPS_DATE_WEEK', 'OPS_DATE_MONTH', 'POST_PROMO_SALES_INC_VAT', 'DELIVERY_FEE_INC_VAT', 
                   'PRIORITY_FEE_INC_VAT', 'SMALL_ORDER_FEE_INC_VAT', 'MP_BAG_FEE_INC_VAT', 'TOTAL_PAYMENT_INC_VAT', 'TIPS_AMOUNT', 'TOTAL_PAYMENT_WITH_TIPS_INC_VAT', 
                   'POST_PROMO_SALES_EXC_VAT', 'DELIVERY_FEE_EXC_VAT', 'PRIORITY_FEE_EXC_VAT', 'SMALL_ORDER_FEE_EXC_VAT', 'MP_BAG_FEE_EXC_VAT', 'TOTAL_REVENUE_EXC_VAT', 
                   'COST_OF_GOODS_INC_VAT', 'COST_OF_GOODS_EXC_VAT', 'ALT_POST_PROMO_SALES_INC_VAT', 'ALT_DELIVERY_FEE_EXC_VAT', 'ALT_PRIORITY_FEE_EXC_VAT', 
                   'ALT_SMALL_ORDER_FEE_EXC_VAT', 'ALT_TOTAL_PAYMENT_WITH_TIPS_INC_VAT', 'TOTAL_PRODUCTS', 'ITEM_QUANTITY_COUNT_0', 'ITEM_QUANTITY_COUNT_5', 
                   'ITEM_QUANTITY_COUNT_20', 'TOTAL_PRICE_EXC_VAT_0', 'TOTAL_PRICE_EXC_VAT_5', 'TOTAL_PRICE_EXC_VAT_20', 'TOTAL_PRICE_INC_VAT_0', 'TOTAL_PRICE_INC_VAT_5', 
                   'TOTAL_PRICE_INC_VAT_20']