# ====================================================================================================
# P01_set_file_paths.py
# ----------------------------------------------------------------------------------------------------
# Centralized definition of all key file and folder paths used across the Orders-to-Cash workflow.
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   • Provides consistent, single-source path references for all data inputs/outputs.
#   • Ensures other modules (e.g. M01_run_order_level.py, GUIs, processors) can import paths
#     without hardcoding or duplicating directory logic.
# ----------------------------------------------------------------------------------------------------
# Update Policy:
#   • Modify only `root_path` to repoint the entire project to a different drive or directory.
#   • Subfolder structures should remain constant across all environments (Windows, Mac, WSL).
# ----------------------------------------------------------------------------------------------------
# Example Usage:
#   from processes.P01_set_file_paths import braintree_path
#   output_file = braintree_path / "25.09 - Braintree Data.csv"
# ====================================================================================================

# ----------------------------------------------------------------------------------------------------
# Import Libraries required to adjust sys path
# ----------------------------------------------------------------------------------------------------
import sys                      # Provides access to system-level parameters and functions
from pathlib import Path        # Provides object-oriented filesystem path handling

# Add parent directory to system path to allow imports from `processes/`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True  # Prevents creation of __pycache__ directories

# Import project-wide dependencies (defined once in P00_set_packages.py)
from processes.P00_set_packages import *

# ----------------------------------------------------------------------------------------------------
# Root Path Definition
# ----------------------------------------------------------------------------------------------------
# Adjust this single variable if the shared drive or folder location changes.
# All downstream folder paths are derived from this base.
root_path = Path(r"H:\Shared drives\Automation Projects\Accounting\Orders to Cash")

# ----------------------------------------------------------------------------------------------------
# Provider-specific DWH Export Paths
# ----------------------------------------------------------------------------------------------------
# Each path corresponds to the “03 DWH” subfolder of its provider’s root folder.
# These are used as the output directories for CSV exports in M01_run_order_level.py.
braintree_path = root_path / '01 Braintree' / '03 DWH'
paypal_path    = root_path / '02 Paypal'    / '03 DWH'
uber_path      = root_path / '03 Uber Eats' / '03 DWH'
deliveroo_path = root_path / '04 Deliveroo' / '03 DWH'
justeat_path   = root_path / '05 Just Eat'  / '03 DWH'
amazon_path    = root_path / '06 Amazon'    / '03 DWH'