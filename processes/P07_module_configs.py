# ====================================================================================================
# P07_module_configs.py
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


# ----------------------------------------------------------------------------------------------------
# MANUAL REPORTING PERIOD OVERRIDE
# 👇 Edit these two lines before each run if you want to change the reporting period
# ----------------------------------------------------------------------------------------------------

REPORTING_START_DATE = "2024-12-01"
REPORTING_END_DATE   = "2024-12-31"

# ----------------------------------------------------------------------------------------------------
# get_reporting_period()
# ----------------------------------------------------------------------------------------------------
def get_reporting_period():
    """
    Returns the current reporting period as (start_date, end_date).

    By default, reads the manual override values above.
    You can later adapt this to read from JSON, GUI input, or environment variables.
    """
    start_date = REPORTING_START_DATE
    end_date   = REPORTING_END_DATE
    return start_date, end_date