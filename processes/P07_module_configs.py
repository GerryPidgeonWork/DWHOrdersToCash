# ====================================================================================================
# P07_module_configs.py
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   Defines configuration parameters used across modules — particularly the reporting period.
#   This file acts as a lightweight, user-editable settings hub.
# ----------------------------------------------------------------------------------------------------
# Usage:
#   • Edit REPORTING_START_DATE and REPORTING_END_DATE before running scripts
#     (e.g. M01_run_order_level.py or GUI tools).
#   • get_reporting_period() provides a consistent interface for accessing
#     the current reporting window across all modules.
# ----------------------------------------------------------------------------------------------------
# Future Extensions:
#   - These manual values can later be replaced or overridden via:
#       → GUI inputs
#       → Environment variables
#       → JSON configuration files
#       → Command-line arguments
# ----------------------------------------------------------------------------------------------------
# Safety:
#   - Dates must be in ISO format: YYYY-MM-DD
#   - This file is imported at runtime; avoid adding heavy imports or runtime logic.
# ====================================================================================================

# ----------------------------------------------------------------------------------------------------
# Import Libraries required to adjust sys path
# ----------------------------------------------------------------------------------------------------
import sys                      # Provides access to system-level parameters and functions
from pathlib import Path        # Provides object-oriented interface for filesystem paths

# Add parent directory to system path to allow imports from `processes/`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True  # Prevents creation of __pycache__ directories

# Import shared project packages (declared centrally in P00_set_packages.py)
from processes.P00_set_packages import *

# ----------------------------------------------------------------------------------------------------
# MANUAL REPORTING PERIOD OVERRIDE
# ----------------------------------------------------------------------------------------------------
# 👇 Edit these two lines before each run if you want to change the reporting period.
#    These values define the inclusive start and end dates for all SQL queries and output naming.
#    Example:
#       REPORTING_START_DATE = "2025-10-01"
#       REPORTING_END_DATE   = "2025-10-31"
# ----------------------------------------------------------------------------------------------------

REPORTING_START_DATE = "2025-11-01"
REPORTING_END_DATE   = "2025-11-30"

# ----------------------------------------------------------------------------------------------------
# get_reporting_period()
# ----------------------------------------------------------------------------------------------------
def get_reporting_period():
    """
    Retrieve the active reporting period.

    Returns:
        tuple[str, str]: (start_date, end_date)
            • start_date → ISO-formatted string (YYYY-MM-DD)
            • end_date   → ISO-formatted string (YYYY-MM-DD)

    Description:
        Provides a central, standardized way for all modules to read the
        reporting window. Currently returns static values defined above,
        but designed to be extended for dynamic inputs (e.g. GUI or config files).

    Example:
        >>> start, end = get_reporting_period()
        >>> print(start, end)
        2025-11-01 2025-11-30
    """
    start_date = REPORTING_START_DATE
    end_date   = REPORTING_END_DATE
    return start_date, end_date