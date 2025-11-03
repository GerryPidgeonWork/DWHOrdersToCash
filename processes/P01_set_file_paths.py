# ====================================================================================================
# P01_set_file_paths.py
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
# Set Export Paths
# ====================================================================================================

root_path = Path(r"H:\Shared drives\Automation Projects\Accounting\Orders to Cash")

braintree_path = root_path / '01 Braintree' / '03 DWH'
paypal_path = root_path / '02 Paypal' / '03 DWH'
uber_path = root_path / '03 Uber Eats' / '03 DWH'
deliveroo_path = root_path / '04 Deliveroo' / '03 DWH'
justeat_path = root_path / '05 Just Eat' / '03 DWH'
amazon_path = root_path / '06 Amazon' / '03 DWH'