# ====================================================================================================
# M00_run_gui.py
# ----------------------------------------------------------------------------------------------------
# Launches the DWH Orders-to-Cash Extractor GUI
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   Provides a clean entry point to launch the Tkinter GUI defined in P05_gui_elements.py.
#   This separation keeps processes/ reserved for shared components and logic,
#   while main/ modules serve as executable entry points.
# ----------------------------------------------------------------------------------------------------
# Integration:
#   • Imports GUI class from processes.P05_gui_elements
#   • Runs the GUI mainloop() when executed directly
# ====================================================================================================

import sys
from pathlib import Path

# Ensure the parent folder (project root) is available for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True

# ----------------------------------------------------------------------------------------------------
# Import GUI
# ----------------------------------------------------------------------------------------------------
from processes.P05_gui_elements import DWHOrdersToCashGUI

# ----------------------------------------------------------------------------------------------------
# Main Launcher
# ----------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    app = DWHOrdersToCashGUI()
    app.mainloop()
