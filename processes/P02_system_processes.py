# ====================================================================================================
# P02_system_processes.py
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   Provides system-level utilities that detect the user's operating environment and
#   determine standard user directories (e.g. Downloads).
# ----------------------------------------------------------------------------------------------------
# Usage:
#   • detect_os() → identifies the current OS environment (Windows, macOS, Linux, WSL, iOS).
#   • user_download_folder() → returns the absolute Path to the user’s Downloads folder.
# ----------------------------------------------------------------------------------------------------
# These functions are used in downstream modules (e.g. GUI tools, reconciliation scripts)
# to dynamically locate user files regardless of platform or setup.
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
# detect_os()
# ----------------------------------------------------------------------------------------------------
def detect_os() -> str:
    """
    Detect the current operating system or execution environment.

    Returns:
        str: One of the following labels:
            - "Windows"       → Native Windows environment.
            - "Windows (WSL)" → Windows Subsystem for Linux (Microsoft kernel signature).
            - "macOS"         → Apple macOS environment.
            - "Linux"         → Generic Linux (non-WSL).
            - "iOS"           → Python on iOS (Pythonista, Pyto, etc.).
            - <other>         → Fallback for unidentified platforms.

    Detection Method:
        • Uses sys.platform for broad OS category.
        • Uses platform.uname().release to distinguish WSL from Linux.
        • Uses platform.machine() for Darwin-based systems (to detect iOS).

    Example:
        >>> detect_os()
        'Windows (WSL)'
    """
    # 1) Native Windows
    if sys.platform == "win32":
        return "Windows"

    # 2) macOS or iOS (Darwin-based systems)
    if sys.platform == "darwin":
        machine = platform.machine() or ""
        # iOS devices report machine identifiers starting with "iP" (iPhone/iPad)
        if machine.startswith(("iP",)):
            return "iOS"
        # Otherwise treat as macOS
        return "macOS"

    # 3) Linux or WSL
    if sys.platform.startswith("linux"):
        release = platform.uname().release.lower()
        # Detect Microsoft kernel tags that indicate WSL
        if "microsoft" in release or "wsl" in release:
            return "Windows (WSL)"
        return "Linux"

    # 4) Any other platform → return raw sys.platform string
    return sys.platform


# ----------------------------------------------------------------------------------------------------
# user_download_folder()
# ----------------------------------------------------------------------------------------------------
def user_download_folder() -> Path:
    """
    Determine the appropriate "Downloads" folder for the current user.

    Returns:
        Path: Absolute path to the user's Downloads directory.
              Falls back to home directory if Downloads folder not applicable.

    Logic:
        • On Windows → Uses C:/Users/<username>/Downloads.
        • On WSL     → Uses ~/Downloads (Linux home path).
        • On macOS   → Uses ~/Downloads.
        • On Linux   → Uses ~/Downloads.
        • On iOS     → Returns the home directory (no standard Downloads folder).

    Example:
        >>> user_download_folder()
        WindowsPath('C:/Users/Gerry/Downloads')
    """
    os_type = detect_os()

    if os_type == "Windows":
        # Native Windows → standard Downloads under user profile
        return Path(f"C:/Users/{getpass.getuser()}/Downloads")

    elif os_type == "Windows (WSL)":
        # WSL → use Linux-style home directory
        return Path.home() / "Downloads"

    elif os_type == "macOS":
        # macOS → user home Downloads folder
        return Path.home() / "Downloads"

    elif os_type == "Linux":
        # Generic Linux → user home Downloads folder
        return Path.home() / "Downloads"

    elif os_type == "iOS":
        # iOS → fallback to user home (no Downloads directory)
        return Path.home()

    # Default fallback for any other environment
    return Path.home()


# ----------------------------------------------------------------------------------------------------
# Standalone Execution (Diagnostic)
# ----------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    """
    Allow this module to be run directly for diagnostic purposes.
    Prints the detected OS and corresponding Downloads folder.
    """
    print(f"Detected OS: {detect_os()}")
    print(f"Download folder: {user_download_folder()}")
