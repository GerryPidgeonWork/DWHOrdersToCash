# ====================================================================================================
# P00_set_packages.py
# ----------------------------------------------------------------------------------------------------
# Centralized import file that sets up all global packages used across the project.
# Ensures consistent imports and prevents duplication across modules.
# Optimized for SQL (Snowflake) + Pandas workflow.
# ====================================================================================================


# ----------------------------------------------------------------------------------------------------
# --- Standard library imports (no installation required) ---
# ----------------------------------------------------------------------------------------------------
import os                                                       # OS-level operations (paths, environment variables)
import io                                                       # Handle in-memory file-like streams (e.g., StringIO)
import re                                                       # Regular expressions for pattern matching
import sys                                                      # Access system-specific parameters, e.g., sys.path
import csv                                                      # Read/write CSV files natively
import time                                                     # Time utilities (sleep, timestamps, timing performance)
import json                                                     # Read/write JSON files for configs or structured data
import glob                                                     # Pattern-based file searches (e.g., *.csv, *.py)
import shutil                                                   # File operations: copy, move, delete
import getpass                                                  # Retrieve current OS username securely
import logging                                                  # Standard logging for info/warning/error tracking
import threading                                                # Run lightweight concurrent tasks
import contextlib                                               # Manage temporary context scopes (e.g., redirect_stdout)
import tkinter as tk
import datetime as dt
import calendar
from datetime import date, datetime, timedelta                  # Work with dates and times
from pathlib import Path                                        # Cross-platform, object-oriented path handling
from functools import lru_cache, partial                        # Memoization + preconfigured function wrappers
from typing import Iterable, Callable, Optional, List, Dict     # Type hints for clean function signatures
from dataclasses import dataclass                               # Lightweight class creation (auto __init__, __repr__, etc.)
from tkinter import ttk

# The following import is optional:
# Only needed if you plan to define your own context managers with the @contextmanager decorator.
from contextlib import contextmanager                           # Simplify resource management (e.g., custom DB contexts)

import warnings                                                 # Control or suppress library warnings
warnings.filterwarnings("ignore", category=UserWarning)         # Suppress noisy UserWarnings (e.g., Pandas or Deprecation)

# ----------------------------------------------------------------------------------------------------
# --- Third-party imports (require installation via pip) ---
# ----------------------------------------------------------------------------------------------------
import pandas as pd                                             # (pip install pandas) Data analysis and manipulation
import numpy as np                                              # (installed with pandas) Numerical arrays, fast math ops
import snowflake.connector                                      # (pip install snowflake-connector-python) Run SQL in Snowflake

# ----------------------------------------------------------------------------------------------------
# Default pandas display settings (for readability and consistency)
# ----------------------------------------------------------------------------------------------------
pd.set_option("display.max_columns", None)                      # Always show all columns in printed DataFrames
pd.set_option("display.width", 200)                             # Wider console display for tabular outputs
pd.set_option("display.float_format", "{:,.2f}".format)         # Uniform float formatting (e.g., 1,234.56)


# ----------------------------------------------------------------------------------------------------
# Logging configuration (used throughout the project)
# ----------------------------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,                                         # Default level: INFO (change to DEBUG for verbose)
    format="%(asctime)s | %(levelname)-8s | %(message)s",        # Timestamp + level + message layout
    datefmt="%Y-%m-%d %H:%M:%S"                                 # Standard timestamp format
)