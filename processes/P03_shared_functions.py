# ====================================================================================================
# P03_shared_functions.py
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   Contains small, reusable helper functions that are shared across multiple modules.
#   These typically perform common cleanup or standardized I/O routines.
# ----------------------------------------------------------------------------------------------------
# Current Functions:
#   • normalize_columns(df) → Standardizes DataFrame column naming to lowercase with underscores.
#   • read_sql_clean(conn, sql_query) → Executes SQL quietly and returns a cleaned DataFrame.
# ----------------------------------------------------------------------------------------------------
# Update Policy:
#   Keep this file focused on small, generic utilities that do not depend on any specific
#   provider or data schema. Larger, context-specific helpers should live elsewhere.
# ====================================================================================================

# ----------------------------------------------------------------------------------------------------
# Import Libraries required to adjust sys path
# ----------------------------------------------------------------------------------------------------
import sys                      # Provides access to system-specific parameters and functions
from pathlib import Path        # Offers an object-oriented interface for filesystem paths

# Add parent directory to system path to allow imports from `processes/`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True  # Prevents creation of __pycache__ directories

# Import shared project packages (declared centrally in P00_set_packages.py)
from processes.P00_set_packages import *

# ----------------------------------------------------------------------------------------------------
# normalize_columns()
# ----------------------------------------------------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize DataFrame column names for consistency across data sources.

    Steps performed:
        • Strips leading/trailing whitespace.
        • Converts all column names to lowercase.
        • Replaces spaces and hyphens with underscores.

    Args:
        df (pd.DataFrame): Input DataFrame whose columns will be renamed.

    Returns:
        pd.DataFrame: The same DataFrame with normalized column names.

    Example:
        >>> df.columns
        Index(['Order ID', 'Payment-System', 'Created At'], dtype='object')
        >>> normalize_columns(df).columns
        Index(['order_id', 'payment_system', 'created_at'], dtype='object')
    """
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df


# ----------------------------------------------------------------------------------------------------
# read_sql_clean()
# ----------------------------------------------------------------------------------------------------
def read_sql_clean(conn, sql_query: str) -> pd.DataFrame:
    """
    Execute an SQL query using a given Snowflake connection, suppressing console output,
    and returning a DataFrame with normalized column names.

    Steps performed:
        1. Executes the SQL query via pandas.read_sql().
        2. Silences any driver-level stdout/stderr output (for cleaner logs).
        3. Applies normalize_columns() to ensure column consistency.

    Args:
        conn: Active database connection (e.g. Snowflake connector).
        sql_query (str): Fully-formed SQL query string to execute.

    Returns:
        pd.DataFrame: Query results with standardized column naming.

    Example:
        >>> df_orders = read_sql_clean(conn, "SELECT * FROM core.orders LIMIT 5")
        >>> df_orders.head()
    """
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        df = pd.read_sql(sql_query, conn)
    return normalize_columns(df)
