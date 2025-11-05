# ====================================================================================================
# P08_snowflake_connector.py
# ----------------------------------------------------------------------------------------------------
# Purpose:
#   Provides a simplified and multi-user-friendly connection layer to Snowflake via Okta SSO.
#   Standardizes connection setup and context configuration for all GoPuff DWH scripts.
# ----------------------------------------------------------------------------------------------------
# Features:
#   • Uses Okta’s “externalbrowser” authenticator for seamless SSO login.
#   • Automatically detects or prompts for the GoPuff email address.
#   • Times out after 20 seconds if login not completed.
#   • Suppresses verbose Snowflake output for a clean console experience.
#   • Confirms connection and warehouse/database/schema context.
# ----------------------------------------------------------------------------------------------------
# Update Policy:
#   • Keep credentials and roles generic — do not hardcode user-specific values.
#   • Test changes with both Windows + macOS/WSL setups before committing.
#   • All imports must remain centralized in P00_set_packages.py.
# ----------------------------------------------------------------------------------------------------
# Typical Usage:
#   from processes.P08_snowflake_connector import connect_to_snowflake, set_snowflake_context
#   conn = connect_to_snowflake()
#   set_snowflake_context(conn)
#   df = pd.read_sql("SELECT * FROM core.orders LIMIT 10", conn)
# ----------------------------------------------------------------------------------------------------
# Safe Exit:
#   If the user fails to authenticate or a warehouse is invalid, the script exits cleanly with
#   explanatory output and no partial state left behind.
# ====================================================================================================


# ----------------------------------------------------------------------------------------------------
# Import Libraries required to adjust sys path
# ----------------------------------------------------------------------------------------------------
import sys                      # Provides access to system-level parameters and functions
from pathlib import Path        # Provides an object-oriented interface for filesystem paths

# Add parent directory to system path to allow imports from `processes/`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.dont_write_bytecode = True  # Prevents creation of __pycache__ directories

# Import shared project packages (declared centrally in P00_set_packages.py)
from processes.P00_set_packages import *


# ----------------------------------------------------------------------------------------------------
# Default Snowflake Configuration
# ----------------------------------------------------------------------------------------------------
# These defaults represent standard GoPuff Analytics DWH context. They are used across
# all scripts unless explicitly overridden at runtime or via environment variables.
# ----------------------------------------------------------------------------------------------------
DEFAULT_ACCOUNT = "HC77929-GOPUFF"
DEFAULT_ROLE = "OKTA_ANALYTICS_ROLE"
DEFAULT_WAREHOUSE = "ANALYTICS"
DEFAULT_DATABASE = "DBT_PROD"
DEFAULT_SCHEMA = "CORE"
AUTHENTICATOR = "externalbrowser"
TIMEOUT_SECONDS = 20


# ----------------------------------------------------------------------------------------------------
# get_snowflake_credentials()
# ----------------------------------------------------------------------------------------------------
def get_snowflake_credentials():
    """
    Retrieve user credentials and environment configuration for Snowflake.

    Priority:
        1. Environment variable `SNOWFLAKE_USER` (if previously set)
        2. Default email fallback (gerry.pidgeon@gopuff.com)
        3. Manual user input prompt

    Returns:
        dict: A dictionary containing Snowflake connection parameters, including:
              - user, account, role, warehouse, database, schema, authenticator

    Notes:
        • The authenticator uses Okta SSO (“externalbrowser”) by default.
        • The retrieved credentials are lightweight — no password or MFA tokens are stored.
        • The function prints which user email is being used to ensure transparency.

    Example:
        >>> creds = get_snowflake_credentials()
        >>> creds["user"]
        'firstname.lastname@gopuff.com'
    """
    stored_user = os.getenv("SNOWFLAKE_USER", "").strip()
    default_email = "gerry.pidgeon@gopuff.com"  # 👈 Default email fallback for convenience

    if stored_user:
        print(f"\n📧 Using stored email address: {stored_user}\n")
        user = stored_user
    elif default_email:
        print(f"\n📧 Using default email address: {default_email}\n")
        user = default_email
        os.environ["SNOWFLAKE_USER"] = user
    else:
        print("\n🔑 Please enter your GoPuff email address (e.g. firstname.lastname@gopuff.com)")
        user = input("Email: ").strip()
        os.environ["SNOWFLAKE_USER"] = user  # Cache for session reuse

    return {
        "user": user,
        "account": os.getenv("SNOWFLAKE_ACCOUNT", DEFAULT_ACCOUNT),
        "role": os.getenv("SNOWFLAKE_ROLE", DEFAULT_ROLE),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE),
        "database": os.getenv("SNOWFLAKE_DATABASE", DEFAULT_DATABASE),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", DEFAULT_SCHEMA),
        "authenticator": AUTHENTICATOR,
    }


# ----------------------------------------------------------------------------------------------------
# connect_to_snowflake()
# ----------------------------------------------------------------------------------------------------
def connect_to_snowflake():
    """
    Establish a Snowflake connection using Okta SSO via the "externalbrowser" method.

    Behaviour:
        • Suppresses verbose Snowflake connector output.
        • Times out gracefully if user does not complete authentication in TIMEOUT_SECONDS.
        • Prints a confirmation with connected username upon success.

    Returns:
        snowflake.connector.connection.SnowflakeConnection:
            An active Snowflake connection object.

    Raises:
        SystemExit: If connection fails or timeout occurs.

    Example:
        >>> conn = connect_to_snowflake()
        🔄 Attempting Snowflake connection...
        ✅ Connected successfully as firstname.lastname@gopuff.com
    """
    creds = get_snowflake_credentials()
    conn_container = {}

    def _connect():
        # Run in background thread to allow timeout handling
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            try:
                conn = snowflake.connector.connect(**creds)
                conn_container["conn"] = conn
            except Exception as e:
                conn_container["error"] = e

    print("🔄 Attempting Snowflake connection...\n")

    thread = threading.Thread(target=_connect)
    thread.start()
    thread.join(timeout=TIMEOUT_SECONDS)

    if thread.is_alive():
        print(f"⏰ Timeout: No authentication detected after {TIMEOUT_SECONDS} seconds. Exiting.")
        sys.exit(1)

    if "error" in conn_container:
        err = str(conn_container["error"])
        if "differs from the user currently logged in" in err:
            print(f"\n❌ Connection failed: {err}")
            user = input("\n🔑 Please enter your correct GoPuff email address: ").strip()
            os.environ["SNOWFLAKE_USER"] = user
            print(f"\n🔁 Retrying connection for {user}...\n")
            creds["user"] = user
            conn = snowflake.connector.connect(**creds)
        else:
            print(f"\n❌ Connection failed: {err}")
            sys.exit(1)
    else:
        conn = conn_container["conn"]

    print(f"✅ Connected successfully as {creds['user']}\n")
    return conn


# ----------------------------------------------------------------------------------------------------
# set_snowflake_context()
# ----------------------------------------------------------------------------------------------------
def set_snowflake_context(conn, warehouse=DEFAULT_WAREHOUSE, database=DEFAULT_DATABASE, schema=DEFAULT_SCHEMA):
    """
    Set the Snowflake session context (warehouse, database, schema) for the active connection.

    Args:
        conn (snowflake.connector.connection.SnowflakeConnection):
            Active Snowflake connection.
        warehouse (str): Target warehouse (default = "ANALYTICS").
        database (str): Target database (default = "DBT_PROD").
        schema (str): Target schema (default = "CORE").

    Behaviour:
        • Attempts to set the active context for the current session.
        • If warehouse is invalid, lists all accessible warehouses before exiting.
        • Prints a summary of the current active context.

    Example:
        >>> set_snowflake_context(conn)
        📂 Active Context: Warehouse=ANALYTICS, Database=DBT_PROD, Schema=CORE
    """
    cur = conn.cursor()
    try:
        cur.execute(f"USE WAREHOUSE {warehouse};")
    except Exception:
        print(f"\n⚠️ Warehouse '{warehouse}' not found or not accessible for your role.")
        cur.execute("SHOW WAREHOUSES;")
        print("\n🏭 Available warehouses for your role:\n")
        for wh in cur.fetchall():
            print(f" - {wh[0]}")
        sys.exit(1)

    # Set database + schema context
    cur.execute(f"USE DATABASE {database};")
    cur.execute(f"USE SCHEMA {schema};")

    # Confirm context
    cur.execute("SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();")
    wh, db, sc = cur.fetchone()
    print(f"\n📂 Active Context: Warehouse={wh}, Database={db}, Schema={sc}\n")

    cur.close()


# ----------------------------------------------------------------------------------------------------
# Standalone test
# ----------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    """
    Manual test runner for verifying Snowflake connectivity and context.
    Safe to execute standalone (outside main workflow) for connection validation.

    Example:
        > python processes/P08_snowflake_connector.py
    """
    try:
        conn = connect_to_snowflake()
        set_snowflake_context(conn)
        print("✅ Snowflake context set successfully.\n")

        # Display session details for confirmation
        cur = conn.cursor()
        cur.execute("""
            SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE(),
                   CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
        """)
        result = cur.fetchone()
        print(
            f"👤 User: {result[0]}\n"
            f"🏢 Account: {result[1]}\n"
            f"🧩 Role: {result[2]}\n"
            f"🏭 Warehouse: {result[3]}\n"
            f"📚 Database: {result[4]}\n"
            f"📁 Schema: {result[5]}"
        )

        cur.close()
        conn.close()
        print("\n🔒 Connection closed cleanly.")

    except KeyboardInterrupt:
        print("\n❌ Connection aborted by user.")
        sys.exit(1)
