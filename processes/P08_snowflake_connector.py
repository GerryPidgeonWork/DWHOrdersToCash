# ====================================================================================================
# P08_snowflake_connector.py
# ====================================================================================================
# Simplified Snowflake Okta connector (multi-user friendly)
# ----------------------------------------------------------------------------------------------------
# • Prompts for GoPuff email only if not preset
# • Uses Okta "externalbrowser" login flow
# • Times out after 20 seconds if authentication not completed
# • Hides SSO URL output for cleaner UX
# • Prints connection summary and closes cleanly
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
from processes.P00_set_packages import *                         # Centralized imports and settings

# ----------------------------------------------------------------------------------------------------
# Default Snowflake Configuration
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
    Retrieve Snowflake credentials (email, account, etc.)
    Uses stored environment variable if available; otherwise prompts user.
    """
    stored_user = os.getenv("SNOWFLAKE_USER", "").strip()
    default_email = ""  # 👈 Default email fallback

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
        os.environ["SNOWFLAKE_USER"] = user  # Cache for current session

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
    Establishes a Snowflake connection using Okta SSO (externalbrowser).
    Includes timeout and clean UX (no long SSO URLs printed).
    """
    creds = get_snowflake_credentials()
    conn_container = {}

    def _connect():
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
    Sets Snowflake session context (warehouse, database, schema).
    If the warehouse is invalid, lists available ones before exiting.
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

    # Set DB + Schema context
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
    try:
        conn = connect_to_snowflake()
        set_snowflake_context(conn)
        print("✅ Snowflake context set successfully.\n")

        cur = conn.cursor()
        cur.execute("""
            SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
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
