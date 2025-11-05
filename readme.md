# GoPuff UK – DWH Orders-to-Cash Extractor

### Automated Snowflake SQL → Pandas Integration with GUI Launcher

---

## 📦 Overview

This project automates the extraction of **order-level** and **item-level** data from GoPuff’s Snowflake Data Warehouse, combining both into a single cleaned DataFrame and generating CSV exports by provider (Braintree, PayPal, Uber Eats, Deliveroo, Just Eat, Amazon).

It forms the **foundation of the Orders-to-Cash reconciliation process** — powering downstream workflows for statement reconciliation, accruals, and performance reporting.

---

## 🧠 Architecture Summary

The project follows a modular, maintainable structure:

```
DWHOrdersToCash/
│
├── main/
│   ├── M00_run_gui.py              # NEW: GUI launcher entry point
│   └── M01_combine_sql.py          # Main orchestrator (runs all queries + saves outputs)
│
├── processes/
│   ├── P00_set_packages.py         # Centralised imports & 3rd-party install notes
│   ├── P01_set_file_paths.py       # Shared folder definitions for each provider
│   ├── P02_system_processes.py     # OS detection and user Downloads path helper
│   ├── P03_shared_functions.py     # Common helpers (normalise_columns, read_sql_clean)
│   ├── P04_static_lists.py         # Fixed reference lists (column ordering, mappings)
│   ├── P05_gui_elements.py         # GUI class definition (Tkinter-based interface)
│   ├── P07_module_configs.py       # Manual config (reporting start/end dates)
│   ├── P08_snowflake_connector.py  # Handles Okta SSO connection + session context
│
└── sql/
    ├── S01_order_level.sql         # Retrieves order-level data
    └── S02_item_level.sql          # Retrieves item-level aggregates per VAT band
```

---

## ⚙️ Key Components

### 1️⃣ `M00_run_gui.py`

The official entry point for running the tool. It launches the Tkinter GUI defined in `P05_gui_elements.py`.

From here, users can:

* Select their Snowflake login (Gerry, Dimitrios, or custom)
* Adjust or confirm the reporting month
* Run the entire extraction process via a button click
* View live logs from the SQL process directly within the GUI

### 2️⃣ `M01_combine_sql.py`

The main orchestrator that:

* Connects to Snowflake via Okta SSO
* Executes `S01_order_level.sql` and `S02_item_level.sql`
* Merges both outputs via `transform_item_data()`
* Exports cleaned CSVs per provider (Braintree, PayPal, Uber Eats, Deliveroo, Just Eat, Amazon)

### 3️⃣ `P05_gui_elements.py`

Defines the interactive GUI used to launch the process. It:

* Displays live log updates (stdout redirected to GUI)
* Handles dynamic date detection (current/prior month logic)
* Allows overrides and manual user email selection

### 4️⃣ `P00_set_packages.py`

Centralised import manager – every module imports from here to maintain consistent dependencies.

### 5️⃣ `P08_snowflake_connector.py`

Handles Okta SSO authentication, connection retries, and schema/warehouse context setup.

---

## 🧰 Workflow Summary

```
M00_run_gui.py
    ↓
P05_gui_elements.DWHOrdersToCashGUI()
    ↓
M01_combine_sql.main()
    ↓
connect_to_snowflake()          → Okta SSO login
set_snowflake_context()         → USE WAREHOUSE / DATABASE / SCHEMA
get_reporting_period()          → From P07_module_configs.py
run_order_level_query()         → Executes S01_order_level.sql
run_item_level_query()          → Executes S02_item_level.sql (via temp table)
transform_item_data()           → Pivot VAT bands + merge with df_orders
save_to_csv()                   → Writes provider-level output files
```

---

## 📁 Output Structure

Exports are saved into structured subfolders on the shared drive:

```
H:\Shared drives\Automation Projects\Accounting\Orders to Cash\
│
├── 01 Braintree\03 DWH\YY.MM - Braintree DWH data.csv
├── 02 Paypal\03 DWH\YY.MM - PayPal DWH data.csv
├── 03 Uber Eats\03 DWH\YY.MM - Uber DWH data.csv
├── 04 Deliveroo\03 DWH\YY.MM - Deliveroo DWH data.csv
├── 05 Just Eat\03 DWH\YY.MM - Just Eat DWH data.csv
└── 06 Amazon\03 DWH\YY.MM - Amazon DWH data.csv
```

Each file contains all relevant rows for that provider for the selected month.

---

## 🗓️ Configuration

Reporting period can be set manually or dynamically via the GUI.

### Manual configuration (optional)

```python
# processes/P07_module_configs.py
REPORTING_START_DATE = "2025-11-01"
REPORTING_END_DATE   = "2025-11-30"
```

### Automatic configuration

When using the GUI, if today is within 9 days after month-end, it defaults to the **previous month**; otherwise, the **current month**.

---

## 🚀 Usage

### Prerequisites

* Python 3.12+
* Access to GoPuff Snowflake via Okta SSO
* Dependencies installed via `P00_set_packages.py`

### Run via GUI

```
python main/M00_run_gui.py
```

### Run directly via terminal (advanced)

```
python main/M01_combine_sql.py
```

---

## 📈 Example GUI Output

```
📧 Using stored email address: gerry.pidgeon@gopuff.com
✅ Connected successfully to Snowflake
✅ Order-level query complete in 30.0s — 474,209 rows.
✅ Uploaded 452,502 order IDs (temp table)
✅ Item-level query complete in 4.1s — 732,215 rows.
✅ Combined order + item data: 474,209 rows, 53 columns.
💾 Saved 65,577 rows for Deliveroo → H:\...\04 Deliveroo\03 DWH\25.10 - Deliveroo DWH data.csv
✅ Extraction completed successfully.
```

---

## 🦯 Data Cleaning Rules

* All columns are normalised (lowercase, underscores)
* Duplicate/missing IDs ignored safely
* VAT bands pivoted into separate columns, e.g.:

  ```
  item_quantity_count_0, total_price_exc_vat_5, total_price_inc_vat_20
  ```

---

## 👨‍💻 Developer Notes

* Always import shared libraries from `P00_set_packages.py`
* SQL files must remain ASCII to avoid encoding issues on Windows
* `M00_run_gui.py` is now the **official entry point**
* When packaging as EXE, point to `M00_run_gui.py`
* Column order is defined in `P04_static_lists.py`

---

## 📚 License & Credits

Developed for internal automation within GoPuff UK Finance.
Maintained by the Finance Automation team (Contact: `gerry.pidgeon@gopuff.com`).

```
Copyright (c) 2025
GoPuff UK Finance Automation
All rights reserved.
```

---

```
README version: 2025-11-05 (with GUI launcher)
```
