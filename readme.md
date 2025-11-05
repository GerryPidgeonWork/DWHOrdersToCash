# GoPuff UK – Order-Level & Item-Level DWH Extraction

### Automated Snowflake SQL → Pandas Integration

---

## 📦 Overview

This module automates the extraction of **order-level** and **item-level** data from GoPuff’s Snowflake Data Warehouse, combining both into a single cleaned DataFrame and generating CSV exports by provider (Braintree, PayPal, Uber Eats, Deliveroo, Just Eat, Amazon).

It is the **foundation of the Orders-to-Cash reconciliation process** — powering downstream tools for statement reconciliation, accruals, and performance reporting.

---

## 🧠 Architecture Summary

The project follows a modular structure for clarity and re-use:

```
OrderDWHData/
│
├── main/
│   └── M01_combine_sql.py           # Main orchestrator (runs all queries + saves outputs)
│
├── processes/
│   ├── P00_set_packages.py          # Centralised imports & 3rd-party install notes
│   ├── P01_set_file_paths.py        # Shared folder definitions for each provider
│   ├── P02_system_processes.py      # OS detection and user Downloads path helper
│   ├── P03_shared_functions.py      # Common functions (normalise columns, read_sql_clean)
│   ├── P04_static_lists.py          # Fixed reference lists (column ordering, mappings)
│   ├── P07_module_configs.py        # Manual config (reporting start/end dates)
│   ├── P08_snowflake_connector.py   # Handles Okta SSO connection + session context
│
└── sql/
    ├── S01_order_level.sql          # Retrieves order-level data
    └── S02_item_level.sql           # Retrieves item-level aggregates per VAT band
```

Each `main/` script calls into the shared `processes/` modules, maintaining **DRY** (Don’t Repeat Yourself) principles and standardised imports.

---

## ⚙️ Key Components

### 1️⃣ `M01_combine_sql.py`

The main orchestrator that:

* Connects to Snowflake via Okta SSO
* Executes `S01_order_level.sql` to fetch order-level data
* Executes `S02_item_level.sql` using the `gp_order_id` list from above
* Merges both DataFrames via `transform_item_data()`
* Exports cleaned CSVs per provider (Just Eat, Deliveroo, etc.)

### 2️⃣ `P00_set_packages.py`

Centralised import manager.
Every other module imports **only from this file**, ensuring consistency and a single install reference.
Each package has an install command and usage comment for new users.

### 3️⃣ `P08_snowflake_connector.py`

Handles:

* Okta SSO login (`externalbrowser` authentication)
* Time-limited connection attempt (20s)
* Automatic credential detection (email, warehouse, role)
* Context setting (`ANALYTICS / DBT_PROD / CORE`)

### 4️⃣ `S01_order_level.sql`

Retrieves order metadata, Braintree transactions, marketplace IDs, and financial metrics.

### 5️⃣ `S02_item_level.sql`

Retrieves aggregated item data grouped by VAT band (0%, 5%, 20%, Other).

---

## 🧩 Workflow Summary

```
M01_combine_sql.py
    ↓
connect_to_snowflake()        → Okta SSO login
set_snowflake_context()       → USE WAREHOUSE / DATABASE / SCHEMA
get_reporting_period()        → From P07_module_configs.py
run_order_level_query()       → Executes S01_order_level.sql
run_item_level_query()        → Executes S02_item_level.sql (temp table of order_ids)
transform_item_data()         → Pivot VAT bands + merge with df_orders
save_to_csv()                 → Writes provider-level output files
```

---

## 📁 Output Structure

Exports are saved into pre-defined subfolders on the shared drive:

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

Each file contains all relevant rows for that vendor within the configured reporting period.

---

## 🗓️ Configuration

Reporting period is controlled manually via:

```python
# processes/P07_module_configs.py
REPORTING_START_DATE = "2025-11-01"
REPORTING_END_DATE   = "2025-11-30"
```

To change the date range, simply update these two lines before running.

---

## 🚀 Usage

### Prerequisites

* Python 3.12+
* Access to GoPuff Snowflake via Okta SSO
* Packages installed (see `P00_set_packages.py`)

### Run the full extraction

```bash
python main/M01_combine_sql.py
```

### Example output

```
📧 Using default email address: gerry.pidgeon@gopuff.com
✅ Connected successfully as gerry.pidgeon@gopuff.com
✅ Order-level query complete in 10.9s — 63,098 rows.
✅ Item-level query complete in 2.2s — 97,605 rows.
✅ Combined order + item data: 63,098 rows, 53 columns.
💾 Saved 15,203 rows for Just Eat → H:\Shared drives\...\05 Just Eat\03 DWH\25.11 - Just Eat DWH data.csv
```

---

## 🪜 Data Cleaning Rules

* All column names are normalised via `normalize_columns()`:

  * Lowercase
  * Spaces and hyphens → underscores
* Duplicate or missing IDs are safely ignored.
* VAT bands are pivoted into separate columns:

  ```
  item_quantity_count_0, item_quantity_count_5, item_quantity_count_20, etc.
  ```

---

## 🧑‍💻 Developer Notes

* All imports must go through `P00_set_packages.py` (strict convention).
* SQL files must remain ASCII-only to avoid encoding issues in Windows.
* Future extensions (e.g. `S03_vendor_rebates.sql`) should follow the same sectioned format.
* Column order consistency is defined in `P04_static_lists.py`.
* When compiling to EXE for non-technical users, set `cwd` to project root.

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
README version: 2025-11-05
```