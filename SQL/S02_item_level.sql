-- ==========================================================================================
-- S02_item_level.sql
-- ------------------------------------------------------------------------------------------
-- Purpose:
--   Extracts item-level financial details for each gp_order_id returned by S01_order_level.sql.
--   Each row in the output represents one VAT band (0%, 5%, 20%, or Other) per order.
--
-- Inputs:
--   - core.eu_order_items
--   - {{order_id_list}} placeholder (replaced dynamically by Python with a SELECT query)
--
-- Integration:
--   - Executed by the Python function run_item_level_query() in M01_run_order_level.py
--   - Results are pivoted by transform_item_data() to merge item-level data into df_orders
--
-- Output Columns:
--   - gp_order_id              : Unique order ID from core.orders
--   - vat_band                 : VAT category (0%, 5%, 20%, or Other)
--   - item_quantity_count      : Total number of units in this VAT band
--   - total_price_inc_vat      : Total revenue including VAT
--   - total_price_exc_vat      : Total revenue excluding VAT
--
-- Notes:
--   - The {{order_id_list}} placeholder is replaced at runtime with:
--       SELECT gp_order_id FROM temp_order_ids
--     This is created by the Python script for efficient batch queries.
--   - No date filtering occurs here; the order filtering is handled upstream in S01_order_level.sql.
--   - The query is intentionally minimal for performance and clarity.
-- ==========================================================================================


-- ==============================================
-- Step 1 - EU Order Items Extraction
-- ----------------------------------------------
-- Filters item-level data from core.eu_order_items for all gp_order_id values
-- returned by S01_order_level.sql. Calculates item quantity based on promo pricing.
-- ==============================================
WITH eu_order_items_data AS (
    SELECT
        eoi.id AS gp_product_id,
        eoi.product_vat_rate AS product_vat_rate,
        eoi.order_id AS gp_order_id,

        -- Unit prices (kept for audit and validation)
        eoi.unit_price_pre_promo_local_inc_vat AS unit_price_exc_vat,
        eoi.unit_price_pre_promo_local_exc_vat AS unit_price_inc_vat,

        -- Line-level totals
        eoi.line_item_revenue_post_promo_local_inc_vat AS total_price_inc_vat,
        eoi.line_item_revenue_post_promo_local_exc_vat AS total_price_exc_vat,

        -- Derived quantity (safe division)
        COALESCE(
            eoi.line_item_revenue_post_promo_local_inc_vat / NULLIF(eoi.unit_price_post_promo_local_inc_vat, 0),
            eoi.line_item_revenue_pre_promo_local_inc_vat / NULLIF(eoi.unit_price_pre_promo_local_inc_vat, 0)
        ) AS item_quantity
    FROM
        core.eu_order_items AS eoi
    WHERE
        eoi.order_id IN ({{order_id_list}})
)


-- ==============================================
-- Step 2 - Aggregation by VAT Band
-- ----------------------------------------------
-- Aggregates item-level totals by VAT band for each gp_order_id.
-- These per-band totals are pivoted horizontally in Python for reporting.
-- ==============================================
, eu_order_items_combined AS (
    SELECT
        eoid.gp_order_id,
        CASE
            WHEN eoid.product_vat_rate = 0 THEN '0% VAT Band'
            WHEN eoid.product_vat_rate = 0.05 THEN '5% VAT Band'
            WHEN eoid.product_vat_rate = 0.2 THEN '20% VAT Band'
            ELSE 'Other / Unknown VAT Band'
        END AS vat_band,
        SUM(eoid.item_quantity) AS item_quantity_count,
        SUM(eoid.total_price_inc_vat) AS total_price_inc_vat,
        SUM(eoid.total_price_exc_vat) AS total_price_exc_vat
    FROM
        eu_order_items_data AS eoid
    GROUP BY
        1, 2
)


-- ==============================================
-- Step 3 - Final Output
-- ----------------------------------------------
-- Returns grouped item-level aggregates per VAT band and order.
-- This data is merged into the order-level dataset in Python.
-- ==============================================
SELECT
    *
FROM
    eu_order_items_combined;
