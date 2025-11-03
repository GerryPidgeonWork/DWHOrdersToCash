-- ===============================
-- Step 1 - EU Order Items Data
-- ===============================
WITH eu_order_items_data AS (
    SELECT
        eoi.id AS gp_product_id
        , eoi.product_vat_rate AS product_vat_rate
        , eoi.order_id AS gp_order_id
        , eoi.unit_price_pre_promo_local_inc_vat AS unit_price_exc_vat
        , eoi.unit_price_pre_promo_local_exc_vat AS unit_price_inc_vat
        , eoi.line_item_revenue_post_promo_local_inc_vat AS total_price_inc_vat
        , eoi.line_item_revenue_post_promo_local_exc_vat AS total_price_exc_vat
        , COALESCE(
            eoi.LINE_ITEM_REVENUE_POST_PROMO_LOCAL_INC_VAT / NULLIF(eoi.UNIT_PRICE_POST_PROMO_LOCAL_INC_VAT, 0),
            eoi.LINE_ITEM_REVENUE_PRE_PROMO_LOCAL_INC_VAT / NULLIF(eoi.UNIT_PRICE_PRE_PROMO_LOCAL_INC_VAT, 0)
        ) AS item_quantity

    FROM
        core.eu_order_items AS eoi

    WHERE
        eoi.order_id IN ({{order_id_list}})
)

-- ===============================
-- Step 2 - Combine Financial Data from EU Order Items
-- ===============================
, eu_order_items_combined AS (
    SELECT
        eoid.gp_order_id
        , CASE
            WHEN eoid.product_vat_rate = 0 THEN '0% VAT Band'
            WHEN eoid.product_vat_rate = 0.05 THEN '5% VAT Band'
            WHEN eoid.product_vat_rate = 0.2 THEN '20% VAT Band'
            ELSE 'Other / Unknown VAT Band'
        END AS vat_band
        , SUM(eoid.item_quantity) AS item_quantity_count
        , SUM(eoid.total_price_inc_vat) AS total_price_inc_vat
        , SUM(eoid.total_price_exc_vat) AS total_price_exc_vat
    FROM
        eu_order_items_data AS eoid
    GROUP BY
        1, 2
)

SELECT * FROM eu_order_items_combined;
