-- ===============================
-- Reporting Period Configuration
-- ===============================

WITH reporting_period AS (
    SELECT
        '{{start_date}}'::DATE AS reporting_start_date_day,
        '{{end_date}}'::DATE AS reporting_end_date_day
)

-- ===============================
-- Base Query (from Orders Table)
-- ===============================

, order_list AS (
    SELECT
        o.id AS gp_order_id
        , o.id_obfuscated AS gp_order_id_obfuscated
        , o.braintree_transaction_id_array AS braintree_tx_id_array
        , o.location_name
        , o.order_vendor
        , CASE WHEN LOWER(o.order_vendor) = 'gopuff' THEN 'DTC' ELSE 'MP' END AS vendor_group
        , o.order_completed
        , o.payment_system
        , DATEDIFF('HOUR', o.created_at, o.created_at_local) AS utc_to_local_hour_diff
        , o.created_at_local AS created_at_timestamp
        , o.delivered_at_local AS delivered_at_timestamp
        , DATE_TRUNC('DAY', o.created_at_local)::DATE AS created_at_day
        , DATE_TRUNC('WEEK', o.created_at_local)::DATE AS created_at_week
        , DATE_TRUNC('MONTH', o.created_at_local)::DATE AS created_at_month
        , DATE_TRUNC('DAY',   o.delivered_at_local)::DATE AS delivered_at_day
        , DATE_TRUNC('WEEK',  o.delivered_at_local)::DATE AS delivered_at_week
        , DATE_TRUNC('MONTH', o.delivered_at_local)::DATE AS delivered_at_month
        , DATE_TRUNC('DAY',   o.ops_date)::DATE AS ops_date_day
        , DATE_TRUNC('WEEK',  o.ops_date)::DATE AS ops_date_week
        , DATE_TRUNC('MONTH', o.ops_date)::DATE AS ops_date_month

        , DATE_TRUNC('DAY',
        LEAST(  COALESCE(DATE_TRUNC('DAY', o.created_at_local), '2999-12-31'::DATE),
                COALESCE(DATE_TRUNC('DAY', o.delivered_at_local), '2999-12-31'::DATE),
                COALESCE(o.ops_date, '2999-12-31'::DATE))
        )::DATE AS earliest_order_date

        , DATE_TRUNC('DAY',
        GREATEST(   COALESCE(DATE_TRUNC('DAY', o.created_at_local), '1900-01-01'::DATE),
                    COALESCE(DATE_TRUNC('DAY', o.delivered_at_local), '1900-01-01'::DATE),
                    COALESCE(o.ops_date, '1900-01-01'::DATE))
        )::DATE AS latest_order_date

    FROM
        core.orders AS o
        CROSS JOIN reporting_period AS rp

    WHERE
        o.country_code = 'GB'
        AND earliest_order_date >= reporting_start_date_day
        AND latest_order_date <= reporting_end_date_day
)

-- ===============================
-- Braintree Tx ID List
-- ===============================

, braintree_tx_data AS (
    SELECT
        ol.gp_order_id
        , ol.gp_order_id_obfuscated
        , ol.payment_system
        , tx.index + 1 AS braintree_tx_index
        , tx.value::string AS braintree_tx_id
        , ol.location_name
        , ol.order_vendor
        , ol.vendor_group
        , ol.order_completed
        , ol.created_at_timestamp
        , ol.delivered_at_timestamp
        , ol.created_at_day
        , ol.created_at_week
        , ol.created_at_month
        , ol.delivered_at_day
        , ol.delivered_at_week
        , ol.delivered_at_month
        , ol.ops_date_day
        , ol.ops_date_week
        , ol.ops_date_month

    FROM
        order_list AS ol,
        LATERAL FLATTEN(input => TRY_PARSE_JSON(TO_JSON(ol.braintree_tx_id_array)), OUTER => TRUE) AS tx
)

-- ===============================
-- Marketplace Order ID Details
-- ===============================

, mp_order_details AS (
    SELECT
        btd.gp_order_id
        , btd.gp_order_id_obfuscated
        , CASE  WHEN btd.vendor_group = 'MP' THEN bpo.partner_customer_order_number
                ELSE NULL END AS mp_order_id
        , btd.payment_system
        , btd.braintree_tx_index
        , btd.braintree_tx_id
        , btd.location_name
        , btd.order_vendor
        , btd.vendor_group
        , btd.order_completed
        , btd.created_at_timestamp
        , btd.delivered_at_timestamp
        , btd.created_at_day
        , btd.created_at_week
        , btd.created_at_month
        , btd.delivered_at_day
        , btd.delivered_at_week
        , btd.delivered_at_month
        , btd.ops_date_day
        , btd.ops_date_week
        , btd.ops_date_month

    FROM
        braintree_tx_data AS btd
        LEFT JOIN core.bse_partner_order AS bpo ON btd.gp_order_id_obfuscated = bpo.id_obfuscated
)

-- ===============================
-- Financial Data from UK PL Orders
-- ===============================

, uk_pl_orders_data AS (
    SELECT
        mod.*

        -- Invoice Element including VAT
        , upo.post_promo_sales_exc_vat * (1 + blended_vat_rate) AS post_promo_sales_inc_vat
        , upo.delivery_fee_exc_vat * (1 + blended_vat_rate) AS delivery_fee_inc_vat
        , upo.priority_fee_exc_vat * 1.2 AS priority_fee_inc_vat
        , upo.small_order_fee_exc_vat * 1.2 AS small_order_fee_inc_vat
        , upo.marketplace_bag_fees_exc_vat * 1.2 AS mp_bag_fee_inc_vat
        , (upo.post_promo_sales_exc_vat * (1 + blended_vat_rate))
        + (upo.delivery_fee_exc_vat * (1 + blended_vat_rate))
        + (upo.priority_fee_exc_vat * 1.2)
        + (upo.small_order_fee_exc_vat * 1.2)
        + (upo.marketplace_bag_fees_exc_vat * 1.2) AS total_payment_inc_vat
        , upo.tips_amount AS tips_amount
        , ((upo.post_promo_sales_exc_vat * (1 + blended_vat_rate))
        + (upo.delivery_fee_exc_vat * (1 + blended_vat_rate))
        + (upo.priority_fee_exc_vat * 1.2)
        + (upo.small_order_fee_exc_vat * 1.2)
        + (upo.marketplace_bag_fees_exc_vat * 1.2)
        + upo.tips_amount) AS total_payment_with_tips_inc_vat

        -- Invoice Element Excluding VAT
        , upo.post_promo_sales_exc_vat AS post_promo_sales_exc_vat
        , upo.delivery_fee_exc_vat AS delivery_fee_exc_vat
        , upo.priority_fee_exc_vat AS priority_fee_exc_vat
        , upo.small_order_fee_exc_vat AS small_order_fee_exc_vat
        , upo.marketplace_bag_fees_exc_vat AS mp_bag_fee_exc_vat
        , (upo.post_promo_sales_exc_vat
        + upo.delivery_fee_exc_vat
        + upo.priority_fee_exc_vat
        + upo.small_order_fee_exc_vat
        + upo.marketplace_bag_fees_exc_vat) AS total_revenue_exc_vat

        -- COGS
        , upo.cost_of_goods_exc_vat AS cost_of_goods_inc_vat
        , upo.cost_of_goods_exc_vat / (1 + blended_vat_rate) AS cost_of_goods_exc_vat

    FROM
        mp_order_details AS mod
        LEFT JOIN core.uk_pl_orders AS upo ON upo.id = mod.gp_order_id AND (mod.braintree_tx_index = 1 OR mod.braintree_tx_index IS NULL)
)

-- ===============================
-- Financial Data from EU Orders
-- ===============================

, eu_order_data AS (
    SELECT
        uod.*
        , eo.products_total_price_local AS alt_post_promo_sales_inc_vat
        , eo.delivery_fee_local AS alt_delivery_fee_exc_vat
        , eo.priority_fee_local AS alt_priority_fee_exc_vat
        , eo.small_order_fee_local AS alt_small_order_fee_exc_vat
        , eo.total_inc_tips_local AS alt_total_payment_with_tips_inc_vat

    FROM
        uk_pl_orders_data AS uod
        LEFT JOIN core.eu_orders AS eo ON eo.id = uod.gp_order_id AND (uod.braintree_tx_index = 1 OR uod.braintree_tx_index IS NULL)
)

-- ===============================
-- Execute SQL
-- ===============================
SELECT 
    * 
    
FROM 
    eu_order_data;