{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['fact', 'incremental']
    )
}}

/*
    Transaction Fact Table

    Central fact table for all revenue transactions. Joins to dimension
    surrogate keys for proper star schema analysis.

    Features:
    - Incremental loading (only new transactions)
    - Point-in-time dimension joins (uses valid_from/valid_to)
    - Pre-calculated metrics for common aggregations

    Grain: One row per transaction

    Measures:
    - amount: Transaction amount
    - is_successful: Whether transaction completed successfully
    - quantity: Derived quantity (amount / product price)

    Foreign Keys (surrogate):
    - customer_sk: Links to dim_customer at transaction time
    - product_sk: Links to dim_product at transaction time
    - date_key: Links to dim_date
*/

WITH transactions AS (
    SELECT
        transaction_id,
        transaction_date,
        amount,
        customer_id,
        product_id,
        status,
        _loaded_at
    FROM {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    -- Only process new transactions
    WHERE _loaded_at > (SELECT COALESCE(MAX(_loaded_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

-- Get customer dimension (point-in-time lookup)
customers AS (
    SELECT
        customer_sk,
        customer_id,
        segment AS customer_segment,
        plan_type AS customer_plan_type,
        valid_from,
        COALESCE(valid_to, '9999-12-31'::TIMESTAMP) AS valid_to
    FROM {{ ref('dim_customer') }}
),

-- Get product dimension (point-in-time lookup)
products AS (
    SELECT
        product_sk,
        product_id,
        category AS product_category,
        price AS product_price,
        valid_from,
        COALESCE(valid_to, '9999-12-31'::TIMESTAMP) AS valid_to
    FROM {{ ref('dim_product') }}
),

-- Get date dimension
dates AS (
    SELECT
        date_key,
        year,
        quarter,
        month,
        week_of_year,
        is_weekend,
        fiscal_year,
        fiscal_quarter
    FROM {{ ref('dim_date') }}
),

-- Join transactions to dimensions
enriched AS (
    SELECT
        t.transaction_id,
        t.transaction_date,

        -- Date dimension key
        d.date_key,

        -- Customer dimension (point-in-time)
        c.customer_sk,
        c.customer_segment,
        c.customer_plan_type,

        -- Product dimension (point-in-time)
        p.product_sk,
        p.product_category,
        p.product_price,

        -- Transaction measures
        t.amount,
        t.status,

        -- Derived measures
        CASE
            WHEN t.status IN ('completed', 'paid') THEN TRUE
            ELSE FALSE
        END AS is_successful,

        CASE
            WHEN t.status = 'refunded' THEN -t.amount
            WHEN t.status IN ('cancelled', 'failed') THEN 0
            ELSE t.amount
        END AS net_amount,

        -- Quantity estimate (transactions / product price)
        CASE
            WHEN p.product_price > 0
            THEN ROUND(t.amount / p.product_price, 0)::INTEGER
            ELSE 1
        END AS estimated_quantity,

        -- Time-based attributes from date dimension
        d.year,
        d.quarter,
        d.month,
        d.week_of_year,
        d.is_weekend,
        d.fiscal_year,
        d.fiscal_quarter,

        -- Metadata
        t._loaded_at,
        CURRENT_TIMESTAMP AS _processed_at

    FROM transactions t

    -- Join to date dimension
    LEFT JOIN dates d
        ON t.transaction_date = d.date_key

    -- Point-in-time join to customer dimension
    LEFT JOIN customers c
        ON t.customer_id = c.customer_id
        AND t.transaction_date >= c.valid_from::DATE
        AND t.transaction_date < c.valid_to::DATE

    -- Point-in-time join to product dimension
    LEFT JOIN products p
        ON t.product_id = p.product_id
        AND t.transaction_date >= p.valid_from::DATE
        AND t.transaction_date < p.valid_to::DATE
)

SELECT
    transaction_id,
    transaction_date,
    date_key,
    customer_sk,
    customer_segment,
    customer_plan_type,
    product_sk,
    product_category,
    product_price,
    amount,
    net_amount,
    status,
    is_successful,
    estimated_quantity,
    year,
    quarter,
    month,
    week_of_year,
    is_weekend,
    fiscal_year,
    fiscal_quarter,
    _loaded_at,
    _processed_at
FROM enriched
