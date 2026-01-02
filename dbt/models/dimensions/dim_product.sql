{{
    config(
        materialized='incremental',
        unique_key='product_sk',
        on_schema_change='sync_all_columns',
        tags=['scd2', 'dimension']
    )
}}

/*
    Product Dimension with SCD Type 2 History Tracking

    Tracks historical changes to product attributes, particularly
    price changes which are critical for accurate revenue analysis.

    Tracked attributes (trigger SCD2 versioning):
    - price (product pricing changes)
    - category (product reclassification)
    - name (product renaming)

    Static attributes:
    - product_id (natural key)
    - created_at

    Use Cases:
    - Historical revenue analysis at point-in-time prices
    - Price elasticity analysis
    - Product category migration tracking
*/

WITH source AS (
    SELECT
        product_id,
        name,
        category,
        price,
        created_at
    FROM {{ ref('stg_products') }}
),

{% if is_incremental() %}

current_records AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

change_detection AS (
    SELECT
        s.product_id,
        s.name,
        s.category,
        s.price,
        s.created_at,
        c.product_sk AS existing_sk,
        c.price AS existing_price,
        c.category AS existing_category,
        c.name AS existing_name,
        CASE
            WHEN c.product_sk IS NULL THEN 'INSERT'
            WHEN s.price != c.price
              OR s.category != c.category
              OR s.name != c.name THEN 'SCD2_UPDATE'
            ELSE 'NO_CHANGE'
        END AS change_type,
        -- Calculate price change percentage for analytics
        CASE
            WHEN c.price IS NOT NULL AND c.price > 0
            THEN ROUND(((s.price - c.price) / c.price) * 100, 2)
            ELSE NULL
        END AS price_change_pct
    FROM source s
    LEFT JOIN current_records c
        ON s.product_id = c.product_id
),

expired_records AS (
    SELECT
        c.product_sk,
        c.product_id,
        c.name,
        c.category,
        c.price,
        c.created_at,
        c.valid_from,
        CURRENT_TIMESTAMP AS valid_to,
        FALSE AS is_current,
        c.price_change_pct
    FROM current_records c
    INNER JOIN change_detection cd
        ON c.product_id = cd.product_id
    WHERE cd.change_type = 'SCD2_UPDATE'
),

new_records AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id', 'price', 'category']) }} AS product_sk,
        product_id,
        name,
        category,
        price,
        created_at,
        CURRENT_TIMESTAMP AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,
        price_change_pct
    FROM change_detection
    WHERE change_type IN ('INSERT', 'SCD2_UPDATE')
),

final AS (
    SELECT * FROM expired_records
    UNION ALL
    SELECT * FROM new_records
)

SELECT * FROM final

{% else %}

initial_load AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id', 'price', 'category']) }} AS product_sk,
        product_id,
        name,
        category,
        price,
        created_at,
        created_at AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,
        CAST(NULL AS DECIMAL(10,2)) AS price_change_pct
    FROM source
)

SELECT * FROM initial_load

{% endif %}
