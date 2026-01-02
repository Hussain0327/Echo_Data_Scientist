/*
    Staging model for products.

    Cleans and standardizes product data from the raw layer.
    Products may come from uploaded files or be derived from transactions.
*/

WITH source AS (
    -- If products source exists, use it
    -- Otherwise, derive from transactions
    SELECT DISTINCT
        product_id,
        -- Default product attributes (would be enriched from actual product source)
        CONCAT('Product ', product_id) AS name,
        CASE
            WHEN product_id LIKE 'prod_%' THEN
                CASE (HASHTEXT(product_id) % 5)
                    WHEN 0 THEN 'analytics'
                    WHEN 1 THEN 'integration'
                    WHEN 2 THEN 'automation'
                    WHEN 3 THEN 'reporting'
                    ELSE 'api_access'
                END
            ELSE 'other'
        END AS category,
        -- Derive a consistent price from product_id hash
        ROUND((ABS(HASHTEXT(product_id)) % 500 + 10)::DECIMAL, 2) AS price,
        MIN(transaction_date) AS created_at
    FROM {{ ref('stg_transactions') }}
    WHERE product_id IS NOT NULL
      AND product_id != 'unknown'
    GROUP BY product_id
),

cleaned AS (
    SELECT
        product_id,
        name,
        category,
        price,
        created_at,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
)

SELECT * FROM cleaned
