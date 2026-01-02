{{
    config(
        materialized='incremental',
        unique_key='customer_sk',
        on_schema_change='sync_all_columns',
        tags=['scd2', 'dimension']
    )
}}

/*
    Customer Dimension with SCD Type 2 History Tracking

    This model implements Slowly Changing Dimension Type 2 to track
    historical changes to customer attributes over time.

    Tracked attributes (trigger SCD2 versioning):
    - segment (starter, growth, professional, enterprise)
    - plan_type (free, basic, standard, premium, enterprise)
    - name (customer name changes)

    Static attributes (don't trigger new version):
    - customer_id (natural key)
    - email
    - acquisition_channel
    - created_at

    Columns:
    - customer_sk: Surrogate key (unique per version)
    - customer_id: Natural/business key
    - valid_from: When this version became active
    - valid_to: When this version was superseded (NULL if current)
    - is_current: Flag for current active record
*/

WITH source AS (
    SELECT
        customer_id,
        email,
        name,
        segment,
        plan_type,
        acquisition_channel,
        created_at,
        updated_at
    FROM {{ ref('stg_customers') }}
),

{% if is_incremental() %}

-- Get all current active records from existing dimension
current_records AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

-- Detect what type of change occurred for each incoming record
change_detection AS (
    SELECT
        s.customer_id,
        s.email,
        s.name,
        s.segment,
        s.plan_type,
        s.acquisition_channel,
        s.created_at,
        s.updated_at,
        c.customer_sk AS existing_sk,
        c.segment AS existing_segment,
        c.plan_type AS existing_plan_type,
        c.name AS existing_name,
        c.valid_from AS existing_valid_from,
        CASE
            -- New customer (not in dimension yet)
            WHEN c.customer_sk IS NULL THEN 'INSERT'
            -- SCD2 change: segment, plan_type, or name changed
            WHEN s.segment != c.segment
              OR s.plan_type != c.plan_type
              OR s.name != c.name THEN 'SCD2_UPDATE'
            -- No meaningful change
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM source s
    LEFT JOIN current_records c
        ON s.customer_id = c.customer_id
),

-- Records to expire (close out old versions)
expired_records AS (
    SELECT
        c.customer_sk,
        c.customer_id,
        c.email,
        c.name,
        c.segment,
        c.plan_type,
        c.acquisition_channel,
        c.created_at,
        c.valid_from,
        CURRENT_TIMESTAMP AS valid_to,
        FALSE AS is_current
    FROM current_records c
    INNER JOIN change_detection cd
        ON c.customer_id = cd.customer_id
    WHERE cd.change_type = 'SCD2_UPDATE'
),

-- New records (both new customers and new versions from SCD2 updates)
new_records AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'updated_at']) }} AS customer_sk,
        customer_id,
        email,
        name,
        segment,
        plan_type,
        acquisition_channel,
        created_at,
        CURRENT_TIMESTAMP AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
    FROM change_detection
    WHERE change_type IN ('INSERT', 'SCD2_UPDATE')
),

-- Combine expired and new records
final AS (
    SELECT
        customer_sk,
        customer_id,
        email,
        name,
        segment,
        plan_type,
        acquisition_channel,
        created_at,
        valid_from,
        valid_to,
        is_current
    FROM expired_records

    UNION ALL

    SELECT
        customer_sk,
        customer_id,
        email,
        name,
        segment,
        plan_type,
        acquisition_channel,
        created_at,
        valid_from,
        valid_to,
        is_current
    FROM new_records
)

SELECT * FROM final

{% else %}

-- Initial full load: all records are current
initial_load AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'created_at']) }} AS customer_sk,
        customer_id,
        email,
        name,
        segment,
        plan_type,
        acquisition_channel,
        created_at,
        created_at AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
    FROM source
)

SELECT * FROM initial_load

{% endif %}
