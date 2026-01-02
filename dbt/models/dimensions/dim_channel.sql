{{
    config(
        materialized='incremental',
        unique_key='channel_sk',
        on_schema_change='sync_all_columns',
        tags=['scd2', 'dimension']
    )
}}

/*
    Marketing Channel Dimension with SCD Type 2 History Tracking

    Tracks changes to marketing channel classifications and attributes.
    Important for accurate attribution analysis when channels are
    reclassified (e.g., "sem" renamed to "paid_search").

    Tracked attributes (trigger SCD2 versioning):
    - channel_name (channel renaming/reclassification)
    - channel_type (paid/organic classification)
    - is_paid (monetization status change)

    Static attributes:
    - channel_id (natural key)

    Use Cases:
    - Historical marketing attribution
    - Channel performance trending
    - ROI analysis at point-in-time classifications
*/

WITH source_channels AS (
    -- Extract unique channels from marketing events
    SELECT DISTINCT
        channel AS channel_name,
        -- Derive channel attributes
        CASE
            WHEN channel IN ('paid_search', 'social') THEN 'paid'
            WHEN channel IN ('organic', 'referral', 'direct') THEN 'organic'
            WHEN channel = 'email' THEN 'owned'
            ELSE 'other'
        END AS channel_type,
        CASE
            WHEN channel IN ('paid_search', 'social') THEN TRUE
            ELSE FALSE
        END AS is_paid,
        MIN(event_date) AS first_seen_at
    FROM {{ ref('stg_marketing_events') }}
    GROUP BY channel
),

source AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['channel_name']) }} AS channel_id,
        channel_name,
        channel_type,
        is_paid,
        first_seen_at
    FROM source_channels
),

{% if is_incremental() %}

current_records AS (
    SELECT * FROM {{ this }}
    WHERE is_current = TRUE
),

change_detection AS (
    SELECT
        s.channel_id,
        s.channel_name,
        s.channel_type,
        s.is_paid,
        s.first_seen_at,
        c.channel_sk AS existing_sk,
        c.channel_type AS existing_channel_type,
        c.is_paid AS existing_is_paid,
        CASE
            WHEN c.channel_sk IS NULL THEN 'INSERT'
            WHEN s.channel_type != c.channel_type
              OR s.is_paid != c.is_paid THEN 'SCD2_UPDATE'
            ELSE 'NO_CHANGE'
        END AS change_type
    FROM source s
    LEFT JOIN current_records c
        ON s.channel_id = c.channel_id
),

expired_records AS (
    SELECT
        c.channel_sk,
        c.channel_id,
        c.channel_name,
        c.channel_type,
        c.is_paid,
        c.first_seen_at,
        c.valid_from,
        CURRENT_TIMESTAMP AS valid_to,
        FALSE AS is_current
    FROM current_records c
    INNER JOIN change_detection cd
        ON c.channel_id = cd.channel_id
    WHERE cd.change_type = 'SCD2_UPDATE'
),

new_records AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['channel_id', 'channel_type', 'is_paid']) }} AS channel_sk,
        channel_id,
        channel_name,
        channel_type,
        is_paid,
        first_seen_at,
        CURRENT_TIMESTAMP AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
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
        {{ dbt_utils.generate_surrogate_key(['channel_id', 'channel_type', 'is_paid']) }} AS channel_sk,
        channel_id,
        channel_name,
        channel_type,
        is_paid,
        first_seen_at,
        first_seen_at AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current
    FROM source
)

SELECT * FROM initial_load

{% endif %}
