{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['fact', 'incremental', 'marketing']
    )
}}

/*
    Marketing Events Fact Table

    Fact table for marketing funnel events including leads,
    conversions, and spend by channel and campaign.

    Features:
    - Incremental loading
    - Channel dimension join for historical attribution
    - Pre-calculated marketing metrics

    Grain: One row per marketing event (daily channel/campaign)

    Measures:
    - leads: Number of leads generated
    - conversions: Number of conversions
    - spend: Marketing spend amount
    - conversion_rate: Conversions / Leads
    - cost_per_lead: Spend / Leads
    - cost_per_conversion: Spend / Conversions
*/

WITH events AS (
    SELECT
        event_id,
        event_date,
        channel,
        campaign,
        leads,
        conversions,
        spend,
        _loaded_at
    FROM {{ ref('stg_marketing_events') }}

    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT COALESCE(MAX(_loaded_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

-- Get channel dimension
channels AS (
    SELECT
        channel_sk,
        channel_name,
        channel_type,
        is_paid,
        valid_from,
        COALESCE(valid_to, '9999-12-31'::TIMESTAMP) AS valid_to
    FROM {{ ref('dim_channel') }}
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

enriched AS (
    SELECT
        e.event_id,
        e.event_date,

        -- Date dimension
        d.date_key,

        -- Channel dimension (point-in-time)
        c.channel_sk,
        c.channel_name,
        c.channel_type,
        c.is_paid,

        -- Campaign (not dimensionalized, kept as degenerate dimension)
        e.campaign,

        -- Measures
        e.leads,
        e.conversions,
        e.spend,

        -- Calculated metrics
        CASE
            WHEN e.leads > 0
            THEN ROUND((e.conversions::DECIMAL / e.leads) * 100, 2)
            ELSE 0
        END AS conversion_rate,

        CASE
            WHEN e.leads > 0
            THEN ROUND(e.spend / e.leads, 2)
            ELSE 0
        END AS cost_per_lead,

        CASE
            WHEN e.conversions > 0
            THEN ROUND(e.spend / e.conversions, 2)
            ELSE 0
        END AS cost_per_conversion,

        -- ROAS proxy (assuming $50 average order value)
        CASE
            WHEN e.spend > 0
            THEN ROUND((e.conversions * 50) / e.spend, 2)
            ELSE 0
        END AS estimated_roas,

        -- Time attributes
        d.year,
        d.quarter,
        d.month,
        d.week_of_year,
        d.fiscal_year,
        d.fiscal_quarter,

        -- Metadata
        e._loaded_at,
        CURRENT_TIMESTAMP AS _processed_at

    FROM events e

    -- Join to date dimension
    LEFT JOIN dates d
        ON e.event_date = d.date_key

    -- Point-in-time join to channel dimension
    LEFT JOIN channels c
        ON e.channel = c.channel_name
        AND e.event_date >= c.valid_from::DATE
        AND e.event_date < c.valid_to::DATE
)

SELECT
    event_id,
    event_date,
    date_key,
    channel_sk,
    channel_name,
    channel_type,
    is_paid,
    campaign,
    leads,
    conversions,
    spend,
    conversion_rate,
    cost_per_lead,
    cost_per_conversion,
    estimated_roas,
    year,
    quarter,
    month,
    week_of_year,
    fiscal_year,
    fiscal_quarter,
    _loaded_at,
    _processed_at
FROM enriched
