{{
    config(
        materialized='table',
        tags=['dimension', 'static']
    )
}}

/*
    Date Dimension

    Standard date dimension for time-based analysis. This is a static
    dimension (not SCD) that provides calendar attributes for any date
    in the analysis range.

    Features:
    - Calendar attributes (year, quarter, month, week, day)
    - Fiscal year support (configurable fiscal year start)
    - Weekend/weekday flags
    - Holiday support (placeholder for custom holidays)
    - Relative date flags (is_current_month, is_ytd, etc.)

    Coverage: 2020-01-01 to 2030-12-31
*/

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

date_details AS (
    SELECT
        date_day AS date_key,

        -- Basic date parts
        EXTRACT(YEAR FROM date_day)::INTEGER AS year,
        EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter,
        EXTRACT(MONTH FROM date_day)::INTEGER AS month,
        EXTRACT(WEEK FROM date_day)::INTEGER AS week_of_year,
        EXTRACT(DOY FROM date_day)::INTEGER AS day_of_year,
        EXTRACT(DAY FROM date_day)::INTEGER AS day_of_month,
        EXTRACT(DOW FROM date_day)::INTEGER AS day_of_week,

        -- Formatted strings
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'YYYY-Q') || EXTRACT(QUARTER FROM date_day)::TEXT AS year_quarter,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_name_short,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_name_short,

        -- Week boundaries
        DATE_TRUNC('week', date_day)::DATE AS week_start_date,
        (DATE_TRUNC('week', date_day) + INTERVAL '6 days')::DATE AS week_end_date,

        -- Month boundaries
        DATE_TRUNC('month', date_day)::DATE AS month_start_date,
        (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end_date,

        -- Quarter boundaries
        DATE_TRUNC('quarter', date_day)::DATE AS quarter_start_date,
        (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months' - INTERVAL '1 day')::DATE AS quarter_end_date,

        -- Year boundaries
        DATE_TRUNC('year', date_day)::DATE AS year_start_date,
        (DATE_TRUNC('year', date_day) + INTERVAL '1 year' - INTERVAL '1 day')::DATE AS year_end_date,

        -- Fiscal year (assuming July 1 start - configurable via var)
        CASE
            WHEN EXTRACT(MONTH FROM date_day) >= 7
            THEN EXTRACT(YEAR FROM date_day)::INTEGER + 1
            ELSE EXTRACT(YEAR FROM date_day)::INTEGER
        END AS fiscal_year,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) >= 7
            THEN EXTRACT(MONTH FROM date_day)::INTEGER - 6
            ELSE EXTRACT(MONTH FROM date_day)::INTEGER + 6
        END AS fiscal_month,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 7 AND 9 THEN 1
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 10 AND 12 THEN 2
            WHEN EXTRACT(MONTH FROM date_day) BETWEEN 1 AND 3 THEN 3
            ELSE 4
        END AS fiscal_quarter,

        -- Flags
        CASE
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        CASE
            WHEN EXTRACT(DOW FROM date_day) BETWEEN 1 AND 5 THEN TRUE
            ELSE FALSE
        END AS is_weekday,
        CASE
            WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE
            ELSE FALSE
        END AS is_month_start,
        CASE
            WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
            THEN TRUE
            ELSE FALSE
        END AS is_month_end,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1
            THEN TRUE
            ELSE FALSE
        END AS is_year_start,
        CASE
            WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 31
            THEN TRUE
            ELSE FALSE
        END AS is_year_end,

        -- Relative to current date (useful for filtering)
        CASE
            WHEN date_day = CURRENT_DATE THEN TRUE
            ELSE FALSE
        END AS is_today,
        CASE
            WHEN DATE_TRUNC('month', date_day) = DATE_TRUNC('month', CURRENT_DATE)
            THEN TRUE
            ELSE FALSE
        END AS is_current_month,
        CASE
            WHEN DATE_TRUNC('quarter', date_day) = DATE_TRUNC('quarter', CURRENT_DATE)
            THEN TRUE
            ELSE FALSE
        END AS is_current_quarter,
        CASE
            WHEN EXTRACT(YEAR FROM date_day) = EXTRACT(YEAR FROM CURRENT_DATE)
            THEN TRUE
            ELSE FALSE
        END AS is_current_year,
        CASE
            WHEN date_day >= DATE_TRUNC('year', CURRENT_DATE)
              AND date_day <= CURRENT_DATE
            THEN TRUE
            ELSE FALSE
        END AS is_ytd,

        -- Days relative to today
        (CURRENT_DATE - date_day)::INTEGER AS days_ago,

        -- Holiday placeholder (would be populated from holiday calendar)
        FALSE AS is_holiday,
        NULL::TEXT AS holiday_name

    FROM date_spine
)

SELECT
    date_key,
    year,
    quarter,
    month,
    week_of_year,
    day_of_year,
    day_of_month,
    day_of_week,
    year_month,
    year_quarter,
    day_name,
    day_name_short,
    month_name,
    month_name_short,
    week_start_date,
    week_end_date,
    month_start_date,
    month_end_date,
    quarter_start_date,
    quarter_end_date,
    year_start_date,
    year_end_date,
    fiscal_year,
    fiscal_month,
    fiscal_quarter,
    is_weekend,
    is_weekday,
    is_month_start,
    is_month_end,
    is_year_start,
    is_year_end,
    is_today,
    is_current_month,
    is_current_quarter,
    is_current_year,
    is_ytd,
    days_ago,
    is_holiday,
    holiday_name
FROM date_details
