{{
  config(
    materialized='table',
    tags=['dimension', 'date']
  )
}}

/*
=====================================================================
DIMENSION TABLE: Date
=====================================================================
Purpose:
  Date dimension with calendar attributes for filtering and grouping.
  Pre-compute date attributes to avoid repeated DATE functions.

Features:
  - Surrogate key (YYYYMMDD as integer) for faster joins
  - All date attributes (year, quarter, month, week, day)
  - Boolean flags (weekend, holiday, trading day)
  - Brazilian holidays

Grain:
  One row per calendar date (2015-2030)

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH date_spine AS (
    -- Generate dates from 2015 to 2030
    SELECT generate_series(
        '2015-01-01'::date,
        '2030-12-31'::date,
        '1 day'::interval
    )::date AS date_day
),

date_attributes AS (
    SELECT
        -- Surrogate key (YYYYMMDD format)
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_id,

        date_day AS date,

        -- Year attributes
        EXTRACT(YEAR FROM date_day)::INTEGER AS year,
        EXTRACT(ISOYEAR FROM date_day)::INTEGER AS iso_year,

        -- Quarter attributes
        EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter,
        'Q' || EXTRACT(QUARTER FROM date_day)::TEXT AS quarter_name,
        DATE_TRUNC('quarter', date_day)::DATE AS quarter_start_date,

        -- Month attributes
        EXTRACT(MONTH FROM date_day)::INTEGER AS month,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_name_short,
        DATE_TRUNC('month', date_day)::DATE AS month_start_date,

        -- Week attributes
        EXTRACT(WEEK FROM date_day)::INTEGER AS week_of_year,
        EXTRACT(DOW FROM date_day)::INTEGER AS day_of_week,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_name_short,

        -- Day attributes
        EXTRACT(DAY FROM date_day)::INTEGER AS day_of_month,
        EXTRACT(DOY FROM date_day)::INTEGER AS day_of_year,

        -- Boolean flags
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
        CASE WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
             THEN TRUE ELSE FALSE END AS is_month_end

    FROM date_spine
),

-- Add Brazilian holidays and trading day flag
with_holidays AS (
    SELECT
        d.*,

        -- Brazilian fixed holidays
        CASE
            WHEN d.month = 1 AND d.day_of_month = 1 THEN 'New Year'
            WHEN d.month = 4 AND d.day_of_month = 21 THEN 'Tiradentes Day'
            WHEN d.month = 5 AND d.day_of_month = 1 THEN 'Labor Day'
            WHEN d.month = 9 AND d.day_of_month = 7 THEN 'Independence Day'
            WHEN d.month = 10 AND d.day_of_month = 12 THEN 'Our Lady of Aparecida'
            WHEN d.month = 11 AND d.day_of_month = 2 THEN 'All Souls Day'
            WHEN d.month = 11 AND d.day_of_month = 15 THEN 'Proclamation of the Republic'
            WHEN d.month = 11 AND d.day_of_month = 20 THEN 'Black Consciousness Day'
            WHEN d.month = 12 AND d.day_of_month = 25 THEN 'Christmas'
            ELSE NULL
        END AS holiday_name,

        -- Is this a fixed holiday?
        CASE
            WHEN (d.month = 1 AND d.day_of_month = 1)
                OR (d.month = 4 AND d.day_of_month = 21)
                OR (d.month = 5 AND d.day_of_month = 1)
                OR (d.month = 9 AND d.day_of_month = 7)
                OR (d.month = 10 AND d.day_of_month = 12)
                OR (d.month = 11 AND d.day_of_month = 2)
                OR (d.month = 11 AND d.day_of_month = 15)
                OR (d.month = 11 AND d.day_of_month = 20)
                OR (d.month = 12 AND d.day_of_month = 25)
            THEN TRUE
            ELSE FALSE
        END AS is_holiday,

        -- Is this a trading day? (not weekend and not fixed holiday)
        CASE
            WHEN d.is_weekend THEN FALSE
            WHEN (d.month = 1 AND d.day_of_month = 1)
                OR (d.month = 4 AND d.day_of_month = 21)
                OR (d.month = 5 AND d.day_of_month = 1)
                OR (d.month = 9 AND d.day_of_month = 7)
                OR (d.month = 10 AND d.day_of_month = 12)
                OR (d.month = 11 AND d.day_of_month = 2)
                OR (d.month = 11 AND d.day_of_month = 15)
                OR (d.month = 11 AND d.day_of_month = 20)
                OR (d.month = 12 AND d.day_of_month = 25)
            THEN FALSE
            ELSE TRUE
        END AS is_trading_day

    FROM date_attributes d
)

SELECT * FROM with_holidays
