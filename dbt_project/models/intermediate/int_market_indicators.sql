{{
  config(
    materialized='view',
    tags=['intermediate', 'indicators']
  )
}}

/*
=====================================================================
INTERMEDIATE MODEL: Market Indicators
=====================================================================
Purpose:
  Pivot economic indicators to columns for easier joining with stocks.
  Transform from multiple rows per date to one row per date with
  all indicators as columns.

Calculations:
  - Pivot indicator codes to named columns
  - Calculate real interest rate (SELIC - IPCA)
  - Categorize SELIC levels for analysis

Dependencies:
  - stg_indicators

Grain:
  One row per date

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH indicators AS (
    SELECT
        date,
        indicator_code,
        value
    FROM {{ ref('stg_indicators') }}
),

-- Pivot: Transform rows to columns
pivoted AS (
    SELECT
        date,

        MAX(CASE WHEN indicator_code = '432' THEN value END) AS selic_rate,
        MAX(CASE WHEN indicator_code = '433' THEN value END) AS ipca,
        MAX(CASE WHEN indicator_code = '1' THEN value END) AS usd_brl,
        MAX(CASE WHEN indicator_code = '4389' THEN value END) AS cdi,
        MAX(CASE WHEN indicator_code = '24363' THEN value END) AS unemployment,
        MAX(CASE WHEN indicator_code = '189' THEN value END) AS igp_m,
        MAX(CASE WHEN indicator_code = '4380' THEN value END) AS selic_target

    FROM indicators
    GROUP BY date
),

-- Create groupings for forward-fill (PostgreSQL workaround for IGNORE NULLS)
-- Each group starts when we have a non-null value
with_groups AS (
    SELECT
        date,
        selic_rate,
        usd_brl,
        cdi,
        selic_target,
        ipca,
        unemployment,
        igp_m,
        -- Create group numbers: increment only when value is not null
        COUNT(ipca) OVER (ORDER BY date) AS ipca_grp,
        COUNT(unemployment) OVER (ORDER BY date) AS unemployment_grp,
        COUNT(igp_m) OVER (ORDER BY date) AS igp_m_grp
    FROM pivoted
),

-- Forward fill for monthly indicators using FIRST_VALUE within each group
with_forward_fill AS (
    SELECT
        date,

        -- Daily indicators (no fill needed)
        selic_rate,
        usd_brl,
        cdi,
        selic_target,

        -- Monthly indicators: forward fill using group-based approach
        FIRST_VALUE(ipca) OVER (
            PARTITION BY ipca_grp
            ORDER BY date
        ) AS ipca,

        FIRST_VALUE(unemployment) OVER (
            PARTITION BY unemployment_grp
            ORDER BY date
        ) AS unemployment,

        FIRST_VALUE(igp_m) OVER (
            PARTITION BY igp_m_grp
            ORDER BY date
        ) AS igp_m

    FROM with_groups
),

-- Calculate derived metrics
with_calculations AS (
    SELECT
        date,
        selic_rate,
        ipca,
        usd_brl,
        cdi,
        unemployment,
        igp_m,
        selic_target,

        -- Real interest rate (SELIC - Inflation)
        -- This is what investors actually care about
        CASE
            WHEN selic_rate IS NOT NULL AND ipca IS NOT NULL
            THEN selic_rate - ipca
            ELSE NULL
        END AS real_interest_rate,

        -- SELIC vs CDI spread (should be close to 0)
        CASE
            WHEN selic_rate IS NOT NULL AND cdi IS NOT NULL
            THEN selic_rate - cdi
            ELSE NULL
        END AS selic_cdi_spread,

        -- USD/BRL change from previous day
        usd_brl - LAG(usd_brl) OVER (ORDER BY date) AS usd_brl_change,

        -- Categorize SELIC level for analysis
        CASE
            WHEN selic_rate < 5 THEN 'Very Low'
            WHEN selic_rate < 8 THEN 'Low'
            WHEN selic_rate < 11 THEN 'Moderate'
            WHEN selic_rate < 14 THEN 'High'
            ELSE 'Very High'
        END AS selic_category

    FROM with_forward_fill
)

SELECT * FROM with_calculations
