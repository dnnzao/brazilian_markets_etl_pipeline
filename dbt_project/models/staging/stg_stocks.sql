{{
  config(
    materialized='view',
    tags=['staging', 'stocks']
  )
}}

/*
=====================================================================
STAGING MODEL: Stock Prices
=====================================================================
Purpose:
  Clean and validate raw stock price data from Yahoo Finance.
  This is the first transformation layer - we fix data quality
  issues but don't apply business logic yet.

Transformations:
  - Remove rows with null close_price (data quality)
  - Filter out negative or zero prices (invalid data)
  - Remove obvious outliers (prices > 1M)
  - Validate high >= low constraint
  - Add data quality flags for suspicious records

Dependencies:
  - raw.stocks (source table)

Grain:
  One row per ticker per trading date

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'stocks') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        ticker,
        date,

        -- Price data with quality checks
        CASE
            WHEN open_price <= 0 THEN NULL
            ELSE open_price
        END AS open_price,

        CASE
            WHEN high_price <= 0 THEN NULL
            ELSE high_price
        END AS high_price,

        CASE
            WHEN low_price <= 0 THEN NULL
            ELSE low_price
        END AS low_price,

        CASE
            WHEN close_price <= 0 THEN NULL
            WHEN close_price > 1000000 THEN NULL  -- Outlier detection
            ELSE close_price
        END AS close_price,

        -- Volume (clean negatives)
        CASE
            WHEN volume < 0 THEN 0
            ELSE volume
        END AS volume,

        -- Adjusted close
        adj_close,

        -- Metadata
        loaded_at,
        source

    FROM source
    WHERE 1=1
        -- Filter date range
        AND date >= '{{ var("start_date") }}'
        AND date <= CURRENT_DATE
        -- Must have close price
        AND close_price IS NOT NULL
        -- Data quality: high should be >= low
        AND (high_price >= low_price OR high_price IS NULL OR low_price IS NULL)
),

-- Add derived columns and quality flags
final AS (
    SELECT
        ticker,
        date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        adj_close,
        loaded_at,
        source,

        -- Flag suspicious records for investigation
        CASE
            WHEN open_price IS NOT NULL AND close_price > open_price * 1.5 THEN TRUE
            WHEN open_price IS NOT NULL AND close_price < open_price * 0.5 THEN TRUE
            ELSE FALSE
        END AS is_suspicious,

        -- Calculate intraday range
        CASE
            WHEN high_price IS NOT NULL AND low_price IS NOT NULL
            THEN high_price - low_price
            ELSE NULL
        END AS intraday_range,

        -- Calculate intraday range percentage
        CASE
            WHEN high_price IS NOT NULL AND low_price IS NOT NULL AND low_price > 0
            THEN ((high_price - low_price) / low_price) * 100
            ELSE NULL
        END AS intraday_range_pct

    FROM cleaned
    WHERE close_price IS NOT NULL
)

SELECT * FROM final
