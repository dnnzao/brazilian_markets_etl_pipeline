{{
  config(
    materialized='view',
    tags=['intermediate', 'calculations']
  )
}}

/*
=====================================================================
INTERMEDIATE MODEL: Stock Volatility
=====================================================================
Purpose:
  Calculate stock volatility (risk measure) at different time windows.
  Volatility = standard deviation of returns over time.
  Higher volatility = more risk.

Calculations:
  - 7-day volatility: Short-term risk
  - 30-day volatility: Medium-term risk
  - 90-day volatility: Long-term risk
  - Annualized volatility: Industry standard (daily vol * sqrt(252))
  - Volatility percentile: Relative ranking among stocks

Dependencies:
  - int_stock_returns

Grain:
  One row per ticker per trading date

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH stock_returns AS (
    SELECT
        ticker,
        date,
        daily_return
    FROM {{ ref('int_stock_returns') }}
    WHERE daily_return IS NOT NULL
),

calculate_volatility AS (
    SELECT
        ticker,
        date,
        daily_return,

        -- 7-day rolling volatility (1 week)
        STDDEV(daily_return) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volatility_7d,

        -- 30-day rolling volatility (1 month)
        STDDEV(daily_return) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d,

        -- 90-day rolling volatility (1 quarter)
        STDDEV(daily_return) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS volatility_90d,

        -- Annualized volatility (industry standard)
        -- Formula: Daily volatility × √252 (trading days in a year)
        STDDEV(daily_return) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) * SQRT(252) AS annualized_volatility,

        -- Moving averages of returns (technical indicators)
        AVG(daily_return) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS avg_return_7d,

        AVG(daily_return) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_return_30d

    FROM stock_returns
),

-- Add volatility percentile ranking
with_rankings AS (
    SELECT
        ticker,
        date,
        daily_return,
        volatility_7d,
        volatility_30d,
        volatility_90d,
        annualized_volatility,
        avg_return_7d,
        avg_return_30d,

        -- Percentile rank within all stocks on this date
        -- 0 = least volatile, 1 = most volatile
        PERCENT_RANK() OVER (
            PARTITION BY date
            ORDER BY volatility_30d NULLS FIRST
        ) AS volatility_percentile

    FROM calculate_volatility
)

SELECT * FROM with_rankings
