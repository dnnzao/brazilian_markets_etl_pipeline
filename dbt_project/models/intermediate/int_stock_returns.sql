{{
  config(
    materialized='view',
    tags=['intermediate', 'calculations']
  )
}}

/*
=====================================================================
INTERMEDIATE MODEL: Stock Returns
=====================================================================
Purpose:
  Calculate stock returns at different time horizons for analysis.
  Returns are the core metric for investment performance.

Calculations:
  - Daily returns: (today - yesterday) / yesterday
  - Weekly returns: 5-day lookback
  - Monthly returns: 21-day lookback
  - Yearly returns: 252-day lookback
  - Year-to-date returns: From first trading day of the year

Dependencies:
  - stg_stocks

Grain:
  One row per ticker per trading date

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH stock_prices AS (
    SELECT
        ticker,
        date,
        close_price,
        adj_close
    FROM {{ ref('stg_stocks') }}
),

with_lags AS (
    SELECT
        ticker,
        date,
        close_price,

        -- Previous day's price for daily return
        LAG(close_price, 1) OVER (
            PARTITION BY ticker
            ORDER BY date
        ) AS prev_day_close,

        -- Price from 5 days ago (weekly)
        LAG(close_price, 5) OVER (
            PARTITION BY ticker
            ORDER BY date
        ) AS prev_week_close,

        -- Price from 21 days ago (monthly, ~trading days)
        LAG(close_price, 21) OVER (
            PARTITION BY ticker
            ORDER BY date
        ) AS prev_month_close,

        -- Price from 252 days ago (yearly, ~trading days)
        LAG(close_price, 252) OVER (
            PARTITION BY ticker
            ORDER BY date
        ) AS prev_year_close

    FROM stock_prices
),

calculate_returns AS (
    SELECT
        ticker,
        date,
        close_price,

        -- Daily return
        CASE
            WHEN prev_day_close IS NOT NULL AND prev_day_close > 0
            THEN ((close_price - prev_day_close) / prev_day_close)
            ELSE NULL
        END AS daily_return,

        -- Weekly return (5 trading days)
        CASE
            WHEN prev_week_close IS NOT NULL AND prev_week_close > 0
            THEN ((close_price - prev_week_close) / prev_week_close)
            ELSE NULL
        END AS weekly_return,

        -- Monthly return (~21 trading days)
        CASE
            WHEN prev_month_close IS NOT NULL AND prev_month_close > 0
            THEN ((close_price - prev_month_close) / prev_month_close)
            ELSE NULL
        END AS monthly_return,

        -- Yearly return (~252 trading days)
        CASE
            WHEN prev_year_close IS NOT NULL AND prev_year_close > 0
            THEN ((close_price - prev_year_close) / prev_year_close)
            ELSE NULL
        END AS yearly_return,

        -- Year-to-date return
        (close_price / FIRST_VALUE(close_price) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) - 1) AS ytd_return

    FROM with_lags
)

SELECT * FROM calculate_returns
