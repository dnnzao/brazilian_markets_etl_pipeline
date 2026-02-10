{{
  config(
    materialized='incremental',
    unique_key=['date_id', 'stock_id'],
    tags=['fact', 'market']
  )
}}

/*
=====================================================================
FACT TABLE: Daily Market
=====================================================================
Purpose:
  Central fact table for daily market analysis.
  Contains stock prices, returns, volatility, and macro context.
  This is the primary table for dashboard queries.

Features:
  - Foreign keys to all dimensions
  - Price data (OHLCV)
  - Calculated returns and volatility
  - Denormalized macro indicators for query performance

Grain:
  One row per stock per trading date

Incremental Strategy:
  Only process dates newer than existing max date

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH stocks AS (
    SELECT
        ticker,
        date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        adj_close,
        intraday_range,
        intraday_range_pct
    FROM {{ ref('stg_stocks') }}
),

returns AS (
    SELECT
        ticker,
        date,
        daily_return,
        weekly_return,
        monthly_return,
        yearly_return,
        ytd_return
    FROM {{ ref('int_stock_returns') }}
),

volatility AS (
    SELECT
        ticker,
        date,
        volatility_7d,
        volatility_30d,
        volatility_90d,
        annualized_volatility,
        volatility_percentile
    FROM {{ ref('int_stock_volatility') }}
),

indicators AS (
    SELECT
        date,
        selic_rate,
        ipca,
        usd_brl,
        cdi,
        unemployment,
        igp_m,
        real_interest_rate,
        selic_category
    FROM {{ ref('int_market_indicators') }}
),

dim_date AS (
    SELECT date_id, date, is_trading_day
    FROM {{ ref('dim_date') }}
),

dim_stock AS (
    SELECT stock_id, ticker
    FROM {{ ref('dim_stock') }}
),

-- Join everything together
joined AS (
    SELECT
        -- Foreign keys
        dd.date_id,
        ds.stock_id,

        -- Stock price data
        s.open_price,
        s.high_price,
        s.low_price,
        s.close_price,
        s.volume,
        s.adj_close,
        s.intraday_range,
        s.intraday_range_pct,

        -- Returns
        r.daily_return,
        r.weekly_return,
        r.monthly_return,
        r.yearly_return,
        r.ytd_return,

        -- Volatility
        v.volatility_7d,
        v.volatility_30d,
        v.volatility_90d,
        v.annualized_volatility,
        v.volatility_percentile,

        -- Economic context (denormalized for performance)
        i.selic_rate,
        i.ipca AS inflation_rate,
        i.usd_brl,
        i.cdi,
        i.unemployment,
        i.real_interest_rate,
        i.selic_category,

        -- Metadata
        CURRENT_TIMESTAMP AS created_at

    FROM stocks s
    INNER JOIN dim_date dd
        ON s.date = dd.date
    INNER JOIN dim_stock ds
        ON s.ticker = ds.ticker
    LEFT JOIN returns r
        ON s.ticker = r.ticker
        AND s.date = r.date
    LEFT JOIN volatility v
        ON s.ticker = v.ticker
        AND s.date = v.date
    LEFT JOIN indicators i
        ON s.date = i.date

    WHERE dd.is_trading_day = TRUE

    {% if is_incremental() %}
        -- Incremental: Only process new dates
        AND s.date > (
            SELECT COALESCE(MAX(dd2.date), '1900-01-01'::date)
            FROM {{ this }} f
            INNER JOIN {{ ref('dim_date') }} dd2 ON f.date_id = dd2.date_id
        )
    {% endif %}
)

SELECT * FROM joined
