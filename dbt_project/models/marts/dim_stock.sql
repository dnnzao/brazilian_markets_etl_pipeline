{{
  config(
    materialized='table',
    tags=['dimension', 'stock']
  )
}}

/*
=====================================================================
DIMENSION TABLE: Stock
=====================================================================
Purpose:
  Stock dimension with company metadata for filtering and grouping.
  Combines actual traded tickers with seed metadata.

Features:
  - Surrogate key for dimensional joins
  - Company name and sector classification
  - Market cap category and listing segment
  - First traded date from actual data

Grain:
  One row per stock

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH stock_list AS (
    -- Get distinct tickers from staging
    SELECT DISTINCT ticker
    FROM {{ ref('stg_stocks') }}
),

-- Build dimension from actual traded stocks
stock_dimension AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY s.ticker) AS stock_id,
        s.ticker,

        -- Derive company name from ticker
        REPLACE(REPLACE(s.ticker, '.SA', ''), '4', '') || ' S.A.' AS company_name,

        -- Default sector mappings based on common Brazilian stocks
        CASE
            WHEN s.ticker LIKE 'PETR%' OR s.ticker LIKE 'CSAN%' THEN 'Oil & Gas'
            WHEN s.ticker LIKE 'VALE%' THEN 'Mining'
            WHEN s.ticker LIKE 'ITUB%' OR s.ticker LIKE 'BBDC%' OR s.ticker LIKE 'BBAS%' THEN 'Banking'
            WHEN s.ticker LIKE 'ABEV%' OR s.ticker LIKE 'JBSS%' THEN 'Food & Beverage'
            WHEN s.ticker LIKE 'B3SA%' THEN 'Financial Services'
            WHEN s.ticker LIKE 'RENT%' OR s.ticker LIKE 'RAIL%' THEN 'Transportation'
            WHEN s.ticker LIKE 'WEGE%' OR s.ticker LIKE 'EMBR%' THEN 'Industrials'
            WHEN s.ticker LIKE 'SUZB%' OR s.ticker LIKE 'GGBR%' THEN 'Materials'
            WHEN s.ticker LIKE 'MGLU%' OR s.ticker LIKE 'LREN%' OR s.ticker LIKE 'RADL%' THEN 'Retail'
            WHEN s.ticker LIKE 'HAPV%' THEN 'Healthcare'
            WHEN s.ticker LIKE 'TOTS%' THEN 'Technology'
            ELSE 'Other'
        END AS sector,

        CASE
            WHEN s.ticker LIKE 'PETR%' THEN 'Exploration & Production'
            WHEN s.ticker LIKE 'VALE%' THEN 'Iron Ore'
            WHEN s.ticker LIKE 'ITUB%' OR s.ticker LIKE 'BBDC%' OR s.ticker LIKE 'BBAS%' THEN 'Commercial Banks'
            WHEN s.ticker LIKE 'ABEV%' THEN 'Beverages'
            WHEN s.ticker LIKE 'JBSS%' THEN 'Meat Processing'
            WHEN s.ticker LIKE 'B3SA%' THEN 'Exchange'
            WHEN s.ticker LIKE 'RENT%' THEN 'Car Rental'
            WHEN s.ticker LIKE 'RAIL%' THEN 'Logistics'
            WHEN s.ticker LIKE 'WEGE%' THEN 'Electrical Equipment'
            WHEN s.ticker LIKE 'EMBR%' THEN 'Aerospace'
            WHEN s.ticker LIKE 'SUZB%' THEN 'Paper & Pulp'
            WHEN s.ticker LIKE 'GGBR%' THEN 'Steel'
            WHEN s.ticker LIKE 'MGLU%' THEN 'E-commerce'
            WHEN s.ticker LIKE 'LREN%' THEN 'Fashion Retail'
            WHEN s.ticker LIKE 'RADL%' THEN 'Pharmacy'
            WHEN s.ticker LIKE 'CSAN%' THEN 'Distribution'
            WHEN s.ticker LIKE 'HAPV%' THEN 'Health Insurance'
            WHEN s.ticker LIKE 'TOTS%' THEN 'Software'
            ELSE 'Other'
        END AS subsector,

        -- Default to Large Cap for major stocks
        CASE
            WHEN s.ticker IN ('PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'ABEV3.SA',
                             'B3SA3.SA', 'WEGE3.SA', 'SUZB3.SA', 'BBAS3.SA', 'GGBR4.SA',
                             'JBSS3.SA', 'LREN3.SA', 'CSAN3.SA', 'RADL3.SA', 'EMBR3.SA',
                             'HAPV3.SA', 'RENT3.SA')
            THEN 'Large Cap'
            ELSE 'Mid Cap'
        END AS market_cap_category,

        'Novo Mercado' AS listing_segment,

        -- Calculate first traded date from actual data
        (SELECT MIN(date) FROM {{ ref('stg_stocks') }} WHERE ticker = s.ticker) AS first_traded_date,

        TRUE AS is_active,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM stock_list s
)

SELECT * FROM stock_dimension
