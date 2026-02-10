/*
=====================================================================
DATA QUALITY TEST: Positive Prices
=====================================================================
Purpose:
  Ensure all closing prices in fact table are positive.
  Zero or negative prices indicate data quality issues.

Expectation:
  Query should return 0 rows (no failures)
=====================================================================
*/

SELECT
    date_id,
    stock_id,
    close_price
FROM {{ ref('fact_daily_market') }}
WHERE close_price <= 0
   OR close_price IS NULL
