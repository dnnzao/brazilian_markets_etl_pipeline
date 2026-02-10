/*
=====================================================================
DATA QUALITY TEST: Valid Returns
=====================================================================
Purpose:
  Ensure daily returns are within reasonable bounds.
  Returns greater than 100% or less than -100% are suspicious.

Expectation:
  Query should return 0 rows (no failures)
=====================================================================
*/

SELECT
    date_id,
    stock_id,
    daily_return
FROM {{ ref('fact_daily_market') }}
WHERE daily_return IS NOT NULL
  AND (daily_return > 1.0 OR daily_return < -1.0)
