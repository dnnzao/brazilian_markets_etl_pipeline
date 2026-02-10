/*
=====================================================================
DATA QUALITY TEST: No Future Dates
=====================================================================
Purpose:
  Ensure no records have dates in the future.
  Future dates indicate data quality issues or loading errors.

Expectation:
  Query should return 0 rows (no failures)
=====================================================================
*/

SELECT
    date_id,
    COUNT(*) AS row_count
FROM {{ ref('fact_daily_market') }}
WHERE date_id > TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER
GROUP BY date_id
