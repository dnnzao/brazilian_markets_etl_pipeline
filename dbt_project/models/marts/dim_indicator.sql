{{
  config(
    materialized='table',
    tags=['dimension', 'indicator']
  )
}}

/*
=====================================================================
DIMENSION TABLE: Indicator
=====================================================================
Purpose:
  Economic indicator dimension with metadata about each BCB series.
  Provides context for indicator values in fact table.

Features:
  - Surrogate key for dimensional joins
  - Human-readable descriptions
  - Unit and frequency information

Grain:
  One row per indicator

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH indicator_list AS (
    SELECT DISTINCT
        indicator_code,
        indicator_name,
        unit,
        frequency
    FROM {{ ref('stg_indicators') }}
),

indicator_dimension AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY indicator_code) AS indicator_id,
        indicator_code,
        indicator_name,

        -- Add human-readable descriptions
        CASE indicator_code
            WHEN '432' THEN 'Brazilian base interest rate set by Central Bank (COPOM)'
            WHEN '433' THEN 'Consumer price inflation index (official inflation measure)'
            WHEN '1' THEN 'US Dollar to Brazilian Real exchange rate'
            WHEN '4389' THEN 'Interbank deposit certificate rate'
            WHEN '24363' THEN 'National unemployment rate (PNAD survey)'
            WHEN '189' THEN 'General market price index (FGV)'
            WHEN '4380' THEN 'SELIC target rate set by COPOM'
            ELSE 'Economic indicator from BCB'
        END AS description,

        unit,
        frequency,
        'bcb_api' AS source,
        CURRENT_TIMESTAMP AS created_at

    FROM indicator_list
)

SELECT * FROM indicator_dimension
