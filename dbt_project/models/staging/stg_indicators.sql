{{
  config(
    materialized='view',
    tags=['staging', 'indicators']
  )
}}

/*
=====================================================================
STAGING MODEL: Economic Indicators
=====================================================================
Purpose:
  Clean and standardize raw economic indicator data from BCB API.
  Standardizes indicator names and ensures data quality.

Transformations:
  - Standardize indicator names
  - Filter date range
  - Remove null values

Dependencies:
  - raw.indicators (source table)

Grain:
  One row per indicator per date

Author: Dênio Barbosa Júnior
=====================================================================
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'indicators') }}
),

cleaned AS (
    SELECT
        indicator_code,

        -- Standardize indicator names
        CASE indicator_code
            WHEN '432' THEN 'SELIC'
            WHEN '433' THEN 'IPCA'
            WHEN '1' THEN 'USD_BRL'
            WHEN '4389' THEN 'CDI'
            WHEN '24363' THEN 'Unemployment'
            WHEN '189' THEN 'IGP_M'
            WHEN '4380' THEN 'SELIC_Target'
            ELSE COALESCE(indicator_name, 'Unknown')
        END AS indicator_name,

        date,
        value,
        unit,
        frequency,
        loaded_at,
        source

    FROM source
    WHERE 1=1
        AND date >= '{{ var("start_date") }}'
        AND date <= CURRENT_DATE
        AND value IS NOT NULL
)

SELECT * FROM cleaned
