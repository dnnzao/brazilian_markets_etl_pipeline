# Detailed dbt Model Examples

---

## dbt Project Structure Review

```
dbt_project/
├── dbt_project.yml              # Project configuration
├── profiles.yml                 # Database connections
├── models/
│   ├── staging/                 # Layer 1: Clean raw data
│   │   ├── _staging.yml         # Documentation
│   │   ├── stg_stocks.sql
│   │   └── stg_indicators.sql
│   ├── intermediate/            # Layer 2: Business logic
│   │   ├── _intermediate.yml
│   │   ├── int_stock_returns.sql
│   │   ├── int_stock_volatility.sql
│   │   └── int_market_indicators.sql
│   └── marts/                   # Layer 3: Final tables
│       ├── _marts.yml
│       ├── dim_date.sql
│       ├── dim_stock.sql
│       ├── dim_indicator.sql
│       └── fact_daily_market.sql
├── tests/                       # Data quality tests
├── macros/                      # Reusable SQL functions
└── seeds/                       # Reference data (CSVs)
```

---

## Configuration Files

### **File: `dbt_project.yml`**

```yaml
name: 'brazilian_market'
version: '1.0.0'
config-version: 2

profile: 'brazilian_market'

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  brazilian_market:
    # Staging models: Views (fast rebuild)
    staging:
      +materialized: view
      +schema: staging

    # Intermediate models: Views (cheap compute)
    intermediate:
      +materialized: view
      +schema: staging

    # Marts: Tables (performance)
    marts:
      +materialized: table
      +schema: analytics

seeds:
  brazilian_market:
    +schema: analytics
    +quote_columns: false

# Variables (can be overridden at runtime)
vars:
  start_date: '2015-01-01'
  stock_tickers:
    - PETR4
    - VALE3
    - ITUB4
    - BBDC4
```

### **File: `profiles.yml`**

```yaml
brazilian_market:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: dataeng
      password: dataeng123
      dbname: brazilian_market
      schema: analytics
      threads: 4
      keepalives_idle: 0
      connect_timeout: 10
      search_path: "raw,staging,analytics,public"

    prod:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST') }}"
      port: "{{ env_var('POSTGRES_PORT') }}"
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: analytics
      threads: 8
```

---

## Layer 1: Staging Models (Clean Raw Data)

### **File: `models/staging/stg_stocks.sql`**

```sql
{{
  config(
    materialized='view',
    tags=['staging', 'stocks']
  )
}}

/*
Staging model for stock prices
- Cleans raw data from Yahoo Finance
- Filters out invalid records
- Standardizes column names
- No business logic yet
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'stocks') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        ticker,
        date,

        -- Price data (clean nulls and negatives)
        CASE
            WHEN close_price <= 0 THEN NULL
            WHEN close_price > 1000000 THEN NULL  -- Outlier detection
            ELSE close_price
        END AS close_price,

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

        -- Volume (clean negatives, convert to BIGINT)
        CASE
            WHEN volume < 0 THEN 0
            ELSE volume
        END AS volume,

        -- Adjusted close (for splits/dividends)
        adj_close,

        -- Metadata
        loaded_at,
        source

    FROM source
    WHERE 1=1
        -- Filter date range
        AND date >= '{{ var("start_date") }}'
        AND date <= CURRENT_DATE

        -- Must have close price (core requirement)
        AND close_price IS NOT NULL

        -- Data quality: high should be >= low
        AND (high_price >= low_price OR high_price IS NULL OR low_price IS NULL)
),

-- Add data quality flag
final AS (
    SELECT
        *,

        -- Flag suspicious records for investigation
        CASE
            WHEN close_price > open_price * 1.5 THEN TRUE  -- 50%+ jump in one day
            WHEN close_price < open_price * 0.5 THEN TRUE  -- 50%+ drop in one day
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
)

SELECT * FROM final
```

### **File: `models/staging/stg_indicators.sql`**

```sql
{{
  config(
    materialized='view',
    tags=['staging', 'indicators']
  )
}}

/*
Staging model for economic indicators
- Cleans raw data from BCB API
- Standardizes indicator names
- Handles different frequencies (daily, monthly, quarterly)
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'indicators') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        indicator_code,

        -- Standardize indicator names
        CASE indicator_code
            WHEN '432' THEN 'SELIC'
            WHEN '433' THEN 'IPCA'
            WHEN '1' THEN 'USD_BRL'
            WHEN '4189' THEN 'GDP'
            WHEN '24363' THEN 'UNEMPLOYMENT'
            WHEN '11' THEN 'CDI'
            WHEN '189' THEN 'IGP_M'
            ELSE indicator_name
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
),

-- Forward fill monthly/quarterly indicators to daily frequency
-- This is needed to join with daily stock data
with_daily_values AS (
    SELECT
        indicator_code,
        indicator_name,
        date,
        value,
        unit,
        frequency,

        -- For monthly/quarterly data, we forward-fill:
        -- Use the most recent value for each day
        CASE
            WHEN frequency IN ('monthly', 'quarterly') THEN
                LAST_VALUE(value) IGNORE NULLS OVER (
                    PARTITION BY indicator_code
                    ORDER BY date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            ELSE value
        END AS daily_value,

        loaded_at,
        source

    FROM cleaned
)

SELECT * FROM with_daily_values
```

### **File: `models/staging/_staging.yml`**

```yaml
version: 2

sources:
  - name: raw
    database: brazilian_market
    schema: raw
    tables:
      - name: stocks
        description: "Raw stock price data from Yahoo Finance"
        columns:
          - name: ticker
            description: "Stock ticker symbol (without .SA suffix)"
            tests:
              - not_null
          - name: date
            description: "Trading date"
            tests:
              - not_null
          - name: close_price
            description: "Closing price in BRL"
            tests:
              - not_null

      - name: indicators
        description: "Raw economic indicators from BCB API"
        columns:
          - name: indicator_code
            description: "BCB series code"
            tests:
              - not_null
          - name: date
            description: "Indicator date"
            tests:
              - not_null

models:
  - name: stg_stocks
    description: "Cleaned and validated stock price data"
    columns:
      - name: ticker
        description: "Stock ticker (PETR4, VALE3, etc.)"
        tests:
          - not_null
          - relationships:
              to: ref('seed_stock_metadata')
              field: ticker

      - name: date
        description: "Trading date"
        tests:
          - not_null

      - name: close_price
        description: "Closing price (cleaned, no nulls/negatives)"
        tests:
          - not_null
          - positive_price:  # Custom test (see tests/)

      - name: is_suspicious
        description: "Flag for records with unusual price movements"

  - name: stg_indicators
    description: "Cleaned and standardized economic indicators"
    columns:
      - name: indicator_code
        description: "Standardized indicator code"
        tests:
          - not_null
          - accepted_values:
              values: ['432', '433', '1', '4189', '24363', '11', '189']

      - name: daily_value
        description: "Indicator value (forward-filled for monthly/quarterly)"
        tests:
          - not_null
```

---

## Layer 2: Intermediate Models (Business Logic)

### **File: `models/intermediate/int_stock_returns.sql`**

```sql
{{
  config(
    materialized='view',
    tags=['intermediate', 'calculations']
  )
}}

/*
Calculate stock returns at different time horizons
- Daily returns
- Weekly returns (5 trading days)
- Monthly returns (~21 trading days)
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

        -- Get previous day's price
        LAG(close_price, 1) OVER (
            PARTITION BY ticker
            ORDER BY date
        ) AS prev_day_close,

        -- Get price from 5 days ago (weekly)
        LAG(close_price, 5) OVER (
            PARTITION BY ticker
            ORDER BY date
        ) AS prev_week_close,

        -- Get price from 21 days ago (monthly)
        LAG(close_price, 21) OVER (
            PARTITION BY ticker
            ORDER BY date
        ) AS prev_month_close,

        -- Get price from 252 days ago (yearly, ~trading days in a year)
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

        -- Daily return (most important)
        -- Formula: (today - yesterday) / yesterday
        CASE
            WHEN prev_day_close IS NOT NULL AND prev_day_close > 0
            THEN ((close_price - prev_day_close) / prev_day_close)
            ELSE NULL
        END AS daily_return,

        -- Weekly return
        CASE
            WHEN prev_week_close IS NOT NULL AND prev_week_close > 0
            THEN ((close_price - prev_week_close) / prev_week_close)
            ELSE NULL
        END AS weekly_return,

        -- Monthly return
        CASE
            WHEN prev_month_close IS NOT NULL AND prev_month_close > 0
            THEN ((close_price - prev_month_close) / prev_month_close)
            ELSE NULL
        END AS monthly_return,

        -- Yearly return
        CASE
            WHEN prev_year_close IS NOT NULL AND prev_year_close > 0
            THEN ((close_price - prev_year_close) / prev_year_close)
            ELSE NULL
        END AS yearly_return,

        -- Year-to-date return
        -- Compare today's price to first trading day of the year
        (close_price / FIRST_VALUE(close_price) OVER (
            PARTITION BY ticker, EXTRACT(YEAR FROM date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) - 1) AS ytd_return

    FROM with_lags
)

SELECT * FROM calculate_returns
```

### **File: `models/intermediate/int_stock_volatility.sql`**

```sql
{{
  config(
    materialized='view',
    tags=['intermediate', 'calculations']
  )
}}

/*
Calculate stock volatility (risk measure)
Volatility = Standard deviation of returns over time window
Higher volatility = More risky
*/

WITH stock_returns AS (
    SELECT
        ticker,
        date,
        daily_return
    FROM {{ ref('int_stock_returns') }}
    WHERE daily_return IS NOT NULL  -- Can't calculate std dev with nulls
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

        -- Moving averages (technical indicators)
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
-- This tells you if a stock is MORE or LESS volatile than average
with_rankings AS (
    SELECT
        *,

        -- Percentile rank within all stocks on this date
        -- 0 = least volatile, 1 = most volatile
        PERCENT_RANK() OVER (
            PARTITION BY date
            ORDER BY volatility_30d
        ) AS volatility_percentile

    FROM calculate_volatility
)

SELECT * FROM with_rankings
```

### **File: `models/intermediate/int_market_indicators.sql`**

```sql
{{
  config(
    materialized='view',
    tags=['intermediate', 'indicators']
  )
}}

/*
Prepare economic indicators for joining with stock data
- Pivot indicators to columns (easier to work with)
- Ensure daily frequency for all indicators
*/

WITH indicators AS (
    SELECT
        date,
        indicator_code,
        daily_value
    FROM {{ ref('stg_indicators') }}
),

-- Pivot: Transform rows to columns
-- From: multiple rows per date (one per indicator)
-- To: one row per date with all indicators as columns
pivoted AS (
    SELECT
        date,

        MAX(CASE WHEN indicator_code = '432' THEN daily_value END) AS selic_rate,
        MAX(CASE WHEN indicator_code = '433' THEN daily_value END) AS ipca,
        MAX(CASE WHEN indicator_code = '1' THEN daily_value END) AS usd_brl,
        MAX(CASE WHEN indicator_code = '4189' THEN daily_value END) AS gdp,
        MAX(CASE WHEN indicator_code = '24363' THEN daily_value END) AS unemployment,
        MAX(CASE WHEN indicator_code = '11' THEN daily_value END) AS cdi,
        MAX(CASE WHEN indicator_code = '189' THEN daily_value END) AS igp_m

    FROM indicators
    GROUP BY date
),

-- Calculate derived metrics
with_calculations AS (
    SELECT
        *,

        -- Real interest rate (SELIC - Inflation)
        -- This is what investors REALLY care about
        selic_rate - ipca AS real_interest_rate,

        -- SELIC vs CDI spread (should be close to 0)
        selic_rate - cdi AS selic_cdi_spread,

        -- Change in USD/BRL (currency depreciation/appreciation)
        usd_brl - LAG(usd_brl) OVER (ORDER BY date) AS usd_brl_change,

        -- Categorize SELIC level (for analysis)
        CASE
            WHEN selic_rate < 5 THEN 'Very Low'
            WHEN selic_rate < 8 THEN 'Low'
            WHEN selic_rate < 11 THEN 'Moderate'
            WHEN selic_rate < 14 THEN 'High'
            ELSE 'Very High'
        END AS selic_category

    FROM pivoted
)

SELECT * FROM with_calculations
```

---

## Layer 3: Marts (Final Dimensional Model)

### **File: `models/marts/dim_date.sql`**

```sql
{{
  config(
    materialized='table',
    tags=['dimension', 'date']
  )
}}

/*
Date dimension table
- One row per date
- Contains all date attributes for filtering/grouping
- Pre-compute to avoid repeated DATE functions in queries
*/

WITH date_spine AS (
    -- Generate all dates from 2015 to 2030
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    )}}
),

date_attributes AS (
    SELECT
        -- Surrogate key (faster joins than actual dates)
        TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_id,

        date_day AS date,

        -- Year attributes
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(ISOYEAR FROM date_day) AS iso_year,

        -- Quarter attributes
        EXTRACT(QUARTER FROM date_day) AS quarter,
        'Q' || EXTRACT(QUARTER FROM date_day)::TEXT AS quarter_name,
        DATE_TRUNC('quarter', date_day)::DATE AS quarter_start_date,

        -- Month attributes
        EXTRACT(MONTH FROM date_day) AS month,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_name_short,
        DATE_TRUNC('month', date_day)::DATE AS month_start_date,

        -- Week attributes
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(DOW FROM date_day) AS day_of_week,  -- 0=Sunday, 6=Saturday
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_name_short,

        -- Day attributes
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DOY FROM date_day) AS day_of_year,

        -- Boolean flags
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE ELSE FALSE END AS is_month_start,
        CASE WHEN date_day = DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day' THEN TRUE ELSE FALSE END AS is_month_end,
        CASE WHEN EXTRACT(MONTH FROM date_day) = 1 AND EXTRACT(DAY FROM date_day) = 1 THEN TRUE ELSE FALSE END AS is_year_start,
        CASE WHEN EXTRACT(MONTH FROM date_day) = 12 AND EXTRACT(DAY FROM date_day) = 31 THEN TRUE ELSE FALSE END AS is_year_end

    FROM date_spine
),

-- Add Brazilian holidays
with_holidays AS (
    SELECT
        *,

        -- Brazilian holidays (static list - could be moved to seed file)
        CASE
            WHEN month = 1 AND day_of_month = 1 THEN 'New Year'
            WHEN month = 4 AND day_of_month = 21 THEN 'Tiradentes Day'
            WHEN month = 5 AND day_of_month = 1 THEN 'Labor Day'
            WHEN month = 9 AND day_of_month = 7 THEN 'Independence Day'
            WHEN month = 10 AND day_of_month = 12 THEN 'Our Lady of Aparecida'
            WHEN month = 11 AND day_of_month = 2 THEN 'All Souls Day'
            WHEN month = 11 AND day_of_month = 15 THEN 'Proclamation of the Republic'
            WHEN month = 11 AND day_of_month = 20 THEN 'Black Consciousness Day'
            WHEN month = 12 AND day_of_month = 25 THEN 'Christmas'
            ELSE NULL
        END AS holiday_name,

        -- Is this a holiday?
        CASE
            WHEN month IN (1, 4, 5, 9, 10, 11, 12)
                AND day_of_month IN (1, 21, 1, 7, 12, 2, 15, 20, 25)
            THEN TRUE
            ELSE FALSE
        END AS is_holiday,

        -- Is this a trading day? (not weekend or holiday)
        CASE
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN FALSE  -- Weekend
            WHEN month IN (1, 4, 5, 9, 10, 11, 12)
                AND day_of_month IN (1, 21, 1, 7, 12, 2, 15, 20, 25) THEN FALSE  -- Holiday
            ELSE TRUE
        END AS is_trading_day

    FROM date_attributes
)

SELECT * FROM with_holidays
```

### **File: `models/marts/dim_stock.sql`**

```sql
{{
  config(
    materialized='table',
    tags=['dimension', 'stock']
  )
}}

/*
Stock dimension table
- One row per stock
- Contains stock metadata (sector, company name, etc.)
- Slowly Changing Dimension Type 1 (just update in place)
*/

WITH stock_list AS (
    -- Get distinct tickers from staging
    SELECT DISTINCT ticker
    FROM {{ ref('stg_stocks') }}
),

-- Join with seed data (manual metadata)
with_metadata AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY s.ticker) AS stock_id,
        s.ticker,
        COALESCE(m.company_name, s.ticker) AS company_name,
        m.sector,
        m.subsector,
        m.market_cap_category,
        m.listing_segment,

        -- Calculate first traded date from actual data
        (SELECT MIN(date) FROM {{ ref('stg_stocks') }} WHERE ticker = s.ticker) AS first_traded_date,

        TRUE AS is_active,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM stock_list s
    LEFT JOIN {{ ref('seed_stock_metadata') }} m
        ON s.ticker = m.ticker
)

SELECT * FROM with_metadata
```

### **File: `models/marts/dim_indicator.sql`**

```sql
{{
  config(
    materialized='table',
    tags=['dimension', 'indicator']
  )
}}

/*
Indicator dimension table
- One row per economic indicator
- Metadata about each indicator (unit, frequency, description)
*/

SELECT
    ROW_NUMBER() OVER (ORDER BY indicator_code) AS indicator_id,
    indicator_code,
    indicator_name,

    -- Add human-readable descriptions
    CASE indicator_code
        WHEN '432' THEN 'Brazilian base interest rate set by Central Bank'
        WHEN '433' THEN 'Consumer price inflation index'
        WHEN '1' THEN 'US Dollar to Brazilian Real exchange rate'
        WHEN '4189' THEN 'Gross Domestic Product'
        WHEN '24363' THEN 'National unemployment rate'
        WHEN '11' THEN 'Interbank deposit certificate rate'
        WHEN '189' THEN 'General market price index'
    END AS description,

    unit,
    frequency,
    'bcb_api' AS source,
    CURRENT_TIMESTAMP AS created_at

FROM (
    SELECT DISTINCT
        indicator_code,
        indicator_name,
        unit,
        frequency
    FROM {{ ref('stg_indicators') }}
) indicators
```

### **File: `models/marts/fact_daily_market.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key=['date_id', 'stock_id'],
    tags=['fact', 'market']
  )
}}

/*
Daily market fact table (STAR SCHEMA CORE)
- Grain: One row per stock per date
- Contains: prices, returns, volatility, macro context
- Incremental: Only process new dates
*/

WITH stocks AS (
    SELECT * FROM {{ ref('stg_stocks') }}
),

returns AS (
    SELECT * FROM {{ ref('int_stock_returns') }}
),

volatility AS (
    SELECT * FROM {{ ref('int_stock_volatility') }}
),

indicators AS (
    SELECT * FROM {{ ref('int_market_indicators') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_stock AS (
    SELECT * FROM {{ ref('dim_stock') }}
),

-- Join everything together
joined AS (
    SELECT
        -- Foreign keys
        dd.date_id,
        ds.stock_id,

        -- Stock data
        s.close_price,
        s.open_price,
        s.high_price,
        s.low_price,
        s.volume,
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
        i.gdp,
        i.unemployment,
        i.cdi,
        i.real_interest_rate,

        -- Metadata
        CURRENT_TIMESTAMP AS created_at

    FROM stocks s
    INNER JOIN returns r
        ON s.ticker = r.ticker
        AND s.date = r.date
    INNER JOIN volatility v
        ON s.ticker = v.ticker
        AND s.date = v.date
    LEFT JOIN indicators i
        ON s.date = i.date
    INNER JOIN dim_date dd
        ON s.date = dd.date
    INNER JOIN dim_stock ds
        ON s.ticker = ds.ticker

    WHERE 1=1
        AND dd.is_trading_day = TRUE  -- Only trading days

    {% if is_incremental() %}
        -- Incremental: Only process new dates
        AND s.date > (SELECT MAX(date) FROM {{ ref('dim_date') }} dd2
                      INNER JOIN {{ this }} f ON dd2.date_id = f.date_id)
    {% endif %}
)

SELECT * FROM joined
```

---

## Macros (Reusable SQL Functions)

### **File: `macros/calculate_return.sql`**

```sql
{% macro calculate_return(current_price, previous_price) %}
    CASE
        WHEN {{ previous_price }} IS NOT NULL
             AND {{ previous_price }} > 0
        THEN (({{ current_price }} - {{ previous_price }}) / {{ previous_price }})
        ELSE NULL
    END
{% endmacro %}
```

**Usage in model:**
```sql
SELECT
    ticker,
    date,
    {{ calculate_return('close_price', 'LAG(close_price) OVER (...)') }} AS daily_return
FROM stocks
```

### **File: `macros/generate_schema_name.sql`**

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

---

## Seeds (Reference Data)

### **File: `seeds/seed_stock_metadata.csv`**

```csv
ticker,company_name,sector,subsector,market_cap_category,listing_segment
PETR4,Petrobras,Oil & Gas,Exploration & Production,Large Cap,Novo Mercado
VALE3,Vale,Mining,Iron Ore,Large Cap,Novo Mercado
ITUB4,Itaú Unibanco,Banking,Commercial Banks,Large Cap,Level 1
BBDC4,Bradesco,Banking,Commercial Banks,Large Cap,Level 1
ABEV3,Ambev,Food & Beverage,Beverages,Large Cap,Novo Mercado
B3SA3,B3,Financial Services,Exchange,Large Cap,Novo Mercado
RENT3,Localiza,Transportation,Car Rental,Large Cap,Novo Mercado
WEGE3,WEG,Industrials,Electrical Equipment,Large Cap,Novo Mercado
SUZB3,Suzano,Materials,Paper & Pulp,Large Cap,Novo Mercado
RAIL3,Rumo,Transportation,Logistics,Mid Cap,Novo Mercado
```

**Load seeds:**
```bash
dbt seed
```

---

## Custom Tests

### **File: `tests/positive_price.sql`**

```sql
{% test positive_price(model, column_name) %}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} <= 0

{% endtest %}
```

**Usage in model YAML:**
```yaml
columns:
  - name: close_price
    tests:
      - positive_price
```

---

## Running dbt

### **Common Commands**

```bash
# Initialize dbt (first time only)
dbt init

# Test database connection
dbt debug

# Install dependencies (if using packages)
dbt deps

# Load seed files
dbt seed

# Run all models
dbt run

# Run specific model
dbt run --select stg_stocks

# Run all staging models
dbt run --select staging.*

# Run all models downstream of stg_stocks
dbt run --select stg_stocks+

# Run tests
dbt test

# Run specific test
dbt test --select stg_stocks

# Generate documentation
dbt docs generate

# Serve documentation site
dbt docs serve

# Full refresh (rebuild all incremental models)
dbt run --full-refresh
```

### **Typical Workflow**

```bash
# 1. Make changes to models
nano models/staging/stg_stocks.sql

# 2. Run just that model to test
dbt run --select stg_stocks

# 3. If it works, run everything downstream
dbt run --select stg_stocks+

# 4. Run tests
dbt test

# 5. If all tests pass, commit to git
git add models/staging/stg_stocks.sql
git commit -m "Update stock staging model"
```

---

## Summary

These dbt models demonstrate:

1. **Layered approach**: Staging → Intermediate → Marts
2. **Incremental loading**: Only process new data
3. **Data quality**: Tests, validations, cleaning
4. **Performance**: Indexes, materialization strategies
5. **Maintainability**: Clear SQL, good documentation
6. **Best practices**: Window functions, CTEs, surrogate keys

**Key concepts you'll learn:**
- Window functions (LAG, LEAD, RANK, STDDEV)
- CTEs (WITH statements)
- Incremental materialization
- Star schema dimensional modeling
- Forward-filling time series data
- Data quality testing

This is production-grade SQL that you can confidently discuss in interviews.