# Data Dictionary

This document provides detailed descriptions of all tables, columns, and data relationships in the Brazilian Financial Markets ETL Pipeline.

## Schema Overview

| Schema | Purpose | Layer |
|--------|---------|-------|
| `raw` | Landing zone for extracted data | Bronze |
| `staging` | Cleaned and validated data (dbt views) | Silver |
| `analytics` | Dimensional model for analysis | Gold |

## Raw Schema Tables

### raw.stocks

Contains daily stock price data extracted from Yahoo Finance.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Surrogate primary key |
| `ticker` | VARCHAR(10) | Stock ticker symbol (e.g., PETR4.SA) |
| `date` | DATE | Trading date |
| `open_price` | NUMERIC(12,4) | Opening price in BRL |
| `high_price` | NUMERIC(12,4) | Highest price of the day |
| `low_price` | NUMERIC(12,4) | Lowest price of the day |
| `close_price` | NUMERIC(12,4) | Closing price in BRL |
| `volume` | BIGINT | Number of shares traded |
| `adj_close` | NUMERIC(12,4) | Adjusted closing price (accounts for splits/dividends) |
| `loaded_at` | TIMESTAMP | Timestamp when row was loaded |
| `source` | VARCHAR(50) | Data source identifier |

**Constraints:**
- PRIMARY KEY: `id`
- UNIQUE: `(ticker, date)`
- CHECK: `close_price > 0`
- CHECK: `high_price >= low_price`

**Indexes:**
- `idx_stocks_ticker`
- `idx_stocks_date`
- `idx_stocks_ticker_date`

### raw.indicators

Contains macroeconomic indicator values from the Brazilian Central Bank.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Surrogate primary key |
| `indicator_code` | VARCHAR(10) | BCB indicator code |
| `date` | DATE | Reference date |
| `value` | NUMERIC(18,6) | Indicator value |
| `unit` | VARCHAR(50) | Unit of measurement |
| `frequency` | VARCHAR(20) | Update frequency (daily/monthly) |
| `loaded_at` | TIMESTAMP | Timestamp when row was loaded |

**Constraints:**
- PRIMARY KEY: `id`
- UNIQUE: `(indicator_code, date)`

**Indicator Codes:**

| Code | Name | Description | Frequency |
|------|------|-------------|-----------|
| 432 | SELIC | Base interest rate | Daily |
| 1 | USD/BRL | Dollar exchange rate | Daily |
| 12 | CDI | Interbank deposit rate | Daily |
| 433 | IPCA | Consumer price inflation | Monthly |
| 24369 | Unemployment | Unemployment rate | Monthly |
| 189 | IGP-M | Market price index | Monthly |
| 7832 | IBOV | Bovespa index points | Daily |

## Analytics Schema Tables

### analytics.dim_date

Date dimension table with calendar attributes.

| Column | Type | Description |
|--------|------|-------------|
| `date_id` | INTEGER | Primary key (YYYYMMDD format) |
| `date` | DATE | Calendar date |
| `year` | INTEGER | Year (e.g., 2024) |
| `quarter` | INTEGER | Quarter (1-4) |
| `month` | INTEGER | Month (1-12) |
| `month_name` | VARCHAR(20) | Month name (January, etc.) |
| `week_of_year` | INTEGER | ISO week number |
| `day_of_week` | INTEGER | Day of week (1=Monday, 7=Sunday) |
| `day_name` | VARCHAR(20) | Day name (Monday, etc.) |
| `is_weekend` | BOOLEAN | True if Saturday or Sunday |
| `is_month_start` | BOOLEAN | True if first day of month |
| `is_month_end` | BOOLEAN | True if last day of month |
| `is_quarter_start` | BOOLEAN | True if first day of quarter |
| `is_quarter_end` | BOOLEAN | True if last day of quarter |

### analytics.dim_stock

Stock dimension table with company information.

| Column | Type | Description |
|--------|------|-------------|
| `stock_id` | INTEGER | Surrogate primary key |
| `ticker` | VARCHAR(10) | Stock ticker symbol |
| `company_name` | VARCHAR(100) | Full company name |
| `sector` | VARCHAR(50) | Industry sector |
| `subsector` | VARCHAR(50) | Industry subsector |
| `market_cap_category` | VARCHAR(20) | Large Cap / Mid Cap / Small Cap |
| `first_trade_date` | DATE | First date with data |
| `last_trade_date` | DATE | Most recent date with data |

**Stock List:**

| Ticker | Company | Sector |
|--------|---------|--------|
| PETR4.SA | Petrobras | Oil & Gas |
| VALE3.SA | Vale | Mining |
| ITUB4.SA | Itaú Unibanco | Banking |
| BBDC4.SA | Bradesco | Banking |
| ABEV3.SA | Ambev | Food & Beverage |
| B3SA3.SA | B3 | Financial Services |
| WEGE3.SA | WEG | Industrials |
| RENT3.SA | Localiza | Transportation |
| SUZB3.SA | Suzano | Materials |
| JBSS3.SA | JBS | Food & Beverage |
| BBAS3.SA | Banco do Brasil | Banking |
| LREN3.SA | Lojas Renner | Retail |
| MGLU3.SA | Magazine Luiza | Retail |
| RADL3.SA | Raia Drogasil | Healthcare |
| CSAN3.SA | Cosan | Oil & Gas |
| GGBR4.SA | Gerdau | Materials |
| CSNA3.SA | CSN | Materials |
| HAPV3.SA | Hapvida | Healthcare |
| RAIL3.SA | Rumo | Transportation |
| EMBR3.SA | Embraer | Industrials |

### analytics.dim_indicator

Indicator dimension table with metadata.

| Column | Type | Description |
|--------|------|-------------|
| `indicator_id` | INTEGER | Surrogate primary key |
| `indicator_code` | VARCHAR(10) | BCB indicator code |
| `indicator_name` | VARCHAR(100) | Full indicator name |
| `description` | TEXT | Detailed description |
| `unit` | VARCHAR(50) | Unit of measurement |
| `frequency` | VARCHAR(20) | Update frequency |
| `source` | VARCHAR(50) | Data source |

### analytics.fact_daily_market

Central fact table containing daily market observations.

| Column | Type | Description |
|--------|------|-------------|
| `date_id` | INTEGER | FK to dim_date |
| `stock_id` | INTEGER | FK to dim_stock |
| `open_price` | NUMERIC(12,4) | Opening price |
| `high_price` | NUMERIC(12,4) | Highest price |
| `low_price` | NUMERIC(12,4) | Lowest price |
| `close_price` | NUMERIC(12,4) | Closing price |
| `volume` | BIGINT | Trading volume |
| `daily_return` | NUMERIC(10,6) | Day-over-day return |
| `monthly_return` | NUMERIC(10,6) | Month-over-month return |
| `volatility_30d` | NUMERIC(10,6) | 30-day rolling volatility |
| `annualized_volatility` | NUMERIC(10,6) | Annualized volatility |
| `selic_rate` | NUMERIC(8,4) | SELIC rate (denormalized) |
| `usd_brl` | NUMERIC(10,4) | USD/BRL exchange rate (denormalized) |
| `ipca` | NUMERIC(8,4) | IPCA inflation rate (denormalized) |
| `selic_category` | VARCHAR(20) | SELIC level category |

**Constraints:**
- PRIMARY KEY: `(date_id, stock_id)`
- FOREIGN KEY: `date_id` → `dim_date.date_id`
- FOREIGN KEY: `stock_id` → `dim_stock.stock_id`

## Calculated Metrics

### Daily Return

```sql
daily_return = (close_price - prev_close_price) / prev_close_price
```

Measures the percentage change in price from the previous trading day.

### Monthly Return

```sql
monthly_return = (close_price - price_30d_ago) / price_30d_ago
```

Measures the percentage change in price over the last 30 calendar days.

### 30-Day Volatility

```sql
volatility_30d = STDDEV(daily_return) OVER (
    PARTITION BY ticker
    ORDER BY date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
)
```

Standard deviation of daily returns over a 30-day rolling window.

### Annualized Volatility

```sql
annualized_volatility = volatility_30d * SQRT(252)
```

Volatility scaled to annual terms assuming 252 trading days per year.

### SELIC Category

```sql
CASE
    WHEN selic_rate < 7 THEN 'Low'
    WHEN selic_rate < 10 THEN 'Medium'
    WHEN selic_rate < 13 THEN 'High'
    ELSE 'Very High'
END AS selic_category
```

Categorizes SELIC rate levels for analytical grouping.

## Data Quality Rules

| Rule | Table | Description |
|------|-------|-------------|
| No null close prices | raw.stocks | Close price must always have a value |
| Positive prices | raw.stocks | All prices must be greater than zero |
| High >= Low | raw.stocks | High price must be >= low price |
| Valid date range | All tables | Dates must be between 2015-01-01 and current date |
| No duplicates | All tables | Unique constraints prevent duplicate entries |
| Referential integrity | fact_daily_market | All foreign keys must exist in dimension tables |
