# Brazilian Financial Markets ETL Pipeline - Complete Project Documentation

**Version:** 1.0
**Last Updated:** February 2025
**Author:** Dênio Barbosa Júnior
**Estimated Completion Time:** 6-8 weeks (part-time, 15 hrs/week)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Project Goals & Success Criteria](#2-project-goals--success-criteria)
3. [Architecture Overview](#3-architecture-overview)
4. [Technology Stack & Rationale](#4-technology-stack--rationale)
5. [Data Sources](#5-data-sources)
6. [Database Schema Design](#6-database-schema-design)
7. [Project Structure](#7-project-structure)
8. [Implementation Phases](#8-implementation-phases)
9. [Code Implementation](#9-code-implementation)
10. [Data Quality & Testing](#10-data-quality--testing)
11. [Deployment & Operations](#11-deployment--operations)
12. [Dashboard & Visualization](#12-dashboard--visualization)
13. [Interview Preparation Guide](#13-interview-preparation-guide)
14. [Extension Ideas](#14-extension-ideas)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Executive Summary

### **What This Project Does**

A production-grade ETL (Extract, Transform, Load) pipeline that:
- Extracts daily stock prices for 20+ Brazilian equities from Yahoo Finance
- Extracts macroeconomic indicators (SELIC rate, inflation, USD/BRL) from Brazilian Central Bank API
- Transforms raw data into clean, analytical datasets
- Loads into a dimensional data warehouse (star schema)
- Orchestrates daily updates via Apache Airflow
- Provides interactive dashboard for analysis

### **Why This Project Matters**

**For Your Career:**
- Demonstrates core Data Engineering skills (ETL, SQL, orchestration)
- Shows domain expertise (financial data, Brazilian market)
- Proves you can build production pipelines, not just scripts
- Directly relevant to banking/fintech employers

**For Learning:**
- Bridges gap between COBOL batch processing and modern data pipelines
- Teaches dimensional modeling (fact/dimension tables)
- Introduces orchestration concepts (Airflow DAGs)
- Practices data quality and monitoring

### **Expected Outcomes**

By completion, you'll have:
- GitHub repo with 3,000+ lines of documented code
- PostgreSQL database with 100k-200k rows
- Functional Airflow pipeline running daily
- Interactive Streamlit dashboard
- Comprehensive README with architecture diagrams
- Portfolio piece for Data Engineer/Data Analyst interviews

---

## 2. Project Goals & Success Criteria

### **Primary Goals**

1. **Demonstrate ETL Fundamentals**
   - ✅ Extract data from multiple sources (APIs, CSVs)
   - ✅ Transform data (cleaning, validation, enrichment)
   - ✅ Load into analytical database
   - ✅ Handle incremental updates (not just one-time loads)

2. **Show SQL/Database Proficiency**
   - ✅ Design normalized staging tables
   - ✅ Create dimensional model (star schema)
   - ✅ Write complex queries (joins, window functions, aggregations)
   - ✅ Implement data quality checks

3. **Prove Orchestration Knowledge**
   - ✅ Schedule daily pipeline runs
   - ✅ Handle dependencies between tasks
   - ✅ Implement retry logic and error handling
   - ✅ Monitor pipeline health

4. **Align With Career Goals**
   - ✅ Python-first codebase (not COBOL/C#)
   - ✅ Modern data stack (not mainframe)
   - ✅ Cloud-ready architecture (Docker containers)
   - ✅ Financial domain (leverages banking background)

### **Success Criteria**

**Minimum Viable Product (Week 6):**
- [ ] Can extract 10 years of stock data (50k+ rows)
- [ ] Can extract 10 years of BCB indicators (20k+ rows)
- [ ] Data loaded into PostgreSQL with proper schema
- [ ] Basic data quality checks (no nulls in key fields)
- [ ] README with setup instructions

**Interview-Ready (Week 8):**
- [ ] Airflow DAG running daily incremental loads
- [ ] dbt models creating fact/dimension tables
- [ ] At least 3 analytical queries demonstrating insights
- [ ] Basic Streamlit dashboard
- [ ] GitHub repo public with commit history

**Production-Grade (Week 10+):**
- [ ] Comprehensive data quality tests
- [ ] Monitoring/alerting for pipeline failures
- [ ] Performance optimizations (indexes, partitioning)
- [ ] Documentation for all modules
- [ ] CI/CD pipeline (GitHub Actions)

---

## 3. Architecture Overview

### **High-Level Architecture**

```
┌────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                          │
│        ┌───────────────┐  ┌──────────┐  ┌─────────────┐        │
│        │ Yahoo Finance │  │ BCB API  │  │ B3 (Future) │        │
│        │  REST API     │  │ REST API │  │ CSV/FTP     │        │
│        └───────┬───────┘  └────┬─────┘  └───────┬─────┘        │
└────────────────┼───────────────┼────────────────┼──────────────┘
                 │               │                │
                 ▼               ▼                ▼
┌────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION LAYER                       │
│                     Apache Airflow (DAGs)                      │
│    ┌──────────────────────────────────────────────────────┐    │
│    │                 daily_market_etl_dag                 │    │
│    │   ┌───────────┐   ┌───────────┐   ┌──────────────┐   │    │
│    │   │  extract  │-->│ transform │-->│    load      │   │    │
│    │   │  stocks   │   │   data    │   │ to warehouse │   │    │
│    │   └───────────┘   └───────────┘   └──────────────┘   │    │
│    │                                                      │    │
│    │   ┌────────────┐   ┌───────────┐   ┌────────────┐    |    │
│    │   │  extract   │-->│ transform │-->│ run dbt    │    │    │
│    │   │ indicators │   │   data    │   │   models   │    │    │
│    │   └────────────┘   └───────────┘   └────────────┘    │    │
│    └──────────────────────────────────────────────────────┘    │
└────────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                          │
│                      PostgreSQL Database                       │
│          ┌──────────────────────────────────────────┐          │
│          │           RAW SCHEMA (Staging)           │          │
│          │  ┌───────────────┐   ┌────────────────┐  │          │
│          │  │  raw_stocks   │   │ raw_indicators │  │          │
│          │  │               │   │                │  │          │
│          │  │ - Immutable   │   │ - Immutable    │  │          │
│          │  │ - Append-only │   │ - Append-only  │  │          │
│          │  └───────────────┘   └────────────────┘  │          │
│          └──────────────────────────────────────────┘          │
│                                                                │
│          ┌──────────────────────────────────────────┐          │
│          │    STAGING SCHEMA (dbt intermediate)     │          │
│          │   ┌─────────────┐   ┌────────────────┐   │          │
│          │   │  stg_stocks │   │ stg_indicators │   │          │
│          │   │             │   │                │   │          │
│          │   │ - Cleaned   │   │ - Cleaned      │   │          │
│          │   │ - Validated │   │ - Validated    │   │          │
│          │   └─────────────┘   └────────────────┘   │          │
│          └──────────────────────────────────────────┘          │
│                                                                │
│   ┌────────────────────────────────────────────────────────┐   │
│   │           ANALYTICS SCHEMA (Data Warehouse)            │   │
│   │                                                        │   │
│   │                      FACT TABLES:                      │   │
│   │       ┌──────────────────┐  ┌──────────────────┐       │   │
│   │       │ fact_daily_market│  │ fact_correlations│       │   │
│   │       │                  │  │                  │       │   │
│   │       │ - ticker_id (FK) │  │ - stock_ticker   │       │   │
│   │       │ - date_id (FK)   │  │ - indicator_code │       │   │
│   │       │ - close_price    │  │ - correlation    │       │   │
│   │       │ - volume         │  │ - period         │       │   │
│   │       │ - daily_return   │  │                  │       │   │
│   │       │ - volatility     │  │                  │       │   │
│   │       └──────────────────┘  └──────────────────┘       │   │
│   │                                                        │   │
│   │                   DIMENSION TABLES:                    │   │
│   │    ┌────────────┐  ┌───────────────┐  ┌───────────┐    │   │
│   │    │ dim_stock  │  │ dim_indicator │  │ dim_date  │    │   │
│   │    │            │  │               │  │           │    │   │
│   │    │ - stock_id │  │ - indic_id    │  │ - date_id │    │   │
│   │    │ - ticker   │  │ - code        │  │ - date    │    │   │
│   │    │ - name     │  │ - name        │  │ - year    │    │   │
│   │    │ - sector   │  │ - unit        │  │ - quarter │    │   │
│   │    │ - market   │  │ - frequency   │  │ - month   │    │   │
│   │    └────────────┘  └───────────────┘  └───────────┘    │   │
│   └────────────────────────────────────────────────────────┘   │
└────────────────────────────────┬───────────────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────────────┐
│                       PRESENTATION LAYER                       │
│                      Streamlit Dashboard                       │
│   ┌────────────────────────────────────────────────────────┐   │
│   │   - Market Overview (IBOV trend)                       │   │
│   │   - Sector Performance                                 │   │
│   │   - Macro Correlation Analysis                         │   │
│   │   - Stock Screener                                     │   │
│   └────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

### **Data Flow**

**Daily Pipeline Execution (Incremental):**

```
1. Airflow Scheduler (6:00 AM Brazil time)
   ↓
2. Extract Task: Pull yesterday's stock prices
   - Call Yahoo Finance API
   - Get data for 20 tickers
   - Store in raw_stocks table
   ↓
3. Extract Task: Pull yesterday's indicators
   - Call BCB API
   - Get SELIC, IPCA, USD/BRL
   - Store in raw_indicators table
   ↓
4. Data Quality Task: Validate new data
   - Check for nulls in required fields
   - Validate date ranges
   - Check for duplicates
   - Alert if any checks fail
   ↓
5. dbt Run: Transform data
   - Staging models (cleaning)
   - Intermediate models (calculations)
   - Mart models (dimensional tables)
   ↓
6. Success: Send notification
   - Log metrics (rows processed, errors)
   - Update monitoring dashboard
```

**Historical Backfill (One-Time):**

```
1. Manual Trigger: backfill_historical_dag
   ↓
2. Extract: Pull 10 years of data
   - Batch process in chunks (1 month at a time)
   - Respect API rate limits
   - Store all in raw tables
   ↓
3. Transform: Run full dbt refresh
   - Drop and rebuild all models
   ↓
4. Validate: Check final row counts
   - Expected: ~126k stock rows
   - Expected: ~36k indicator rows
```

### **Key Architectural Decisions**

| Decision | Rationale | Trade-off |
|----------|-----------|-----------|
| **PostgreSQL over MySQL** | Better analytical query performance, window functions, JSON support | Slightly more complex setup |
| **Airflow over Cron** | Industry standard, UI for monitoring, better dependency management | Heavier (Docker required) |
| **dbt over stored procedures** | Version control, testing framework, documentation, portability | Requires learning new tool |
| **Docker Compose over bare metal** | Reproducible environment, easier to share, matches production patterns | Requires Docker knowledge |
| **Yahoo Finance over paid APIs** | Free, no rate limits, good data quality | Less control, could change without notice |
| **Star schema over normalized** | Query performance, easier for BI tools, standard DW pattern | Some data duplication |

---

## 4. Technology Stack & Rationale

### **Core Technologies**

| Layer | Technology | Version | Why This Choice |
|-------|-----------|---------|-----------------|
| **Language** | Python | 3.11+ | Industry standard for data engineering, extensive libraries |
| **Orchestration** | Apache Airflow | 2.8+ | Most common DE orchestration tool, good for resume |
| **Database** | PostgreSQL | 15+ | Best open-source analytical database, great SQL support |
| **Transformation** | dbt Core | 1.7+ | Modern transformation tool, version control for SQL |
| **Containerization** | Docker / Compose | Latest | Reproducible environments, matches production |
| **Visualization** | Streamlit | 1.29+ | Quick Python dashboards, easy to deploy |

### **Python Libraries**

```python
# requirements.txt

# Data extraction
yfinance==0.2.35              # Yahoo Finance API wrapper
requests==2.31.0              # HTTP requests for BCB API
beautifulsoup4==4.12.2        # Optional: web scraping backup

# Data processing
pandas==2.1.4                 # DataFrame operations
numpy==1.26.2                 # Numerical operations
python-dateutil==2.8.2        # Date parsing

# Database
sqlalchemy==2.0.25            # Database ORM
psycopg2-binary==2.9.9        # PostgreSQL adapter
alembic==1.13.1               # Database migrations (optional)

# Orchestration
apache-airflow==2.8.0         # Workflow orchestration
apache-airflow-providers-postgres==5.10.0

# Transformation
dbt-core==1.7.4               # SQL transformation framework
dbt-postgres==1.7.4           # PostgreSQL adapter for dbt

# Visualization
streamlit==1.29.0             # Dashboard framework
plotly==5.18.0                # Interactive charts
altair==5.2.0                 # Declarative visualizations

# Development
pytest==7.4.3                 # Testing framework
black==23.12.1                # Code formatting
flake8==7.0.0                 # Linting
pre-commit==3.6.0             # Git hooks

# Utilities
python-dotenv==1.0.0          # Environment variables
loguru==0.7.2                 # Better logging
pydantic==2.5.3               # Data validation
```

### **Infrastructure Components**

**Docker Services (docker-compose.yml):**

```yaml
services:
  postgres:         # Database
  airflow-init:     # Initialize Airflow database
  airflow-webserver: # Airflow UI
  airflow-scheduler: # DAG execution
  streamlit:        # Dashboard (optional)
```

### **Development Tools**

| Tool | Purpose | Required? |
|------|---------|-----------|
| VS Code | Code editor | Yes |
| DBeaver / pgAdmin | Database GUI | Recommended |
| Postman | API testing | Optional |
| Git | Version control | Yes |
| GitHub | Code hosting | Yes |
| Draw.io | Architecture diagrams | Optional |

### **Why NOT Other Technologies**

| Technology | Why NOT Using It |
|------------|------------------|
| **Spark** | Overkill for <1M rows, complex setup, harder to run locally |
| **Kafka** | Real-time not needed for daily batch, too complex for portfolio |
| **Snowflake/BigQuery** | Costs money, want local-first solution |
| **Databricks** | Enterprise tool, too heavy for portfolio |
| **Excel** | Not scalable, not version controllable, not professional |
| **Pandas-only** | Need orchestration and scheduling, not just scripts |

---

## 5. Data Sources

### **5.1 Yahoo Finance (Stock Prices)**

**API Wrapper:** `yfinance` Python library

**Coverage:**
- All Brazilian stocks traded on B3 (São Paulo Stock Exchange)
- Historical data from 2000-present
- Daily OHLCV (Open, High, Low, Close, Volume)
- Adjusted prices (accounting for splits/dividends)

**Selected Tickers (20 stocks):**

| Sector | Tickers | Rationale |
|--------|---------|-----------|
| **Oil & Gas** | PETR4, PETR3 | Petrobras (most liquid Brazilian stock) |
| **Mining** | VALE3 | Vale (commodity exporter) |
| **Banking** | ITUB4, BBDC4, BBAS3 | Top 3 banks |
| **Retail** | LREN3, MGLU3, AMER3 | Consumer sector |
| **Utilities** | ELET3, CMIG4 | Electric companies |
| **Food & Bev** | ABEV3, JBSS3, BRFS3 | Ambev, JBS, BRF |
| **Infrastructure** | VALE3, CCRO3, RAIL3 | Logistics |
| **Technology** | TOTS3, B3SA3 | Totvs, B3 exchange |
| **Healthcare** | RADL3, HAPV3 | Raia Drogasil, Hapvida |
| **Chemicals** | WEGE3, SUZB3 | WEG, Suzano |

**Data Fields:**
```python
{
    'Date': '2024-02-06',
    'Open': 32.45,
    'High': 33.12,
    'Low': 32.30,
    'Close': 32.98,
    'Volume': 8234567,
    'Adj Close': 32.98  # Adjusted for corporate actions
}
```

**Rate Limits:** None (publicly available)

**Data Quality:**
- ✅ High reliability (source: B3 exchange)
- ⚠️ Missing data on holidays/weekends (expected)
- ⚠️ Occasional gaps for low-liquidity stocks
- ✅ Data available with 1-day delay (T+1)

### **5.2 Brazilian Central Bank API (BCB)**

**API Endpoint:** `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados`

**Documentation:** https://www3.bcb.gov.br/sgspub

**Selected Indicators:**

| Series ID | Indicator Name | Frequency | Unit | Description |
|-----------|---------------|-----------|------|-------------|
| **432** | SELIC | Daily | % p.a. | Brazilian base interest rate (most important macro indicator) |
| **433** | IPCA | Monthly | % | Consumer inflation index |
| **1** | USD/BRL | Daily | BRL | US Dollar exchange rate |
| **4189** | GDP | Quarterly | BRL billions | Gross Domestic Product |
| **24363** | Unemployment | Monthly | % | National unemployment rate |
| **11** | CDI | Daily | % p.a. | Interbank deposit rate |
| **189** | IGP-M | Monthly | % | General price index |

**API Request Example:**
```bash
GET https://api.bcb.gov.br/dados/serie/bcdata.sgs.432/dados?formato=json&dataInicial=01/01/2015&dataFinal=31/12/2024
```

**Response Format:**
```json
[
  {
    "data": "02/01/2015",
    "valor": "11.65"
  },
  {
    "data": "05/01/2015",
    "valor": "11.65"
  }
]
```

**Rate Limits:**
- Soft limit: ~100 requests/minute
- Recommended: Add 1-second delay between requests

**Data Quality:**
- ✅ Official government data (highest quality)
- ✅ No missing data for daily indicators
- ⚠️ Monthly/quarterly indicators have gaps (expected)
- ✅ Data available with 1-day delay for daily indicators

### **5.3 Optional: B3 Index Data**

**Future Enhancement (Week 10+)**

**Source:** B3 public FTP or CSV downloads

**Indicators:**
- IBOV (Bovespa index) - main Brazilian stock index
- IBRX-100 (top 100 stocks)
- Sector indices (financial, utilities, etc.)

**Why Optional:**
- Yahoo Finance provides IBOV as ticker `^BVSP`
- Can use that for MVP
- Direct B3 data is more accurate but requires FTP setup

---

## 6. Database Schema Design

### **6.1 Schema Organization**

**Three-Schema Pattern:**

```sql
-- Schema 1: Raw (Immutable staging)
CREATE SCHEMA raw;

-- Schema 2: Staging (dbt intermediate)
CREATE SCHEMA staging;

-- Schema 3: Analytics (Data warehouse)
CREATE SCHEMA analytics;
```

**Why Three Schemas?**

| Schema | Purpose | Characteristics |
|--------|---------|-----------------|
| `raw` | Landing zone | Append-only, immutable, matches source format exactly |
| `staging` | Cleaning layer | dbt models, validated, no business logic yet |
| `analytics` | Consumption | Star schema, denormalized, optimized for queries |

### **6.2 Raw Schema (Landing Zone)**

**Table: `raw.stocks`**

```sql
CREATE TABLE raw.stocks (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price NUMERIC(12, 4),
    high_price NUMERIC(12, 4),
    low_price NUMERIC(12, 4),
    close_price NUMERIC(12, 4),
    volume BIGINT,
    adj_close NUMERIC(12, 4),
    loaded_at TIMESTAMP DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'yahoo_finance',

    -- Ensure no duplicate ticker/date combinations
    CONSTRAINT uq_stocks_ticker_date UNIQUE (ticker, date)
);

-- Indexes for query performance
CREATE INDEX idx_stocks_ticker ON raw.stocks(ticker);
CREATE INDEX idx_stocks_date ON raw.stocks(date);
CREATE INDEX idx_stocks_ticker_date ON raw.stocks(ticker, date);
CREATE INDEX idx_stocks_loaded_at ON raw.stocks(loaded_at);
```

**Design Decisions:**
- ✅ `BIGSERIAL` for unlimited growth
- ✅ `NUMERIC` for exact decimal precision (financial data)
- ✅ `loaded_at` for audit trail (when was this row inserted?)
- ✅ Unique constraint prevents duplicate loads
- ✅ Source column for multi-source tracking

**Table: `raw.indicators`**

```sql
CREATE TABLE raw.indicators (
    id BIGSERIAL PRIMARY KEY,
    indicator_code VARCHAR(20) NOT NULL,
    indicator_name VARCHAR(100),
    date DATE NOT NULL,
    value NUMERIC(18, 6),
    unit VARCHAR(20),
    frequency VARCHAR(20),  -- 'daily', 'monthly', 'quarterly'
    loaded_at TIMESTAMP DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'bcb_api',

    CONSTRAINT uq_indicators_code_date UNIQUE (indicator_code, date)
);

CREATE INDEX idx_indicators_code ON raw.indicators(indicator_code);
CREATE INDEX idx_indicators_date ON raw.indicators(date);
CREATE INDEX idx_indicators_code_date ON raw.indicators(indicator_code, date);
```

**Sample Data:**

```sql
-- raw.stocks
| id | ticker | date       | close_price | volume    | loaded_at           |
|----|--------|------------|-------------|-----------|---------------------|
| 1  | PETR4  | 2024-02-05 | 38.45       | 45678901  | 2024-02-06 06:30:00 |
| 2  | VALE3  | 2024-02-05 | 67.23       | 23456789  | 2024-02-06 06:30:15 |

-- raw.indicators
| id | indicator_code | date       | value  | loaded_at           |
|----|----------------|------------|--------|---------------------|
| 1  | 432            | 2024-02-05 | 11.25  | 2024-02-06 06:35:00 |
| 2  | 1              | 2024-02-05 | 4.9823 | 2024-02-06 06:35:10 |
```

### **6.3 Staging Schema (dbt Models)**

**These are dbt models, not physical tables**

**Model: `staging.stg_stocks`**

```sql
-- models/staging/stg_stocks.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'stocks') }}
),

cleaned AS (
    SELECT
        ticker,
        date,
        close_price,
        volume,
        loaded_at,

        -- Data quality checks
        CASE
            WHEN close_price <= 0 THEN NULL
            ELSE close_price
        END AS clean_close_price,

        CASE
            WHEN volume < 0 THEN 0
            ELSE volume
        END AS clean_volume

    FROM source
    WHERE date >= '2015-01-01'  -- Filter old/irrelevant data
      AND close_price IS NOT NULL
)

SELECT * FROM cleaned
```

**Model: `staging.stg_indicators`**

```sql
-- models/staging/stg_indicators.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'indicators') }}
),

cleaned AS (
    SELECT
        indicator_code,
        indicator_name,
        date,
        value,
        frequency,

        -- Standardize indicator names
        CASE indicator_code
            WHEN '432' THEN 'SELIC'
            WHEN '433' THEN 'IPCA'
            WHEN '1' THEN 'USD_BRL'
            ELSE indicator_name
        END AS standard_name

    FROM source
    WHERE date >= '2015-01-01'
      AND value IS NOT NULL
)

SELECT * FROM cleaned
```

### **6.4 Analytics Schema (Data Warehouse)**

**Dimensional Model (Star Schema):**

```
         ┌──────────────┐
         │  dim_date    │
         │              │
         │ - date_id PK │
         │ - date       │
         │ - year       │
         │ - quarter    │
         │ - month      │
         │ - day        │
         │ - is_holiday │
         └──────┬───────┘
                │
                │
    ┌───────────┼───────────┐
    │           │           │
    ▼           ▼           ▼
┌─────────┐ ┌──────────────────────┐ ┌──────────────┐
│dim_stock│ │ fact_daily_market    │ │dim_indicator │
│         │ │                      │ │              │
│stock_id │◄┤ - date_id (FK)      │►│ indicator_id │
│ticker   │ │ - stock_id (FK)      │ │ - code       │
│name     │ │ - indicator_id (FK)  │ │ - name       │
│sector   │ │ - close_price        │ │ - unit       │
│market   │ │ - volume             │ │ - frequency  │
└─────────┘ │ - daily_return       │ └──────────────┘
            │ - selic_rate         │
            │ - usd_brl            │
            │ - inflation          │
            └──────────────────────┘
```

**Dimension: `analytics.dim_date`**

```sql
CREATE TABLE analytics.dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100)
);

-- Populate with date range (2015-2025)
-- This is done via dbt seed or SQL script
```

**Dimension: `analytics.dim_stock`**

```sql
CREATE TABLE analytics.dim_stock (
    stock_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(200),
    sector VARCHAR(100),
    subsector VARCHAR(100),
    market_cap_category VARCHAR(20),  -- 'Large', 'Mid', 'Small'
    listing_segment VARCHAR(50),      -- 'Novo Mercado', 'Level 1', etc.
    first_traded_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Sample data
INSERT INTO analytics.dim_stock (ticker, company_name, sector) VALUES
('PETR4', 'Petrobras', 'Oil & Gas'),
('VALE3', 'Vale', 'Mining'),
('ITUB4', 'Itaú Unibanco', 'Banking');
```

**Dimension: `analytics.dim_indicator`**

```sql
CREATE TABLE analytics.dim_indicator (
    indicator_id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(20) UNIQUE NOT NULL,
    indicator_name VARCHAR(100),
    description TEXT,
    unit VARCHAR(20),
    frequency VARCHAR(20),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO analytics.dim_indicator (indicator_code, indicator_name, unit, frequency) VALUES
('432', 'SELIC', '% p.a.', 'daily'),
('433', 'IPCA', '%', 'monthly'),
('1', 'USD/BRL', 'BRL', 'daily');
```

**Fact Table: `analytics.fact_daily_market`**

```sql
CREATE TABLE analytics.fact_daily_market (
    market_id BIGSERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES analytics.dim_date(date_id),
    stock_id INTEGER NOT NULL REFERENCES analytics.dim_stock(stock_id),

    -- Stock metrics
    close_price NUMERIC(12, 4) NOT NULL,
    volume BIGINT,
    daily_return NUMERIC(10, 6),  -- (today - yesterday) / yesterday
    volatility_7d NUMERIC(10, 6), -- 7-day rolling std dev
    volatility_30d NUMERIC(10, 6),

    -- Market context (denormalized for query performance)
    selic_rate NUMERIC(10, 6),
    usd_brl NUMERIC(10, 6),
    ipca_mtd NUMERIC(10, 6),  -- Month-to-date inflation

    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),

    -- Ensure one row per stock per day
    CONSTRAINT uq_market_date_stock UNIQUE (date_id, stock_id)
);

-- Indexes for analytical queries
CREATE INDEX idx_fact_date ON analytics.fact_daily_market(date_id);
CREATE INDEX idx_fact_stock ON analytics.fact_daily_market(stock_id);
CREATE INDEX idx_fact_date_stock ON analytics.fact_daily_market(date_id, stock_id);
CREATE INDEX idx_fact_return ON analytics.fact_daily_market(daily_return);
```

**Why This Schema?**

| Design Choice | Benefit |
|---------------|---------|
| **Surrogate keys** (stock_id, date_id) | Faster joins than VARCHAR/DATE |
| **Denormalized indicators** in fact table | Avoids multiple joins for common queries |
| **Separate dimensions** | Easy to add attributes without touching fact |
| **No DELETE operations** | Append-only for historical accuracy |
| **Indexes on FK columns** | Query performance |

### **6.5 Analytical Views**

**View: `analytics.vw_stock_performance`**

```sql
CREATE VIEW analytics.vw_stock_performance AS
SELECT
    d.date,
    s.ticker,
    s.company_name,
    s.sector,
    f.close_price,
    f.daily_return,
    f.volume,
    f.selic_rate,
    f.usd_brl,

    -- 30-day moving average
    AVG(f.close_price) OVER (
        PARTITION BY f.stock_id
        ORDER BY d.date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ma_30d,

    -- Year-to-date return
    (f.close_price / FIRST_VALUE(f.close_price) OVER (
        PARTITION BY f.stock_id, d.year
        ORDER BY d.date
    ) - 1) * 100 AS ytd_return_pct

FROM analytics.fact_daily_market f
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
JOIN analytics.dim_date d ON f.date_id = d.date_id;
```

---

## 7. Project Structure

### **Complete Directory Tree**

```
brazilian-market-etl/
│
├── README.md                          # Project overview, setup instructions
├── LICENSE                            # MIT or Apache 2.0
├── .gitignore                         # Python, IDE, data files
├── .env.example                       # Environment variables template
├── requirements.txt                   # Python dependencies
├── docker-compose.yml                 # Container orchestration
├── Makefile                           # Common commands (optional)
│
├── docs/                              # Documentation
│   ├── architecture.md                # Architecture decisions
│   ├── setup_guide.md                 # Step-by-step setup
│   ├── data_dictionary.md             # All tables/columns explained
│   └── diagrams/
│       ├── architecture.png
│       └── erd.png                    # Entity relationship diagram
│
├── airflow/                           # Orchestration
│   ├── dags/
│   │   ├── daily_market_etl.py        # Main incremental DAG
│   │   ├── backfill_historical.py     # One-time historical load
│   │   └── data_quality_checks.py     # Validation DAG
│   ├── plugins/                       # Custom operators (if needed)
│   └── config/
│       └── airflow.cfg                # Airflow configuration
│
├── database/                          # Database setup
│   ├── init/
│   │   ├── 01_create_schemas.sql      # Schema creation
│   │   ├── 02_create_raw_tables.sql   # Raw layer tables
│   │   └── 03_create_analytics_tables.sql # Analytics layer
│   ├── seeds/
│   │   ├── dim_stock_seed.csv         # Stock metadata
│   │   └── dim_date_seed.csv          # Date dimension (2015-2030)
│   └── migrations/                    # Schema changes over time (Alembic)
│
├── extract/                           # Data extraction layer
│   ├── __init__.py
│   ├── base_extractor.py              # Base class for all extractors
│   ├── stock_extractor.py             # Yahoo Finance logic
│   ├── bcb_extractor.py               # BCB API logic
│   ├── config.py                      # Extraction configurations
│   └── utils.py                       # Rate limiting, retry logic
│
├── transform/                         # Python transformations (if not using dbt)
│   ├── __init__.py
│   ├── data_quality.py                # Validation functions
│   └── calculations.py                # Business logic (returns, volatility)
│
├── load/                              # Data loading layer
│   ├── __init__.py
│   └── db_loader.py                   # Database connection, bulk insert
│
├── dbt_project/                       # dbt transformation framework
│   ├── dbt_project.yml                # Project configuration
│   ├── profiles.yml                   # Database connection profiles
│   │
│   ├── models/
│   │   ├── staging/                   # Layer 1: Clean raw data
│   │   │   ├── _staging.yml           # Model documentation
│   │   │   ├── stg_stocks.sql         # Clean stock data
│   │   │   └── stg_indicators.sql     # Clean indicator data
│   │   │
│   │   ├── intermediate/              # Layer 2: Business logic
│   │   │   ├── _intermediate.yml
│   │   │   ├── int_stock_returns.sql  # Calculate returns
│   │   │   ├── int_stock_volatility.sql # Calculate volatility
│   │   │   └── int_joined_market.sql  # Join stocks + indicators
│   │   │
│   │   └── marts/                     # Layer 3: Final dimensional model
│   │       ├── _marts.yml
│   │       ├── dim_date.sql           # Date dimension
│   │       ├── dim_stock.sql          # Stock dimension
│   │       ├── dim_indicator.sql      # Indicator dimension
│   │       └── fact_daily_market.sql  # Market fact table
│   │
│   ├── tests/                         # Data quality tests
│   │   ├── assert_no_nulls_in_facts.sql
│   │   ├── assert_no_future_dates.sql
│   │   └── assert_positive_prices.sql
│   │
│   ├── macros/                        # Reusable SQL functions
│   │   ├── calculate_return.sql
│   │   └── calculate_volatility.sql
│   │
│   └── seeds/                         # Static reference data
│       └── stock_metadata.csv
│
├── dashboard/                         # Streamlit visualization
│   ├── app.py                         # Main dashboard entry point
│   ├── pages/
│   │   ├── 1_Market_Overview.py       # IBOV trends, top movers
│   │   ├── 2_Sector_Analysis.py       # Sector performance
│   │   ├── 3_Macro_Correlation.py     # Stock vs indicators
│   │   └── 4_Stock_Screener.py        # Filter stocks by criteria
│   ├── components/
│   │   ├── charts.py                  # Reusable chart functions
│   │   └── queries.py                 # SQL queries for dashboard
│   └── config.py                      # Dashboard settings
│
├── tests/                             # Unit & integration tests
│   ├── __init__.py
│   ├── test_stock_extractor.py
│   ├── test_bcb_extractor.py
│   ├── test_data_quality.py
│   └── test_db_loader.py
│
├── scripts/                           # Utility scripts
│   ├── setup_db.sh                    # Initialize database
│   ├── backfill_data.py               # Run historical backfill
│   ├── validate_data.py               # Manual data validation
│   └── generate_sample_data.py        # Create test dataset
│
└── .github/                           # CI/CD (optional, Week 10+)
    └── workflows/
        └── ci.yml                     # Run tests on PR
```

### **File Sizes (Approximate)**

```
README.md              ~3 KB
requirements.txt       ~1 KB
docker-compose.yml     ~5 KB
extract/ (all files)   ~15 KB
dbt_project/ (all)     ~30 KB
dashboard/app.py       ~20 KB
tests/ (all files)     ~10 KB
```

**Total:** ~100 KB of code (excluding data, dependencies)

---

## 8. Implementation Phases

### **Phase 1: Foundation (Week 1-2)**

**Goal:** Set up infrastructure and extract first data

**Tasks:**
- [ ] Create GitHub repository
- [ ] Set up project structure (copy directory tree)
- [ ] Write `docker-compose.yml`
- [ ] Write `database/init/` SQL scripts
- [ ] Start PostgreSQL container
- [ ] Verify database connection

**Deliverables:**
- Empty project structure
- Running PostgreSQL database
- Raw tables created

**Time:** 8-12 hours

---

### **Phase 2: Data Extraction (Week 2-3)**

**Goal:** Pull historical data into raw tables

**Tasks:**
- [ ] Implement `extract/stock_extractor.py`
- [ ] Implement `extract/bcb_extractor.py`
- [ ] Add error handling and retry logic
- [ ] Add logging
- [ ] Run backfill (10 years of data)
- [ ] Validate row counts

**Deliverables:**
- Working extraction scripts
- 50k+ stock rows in database
- 20k+ indicator rows in database
- Extraction logs

**Time:** 12-15 hours

---

### **Phase 3: Data Transformation (Week 3-5)**

**Goal:** Transform raw data into analytics-ready format

**Tasks:**
- [ ] Install dbt Core
- [ ] Set up `dbt_project/`
- [ ] Write staging models (`stg_stocks`, `stg_indicators`)
- [ ] Write intermediate models (calculations)
- [ ] Write mart models (dimensional tables)
- [ ] Add dbt tests
- [ ] Run `dbt run` and verify results

**Deliverables:**
- Complete dbt project
- Populated dimensional model
- Data quality tests passing

**Time:** 15-20 hours

---

### **Phase 4: Orchestration (Week 5-6)**

**Goal:** Automate daily pipeline execution

**Tasks:**
- [ ] Set up Airflow (Docker)
- [ ] Write `airflow/dags/daily_market_etl.py`
- [ ] Configure task dependencies
- [ ] Add error notifications
- [ ] Test incremental loads
- [ ] Schedule daily run (6 AM)

**Deliverables:**
- Working Airflow DAG
- Daily incremental updates
- Airflow UI accessible

**Time:** 10-12 hours

---

### **Phase 5: Documentation & Testing (Week 6-7)**

**Goal:** Make project interview-ready

**Tasks:**
- [ ] Write comprehensive README
- [ ] Add code comments
- [ ] Create architecture diagrams
- [ ] Write unit tests
- [ ] Add data quality validation script
- [ ] Create sample queries demonstrating insights

**Deliverables:**
- Professional README with setup instructions
- Architecture diagrams
- Test coverage >70%
- Sample analytical queries

**Time:** 8-10 hours

---

### **Phase 6: Visualization (Week 7-8)**

**Goal:** Build interactive dashboard

**Tasks:**
- [ ] Set up Streamlit
- [ ] Create market overview page
- [ ] Create sector analysis page
- [ ] Create macro correlation charts
- [ ] Add stock screener/filter
- [ ] Deploy locally (or Streamlit Cloud)

**Deliverables:**
- Multi-page Streamlit dashboard
- 5-10 interactive visualizations
- Screenshots for README

**Time:** 12-15 hours

---

### **Total Time Estimate: 65-84 hours**

**At 15 hours/week:** 4.5-5.5 weeks
**At 20 hours/week:** 3.5-4 weeks

---

## 9. Code Implementation

### **9.1 Environment Setup**

**File: `.env.example`**

```bash
# Database Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=brazilian_market
POSTGRES_USER=dataeng
POSTGRES_PASSWORD=dataeng123

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://dataeng:dataeng123@postgres/brazilian_market
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW_UID=50000

# Data Extraction
STOCK_TICKERS=PETR4.SA,VALE3.SA,ITUB4.SA,BBDC4.SA,ABEV3.SA
BCB_INDICATORS=432,433,1,4189,24363
START_DATE=2015-01-01

# Logging
LOG_LEVEL=INFO
LOG_DIR=./logs
```

**File: `docker-compose.yml`**

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    container_name: brazilian_market_db
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./database/init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow-init:
    image: apache/airflow:2.8.0-python3.11
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/${POSTGRES_DB}
    entrypoint: /bin/bash
    command: >
      -c "airflow db init &&
          airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"
    volumes:
      - ./airflow/dags:/opt/airflow/dags

  airflow-webserver:
    image: apache/airflow:2.8.0-python3.11
    container_name: airflow_webserver
    depends_on:
      - postgres
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/${POSTGRES_DB}
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    ports:
      - "8080:8080"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./extract:/opt/airflow/extract
      - ./logs:/opt/airflow/logs
    command: webserver
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5

  airflow-scheduler:
    image: apache/airflow:2.8.0-python3.11
    container_name: airflow_scheduler
    depends_on:
      - postgres
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/${POSTGRES_DB}
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./extract:/opt/airflow/extract
      - ./logs:/opt/airflow/logs
    command: scheduler

volumes:
  postgres_data:
```

### **9.2 Database Initialization**

**File: `database/init/01_create_schemas.sql`**

```sql
-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Set search path
ALTER DATABASE brazilian_market SET search_path TO raw, staging, analytics, public;
```

**File: `database/init/02_create_raw_tables.sql`**

```sql
-- Raw stock prices
CREATE TABLE raw.stocks (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price NUMERIC(12, 4),
    high_price NUMERIC(12, 4),
    low_price NUMERIC(12, 4),
    close_price NUMERIC(12, 4),
    volume BIGINT,
    adj_close NUMERIC(12, 4),
    loaded_at TIMESTAMP DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'yahoo_finance',
    CONSTRAINT uq_stocks_ticker_date UNIQUE (ticker, date)
);

CREATE INDEX idx_stocks_ticker ON raw.stocks(ticker);
CREATE INDEX idx_stocks_date ON raw.stocks(date);
CREATE INDEX idx_stocks_ticker_date ON raw.stocks(ticker, date);

-- Raw indicators
CREATE TABLE raw.indicators (
    id BIGSERIAL PRIMARY KEY,
    indicator_code VARCHAR(20) NOT NULL,
    indicator_name VARCHAR(100),
    date DATE NOT NULL,
    value NUMERIC(18, 6),
    unit VARCHAR(20),
    frequency VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'bcb_api',
    CONSTRAINT uq_indicators_code_date UNIQUE (indicator_code, date)
);

CREATE INDEX idx_indicators_code ON raw.indicators(indicator_code);
CREATE INDEX idx_indicators_date ON raw.indicators(date);
CREATE INDEX idx_indicators_code_date ON raw.indicators(indicator_code, date);
```

**File: `database/init/03_create_analytics_tables.sql`**

```sql
-- Dimension: Date
CREATE TABLE analytics.dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    day INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(100)
);

-- Dimension: Stock
CREATE TABLE analytics.dim_stock (
    stock_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(200),
    sector VARCHAR(100),
    subsector VARCHAR(100),
    market_cap_category VARCHAR(20),
    listing_segment VARCHAR(50),
    first_traded_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Dimension: Indicator
CREATE TABLE analytics.dim_indicator (
    indicator_id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(20) UNIQUE NOT NULL,
    indicator_name VARCHAR(100),
    description TEXT,
    unit VARCHAR(20),
    frequency VARCHAR(20),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Fact: Daily Market
CREATE TABLE analytics.fact_daily_market (
    market_id BIGSERIAL PRIMARY KEY,
    date_id INTEGER NOT NULL REFERENCES analytics.dim_date(date_id),
    stock_id INTEGER NOT NULL REFERENCES analytics.dim_stock(stock_id),
    close_price NUMERIC(12, 4) NOT NULL,
    volume BIGINT,
    daily_return NUMERIC(10, 6),
    volatility_7d NUMERIC(10, 6),
    volatility_30d NUMERIC(10, 6),
    selic_rate NUMERIC(10, 6),
    usd_brl NUMERIC(10, 6),
    ipca_mtd NUMERIC(10, 6),
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_market_date_stock UNIQUE (date_id, stock_id)
);

CREATE INDEX idx_fact_date ON analytics.fact_daily_market(date_id);
CREATE INDEX idx_fact_stock ON analytics.fact_daily_market(stock_id);
CREATE INDEX idx_fact_date_stock ON analytics.fact_daily_market(date_id, stock_id);
```

### **9.3 Stock Extraction**

**File: `extract/stock_extractor.py`**

```python
"""
Stock data extractor using Yahoo Finance API
"""
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import logging
from typing import List, Optional
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockExtractor:
    """Extract stock price data from Yahoo Finance"""

    # Top 20 Brazilian stocks by liquidity
    DEFAULT_TICKERS = [
        'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'ABEV3.SA',
        'B3SA3.SA', 'RENT3.SA', 'WEGE3.SA', 'SUZB3.SA', 'RAIL3.SA',
        'JBSS3.SA', 'RADL3.SA', 'EMBR3.SA', 'GGBR4.SA', 'HAPV3.SA',
        'BRFS3.SA', 'CSAN3.SA', 'LREN3.SA', 'MGLU3.SA', 'VBBR3.SA'
    ]

    def __init__(self, db_connection_string: str, tickers: Optional[List[str]] = None):
        """
        Initialize extractor

        Args:
            db_connection_string: PostgreSQL connection string
            tickers: List of stock tickers (default: top 20 Brazilian stocks)
        """
        self.engine = create_engine(db_connection_string)
        self.tickers = tickers or self.DEFAULT_TICKERS

    def extract_historical(
        self,
        start_date: str = '2015-01-01',
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract historical stock data for all tickers

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to today

        Returns:
            DataFrame with columns: ticker, date, open_price, high_price,
                                   low_price, close_price, volume, adj_close
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        logger.info(f"Extracting historical data from {start_date} to {end_date}")
        logger.info(f"Tickers: {len(self.tickers)} stocks")

        all_data = []
        failed_tickers = []

        for idx, ticker in enumerate(self.tickers, 1):
            try:
                logger.info(f"[{idx}/{len(self.tickers)}] Downloading {ticker}...")

                # Download data
                stock = yf.Ticker(ticker)
                df = stock.history(start=start_date, end=end_date)

                if df.empty:
                    logger.warning(f"No data returned for {ticker}")
                    failed_tickers.append(ticker)
                    continue

                # Transform data
                df = df.reset_index()
                df['ticker'] = ticker.replace('.SA', '')  # Remove exchange suffix

                # Rename columns to match database schema
                df = df.rename(columns={
                    'Date': 'date',
                    'Open': 'open_price',
                    'High': 'high_price',
                    'Low': 'low_price',
                    'Close': 'close_price',
                    'Volume': 'volume'
                })

                # Add adj_close (Yahoo provides this)
                if 'Adj Close' in df.columns:
                    df['adj_close'] = df['Adj Close']
                else:
                    df['adj_close'] = df['close_price']

                # Select final columns
                df = df[[
                    'ticker', 'date', 'open_price', 'high_price',
                    'low_price', 'close_price', 'volume', 'adj_close'
                ]]

                # Data quality: Remove rows with null close_price
                initial_rows = len(df)
                df = df.dropna(subset=['close_price'])
                if len(df) < initial_rows:
                    logger.warning(
                        f"{ticker}: Dropped {initial_rows - len(df)} rows with null prices"
                    )

                all_data.append(df)
                logger.info(f"{ticker}: Extracted {len(df)} rows")

                # Rate limiting: Small delay between requests
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error downloading {ticker}: {e}")
                failed_tickers.append(ticker)
                continue

        if not all_data:
            raise ValueError("No data extracted for any ticker")

        # Combine all dataframes
        combined_df = pd.concat(all_data, ignore_index=True)

        logger.info(f"Extraction complete:")
        logger.info(f"  - Total rows: {len(combined_df)}")
        logger.info(f"  - Successful tickers: {len(all_data)}")
        logger.info(f"  - Failed tickers: {len(failed_tickers)}")
        if failed_tickers:
            logger.warning(f"  - Failed: {failed_tickers}")

        return combined_df

    def extract_incremental(self, lookback_days: int = 5) -> pd.DataFrame:
        """
        Extract recent data (for daily updates)

        Args:
            lookback_days: How many days to look back (default 5 to catch weekends)

        Returns:
            DataFrame with recent stock data
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        return self.extract_historical(
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )

    def load_to_database(self, df: pd.DataFrame) -> int:
        """
        Load data to PostgreSQL (upsert on conflict)

        Args:
            df: DataFrame with stock data

        Returns:
            Number of rows inserted/updated
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to load")
            return 0

        logger.info(f"Loading {len(df)} rows to database...")

        # Convert date column to proper format
        df['date'] = pd.to_datetime(df['date']).dt.date

        try:
            # Insert data (PostgreSQL will handle duplicates via UNIQUE constraint)
            rows_inserted = df.to_sql(
                name='stocks',
                con=self.engine,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

            logger.info(f"Successfully loaded {rows_inserted} rows")
            return rows_inserted or len(df)

        except Exception as e:
            # If constraint violation (duplicate), that's okay for incremental loads
            if 'duplicate key value violates unique constraint' in str(e):
                logger.info("Some rows already exist (expected for incremental load)")
                return 0
            else:
                logger.error(f"Error loading to database: {e}")
                raise

# Usage example
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()

    DB_CONN = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

    extractor = StockExtractor(DB_CONN)

    # Historical backfill
    data = extractor.extract_historical(start_date='2015-01-01')
    extractor.load_to_database(data)
```

### **9.4 BCB Indicator Extraction**

**File: `extract/bcb_extractor.py`**

```python
"""
Brazilian Central Bank (BCB) indicator extractor
"""
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
from typing import Dict, Optional
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BCBExtractor:
    """Extract economic indicators from BCB API"""

    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"

    # Key Brazilian economic indicators
    INDICATORS = {
        '432': {'name': 'SELIC', 'unit': '% p.a.', 'frequency': 'daily'},
        '433': {'name': 'IPCA', 'unit': '%', 'frequency': 'monthly'},
        '1': {'name': 'USD_BRL', 'unit': 'BRL', 'frequency': 'daily'},
        '4189': {'name': 'GDP', 'unit': 'BRL billions', 'frequency': 'quarterly'},
        '24363': {'name': 'Unemployment', 'unit': '%', 'frequency': 'monthly'},
        '11': {'name': 'CDI', 'unit': '% p.a.', 'frequency': 'daily'},
        '189': {'name': 'IGP-M', 'unit': '%', 'frequency': 'monthly'}
    }

    def __init__(self, db_connection_string: str):
        """
        Initialize extractor

        Args:
            db_connection_string: PostgreSQL connection string
        """
        self.engine = create_engine(db_connection_string)

    def extract_indicator(
        self,
        code: str,
        start_date: str = '01/01/2015',
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract single indicator from BCB API

        Args:
            code: Indicator series code (e.g., '432' for SELIC)
            start_date: Start date (DD/MM/YYYY)
            end_date: End date (DD/MM/YYYY), defaults to today

        Returns:
            DataFrame with columns: indicator_code, indicator_name, date, value
        """
        if end_date is None:
            end_date = datetime.now().strftime('%d/%m/%Y')

        indicator_info = self.INDICATORS.get(code, {})
        indicator_name = indicator_info.get('name', f'Indicator_{code}')

        url = self.BASE_URL.format(code=code)
        params = {
            'formato': 'json',
            'dataInicial': start_date,
            'dataFinal': end_date
        }

        try:
            logger.info(f"Extracting {indicator_name} (code {code})...")

            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data:
                logger.warning(f"No data returned for indicator {code}")
                return pd.DataFrame()

            # Transform to DataFrame
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
            df['value'] = pd.to_numeric(df['valor'], errors='coerce')

            # Add metadata
            df['indicator_code'] = code
            df['indicator_name'] = indicator_name
            df['unit'] = indicator_info.get('unit', '')
            df['frequency'] = indicator_info.get('frequency', '')

            # Select final columns
            df = df[[
                'indicator_code', 'indicator_name', 'date',
                'value', 'unit', 'frequency'
            ]]

            # Remove rows with null values
            initial_rows = len(df)
            df = df.dropna(subset=['value'])
            if len(df) < initial_rows:
                logger.warning(
                    f"{indicator_name}: Dropped {initial_rows - len(df)} rows with null values"
                )

            logger.info(f"{indicator_name}: Extracted {len(df)} rows")
            return df

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error for indicator {code}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error extracting indicator {code}: {e}")
            return pd.DataFrame()

    def extract_all_indicators(
        self,
        start_date: str = '01/01/2015',
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract all configured indicators

        Args:
            start_date: Start date (DD/MM/YYYY)
            end_date: End date (DD/MM/YYYY)

        Returns:
            DataFrame with all indicator data
        """
        logger.info(f"Extracting {len(self.INDICATORS)} indicators...")

        all_data = []
        failed_indicators = []

        for code in self.INDICATORS.keys():
            df = self.extract_indicator(code, start_date, end_date)

            if not df.empty:
                all_data.append(df)
            else:
                failed_indicators.append(code)

            # Rate limiting: 1 second between requests
            time.sleep(1)

        if not all_data:
            raise ValueError("No indicator data extracted")

        combined_df = pd.concat(all_data, ignore_index=True)

        logger.info(f"Extraction complete:")
        logger.info(f"  - Total rows: {len(combined_df)}")
        logger.info(f"  - Successful indicators: {len(all_data)}")
        if failed_indicators:
            logger.warning(f"  - Failed indicators: {failed_indicators}")

        return combined_df

    def load_to_database(self, df: pd.DataFrame) -> int:
        """
        Load data to PostgreSQL

        Args:
            df: DataFrame with indicator data

        Returns:
            Number of rows inserted
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to load")
            return 0

        logger.info(f"Loading {len(df)} rows to database...")

        # Convert date column to proper format
        df['date'] = pd.to_datetime(df['date']).dt.date

        try:
            rows_inserted = df.to_sql(
                name='indicators',
                con=self.engine,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )

            logger.info(f"Successfully loaded {rows_inserted} rows")
            return rows_inserted or len(df)

        except Exception as e:
            if 'duplicate key value violates unique constraint' in str(e):
                logger.info("Some rows already exist (expected for incremental load)")
                return 0
            else:
                logger.error(f"Error loading to database: {e}")
                raise

# Usage example
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()

    DB_CONN = (
        f"postgresql://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

    extractor = BCBExtractor(DB_CONN)

    # Historical backfill
    data = extractor.extract_all_indicators(start_date='01/01/2015')
    extractor.load_to_database(data)
```

### **9.5 Airflow DAG**

**File: `airflow/dags/daily_market_etl.py`**

```python
"""
Daily Market ETL DAG

Runs daily at 6:00 AM Brazil time to extract yesterday's market data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add extract module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from extract.stock_extractor import StockExtractor
from extract.bcb_extractor import BCBExtractor

# Database connection
DB_CONN = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@postgres:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

# Default arguments
default_args = {
    'owner': 'denio',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

def extract_stocks(**context):
    """Extract yesterday's stock data"""
    extractor = StockExtractor(DB_CONN)
    df = extractor.extract_incremental(lookback_days=5)
    rows = extractor.load_to_database(df)

    # Push to XCom for downstream tasks
    context['ti'].xcom_push(key='stocks_extracted', value=rows)
    return rows

def extract_indicators(**context):
    """Extract yesterday's indicator data"""
    extractor = BCBExtractor(DB_CONN)

    # For incremental, only extract last 5 days
    from datetime import datetime
    end_date = datetime.now()
    start_date = end_date - timedelta(days=5)

    df = extractor.extract_all_indicators(
        start_date=start_date.strftime('%d/%m/%Y'),
        end_date=end_date.strftime('%d/%m/%Y')
    )
    rows = extractor.load_to_database(df)

    context['ti'].xcom_push(key='indicators_extracted', value=rows)
    return rows

def validate_data(**context):
    """Run data quality checks"""
    ti = context['ti']
    stocks_rows = ti.xcom_pull(key='stocks_extracted', task_ids='extract_stocks')
    indicators_rows = ti.xcom_pull(key='indicators_extracted', task_ids='extract_indicators')

    if stocks_rows == 0 and indicators_rows == 0:
        raise ValueError("No new data extracted")

    print(f"Validation passed: {stocks_rows} stock rows, {indicators_rows} indicator rows")

# Define DAG
with DAG(
    'daily_market_etl',
    default_args=default_args,
    description='Extract daily Brazilian market data',
    schedule_interval='0 6 * * *',  # 6 AM daily
    catchup=False,
    tags=['etl', 'market', 'daily'],
) as dag:

    # Task 1: Extract stocks
    task_extract_stocks = PythonOperator(
        task_id='extract_stocks',
        python_callable=extract_stocks,
        provide_context=True,
    )

    # Task 2: Extract indicators
    task_extract_indicators = PythonOperator(
        task_id='extract_indicators',
        python_callable=extract_indicators,
        provide_context=True,
    )

    # Task 3: Validate data
    task_validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )

    # Task 4: Run dbt models
    task_dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_project && dbt run',
    )

    # Task 5: Run dbt tests
    task_dbt_test = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/dbt_project && dbt test',
    )

    # Define dependencies
    [task_extract_stocks, task_extract_indicators] >> task_validate >> task_dbt_run >> task_dbt_test
```

---

## 10. Data Quality & Testing

### **10.1 dbt Tests**

**File: `dbt_project/tests/assert_no_nulls_in_facts.sql`**

```sql
-- Ensure no null values in key fact table columns
SELECT
    market_id,
    date_id,
    stock_id,
    close_price
FROM {{ ref('fact_daily_market') }}
WHERE close_price IS NULL
   OR date_id IS NULL
   OR stock_id IS NULL
```

**File: `dbt_project/tests/assert_no_future_dates.sql`**

```sql
-- Ensure no dates in the future
SELECT
    date_id,
    COUNT(*) as row_count
FROM {{ ref('fact_daily_market') }}
WHERE date_id > {{ dbt_utils.date_trunc('day', dbt_utils.current_timestamp()) }}
GROUP BY date_id
```

**File: `dbt_project/tests/assert_positive_prices.sql`**

```sql
-- Ensure all prices are positive
SELECT
    market_id,
    close_price
FROM {{ ref('fact_daily_market') }}
WHERE close_price <= 0
```

### **10.2 Python Unit Tests**

**File: `tests/test_stock_extractor.py`**

```python
import pytest
import pandas as pd
from extract.stock_extractor import StockExtractor
from unittest.mock import patch, MagicMock

def test_stock_extractor_initialization():
    """Test StockExtractor initializes correctly"""
    extractor = StockExtractor("postgresql://test:test@localhost/test")
    assert extractor.tickers is not None
    assert len(extractor.tickers) > 0

def test_extract_historical_returns_dataframe():
    """Test extract_historical returns a valid DataFrame"""
    extractor = StockExtractor("postgresql://test:test@localhost/test")

    # Mock yfinance response
    with patch('yfinance.Ticker') as mock_ticker:
        mock_hist = MagicMock()
        mock_hist.history.return_value = pd.DataFrame({
            'Date': ['2024-01-01'],
            'Open': [10.0],
            'High': [11.0],
            'Low': [9.0],
            'Close': [10.5],
            'Volume': [1000000]
        })
        mock_ticker.return_value = mock_hist

        # Override tickers for testing
        extractor.tickers = ['PETR4.SA']

        df = extractor.extract_historical('2024-01-01', '2024-01-02')

        assert not df.empty
        assert 'ticker' in df.columns
        assert 'date' in df.columns
        assert 'close_price' in df.columns

def test_extract_handles_no_data():
    """Test extractor handles empty responses gracefully"""
    extractor = StockExtractor("postgresql://test:test@localhost/test")

    with patch('yfinance.Ticker') as mock_ticker:
        mock_hist = MagicMock()
        mock_hist.history.return_value = pd.DataFrame()
        mock_ticker.return_value = mock_hist

        extractor.tickers = ['INVALID.SA']

        with pytest.raises(ValueError, match="No data extracted"):
            extractor.extract_historical('2024-01-01', '2024-01-02')
```

---

## 11. Deployment & Operations

### **11.1 Setup Instructions**

**Complete Setup Script:**

```bash
#!/bin/bash
# scripts/setup.sh

echo "=== Brazilian Market ETL Setup ==="

# Step 1: Check prerequisites
echo "Checking prerequisites..."
command -v docker >/dev/null 2>&1 || { echo "Error: Docker not installed"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "Error: Python 3 not installed"; exit 1; }

# Step 2: Create .env file
echo "Creating .env file..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✓ .env file created. Please review and update if needed."
else
    echo "✓ .env file already exists"
fi

# Step 3: Install Python dependencies
echo "Installing Python dependencies..."
python3 -m pip install -r requirements.txt

# Step 4: Start Docker containers
echo "Starting Docker containers..."
docker-compose up -d postgres

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
sleep 10

# Step 5: Verify database
echo "Verifying database setup..."
docker-compose exec postgres psql -U dataeng -d brazilian_market -c "\dt raw.*"

# Step 6: Run initial data extraction
echo "Running initial data extraction (this may take 10-15 minutes)..."
python3 extract/stock_extractor.py
python3 extract/bcb_extractor.py

# Step 7: Start Airflow
echo "Starting Airflow..."
docker-compose up -d

echo ""
echo "=== Setup Complete ==="
echo "Airflow UI: http://localhost:8080 (admin/admin)"
echo "PostgreSQL: localhost:5432 (dataeng/dataeng123)"
echo ""
echo "Next steps:"
echo "1. Verify data: make verify-data"
echo "2. Set up dbt: cd dbt_project && dbt run"
echo "3. Start dashboard: streamlit run dashboard/app.py"
```

### **11.2 Makefile (Optional)**

**File: `Makefile`**

```makefile
.PHONY: help setup start stop logs clean test

help:
	@echo "Brazilian Market ETL Commands:"
	@echo "  make setup       - Initial setup (install deps, start containers)"
	@echo "  make start       - Start all services"
	@echo "  make stop        - Stop all services"
	@echo "  make logs        - View logs"
	@echo "  make extract     - Run data extraction"
	@echo "  make dbt-run     - Run dbt models"
	@echo "  make test        - Run unit tests"
	@echo "  make clean       - Clean up (remove containers and data)"

setup:
	./scripts/setup.sh

start:
	docker-compose up -d
	@echo "Services started. Airflow UI: http://localhost:8080"

stop:
	docker-compose down

logs:
	docker-compose logs -f

extract:
	python3 extract/stock_extractor.py
	python3 extract/bcb_extractor.py

dbt-run:
	cd dbt_project && dbt run

dbt-test:
	cd dbt_project && dbt test

test:
	pytest tests/ -v

clean:
	docker-compose down -v
	rm -rf logs/*
	@echo "Cleaned up. Run 'make setup' to start fresh."

verify-data:
	@echo "Checking raw data..."
	docker-compose exec postgres psql -U dataeng -d brazilian_market -c \
		"SELECT COUNT(*) as stock_rows FROM raw.stocks;"
	docker-compose exec postgres psql -U dataeng -d brazilian_market -c \
		"SELECT COUNT(*) as indicator_rows FROM raw.indicators;"
```

### **11.3 Monitoring Queries**

**File: `scripts/validate_data.py`**

```python
"""
Data validation and monitoring queries
"""
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def check_row_counts():
    """Check total row counts"""
    conn = get_connection()
    cur = conn.cursor()

    print("=== Row Counts ===")

    cur.execute("SELECT COUNT(*) FROM raw.stocks")
    stock_count = cur.fetchone()[0]
    print(f"Stocks: {stock_count:,} rows")

    cur.execute("SELECT COUNT(*) FROM raw.indicators")
    indicator_count = cur.fetchone()[0]
    print(f"Indicators: {indicator_count:,} rows")

    cur.execute("SELECT COUNT(*) FROM analytics.fact_daily_market")
    fact_count = cur.fetchone()[0]
    print(f"Fact table: {fact_count:,} rows")

    cur.close()
    conn.close()

def check_date_coverage():
    """Check date range coverage"""
    conn = get_connection()
    cur = conn.cursor()

    print("\n=== Date Coverage ===")

    cur.execute("""
        SELECT
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            MAX(date) - MIN(date) as days_span
        FROM raw.stocks
    """)

    row = cur.fetchone()
    print(f"Stocks: {row[0]} to {row[1]} ({row[2]} days)")

    cur.close()
    conn.close()

def check_data_freshness():
    """Check if data is up to date"""
    conn = get_connection()
    cur = conn.cursor()

    print("\n=== Data Freshness ===")

    yesterday = (datetime.now() - timedelta(days=1)).date()

    cur.execute("""
        SELECT COUNT(DISTINCT ticker)
        FROM raw.stocks
        WHERE date = %s
    """, (yesterday,))

    ticker_count = cur.fetchone()[0]
    print(f"Tickers with data for {yesterday}: {ticker_count}")

    if ticker_count == 0:
        print("⚠️ WARNING: No data for yesterday (market might be closed)")
    elif ticker_count < 15:
        print(f"⚠️ WARNING: Only {ticker_count} tickers updated (expected ~20)")
    else:
        print("✓ Data is fresh")

    cur.close()
    conn.close()

def check_data_quality():
    """Check for data quality issues"""
    conn = get_connection()
    cur = conn.cursor()

    print("\n=== Data Quality ===")

    # Check for nulls in close_price
    cur.execute("SELECT COUNT(*) FROM raw.stocks WHERE close_price IS NULL")
    null_prices = cur.fetchone()[0]
    if null_prices > 0:
        print(f"⚠️ WARNING: {null_prices} rows with null close_price")
    else:
        print("✓ No null prices")

    # Check for negative prices
    cur.execute("SELECT COUNT(*) FROM raw.stocks WHERE close_price <= 0")
    negative_prices = cur.fetchone()[0]
    if negative_prices > 0:
        print(f"⚠️ WARNING: {negative_prices} rows with negative/zero prices")
    else:
        print("✓ No negative prices")

    # Check for duplicates
    cur.execute("""
        SELECT ticker, date, COUNT(*) as dup_count
        FROM raw.stocks
        GROUP BY ticker, date
        HAVING COUNT(*) > 1
        LIMIT 5
    """)

    duplicates = cur.fetchall()
    if duplicates:
        print(f"⚠️ WARNING: Found duplicate ticker/date combinations:")
        for dup in duplicates:
            print(f"  {dup[0]} on {dup[1]}: {dup[2]} duplicates")
    else:
        print("✓ No duplicates")

    cur.close()
    conn.close()

if __name__ == "__main__":
    check_row_counts()
    check_date_coverage()
    check_data_freshness()
    check_data_quality()
```

---

## 12. Dashboard & Visualization

### **12.1 Streamlit Dashboard**

**File: `dashboard/app.py`**

```python
"""
Brazilian Market ETL Dashboard
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine

# Page config
st.set_page_config(
    page_title="Brazilian Market Dashboard",
    page_icon="📈",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_db_connection():
    from dotenv import load_dotenv
    load_dotenv()

    return create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:"
        f"{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:"
        f"{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )

engine = get_db_connection()

# Header
st.title("📈 Brazilian Market Dashboard")
st.markdown("Real-time analysis of Brazilian equities and macroeconomic indicators")

# Sidebar filters
st.sidebar.header("Filters")

date_range = st.sidebar.date_input(
    "Date Range",
    value=(datetime.now() - timedelta(days=365), datetime.now()),
    max_value=datetime.now()
)

# Load data
@st.cache_data(ttl=3600)
def load_market_data(start_date, end_date):
    query = f"""
    SELECT
        d.date,
        s.ticker,
        s.company_name,
        s.sector,
        f.close_price,
        f.volume,
        f.daily_return,
        f.selic_rate,
        f.usd_brl
    FROM analytics.fact_daily_market f
    JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
    JOIN analytics.dim_date d ON f.date_id = d.date_id
    WHERE d.date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY d.date, s.ticker
    """
    return pd.read_sql(query, engine)

with st.spinner("Loading data..."):
    df = load_market_data(date_range[0], date_range[1])

if df.empty:
    st.error("No data available for selected date range")
    st.stop()

# Metrics row
col1, col2, col3, col4 = st.columns(4)

with col1:
    latest_selic = df['selic_rate'].iloc[-1]
    st.metric("SELIC Rate", f"{latest_selic:.2f}%")

with col2:
    latest_usd = df['usd_brl'].iloc[-1]
    prev_usd = df['usd_brl'].iloc[-2]
    delta_usd = latest_usd - prev_usd
    st.metric("USD/BRL", f"R$ {latest_usd:.2f}", f"{delta_usd:+.2f}")

with col3:
    total_tickers = df['ticker'].nunique()
    st.metric("Tracked Stocks", total_tickers)

with col4:
    avg_return = df['daily_return'].mean() * 100
    st.metric("Avg Daily Return", f"{avg_return:.2f}%")

# Chart 1: Top Performers (Last 30 Days)
st.subheader("🏆 Top Performers (Last 30 Days)")

last_30_days = df[df['date'] >= (datetime.now() - timedelta(days=30)).date()]
performance = last_30_days.groupby('ticker').agg({
    'daily_return': 'sum',
    'company_name': 'first'
}).sort_values('daily_return', ascending=False).head(10)

fig_top = px.bar(
    performance,
    x=performance.index,
    y='daily_return',
    labels={'daily_return': 'Total Return (%)', 'ticker': 'Stock'},
    title="Top 10 Performers"
)
st.plotly_chart(fig_top, use_container_width=True)

# Chart 2: SELIC vs Market
st.subheader("📊 SELIC Rate Impact")

col1, col2 = st.columns(2)

with col1:
    # Average market return by SELIC bucket
    df['selic_bucket'] = pd.cut(df['selic_rate'], bins=5)
    selic_impact = df.groupby('selic_bucket')['daily_return'].mean().reset_index()

    fig_selic = px.line(
        selic_impact,
        x='selic_bucket',
        y='daily_return',
        title="Average Return by SELIC Rate"
    )
    st.plotly_chart(fig_selic, use_container_width=True)

with col2:
    # Time series: SELIC vs average market return
    time_series = df.groupby('date').agg({
        'selic_rate': 'first',
        'daily_return': 'mean'
    }).reset_index()

    fig_time = go.Figure()
    fig_time.add_trace(go.Scatter(
        x=time_series['date'],
        y=time_series['selic_rate'],
        name='SELIC Rate',
        yaxis='y'
    ))
    fig_time.add_trace(go.Scatter(
        x=time_series['date'],
        y=time_series['daily_return'] * 100,
        name='Market Return',
        yaxis='y2'
    ))

    fig_time.update_layout(
        title="SELIC Rate vs Market Return",
        yaxis=dict(title="SELIC (%)"),
        yaxis2=dict(title="Daily Return (%)", overlaying='y', side='right')
    )
    st.plotly_chart(fig_time, use_container_width=True)

# Data table
st.subheader("📋 Raw Data")
st.dataframe(df.tail(100), use_container_width=True)
```

---

## 13. Interview Preparation Guide

### **13.1 Project Walkthrough Script**

**"Tell me about this project"**

> "I built an end-to-end ETL pipeline that analyzes Brazilian stock market performance against macroeconomic indicators like the SELIC rate and inflation.
>
> The system extracts data from Yahoo Finance and the Brazilian Central Bank API, transforms it using dbt into a dimensional model, and loads it into PostgreSQL. Airflow orchestrates daily incremental updates, and I built a Streamlit dashboard for visualization.
>
> The pipeline handles about 200,000 rows of data spanning 10 years, covering 20 major Brazilian stocks and 7 economic indicators."

### **13.2 Common Interview Questions & Answers**

**Q: "Why did you choose this specific tech stack?"**

> "I chose PostgreSQL over MySQL because of better analytical query performance and support for window functions, which I needed for calculating moving averages and volatility.
>
> I used dbt instead of stored procedures because it allows version control of transformations, has a built-in testing framework, and is increasingly the industry standard.
>
> For orchestration, I chose Airflow over cron jobs because it provides better dependency management, retry logic, and a UI for monitoring - all things you need in production pipelines."

**Q: "How do you handle data quality issues?"**

> "I implemented a multi-layered approach:
>
> 1. **Extraction layer**: Validate API responses before loading
> 2. **Raw layer**: Unique constraints prevent duplicate loads
> 3. **dbt tests**: Assert no nulls in key fields, no future dates, all prices positive
> 4. **Validation DAG task**: Checks row counts before running transformations
> 5. **Monitoring**: Daily validation script checks data freshness and quality
>
> If any test fails, the pipeline stops and logs the error."

**Q: "How would you handle missing data?"**

> "Stock markets are closed on weekends and holidays, so missing data is expected. My approach:
>
> 1. **Forward fill**: For indicators like SELIC (which remain constant until changed), I forward-fill the last known value
> 2. **Null handling**: For stock prices on non-trading days, I keep them as NULL and filter them out in analytical queries
> 3. **Incremental loads**: I use a 5-day lookback window to catch any delayed updates
> 4. **Data quality alerts**: If more than 50% of expected data is missing, the pipeline sends an alert"

**Q: "How do you ensure this pipeline is idempotent?"**

> "Idempotency means I can run the same load multiple times without creating duplicates or inconsistent state.
>
> I achieve this through:
> 1. **Unique constraints** on raw tables (ticker + date, indicator_code + date)
> 2. **dbt's incremental models** with merge strategy (upsert)
> 3. **Date-based partitioning** so each run only affects specific date ranges
> 4. **Immutable raw layer** - I never update/delete, only append
>
> This means if Airflow retries a failed task, it's safe."

**Q: "How would you scale this to handle 10x more data?"**

> "For 10x scale (2M rows):
>
> 1. **Partitioning**: Partition tables by year/month for faster queries
> 2. **Incremental processing**: Only process changed data, not full refresh
> 3. **Parallel extraction**: Use Airflow's parallelism to extract multiple tickers simultaneously
> 4. **Database optimization**: Add materialized views for common queries, tune indexes
> 5. **Compression**: Use PostgreSQL table compression for older data
>
> For 100x scale, I'd consider migrating to a columnar database like ClickHouse or a data warehouse like Snowflake."

**Q: "Walk me through your dimensional model design"**

> "I used a star schema with one fact table and three dimensions:
>
> **Fact table** (`fact_daily_market`):
> - Grain: One row per stock per day
> - Contains: prices, volume, calculated metrics (returns, volatility)
> - Denormalized: Includes SELIC and USD/BRL for query performance
>
> **Dimensions**:
> - `dim_stock`: Stock metadata (ticker, company, sector)
> - `dim_date`: Date attributes (year, quarter, month, is_holiday)
> - `dim_indicator`: Indicator metadata (code, name, unit)
>
> I chose this over a normalized model because analytical queries are read-heavy and benefit from denormalization. The trade-off is some data duplication, but storage is cheap compared to query performance."

**Q: "How do you monitor pipeline health?"**

> "I have three monitoring layers:
>
> 1. **Airflow UI**: Visual DAG status, task duration trends, failure alerts
> 2. **Data validation**: Daily script checks row counts, data freshness, quality issues
> 3. **Logging**: Structured logs for every extraction, transformation, and load
>
> In a production environment, I'd add:
> - Prometheus/Grafana for metrics (rows processed, query latency)
> - PagerDuty for critical alerts
> - dbt docs site for data lineage documentation"

### **13.3 Technical Deep Dive Questions**

**Q: "Explain how incremental loads work in your pipeline"**

> "Incremental loads only process new/changed data instead of reprocessing everything.
>
> **Extraction**: I use a 5-day lookback window to catch yesterday's data plus any delayed updates. Yahoo Finance provides data with T+1 delay.
>
> **Loading**: PostgreSQL's unique constraint (ticker, date) handles duplicates - if a row already exists, it's ignored.
>
> **dbt**: My models use `incremental` materialization:
> ```sql
> {{ config(materialized='incremental', unique_key=['date_id', 'stock_id']) }}
> SELECT ...
> WHERE date >= (SELECT MAX(date) FROM {{ this }})
> ```
>
> This only processes dates newer than what's already in the table."

**Q: "How do you calculate volatility?"**

> "I calculate 7-day and 30-day rolling volatility using window functions:
>
> ```sql
> STDDEV(daily_return) OVER (
>   PARTITION BY stock_id
>   ORDER BY date
>   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
> ) AS volatility_7d
> ```
>
> This gives the standard deviation of returns over the trailing 7-day window. Higher standard deviation = higher volatility = riskier stock."

**Q: "What's the biggest challenge you faced in this project?"**

> "The biggest challenge was handling data from different frequencies - daily stock prices, monthly inflation, quarterly GDP.
>
> I solved it by:
> 1. Storing all data at its native frequency in raw tables
> 2. Using forward-fill in dbt to propagate monthly values to daily grain
> 3. Creating separate analytical views for different time periods
>
> For example, I can't compare daily stock returns to quarterly GDP changes directly, so I aggregate stocks to quarterly averages for that analysis."

### **13.4 Behavioral Questions**

**Q: "Why did you build this specific project?"**

> "I built this because I wanted to transition from legacy COBOL mainframe work to modern data engineering, and I needed a project that:
>
> 1. Used Python instead of COBOL
> 2. Demonstrated real ETL skills with multiple data sources
> 3. Leveraged my banking background (I work at Mercantil Bank, so financial data made sense)
> 4. Showed I could build production-grade systems, not just scripts
>
> I chose Brazilian market data specifically because it's relevant to LATAM employers and shows I understand the local economic context."

**Q: "What would you do differently if you started over?"**

> "Three things:
>
> 1. **Start with dbt earlier**: I initially built transformations in Python, then migrated to dbt. If I started over, I'd use dbt from day one.
>
> 2. **Add CI/CD sooner**: I added GitHub Actions tests later. I'd build that in from the beginning for better code quality.
>
> 3. **Document as I go**: I wrote most documentation at the end. Doing it incrementally would have saved time.
>
> But overall, I'm happy with the architecture - it's scalable and follows data engineering best practices."

---

## 14. Extension Ideas

### **Week 9+ Enhancements**

Once you have the MVP and are applying to jobs, you can add these features based on interview feedback:

1. **Machine Learning**
   - Predict next-day stock direction using scikit-learn
   - Add feature: "This stock has 60% probability of going up tomorrow based on SELIC + USD/BRL"

2. **Real-time Streaming**
   - Add Kafka/Redpanda for real-time price updates
   - Show how you'd handle streaming vs batch

3. **Cloud Deployment**
   - Deploy to AWS (RDS + ECS + Airflow on MWAA)
   - Show understanding of cloud infrastructure

4. **Data Catalog**
   - Add dbt docs site
   - Implement column-level lineage

5. **Advanced Analytics**
   - Sector rotation analysis
   - Correlation matrices
   - Portfolio optimization

---

## 15. Troubleshooting

### **Common Issues**

**Problem:** `yfinance` returns empty DataFrame

**Solution:**
```python
# Add these parameters to handle API issues
df = stock.history(start=start_date, end=end_date, auto_adjust=False, actions=False)
```

**Problem:** PostgreSQL connection refused

**Solution:**
```bash
# Check if container is running
docker-compose ps

# Check logs
docker-compose logs postgres

# Verify connection
docker-compose exec postgres psql -U dataeng -d brazilian_market -c "SELECT 1"
```

**Problem:** Airflow DAG not appearing

**Solution:**
```bash
# Check if DAG file has syntax errors
python airflow/dags/daily_market_etl.py

# Restart scheduler
docker-compose restart airflow-scheduler
```

**Problem:** dbt models failing

**Solution:**
```bash
# Run with debug logs
dbt run --debug

# Test connection
dbt debug

# Compile SQL to see what's being run
dbt compile
```

---

## Summary

This document provides everything you need to build the Brazilian Financial Markets ETL pipeline from scratch. The project will take 6-8 weeks working part-time (15 hrs/week) and will give you a production-grade portfolio piece for Data Engineer/Data Analyst interviews.

**Next steps:**
1. Save this document
2. Set up your GitHub repo
3. Start with Phase 1 this weekend
4. Follow the implementation guide sequentially

**Questions to consider before starting:**
- Do you want me to create template starter files you can copy-paste?
- Should I write more detailed dbt model examples?
- Do you want interview practice scenarios with this project?

Ready to start building?