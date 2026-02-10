# Brazilian Financial Markets ETL Pipeline - Complete Project Specification for Claude Code

**Project Version:** 1.0
**Target Completion:** 6-8 weeks (part-time, 15 hrs/week)
**Developer:** Dênio Barbosa Júnior
**Purpose:** Portfolio project demonstrating modern Data Engineering skills for international remote roles

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Success Criteria](#success-criteria)
3. [Architecture & Design Principles](#architecture--design-principles)
4. [Technology Stack](#technology-stack)
5. [Implementation Phases](#implementation-phases)
6. [Detailed Implementation Instructions](#detailed-implementation-instructions)
7. [Code Quality Standards](#code-quality-standards)
8. [Testing Requirements](#testing-requirements)
9. [Documentation Requirements](#documentation-requirements)
10. [Constraints & Guidelines](#constraints--guidelines)

---

## Project Overview

### Goal
Build a production-grade ETL (Extract, Transform, Load) pipeline that extracts Brazilian stock market data and macroeconomic indicators, transforms them into a dimensional data warehouse, and provides an interactive dashboard for analysis.

### Why This Project
This project repositions the developer from legacy COBOL/.NET work to modern Data Engineering by demonstrating:
- Multi-source data ingestion (APIs)
- Data transformation and modeling (dbt)
- Workflow orchestration (Apache Airflow)
- Dimensional modeling (star schema)
- Data quality testing
- Containerization (Docker)

### Business Context
The pipeline analyzes relationships between Brazilian stock prices and macroeconomic indicators (SELIC interest rate, inflation, USD/BRL exchange rate). This enables analysis questions like:
- "How does the SELIC rate affect IBOV index performance?"
- "Which sectors perform best during high inflation?"
- "What's the correlation between USD/BRL and commodity stocks?"

---

## Success Criteria

### Minimum Viable Product (Week 6)
- [ ] Extract 10 years of stock data for 20 Brazilian stocks (50k+ rows)
- [ ] Extract 10 years of BCB economic indicators (20k+ rows)
- [ ] Data loaded into PostgreSQL with proper three-schema structure (raw → staging → analytics)
- [ ] Basic data quality checks pass (no nulls in key fields, no negative prices)
- [ ] README with setup instructions exists
- [ ] All code committed to GitHub with meaningful commit history

### Interview-Ready (Week 8)
- [ ] Airflow DAG running daily incremental loads successfully
- [ ] dbt models creating fact/dimension tables correctly
- [ ] At least 3 analytical SQL queries demonstrating insights
- [ ] Basic Streamlit dashboard displaying market trends
- [ ] GitHub repository public with clean commit history
- [ ] CI/CD pipeline (GitHub Actions) running linting and tests
- [ ] Comprehensive README with architecture diagrams

### Production-Grade (Week 10+ - Optional)
- [ ] Advanced data quality tests covering edge cases
- [ ] Performance optimizations (indexes, partitioning)
- [ ] Complete documentation for all modules
- [ ] CloudWatch-style monitoring/alerting for pipeline failures
- [ ] Optional: AWS deployment (S3 + RDS)

---

## Architecture & Design Principles

### Three-Schema Pattern (Medallion Architecture)

```
┌─────────────────────────────────────────────────────────────┐
│                    EXTRACTION LAYER                         │
│  ┌──────────────┐              ┌──────────────┐             │
│  │Yahoo Finance │              │   BCB API    │             │
│  │  (Stocks)    │              │ (Indicators) │             │
│  └──────┬───────┘              └──────┬───────┘             │
└─────────┼──────────────────────────────┼────────────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────────────────────────────────────────────┐
│              RAW SCHEMA (Bronze Layer)                      │
│  - Immutable, append-only                                   │
│  - Matches source format exactly                            │
│  - No transformations                                       │
│  ┌──────────────┐              ┌──────────────┐             │
│  │  raw.stocks  │              │raw.indicators│             │
│  └──────────────┘              └──────────────┘             │
└─────────┬───────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│            STAGING SCHEMA (Silver Layer)                    │
│  - Cleaned and validated                                    │
│  - Data quality checks applied                              │
│  - No business logic yet                                    │
│  ┌──────────────┐              ┌──────────────┐             │
│  │  stg_stocks  │              │stg_indicators│             │
│  └──────────────┘              └──────────────┘             │
└─────────┬───────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│          ANALYTICS SCHEMA (Gold Layer)                      │
│  - Dimensional model (star schema)                          │
│  - Business logic applied                                   │
│  - Optimized for queries                                    │
│  ┌────────────────────────────────────────────────────┐     │
│  │              FACT TABLE                            │     │
│  │  ┌──────────────────┐                              │     │
│  │  │ fact_daily_market│                              │     │
│  │  │                  │                              │     │
│  │  │ - date_id (FK)   │                              │     │
│  │  │ - stock_id (FK)  │                              │     │
│  │  │ - close_price    │                              │     │
│  │  │ - volume         │                              │     │
│  │  │ - daily_return   │                              │     │
│  │  │ - volatility     │                              │     │
│  │  │ - selic_rate     │  (denormalized)              │     │
│  │  │ - usd_brl        │  (denormalized)              │     │
│  │  └──────────────────┘                              │     │
│  └────────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────────┐     │
│  │           DIMENSION TABLES                         │     │
│  │  ┌──────────┐  ┌────────────┐  ┌──────────┐        │     │
│  │  │dim_stock │  │dim_indicator│ │ dim_date │        │     │
│  │  │          │  │            │  │          │        │     │
│  │  │- stock_id│  │- indic_id  │  │ - date_id│        │     │
│  │  │- ticker  │  │- code      │  │ - date   │        │     │
│  │  │- name    │  │- name      │  │ - year   │        │     │
│  │  │- sector  │  │- unit      │  │ - quarter│        │     │
│  │  └──────────┘  └────────────┘  └──────────┘        │     │
│  └────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

### Design Principles

**1. Separation of Concerns**
- Extraction logic isolated in `extract/` directory
- Transformation logic in dbt models
- Loading logic in database connection modules
- Never mix these responsibilities

**2. Idempotency**
- Pipeline can be run multiple times safely
- Uses UNIQUE constraints to prevent duplicates
- Incremental loads only process new data
- Full backfill can be re-run without issues

**3. Incremental Processing**
- Daily runs only process yesterday's data
- Uses lookback window (5 days) to catch delayed updates
- dbt incremental models rebuild only changed data
- Reduces processing time from hours to minutes

**4. Data Quality at Every Layer**
- Raw layer: Schema validation, null checks
- Staging layer: Business rule validation (positive prices, valid dates)
- Analytics layer: Referential integrity (foreign keys), aggregation checks

**5. Observability**
- Comprehensive logging at INFO level
- Metrics tracked (rows processed, errors, duration)
- Airflow UI shows task status and logs
- dbt generates data lineage documentation

---

## Technology Stack

### Core Technologies

| Component | Technology | Version | Rationale |
|-----------|-----------|---------|-----------|
| **Language** | Python | 3.11+ | Industry standard for data engineering, extensive libraries |
| **Orchestration** | Apache Airflow | 2.8+ | Most common DE orchestration tool, excellent for resume |
| **Database** | PostgreSQL | 15+ | Best open-source analytical database, window functions |
| **Transformation** | dbt Core | 1.7+ | Modern SQL transformation framework, version control |
| **Containerization** | Docker Compose | Latest | Reproducible environments, production-ready patterns |
| **Visualization** | Streamlit | 1.29+ | Quick Python dashboards, easy deployment |

### Python Dependencies

```python
# requirements.txt

# Data extraction
yfinance==0.2.35              # Yahoo Finance API wrapper
requests==2.31.0              # HTTP requests for BCB API

# Data processing
pandas==2.1.4                 # DataFrame operations
numpy==1.26.2                 # Numerical operations

# Database
sqlalchemy==2.0.25            # Database ORM
psycopg2-binary==2.9.9        # PostgreSQL adapter

# Orchestration
apache-airflow==2.8.0         # Workflow orchestration
apache-airflow-providers-postgres==5.10.0

# Transformation
dbt-core==1.7.4               # SQL transformation framework
dbt-postgres==1.7.4           # PostgreSQL adapter for dbt

# Visualization
streamlit==1.29.0             # Dashboard framework
plotly==5.18.0                # Interactive charts

# Development
pytest==7.4.3                 # Testing framework
black==23.12.1                # Code formatting
flake8==7.0.0                 # Linting

# Utilities
python-dotenv==1.0.0          # Environment variables
loguru==0.7.2                 # Better logging
```

---

## Implementation Phases

### Phase 1: Foundation (Week 1-2)
**Goal:** Set up infrastructure and verify database connectivity

**Tasks:**
1. Create project directory structure
2. Initialize Git repository with proper .gitignore
3. Create docker-compose.yml with PostgreSQL service
4. Write database initialization SQL scripts
5. Start PostgreSQL container and verify connection
6. Create .env.example with all required variables

**Deliverables:**
- Complete directory structure matching specification
- Running PostgreSQL container
- Database schemas (raw, staging, analytics) created
- Connection test passing

**Success Criteria:**
```bash
# These commands should succeed:
docker-compose ps | grep postgres
psql -h localhost -U dataeng -d brazilian_market -c "\dt raw.*"
psql -h localhost -U dataeng -d brazilian_market -c "\dt analytics.*"
```

---

### Phase 2: Data Extraction (Week 2-3)
**Goal:** Extract historical data from APIs into raw tables

**Tasks:**
1. Implement `extract/stock_extractor.py` with StockExtractor class
2. Implement `extract/bcb_extractor.py` with BCBExtractor class
3. Add comprehensive error handling and retry logic
4. Add logging using loguru
5. Run backfill for 10 years of data
6. Validate row counts and data quality

**Deliverables:**
- Working extraction scripts with proper error handling
- 50k+ stock rows in raw.stocks table
- 20k+ indicator rows in raw.indicators table
- Extraction logs showing successful completion

**Success Criteria:**
```bash
# Verify data loaded
psql -h localhost -U dataeng -d brazilian_market -c \
  "SELECT COUNT(*), MIN(date), MAX(date) FROM raw.stocks;"
# Expected: ~50,000+ rows, date range 2015-01-01 to present

psql -h localhost -U dataeng -d brazilian_market -c \
  "SELECT indicator_code, COUNT(*) FROM raw.indicators GROUP BY indicator_code;"
# Expected: 7 indicators with 1000+ rows each
```

---

### Phase 3: Data Transformation (Week 3-5)
**Goal:** Transform raw data into analytics-ready dimensional model

**Tasks:**
1. Install and configure dbt Core
2. Create dbt project structure
3. Write staging models (stg_stocks.sql, stg_indicators.sql)
4. Write intermediate models (int_stock_returns.sql, int_stock_volatility.sql, int_market_indicators.sql)
5. Write mart models (dim_date.sql, dim_stock.sql, dim_indicator.sql, fact_daily_market.sql)
6. Add dbt tests for data quality
7. Run dbt models and validate output

**Deliverables:**
- Complete dbt project with all models
- Populated dimensional tables in analytics schema
- All dbt tests passing
- dbt documentation generated

**Success Criteria:**
```bash
# Run dbt and verify success
cd dbt_project
dbt run
# Expected: All models succeed

dbt test
# Expected: All tests pass

# Verify fact table populated
psql -h localhost -U dataeng -d brazilian_market -c \
  "SELECT COUNT(*), MIN(date_id), MAX(date_id) FROM analytics.fact_daily_market;"
# Expected: ~50,000 rows with date range matching raw data
```

---

### Phase 4: Orchestration (Week 5-6)
**Goal:** Automate pipeline execution with Airflow

**Tasks:**
1. Set up Airflow in Docker Compose
2. Write daily_market_etl.py DAG with proper dependencies
3. Configure task retries and error handling
4. Add data validation task between extraction and transformation
5. Test incremental loads
6. Schedule daily execution (6 AM Brazil time)

**Deliverables:**
- Working Airflow installation accessible at localhost:8080
- Functional DAG with all tasks
- Successful test run of incremental load
- Scheduled daily execution

**Success Criteria:**
```bash
# Access Airflow UI
curl http://localhost:8080/health
# Expected: {"status": "healthy"}

# Trigger DAG manually and verify success
# (via Airflow UI or CLI)
```

---

### Phase 5: Documentation & Testing (Week 6-7)
**Goal:** Make project interview-ready

**Tasks:**
1. Write comprehensive README.md with architecture diagrams
2. Add docstrings to all Python functions
3. Create architecture diagrams (use Draw.io or Mermaid)
4. Write unit tests for extraction modules
5. Add integration tests for end-to-end pipeline
6. Create sample analytical queries demonstrating insights
7. Document setup instructions step-by-step

**Deliverables:**
- Professional README with clear setup instructions
- Architecture diagrams (PNG or SVG)
- Test coverage >70%
- Sample SQL queries with business context

**Success Criteria:**
- New developer can set up project in <30 minutes following README
- All tests pass with `pytest`
- Architecture diagrams clearly show data flow

---

### Phase 6: Visualization (Week 7-8)
**Goal:** Build interactive dashboard

**Tasks:**
1. Set up Streamlit application
2. Create market overview page (IBOV trends, top movers)
3. Create sector analysis page
4. Create macro correlation page (SELIC vs stocks)
5. Add stock screener with filters
6. Deploy locally and test

**Deliverables:**
- Multi-page Streamlit dashboard
- 5-10 interactive visualizations
- Screenshots for README

**Success Criteria:**
```bash
# Start dashboard
streamlit run dashboard/app.py
# Expected: Dashboard opens at localhost:8501 with data displayed
```

---

## Detailed Implementation Instructions

<investigate_before_answering>
Before writing any code, investigate the existing codebase structure first. Read relevant files to understand the current state. Never speculate about code you have not opened. If referencing specific files or patterns, read those files before making claims. This ensures grounded, hallucination-free implementations.
</investigate_before_answering>

<default_to_action>
By default, implement changes rather than only suggesting them. When the task is clear, proceed with creating files, writing code, and making edits. If details are missing, use tools to discover them (read files, check database schema) rather than asking for clarification. Infer the most useful likely action and execute it.
</default_to_action>

### Directory Structure

Create this exact structure:

```
brazilian-market-etl/
│
├── README.md                          # Comprehensive project documentation
├── LICENSE                            # MIT License
├── .gitignore                         # Python, IDE, data files, .env
├── .env.example                       # Environment variables template
├── requirements.txt                   # Python dependencies
├── docker-compose.yml                 # Container orchestration
│
├── docs/                              # Documentation
│   ├── architecture.md                # Architecture decisions
│   ├── setup_guide.md                 # Step-by-step setup
│   ├── data_dictionary.md             # All tables/columns explained
│   └── diagrams/
│       ├── architecture.png           # High-level architecture
│       └── erd.png                    # Entity relationship diagram
│
├── airflow/                           # Orchestration
│   ├── dags/
│   │   ├── daily_market_etl.py        # Main incremental DAG
│   │   └── backfill_historical.py     # One-time historical load
│   └── config/
│       └── airflow.cfg                # Airflow configuration
│
├── database/                          # Database setup
│   ├── init/
│   │   ├── 01_create_schemas.sql      # Schema creation
│   │   ├── 02_create_raw_tables.sql   # Raw layer tables
│   │   └── 03_create_analytics_tables.sql # Analytics layer
│   └── seeds/
│       ├── dim_stock_seed.csv         # Stock metadata
│       └── dim_date_seed.csv          # Date dimension (2015-2030)
│
├── extract/                           # Data extraction layer
│   ├── __init__.py
│   ├── base_extractor.py              # Base class with common functionality
│   ├── stock_extractor.py             # Yahoo Finance logic
│   ├── bcb_extractor.py               # BCB API logic
│   ├── config.py                      # Extraction configurations
│   └── utils.py                       # Rate limiting, retry logic
│
├── load/                              # Data loading layer
│   ├── __init__.py
│   └── db_loader.py                   # Database connection, bulk insert
│
├── dbt_project/                       # dbt transformation framework
│   ├── dbt_project.yml                # Project configuration
│   ├── profiles.yml                   # Database connection profiles
│   ├── models/
│   │   ├── staging/                   # Layer 1: Clean raw data
│   │   │   ├── _staging.yml           # Model documentation
│   │   │   ├── stg_stocks.sql
│   │   │   └── stg_indicators.sql
│   │   ├── intermediate/              # Layer 2: Business logic
│   │   │   ├── _intermediate.yml
│   │   │   ├── int_stock_returns.sql
│   │   │   ├── int_stock_volatility.sql
│   │   │   └── int_market_indicators.sql
│   │   └── marts/                     # Layer 3: Dimensional model
│   │       ├── _marts.yml
│   │       ├── dim_date.sql
│   │       ├── dim_stock.sql
│   │       ├── dim_indicator.sql
│   │       └── fact_daily_market.sql
│   ├── tests/                         # Data quality tests
│   │   ├── assert_no_nulls_in_facts.sql
│   │   ├── assert_no_future_dates.sql
│   │   └── assert_positive_prices.sql
│   ├── macros/                        # Reusable SQL functions
│   │   ├── calculate_return.sql
│   │   └── calculate_volatility.sql
│   └── seeds/
│       └── stock_metadata.csv
│
├── dashboard/                         # Streamlit visualization
│   ├── app.py                         # Main dashboard entry point
│   ├── pages/
│   │   ├── 1_Market_Overview.py
│   │   ├── 2_Sector_Analysis.py
│   │   ├── 3_Macro_Correlation.py
│   │   └── 4_Stock_Screener.py
│   ├── components/
│   │   ├── charts.py                  # Reusable chart functions
│   │   └── queries.py                 # SQL queries for dashboard
│   └── config.py
│
├── tests/                             # Unit & integration tests
│   ├── __init__.py
│   ├── test_stock_extractor.py
│   ├── test_bcb_extractor.py
│   └── test_db_loader.py
│
└── scripts/                           # Utility scripts
    ├── setup_db.sh                    # Initialize database
    ├── backfill_data.py               # Run historical backfill
    └── validate_data.py               # Manual data validation
```

### File Creation Guidelines

<use_parallel_tool_calls>
When creating multiple independent files (like database initialization scripts or test files), create them in parallel using multiple tool calls simultaneously. This significantly speeds up project setup. Only create files sequentially when they have dependencies on each other.
</use_parallel_tool_calls>

**Critical file creation rules:**

1. **Always create actual files** - Never just show content as examples. Write the complete implementation to disk.

2. **Use proper paths** - All files must be created in the correct directory according to the structure above.

3. **Include comprehensive docstrings** - Every Python function/class needs a docstring explaining:
   - What it does
   - Parameters (type and description)
   - Return value (type and description)
   - Example usage
   - Any exceptions raised

4. **Add logging statements** - Use loguru for logging at INFO level for normal operations, WARNING for recoverable issues, ERROR for failures.

5. **Implement error handling** - Every API call, database operation, and file I/O must have try/except blocks with meaningful error messages.

### Code Implementation Standards

**Python Code Quality:**

```python
# Example of expected code quality

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import create_engine

# Configure logging
logger = logging.getLogger(__name__)

class StockExtractor:
    """
    Extract stock price data from Yahoo Finance API.

    This class handles the extraction of historical and incremental
    stock price data for Brazilian equities traded on B3 exchange.

    Attributes:
        engine: SQLAlchemy database engine
        tickers: List of stock tickers to extract

    Example:
        >>> extractor = StockExtractor("postgresql://...")
        >>> df = extractor.extract_historical(start_date='2015-01-01')
        >>> extractor.load_to_database(df)
    """

    DEFAULT_TICKERS = [
        'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA',
        # ... more tickers
    ]

    def __init__(
        self,
        db_connection_string: str,
        tickers: Optional[List[str]] = None
    ):
        """
        Initialize the stock extractor.

        Args:
            db_connection_string: PostgreSQL connection string
            tickers: Optional list of stock tickers. If None, uses DEFAULT_TICKERS

        Raises:
            ConnectionError: If database connection fails
        """
        try:
            self.engine = create_engine(db_connection_string)
            self.tickers = tickers or self.DEFAULT_TICKERS
            logger.info(f"Initialized StockExtractor with {len(self.tickers)} tickers")
        except Exception as e:
            logger.error(f"Failed to initialize StockExtractor: {e}")
            raise ConnectionError(f"Database connection failed: {e}")

    def extract_historical(
        self,
        start_date: str = '2015-01-01',
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Extract historical stock data for all configured tickers.

        This method downloads daily OHLCV (Open, High, Low, Close, Volume)
        data from Yahoo Finance for each ticker in the configured list.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format. Defaults to today.

        Returns:
            DataFrame with columns: ticker, date, open_price, high_price,
                                   low_price, close_price, volume, adj_close

        Raises:
            ValueError: If no data extracted for any ticker

        Example:
            >>> df = extractor.extract_historical('2024-01-01', '2024-12-31')
            >>> print(f"Extracted {len(df)} rows")
        """
        # Implementation here...
        pass
```

**SQL Code Quality (dbt models):**

```sql
-- models/staging/stg_stocks.sql

{{
  config(
    materialized='view',
    tags=['staging', 'stocks']
  )
}}

/*
=====================================================================
STAGING MODEL: Stock Prices
=====================================================================
Purpose:
  Clean and validate raw stock price data from Yahoo Finance.
  This is the first transformation layer - we fix data quality
  issues but don't apply business logic yet.

Transformations:
  - Remove rows with null close_price (data quality)
  - Filter out negative or zero prices (invalid data)
  - Remove obvious outliers (prices > 1M)
  - Validate high >= low constraint
  - Add data quality flags for suspicious records

Dependencies:
  - raw.stocks (source table)

Grain:
  One row per ticker per trading date

Author: Dênio Barbosa Júnior
Created: 2025-02-07
=====================================================================
*/

WITH source AS (
    -- Pull from raw layer
    SELECT * FROM {{ source('raw', 'stocks') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        ticker,
        date,

        -- Price data with quality checks
        CASE
            WHEN close_price <= 0 THEN NULL
            WHEN close_price > 1000000 THEN NULL  -- Outlier
            ELSE close_price
        END AS close_price,

        -- More cleaning logic...

    FROM source
    WHERE 1=1
        AND date >= '{{ var("start_date") }}'
        AND date <= CURRENT_DATE
        AND close_price IS NOT NULL
),

-- Add derived columns and quality flags
final AS (
    SELECT
        *,

        -- Flag suspicious records for manual review
        CASE
            WHEN close_price > open_price * 1.5 THEN TRUE
            WHEN close_price < open_price * 0.5 THEN TRUE
            ELSE FALSE
        END AS is_suspicious,

        -- Calculate intraday range
        CASE
            WHEN high_price IS NOT NULL AND low_price IS NOT NULL
            THEN high_price - low_price
            ELSE NULL
        END AS intraday_range

    FROM cleaned
)

SELECT * FROM final
```

### Database Schema Implementation

**Critical schema requirements:**

1. **Use appropriate data types:**
   - Prices: `NUMERIC(12, 4)` (exact precision for money)
   - Counts: `BIGINT` for large numbers, `INTEGER` for dimensions
   - Dates: `DATE` not `TIMESTAMP` (we only care about day-level)
   - Text: `VARCHAR(N)` with appropriate length limits

2. **Add constraints:**
   - Primary keys on all tables
   - Foreign keys in fact tables referencing dimensions
   - UNIQUE constraints to prevent duplicates
   - CHECK constraints for data validation

3. **Create indexes:**
   - Index all foreign key columns
   - Index frequently queried columns (date, ticker)
   - Consider composite indexes for common query patterns

**Example schema file:**

```sql
-- database/init/02_create_raw_tables.sql

-- =====================================================================
-- RAW SCHEMA: Landing zone for extracted data
-- =====================================================================
-- Purpose: Store data exactly as received from source systems
-- Pattern: Immutable, append-only
-- Retention: Indefinite (source of truth)
-- =====================================================================

-- Raw stock prices from Yahoo Finance
CREATE TABLE raw.stocks (
    -- Surrogate key for internal use
    id BIGSERIAL PRIMARY KEY,

    -- Business keys
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,

    -- Price data (NUMERIC for exact precision)
    open_price NUMERIC(12, 4),
    high_price NUMERIC(12, 4),
    low_price NUMERIC(12, 4),
    close_price NUMERIC(12, 4),
    volume BIGINT,
    adj_close NUMERIC(12, 4),

    -- Metadata
    loaded_at TIMESTAMP DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'yahoo_finance',

    -- Constraints
    CONSTRAINT uq_stocks_ticker_date UNIQUE (ticker, date),
    CONSTRAINT chk_stocks_positive_price CHECK (close_price > 0),
    CONSTRAINT chk_stocks_high_low CHECK (high_price >= low_price)
);

-- Indexes for query performance
CREATE INDEX idx_stocks_ticker ON raw.stocks(ticker);
CREATE INDEX idx_stocks_date ON raw.stocks(date);
CREATE INDEX idx_stocks_ticker_date ON raw.stocks(ticker, date);
CREATE INDEX idx_stocks_loaded_at ON raw.stocks(loaded_at);

-- Add table comment for documentation
COMMENT ON TABLE raw.stocks IS
'Raw stock price data from Yahoo Finance. One row per ticker per trading day. Immutable - never update or delete rows.';

COMMENT ON COLUMN raw.stocks.adj_close IS
'Adjusted closing price accounting for splits and dividends. Use this for return calculations.';
```

### Airflow DAG Implementation

**Critical DAG requirements:**

1. **Set proper default_args:**
   - owner: 'denio'
   - depends_on_past: False (each day independent)
   - email_on_failure: True (if email configured)
   - retries: 2
   - retry_delay: timedelta(minutes=5)

2. **Use task dependencies clearly:**
   ```python
   # Preferred: Explicit dependency chain
   extract_stocks >> validate_data >> transform_data >> test_data

   # Also acceptable: List notation for parallel tasks
   [extract_stocks, extract_indicators] >> validate_data
   ```

3. **Add monitoring:**
   - Log start/end of each task
   - Track row counts processed
   - Alert on failures

**Example DAG structure:**

```python
# airflow/dags/daily_market_etl.py

"""
Daily Market ETL Pipeline
========================

This DAG runs daily at 6:00 AM Brazil time to extract and process
yesterday's market data.

Schedule: Daily at 06:00 BRT (09:00 UTC)
Retry: 2 attempts with 5-minute delays
Timeout: 30 minutes per task

Tasks:
1. extract_stocks - Pull yesterday's stock prices from Yahoo Finance
2. extract_indicators - Pull yesterday's indicators from BCB API
3. validate_data - Check data quality before processing
4. run_dbt_models - Transform data using dbt
5. run_dbt_tests - Validate transformed data

Dependencies:
extract_stocks ─┐
               ├─→ validate_data → run_dbt_models → run_dbt_tests
extract_indicators ─┘

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add project modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from extract.stock_extractor import StockExtractor
from extract.bcb_extractor import BCBExtractor

# Configuration
DB_CONN = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@postgres:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

# Default arguments applied to all tasks
default_args = {
    'owner': 'denio',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'execution_timeout': timedelta(minutes=30),
}

# Task implementations
def extract_stocks(**context):
    """Extract yesterday's stock data from Yahoo Finance."""
    logger = context['task_instance'].log
    logger.info("Starting stock extraction...")

    extractor = StockExtractor(DB_CONN)
    df = extractor.extract_incremental(lookback_days=5)
    rows = extractor.load_to_database(df)

    # Store metrics in XCom for downstream tasks
    context['ti'].xcom_push(key='stocks_extracted', value=rows)
    logger.info(f"Successfully extracted {rows} stock records")

    return rows

# More task implementations...

# Define DAG
with DAG(
    'daily_market_etl',
    default_args=default_args,
    description='Extract daily Brazilian market data',
    schedule_interval='0 6 * * *',  # 6 AM daily
    catchup=False,  # Don't backfill missed runs
    tags=['etl', 'market', 'daily', 'production'],
) as dag:

    # Task definitions...
    pass
```

---

## Code Quality Standards

<avoid_excessive_markdown_and_bullet_points>
When writing documentation, reports, or technical explanations in markdown files, write in clear, flowing prose using complete paragraphs and sentences. Use standard paragraph breaks for organization. Reserve markdown primarily for inline code, code blocks, and simple headings. Avoid excessive use of bold, italics, ordered lists, or unordered lists unless presenting truly discrete items where a list format is the best option. Instead of listing items with bullets or numbers, incorporate them naturally into sentences. Your goal is readable, flowing text that guides the reader naturally through ideas rather than fragmenting information into isolated points.
</avoid_excessive_markdown_and_bullet_points>

### Python Code Standards

**Follow PEP 8 style guide strictly:**

- Maximum line length: 88 characters (Black formatter default)
- Use 4 spaces for indentation, never tabs
- Two blank lines between top-level functions and classes
- One blank line between methods in a class
- Imports organized: standard library, third-party, local modules

**Type hints required:**

```python
from typing import Optional, List, Dict, Union
import pandas as pd

def process_data(
    data: pd.DataFrame,
    start_date: str,
    end_date: Optional[str] = None,
    config: Dict[str, Union[str, int]] = None
) -> pd.DataFrame:
    """Process data within date range."""
    pass
```

**Logging standards:**

```python
from loguru import logger

# Configure logging
logger.add(
    "logs/extraction_{time}.log",
    rotation="1 day",
    retention="7 days",
    level="INFO"
)

# Use appropriate log levels
logger.info("Starting extraction for 20 tickers")
logger.warning(f"No data returned for {ticker}")
logger.error(f"API request failed: {error}")
logger.debug(f"Request params: {params}")
```

**Error handling patterns:**

```python
# Good: Specific exception handling
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
except requests.exceptions.Timeout:
    logger.error(f"Request timed out for {url}")
    raise
except requests.exceptions.HTTPError as e:
    logger.error(f"HTTP error {e.response.status_code}: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    raise

# Bad: Bare except or catching too broadly
try:
    # code
except:  # Don't do this
    pass
```

### SQL Code Standards

**dbt model naming conventions:**

- Staging models: `stg_<source>_<entity>.sql` (e.g., `stg_yahoo_stocks.sql`)
- Intermediate models: `int_<business_concept>.sql` (e.g., `int_stock_returns.sql`)
- Mart models: `dim_<entity>.sql` or `fact_<entity>.sql`

**SQL formatting:**

```sql
-- Good: Readable, consistent formatting
SELECT
    ticker,
    date,
    close_price,

    -- Calculate daily return
    CASE
        WHEN prev_close IS NOT NULL AND prev_close > 0
        THEN ((close_price - prev_close) / prev_close)
        ELSE NULL
    END AS daily_return,

    -- 30-day moving average
    AVG(close_price) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ma_30d

FROM {{ ref('stg_stocks') }}
WHERE date >= '2015-01-01'
ORDER BY ticker, date

-- Bad: Hard to read
SELECT ticker,date,close_price,CASE WHEN prev_close IS NOT NULL AND prev_close>0
THEN ((close_price-prev_close)/prev_close) ELSE NULL END AS daily_return FROM stg_stocks
```

**Always add comments:**

```sql
-- Every dbt model needs:
-- 1. Header comment explaining purpose
-- 2. CTE comments explaining each step
-- 3. Column comments for complex calculations

WITH source AS (
    -- Pull raw data from landing zone
    SELECT * FROM {{ source('raw', 'stocks') }}
),

cleaned AS (
    -- Remove invalid records and standardize formats
    SELECT
        ticker,
        date,
        -- Use adjusted close for return calculations to account for splits
        adj_close AS close_price
    FROM source
    WHERE close_price > 0  -- Filter invalid prices
)

SELECT * FROM cleaned
```

### Git Commit Standards

**Commit message format:**

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code formatting, no logic change
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance tasks

**Examples:**

```
feat(extraction): add BCB API integration for economic indicators

- Implement BCBExtractor class with retry logic
- Add support for 7 key economic indicators
- Include rate limiting (1 request/second)

Closes #12
```

```
fix(dbt): correct volatility calculation window function

The previous implementation used ROWS BETWEEN 6 PRECEDING instead of
29 PRECEDING for 30-day volatility. This has been corrected.
```

---

## Testing Requirements

### Unit Tests

**Test coverage targets:**
- Extraction modules: >80%
- dbt models: >70%
- Overall project: >70%

**Example test structure:**

```python
# tests/test_stock_extractor.py

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from extract.stock_extractor import StockExtractor

class TestStockExtractor:
    """Test suite for StockExtractor class."""

    @pytest.fixture
    def mock_db_connection(self):
        """Provide mock database connection."""
        return "postgresql://test:test@localhost/test"

    @pytest.fixture
    def extractor(self, mock_db_connection):
        """Create StockExtractor instance with mock connection."""
        return StockExtractor(mock_db_connection)

    def test_initialization_with_default_tickers(self, extractor):
        """Test that extractor initializes with default ticker list."""
        assert len(extractor.tickers) > 0
        assert 'PETR4.SA' in extractor.tickers

    def test_initialization_with_custom_tickers(self, mock_db_connection):
        """Test initialization with custom ticker list."""
        custom_tickers = ['VALE3.SA', 'ITUB4.SA']
        extractor = StockExtractor(mock_db_connection, tickers=custom_tickers)
        assert extractor.tickers == custom_tickers

    @patch('yfinance.Ticker')
    def test_extract_historical_returns_dataframe(self, mock_ticker, extractor):
        """Test that extract_historical returns a valid DataFrame."""
        # Setup mock
        mock_hist = MagicMock()
        mock_hist.history.return_value = pd.DataFrame({
            'Date': ['2024-01-01'],
            'Open': [10.0],
            'Close': [10.5],
            'Volume': [1000000]
        })
        mock_ticker.return_value = mock_hist

        # Execute
        extractor.tickers = ['TEST.SA']
        df = extractor.extract_historical('2024-01-01', '2024-01-02')

        # Assert
        assert not df.empty
        assert 'ticker' in df.columns
        assert 'date' in df.columns
        assert 'close_price' in df.columns

    def test_extract_handles_empty_response(self, extractor):
        """Test that extractor handles empty API responses gracefully."""
        with patch('yfinance.Ticker') as mock_ticker:
            mock_hist = MagicMock()
            mock_hist.history.return_value = pd.DataFrame()
            mock_ticker.return_value = mock_hist

            extractor.tickers = ['INVALID.SA']

            with pytest.raises(ValueError, match="No data extracted"):
                extractor.extract_historical()
```

### dbt Tests

**Schema tests (in .yml files):**

```yaml
# models/staging/_staging.yml

models:
  - name: stg_stocks
    description: "Cleaned stock price data"
    columns:
      - name: ticker
        description: "Stock ticker symbol"
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
        description: "Closing price"
        tests:
          - not_null
          - positive_price  # Custom test

      - name: volume
        description: "Trading volume"
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: true
```

**Custom tests (in tests/ directory):**

```sql
-- tests/assert_no_future_dates.sql

-- Ensure no dates in the future exist in fact table
SELECT
    date_id,
    COUNT(*) as row_count
FROM {{ ref('fact_daily_market') }}
WHERE date_id > TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INTEGER
GROUP BY date_id
```

### Integration Tests

Create end-to-end tests that verify the entire pipeline:

```python
# tests/test_integration_pipeline.py

import pytest
from datetime import datetime, timedelta
from extract.stock_extractor import StockExtractor
from extract.bcb_extractor import BCBExtractor
import subprocess

@pytest.mark.integration
class TestPipelineIntegration:
    """End-to-end integration tests for the full pipeline."""

    def test_extraction_to_database_flow(self, db_connection):
        """Test that data flows from extraction to database correctly."""
        # Extract data
        extractor = StockExtractor(db_connection)
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        df = extractor.extract_incremental(lookback_days=1)

        # Load to database
        rows = extractor.load_to_database(df)

        # Verify in database
        with db_connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM raw.stocks WHERE date = %s",
                (yesterday,)
            )
            count = cursor.fetchone()[0]

        assert count > 0
        assert count == rows

    def test_dbt_models_execute_successfully(self):
        """Test that all dbt models run without errors."""
        result = subprocess.run(
            ['dbt', 'run', '--project-dir', 'dbt_project'],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0
        assert 'Completed successfully' in result.stdout

    def test_dbt_tests_pass(self):
        """Test that all dbt data quality tests pass."""
        result = subprocess.run(
            ['dbt', 'test', '--project-dir', 'dbt_project'],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0
        assert 'PASS' in result.stdout
```

---

## Documentation Requirements

### README.md Structure

The README must include these sections in this order:

1. **Project Title & Description** - One paragraph explaining what this is
2. **Architecture Diagram** - High-level visual of data flow
3. **Features** - Bullet list of key capabilities
4. **Technology Stack** - Table of technologies used
5. **Prerequisites** - What needs to be installed
6. **Quick Start** - Commands to get running in 5 minutes
7. **Detailed Setup** - Step-by-step instructions
8. **Usage** - How to run each component
9. **Project Structure** - Directory tree with explanations
10. **Data Sources** - APIs used and their documentation links
11. **Pipeline Details** - Explanation of each phase
12. **Testing** - How to run tests
13. **Troubleshooting** - Common issues and solutions
14. **Future Enhancements** - Planned improvements
15. **Contributing** - Not accepting contributions (personal project)
16. **License** - MIT License
17. **Contact** - Your LinkedIn/GitHub

**Example README structure:**

```markdown
# Brazilian Financial Markets ETL Pipeline

A production-grade data engineering project that extracts Brazilian stock market data and macroeconomic indicators, transforms them into a dimensional data warehouse, and provides interactive analysis through a Streamlit dashboard.

![Architecture Diagram](docs/diagrams/architecture.png)

## Features

- Multi-source data ingestion from Yahoo Finance and Brazilian Central Bank API
- Three-layer data architecture following medallion pattern (Raw → Staging → Analytics)
- Dimensional modeling with star schema for optimal query performance
- Automated daily pipeline orchestration with Apache Airflow
- Comprehensive data quality testing with dbt
- Interactive dashboard for market analysis and correlation studies
- Fully containerized with Docker for reproducible environments

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Python 3.11 | Data processing and orchestration |
| Orchestration | Apache Airflow 2.8 | Workflow scheduling and monitoring |
| Database | PostgreSQL 15 | Data warehouse |
| Transformation | dbt Core 1.7 | SQL-based data transformations |
| Visualization | Streamlit 1.29 | Interactive dashboard |
| Containerization | Docker Compose | Environment management |

## Prerequisites

Before setting up this project, ensure you have installed:

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- Git
- Python 3.11 or higher (for local development)

## Quick Start

Get the project running in 5 minutes:

```bash
# Clone repository
git clone https://github.com/your-username/brazilian-market-etl.git
cd brazilian-market-etl

# Copy environment variables
cp .env.example .env

# Start all services
docker-compose up -d

# Run initial data backfill (takes 10-15 minutes)
python extract/stock_extractor.py
python extract/bcb_extractor.py

# Access services
# Airflow UI: http://localhost:8080 (admin/admin)
# Dashboard: http://localhost:8501
```

## Detailed Setup

Follow these steps for a complete setup with understanding of each component:

### Step 1: Clone and Configure

...
```

### Code Documentation

Every module must have a module-level docstring:

```python
"""
stock_extractor.py
==================

This module handles extraction of stock price data from Yahoo Finance API.

The StockExtractor class provides methods to:
- Extract historical stock prices for configurable date ranges
- Extract incremental updates (daily batch processing)
- Load data to PostgreSQL raw layer with deduplication

Key Features:
- Retry logic with exponential backoff
- Rate limiting to respect API constraints
- Comprehensive error handling and logging
- Data validation before database insertion

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""
```

### Inline Comments

Add comments for complex logic:

```python
# Calculate 30-day rolling volatility
# We use standard deviation of returns over trailing 30-day window
# This measures price variability and is a key risk metric
volatility = (
    df.groupby('ticker')['daily_return']
    .rolling(window=30, min_periods=20)  # Need at least 20 days for statistical validity
    .std()
    .reset_index(drop=True)
)
```

---

## Constraints & Guidelines

<do_not_act_before_instructions>
When facing ambiguous requirements or unclear instructions, default to asking for clarification rather than making assumptions. Do not jump into implementation or change files unless clearly instructed to make changes. When the user's intent is ambiguous, provide information, do research, and offer recommendations before taking action. Only proceed with edits, modifications, or implementations when explicitly requested.
</do_not_act_before_instructions>

### What NOT to Do

**Never hard-code values:**

```python
# Bad
db_connection = "postgresql://dataeng:password123@localhost:5432/mydb"

# Good
db_connection = os.getenv('DATABASE_URL')
```

**Never commit sensitive data:**

```bash
# These should be in .gitignore
.env
*.key
secrets.txt
config/credentials.yml
```

**Never use placeholder data in production code:**

```python
# Bad
def load_data():
    # TODO: implement this later
    pass

# Good
def load_data():
    """Load data from source to destination."""
    raise NotImplementedError("load_data not yet implemented")
```

**Never skip error handling:**

```python
# Bad
df = pd.read_csv('data.csv')

# Good
try:
    df = pd.read_csv('data.csv')
except FileNotFoundError:
    logger.error(f"File not found: data.csv")
    raise
except pd.errors.EmptyDataError:
    logger.error("CSV file is empty")
    raise
```

### Performance Considerations

**Use bulk operations:**

```python
# Bad: Row-by-row insertion (very slow)
for row in df.iterrows():
    cursor.execute("INSERT INTO ...", row)

# Good: Bulk insertion
df.to_sql('table', engine, if_exists='append',
          index=False, method='multi', chunksize=1000)
```

**Use appropriate data types:**

```python
# Bad: Using strings for everything
df['date'] = df['date'].astype(str)
df['price'] = df['price'].astype(str)

# Good: Use appropriate types
df['date'] = pd.to_datetime(df['date']).dt.date
df['price'] = df['price'].astype(float)
```

**Optimize SQL queries:**

```sql
-- Bad: Cartesian join
SELECT * FROM stocks, indicators;

-- Good: Proper join with indexes
SELECT s.*, i.*
FROM stocks s
JOIN indicators i ON s.date = i.date
WHERE s.date >= '2024-01-01';
```

### Security Guidelines

**Never log sensitive data:**

```python
# Bad
logger.info(f"Connecting with password: {password}")

# Good
logger.info(f"Connecting to database as user: {username}")
```

**Use parameterized queries:**

```python
# Bad: SQL injection risk
cursor.execute(f"SELECT * FROM stocks WHERE ticker = '{ticker}'")

# Good: Parameterized query
cursor.execute("SELECT * FROM stocks WHERE ticker = %s", (ticker,))
```

### Incremental Development

Build the project incrementally. Do not attempt to create all files at once. Follow this pattern:

1. **Create infrastructure first** (docker-compose.yml, database init scripts)
2. **Verify infrastructure works** (test database connection)
3. **Create extraction modules** (one at a time)
4. **Test each module** before moving to next
5. **Create transformation layer** (dbt models)
6. **Create orchestration** (Airflow DAGs)
7. **Create visualization** (dashboard)

After each major component, verify it works before proceeding.

### Progress Tracking

As you build the project, maintain a progress.txt file:

```text
# progress.txt

## Completed (2025-02-07)
- Project structure created
- Docker Compose configured with PostgreSQL
- Database schemas initialized (raw, staging, analytics)
- Raw layer tables created with proper constraints and indexes

## In Progress
- Implementing stock_extractor.py
  - StockExtractor class skeleton done
  - Need to add: extract_historical method
  - Need to add: load_to_database method

## Next Steps
1. Complete stock_extractor.py implementation
2. Test extraction with 1 year of data
3. Implement bcb_extractor.py
4. Run full backfill for 10 years

## Blockers
None currently

## Notes
- Using yfinance 0.2.35 (latest stable)
- UNIQUE constraint on (ticker, date) prevents duplicates
- Extraction takes ~2 minutes for 20 stocks × 10 years
```

### State Persistence

When implementing long-running processes, save state frequently:

```python
# Good: Save state every N iterations
for i, ticker in enumerate(tickers):
    process_ticker(ticker)

    if i % 5 == 0:  # Every 5 tickers
        save_progress({
            'completed': i,
            'total': len(tickers),
            'last_ticker': ticker
        })
```

### Validation Before Proceeding

Before moving to the next phase, validate the current phase:

```python
# Add validation functions
def validate_extraction_complete():
    """Verify extraction phase completed successfully."""
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT
                COUNT(*) as total_rows,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(DISTINCT ticker) as unique_tickers
            FROM raw.stocks
        """)
        row = result.fetchone()

        assert row.total_rows > 50000, "Expected at least 50k rows"
        assert row.unique_tickers == 20, "Expected 20 tickers"
        assert str(row.earliest_date) == '2015-01-01', "Expected data from 2015"

        logger.info("✓ Extraction validation passed")
        return True
```

---

## Final Instructions

<frontend_aesthetics>
When creating the Streamlit dashboard, avoid generic AI-generated aesthetics. Create a distinctive, professional design:

- Typography: Use interesting fonts, not just Arial or default system fonts
- Color & Theme: Commit to a cohesive financial/data aesthetic with a professional color palette
- Motion: Use Streamlit's animation features for smooth transitions
- Layout: Create visual hierarchy with proper spacing and grouping

The dashboard should feel like a professional financial analytics tool, not a generic Streamlit template.
</frontend_aesthetics>

### Git Workflow

Initialize the repository properly:

```bash
git init
git add .gitignore README.md
git commit -m "chore: initial commit with project structure"

# Create meaningful commits as you build
git add extract/
git commit -m "feat(extraction): implement Yahoo Finance stock extractor

- Add StockExtractor class with historical and incremental methods
- Implement error handling and retry logic
- Add comprehensive logging
- Include unit tests with 85% coverage"
```

### Testing Your Work

Before considering a phase complete:

1. Run all unit tests: `pytest tests/ -v`
2. Run dbt tests: `dbt test`
3. Verify data in database: Check row counts, date ranges
4. Review logs: Ensure no errors or warnings
5. Test edge cases: Empty responses, API failures, duplicate data

### Communication

As you work, provide clear progress updates:

- After completing each file, summarize what was created
- After each test run, report results
- When encountering issues, explain the problem and proposed solution
- When making design decisions, explain the reasoning

### Incremental Commits

Commit frequently with meaningful messages:

```bash
# Good commit sequence
git commit -m "feat(db): add raw schema and tables"
git commit -m "feat(db): add analytics schema and dimensional model"
git commit -m "feat(extraction): implement StockExtractor class"
git commit -m "test(extraction): add unit tests for StockExtractor"
git commit -m "docs(extraction): add docstrings and comments"
```

---

## Success Metrics

This project will be considered complete when:

1. **Functional completeness:**
   - All extraction, transformation, and loading code works
   - Airflow DAG runs successfully
   - Dashboard displays data correctly
   - All tests pass

2. **Code quality:**
   - Test coverage >70%
   - No linting errors (flake8)
   - Consistent formatting (black)
   - Comprehensive documentation

3. **Professional presentation:**
   - Clean, organized repository
   - Professional README with diagrams
   - Clear commit history
   - Deployable with documented instructions

4. **Interview readiness:**
   - Can explain architecture decisions
   - Can demonstrate pipeline execution
   - Can discuss trade-offs and alternatives
   - Can show data quality measures

---

## Getting Started

To begin implementation:

1. Read through this entire specification document
2. Create the project directory structure
3. Initialize Git repository
4. Create docker-compose.yml
5. Start PostgreSQL container
6. Create database schemas
7. Verify database connection
8. Begin Phase 1 implementation

Focus on completing one phase at a time. Verify each phase works before moving to the next. Maintain progress.txt and commit frequently.

This is a complete, production-grade portfolio project that demonstrates modern Data Engineering skills for international remote roles targeting $4-5k USD/month salary range.

**Remember: Build incrementally, test thoroughly, document comprehensively.**