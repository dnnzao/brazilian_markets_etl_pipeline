# Architecture Documentation

This document describes the technical architecture of the Brazilian Financial Markets ETL Pipeline, including design decisions, data flow patterns, and component interactions.

## System Overview

The pipeline follows a modern data engineering architecture based on the medallion pattern (Bronze/Silver/Gold layers), implemented using industry-standard tools. The system extracts financial data from external APIs, transforms it through multiple quality gates, and serves it through an interactive dashboard.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                   │
│                                                                             │
│   ┌─────────────────────┐              ┌─────────────────────┐              │
│   │    Yahoo Finance    │              │       BCB API       │              │
│   │                     │              │                     │              │
│   │  - 20 Stock Tickers │              │  - SELIC Rate       │              │
│   │  - Daily OHLCV Data │              │  - USD/BRL Exchange │              │
│   │  - 10 Years History │              │  - IPCA Inflation   │              │
│   │                     │              │  - CDI Rate         │              │
│   └──────────┬──────────┘              └──────────┬──────────┘              │
└──────────────┼──────────────────────────────────────┼───────────────────────┘
               │                                      │
               ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTRACTION LAYER (Python)                           │
│                                                                             │
│   ┌─────────────────────┐              ┌─────────────────────┐              │
│   │   StockExtractor    │              │    BCBExtractor     │              │
│   │                     │              │                     │              │
│   │  - yfinance library │              │  - REST API calls   │              │
│   │  - Ticker normalize │              │  - Date parsing     │              │
│   │  - Rate limiting    │              │  - Error handling   │              │
│   └──────────┬──────────┘              └──────────┬──────────┘              │
└──────────────┼──────────────────────────────────────┼───────────────────────┘
               │                                      │
               ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      RAW SCHEMA (Bronze Layer)                              │
│                                                                             │
│   ┌─────────────────────┐              ┌─────────────────────┐              │
│   │     raw.stocks      │              │   raw.indicators    │              │
│   │                     │              │                     │              │
│   │  - ticker           │              │  - indicator_code   │              │
│   │  - date             │              │  - date             │              │
│   │  - open/high/low    │              │  - value            │              │
│   │  - close/volume     │              │  - unit             │              │
│   │  - adj_close        │              │  - frequency        │              │
│   └──────────┬──────────┘              └──────────┬──────────┘              │
│                                                                             │
│   Pattern: Immutable, append-only, source format preserved                  │
└──────────────┼──────────────────────────────────────┼───────────────────────┘
               │                                      │
               ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STAGING SCHEMA (Silver Layer - dbt)                      │
│                                                                             │
│   ┌─────────────────────┐              ┌─────────────────────┐              │
│   │     stg_stocks      │              │   stg_indicators    │              │
│   │                     │              │                     │              │
│   │  - Data cleaning    │              │  - Value validation │              │
│   │  - Null handling    │              │  - Date formatting  │              │
│   │  - Type casting     │              │  - Unit mapping     │              │
│   └──────────┬──────────┘              └──────────┬──────────┘              │
│                                                                             │
│   Pattern: Cleaned, validated, standardized formats                         │
└──────────────┼──────────────────────────────────────┼───────────────────────┘
               │                                      │
               ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 INTERMEDIATE LAYER (Silver Layer - dbt)                     │
│                                                                             │
│   ┌───────────────┐  ┌────────────────┐  ┌────────────────────┐             │
│   │int_stock_     │  │int_stock_      │  │int_market_         │             │
│   │returns        │  │volatility      │  │indicators          │             │
│   │               │  │                │  │                    │             │
│   │- Daily returns│  │- 30d volatility│  │- Pivot indicators  │             │
│   │- Cumulative   │  │- Annualized    │  │- Forward fill      │             │
│   │- Monthly      │  │- Rolling stats │  │- Daily alignment   │             │
│   └───────┬───────┘  └───────┬────────┘  └─────────┬──────────┘             │
│                                                                             │
│   Pattern: Business calculations, derived metrics                           │
└───────────┼──────────────────┼─────────────────────┼────────────────────────┘
            │                  │                     │
            └──────────────────┼─────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ANALYTICS SCHEMA (Gold Layer - dbt)                      │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │                      fact_daily_market                          │       │
│   │                                                                 │       │
│   │   date_id ──────────────────────────────────────► dim_date      │       │
│   │   stock_id ─────────────────────────────────────► dim_stock     │       │
│   │                                                                 │       │
│   │   Measures: close_price, volume, daily_return, monthly_return   │       │
│   │             volatility_30d, annualized_volatility               │       │
│   │             selic_rate, usd_brl, ipca (denormalized)            │       │
│   └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
│   ┌────────────┐     ┌────────────────┐     ┌────────────────┐              │
│   │  dim_date  │     │   dim_stock    │     │ dim_indicator  │              │
│   │            │     │                │     │                │              │
│   │ - date_id  │     │ - stock_id     │     │ - indicator_id │              │
│   │ - date     │     │ - ticker       │     │ - code         │              │
│   │ - year     │     │ - company_name │     │ - name         │              │
│   │ - quarter  │     │ - sector       │     │ - description  │              │
│   │ - month    │     │ - market_cap   │     │ - unit         │              │
│   │ - weekday  │     │                │     │ - frequency    │              │
│   └────────────┘     └────────────────┘     └────────────────┘              │
│                                                                             │
│   Pattern: Star schema, optimized for analytical queries                    │
└─────────────────────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PRESENTATION LAYER                                  │
│                                                                             │
│   ┌─────────────────────────────────────────────────────────────────┐       │
│   │                    Streamlit Dashboard                          │       │
│   │                                                                 │       │
│   │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │       │
│   │   │   Market     │  │   Sector     │  │    Macro     │          │       │
│   │   │  Overview    │  │  Analysis    │  │ Correlation  │          │       │
│   │   └──────────────┘  └──────────────┘  └──────────────┘          │       │
│   │                                                                 │       │
│   │   ┌──────────────┐                                              │       │
│   │   │    Stock     │                                              │       │
│   │   │  Screener    │                                              │       │
│   │   └──────────────┘                                              │       │
│   └─────────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### Extraction Layer

The extraction layer consists of two Python classes that handle data ingestion from external sources:

**StockExtractor** connects to Yahoo Finance using the yfinance library. It normalizes ticker symbols to the Brazilian format (adding .SA suffix), handles rate limiting to avoid API throttling, and implements retry logic for transient failures. The extractor supports both historical backfill and incremental daily loads.

**BCBExtractor** interfaces with the Brazilian Central Bank's public API. It handles the specific date format used by BCB (dd/MM/yyyy), manages different indicator frequencies (daily, monthly), and implements forward-fill logic for monthly data to align with daily stock prices.

### Database Schema Design

The database follows a three-schema medallion architecture:

**Raw Schema (Bronze)**: Contains tables that mirror the source data exactly. No transformations are applied. Tables include unique constraints on (ticker, date) and (indicator_code, date) to prevent duplicates during incremental loads.

**Staging Schema (Silver)**: Implemented as dbt views that apply data quality rules. Invalid prices (negative or zero) are filtered, dates are validated, and types are cast to appropriate formats.

**Analytics Schema (Gold)**: Contains the dimensional model. The fact table is denormalized for query performance, including key macroeconomic indicators directly in the fact rows to avoid expensive joins during analysis.

### Orchestration with Airflow

Apache Airflow manages workflow scheduling and monitoring through two DAGs:

**daily_market_etl**: Runs at 6 AM Brazil time (09:00 UTC) every day. It extracts the previous day's data with a 5-day lookback window to catch any delayed updates, then triggers dbt to refresh the transformation layers.

**backfill_historical**: A one-time DAG for initial data population. It extracts 10 years of historical data in batches to avoid memory issues and API rate limits.

### Dashboard Architecture

The Streamlit dashboard is organized into four pages, each providing different analytical perspectives:

The **Market Overview** page shows overall market trends, top performers, and recent activity. **Sector Analysis** allows comparison across industry sectors. **Macro Correlation** visualizes relationships between stock performance and economic indicators. The **Stock Screener** provides filtering capabilities for finding stocks matching specific criteria.

## Design Decisions

### Why PostgreSQL over Cloud Data Warehouses?

PostgreSQL was chosen because it is free, runs locally in Docker, and provides sufficient analytical capabilities for this dataset size. Window functions, CTEs, and proper indexing enable performant queries. For a portfolio project, demonstrating PostgreSQL proficiency is valuable since many companies use it.

### Why dbt over Pure Python Transformations?

dbt provides version-controlled SQL transformations, built-in testing, and automatic documentation generation. It's an industry-standard tool that demonstrates modern data engineering practices. The SQL-based approach also makes transformations more accessible to analysts.

### Why Denormalize the Fact Table?

While traditional dimensional modeling suggests keeping dimensions separate, denormalizing key macro indicators (SELIC, USD/BRL) into the fact table significantly improves query performance for the most common analytical patterns. The trade-off of some storage overhead is acceptable for the performance gains.

### Why Docker for Everything?

Containerization ensures reproducible environments across development machines and potential deployment targets. It eliminates "works on my machine" issues and simplifies onboarding for new developers.

## Performance Considerations

The database includes indexes on all foreign key columns and frequently filtered columns (date, ticker). The fact table partitioning by date was considered but deemed unnecessary for the current data volume (~100k rows). If the dataset grows significantly, partitioning would be the next optimization step.

dbt incremental models are used for the fact table to avoid full table rebuilds on daily loads. Only rows with dates greater than the maximum existing date are processed.

## Docker Build Optimization

A `.dockerignore` file at the project root excludes the local virtual environment (`venv/`, ~800 MB), git history, runtime logs, test suites, documentation, and Python bytecache from the Docker build context. This reduces the context transfer from ~800 MB to under 1 MB, making rebuilds near-instantaneous. Each Dockerfile pins only its own runtime dependencies rather than reading from the shared `requirements.txt`, so changes to development-only packages (pytest, black, dbt-core) do not invalidate image layer caches. The dashboard image omits build-time compilers (gcc) since all its Python packages ship as pre-built binary wheels.

## Security Considerations

Sensitive configuration (database credentials, API keys) is stored in environment variables, never in code. The `.env` file is excluded from version control via `.gitignore` and from Docker images via `.dockerignore`. In a production deployment, these would be managed through a secrets manager like AWS Secrets Manager or HashiCorp Vault.
