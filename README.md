# Brazilian Financial Markets ETL Pipeline

A production-grade data engineering project that extracts Brazilian stock market data and macroeconomic indicators, transforms them into a dimensional data warehouse, and provides interactive analysis through a Streamlit dashboard.

This project demonstrates modern Data Engineering practices including multi-source data ingestion, data transformation with dbt, workflow orchestration with Apache Airflow, dimensional modeling using a star schema, comprehensive data quality testing, and full containerization with Docker.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                   │
│                                                                             │
│      ┌─────────────────┐                    ┌─────────────────┐             │
│      │  Yahoo Finance  │                    │    BCB API      │             │
│      │   (20 Stocks)   │                    │  (7 Indicators) │             │
│      └────────┬────────┘                    └────────┬────────┘             │
└───────────────┼──────────────────────────────────────┼──────────────────────┘
                │                                      │
                ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTRACTION LAYER (Python)                           │
│                   StockExtractor       BCBExtractor                         │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      RAW SCHEMA (Bronze Layer)                              │
│                   raw.stocks       raw.indicators                           │
│               Immutable, append-only, source format                         │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    STAGING SCHEMA (Silver Layer - dbt)                      │
│                  stg_stocks       stg_indicators                            │
│              Cleaned, validated, data quality applied                       │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 INTERMEDIATE LAYER (Silver Layer - dbt)                     │
│       int_stock_returns   int_stock_volatility   int_market_indicators      │
│                    Business logic, calculations applied                     │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    ANALYTICS SCHEMA (Gold Layer - dbt)                      │
│                                                                             │
│                         fact_daily_market                                   │
│                               │                                             │
│         ┌─────────────────────┼─────────────────────┐                       │
│         ▼                     ▼                     ▼                       │
│     dim_date              dim_stock           dim_indicator                 │
│                                                                             │
│              Star Schema - Optimized for Analytical Queries                 │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PRESENTATION LAYER                                  │
│                                                                             │
│                        Streamlit Dashboard                                  │
│      Market Overview | Sector Analysis | Macro Correlation | Screener       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features

The pipeline extracts stock price data for 20 major Brazilian companies from Yahoo Finance and 7 key macroeconomic indicators from the Brazilian Central Bank API. Data is processed through a three-layer medallion architecture using dbt for transformations. Apache Airflow orchestrates daily incremental loads. The dimensional model follows a star schema optimized for analytical queries. Comprehensive data quality tests (39 dbt tests) ensure data integrity at every layer. A multi-page Streamlit dashboard provides interactive visualizations.

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Python 3.11+ | Data processing and orchestration |
| Orchestration | Apache Airflow 2.8 | Workflow scheduling and monitoring |
| Database | PostgreSQL 15 | Data warehouse |
| Transformation | dbt Core 1.7 | SQL-based data transformations |
| Visualization | Streamlit | Interactive dashboard |
| Containerization | Docker Compose | Full environment management |

## Data Sources

**Yahoo Finance (via yfinance)** provides daily OHLCV data for 20 Brazilian stocks including Petrobras (PETR4), Vale (VALE3), Itaú (ITUB4), Bradesco (BBDC4), Ambev (ABEV3), and others representing major sectors of the Brazilian economy.

**Brazilian Central Bank API** provides macroeconomic indicators including SELIC base interest rate, IPCA consumer inflation, USD/BRL exchange rate, CDI interbank rate, unemployment rate, and IGP-M market price index.

## Prerequisites

Before setting up this project, ensure you have installed:

- Docker (version 20.10+)
- Docker Compose (version 2.0+)
- Git

## Quick Start

Clone the repository and start all services with a single command:

```bash
# Clone the repository
git clone https://github.com/your-username/brazilian-market-etl.git
cd brazilian-market-etl

# Copy environment configuration
cp .env.example .env

# Start all services (PostgreSQL, Airflow, Dashboard, dbt)
sudo ./scripts/start_project.sh
```

After startup completes, access:
- **Dashboard**: http://localhost:8501
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Database**: localhost:5432

To stop all services:

```bash
sudo ./scripts/close_project.sh
```

## Project Structure

```
brazilian-market-etl/
├── README.md                    # Project documentation
├── LICENSE                      # MIT License
├── .env.example                 # Environment template
├── .gitignore                   # Git ignore rules
├── .dockerignore                # Docker build context exclusions
├── docker-compose.yml           # Container orchestration
├── requirements.txt             # Python dependencies
│
├── docker/                      # Docker configurations
│   ├── Dockerfile.airflow       # Airflow custom image
│   └── Dockerfile.dashboard     # Dashboard image
│
├── scripts/                     # Utility scripts
│   ├── start_project.sh         # Start all Docker services
│   ├── close_project.sh         # Stop all Docker services
│   ├── verifying_project.sh     # Verify installation
│   ├── setup_db.sh              # Database initialization
│   └── run_dbt.sh               # Run dbt commands
│
├── docs/                        # Documentation
│   ├── architecture.md          # Architecture decisions
│   ├── data_dictionary.md       # Table/column definitions
│   └── setup_guide.md           # Detailed setup guide
│
├── airflow/                     # Airflow DAGs
│   └── dags/
│       ├── daily_market_etl.py
│       └── backfill_historical.py
│
├── dashboard/                   # Streamlit application
│   ├── .streamlit/
│   │   └── config.toml          # Streamlit server configuration
│   ├── app.py
│   ├── pages/
│   │   ├── 1_Market_Overview.py
│   │   ├── 2_Sector_Analysis.py
│   │   ├── 3_Macro_Correlation.py
│   │   ├── 4_Stock_Screener.py
│   │   └── 5_Historical_Analysis.py
│   └── components/
│       ├── charts.py
│       └── queries.py
│
├── database/                    # Database initialization
│   └── init/
│       ├── 01_create_schemas.sql
│       ├── 02_create_raw_tables.sql
│       └── 03_create_analytics_tables.sql
│
├── dbt_project/                 # dbt transformation
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   └── macros/
│
├── extract/                     # Python extractors
│   ├── stock_extractor.py
│   ├── bcb_extractor.py
│   └── config.py
│
├── load/                        # Database loader
│   └── db_loader.py
│
└── tests/                       # Unit tests
    ├── test_stock_extractor.py
    ├── test_bcb_extractor.py
    └── test_db_loader.py
```

## Running dbt Transformations

After the database contains raw data, run dbt to build the dimensional model:

```bash
# Run dbt models
sudo docker exec dbt_runner dbt run

# Run dbt tests (39 data quality tests)
sudo docker exec dbt_runner dbt test

# Generate documentation
sudo docker exec dbt_runner dbt docs generate
```

## Running Tests

Execute Python unit tests:

```bash
# Activate virtual environment (for local development)
source venv/bin/activate

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=extract --cov=load
```

## Sample Analytical Queries

Query the dimensional model to analyze stock performance:

```sql
-- Find stocks with positive correlation to SELIC rate
SELECT
    s.ticker,
    s.sector,
    AVG(f.daily_return) as avg_return,
    CORR(f.daily_return, f.selic_rate) as selic_correlation
FROM analytics.fact_daily_market f
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
GROUP BY s.ticker, s.sector
HAVING CORR(f.daily_return, f.selic_rate) > 0.1
ORDER BY selic_correlation DESC;
```

```sql
-- Sector performance by SELIC category
SELECT
    s.sector,
    f.selic_category,
    AVG(f.monthly_return) * 100 as avg_monthly_return,
    AVG(f.volatility_30d) * 100 as avg_volatility
FROM analytics.fact_daily_market f
JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
GROUP BY s.sector, f.selic_category
ORDER BY s.sector, f.selic_category;
```

## Environment Variables

Configure these variables in your `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| POSTGRES_USER | Database username | dataeng |
| POSTGRES_PASSWORD | Database password | dataeng123 |
| POSTGRES_DB | Database name | brazilian_market |
| POSTGRES_PORT | Database port | 5432 |
| AIRFLOW_ADMIN_USER | Airflow UI username | admin |
| AIRFLOW_ADMIN_PASSWORD | Airflow UI password | admin |
| DASHBOARD_PORT | Streamlit port | 8501 |

## Documentation

Detailed documentation is available in the `docs/` directory:

- **[Architecture](docs/architecture.md)** - System design and component details
- **[Data Dictionary](docs/data_dictionary.md)** - Table and column definitions
- **[Setup Guide](docs/setup_guide.md)** - Step-by-step installation guide

## Troubleshooting

**Database connection fails:**
Check if PostgreSQL container is running: `sudo docker compose ps`

**dbt models fail:**
Ensure raw tables have data before running dbt. Check with:
`sudo docker exec brazilian_market_db psql -U dataeng -d brazilian_market -c "SELECT COUNT(*) FROM raw.stocks;"`

**Dashboard shows no data:**
Run the dbt models first to populate the analytics schema.

**Port already in use:**
Stop conflicting services or change ports in `.env` file.

## Future Enhancements

Planned improvements include adding more Brazilian stocks and international indices, implementing real-time data streaming, deploying to AWS with S3 and RDS, and adding ML models for price prediction.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) file for details.

## Author

**Dênio Barbosa Júnior**

This project was created as a portfolio piece demonstrating modern Data Engineering skills for international remote roles. It showcases proficiency in Python, SQL, Apache Airflow, dbt, PostgreSQL, Docker, and data visualization.

---

*Built with Python, PostgreSQL, Apache Airflow, dbt, and Streamlit*
