"""
daily_market_etl.py
===================

Daily Market ETL Pipeline DAG for Apache Airflow.

This DAG runs daily at 6:00 AM Brazil time to extract and process
yesterday's market data from Yahoo Finance and BCB API.

Schedule: Daily at 06:00 BRT (09:00 UTC)
Retry: 2 attempts with 5-minute delays
Timeout: 30 minutes per task

Tasks:
    1. extract_stocks - Pull yesterday's stock prices from Yahoo Finance
    2. extract_indicators - Pull yesterday's indicators from BCB API
    3. validate_raw_data - Check data quality before processing
    4. run_dbt_models - Transform data using dbt
    5. run_dbt_tests - Validate transformed data

Dependencies:
    extract_stocks ─┐
                   ├─→ validate_raw_data → run_dbt_models → run_dbt_tests
    extract_indicators ─┘

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Add project modules to path
sys.path.insert(0, "/opt/airflow")

# Configuration
DB_CONN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'dataeng')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'dataeng123')}@"
    f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'brazilian_market')}"
)

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

# Default arguments
default_args = {
    "owner": "denio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def extract_stocks(**context):
    """Extract yesterday's stock data from Yahoo Finance."""
    from extract.stock_extractor import StockExtractor

    logger = context["task_instance"].log
    logger.info("Starting stock extraction...")

    extractor = StockExtractor(DB_CONN)
    df = extractor.extract_incremental(lookback_days=5)
    rows = extractor.load_to_database(df)

    context["ti"].xcom_push(key="stocks_extracted", value=rows)
    logger.info(f"Successfully extracted {rows} stock records")

    return rows


def extract_indicators(**context):
    """Extract yesterday's indicators from BCB API."""
    from extract.bcb_extractor import BCBExtractor

    logger = context["task_instance"].log
    logger.info("Starting BCB indicator extraction...")

    extractor = BCBExtractor(DB_CONN)
    df = extractor.extract_incremental(lookback_days=5)
    rows = extractor.load_to_database(df)

    context["ti"].xcom_push(key="indicators_extracted", value=rows)
    logger.info(f"Successfully extracted {rows} indicator records")

    return rows


def validate_raw_data(**context):
    """Validate data quality before transformation."""
    from sqlalchemy import create_engine, text

    logger = context["task_instance"].log
    logger.info("Validating raw data quality...")

    engine = create_engine(DB_CONN)

    with engine.connect() as conn:
        # Check stocks table
        stocks_result = conn.execute(
            text(
                """
            SELECT
                COUNT(*) as total_rows,
                COUNT(CASE WHEN close_price IS NULL THEN 1 END) as null_prices,
                COUNT(CASE WHEN close_price <= 0 THEN 1 END) as invalid_prices
            FROM raw.stocks
            WHERE loaded_at > NOW() - INTERVAL '1 day'
        """
            )
        )
        stocks_row = stocks_result.fetchone()

        # Check indicators table
        indicators_result = conn.execute(
            text(
                """
            SELECT
                COUNT(*) as total_rows,
                COUNT(CASE WHEN value IS NULL THEN 1 END) as null_values
            FROM raw.indicators
            WHERE loaded_at > NOW() - INTERVAL '1 day'
        """
            )
        )
        indicators_row = indicators_result.fetchone()

    # Validation checks
    if stocks_row[0] == 0:
        raise ValueError("No stock data loaded in the last day")

    if stocks_row[1] > 0:
        logger.warning(f"Found {stocks_row[1]} null prices in stocks")

    if stocks_row[2] > 0:
        raise ValueError(f"Found {stocks_row[2]} invalid (<=0) prices in stocks")

    logger.info(
        f"Validation passed: {stocks_row[0]} stocks, {indicators_row[0]} indicators"
    )

    return {
        "stocks_count": stocks_row[0],
        "indicators_count": indicators_row[0],
    }


# Define DAG
with DAG(
    "daily_market_etl",
    default_args=default_args,
    description="Extract daily Brazilian market data and transform with dbt",
    schedule_interval="0 9 * * *",  # 6 AM BRT = 9 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "market", "daily", "production"],
    doc_md=__doc__,
) as dag:

    # Extraction tasks (parallel)
    with TaskGroup("extraction") as extraction_group:
        extract_stocks_task = PythonOperator(
            task_id="extract_stocks",
            python_callable=extract_stocks,
            provide_context=True,
        )

        extract_indicators_task = PythonOperator(
            task_id="extract_indicators",
            python_callable=extract_indicators,
            provide_context=True,
        )

    # Validation task
    validate_task = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
        provide_context=True,
    )

    # dbt tasks
    with TaskGroup("transformation") as transformation_group:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
        )

        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        )

        dbt_deps >> dbt_run >> dbt_test

    # Task dependencies
    extraction_group >> validate_task >> transformation_group
