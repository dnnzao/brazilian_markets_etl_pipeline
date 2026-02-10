"""
backfill_historical.py
======================

Historical Backfill DAG for initial data loading.

This DAG is used for the initial historical data load and should
be run manually once during project setup. It extracts 10 years
of historical data from Yahoo Finance and BCB API.

Schedule: Manual trigger only (@once)
Timeout: 2 hours per task (historical loads take longer)

Tasks:
    1. backfill_stocks - Load 10 years of stock data
    2. backfill_indicators - Load 10 years of indicator data
    3. validate_backfill - Verify data completeness
    4. run_full_dbt - Run dbt with full refresh

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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
START_DATE = "2015-01-01"

# Default arguments
default_args = {
    "owner": "denio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}


def backfill_stocks(**context):
    """Backfill historical stock data from 2015."""
    from extract.stock_extractor import StockExtractor

    logger = context["task_instance"].log
    logger.info(f"Starting historical stock backfill from {START_DATE}...")

    extractor = StockExtractor(DB_CONN)
    df = extractor.extract_historical(start_date=START_DATE)
    rows = extractor.load_to_database(df)

    validation = extractor.validate_extraction()
    logger.info(f"Validation: {validation}")

    context["ti"].xcom_push(key="stocks_backfilled", value=rows)
    context["ti"].xcom_push(key="stocks_validation", value=validation)

    return rows


def backfill_indicators(**context):
    """Backfill historical indicator data from 2015."""
    from extract.bcb_extractor import BCBExtractor

    logger = context["task_instance"].log
    logger.info(f"Starting historical indicator backfill from {START_DATE}...")

    extractor = BCBExtractor(DB_CONN)
    df = extractor.extract_historical(start_date=START_DATE)
    rows = extractor.load_to_database(df)

    validation = extractor.validate_extraction()
    logger.info(f"Validation: {validation}")

    context["ti"].xcom_push(key="indicators_backfilled", value=rows)
    context["ti"].xcom_push(key="indicators_validation", value=validation)

    return rows


def validate_backfill(**context):
    """Validate that backfill completed successfully."""
    from sqlalchemy import create_engine, text

    logger = context["task_instance"].log

    # Get XCom values
    ti = context["ti"]
    stocks_validation = ti.xcom_pull(
        task_ids="backfill_stocks", key="stocks_validation"
    )
    indicators_validation = ti.xcom_pull(
        task_ids="backfill_indicators", key="indicators_validation"
    )

    engine = create_engine(DB_CONN)

    with engine.connect() as conn:
        # Verify minimum data requirements
        stocks_count = conn.execute(
            text("SELECT COUNT(*) FROM raw.stocks")
        ).scalar()

        indicators_count = conn.execute(
            text("SELECT COUNT(*) FROM raw.indicators")
        ).scalar()

        unique_tickers = conn.execute(
            text("SELECT COUNT(DISTINCT ticker) FROM raw.stocks")
        ).scalar()

        unique_indicators = conn.execute(
            text("SELECT COUNT(DISTINCT indicator_code) FROM raw.indicators")
        ).scalar()

    # Validation checks
    assert stocks_count >= 50000, f"Expected 50k+ stock rows, got {stocks_count}"
    assert indicators_count >= 20000, f"Expected 20k+ indicator rows, got {indicators_count}"
    assert unique_tickers >= 15, f"Expected 15+ tickers, got {unique_tickers}"
    assert unique_indicators >= 5, f"Expected 5+ indicators, got {unique_indicators}"

    logger.info("=" * 60)
    logger.info("BACKFILL VALIDATION PASSED")
    logger.info(f"  Stocks: {stocks_count:,} rows, {unique_tickers} tickers")
    logger.info(f"  Indicators: {indicators_count:,} rows, {unique_indicators} indicators")
    logger.info("=" * 60)

    return {
        "stocks_count": stocks_count,
        "indicators_count": indicators_count,
        "unique_tickers": unique_tickers,
        "unique_indicators": unique_indicators,
    }


# Define DAG
with DAG(
    "backfill_historical",
    default_args=default_args,
    description="One-time historical data backfill (2015-present)",
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "backfill", "historical", "setup"],
    doc_md=__doc__,
) as dag:

    backfill_stocks_task = PythonOperator(
        task_id="backfill_stocks",
        python_callable=backfill_stocks,
        provide_context=True,
    )

    backfill_indicators_task = PythonOperator(
        task_id="backfill_indicators",
        python_callable=backfill_indicators,
        provide_context=True,
    )

    validate_backfill_task = PythonOperator(
        task_id="validate_backfill",
        python_callable=validate_backfill,
        provide_context=True,
    )

    # --log-path /tmp/dbt_logs avoids PermissionError on the volume-mounted
    # dbt_project/logs/dbt.log (owned by root, but Airflow runs as UID 50000).
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --log-path /tmp/dbt_logs",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --log-path /tmp/dbt_logs",
    )

    dbt_run = BashOperator(
        task_id="dbt_run_full_refresh",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --full-refresh --log-path /tmp/dbt_logs",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --log-path /tmp/dbt_logs",
    )

    # Task dependencies
    [backfill_stocks_task, backfill_indicators_task] >> validate_backfill_task
    validate_backfill_task >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
