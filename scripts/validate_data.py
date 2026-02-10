#!/usr/bin/env python3
"""
validate_data.py
================

Validate data quality in the database.

This script runs data quality checks on all layers of the
data warehouse and reports any issues found.

Usage:
    python scripts/validate_data.py

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text


# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_db_url():
    """Get database connection URL from environment."""
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "dataeng")
    password = os.getenv("POSTGRES_PASSWORD", "dataeng123")
    database = os.getenv("POSTGRES_DB", "brazilian_market")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def check_raw_stocks(conn) -> dict:
    """Check raw stocks table quality."""
    logger.info("Checking raw.stocks...")

    result = conn.execute(text("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT ticker) as unique_tickers,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(CASE WHEN close_price IS NULL THEN 1 END) as null_prices,
            COUNT(CASE WHEN close_price <= 0 THEN 1 END) as invalid_prices,
            COUNT(CASE WHEN high_price < low_price THEN 1 END) as invalid_ranges
        FROM raw.stocks
    """))
    row = result.fetchone()

    return {
        "table": "raw.stocks",
        "total_rows": row[0],
        "unique_tickers": row[1],
        "date_range": f"{row[2]} to {row[3]}",
        "null_prices": row[4],
        "invalid_prices": row[5],
        "invalid_ranges": row[6],
        "status": "PASS" if row[4] == 0 and row[5] == 0 and row[6] == 0 else "FAIL",
    }


def check_raw_indicators(conn) -> dict:
    """Check raw indicators table quality."""
    logger.info("Checking raw.indicators...")

    result = conn.execute(text("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT indicator_code) as unique_indicators,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(CASE WHEN value IS NULL THEN 1 END) as null_values
        FROM raw.indicators
    """))
    row = result.fetchone()

    return {
        "table": "raw.indicators",
        "total_rows": row[0],
        "unique_indicators": row[1],
        "date_range": f"{row[2]} to {row[3]}",
        "null_values": row[4],
        "status": "PASS" if row[4] == 0 else "FAIL",
    }


def check_fact_table(conn) -> dict:
    """Check analytics fact table quality."""
    logger.info("Checking analytics.fact_daily_market...")

    try:
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT stock_id) as unique_stocks,
                COUNT(CASE WHEN close_price IS NULL THEN 1 END) as null_prices,
                COUNT(CASE WHEN close_price <= 0 THEN 1 END) as invalid_prices,
                COUNT(CASE WHEN daily_return > 1 OR daily_return < -1 THEN 1 END) as extreme_returns
            FROM analytics.fact_daily_market
        """))
        row = result.fetchone()

        return {
            "table": "analytics.fact_daily_market",
            "total_rows": row[0],
            "unique_stocks": row[1],
            "null_prices": row[2],
            "invalid_prices": row[3],
            "extreme_returns": row[4],
            "status": "PASS" if row[2] == 0 and row[3] == 0 else "FAIL",
        }
    except Exception as e:
        return {
            "table": "analytics.fact_daily_market",
            "error": str(e),
            "status": "NOT FOUND",
        }


def main():
    """Main entry point."""
    load_dotenv()

    # Configure logging
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO",
    )

    logger.info("=" * 60)
    logger.info("DATA VALIDATION REPORT")
    logger.info(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    db_url = get_db_url()
    engine = create_engine(db_url)

    all_passed = True
    results = []

    with engine.connect() as conn:
        # Check raw layer
        results.append(check_raw_stocks(conn))
        results.append(check_raw_indicators(conn))

        # Check analytics layer
        results.append(check_fact_table(conn))

    # Report results
    logger.info("")
    logger.info("VALIDATION RESULTS")
    logger.info("-" * 60)

    for result in results:
        status = result.get("status", "UNKNOWN")
        status_color = "green" if status == "PASS" else "red"
        logger.info(f"\n{result['table']}:")

        for key, value in result.items():
            if key not in ["table", "status"]:
                logger.info(f"  {key}: {value}")

        if status == "PASS":
            logger.info(f"  Status: ✓ PASS")
        elif status == "NOT FOUND":
            logger.warning(f"  Status: ⚠ NOT FOUND (run dbt first)")
        else:
            logger.error(f"  Status: ✗ FAIL")
            all_passed = False

    logger.info("")
    logger.info("=" * 60)

    if all_passed:
        logger.info("ALL VALIDATIONS PASSED ✓")
        return 0
    else:
        logger.error("SOME VALIDATIONS FAILED ✗")
        return 1


if __name__ == "__main__":
    sys.exit(main())
