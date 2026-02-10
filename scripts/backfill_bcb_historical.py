#!/usr/bin/env python3
"""
backfill_bcb_historical.py
==========================

Script to backfill all BCB indicator historical data in 5-year batches.

This script extracts data from each indicator's initial recording date
up to the present, loading data in manageable 5-year batches to avoid
memory issues and browser/API timeouts.

Indicator Start Dates:
    - 432 (SELIC): 05/03/1999
    - 1 (USD/BRL): 28/11/1984
    - 433 (IPCA): 01/01/1980
    - 12 (IPCA 12m): 06/03/1986
    - 24369 (CDI daily): 01/03/2012
    - 189 (IGP-M): 01/06/1989
    - 7832 (USD/BRL PTAX): 01/02/1987

Usage:
    python scripts/backfill_bcb_historical.py [--indicator CODE] [--batch-size YEARS]

Examples:
    # Backfill all indicators
    python scripts/backfill_bcb_historical.py

    # Backfill only SELIC
    python scripts/backfill_bcb_historical.py --indicator 432

    # Use 3-year batches instead of 5
    python scripts/backfill_bcb_historical.py --batch-size 3

Author: Dênio Barbosa Júnior
Created: 2025-02-09
"""

import os
import sys
import argparse
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from loguru import logger

from extract.bcb_extractor import BCBExtractor


def setup_logging():
    """Configure logging for the backfill script."""
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"bcb_backfill_{timestamp}.log")

    logger.add(
        log_file,
        rotation="100 MB",
        retention="30 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )

    logger.info(f"Logging to: {log_file}")
    return log_file


def get_database_url():
    """Get database URL from environment or use default."""
    load_dotenv()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        # Construct from individual env vars
        db_user = os.getenv("POSTGRES_USER", "dataeng")
        db_pass = os.getenv("POSTGRES_PASSWORD", "dataeng123")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB", "brazilian_market")
        db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    return db_url


def main():
    """Main entry point for the backfill script."""
    parser = argparse.ArgumentParser(
        description="Backfill BCB historical data in batches"
    )
    parser.add_argument(
        "--indicator",
        type=str,
        help="Specific indicator code to backfill (e.g., 432 for SELIC)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Batch size in years (default: 5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually fetching data",
    )

    args = parser.parse_args()

    # Setup
    log_file = setup_logging()
    db_url = get_database_url()

    logger.info("=" * 70)
    logger.info("BCB HISTORICAL DATA BACKFILL")
    logger.info("=" * 70)
    logger.info(f"Batch size: {args.batch_size} years")
    logger.info(f"Dry run: {args.dry_run}")

    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be fetched or saved")
        show_backfill_plan(args.batch_size, args.indicator)
        return

    try:
        extractor = BCBExtractor(db_url)

        if args.indicator:
            # Backfill single indicator
            logger.info(f"Backfilling single indicator: {args.indicator}")
            rows = extractor.extract_single_indicator_full_history(
                args.indicator,
                batch_size_years=args.batch_size,
            )
            logger.info(f"Backfill complete: {rows} rows inserted")
        else:
            # Backfill all indicators
            logger.info("Backfilling all configured indicators")
            rows = extractor.extract_full_historical_batched(
                batch_size_years=args.batch_size,
                save_after_each_batch=True,
            )
            logger.info(f"Backfill complete: {rows} total rows inserted")

        # Validate
        validation = extractor.validate_extraction()
        logger.info("Validation results:")
        logger.info(f"  Total rows: {validation['total_rows']}")
        logger.info(f"  Date range: {validation['earliest_date']} to {validation['latest_date']}")
        logger.info(f"  Indicators: {validation['unique_indicators']}")
        for code, count in validation['indicator_counts'].items():
            logger.info(f"    {code}: {count} rows")

    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        raise

    logger.info("=" * 70)
    logger.info("BACKFILL COMPLETE")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 70)


def show_backfill_plan(batch_size_years: int, indicator_code: str = None):
    """Show the backfill plan without executing."""
    from datetime import datetime, timedelta
    from extract.config import ExtractionConfig

    config = ExtractionConfig()
    indicators = config.bcb_indicators
    start_dates = config.bcb_indicator_start_dates

    if indicator_code:
        indicators = {indicator_code: indicators.get(indicator_code, "Unknown")}

    end_date = datetime.now()

    logger.info("")
    logger.info("BACKFILL PLAN:")
    logger.info("-" * 50)

    total_batches = 0

    for code, name in indicators.items():
        start_str = start_dates.get(code)
        if not start_str:
            logger.warning(f"  {code} ({name}): No start date configured")
            continue

        start = datetime.strptime(start_str, "%Y-%m-%d")
        years = (end_date - start).days / 365.25
        batches = int(years / batch_size_years) + 1
        total_batches += batches

        logger.info(f"  {code} ({name}):")
        logger.info(f"    Start: {start_str}")
        logger.info(f"    Years of data: {years:.1f}")
        logger.info(f"    Batches needed: {batches}")

    logger.info("-" * 50)
    logger.info(f"Total batches: {total_batches}")
    logger.info(f"Estimated time: {total_batches * 2} - {total_batches * 5} minutes")
    logger.info("")


if __name__ == "__main__":
    main()
