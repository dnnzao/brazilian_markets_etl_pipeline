#!/usr/bin/env python3
"""
backfill_data.py
================

Run historical data backfill for the ETL pipeline.

This script extracts 10 years of historical data from Yahoo Finance
and BCB API, loading it into the raw layer of the database.

Usage:
    python scripts/backfill_data.py [--start-date 2015-01-01]

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import os
import sys
import argparse
from datetime import datetime

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import create_engine, text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extract.stock_extractor import StockExtractor
from extract.bcb_extractor import BCBExtractor


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill historical market data"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2015-01-01",
        help="Start date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date for backfill (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--stocks-only",
        action="store_true",
        help="Only backfill stock data",
    )
    parser.add_argument(
        "--indicators-only",
        action="store_true",
        help="Only backfill indicator data",
    )
    return parser.parse_args()


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


def backfill_stocks(db_url: str, start_date: str, end_date: str) -> dict:
    """Backfill stock data."""
    logger.info("=" * 60)
    logger.info("STOCK DATA BACKFILL")
    logger.info("=" * 60)

    extractor = StockExtractor(db_url)

    logger.info(f"Extracting stocks from {start_date} to {end_date}")
    df = extractor.extract_historical(start_date=start_date, end_date=end_date)

    logger.info("Loading to database...")
    rows = extractor.load_to_database(df)

    validation = extractor.validate_extraction()
    logger.info(f"Validation: {validation}")

    return validation


def backfill_usd_brl_from_yahoo(db_url: str, start_date: str, end_date: str) -> int:
    """
    Backfill USD/BRL exchange rate from Yahoo Finance.

    This is a fallback in case BCB API fails for exchange rate series.
    Uses the USDBRL=X ticker from Yahoo Finance.
    """
    logger.info("Extracting USD/BRL from Yahoo Finance (fallback)...")

    try:
        ticker = yf.Ticker("USDBRL=X")
        hist = ticker.history(start=start_date, end=end_date)

        if hist.empty:
            logger.warning("No USD/BRL data returned from Yahoo Finance")
            return 0

        # Transform to indicators format
        records = []
        for date, row in hist.iterrows():
            records.append({
                "indicator_code": "USDBRL",
                "indicator_name": "USD_BRL",
                "date": date.date(),
                "value": float(row["Close"]),
                "unit": "BRL/USD",
                "frequency": "daily",
                "loaded_at": datetime.now(),
                "source": "yahoo_finance",
            })

        df = pd.DataFrame(records)
        logger.info(f"  Retrieved {len(df)} rows for USD_BRL")

        # Load to database
        engine = create_engine(db_url)
        temp_table = f"temp_usdbrl_{int(datetime.now().timestamp())}"

        df.to_sql(
            temp_table,
            engine,
            schema="raw",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

        insert_sql = f"""
            INSERT INTO raw.indicators
                (indicator_code, indicator_name, date, value,
                 unit, frequency, loaded_at, source)
            SELECT indicator_code, indicator_name, date, value,
                   unit, frequency, loaded_at, source
            FROM raw.{temp_table}
            ON CONFLICT (indicator_code, date) DO NOTHING
        """

        with engine.begin() as conn:
            conn.execute(text(insert_sql))
            conn.execute(text(f"DROP TABLE IF EXISTS raw.{temp_table}"))

        logger.info(f"  Loaded {len(df)} USD/BRL rows to database")
        return len(df)

    except Exception as e:
        logger.error(f"Failed to extract USD/BRL from Yahoo Finance: {e}")
        return 0


def backfill_indicators(db_url: str, start_date: str, end_date: str) -> dict:
    """Backfill indicator data."""
    logger.info("=" * 60)
    logger.info("INDICATOR DATA BACKFILL")
    logger.info("=" * 60)

    extractor = BCBExtractor(db_url)

    logger.info(f"Extracting indicators from {start_date} to {end_date}")
    logger.info("Note: Daily series (SELIC, USD_BRL, CDI) may take 2+ minutes each...")
    df = extractor.extract_historical(start_date=start_date, end_date=end_date)

    logger.info("Loading to database...")
    rows = extractor.load_to_database(df)

    validation = extractor.validate_extraction()

    # Fetch USD/BRL from Yahoo Finance as fallback if BCB failed
    if "1" not in validation.get("indicator_counts", {}):
        logger.warning("USD_BRL not found from BCB, trying Yahoo Finance fallback...")
        usd_brl_rows = backfill_usd_brl_from_yahoo(db_url, start_date, end_date)
        validation = extractor.validate_extraction()  # Refresh validation

    logger.info(f"Validation: {validation}")

    return validation


def main():
    """Main entry point."""
    load_dotenv()

    args = parse_args()

    # Configure logging
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO",
    )
    logger.add(
        "logs/backfill_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="DEBUG",
    )

    logger.info("=" * 60)
    logger.info("BRAZILIAN MARKET ETL - HISTORICAL BACKFILL")
    logger.info("=" * 60)
    logger.info(f"Start date: {args.start_date}")
    logger.info(f"End date: {args.end_date}")

    db_url = get_db_url()
    results = {}

    try:
        if not args.indicators_only:
            results["stocks"] = backfill_stocks(db_url, args.start_date, args.end_date)

        if not args.stocks_only:
            results["indicators"] = backfill_indicators(db_url, args.start_date, args.end_date)

        logger.info("=" * 60)
        logger.info("BACKFILL COMPLETE")
        logger.info("=" * 60)

        for source, validation in results.items():
            logger.info(f"{source.upper()}:")
            for key, value in validation.items():
                logger.info(f"  {key}: {value}")

        return 0

    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
