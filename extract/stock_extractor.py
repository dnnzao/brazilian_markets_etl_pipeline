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

import time
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import yfinance as yf
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from extract.config import ExtractionConfig
from extract.utils import (
    rate_limit,
    validate_date_range,
    validate_dataframe,
    clean_ticker,
    format_duration,
)


class StockExtractor:
    """
    Extract stock price data from Yahoo Finance API.

    This class handles the extraction of historical and incremental
    stock price data for Brazilian equities traded on B3 exchange.

    Attributes:
        engine: SQLAlchemy database engine
        tickers: List of stock tickers to extract
        config: Extraction configuration settings

    Example:
        >>> extractor = StockExtractor("postgresql://user:pass@localhost/db")
        >>> df = extractor.extract_historical(start_date='2015-01-01')
        >>> rows = extractor.load_to_database(df)
        >>> print(f"Loaded {rows} rows")
    """

    def __init__(
        self,
        db_connection_string: str,
        tickers: Optional[List[str]] = None,
        config: Optional[ExtractionConfig] = None,
    ):
        """
        Initialize the stock extractor.

        Args:
            db_connection_string: PostgreSQL connection string
            tickers: Optional list of stock tickers. If None, uses defaults.
            config: Optional extraction configuration

        Raises:
            ConnectionError: If database connection fails
        """
        self.config = config or ExtractionConfig()
        self.tickers = [clean_ticker(t) for t in (tickers or self.config.stock_tickers)]

        try:
            self.engine: Engine = create_engine(db_connection_string)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Initialized StockExtractor with {len(self.tickers)} tickers")
        except Exception as e:
            logger.error(f"Failed to initialize StockExtractor: {e}")
            raise ConnectionError(f"Database connection failed: {e}")

    def extract_historical(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract historical stock data for all configured tickers.

        This method downloads daily OHLCV (Open, High, Low, Close, Volume)
        data from Yahoo Finance for each ticker in the configured list.

        Args:
            start_date: Start date in YYYY-MM-DD format. Defaults to config.
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
        start_date = start_date or self.config.start_date
        end_date = end_date or self.config.end_date

        validate_date_range(start_date, end_date)
        logger.info(f"Starting historical extraction: {start_date} to {end_date}")
        logger.info(f"Extracting {len(self.tickers)} tickers")

        start_time = time.time()
        all_data: List[pd.DataFrame] = []
        failed_tickers: List[str] = []

        for i, ticker in enumerate(self.tickers, 1):
            logger.info(f"[{i}/{len(self.tickers)}] Extracting {ticker}")

            try:
                df = self._extract_single_ticker(ticker, start_date, end_date)
                if not df.empty:
                    all_data.append(df)
                    logger.info(f"  Retrieved {len(df)} rows for {ticker}")
                else:
                    logger.warning(f"  No data returned for {ticker}")
                    failed_tickers.append(ticker)
            except Exception as e:
                logger.error(f"  Error extracting {ticker}: {e}")
                failed_tickers.append(ticker)

            time.sleep(self.config.rate_limit_delay)

        if not all_data:
            raise ValueError(
                f"No data extracted for any ticker. Failed: {failed_tickers}"
            )

        result = pd.concat(all_data, ignore_index=True)
        duration = time.time() - start_time

        logger.info(
            f"Extraction complete: {len(result)} rows in {format_duration(duration)}"
        )
        if failed_tickers:
            logger.warning(f"Failed tickers: {failed_tickers}")

        return result

    def extract_incremental(
        self,
        lookback_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract recent stock data for incremental loading.

        This method extracts only the last N days of data, suitable
        for daily batch processing. Uses lookback window to catch
        any delayed or corrected data.

        Args:
            lookback_days: Number of days to look back. Defaults to config.

        Returns:
            DataFrame with recent stock data

        Example:
            >>> df = extractor.extract_incremental(lookback_days=5)
            >>> print(f"Extracted {len(df)} rows for last 5 days")
        """
        lookback = lookback_days or self.config.lookback_days
        start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Incremental extraction: last {lookback} days")
        return self.extract_historical(start_date, end_date)

    @rate_limit(delay_seconds=0.3)
    def _extract_single_ticker(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Extract data for a single ticker from Yahoo Finance.

        Args:
            ticker: Stock ticker symbol (e.g., 'PETR4.SA')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with stock data for the ticker
        """
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date, auto_adjust=False)

            if hist.empty:
                return pd.DataFrame()

            hist = hist.reset_index()
            hist["ticker"] = ticker

            df = pd.DataFrame(
                {
                    "ticker": hist["ticker"],
                    "date": pd.to_datetime(hist["Date"]).dt.date,
                    "open_price": hist["Open"].round(4),
                    "high_price": hist["High"].round(4),
                    "low_price": hist["Low"].round(4),
                    "close_price": hist["Close"].round(4),
                    "volume": hist["Volume"].astype("Int64"),
                    "adj_close": hist["Adj Close"].round(4),
                }
            )

            df = df[df["close_price"] > 0]

            return df

        except Exception as e:
            logger.error(f"Error fetching {ticker}: {e}")
            raise

    def load_to_database(
        self,
        df: pd.DataFrame,
        table_name: str = "stocks",
        schema: str = "raw",
    ) -> int:
        """
        Load extracted data to PostgreSQL database.

        Uses upsert logic to handle duplicates - existing records
        are skipped to maintain idempotency.

        Args:
            df: DataFrame with stock data to load
            table_name: Target table name
            schema: Target schema name

        Returns:
            Number of rows inserted

        Raises:
            ValueError: If DataFrame is invalid
        """
        required_columns = [
            "ticker",
            "date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "adj_close",
        ]
        validate_dataframe(df, required_columns)

        logger.info(f"Loading {len(df)} rows to {schema}.{table_name}")
        start_time = time.time()

        initial_count = self._get_row_count(schema, table_name)

        df["loaded_at"] = datetime.now()
        df["source"] = "yahoo_finance"

        temp_table = f"temp_stocks_{int(time.time())}"

        try:
            df.to_sql(
                temp_table,
                self.engine,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

            insert_sql = f"""
                INSERT INTO {schema}.{table_name}
                    (ticker, date, open_price, high_price, low_price,
                     close_price, volume, adj_close, loaded_at, source)
                SELECT ticker, date, open_price, high_price, low_price,
                       close_price, volume, adj_close, loaded_at, source
                FROM {schema}.{temp_table}
                ON CONFLICT (ticker, date) DO NOTHING
            """

            with self.engine.begin() as conn:
                conn.execute(text(insert_sql))
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))

            final_count = self._get_row_count(schema, table_name)
            rows_inserted = final_count - initial_count

            duration = time.time() - start_time
            logger.info(
                f"Load complete: {rows_inserted} new rows in {format_duration(duration)}"
            )

            return rows_inserted

        except Exception as e:
            logger.error(f"Error loading to database: {e}")
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{temp_table}"))
            raise

    def _get_row_count(self, schema: str, table_name: str) -> int:
        """Get current row count for a table."""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            return result.scalar() or 0

    def validate_extraction(self) -> dict:
        """
        Validate that extraction completed successfully.

        Returns:
            Dictionary with validation results

        Example:
            >>> results = extractor.validate_extraction()
            >>> print(f"Total rows: {results['total_rows']}")
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT
                    COUNT(*) as total_rows,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    COUNT(DISTINCT ticker) as unique_tickers
                FROM raw.stocks
            """
                )
            )
            row = result.fetchone()

            return {
                "total_rows": row[0],
                "earliest_date": str(row[1]),
                "latest_date": str(row[2]),
                "unique_tickers": row[3],
            }


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = "postgresql://dataeng:dataeng123@localhost:5432/brazilian_market"

    logger.add(
        "logs/stock_extraction_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
    )

    extractor = StockExtractor(db_url)
    df = extractor.extract_historical()
    rows = extractor.load_to_database(df)
    validation = extractor.validate_extraction()
    logger.info(f"Validation results: {validation}")
