"""
bcb_extractor.py
================

This module handles extraction of economic indicators from the
Brazilian Central Bank (BCB) API.

The BCBExtractor class provides methods to:
- Extract historical economic indicators (SELIC, IPCA, USD/BRL, etc.)
- Extract incremental updates for daily processing
- Load data to PostgreSQL raw layer with deduplication

Key Features:
- Support for 7 key Brazilian economic indicators
- Automatic date format conversion (DD/MM/YYYY to YYYY-MM-DD)
- Rate limiting to respect API constraints
- Comprehensive error handling and logging

BCB API Documentation:
    https://dadosabertos.bcb.gov.br/dataset/taxas-de-juros-diarias

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import pandas as pd
import requests
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from extract.config import ExtractionConfig
from extract.utils import (
    rate_limit,
    with_retry,
    validate_date_range,
    validate_dataframe,
    parse_bcb_date,
    format_duration,
)


class BCBExtractor:
    """
    Extract economic indicators from Brazilian Central Bank API.

    This class handles the extraction of macroeconomic indicators
    including interest rates, inflation indices, and exchange rates.

    Supported Indicators:
        - 432: SELIC daily rate (base interest rate)
        - 433: IPCA (consumer price inflation, monthly)
        - 1: USD/BRL daily exchange rate
        - 4389: CDI daily rate (interbank rate)
        - 24363: Unemployment rate (monthly)
        - 189: IGP-M (market price index, monthly)
        - 4380: SELIC target rate

    Note: Daily series (432, 1, 4389) may take 2+ minutes to fetch due to large data volume

    Attributes:
        engine: SQLAlchemy database engine
        indicators: Dictionary of indicator codes and names
        config: Extraction configuration settings

    Example:
        >>> extractor = BCBExtractor("postgresql://user:pass@localhost/db")
        >>> df = extractor.extract_historical(start_date='2015-01-01')
        >>> rows = extractor.load_to_database(df)
        >>> print(f"Loaded {rows} rows")
    """

    INDICATOR_FREQUENCIES = {
        "432": "daily",    # SELIC daily
        "433": "monthly",  # IPCA
        "1": "daily",      # USD/BRL daily
        "12": "monthly",   # IPCA 12m accumulated
        "24369": "daily",  # CDI daily
        "189": "monthly",  # IGP-M
        "7832": "daily",   # USD/BRL PTAX
    }

    INDICATOR_UNITS = {
        "432": "% per day",
        "433": "% monthly",
        "1": "BRL/USD",
        "12": "% 12 months",
        "24369": "% per day",
        "189": "% monthly",
        "7832": "BRL/USD",
    }

    # Batch size in years for historical extraction
    BATCH_SIZE_YEARS = 5

    def __init__(
        self,
        db_connection_string: str,
        indicators: Optional[Dict[str, str]] = None,
        config: Optional[ExtractionConfig] = None,
    ):
        """
        Initialize the BCB extractor.

        Args:
            db_connection_string: PostgreSQL connection string
            indicators: Optional dict of indicator codes/names
            config: Optional extraction configuration

        Raises:
            ConnectionError: If database connection fails
        """
        self.config = config or ExtractionConfig()
        self.indicators = indicators or self.config.bcb_indicators

        try:
            self.engine: Engine = create_engine(db_connection_string)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(
                f"Initialized BCBExtractor with {len(self.indicators)} indicators"
            )
        except Exception as e:
            logger.error(f"Failed to initialize BCBExtractor: {e}")
            raise ConnectionError(f"Database connection failed: {e}")

    def extract_historical(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Extract historical indicator data from BCB API.

        Downloads data for all configured indicators within the
        specified date range.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with columns: indicator_code, indicator_name,
                                   date, value, unit, frequency

        Raises:
            ValueError: If no data extracted for any indicator
        """
        start_date = start_date or self.config.start_date
        end_date = end_date or self.config.end_date

        validate_date_range(start_date, end_date)
        logger.info(f"Starting BCB extraction: {start_date} to {end_date}")
        logger.info(f"Extracting {len(self.indicators)} indicators")

        start_time = time.time()
        all_data: List[pd.DataFrame] = []
        failed_indicators: List[str] = []

        for i, (code, name) in enumerate(self.indicators.items(), 1):
            logger.info(f"[{i}/{len(self.indicators)}] Extracting {name} ({code})")

            try:
                df = self._extract_single_indicator(code, name, start_date, end_date)
                if not df.empty:
                    all_data.append(df)
                    logger.info(f"  Retrieved {len(df)} rows for {name}")
                else:
                    logger.warning(f"  No data returned for {name}")
                    failed_indicators.append(code)
            except Exception as e:
                logger.error(f"  Error extracting {name}: {e}")
                failed_indicators.append(code)

            time.sleep(self.config.rate_limit_delay)

        if not all_data:
            raise ValueError(
                f"No data extracted for any indicator. Failed: {failed_indicators}"
            )

        result = pd.concat(all_data, ignore_index=True)
        duration = time.time() - start_time

        logger.info(
            f"Extraction complete: {len(result)} rows in {format_duration(duration)}"
        )
        if failed_indicators:
            logger.warning(f"Failed indicators: {failed_indicators}")

        return result

    def extract_incremental(
        self,
        lookback_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Extract recent indicator data for incremental loading.

        Args:
            lookback_days: Number of days to look back

        Returns:
            DataFrame with recent indicator data
        """
        lookback = lookback_days or self.config.lookback_days
        start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Incremental extraction: last {lookback} days")
        return self.extract_historical(start_date, end_date)

    def extract_full_historical_batched(
        self,
        batch_size_years: int = 5,
        save_after_each_batch: bool = True,
    ) -> int:
        """
        Extract full historical data in batches from each indicator's start date.

        This method fetches data in batches of N years to avoid overwhelming
        the browser/memory and respects BCB API limits.

        Args:
            batch_size_years: Number of years per batch (default: 5)
            save_after_each_batch: Whether to save to database after each batch

        Returns:
            Total number of rows inserted across all batches
        """
        logger.info("=" * 60)
        logger.info("STARTING FULL HISTORICAL BATCH EXTRACTION")
        logger.info("=" * 60)

        total_rows_inserted = 0
        end_date = datetime.now()

        # Get indicator start dates from config
        indicator_start_dates = self.config.bcb_indicator_start_dates

        for code, name in self.indicators.items():
            logger.info("-" * 60)
            logger.info(f"Processing indicator: {name} ({code})")

            # Get the indicator's start date
            start_date_str = indicator_start_dates.get(code)
            if not start_date_str:
                logger.warning(f"  No start date configured for {code}, skipping")
                continue

            indicator_start = datetime.strptime(start_date_str, "%Y-%m-%d")
            logger.info(f"  Historical range: {start_date_str} to {end_date.strftime('%Y-%m-%d')}")

            # Calculate batches
            batches = self._calculate_batches(indicator_start, end_date, batch_size_years)
            logger.info(f"  Will process {len(batches)} batches of ~{batch_size_years} years each")

            indicator_rows = 0

            for batch_num, (batch_start, batch_end) in enumerate(batches, 1):
                batch_start_str = batch_start.strftime("%Y-%m-%d")
                batch_end_str = batch_end.strftime("%Y-%m-%d")

                logger.info(f"  Batch {batch_num}/{len(batches)}: {batch_start_str} to {batch_end_str}")

                try:
                    df = self._extract_single_indicator(
                        code, name, batch_start_str, batch_end_str
                    )

                    if df.empty:
                        logger.warning(f"    No data returned for this batch")
                        continue

                    logger.info(f"    Retrieved {len(df)} rows")

                    if save_after_each_batch:
                        rows = self.load_to_database(df)
                        indicator_rows += rows
                        logger.info(f"    Saved {rows} new rows to database")
                    else:
                        indicator_rows += len(df)

                    # Rate limiting between batches
                    time.sleep(self.config.rate_limit_delay * 2)

                except Exception as e:
                    logger.error(f"    Error in batch {batch_num}: {e}")
                    continue

            total_rows_inserted += indicator_rows
            logger.info(f"  Total rows for {name}: {indicator_rows}")

        logger.info("=" * 60)
        logger.info(f"EXTRACTION COMPLETE: {total_rows_inserted} total rows inserted")
        logger.info("=" * 60)

        return total_rows_inserted

    def _calculate_batches(
        self,
        start_date: datetime,
        end_date: datetime,
        batch_size_years: int,
    ) -> List[tuple]:
        """
        Calculate batch date ranges for extraction.

        Args:
            start_date: Start of the full historical range
            end_date: End of the full historical range
            batch_size_years: Size of each batch in years

        Returns:
            List of (batch_start, batch_end) tuples
        """
        batches = []
        current_start = start_date

        while current_start < end_date:
            # Calculate batch end (N years from start, or end_date if earlier)
            batch_end = current_start + timedelta(days=batch_size_years * 365)

            if batch_end > end_date:
                batch_end = end_date

            batches.append((current_start, batch_end))

            # Move to next batch (add 1 day to avoid overlap)
            current_start = batch_end + timedelta(days=1)

        return batches

    def extract_single_indicator_full_history(
        self,
        code: str,
        batch_size_years: int = 5,
    ) -> int:
        """
        Extract full historical data for a single indicator in batches.

        Args:
            code: BCB indicator code (e.g., '432' for SELIC)
            batch_size_years: Number of years per batch

        Returns:
            Number of rows inserted
        """
        if code not in self.indicators:
            raise ValueError(f"Unknown indicator code: {code}")

        name = self.indicators[code]
        indicator_start_dates = self.config.bcb_indicator_start_dates

        start_date_str = indicator_start_dates.get(code)
        if not start_date_str:
            raise ValueError(f"No start date configured for indicator {code}")

        logger.info(f"Extracting full history for {name} ({code})")
        logger.info(f"  Start date: {start_date_str}")

        indicator_start = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.now()

        batches = self._calculate_batches(indicator_start, end_date, batch_size_years)
        logger.info(f"  Processing {len(batches)} batches")

        total_rows = 0

        for batch_num, (batch_start, batch_end) in enumerate(batches, 1):
            batch_start_str = batch_start.strftime("%Y-%m-%d")
            batch_end_str = batch_end.strftime("%Y-%m-%d")

            logger.info(f"  Batch {batch_num}/{len(batches)}: {batch_start_str} to {batch_end_str}")

            try:
                df = self._extract_single_indicator(
                    code, name, batch_start_str, batch_end_str
                )

                if not df.empty:
                    rows = self.load_to_database(df)
                    total_rows += rows
                    logger.info(f"    Saved {rows} new rows")

                time.sleep(self.config.rate_limit_delay * 2)

            except Exception as e:
                logger.error(f"    Error: {e}")
                continue

        logger.info(f"Complete: {total_rows} total rows for {name}")
        return total_rows

    @rate_limit(delay_seconds=0.5)
    @with_retry(max_attempts=3)
    def _extract_single_indicator(
        self,
        code: str,
        name: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Extract data for a single indicator from BCB API.

        Args:
            code: BCB series code (e.g., '432')
            name: Indicator name (e.g., 'SELIC')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format

        Returns:
            DataFrame with indicator data

        Note:
            For full historical extraction, use extract_full_historical_batched()
            which handles the 5-year batching automatically.
        """
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        bcb_start = start_dt.strftime("%d/%m/%Y")
        bcb_end = end_dt.strftime("%d/%m/%Y")

        url = self.config.get_bcb_url(code)
        params = {
            "formato": "json",
            "dataInicial": bcb_start,
            "dataFinal": bcb_end,
        }

        # BCB API requires proper headers to avoid 406 errors
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        }

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=self.config.request_timeout,
        )
        response.raise_for_status()

        data = response.json()

        if not data:
            return pd.DataFrame()

        records = []
        for item in data:
            try:
                date = parse_bcb_date(item["data"])
                value = float(item["valor"])
                records.append(
                    {
                        "indicator_code": code,
                        "indicator_name": name,
                        "date": date.date(),
                        "value": value,
                        "unit": self.INDICATOR_UNITS.get(code, ""),
                        "frequency": self.INDICATOR_FREQUENCIES.get(code, "unknown"),
                    }
                )
            except (KeyError, ValueError) as e:
                logger.warning(f"Skipping invalid record: {item}, error: {e}")
                continue

        return pd.DataFrame(records)

    def load_to_database(
        self,
        df: pd.DataFrame,
        table_name: str = "indicators",
        schema: str = "raw",
    ) -> int:
        """
        Load extracted data to PostgreSQL database.

        Uses upsert logic to handle duplicates.

        Args:
            df: DataFrame with indicator data to load
            table_name: Target table name
            schema: Target schema name

        Returns:
            Number of rows inserted
        """
        required_columns = [
            "indicator_code",
            "indicator_name",
            "date",
            "value",
            "unit",
            "frequency",
        ]
        validate_dataframe(df, required_columns)

        logger.info(f"Loading {len(df)} rows to {schema}.{table_name}")
        start_time = time.time()

        initial_count = self._get_row_count(schema, table_name)

        df["loaded_at"] = datetime.now()
        df["source"] = "bcb_api"

        temp_table = f"temp_indicators_{int(time.time())}"

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
                    (indicator_code, indicator_name, date, value,
                     unit, frequency, loaded_at, source)
                SELECT indicator_code, indicator_name, date, value,
                       unit, frequency, loaded_at, source
                FROM {schema}.{temp_table}
                ON CONFLICT (indicator_code, date) DO NOTHING
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
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                SELECT
                    COUNT(*) as total_rows,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    COUNT(DISTINCT indicator_code) as unique_indicators
                FROM raw.indicators
            """
                )
            )
            row = result.fetchone()

            indicator_counts = conn.execute(
                text(
                    """
                SELECT indicator_code, COUNT(*) as count
                FROM raw.indicators
                GROUP BY indicator_code
                ORDER BY indicator_code
            """
                )
            )

            counts = {row[0]: row[1] for row in indicator_counts}

            return {
                "total_rows": row[0],
                "earliest_date": str(row[1]),
                "latest_date": str(row[2]),
                "unique_indicators": row[3],
                "indicator_counts": counts,
            }


if __name__ == "__main__":
    import os
    import argparse
    from dotenv import load_dotenv

    load_dotenv()

    parser = argparse.ArgumentParser(description="BCB Indicator Extractor")
    parser.add_argument(
        "--mode",
        choices=["incremental", "historical", "full-historical"],
        default="incremental",
        help="Extraction mode: incremental (last 5 days), historical (default range), full-historical (batched from start)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Batch size in years for full-historical mode (default: 5)",
    )
    parser.add_argument(
        "--indicator",
        type=str,
        help="Specific indicator code to extract (e.g., 432)",
    )

    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = "postgresql://dataeng:dataeng123@localhost:5432/brazilian_market"

    logger.add(
        "logs/bcb_extraction_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
    )

    extractor = BCBExtractor(db_url)

    if args.mode == "full-historical":
        logger.info("Running full historical extraction in batches")
        if args.indicator:
            rows = extractor.extract_single_indicator_full_history(
                args.indicator,
                batch_size_years=args.batch_size,
            )
        else:
            rows = extractor.extract_full_historical_batched(
                batch_size_years=args.batch_size,
                save_after_each_batch=True,
            )
        logger.info(f"Total rows inserted: {rows}")
    elif args.mode == "historical":
        logger.info("Running standard historical extraction")
        df = extractor.extract_historical()
        rows = extractor.load_to_database(df)
        logger.info(f"Rows inserted: {rows}")
    else:
        logger.info("Running incremental extraction")
        df = extractor.extract_incremental()
        rows = extractor.load_to_database(df)
        logger.info(f"Rows inserted: {rows}")

    validation = extractor.validate_extraction()
    logger.info(f"Validation results: {validation}")
