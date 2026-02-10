"""
utils.py
========

Utility functions for data extraction modules.

This module provides common functionality used across extraction classes,
including rate limiting, retry logic, and data validation utilities.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import time
from functools import wraps
from typing import Callable, Any, TypeVar, Optional
from datetime import datetime

from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import requests
import pandas as pd


T = TypeVar("T")


def rate_limit(delay_seconds: float = 0.5) -> Callable:
    """
    Decorator to add rate limiting between function calls.

    This decorator ensures a minimum delay between consecutive calls
    to respect API rate limits and avoid being blocked.

    Args:
        delay_seconds: Minimum seconds to wait between calls

    Returns:
        Decorated function with rate limiting

    Example:
        >>> @rate_limit(1.0)
        ... def fetch_data(url):
        ...     return requests.get(url)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        last_call_time: Optional[float] = None

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            nonlocal last_call_time

            if last_call_time is not None:
                elapsed = time.time() - last_call_time
                if elapsed < delay_seconds:
                    sleep_time = delay_seconds - elapsed
                    logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)

            last_call_time = time.time()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def with_retry(
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 10.0,
) -> Callable:
    """
    Decorator to add retry logic with exponential backoff.

    This decorator automatically retries failed operations with
    increasing delays between attempts.

    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time between retries (seconds)
        max_wait: Maximum wait time between retries (seconds)

    Returns:
        Decorated function with retry logic

    Example:
        >>> @with_retry(max_attempts=3)
        ... def unreliable_api_call():
        ...     return requests.get("https://api.example.com/data")
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(
                (requests.exceptions.RequestException, ConnectionError)
            ),
            before_sleep=lambda retry_state: logger.warning(
                f"Retry attempt {retry_state.attempt_number} after error: "
                f"{retry_state.outcome.exception()}"
            ),
        )
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return func(*args, **kwargs)

        return wrapper

    return decorator


def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Validate that date range is valid and reasonable.

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        True if date range is valid

    Raises:
        ValueError: If dates are invalid or range is unreasonable
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")

    if start < datetime(2000, 1, 1):
        raise ValueError(f"Start date {start_date} is too far in the past")

    if end > datetime.now():
        logger.warning(f"End date {end_date} is in the future, using today instead")

    return True


def validate_dataframe(df: pd.DataFrame, required_columns: list) -> bool:
    """
    Validate that DataFrame has required columns and is not empty.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must exist

    Returns:
        True if DataFrame is valid

    Raises:
        ValueError: If DataFrame is invalid
    """
    if df.empty:
        raise ValueError("DataFrame is empty")

    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    return True


def clean_ticker(ticker: str) -> str:
    """
    Clean and standardize ticker symbol.

    Ensures ticker has .SA suffix for Brazilian stocks.

    Args:
        ticker: Raw ticker symbol

    Returns:
        Cleaned ticker with .SA suffix

    Example:
        >>> clean_ticker("PETR4")
        'PETR4.SA'
        >>> clean_ticker("PETR4.SA")
        'PETR4.SA'
    """
    ticker = ticker.upper().strip()
    if not ticker.endswith(".SA"):
        ticker = f"{ticker}.SA"
    return ticker


def parse_bcb_date(date_str: str) -> datetime:
    """
    Parse date string from BCB API response.

    BCB API returns dates in DD/MM/YYYY format.

    Args:
        date_str: Date string in DD/MM/YYYY format

    Returns:
        Parsed datetime object

    Example:
        >>> parse_bcb_date("15/01/2024")
        datetime.datetime(2024, 1, 15, 0, 0)
    """
    return datetime.strptime(date_str, "%d/%m/%Y")


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string

    Example:
        >>> format_duration(125.5)
        '2m 5.5s'
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    return f"{minutes}m {remaining_seconds:.1f}s"
