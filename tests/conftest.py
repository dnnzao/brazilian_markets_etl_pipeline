"""
conftest.py
===========

Pytest configuration and shared fixtures.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch


@pytest.fixture
def sample_stock_data():
    """Provide sample stock data for testing."""
    return pd.DataFrame(
        {
            "ticker": ["PETR4.SA", "PETR4.SA", "VALE3.SA", "VALE3.SA"],
            "date": pd.to_datetime(
                ["2024-01-02", "2024-01-03", "2024-01-02", "2024-01-03"]
            ),
            "open_price": [35.0, 35.5, 70.0, 71.0],
            "high_price": [36.0, 36.5, 72.0, 73.0],
            "low_price": [34.5, 35.0, 69.0, 70.0],
            "close_price": [35.5, 36.0, 71.0, 72.5],
            "volume": [10000000, 12000000, 8000000, 9000000],
            "adj_close": [35.5, 36.0, 71.0, 72.5],
        }
    )


@pytest.fixture
def sample_indicator_data():
    """Provide sample indicator data for testing."""
    return pd.DataFrame(
        {
            "indicator_code": ["432", "432", "1", "1"],
            "indicator_name": ["SELIC", "SELIC", "USD_BRL", "USD_BRL"],
            "date": pd.to_datetime(
                ["2024-01-02", "2024-01-03", "2024-01-02", "2024-01-03"]
            ),
            "value": [11.75, 11.75, 4.95, 4.97],
            "unit": ["% per year", "% per year", "BRL/USD", "BRL/USD"],
            "frequency": ["daily", "daily", "daily", "daily"],
        }
    )


@pytest.fixture
def mock_db_engine():
    """Provide a mock database engine."""
    engine = MagicMock()
    conn = MagicMock()

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    conn.execute.return_value.scalar.return_value = 100
    conn.execute.return_value.fetchone.return_value = (100, "2024-01-01", "2024-12-31", 20)

    return engine


@pytest.fixture(autouse=True)
def reset_rate_limiter():
    """Reset rate limiter state between tests."""
    yield
