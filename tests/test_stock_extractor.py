"""
test_stock_extractor.py
=======================

Unit tests for StockExtractor class.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

from extract.stock_extractor import StockExtractor
from extract.config import ExtractionConfig


class TestStockExtractor:
    """Test suite for StockExtractor class."""

    @pytest.fixture
    def mock_db_connection(self):
        """Provide mock database connection string."""
        return "postgresql://test:test@localhost:5432/test_db"

    @pytest.fixture
    def mock_engine(self):
        """Create mock SQLAlchemy engine."""
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.scalar.return_value = 1
        return engine

    @pytest.fixture
    def extractor(self, mock_db_connection, mock_engine):
        """Create StockExtractor instance with mocked database."""
        with patch("extract.stock_extractor.create_engine", return_value=mock_engine):
            return StockExtractor(mock_db_connection)

    def test_initialization_with_default_tickers(self, extractor):
        """Test that extractor initializes with default ticker list."""
        assert len(extractor.tickers) > 0
        assert "PETR4.SA" in extractor.tickers
        assert "VALE3.SA" in extractor.tickers

    def test_initialization_with_custom_tickers(self, mock_db_connection, mock_engine):
        """Test initialization with custom ticker list."""
        custom_tickers = ["VALE3.SA", "ITUB4.SA"]

        with patch("extract.stock_extractor.create_engine", return_value=mock_engine):
            extractor = StockExtractor(mock_db_connection, tickers=custom_tickers)

        assert extractor.tickers == custom_tickers

    def test_ticker_normalization(self, mock_db_connection, mock_engine):
        """Test that tickers without .SA suffix are normalized."""
        tickers = ["PETR4", "vale3", "  ITUB4.sa  "]

        with patch("extract.stock_extractor.create_engine", return_value=mock_engine):
            extractor = StockExtractor(mock_db_connection, tickers=tickers)

        assert "PETR4.SA" in extractor.tickers
        assert "VALE3.SA" in extractor.tickers
        assert "ITUB4.SA" in extractor.tickers

    @patch("yfinance.Ticker")
    def test_extract_single_ticker_returns_dataframe(self, mock_yf_ticker, extractor):
        """Test that _extract_single_ticker returns a valid DataFrame."""
        mock_hist = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
                "Open": [10.0, 10.5],
                "High": [11.0, 11.5],
                "Low": [9.5, 10.0],
                "Close": [10.5, 11.0],
                "Volume": [1000000, 1200000],
                "Adj Close": [10.5, 11.0],
            }
        )

        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = mock_hist
        mock_yf_ticker.return_value = mock_ticker_instance

        df = extractor._extract_single_ticker("PETR4.SA", "2024-01-01", "2024-01-05")

        assert not df.empty
        assert "ticker" in df.columns
        assert "date" in df.columns
        assert "close_price" in df.columns
        assert len(df) == 2

    @patch("yfinance.Ticker")
    def test_extract_handles_empty_response(self, mock_yf_ticker, extractor):
        """Test that extractor handles empty API responses gracefully."""
        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = pd.DataFrame()
        mock_yf_ticker.return_value = mock_ticker_instance

        df = extractor._extract_single_ticker("INVALID.SA", "2024-01-01", "2024-01-05")

        assert df.empty

    @patch("yfinance.Ticker")
    def test_extract_filters_invalid_prices(self, mock_yf_ticker, extractor):
        """Test that invalid (negative/zero) prices are filtered out."""
        mock_hist = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                "Open": [10.0, -5.0, 10.0],
                "High": [11.0, 11.0, 11.0],
                "Low": [9.5, 9.5, 9.5],
                "Close": [10.5, 0.0, 11.0],
                "Volume": [1000000, 1000000, 1000000],
                "Adj Close": [10.5, 0.0, 11.0],
            }
        )

        mock_ticker_instance = MagicMock()
        mock_ticker_instance.history.return_value = mock_hist
        mock_yf_ticker.return_value = mock_ticker_instance

        df = extractor._extract_single_ticker("PETR4.SA", "2024-01-01", "2024-01-05")

        assert len(df) == 2
        assert (df["close_price"] > 0).all()


class TestExtractionConfig:
    """Test suite for ExtractionConfig class."""

    def test_default_config_values(self):
        """Test that default config values are sensible."""
        config = ExtractionConfig()

        assert config.start_date == "2016-03-01"  # BCB limits daily series to 10-year window
        assert len(config.stock_tickers) == 20
        assert len(config.bcb_indicators) == 7
        assert config.retry_attempts == 3

    def test_bcb_url_generation(self):
        """Test BCB API URL generation."""
        config = ExtractionConfig()
        url = config.get_bcb_url("432")

        assert "432" in url
        assert "bcb.gov.br" in url

    def test_incremental_start_date(self):
        """Test incremental start date calculation."""
        config = ExtractionConfig()
        config.lookback_days = 5

        start_date = config.get_incremental_start_date()

        assert start_date is not None
        parsed = datetime.strptime(start_date, "%Y-%m-%d")
        assert parsed < datetime.now()
