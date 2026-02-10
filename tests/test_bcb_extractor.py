"""
test_bcb_extractor.py
=====================

Unit tests for BCBExtractor class.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import json

from extract.bcb_extractor import BCBExtractor
from extract.config import ExtractionConfig


class TestBCBExtractor:
    """Test suite for BCBExtractor class."""

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
        """Create BCBExtractor instance with mocked database."""
        with patch("extract.bcb_extractor.create_engine", return_value=mock_engine):
            return BCBExtractor(mock_db_connection)

    def test_initialization_with_default_indicators(self, extractor):
        """Test that extractor initializes with default indicators."""
        assert len(extractor.indicators) > 0
        assert "432" in extractor.indicators
        assert extractor.indicators["432"] == "SELIC"

    def test_initialization_with_custom_indicators(self, mock_db_connection, mock_engine):
        """Test initialization with custom indicator list."""
        custom_indicators = {"432": "SELIC", "1": "USD_BRL"}

        with patch("extract.bcb_extractor.create_engine", return_value=mock_engine):
            extractor = BCBExtractor(mock_db_connection, indicators=custom_indicators)

        assert extractor.indicators == custom_indicators

    @patch("requests.get")
    def test_extract_single_indicator_returns_dataframe(self, mock_get, extractor):
        """Test that _extract_single_indicator returns valid DataFrame."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"data": "02/01/2024", "valor": "11.75"},
            {"data": "03/01/2024", "valor": "11.75"},
        ]
        mock_get.return_value = mock_response

        df = extractor._extract_single_indicator(
            "432", "SELIC", "2024-01-01", "2024-01-05"
        )

        assert not df.empty
        assert "indicator_code" in df.columns
        assert "indicator_name" in df.columns
        assert "date" in df.columns
        assert "value" in df.columns
        assert len(df) == 2

    @patch("requests.get")
    def test_extract_handles_empty_response(self, mock_get, extractor):
        """Test that extractor handles empty API responses."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        df = extractor._extract_single_indicator(
            "999", "Unknown", "2024-01-01", "2024-01-05"
        )

        assert df.empty

    @patch("requests.get")
    def test_extract_assigns_correct_frequency(self, mock_get, extractor):
        """Test that indicators get correct frequency assignment."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"data": "02/01/2024", "valor": "11.75"},
        ]
        mock_get.return_value = mock_response

        df = extractor._extract_single_indicator(
            "432", "SELIC", "2024-01-01", "2024-01-05"
        )

        assert df.iloc[0]["frequency"] == "daily"

        df = extractor._extract_single_indicator(
            "433", "IPCA", "2024-01-01", "2024-01-05"
        )

        assert df.iloc[0]["frequency"] == "monthly"

    @patch("requests.get")
    def test_extract_assigns_correct_unit(self, mock_get, extractor):
        """Test that indicators get correct unit assignment."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"data": "02/01/2024", "valor": "4.95"},
        ]
        mock_get.return_value = mock_response

        df = extractor._extract_single_indicator(
            "1", "USD_BRL", "2024-01-01", "2024-01-05"
        )

        assert df.iloc[0]["unit"] == "BRL/USD"


class TestBCBDataParsing:
    """Test suite for BCB data parsing utilities."""

    def test_parse_bcb_date(self):
        """Test BCB date format parsing."""
        from extract.utils import parse_bcb_date

        result = parse_bcb_date("15/01/2024")

        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_bcb_date_invalid(self):
        """Test that invalid dates raise error."""
        from extract.utils import parse_bcb_date

        with pytest.raises(ValueError):
            parse_bcb_date("invalid-date")
