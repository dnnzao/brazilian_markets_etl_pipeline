"""
test_db_loader.py
=================

Unit tests for DatabaseLoader class.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from load.db_loader import DatabaseLoader


class TestDatabaseLoader:
    """Test suite for DatabaseLoader class."""

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
    def loader(self, mock_engine):
        """Create DatabaseLoader instance with mocked engine."""
        with patch("load.db_loader.create_engine", return_value=mock_engine):
            return DatabaseLoader("postgresql://test:test@localhost/test")

    def test_initialization_success(self, loader):
        """Test successful initialization."""
        assert loader.engine is not None

    def test_initialization_from_env_vars(self, mock_engine):
        """Test initialization from environment variables."""
        env_vars = {
            "POSTGRES_HOST": "testhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_DB": "testdb",
        }

        with patch.dict("os.environ", env_vars):
            with patch("load.db_loader.create_engine", return_value=mock_engine):
                loader = DatabaseLoader()

        assert loader.engine is not None

    def test_get_row_count(self, loader):
        """Test row count retrieval."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = 100
        mock_conn.execute.return_value = mock_result

        with patch.object(loader, 'get_connection') as mock_get_conn:
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            count = loader.get_row_count("raw", "stocks")

        assert count == 100

    def test_table_exists_true(self, loader):
        """Test table existence check when table exists."""
        with patch("load.db_loader.inspect") as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["stocks", "indicators"]
            mock_inspect.return_value = mock_inspector

            result = loader.table_exists("raw", "stocks")

        assert result is True

    def test_table_exists_false(self, loader):
        """Test table existence check when table doesn't exist."""
        with patch("load.db_loader.inspect") as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["other_table"]
            mock_inspect.return_value = mock_inspector

            result = loader.table_exists("raw", "stocks")

        assert result is False

    def test_execute_query_returns_dataframe(self, loader):
        """Test that execute_query returns a DataFrame."""
        with patch("pandas.read_sql") as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame({"col1": [1, 2, 3]})

            result = loader.execute_query("SELECT * FROM raw.stocks")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3


class TestDatabaseLoaderContextManagers:
    """Test context managers for DatabaseLoader."""

    @pytest.fixture
    def mock_engine(self):
        """Create mock SQLAlchemy engine."""
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value = conn
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.scalar.return_value = 1
        return engine

    @pytest.fixture
    def loader(self, mock_engine):
        """Create DatabaseLoader with mocked engine."""
        with patch("load.db_loader.create_engine", return_value=mock_engine):
            return DatabaseLoader("postgresql://test:test@localhost/test")

    def test_get_connection_context(self, loader):
        """Test get_connection context manager."""
        with loader.get_connection() as conn:
            assert conn is not None

    def test_close_disposes_engine(self, loader):
        """Test that close() disposes the engine."""
        loader.close()
        loader.engine.dispose.assert_called_once()
