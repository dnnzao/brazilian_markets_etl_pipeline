"""
db_loader.py
============

Database connection and loading utilities for the ETL pipeline.

This module provides a centralized database connection manager and
utility functions for bulk data loading operations.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import os
from typing import Optional, Dict, Any
from contextlib import contextmanager

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool


class DatabaseLoader:
    """
    Database connection manager and data loading utilities.

    This class provides a centralized interface for database operations,
    including connection management, bulk loading, and validation.

    Attributes:
        engine: SQLAlchemy database engine with connection pooling

    Example:
        >>> loader = DatabaseLoader()
        >>> with loader.get_connection() as conn:
        ...     result = conn.execute(text("SELECT COUNT(*) FROM raw.stocks"))
        ...     print(result.scalar())
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
    ):
        """
        Initialize database connection.

        Args:
            connection_string: PostgreSQL connection URL. If None, reads from
                             DATABASE_URL environment variable.
            pool_size: Number of connections to keep in the pool
            max_overflow: Maximum overflow connections allowed

        Raises:
            ValueError: If no connection string provided and DATABASE_URL not set
            ConnectionError: If database connection fails
        """
        conn_str = connection_string or os.getenv("DATABASE_URL")

        if not conn_str:
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            user = os.getenv("POSTGRES_USER", "dataeng")
            password = os.getenv("POSTGRES_PASSWORD", "dataeng123")
            database = os.getenv("POSTGRES_DB", "brazilian_market")
            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        try:
            self.engine: Engine = create_engine(
                conn_str,
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
            )
            self._test_connection()
            logger.info("Database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise ConnectionError(f"Database connection failed: {e}")

    def _test_connection(self) -> None:
        """Test database connection is working."""
        with self.engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    @contextmanager
    def get_connection(self):
        """
        Get a database connection from the pool.

        Yields:
            SQLAlchemy connection object

        Example:
            >>> with loader.get_connection() as conn:
            ...     conn.execute(text("SELECT * FROM raw.stocks LIMIT 10"))
        """
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def get_transaction(self):
        """
        Get a database connection with transaction context.

        Commits on success, rolls back on exception.

        Yields:
            SQLAlchemy connection object with active transaction
        """
        with self.engine.begin() as connection:
            yield connection

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        """
        Get information about a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Dictionary with table information
        """
        inspector = inspect(self.engine)

        columns = inspector.get_columns(table, schema=schema)
        pk = inspector.get_pk_constraint(table, schema=schema)
        indexes = inspector.get_indexes(table, schema=schema)

        with self.get_connection() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {schema}.{table}")
            )
            row_count = result.scalar()

        return {
            "schema": schema,
            "table": table,
            "columns": [col["name"] for col in columns],
            "primary_key": pk.get("constrained_columns", []),
            "indexes": [idx["name"] for idx in indexes],
            "row_count": row_count,
        }

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            DataFrame with query results
        """
        with self.get_connection() as conn:
            return pd.read_sql(text(query), conn, params=params)

    def bulk_insert(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "raw",
        if_exists: str = "append",
        chunksize: int = 1000,
    ) -> int:
        """
        Bulk insert DataFrame to database table.

        Args:
            df: DataFrame to insert
            table: Target table name
            schema: Target schema name
            if_exists: What to do if table exists ('append', 'replace', 'fail')
            chunksize: Number of rows per insert batch

        Returns:
            Number of rows inserted
        """
        logger.info(f"Bulk inserting {len(df)} rows to {schema}.{table}")

        df.to_sql(
            table,
            self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=chunksize,
        )

        logger.info(f"Bulk insert complete: {len(df)} rows")
        return len(df)

    def truncate_table(self, schema: str, table: str) -> None:
        """
        Truncate a table (delete all rows).

        Args:
            schema: Schema name
            table: Table name
        """
        logger.warning(f"Truncating table {schema}.{table}")
        with self.get_transaction() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table}"))

    def get_row_count(self, schema: str, table: str) -> int:
        """
        Get row count for a table.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            Number of rows in table
        """
        with self.get_connection() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
            return result.scalar() or 0

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Check if a table exists.

        Args:
            schema: Schema name
            table: Table name

        Returns:
            True if table exists
        """
        inspector = inspect(self.engine)
        return table in inspector.get_table_names(schema=schema)

    def get_schemas(self) -> list:
        """Get list of all schemas in database."""
        inspector = inspect(self.engine)
        return inspector.get_schema_names()

    def close(self) -> None:
        """Close database connection pool."""
        self.engine.dispose()
        logger.info("Database connection pool closed")


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    loader = DatabaseLoader()

    print("Schemas:", loader.get_schemas())

    if loader.table_exists("raw", "stocks"):
        info = loader.get_table_info("raw", "stocks")
        print(f"Table info: {info}")
    else:
        print("Table raw.stocks does not exist yet")

    loader.close()
