"""
extract
=======

This module handles extraction of financial data from various sources.

The extract package provides classes for extracting:
- Stock price data from Yahoo Finance API
- Economic indicators from Brazilian Central Bank (BCB) API

Classes:
    StockExtractor: Extract stock prices from Yahoo Finance
    BCBExtractor: Extract economic indicators from BCB API

Example:
    >>> from extract import StockExtractor, BCBExtractor
    >>> stock_extractor = StockExtractor(db_connection_string)
    >>> bcb_extractor = BCBExtractor(db_connection_string)
"""

from extract.stock_extractor import StockExtractor
from extract.bcb_extractor import BCBExtractor
from extract.config import ExtractionConfig

__all__ = ["StockExtractor", "BCBExtractor", "ExtractionConfig"]
__version__ = "1.0.0"
