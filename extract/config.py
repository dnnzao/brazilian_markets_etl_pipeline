"""
config.py
=========

Configuration settings for data extraction modules.

This module contains all configurable parameters for the extraction
process, including ticker lists, API endpoints, date ranges, and
retry settings.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

from dataclasses import dataclass, field
from typing import List, Dict
from datetime import datetime, timedelta


@dataclass
class ExtractionConfig:
    """
    Configuration class for data extraction settings.

    This class centralizes all configuration parameters used by the
    extraction modules. It provides sensible defaults while allowing
    customization for different environments.

    Attributes:
        start_date: Beginning of historical data extraction period
        end_date: End of extraction period (defaults to today)
        lookback_days: Days to look back for incremental loads
        stock_tickers: List of Brazilian stock tickers to extract
        bcb_indicators: Dictionary mapping indicator codes to names
        retry_attempts: Number of retry attempts for failed requests
        retry_delay: Seconds to wait between retries
        request_timeout: Timeout in seconds for API requests
        rate_limit_delay: Seconds to wait between API requests

    Example:
        >>> config = ExtractionConfig()
        >>> print(config.stock_tickers[:5])
        ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'ABEV3.SA']
    """

    start_date: str = "2016-03-01"  # BCB limits daily series to 10-year window
    end_date: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))
    lookback_days: int = 5

    stock_tickers: List[str] = field(
        default_factory=lambda: [
            "PETR4.SA",   # Petrobras (oil & gas)
            "VALE3.SA",   # Vale (mining)
            "ITUB4.SA",   # Itaú Unibanco (banking)
            "BBDC4.SA",   # Bradesco (banking)
            "ABEV3.SA",   # Ambev (beverages)
            "B3SA3.SA",   # B3 (stock exchange)
            "RENT3.SA",   # Localiza (car rental)
            "WEGE3.SA",   # WEG (motors/equipment)
            "SUZB3.SA",   # Suzano (pulp & paper)
            "RAIL3.SA",   # Rumo (logistics)
            "BBAS3.SA",   # Banco do Brasil (banking)
            "GGBR4.SA",   # Gerdau (steel)
            "VIVT3.SA",   # Vivo (telecom) - replaces JBSS3.SA
            "MGLU3.SA",   # Magazine Luiza (retail)
            "LREN3.SA",   # Lojas Renner (retail)
            "CSAN3.SA",   # Cosan (energy)
            "RADL3.SA",   # Raia Drogasil (pharmacy)
            "PRIO3.SA",   # PetroRio (oil) - replaces EMBR3.SA
            "HAPV3.SA",   # Hapvida (healthcare)
            "TOTS3.SA",   # Totvs (software)
        ]
    )

    bcb_indicators: Dict[str, str] = field(
        default_factory=lambda: {
            "432": "SELIC",        # SELIC daily rate
            "433": "IPCA",         # Monthly inflation
            "1": "USD_BRL",        # USD/BRL daily exchange rate
            "12": "IPCA_12M",      # IPCA accumulated 12 months
            "24369": "CDI_Daily",  # CDI daily rate
            "189": "IGP_M",        # IGP-M monthly
            "7832": "USD_BRL_PTAX",# USD/BRL PTAX rate
        }
    )

    # Initial dates for each BCB indicator series (DD/MM/YYYY format from BCB)
    # These are the earliest dates when each series started being recorded
    bcb_indicator_start_dates: Dict[str, str] = field(
        default_factory=lambda: {
            "432": "1999-03-05",   # SELIC started 05/03/1999
            "1": "1984-11-28",     # USD/BRL started 28/11/1984
            "433": "1980-01-01",   # IPCA started 01/01/1980
            "12": "1986-03-06",    # IPCA 12m started 06/03/1986
            "24369": "2012-03-01", # CDI daily started 01/03/2012
            "189": "1989-06-01",   # IGP-M started 01/06/1989
            "7832": "1987-02-01",  # USD/BRL PTAX started 01/02/1987
        }
    )

    retry_attempts: int = 3
    retry_delay: float = 2.0
    request_timeout: int = 180  # Increased for large BCB daily series
    rate_limit_delay: float = 0.5

    bcb_api_base_url: str = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"

    def get_bcb_url(self, indicator_code: str) -> str:
        """
        Generate BCB API URL for a specific indicator.

        Args:
            indicator_code: BCB series code (e.g., '432' for SELIC)

        Returns:
            Complete URL for the BCB API endpoint
        """
        return self.bcb_api_base_url.format(code=indicator_code)

    def get_incremental_start_date(self) -> str:
        """
        Calculate start date for incremental extraction.

        Returns:
            Date string (YYYY-MM-DD) for lookback_days ago
        """
        start = datetime.now() - timedelta(days=self.lookback_days)
        return start.strftime("%Y-%m-%d")
