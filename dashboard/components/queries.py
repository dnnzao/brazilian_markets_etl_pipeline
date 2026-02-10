"""
queries.py
==========

SQL queries for dashboard data retrieval.

This module provides comprehensive data access functions for the Brazilian
Financial Markets Dashboard, including real-time metrics and historical
analysis spanning up to 10 years of data.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
Updated: 2025-02-09 - Added historical analysis queries
"""

from typing import Optional, List, Tuple
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Engine


# =============================================================================
# Date Range Utilities
# =============================================================================

def get_date_range_info(engine: Engine) -> dict:
    """
    Get the available date range in the database.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        Dictionary with min_date, max_date, and total_days
    """
    query = """
        SELECT
            MIN(d.date) as min_date,
            MAX(d.date) as max_date,
            COUNT(DISTINCT d.date) as total_days,
            COUNT(DISTINCT f.stock_id) as total_stocks
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_date d ON f.date_id = d.date_id
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()

        if row:
            return {
                "min_date": row[0],
                "max_date": row[1],
                "total_days": row[2],
                "total_stocks": row[3],
                "years_of_data": (row[1] - row[0]).days / 365.25 if row[0] and row[1] else 0,
            }

    return {}


def calculate_period_dates(period: str) -> Tuple[str, str]:
    """
    Calculate start and end dates based on period string.

    Args:
        period: One of '1M', '3M', '6M', '1Y', '3Y', '5Y', '10Y', 'MAX'

    Returns:
        Tuple of (start_date, end_date) as strings
    """
    end_date = datetime.now()

    period_map = {
        "1M": relativedelta(months=1),
        "3M": relativedelta(months=3),
        "6M": relativedelta(months=6),
        "1Y": relativedelta(years=1),
        "2Y": relativedelta(years=2),
        "3Y": relativedelta(years=3),
        "5Y": relativedelta(years=5),
        "10Y": relativedelta(years=10),
        "MAX": relativedelta(years=50),
    }

    delta = period_map.get(period, relativedelta(years=1))
    start_date = end_date - delta

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_market_summary(engine: Engine) -> dict:
    """
    Get overall market summary statistics.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        Dictionary with market summary metrics
    """
    query = """
        WITH latest_date AS (
            SELECT MAX(d.date) as max_date
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_date d ON f.date_id = d.date_id
        ),
        market_stats AS (
            SELECT
                COUNT(DISTINCT f.stock_id) as total_stocks,
                AVG(f.daily_return) as avg_daily_return,
                AVG(f.volatility_30d) as avg_volatility,
                SUM(f.volume) as total_volume,
                AVG(f.selic_rate) as current_selic,
                AVG(f.usd_brl) as current_usd_brl
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            CROSS JOIN latest_date ld
            WHERE d.date = ld.max_date
        )
        SELECT * FROM market_stats
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        row = result.fetchone()

        if row:
            return {
                "total_stocks": row[0],
                "avg_daily_return": float(row[1] or 0) * 100,
                "avg_volatility": float(row[2] or 0) * 100,
                "total_volume": int(row[3] or 0),
                "selic_rate": float(row[4] or 0),
                "usd_brl": float(row[5] or 0),
            }

    return {}


def get_top_movers(
    engine: Engine, n: int = 10, direction: str = "gainers"
) -> pd.DataFrame:
    """
    Get top gaining or losing stocks.

    Args:
        engine: SQLAlchemy database engine
        n: Number of stocks to return
        direction: 'gainers' or 'losers'

    Returns:
        DataFrame with top movers
    """
    order = "DESC" if direction == "gainers" else "ASC"

    query = f"""
        WITH latest_date AS (
            SELECT MAX(d.date) as max_date
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_date d ON f.date_id = d.date_id
        )
        SELECT
            s.ticker,
            s.company_name,
            s.sector,
            f.close_price,
            f.daily_return * 100 as daily_return_pct,
            f.volume
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        CROSS JOIN latest_date ld
        WHERE d.date = ld.max_date
          AND f.daily_return IS NOT NULL
        ORDER BY f.daily_return {order}
        LIMIT {n}
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def get_sector_performance(
    engine: Engine, days: int = 30
) -> pd.DataFrame:
    """
    Get sector performance over period.

    Args:
        engine: SQLAlchemy database engine
        days: Number of days to analyze

    Returns:
        DataFrame with sector performance
    """
    query = f"""
        WITH period_returns AS (
            SELECT
                s.sector,
                AVG(f.monthly_return) * 100 as avg_return,
                AVG(f.volatility_30d) * 100 as avg_volatility,
                COUNT(DISTINCT s.stock_id) as stock_count
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE d.date >= CURRENT_DATE - INTERVAL '{days} days'
              AND s.sector IS NOT NULL
            GROUP BY s.sector
        )
        SELECT
            sector,
            avg_return,
            avg_volatility,
            stock_count,
            avg_return / NULLIF(avg_volatility, 0) as sharpe_ratio
        FROM period_returns
        ORDER BY avg_return DESC
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def get_stock_history(
    engine: Engine,
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get historical data for a specific stock.

    Args:
        engine: SQLAlchemy database engine
        ticker: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with stock history
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    query = """
        SELECT
            d.date,
            f.open_price,
            f.high_price,
            f.low_price,
            f.close_price,
            f.volume,
            f.daily_return * 100 as daily_return_pct,
            f.volatility_30d * 100 as volatility_pct,
            f.selic_rate,
            f.usd_brl
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        WHERE s.ticker = :ticker
          AND d.date BETWEEN :start_date AND :end_date
        ORDER BY d.date
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(query),
            conn,
            params={"ticker": ticker, "start_date": start_date, "end_date": end_date},
        )


def get_stock_list(engine: Engine) -> pd.DataFrame:
    """
    Get list of all available stocks.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        DataFrame with stock list
    """
    query = """
        SELECT
            ticker,
            company_name,
            sector,
            subsector,
            market_cap_category
        FROM analytics.dim_stock
        WHERE is_active = TRUE
        ORDER BY ticker
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def get_correlation_data(
    engine: Engine, days: int = 252
) -> pd.DataFrame:
    """
    Get data for correlation analysis between stocks and macro indicators.

    Args:
        engine: SQLAlchemy database engine
        days: Number of days to analyze

    Returns:
        DataFrame with correlation data
    """
    query = f"""
        SELECT
            s.sector,
            AVG(f.daily_return) as avg_return,
            AVG(f.selic_rate) as avg_selic,
            AVG(f.usd_brl) as avg_usd_brl,
            AVG(f.inflation_rate) as avg_inflation,
            CORR(f.daily_return, f.selic_rate) as corr_selic,
            CORR(f.daily_return, f.usd_brl) as corr_usd
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        WHERE d.date >= CURRENT_DATE - INTERVAL '{days} days'
          AND s.sector IS NOT NULL
        GROUP BY s.sector
        ORDER BY s.sector
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


# =============================================================================
# Historical Analysis Queries
# =============================================================================

def get_multi_year_returns(engine: Engine) -> pd.DataFrame:
    """
    Calculate 1-year, 3-year, 5-year, and 10-year returns for all stocks.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        DataFrame with multi-period returns for each stock
    """
    query = """
        WITH date_bounds AS (
            SELECT
                MAX(d.date) as max_date,
                MAX(d.date) - INTERVAL '1 year' as date_1y,
                MAX(d.date) - INTERVAL '3 years' as date_3y,
                MAX(d.date) - INTERVAL '5 years' as date_5y,
                MAX(d.date) - INTERVAL '10 years' as date_10y
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_date d ON f.date_id = d.date_id
        ),
        latest_prices AS (
            SELECT
                s.stock_id,
                s.ticker,
                s.company_name,
                s.sector,
                f.close_price as current_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            CROSS JOIN date_bounds db
            WHERE d.date = db.max_date
        ),
        -- Get price closest to 1 year ago
        price_1y AS (
            SELECT DISTINCT ON (s.stock_id)
                s.stock_id,
                f.close_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            CROSS JOIN date_bounds db
            WHERE d.date <= db.date_1y
            ORDER BY s.stock_id, d.date DESC
        ),
        -- Get price closest to 3 years ago
        price_3y AS (
            SELECT DISTINCT ON (s.stock_id)
                s.stock_id,
                f.close_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            CROSS JOIN date_bounds db
            WHERE d.date <= db.date_3y
            ORDER BY s.stock_id, d.date DESC
        ),
        -- Get price closest to 5 years ago
        price_5y AS (
            SELECT DISTINCT ON (s.stock_id)
                s.stock_id,
                f.close_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            CROSS JOIN date_bounds db
            WHERE d.date <= db.date_5y
            ORDER BY s.stock_id, d.date DESC
        ),
        -- Get price closest to 10 years ago
        price_10y AS (
            SELECT DISTINCT ON (s.stock_id)
                s.stock_id,
                f.close_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            CROSS JOIN date_bounds db
            WHERE d.date <= db.date_10y
            ORDER BY s.stock_id, d.date DESC
        )
        SELECT
            lp.ticker,
            lp.company_name,
            lp.sector,
            lp.current_price,
            ROUND(((lp.current_price - p1.close_price) / NULLIF(p1.close_price, 0) * 100)::numeric, 2) as return_1y,
            ROUND(((lp.current_price - p3.close_price) / NULLIF(p3.close_price, 0) * 100)::numeric, 2) as return_3y,
            ROUND(((lp.current_price - p5.close_price) / NULLIF(p5.close_price, 0) * 100)::numeric, 2) as return_5y,
            ROUND(((lp.current_price - p10.close_price) / NULLIF(p10.close_price, 0) * 100)::numeric, 2) as return_10y
        FROM latest_prices lp
        LEFT JOIN price_1y p1 ON lp.stock_id = p1.stock_id
        LEFT JOIN price_3y p3 ON lp.stock_id = p3.stock_id
        LEFT JOIN price_5y p5 ON lp.stock_id = p5.stock_id
        LEFT JOIN price_10y p10 ON lp.stock_id = p10.stock_id
        ORDER BY lp.ticker
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    # Ensure return columns are numeric (convert any None/NaN to proper pandas NA)
    return_cols = ['return_1y', 'return_3y', 'return_5y', 'return_10y']
    for col in return_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def get_cumulative_returns(
    engine: Engine,
    ticker: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calculate cumulative returns over time for stocks.

    Args:
        engine: SQLAlchemy database engine
        ticker: Optional specific ticker (None for all stocks)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with cumulative returns time series
    """
    if not start_date:
        start_date = (datetime.now() - relativedelta(years=10)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    ticker_filter = "AND s.ticker = :ticker" if ticker else ""

    query = f"""
        WITH base_data AS (
            SELECT
                s.ticker,
                d.date,
                f.close_price,
                f.daily_return,
                FIRST_VALUE(f.close_price) OVER (
                    PARTITION BY s.stock_id
                    ORDER BY d.date
                ) as first_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE d.date BETWEEN :start_date AND :end_date
              {ticker_filter}
        )
        SELECT
            ticker,
            date,
            close_price,
            daily_return * 100 as daily_return_pct,
            ((close_price - first_price) / NULLIF(first_price, 0)) * 100 as cumulative_return_pct
        FROM base_data
        ORDER BY ticker, date
    """

    params = {"start_date": start_date, "end_date": end_date}
    if ticker:
        params["ticker"] = ticker

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


def get_drawdown_analysis(
    engine: Engine,
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calculate drawdown analysis for a specific stock.

    Drawdown measures the decline from a historical peak, useful for
    understanding risk and recovery periods.

    Args:
        engine: SQLAlchemy database engine
        ticker: Stock ticker symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with drawdown metrics over time
    """
    if not start_date:
        start_date = (datetime.now() - relativedelta(years=10)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    query = """
        WITH price_data AS (
            SELECT
                d.date,
                f.close_price,
                MAX(f.close_price) OVER (
                    ORDER BY d.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as running_max
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE s.ticker = :ticker
              AND d.date BETWEEN :start_date AND :end_date
        )
        SELECT
            date,
            close_price,
            running_max as peak_price,
            ((close_price - running_max) / NULLIF(running_max, 0)) * 100 as drawdown_pct
        FROM price_data
        ORDER BY date
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(query),
            conn,
            params={"ticker": ticker, "start_date": start_date, "end_date": end_date},
        )


def get_max_drawdowns(engine: Engine) -> pd.DataFrame:
    """
    Get maximum drawdown for each stock over entire history.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        DataFrame with max drawdown for each stock
    """
    query = """
        WITH price_data AS (
            SELECT
                s.ticker,
                s.company_name,
                s.sector,
                d.date,
                f.close_price,
                MAX(f.close_price) OVER (
                    PARTITION BY s.stock_id
                    ORDER BY d.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as running_max
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
        ),
        drawdowns AS (
            SELECT
                ticker,
                company_name,
                sector,
                date,
                ((close_price - running_max) / NULLIF(running_max, 0)) * 100 as drawdown_pct
            FROM price_data
        )
        SELECT
            ticker,
            company_name,
            sector,
            MIN(drawdown_pct) as max_drawdown_pct,
            (SELECT date FROM drawdowns d2
             WHERE d2.ticker = drawdowns.ticker
             ORDER BY drawdown_pct LIMIT 1) as max_drawdown_date
        FROM drawdowns
        GROUP BY ticker, company_name, sector
        ORDER BY max_drawdown_pct
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def get_rolling_correlations(
    engine: Engine,
    ticker: str,
    window_days: int = 60,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calculate rolling correlations between a stock and macro indicators.

    Args:
        engine: SQLAlchemy database engine
        ticker: Stock ticker symbol
        window_days: Rolling window size in days
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with rolling correlations over time
    """
    if not start_date:
        start_date = (datetime.now() - relativedelta(years=5)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Get raw data first, then calculate rolling correlations in pandas
    query = """
        SELECT
            d.date,
            f.daily_return,
            f.selic_rate,
            f.usd_brl,
            f.inflation_rate
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        WHERE s.ticker = :ticker
          AND d.date BETWEEN :start_date AND :end_date
        ORDER BY d.date
    """

    with engine.connect() as conn:
        df = pd.read_sql(
            text(query),
            conn,
            params={"ticker": ticker, "start_date": start_date, "end_date": end_date},
        )

    if df.empty:
        return df

    # Calculate rolling correlations using pandas
    df["corr_selic"] = (
        df["daily_return"]
        .rolling(window=window_days, min_periods=30)
        .corr(df["selic_rate"])
    )
    df["corr_usd"] = (
        df["daily_return"]
        .rolling(window=window_days, min_periods=30)
        .corr(df["usd_brl"])
    )
    df["corr_inflation"] = (
        df["daily_return"]
        .rolling(window=window_days, min_periods=30)
        .corr(df["inflation_rate"])
    )

    return df


def get_selic_regime_performance(engine: Engine) -> pd.DataFrame:
    """
    Analyze stock performance during different SELIC rate regimes.

    Regimes:
    - Low: SELIC < 7%
    - Medium: 7% <= SELIC < 12%
    - High: SELIC >= 12%

    Args:
        engine: SQLAlchemy database engine

    Returns:
        DataFrame with performance by sector and SELIC regime
    """
    query = """
        WITH regime_data AS (
            SELECT
                s.ticker,
                s.sector,
                d.date,
                f.daily_return,
                f.selic_rate,
                CASE
                    WHEN f.selic_rate < 7 THEN 'Low (<7%)'
                    WHEN f.selic_rate < 12 THEN 'Medium (7-12%)'
                    ELSE 'High (>=12%)'
                END as selic_regime
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE s.sector IS NOT NULL
              AND f.selic_rate IS NOT NULL
        )
        SELECT
            sector,
            selic_regime,
            COUNT(*) as trading_days,
            AVG(daily_return) * 252 * 100 as annualized_return_pct,
            STDDEV(daily_return) * SQRT(252) * 100 as annualized_volatility_pct,
            AVG(selic_rate) as avg_selic_rate
        FROM regime_data
        GROUP BY sector, selic_regime
        ORDER BY sector, selic_regime
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def get_yearly_comparison(engine: Engine) -> pd.DataFrame:
    """
    Get year-over-year performance comparison for all stocks.

    Args:
        engine: SQLAlchemy database engine

    Returns:
        DataFrame with yearly returns for each stock
    """
    query = """
        WITH yearly_data AS (
            SELECT
                s.ticker,
                s.sector,
                d.year,
                FIRST_VALUE(f.close_price) OVER (
                    PARTITION BY s.stock_id, d.year
                    ORDER BY d.date
                ) as first_price,
                LAST_VALUE(f.close_price) OVER (
                    PARTITION BY s.stock_id, d.year
                    ORDER BY d.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as last_price,
                AVG(f.volatility_30d) as avg_volatility,
                SUM(f.volume) as total_volume
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            GROUP BY s.stock_id, s.ticker, s.sector, d.year, f.close_price, d.date
        )
        SELECT DISTINCT
            ticker,
            sector,
            year,
            ROUND(((last_price - first_price) / NULLIF(first_price, 0) * 100)::numeric, 2) as yearly_return_pct,
            ROUND((avg_volatility * 100)::numeric, 2) as avg_volatility_pct,
            total_volume
        FROM yearly_data
        ORDER BY ticker, year
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def get_crisis_periods_performance(engine: Engine) -> pd.DataFrame:
    """
    Analyze stock performance during known crisis periods.

    Crisis periods covered:
    - 2015-2016 Brazilian Recession
    - 2020 COVID-19 Crash
    - 2022 Rate Hiking Cycle

    Args:
        engine: SQLAlchemy database engine

    Returns:
        DataFrame with performance during crisis periods
    """
    query = """
        WITH crisis_periods AS (
            SELECT 'Brazilian Recession' as crisis, '2015-01-01'::date as start_date, '2016-12-31'::date as end_date
            UNION ALL
            SELECT 'COVID-19 Crash', '2020-02-01'::date, '2020-04-30'::date
            UNION ALL
            SELECT 'COVID Recovery', '2020-05-01'::date, '2020-12-31'::date
            UNION ALL
            SELECT 'Rate Hiking 2022', '2022-01-01'::date, '2022-12-31'::date
        ),
        crisis_returns AS (
            SELECT
                cp.crisis,
                s.ticker,
                s.sector,
                cp.start_date,
                cp.end_date,
                FIRST_VALUE(f.close_price) OVER (
                    PARTITION BY s.stock_id, cp.crisis
                    ORDER BY d.date
                ) as first_price,
                LAST_VALUE(f.close_price) OVER (
                    PARTITION BY s.stock_id, cp.crisis
                    ORDER BY d.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as last_price,
                MIN(f.close_price) OVER (
                    PARTITION BY s.stock_id, cp.crisis
                ) as min_price,
                MAX(f.close_price) OVER (
                    PARTITION BY s.stock_id, cp.crisis
                ) as max_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            CROSS JOIN crisis_periods cp
            WHERE d.date BETWEEN cp.start_date AND cp.end_date
        )
        SELECT DISTINCT
            crisis,
            ticker,
            sector,
            start_date,
            end_date,
            ROUND(((last_price - first_price) / NULLIF(first_price, 0) * 100)::numeric, 2) as period_return_pct,
            ROUND(((min_price - first_price) / NULLIF(first_price, 0) * 100)::numeric, 2) as max_drawdown_pct,
            ROUND(((max_price - first_price) / NULLIF(first_price, 0) * 100)::numeric, 2) as max_gain_pct
        FROM crisis_returns
        ORDER BY crisis, ticker
    """

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def get_historical_volatility(
    engine: Engine,
    ticker: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get historical volatility analysis.

    Args:
        engine: SQLAlchemy database engine
        ticker: Optional specific ticker (None for all stocks)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with volatility metrics over time
    """
    if not start_date:
        start_date = (datetime.now() - relativedelta(years=10)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    ticker_filter = "AND s.ticker = :ticker" if ticker else ""

    query = f"""
        SELECT
            s.ticker,
            d.date,
            d.year,
            d.month,
            f.volatility_30d * 100 as volatility_30d_pct,
            f.annualized_volatility * 100 as annualized_volatility_pct,
            f.close_price,
            f.volume
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        WHERE d.date BETWEEN :start_date AND :end_date
          {ticker_filter}
        ORDER BY s.ticker, d.date
    """

    params = {"start_date": start_date, "end_date": end_date}
    if ticker:
        params["ticker"] = ticker

    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)


def get_sector_performance_history(
    engine: Engine,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    frequency: str = "monthly",
) -> pd.DataFrame:
    """
    Get sector performance over time.

    Args:
        engine: SQLAlchemy database engine
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        frequency: 'daily', 'weekly', 'monthly', or 'yearly'

    Returns:
        DataFrame with sector performance over time
    """
    if not start_date:
        start_date = (datetime.now() - relativedelta(years=10)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Determine grouping based on frequency
    if frequency == "daily":
        date_group = "d.date"
        date_select = "d.date as period"
    elif frequency == "weekly":
        date_group = "DATE_TRUNC('week', d.date)"
        date_select = "DATE_TRUNC('week', d.date)::date as period"
    elif frequency == "monthly":
        date_group = "DATE_TRUNC('month', d.date)"
        date_select = "DATE_TRUNC('month', d.date)::date as period"
    else:  # yearly
        date_group = "d.year"
        date_select = "d.year::text || '-01-01' as period"

    query = f"""
        SELECT
            {date_select},
            s.sector,
            AVG(f.daily_return) * 100 as avg_daily_return_pct,
            AVG(f.volatility_30d) * 100 as avg_volatility_pct,
            SUM(f.volume) as total_volume,
            COUNT(DISTINCT s.stock_id) as stock_count
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        WHERE d.date BETWEEN :start_date AND :end_date
          AND s.sector IS NOT NULL
        GROUP BY {date_group}, s.sector
        ORDER BY period, s.sector
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(query),
            conn,
            params={"start_date": start_date, "end_date": end_date},
        )


def get_market_trends(
    engine: Engine,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get overall market trend data with macro indicators.

    Args:
        engine: SQLAlchemy database engine
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with market trends and macro indicators
    """
    if not start_date:
        start_date = (datetime.now() - relativedelta(years=10)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    query = """
        SELECT
            d.date,
            AVG(f.close_price) as avg_price,
            SUM(f.volume) as total_volume,
            AVG(f.daily_return) * 100 as avg_daily_return_pct,
            AVG(f.volatility_30d) * 100 as avg_volatility_pct,
            AVG(f.selic_rate) as selic_rate,
            AVG(f.usd_brl) as usd_brl,
            AVG(f.inflation_rate) as inflation_rate,
            COUNT(DISTINCT f.stock_id) as stocks_traded
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        WHERE d.date BETWEEN :start_date AND :end_date
        GROUP BY d.date
        ORDER BY d.date
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(query),
            conn,
            params={"start_date": start_date, "end_date": end_date},
        )


def get_best_worst_periods(
    engine: Engine,
    ticker: str,
    period_days: int = 30,
    n: int = 5,
) -> dict:
    """
    Find the best and worst performing periods for a stock.

    Args:
        engine: SQLAlchemy database engine
        ticker: Stock ticker symbol
        period_days: Length of period to analyze in days
        n: Number of best/worst periods to return

    Returns:
        Dictionary with 'best' and 'worst' DataFrames
    """
    query = f"""
        WITH rolling_returns AS (
            SELECT
                d.date as end_date,
                d.date - INTERVAL '{period_days} days' as start_date,
                f.close_price as end_price,
                LAG(f.close_price, {period_days}) OVER (ORDER BY d.date) as start_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE s.ticker = :ticker
        )
        SELECT
            start_date,
            end_date,
            start_price,
            end_price,
            ROUND(((end_price - start_price) / NULLIF(start_price, 0) * 100)::numeric, 2) as return_pct
        FROM rolling_returns
        WHERE start_price IS NOT NULL
        ORDER BY return_pct {{order}}
        LIMIT :n
    """

    with engine.connect() as conn:
        best = pd.read_sql(
            text(query.format(order="DESC")),
            conn,
            params={"ticker": ticker, "n": n},
        )
        worst = pd.read_sql(
            text(query.format(order="ASC")),
            conn,
            params={"ticker": ticker, "n": n},
        )

    return {"best": best, "worst": worst}


def get_monthly_returns_heatmap(
    engine: Engine,
    ticker: str,
) -> pd.DataFrame:
    """
    Get monthly returns organized by year and month for heatmap visualization.

    Args:
        engine: SQLAlchemy database engine
        ticker: Stock ticker symbol

    Returns:
        DataFrame with monthly returns pivoted by year and month
    """
    query = """
        WITH monthly_data AS (
            SELECT
                d.year,
                d.month,
                FIRST_VALUE(f.close_price) OVER (
                    PARTITION BY d.year, d.month
                    ORDER BY d.date
                ) as first_price,
                LAST_VALUE(f.close_price) OVER (
                    PARTITION BY d.year, d.month
                    ORDER BY d.date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as last_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE s.ticker = :ticker
        )
        SELECT DISTINCT
            year,
            month,
            ROUND(((last_price - first_price) / NULLIF(first_price, 0) * 100)::numeric, 2) as monthly_return_pct
        FROM monthly_data
        ORDER BY year, month
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"ticker": ticker})

    # Pivot for heatmap format
    if not df.empty:
        pivot_df = df.pivot(index="year", columns="month", values="monthly_return_pct")
        pivot_df.columns = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ][:len(pivot_df.columns)]
        return pivot_df

    return df


def get_comparative_performance(
    engine: Engine,
    tickers: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compare performance of multiple stocks over time.

    Args:
        engine: SQLAlchemy database engine
        tickers: List of stock tickers to compare
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with normalized performance (base 100)
    """
    if not start_date:
        start_date = (datetime.now() - relativedelta(years=5)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    ticker_list = ", ".join([f"'{t}'" for t in tickers])

    query = f"""
        WITH base_prices AS (
            SELECT
                s.ticker,
                FIRST_VALUE(f.close_price) OVER (
                    PARTITION BY s.stock_id
                    ORDER BY d.date
                ) as base_price
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE s.ticker IN ({ticker_list})
              AND d.date >= :start_date
        )
        SELECT
            s.ticker,
            d.date,
            f.close_price,
            (f.close_price / NULLIF(bp.base_price, 0)) * 100 as normalized_price
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        JOIN base_prices bp ON s.ticker = bp.ticker
        WHERE s.ticker IN ({ticker_list})
          AND d.date BETWEEN :start_date AND :end_date
        ORDER BY s.ticker, d.date
    """

    with engine.connect() as conn:
        return pd.read_sql(
            text(query),
            conn,
            params={"start_date": start_date, "end_date": end_date},
        )
