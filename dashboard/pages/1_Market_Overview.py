"""
1_Market_Overview.py
====================

Market Overview page showing key metrics and trends.

Updated: 2025-02-09 - Added date range selectors for historical analysis

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard.config import get_database_connection, get_config
from dashboard.components.charts import create_line_chart, create_bar_chart, create_candlestick_chart
from dashboard.components.queries import (
    get_market_summary,
    get_top_movers,
    get_stock_history,
    get_date_range_info,
    get_cumulative_returns,
    get_market_trends,
    calculate_period_dates,
    get_stock_list,
)

# Page config
st.set_page_config(page_title="Market Overview", page_icon="📊", layout="wide")
config = get_config()

st.title("📊 Market Overview")
st.markdown("Real-time view of Brazilian stock market performance and key metrics.")

# Get database connection
try:
    engine = get_database_connection()

    # ==========================================================================
    # Market Summary Section (Current Data)
    # ==========================================================================
    st.subheader("Market Summary")
    summary = get_market_summary(engine)

    if summary:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Active Stocks",
                f"{summary.get('total_stocks', 0)}",
            )

        with col2:
            return_val = summary.get("avg_daily_return", 0)
            st.metric(
                "Avg Daily Return",
                f"{return_val:.2f}%",
                delta=f"{return_val:.2f}%",
            )

        with col3:
            st.metric(
                "SELIC Rate",
                f"{summary.get('selic_rate', 0):.2f}%",
            )

        with col4:
            st.metric(
                "USD/BRL",
                f"R$ {summary.get('usd_brl', 0):.2f}",
            )

    st.markdown("---")

    # ==========================================================================
    # Top Movers Section
    # ==========================================================================
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🚀 Top Gainers")
        gainers = get_top_movers(engine, n=5, direction="gainers")
        if not gainers.empty:
            fig = create_bar_chart(
                gainers,
                x="daily_return_pct",
                y="ticker",
                color="daily_return_pct",
                orientation="h",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("📉 Top Losers")
        losers = get_top_movers(engine, n=5, direction="losers")
        if not losers.empty:
            fig = create_bar_chart(
                losers,
                x="daily_return_pct",
                y="ticker",
                color="daily_return_pct",
                orientation="h",
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ==========================================================================
    # Stock Price Trends Section (With Date Range)
    # ==========================================================================
    st.subheader("📈 Stock Price Trends")

    # Get available stocks
    stock_list = get_stock_list(engine)
    available_tickers = stock_list["ticker"].tolist() if not stock_list.empty else [
        "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "ABEV3.SA"
    ]

    col1, col2 = st.columns([2, 1])

    with col1:
        selected_stock = st.selectbox(
            "Select Stock",
            available_tickers,
            index=0,
        )

    with col2:
        period = st.selectbox(
            "Time Period",
            ["1M", "3M", "6M", "1Y", "3Y", "5Y", "10Y", "MAX"],
            index=3,  # Default to 1Y
            key="stock_trend_period",
        )

    # Calculate date range
    start_date, end_date = calculate_period_dates(period)

    # Get historical data with selected period
    history = get_stock_history(engine, selected_stock, start_date, end_date)

    if not history.empty:
        # Create candlestick chart for larger time frames, line chart for smaller
        if period in ["1M", "3M"]:
            fig = create_candlestick_chart(
                history,
                title=f"{selected_stock} - Price History ({period})",
                height=450,
            )
        else:
            fig = create_line_chart(
                history,
                x="date",
                y="close_price",
                title=f"{selected_stock} - Closing Price ({period})",
                height=450,
            )
        st.plotly_chart(fig, use_container_width=True)

        # Key statistics for selected period
        col1, col2, col3, col4 = st.columns(4)

        first_price = history.iloc[0]["close_price"]
        last_price = history.iloc[-1]["close_price"]
        period_return = ((last_price - first_price) / first_price) * 100 if first_price else 0
        max_price = history["close_price"].max()
        min_price = history["close_price"].min()

        with col1:
            st.metric(
                f"{period} Return",
                f"{period_return:.2f}%",
                delta=f"{period_return:.2f}%",
            )

        with col2:
            st.metric("Current Price", f"R$ {last_price:.2f}")

        with col3:
            st.metric(f"{period} High", f"R$ {max_price:.2f}")

        with col4:
            st.metric(f"{period} Low", f"R$ {min_price:.2f}")

        # Volume chart
        st.subheader("📊 Trading Volume")
        volume_days = min(60, len(history))
        fig_vol = create_bar_chart(
            history.tail(volume_days),
            x="date",
            y="volume",
            title=f"{selected_stock} - Trading Volume (Last {volume_days} Days)",
            height=250,
        )
        st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("---")

    # ==========================================================================
    # Market Trends with Macro Overlay
    # ==========================================================================
    st.subheader("📈 Market Trends & Macro Indicators")

    trend_period = st.selectbox(
        "Trend Period",
        ["1Y", "3Y", "5Y", "10Y"],
        index=1,
        key="market_trend_period",
    )

    start_date, end_date = calculate_period_dates(trend_period)
    market_trends = get_market_trends(engine, start_date, end_date)

    if not market_trends.empty:
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            row_heights=[0.6, 0.4],
            subplot_titles=("Average Market Volatility", "SELIC Rate & USD/BRL")
        )

        # Volatility
        fig.add_trace(
            go.Scatter(
                x=market_trends["date"],
                y=market_trends["avg_volatility_pct"],
                name="Market Volatility",
                line=dict(color="#00d4aa", width=1.5),
                fill="tozeroy",
                fillcolor="rgba(0, 212, 170, 0.2)",
            ),
            row=1, col=1
        )

        # SELIC Rate
        fig.add_trace(
            go.Scatter(
                x=market_trends["date"],
                y=market_trends["selic_rate"],
                name="SELIC Rate (%)",
                line=dict(color="#ff4b4b", width=2),
            ),
            row=2, col=1
        )

        # USD/BRL on secondary axis
        fig.add_trace(
            go.Scatter(
                x=market_trends["date"],
                y=market_trends["usd_brl"],
                name="USD/BRL",
                line=dict(color="#ffd700", width=2, dash="dash"),
                yaxis="y4",
            ),
            row=2, col=1
        )

        fig.update_layout(
            template="plotly_dark",
            height=500,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            yaxis4=dict(
                anchor="x2",
                overlaying="y2",
                side="right",
                title="USD/BRL",
            ),
        )

        fig.update_xaxes(gridcolor="#2d3548")
        fig.update_yaxes(gridcolor="#2d3548")

        st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # Comparative Returns Section
    # ==========================================================================
    st.markdown("---")
    st.subheader("📊 Compare Multiple Stocks")

    col1, col2 = st.columns([3, 1])

    with col1:
        compare_stocks = st.multiselect(
            "Select stocks to compare",
            available_tickers,
            default=available_tickers[:4] if len(available_tickers) >= 4 else available_tickers,
            max_selections=6,
        )

    with col2:
        compare_period = st.selectbox(
            "Period",
            ["1Y", "3Y", "5Y"],
            index=0,
            key="compare_period",
        )

    if compare_stocks:
        start_date, end_date = calculate_period_dates(compare_period)

        # Get cumulative returns for each stock
        all_returns = []
        for ticker in compare_stocks:
            returns = get_cumulative_returns(engine, ticker, start_date, end_date)
            if not returns.empty:
                returns["ticker"] = ticker
                all_returns.append(returns)

        if all_returns:
            combined = pd.concat(all_returns, ignore_index=True)

            fig = go.Figure()

            import plotly.express as px
            colors = px.colors.qualitative.Set2[:len(compare_stocks)]

            for i, ticker in enumerate(compare_stocks):
                ticker_data = combined[combined["ticker"] == ticker]
                fig.add_trace(go.Scatter(
                    x=ticker_data["date"],
                    y=ticker_data["cumulative_return_pct"],
                    name=ticker,
                    line=dict(color=colors[i], width=2),
                ))

            fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

            fig.update_layout(
                title=f"Cumulative Returns Comparison ({compare_period})",
                template="plotly_dark",
                height=400,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(gridcolor="#2d3548"),
                yaxis=dict(gridcolor="#2d3548", title="Cumulative Return (%)"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
                hovermode="x unified",
            )

            st.plotly_chart(fig, use_container_width=True)

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the database is running and contains data.")
