"""
4_Stock_Screener.py
===================

Stock screener with filtering capabilities and historical analysis.

Updated: 2025-02-09 - Added long-term metrics (3Y, 5Y, 10Y returns) and historical views

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sqlalchemy import text

from dashboard.config import get_database_connection, get_config
from dashboard.components.charts import create_candlestick_chart, create_line_chart, create_bar_chart
from dashboard.components.queries import (
    get_stock_list,
    get_stock_history,
    get_multi_year_returns,
    get_drawdown_analysis,
    get_monthly_returns_heatmap,
    get_cumulative_returns,
    calculate_period_dates,
)

# Page config
st.set_page_config(page_title="Stock Screener", page_icon="🔍", layout="wide")
config = get_config()

st.title("🔍 Stock Screener")
st.markdown("""
Filter and analyze stocks based on various criteria. Explore long-term performance,
risk metrics, and historical patterns for each stock.
""")

# Get database connection
try:
    engine = get_database_connection()

    # ==========================================================================
    # Long-Term Returns Overview
    # ==========================================================================
    st.subheader("📊 Multi-Year Returns Overview")

    multi_year = get_multi_year_returns(engine)

    if not multi_year.empty:
        # Create tabs for different views
        tab1, tab2 = st.tabs(["📋 Table View", "📊 Chart View"])

        with tab1:
            # Sorting options
            col1, col2 = st.columns([1, 3])

            with col1:
                sort_by = st.selectbox(
                    "Sort by",
                    ["return_5y", "return_10y", "return_3y", "return_1y", "ticker"],
                    format_func=lambda x: {
                        "return_5y": "5-Year Return",
                        "return_10y": "10-Year Return",
                        "return_3y": "3-Year Return",
                        "return_1y": "1-Year Return",
                        "ticker": "Ticker",
                    }.get(x, x),
                )

            # Sort dataframe
            ascending = sort_by == "ticker"
            sorted_df = multi_year.sort_values(sort_by, ascending=ascending, na_position="last")

            # Style function for returns
            def style_return(val):
                if pd.isna(val):
                    return ""
                color = "#00d4aa" if val >= 0 else "#ff4b4b"
                return f"color: {color}; font-weight: bold"

            display_df = sorted_df.copy()
            display_df.columns = [
                "Ticker", "Company", "Sector", "Current Price",
                "1Y Return (%)", "3Y Return (%)", "5Y Return (%)", "10Y Return (%)"
            ]

            # Define safe formatters that handle None/NaN values
            def safe_price_format(val):
                if pd.isna(val):
                    return "N/A"
                return f"R$ {val:.2f}"

            def safe_pct_format(val):
                if pd.isna(val):
                    return "N/A"
                return f"{val:.1f}%"

            st.dataframe(
                display_df.style.applymap(
                    style_return,
                    subset=["1Y Return (%)", "3Y Return (%)", "5Y Return (%)", "10Y Return (%)"]
                ).format({
                    "Current Price": safe_price_format,
                    "1Y Return (%)": safe_pct_format,
                    "3Y Return (%)": safe_pct_format,
                    "5Y Return (%)": safe_pct_format,
                    "10Y Return (%)": safe_pct_format,
                }),
                use_container_width=True,
                height=500,
            )

        with tab2:
            # Bar chart comparison
            col1, col2 = st.columns(2)

            with col1:
                chart_period = st.selectbox(
                    "Return Period",
                    ["return_5y", "return_3y", "return_1y", "return_10y"],
                    format_func=lambda x: {
                        "return_5y": "5-Year Return",
                        "return_10y": "10-Year Return",
                        "return_3y": "3-Year Return",
                        "return_1y": "1-Year Return",
                    }.get(x, x),
                )

            with col2:
                top_n = st.slider("Top N Stocks", 5, 20, 10)

            # Ensure numeric types for the selected period
            chart_df = multi_year.copy()
            chart_df[chart_period] = pd.to_numeric(chart_df[chart_period], errors='coerce')
            chart_df = chart_df.dropna(subset=[chart_period])

            # Top performers
            if not chart_df.empty:
                top_stocks = chart_df.nlargest(top_n, chart_period)
                fig = create_bar_chart(
                    top_stocks,
                    x=chart_period,
                    y="ticker",
                    color=chart_period,
                    orientation="h",
                    title=f"Top {top_n} Stocks by {chart_period.replace('return_', '').upper()} Return",
                    height=400,
                )
                st.plotly_chart(fig, use_container_width=True)

                # Bottom performers
                bottom_stocks = chart_df.nsmallest(top_n, chart_period)
                fig = create_bar_chart(
                    bottom_stocks,
                    x=chart_period,
                    y="ticker",
                    color=chart_period,
                    orientation="h",
                    title=f"Bottom {top_n} Stocks by {chart_period.replace('return_', '').upper()} Return",
                    height=400,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"No data available for {chart_period.replace('return_', '').upper()} returns")

    st.markdown("---")

    # ==========================================================================
    # Stock Screener with Filters
    # ==========================================================================
    st.subheader("🔎 Advanced Screener")

    # Filters
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        sectors = st.multiselect(
            "Sector",
            [
                "Banking",
                "Oil & Gas",
                "Mining",
                "Food & Beverage",
                "Retail",
                "Technology",
                "Healthcare",
                "Materials",
                "Industrials",
                "Transportation",
                "Financial Services",
            ],
            default=[],
        )

    with col2:
        min_return = st.slider(
            "Min Monthly Return (%)",
            min_value=-50.0,
            max_value=50.0,
            value=-50.0,
            step=1.0,
        )

    with col3:
        max_volatility = st.slider(
            "Max Volatility (%)",
            min_value=0.0,
            max_value=100.0,
            value=100.0,
            step=5.0,
        )

    with col4:
        market_cap = st.multiselect(
            "Market Cap",
            ["Large Cap", "Mid Cap", "Small Cap"],
            default=[],
        )

    # Build query
    query = """
        WITH latest_date AS (
            SELECT MAX(d.date) as max_date
            FROM analytics.fact_daily_market f
            JOIN analytics.dim_date d ON f.date_id = d.date_id
        )
        SELECT
            s.ticker,
            s.company_name,
            s.sector,
            s.market_cap_category,
            f.close_price,
            f.daily_return * 100 as daily_return_pct,
            f.monthly_return * 100 as monthly_return_pct,
            f.volatility_30d * 100 as volatility_pct,
            f.volume
        FROM analytics.fact_daily_market f
        JOIN analytics.dim_stock s ON f.stock_id = s.stock_id
        JOIN analytics.dim_date d ON f.date_id = d.date_id
        CROSS JOIN latest_date ld
        WHERE d.date = ld.max_date
    """

    # Apply filters
    conditions = []
    if sectors:
        sector_list = ", ".join([f"'{s}'" for s in sectors])
        conditions.append(f"s.sector IN ({sector_list})")
    if min_return > -50:
        conditions.append(f"f.monthly_return * 100 >= {min_return}")
    if max_volatility < 100:
        conditions.append(f"f.volatility_30d * 100 <= {max_volatility}")
    if market_cap:
        cap_list = ", ".join([f"'{c}'" for c in market_cap])
        conditions.append(f"s.market_cap_category IN ({cap_list})")

    if conditions:
        query += " AND " + " AND ".join(conditions)

    query += " ORDER BY f.monthly_return DESC"

    # Execute query
    with engine.connect() as conn:
        screener_data = pd.read_sql(text(query), conn)

    # Results
    st.subheader(f"Results ({len(screener_data)} stocks)")

    if not screener_data.empty:
        # Format and display
        display_df = screener_data.copy()
        display_df.columns = [
            "Ticker",
            "Company",
            "Sector",
            "Market Cap",
            "Price",
            "Daily Return (%)",
            "Monthly Return (%)",
            "Volatility (%)",
            "Volume",
        ]

        # Define safe formatters that handle None/NaN values
        def safe_price(val):
            if pd.isna(val):
                return "N/A"
            return f"R$ {val:.2f}"

        def safe_pct(val):
            if pd.isna(val):
                return "N/A"
            return f"{val:.2f}"

        def safe_volume(val):
            if pd.isna(val):
                return "N/A"
            return f"{val:,.0f}"

        st.dataframe(
            display_df.style.format(
                {
                    "Price": safe_price,
                    "Daily Return (%)": safe_pct,
                    "Monthly Return (%)": safe_pct,
                    "Volatility (%)": safe_pct,
                    "Volume": safe_volume,
                }
            ),
            use_container_width=True,
            height=350,
        )

        st.markdown("---")

        # ==========================================================================
        # Stock Detail View
        # ==========================================================================
        st.subheader("📈 Stock Deep Dive")

        col1, col2 = st.columns([2, 1])

        with col1:
            selected = st.selectbox(
                "Select stock for detailed view",
                screener_data["ticker"].tolist(),
            )

        with col2:
            detail_period = st.selectbox(
                "Time Period",
                ["1Y", "3Y", "5Y", "10Y"],
                index=1,
                key="detail_period",
            )

        if selected:
            start_date, end_date = calculate_period_dates(detail_period)
            history = get_stock_history(engine, selected, start_date, end_date)

            if not history.empty:
                # Price chart with volume
                fig = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.08,
                    row_heights=[0.7, 0.3],
                    subplot_titles=(f"{selected} - Price History ({detail_period})", "Volume")
                )

                # Price line
                fig.add_trace(
                    go.Scatter(
                        x=history["date"],
                        y=history["close_price"],
                        name="Close Price",
                        line=dict(color="#00d4aa", width=2),
                    ),
                    row=1, col=1
                )

                # Volume bars
                colors = [
                    "#00d4aa" if history.iloc[i]["daily_return_pct"] >= 0 else "#ff4b4b"
                    for i in range(len(history))
                ]
                fig.add_trace(
                    go.Bar(
                        x=history["date"],
                        y=history["volume"],
                        name="Volume",
                        marker_color=colors,
                    ),
                    row=2, col=1
                )

                fig.update_layout(
                    template="plotly_dark",
                    height=500,
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                )

                fig.update_xaxes(gridcolor="#2d3548")
                fig.update_yaxes(gridcolor="#2d3548")

                st.plotly_chart(fig, use_container_width=True)

                # Key statistics
                col1, col2, col3, col4 = st.columns(4)

                first_price = history.iloc[0]["close_price"]
                latest = history.iloc[-1]
                period_return = ((latest["close_price"] - first_price) / first_price) * 100 if first_price else 0

                with col1:
                    st.metric(
                        "Close Price",
                        f"R$ {latest['close_price']:.2f}",
                    )

                with col2:
                    st.metric(
                        f"{detail_period} Return",
                        f"{period_return:.2f}%",
                        delta=f"{period_return:.2f}%",
                    )

                with col3:
                    st.metric(
                        "30D Volatility",
                        f"{latest['volatility_pct']:.2f}%",
                    )

                with col4:
                    st.metric(
                        "Avg Volume",
                        f"{history['volume'].mean():,.0f}",
                    )

                # ==========================================================================
                # Monthly Returns Heatmap
                # ==========================================================================
                st.markdown("---")
                st.subheader("🗓️ Monthly Returns Pattern")

                monthly_returns = get_monthly_returns_heatmap(engine, selected)

                if not monthly_returns.empty and isinstance(monthly_returns, pd.DataFrame):
                    fig = go.Figure(data=go.Heatmap(
                        z=monthly_returns.values,
                        x=monthly_returns.columns.tolist(),
                        y=monthly_returns.index.tolist(),
                        colorscale=[[0, "#ff4b4b"], [0.5, "#1a1f2c"], [1, "#00d4aa"]],
                        zmid=0,
                        text=monthly_returns.values,
                        texttemplate="%{text:.1f}%",
                        textfont={"size": 9},
                        hovertemplate="Year: %{y}<br>Month: %{x}<br>Return: %{z:.2f}%<extra></extra>",
                    ))

                    fig.update_layout(
                        title=f"{selected} Monthly Returns (%)",
                        template="plotly_dark",
                        height=400,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(title="Month"),
                        yaxis=dict(title="Year", autorange="reversed"),
                    )

                    st.plotly_chart(fig, use_container_width=True)

                # ==========================================================================
                # Drawdown Analysis
                # ==========================================================================
                st.markdown("---")
                st.subheader("📉 Drawdown Analysis")

                drawdown_data = get_drawdown_analysis(engine, selected, start_date, end_date)

                if not drawdown_data.empty:
                    fig = go.Figure()

                    fig.add_trace(go.Scatter(
                        x=drawdown_data["date"],
                        y=drawdown_data["drawdown_pct"],
                        name="Drawdown",
                        fill="tozeroy",
                        line=dict(color="#ff4b4b", width=1.5),
                        fillcolor="rgba(255, 75, 75, 0.3)",
                    ))

                    fig.update_layout(
                        title=f"{selected} - Drawdown from Peak (%)",
                        template="plotly_dark",
                        height=350,
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        xaxis=dict(gridcolor="#2d3548"),
                        yaxis=dict(gridcolor="#2d3548", title="Drawdown (%)"),
                    )

                    st.plotly_chart(fig, use_container_width=True)

                    # Drawdown stats
                    col1, col2, col3 = st.columns(3)

                    max_drawdown = drawdown_data["drawdown_pct"].min()
                    max_dd_date = drawdown_data.loc[drawdown_data["drawdown_pct"].idxmin(), "date"]
                    avg_drawdown = drawdown_data["drawdown_pct"].mean()

                    with col1:
                        st.metric("Maximum Drawdown", f"{max_drawdown:.2f}%")

                    with col2:
                        st.metric("Max Drawdown Date", str(max_dd_date))

                    with col3:
                        st.metric("Average Drawdown", f"{avg_drawdown:.2f}%")

                # ==========================================================================
                # Cumulative Returns Comparison
                # ==========================================================================
                st.markdown("---")
                st.subheader("📊 Compare with Other Stocks")

                compare_tickers = st.multiselect(
                    "Select stocks to compare",
                    [t for t in screener_data["ticker"].tolist() if t != selected],
                    default=[],
                    max_selections=5,
                )

                if compare_tickers:
                    all_tickers = [selected] + compare_tickers

                    fig = go.Figure()
                    colors = px.colors.qualitative.Set2[:len(all_tickers)]

                    for i, ticker in enumerate(all_tickers):
                        returns = get_cumulative_returns(engine, ticker, start_date, end_date)
                        if not returns.empty:
                            fig.add_trace(go.Scatter(
                                x=returns["date"],
                                y=returns["cumulative_return_pct"],
                                name=ticker,
                                line=dict(color=colors[i], width=2 if ticker == selected else 1.5),
                            ))

                    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

                    fig.update_layout(
                        title=f"Cumulative Returns Comparison ({detail_period})",
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

    else:
        st.info("No stocks match the selected criteria. Try adjusting the filters.")

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the database is running and contains data.")
    st.exception(e)
