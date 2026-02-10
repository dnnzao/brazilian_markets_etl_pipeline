"""
3_Macro_Correlation.py
======================

Macroeconomic correlation analysis page.

Updated: 2025-02-09 - Added historical correlation analysis and rolling correlations

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dashboard.config import get_database_connection, get_config
from dashboard.components.charts import create_line_chart, create_heatmap, create_bar_chart
from dashboard.components.queries import (
    get_correlation_data,
    get_stock_history,
    get_rolling_correlations,
    get_selic_regime_performance,
    get_market_trends,
    calculate_period_dates,
    get_stock_list,
)

# Page config
st.set_page_config(page_title="Macro Correlation", page_icon="🔄", layout="wide")
config = get_config()

st.title("🔄 Macro Correlation Analysis")
st.markdown("""
Analyze how macroeconomic factors affect stock performance.
Explore relationships between SELIC rate, USD/BRL, inflation, and stock returns
across different time horizons.
""")

# Get database connection
try:
    engine = get_database_connection()

    # Get available stocks
    stock_list = get_stock_list(engine)
    available_tickers = stock_list["ticker"].tolist() if not stock_list.empty else [
        "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA"
    ]

    # ==========================================================================
    # Current Correlation Analysis
    # ==========================================================================
    st.subheader("📊 Sector Correlation with Macro Indicators")

    col1, col2 = st.columns([1, 3])

    with col1:
        days = st.selectbox(
            "Analysis Period",
            [90, 180, 252, 504, 756, 1260],
            format_func=lambda x: {
                90: "Last 90 days (~3 months)",
                180: "Last 180 days (~6 months)",
                252: "Last 252 days (~1 year)",
                504: "Last 504 days (~2 years)",
                756: "Last 756 days (~3 years)",
                1260: "Last 1260 days (~5 years)",
            }.get(x, f"Last {x} days"),
            index=2,
        )

    # Correlation Analysis
    corr_data = get_correlation_data(engine, days=days)

    if not corr_data.empty:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Correlation with SELIC Rate**")
            sorted_selic = corr_data.dropna(subset=["corr_selic"]).sort_values("corr_selic")
            fig = create_bar_chart(
                sorted_selic,
                x="corr_selic",
                y="sector",
                color="corr_selic",
                orientation="h",
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("**Correlation with USD/BRL**")
            sorted_usd = corr_data.dropna(subset=["corr_usd"]).sort_values("corr_usd")
            fig = create_bar_chart(
                sorted_usd,
                x="corr_usd",
                y="sector",
                color="corr_usd",
                orientation="h",
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Interpretation
        with st.expander("📖 Interpretation Guide", expanded=False):
            st.markdown("""
            **Understanding Correlations:**

            - **Positive correlation with SELIC**: Sector tends to perform better when interest rates rise.
              Banks often benefit from higher rates as it improves their net interest margins.

            - **Negative correlation with SELIC**: Sector tends to perform worse when interest rates rise.
              Growth stocks and real estate often struggle with higher borrowing costs.

            - **Positive correlation with USD**: Sector benefits from currency depreciation (exporters).
              Commodity companies like Vale benefit when the real weakens.

            - **Negative correlation with USD**: Sector suffers from currency depreciation (importers).
              Companies with dollar-denominated debt or import-dependent operations suffer.

            **Correlation Strength:**
            - |r| < 0.3: Weak correlation
            - 0.3 ≤ |r| < 0.7: Moderate correlation
            - |r| ≥ 0.7: Strong correlation
            """)

    st.markdown("---")

    # ==========================================================================
    # Stock vs Macro Overlay
    # ==========================================================================
    st.subheader("📈 Stock vs Macro Indicator Overlay")

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        selected_stock = st.selectbox(
            "Select Stock",
            available_tickers,
            key="macro_stock",
        )

    with col2:
        macro_indicator = st.selectbox(
            "Macro Indicator",
            ["selic_rate", "usd_brl"],
            format_func=lambda x: "SELIC Rate" if x == "selic_rate" else "USD/BRL",
        )

    with col3:
        overlay_period = st.selectbox(
            "Time Period",
            ["1Y", "3Y", "5Y", "10Y"],
            index=1,
            key="overlay_period",
        )

    start_date, end_date = calculate_period_dates(overlay_period)
    history = get_stock_history(engine, selected_stock, start_date, end_date)

    if not history.empty:
        # Create dual-axis chart
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        # Stock price line
        fig.add_trace(
            go.Scatter(
                x=history["date"],
                y=history["close_price"],
                name=f"{selected_stock} Price",
                line=dict(color="#00d4aa", width=2),
            ),
            secondary_y=False,
        )

        # Macro indicator line
        indicator_label = "SELIC Rate (%)" if macro_indicator == "selic_rate" else "USD/BRL"
        fig.add_trace(
            go.Scatter(
                x=history["date"],
                y=history[macro_indicator],
                name=indicator_label,
                line=dict(color="#ff4b4b", width=2, dash="dash"),
            ),
            secondary_y=True,
        )

        fig.update_layout(
            title=f"{selected_stock} vs {indicator_label} ({overlay_period})",
            template="plotly_dark",
            height=450,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
        )

        fig.update_xaxes(gridcolor="#2d3548")
        fig.update_yaxes(title_text="Stock Price (R$)", secondary_y=False, gridcolor="#2d3548")
        fig.update_yaxes(title_text=indicator_label, secondary_y=True, gridcolor="#2d3548")

        st.plotly_chart(fig, use_container_width=True)

        # Calculate correlation for the selected period
        if macro_indicator in history.columns:
            correlation = history["close_price"].corr(history[macro_indicator])
            st.metric(
                f"Correlation ({selected_stock} vs {indicator_label})",
                f"{correlation:.3f}",
                help="Pearson correlation coefficient for the selected period"
            )

    st.markdown("---")

    # ==========================================================================
    # Rolling Correlations Over Time
    # ==========================================================================
    st.subheader("🔄 Rolling Correlations Over Time")
    st.markdown("""
    See how the correlation between stock returns and macro indicators changes over time.
    This reveals regime shifts and evolving market dynamics that static correlations miss.
    """)

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        rolling_stock = st.selectbox(
            "Select Stock",
            available_tickers,
            key="rolling_stock",
        )

    with col2:
        rolling_window = st.slider(
            "Rolling Window (days)",
            min_value=30,
            max_value=180,
            value=60,
            step=10,
            help="Number of trading days for rolling correlation calculation",
        )

    with col3:
        rolling_period = st.selectbox(
            "Time Period",
            ["3Y", "5Y", "10Y"],
            index=1,
            key="rolling_period",
        )

    start_date, end_date = calculate_period_dates(rolling_period)
    rolling_corr = get_rolling_correlations(
        engine, rolling_stock, window_days=rolling_window, start_date=start_date
    )

    if not rolling_corr.empty and "corr_selic" in rolling_corr.columns:
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=rolling_corr["date"],
            y=rolling_corr["corr_selic"],
            name="Correlation with SELIC",
            line=dict(color="#00d4aa", width=2),
            fill="tozeroy",
            fillcolor="rgba(0, 212, 170, 0.1)",
        ))

        fig.add_trace(go.Scatter(
            x=rolling_corr["date"],
            y=rolling_corr["corr_usd"],
            name="Correlation with USD/BRL",
            line=dict(color="#ff4b4b", width=2),
            fill="tozeroy",
            fillcolor="rgba(255, 75, 75, 0.1)",
        ))

        # Add reference lines
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_hline(y=0.3, line_dash="dot", line_color="white", opacity=0.3)
        fig.add_hline(y=-0.3, line_dash="dot", line_color="white", opacity=0.3)

        fig.update_layout(
            title=f"{rolling_stock} - {rolling_window}-Day Rolling Correlations",
            template="plotly_dark",
            height=450,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#2d3548"),
            yaxis=dict(gridcolor="#2d3548", title="Correlation", range=[-1, 1]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True)

        # Statistics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            avg_selic_corr = rolling_corr["corr_selic"].mean()
            st.metric("Avg SELIC Correlation", f"{avg_selic_corr:.3f}")

        with col2:
            avg_usd_corr = rolling_corr["corr_usd"].mean()
            st.metric("Avg USD Correlation", f"{avg_usd_corr:.3f}")

        with col3:
            std_selic = rolling_corr["corr_selic"].std()
            st.metric("SELIC Corr Std Dev", f"{std_selic:.3f}")

        with col4:
            std_usd = rolling_corr["corr_usd"].std()
            st.metric("USD Corr Std Dev", f"{std_usd:.3f}")

    st.markdown("---")

    # ==========================================================================
    # Historical Macro Trends
    # ==========================================================================
    st.subheader("📈 Historical Macro Trends")
    st.markdown("""
    Visualize how key macro indicators have evolved over time and their relationship
    with overall market performance.
    """)

    macro_trend_period = st.selectbox(
        "Time Period",
        ["3Y", "5Y", "10Y", "MAX"],
        index=2,
        key="macro_trend_period",
    )

    start_date, end_date = calculate_period_dates(macro_trend_period)
    market_trends = get_market_trends(engine, start_date, end_date)

    if not market_trends.empty:
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.33, 0.33, 0.33],
            subplot_titles=("SELIC Rate (%)", "USD/BRL Exchange Rate", "Inflation Rate (%)")
        )

        # SELIC
        fig.add_trace(
            go.Scatter(
                x=market_trends["date"],
                y=market_trends["selic_rate"],
                name="SELIC Rate",
                line=dict(color="#00d4aa", width=2),
                fill="tozeroy",
                fillcolor="rgba(0, 212, 170, 0.2)",
            ),
            row=1, col=1
        )

        # USD/BRL
        fig.add_trace(
            go.Scatter(
                x=market_trends["date"],
                y=market_trends["usd_brl"],
                name="USD/BRL",
                line=dict(color="#ffd700", width=2),
                fill="tozeroy",
                fillcolor="rgba(255, 215, 0, 0.2)",
            ),
            row=2, col=1
        )

        # Inflation
        if "inflation_rate" in market_trends.columns:
            fig.add_trace(
                go.Scatter(
                    x=market_trends["date"],
                    y=market_trends["inflation_rate"],
                    name="Inflation",
                    line=dict(color="#ff4b4b", width=2),
                    fill="tozeroy",
                    fillcolor="rgba(255, 75, 75, 0.2)",
                ),
                row=3, col=1
            )

        fig.update_layout(
            template="plotly_dark",
            height=700,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )

        fig.update_xaxes(gridcolor="#2d3548")
        fig.update_yaxes(gridcolor="#2d3548")

        st.plotly_chart(fig, use_container_width=True)

        # Key statistics
        st.subheader("📊 Period Statistics")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**SELIC Rate**")
            st.metric("Current", f"{market_trends['selic_rate'].iloc[-1]:.2f}%")
            st.metric("Period Average", f"{market_trends['selic_rate'].mean():.2f}%")
            st.metric("Period High", f"{market_trends['selic_rate'].max():.2f}%")
            st.metric("Period Low", f"{market_trends['selic_rate'].min():.2f}%")

        with col2:
            st.markdown("**USD/BRL**")
            st.metric("Current", f"R$ {market_trends['usd_brl'].iloc[-1]:.2f}")
            st.metric("Period Average", f"R$ {market_trends['usd_brl'].mean():.2f}")
            st.metric("Period High", f"R$ {market_trends['usd_brl'].max():.2f}")
            st.metric("Period Low", f"R$ {market_trends['usd_brl'].min():.2f}")

        with col3:
            if "inflation_rate" in market_trends.columns:
                st.markdown("**Inflation (IPCA)**")
                st.metric("Current", f"{market_trends['inflation_rate'].iloc[-1]:.2f}%")
                st.metric("Period Average", f"{market_trends['inflation_rate'].mean():.2f}%")
                st.metric("Period High", f"{market_trends['inflation_rate'].max():.2f}%")
                st.metric("Period Low", f"{market_trends['inflation_rate'].min():.2f}%")

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the database is running and contains data.")
