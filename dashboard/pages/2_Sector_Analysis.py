"""
2_Sector_Analysis.py
====================

Sector performance analysis page.

Updated: 2025-02-09 - Added historical sector analysis with multi-year trends

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
from dashboard.components.charts import create_bar_chart
from dashboard.components.queries import (
    get_sector_performance,
    get_sector_performance_history,
    get_selic_regime_performance,
    calculate_period_dates,
)

# Page config
st.set_page_config(page_title="Sector Analysis", page_icon="🏭", layout="wide")
config = get_config()

st.title("🏭 Sector Analysis")
st.markdown("Compare performance across different market sectors and analyze historical trends.")

# Get database connection
try:
    engine = get_database_connection()

    # ==========================================================================
    # Current Sector Performance
    # ==========================================================================
    st.subheader("📊 Current Sector Performance")

    col1, col2 = st.columns([1, 3])

    with col1:
        period_days = st.selectbox(
            "Analysis Period",
            [30, 60, 90, 180, 365, 730, 1825],
            format_func=lambda x: {
                30: "Last 30 days",
                60: "Last 60 days",
                90: "Last 90 days",
                180: "Last 6 months",
                365: "Last 1 year",
                730: "Last 2 years",
                1825: "Last 5 years",
            }.get(x, f"Last {x} days"),
            index=3,
        )

    # Sector Performance
    sector_data = get_sector_performance(engine, days=period_days)

    if not sector_data.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Return by sector
            sorted_data = sector_data.sort_values("avg_return", ascending=True)
            fig = create_bar_chart(
                sorted_data,
                x="avg_return",
                y="sector",
                color="avg_return",
                orientation="h",
                title="Average Return by Sector (%)",
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Volatility by sector
            sorted_vol = sector_data.sort_values("avg_volatility", ascending=True)
            fig = create_bar_chart(
                sorted_vol,
                x="avg_volatility",
                y="sector",
                color="avg_volatility",
                orientation="h",
                title="Average Volatility by Sector (%)",
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True)

        # Risk-Return Scatter
        st.subheader("📈 Risk-Return Analysis")

        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=sector_data["avg_volatility"],
            y=sector_data["avg_return"],
            mode="markers+text",
            text=sector_data["sector"],
            textposition="top center",
            marker=dict(
                size=sector_data["stock_count"] * 5,
                color=sector_data["sharpe_ratio"],
                colorscale=[[0, "#ff4b4b"], [0.5, "#1a1f2c"], [1, "#00d4aa"]],
                colorbar=dict(title="Sharpe Ratio"),
                showscale=True,
            ),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Return: %{y:.2f}%<br>"
                "Volatility: %{x:.2f}%<br>"
                "<extra></extra>"
            ),
        ))

        # Add quadrant lines
        avg_return = sector_data["avg_return"].mean()
        avg_vol = sector_data["avg_volatility"].mean()

        fig.add_hline(y=avg_return, line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_vline(x=avg_vol, line_dash="dash", line_color="gray", opacity=0.5)

        fig.update_layout(
            title="Sector Risk-Return Profile",
            template="plotly_dark",
            height=450,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="Volatility (%)", gridcolor="#2d3548"),
            yaxis=dict(title="Return (%)", gridcolor="#2d3548"),
            showlegend=False,
        )

        # Add quadrant labels
        fig.add_annotation(
            x=avg_vol * 0.5, y=avg_return * 1.5,
            text="Low Risk, High Return",
            showarrow=False,
            font=dict(color="#00d4aa", size=10),
        )
        fig.add_annotation(
            x=avg_vol * 1.5, y=avg_return * 1.5,
            text="High Risk, High Return",
            showarrow=False,
            font=dict(color="#ffd700", size=10),
        )

        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Sector data table
        st.subheader("📋 Sector Details")
        display_df = sector_data.copy()
        display_df.columns = [
            "Sector",
            "Avg Return (%)",
            "Avg Volatility (%)",
            "Stock Count",
            "Sharpe Ratio",
        ]

        # Style based on values
        def style_sharpe(val):
            if pd.isna(val):
                return ""
            color = "#00d4aa" if val > 0 else "#ff4b4b"
            return f"color: {color}"

        # Define safe formatter that handles None/NaN values
        def safe_float_format(val):
            if pd.isna(val):
                return "N/A"
            return f"{val:.2f}"

        st.dataframe(
            display_df.style.format(
                {
                    "Avg Return (%)": safe_float_format,
                    "Avg Volatility (%)": safe_float_format,
                    "Sharpe Ratio": safe_float_format,
                }
            ).applymap(style_sharpe, subset=["Sharpe Ratio"]),
            use_container_width=True,
        )

    else:
        st.warning("No sector data available for the selected period.")

    # ==========================================================================
    # Historical Sector Performance
    # ==========================================================================
    st.markdown("---")
    st.subheader("📜 Historical Sector Performance")
    st.markdown("""
    Analyze how sector performance has evolved over time. Identify sector rotation
    patterns and long-term trends.
    """)

    col1, col2 = st.columns([1, 1])

    with col1:
        history_period = st.selectbox(
            "Historical Period",
            ["1Y", "3Y", "5Y", "10Y"],
            index=2,
            key="sector_history_period",
        )

    with col2:
        frequency = st.selectbox(
            "Aggregation",
            ["monthly", "weekly", "daily"],
            index=0,
            key="sector_frequency",
        )

    start_date, end_date = calculate_period_dates(history_period)
    sector_history = get_sector_performance_history(
        engine, start_date, end_date, frequency=frequency
    )

    if not sector_history.empty:
        # Line chart of sector performance over time
        sectors = sector_history["sector"].unique().tolist()

        fig = go.Figure()
        colors = px.colors.qualitative.Set2[:len(sectors)]

        for i, sector in enumerate(sectors):
            sector_df = sector_history[sector_history["sector"] == sector]
            fig.add_trace(go.Scatter(
                x=sector_df["period"],
                y=sector_df["avg_daily_return_pct"],
                name=sector,
                line=dict(color=colors[i % len(colors)], width=2),
            ))

        fig.update_layout(
            title=f"Sector Returns Over Time ({history_period})",
            template="plotly_dark",
            height=450,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#2d3548", title="Date"),
            yaxis=dict(gridcolor="#2d3548", title="Average Daily Return (%)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True)

        # Sector volatility over time
        fig_vol = go.Figure()

        for i, sector in enumerate(sectors):
            sector_df = sector_history[sector_history["sector"] == sector]
            fig_vol.add_trace(go.Scatter(
                x=sector_df["period"],
                y=sector_df["avg_volatility_pct"],
                name=sector,
                line=dict(color=colors[i % len(colors)], width=2),
            ))

        fig_vol.update_layout(
            title=f"Sector Volatility Over Time ({history_period})",
            template="plotly_dark",
            height=400,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#2d3548", title="Date"),
            yaxis=dict(gridcolor="#2d3548", title="Average 30D Volatility (%)"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
        )

        st.plotly_chart(fig_vol, use_container_width=True)

    # ==========================================================================
    # Sector Performance by SELIC Regime
    # ==========================================================================
    st.markdown("---")
    st.subheader("📊 Sector Performance by Interest Rate Environment")
    st.markdown("""
    Different sectors respond differently to interest rate changes. Understanding
    these patterns can help anticipate sector rotation during monetary policy shifts.
    """)

    regime_data = get_selic_regime_performance(engine)

    if not regime_data.empty:
        # Create a grouped bar chart
        sectors = regime_data["sector"].unique().tolist()
        regimes = regime_data["selic_regime"].unique().tolist()

        fig = go.Figure()

        colors = {"Low (<7%)": "#00d4aa", "Medium (7-12%)": "#ffd700", "High (>=12%)": "#ff4b4b"}

        for regime in regimes:
            regime_df = regime_data[regime_data["selic_regime"] == regime]
            fig.add_trace(go.Bar(
                x=regime_df["sector"],
                y=regime_df["annualized_return_pct"],
                name=regime,
                marker_color=colors.get(regime, "#ffffff"),
            ))

        fig.update_layout(
            title="Annualized Returns by Sector and SELIC Regime",
            template="plotly_dark",
            height=450,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#2d3548", title="Sector"),
            yaxis=dict(gridcolor="#2d3548", title="Annualized Return (%)"),
            barmode="group",
            legend=dict(title="SELIC Regime"),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Key insights
        st.subheader("💡 Key Insights")

        col1, col2, col3 = st.columns(3)

        with col1:
            low_regime = regime_data[regime_data["selic_regime"] == "Low (<7%)"]
            if not low_regime.empty:
                best = low_regime.nlargest(1, "annualized_return_pct").iloc[0]
                worst = low_regime.nsmallest(1, "annualized_return_pct").iloc[0]
                st.markdown("**Low SELIC (<7%)**")
                st.markdown(f"Best: {best['sector']} ({best['annualized_return_pct']:.1f}%)")
                st.markdown(f"Worst: {worst['sector']} ({worst['annualized_return_pct']:.1f}%)")

        with col2:
            med_regime = regime_data[regime_data["selic_regime"] == "Medium (7-12%)"]
            if not med_regime.empty:
                best = med_regime.nlargest(1, "annualized_return_pct").iloc[0]
                worst = med_regime.nsmallest(1, "annualized_return_pct").iloc[0]
                st.markdown("**Medium SELIC (7-12%)**")
                st.markdown(f"Best: {best['sector']} ({best['annualized_return_pct']:.1f}%)")
                st.markdown(f"Worst: {worst['sector']} ({worst['annualized_return_pct']:.1f}%)")

        with col3:
            high_regime = regime_data[regime_data["selic_regime"] == "High (>=12%)"]
            if not high_regime.empty:
                best = high_regime.nlargest(1, "annualized_return_pct").iloc[0]
                worst = high_regime.nsmallest(1, "annualized_return_pct").iloc[0]
                st.markdown("**High SELIC (>=12%)**")
                st.markdown(f"Best: {best['sector']} ({best['annualized_return_pct']:.1f}%)")
                st.markdown(f"Worst: {worst['sector']} ({worst['annualized_return_pct']:.1f}%)")

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the database is running and contains data.")
