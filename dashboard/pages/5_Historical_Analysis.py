"""
5_Historical_Analysis.py
========================

Historical Analysis page providing deep insights into 10 years of market data.

This page enables analysis of:
- Multi-year return comparisons (1Y, 3Y, 5Y, 10Y)
- Cumulative performance over time
- Monthly returns heatmaps
- Drawdown analysis
- Rolling correlations with macro indicators
- Performance during different SELIC regimes
- Crisis period performance
- Year-over-year comparisons

Author: Dênio Barbosa Júnior
Created: 2025-02-09
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
from dashboard.components.charts import create_line_chart, create_bar_chart, create_heatmap
from dashboard.components.queries import (
    get_date_range_info,
    get_multi_year_returns,
    get_cumulative_returns,
    get_drawdown_analysis,
    get_max_drawdowns,
    get_rolling_correlations,
    get_selic_regime_performance,
    get_yearly_comparison,
    get_crisis_periods_performance,
    get_monthly_returns_heatmap,
    get_comparative_performance,
    get_stock_list,
    get_market_trends,
    calculate_period_dates,
)

# Page config
st.set_page_config(
    page_title="Historical Analysis",
    page_icon="📜",
    layout="wide",
    initial_sidebar_state="expanded",
)
config = get_config()

# Custom styling
st.markdown("""
<style>
    .metric-highlight {
        background: linear-gradient(135deg, #1a1f2c 0%, #2d3548 100%);
        border-radius: 10px;
        padding: 15px;
        margin: 5px 0;
    }
    .positive-return { color: #00d4aa; font-weight: bold; }
    .negative-return { color: #ff4b4b; font-weight: bold; }
    .section-divider {
        border-top: 2px solid #00d4aa;
        margin: 30px 0;
    }
</style>
""", unsafe_allow_html=True)

st.title("📜 Historical Analysis")
st.markdown("""
Explore 10 years of Brazilian market history. Analyze long-term trends,
compare multi-year performance, and understand how stocks behaved during
different market conditions and economic regimes.
""")

# Get database connection
try:
    engine = get_database_connection()

    # ==========================================================================
    # Data Overview Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("📊 Data Overview")

    date_info = get_date_range_info(engine)

    if date_info:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Data Start",
                str(date_info.get("min_date", "N/A")),
            )

        with col2:
            st.metric(
                "Data End",
                str(date_info.get("max_date", "N/A")),
            )

        with col3:
            st.metric(
                "Years of Data",
                f"{date_info.get('years_of_data', 0):.1f}",
            )

        with col4:
            st.metric(
                "Total Trading Days",
                f"{date_info.get('total_days', 0):,}",
            )

    # ==========================================================================
    # Multi-Year Returns Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("📈 Multi-Year Returns")
    st.markdown("""
    Compare stock performance across different time horizons. Long-term returns
    reveal which companies have created sustained value for investors.
    """)

    multi_year = get_multi_year_returns(engine)

    if not multi_year.empty:
        # Create styled dataframe
        def style_returns(val):
            if pd.isna(val):
                return ""
            color = "#00d4aa" if val >= 0 else "#ff4b4b"
            return f"color: {color}; font-weight: bold"

        display_df = multi_year.copy()
        display_df.columns = [
            "Ticker", "Company", "Sector", "Current Price",
            "1Y Return (%)", "3Y Return (%)", "5Y Return (%)", "10Y Return (%)"
        ]

        # Sort by 5Y returns for visibility
        display_df = display_df.sort_values("5Y Return (%)", ascending=False, na_position="last")

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
                style_returns,
                subset=["1Y Return (%)", "3Y Return (%)", "5Y Return (%)", "10Y Return (%)"]
            ).format({
                "Current Price": safe_price_format,
                "1Y Return (%)": safe_pct_format,
                "3Y Return (%)": safe_pct_format,
                "5Y Return (%)": safe_pct_format,
                "10Y Return (%)": safe_pct_format,
            }),
            use_container_width=True,
            height=400,
        )

        # Bar chart comparison
        col1, col2 = st.columns(2)

        # Ensure numeric type for return_5y
        chart_df = multi_year.copy()
        chart_df["return_5y"] = pd.to_numeric(chart_df["return_5y"], errors='coerce')
        chart_df = chart_df.dropna(subset=["return_5y"])

        if not chart_df.empty:
            with col1:
                fig = create_bar_chart(
                    chart_df.nlargest(10, "return_5y"),
                    x="return_5y",
                    y="ticker",
                    color="return_5y",
                    orientation="h",
                    title="Top 10 Stocks by 5-Year Return",
                    height=350,
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = create_bar_chart(
                    chart_df.nsmallest(10, "return_5y"),
                    x="return_5y",
                    y="ticker",
                    color="return_5y",
                    orientation="h",
                    title="Bottom 10 Stocks by 5-Year Return",
                    height=350,
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No 5-year return data available yet. Stock data may not go back 5 years.")

    # ==========================================================================
    # Comparative Performance Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("📊 Comparative Performance")
    st.markdown("""
    Compare multiple stocks side-by-side with normalized performance (base 100).
    Select stocks and time period to visualize relative performance.
    """)

    col1, col2 = st.columns([2, 1])

    with col1:
        stock_list = get_stock_list(engine)
        available_tickers = stock_list["ticker"].tolist() if not stock_list.empty else []

        default_tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA"]
        default_selection = [t for t in default_tickers if t in available_tickers][:4]

        selected_tickers = st.multiselect(
            "Select Stocks to Compare",
            available_tickers,
            default=default_selection,
            max_selections=6,
        )

    with col2:
        period = st.selectbox(
            "Time Period",
            ["1Y", "3Y", "5Y", "10Y", "MAX"],
            index=2,
            key="comparative_period",
        )

    if selected_tickers:
        start_date, end_date = calculate_period_dates(period)
        comparative = get_comparative_performance(engine, selected_tickers, start_date, end_date)

        if not comparative.empty:
            fig = go.Figure()

            colors = px.colors.qualitative.Set2[:len(selected_tickers)]

            for i, ticker in enumerate(selected_tickers):
                ticker_data = comparative[comparative["ticker"] == ticker]
                fig.add_trace(go.Scatter(
                    x=ticker_data["date"],
                    y=ticker_data["normalized_price"],
                    name=ticker,
                    line=dict(color=colors[i], width=2),
                ))

            fig.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.5)

            fig.update_layout(
                title=f"Normalized Performance (Base 100) - {period}",
                template="plotly_dark",
                height=450,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(gridcolor="#2d3548"),
                yaxis=dict(gridcolor="#2d3548", title="Normalized Price"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified",
            )

            st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # Monthly Returns Heatmap Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("🗓️ Monthly Returns Heatmap")
    st.markdown("""
    Visualize monthly returns by year. Identify seasonal patterns and
    understand which months tend to perform better or worse historically.
    """)

    heatmap_stock = st.selectbox(
        "Select Stock for Heatmap",
        available_tickers if available_tickers else ["PETR4.SA"],
        key="heatmap_stock",
    )

    monthly_returns = get_monthly_returns_heatmap(engine, heatmap_stock)

    if not monthly_returns.empty and isinstance(monthly_returns, pd.DataFrame):
        # Create heatmap using plotly
        fig = go.Figure(data=go.Heatmap(
            z=monthly_returns.values,
            x=monthly_returns.columns.tolist(),
            y=monthly_returns.index.tolist(),
            colorscale=[[0, "#ff4b4b"], [0.5, "#1a1f2c"], [1, "#00d4aa"]],
            zmid=0,
            text=monthly_returns.values,
            texttemplate="%{text:.1f}%",
            textfont={"size": 10},
            hovertemplate="Year: %{y}<br>Month: %{x}<br>Return: %{z:.2f}%<extra></extra>",
        ))

        fig.update_layout(
            title=f"{heatmap_stock} Monthly Returns (%)",
            template="plotly_dark",
            height=500,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="Month"),
            yaxis=dict(title="Year", autorange="reversed"),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Monthly statistics
        st.subheader("Monthly Statistics")
        monthly_avg = monthly_returns.mean()
        monthly_positive = (monthly_returns > 0).sum() / monthly_returns.count() * 100

        stats_df = pd.DataFrame({
            "Month": monthly_avg.index,
            "Average Return (%)": monthly_avg.values,
            "Win Rate (%)": monthly_positive.values,
        })

        col1, col2 = st.columns(2)

        with col1:
            fig = create_bar_chart(
                stats_df,
                x="Month",
                y="Average Return (%)",
                color="Average Return (%)",
                title="Average Return by Month",
                height=300,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = create_bar_chart(
                stats_df,
                x="Month",
                y="Win Rate (%)",
                title="Win Rate by Month (%)",
                height=300,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # Drawdown Analysis Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("📉 Drawdown Analysis")
    st.markdown("""
    Drawdown measures the decline from a historical peak to a trough.
    Understanding drawdowns helps assess risk and recovery patterns.
    """)

    col1, col2 = st.columns([1, 1])

    with col1:
        drawdown_stock = st.selectbox(
            "Select Stock",
            available_tickers if available_tickers else ["PETR4.SA"],
            key="drawdown_stock",
        )

    with col2:
        drawdown_period = st.selectbox(
            "Time Period",
            ["3Y", "5Y", "10Y", "MAX"],
            index=2,
            key="drawdown_period",
        )

    start_date, end_date = calculate_period_dates(drawdown_period)
    drawdown_data = get_drawdown_analysis(engine, drawdown_stock, start_date, end_date)

    if not drawdown_data.empty:
        # Create drawdown chart
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.08,
            row_heights=[0.6, 0.4],
            subplot_titles=(f"{drawdown_stock} Price", "Drawdown (%)")
        )

        # Price chart
        fig.add_trace(
            go.Scatter(
                x=drawdown_data["date"],
                y=drawdown_data["close_price"],
                name="Price",
                line=dict(color="#00d4aa", width=1.5),
            ),
            row=1, col=1
        )

        # Peak line
        fig.add_trace(
            go.Scatter(
                x=drawdown_data["date"],
                y=drawdown_data["peak_price"],
                name="Peak",
                line=dict(color="#ffffff", width=1, dash="dot"),
                opacity=0.5,
            ),
            row=1, col=1
        )

        # Drawdown chart
        fig.add_trace(
            go.Scatter(
                x=drawdown_data["date"],
                y=drawdown_data["drawdown_pct"],
                name="Drawdown",
                fill="tozeroy",
                line=dict(color="#ff4b4b", width=1.5),
                fillcolor="rgba(255, 75, 75, 0.3)",
            ),
            row=2, col=1
        )

        fig.update_layout(
            template="plotly_dark",
            height=600,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )

        fig.update_xaxes(gridcolor="#2d3548")
        fig.update_yaxes(gridcolor="#2d3548")

        st.plotly_chart(fig, use_container_width=True)

        # Drawdown statistics
        max_drawdown = drawdown_data["drawdown_pct"].min()
        max_dd_date = drawdown_data.loc[drawdown_data["drawdown_pct"].idxmin(), "date"]
        avg_drawdown = drawdown_data["drawdown_pct"].mean()

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Maximum Drawdown", f"{max_drawdown:.2f}%")

        with col2:
            st.metric("Max Drawdown Date", str(max_dd_date))

        with col3:
            st.metric("Average Drawdown", f"{avg_drawdown:.2f}%")

    # Maximum Drawdowns Ranking
    st.subheader("Maximum Drawdowns Ranking (All Stocks)")
    max_dd = get_max_drawdowns(engine)

    if not max_dd.empty:
        fig = create_bar_chart(
            max_dd.head(15),
            x="max_drawdown_pct",
            y="ticker",
            color="max_drawdown_pct",
            orientation="h",
            title="Stocks by Maximum Drawdown (All Time)",
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # SELIC Regime Performance Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("📊 SELIC Regime Performance")
    st.markdown("""
    Analyze how different sectors performed during various interest rate environments.
    Understanding this relationship helps anticipate sector rotations during monetary policy changes.
    """)

    regime_data = get_selic_regime_performance(engine)

    if not regime_data.empty:
        # Pivot for heatmap
        regime_pivot = regime_data.pivot(
            index="sector",
            columns="selic_regime",
            values="annualized_return_pct"
        )

        fig = go.Figure(data=go.Heatmap(
            z=regime_pivot.values,
            x=regime_pivot.columns.tolist(),
            y=regime_pivot.index.tolist(),
            colorscale=[[0, "#ff4b4b"], [0.5, "#1a1f2c"], [1, "#00d4aa"]],
            zmid=0,
            text=np.round(regime_pivot.values, 1),
            texttemplate="%{text}%",
            textfont={"size": 11},
            hovertemplate="Sector: %{y}<br>SELIC Regime: %{x}<br>Annualized Return: %{z:.1f}%<extra></extra>",
        ))

        fig.update_layout(
            title="Annualized Returns by Sector and SELIC Regime (%)",
            template="plotly_dark",
            height=400,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(title="SELIC Regime"),
            yaxis=dict(title="Sector"),
        )

        st.plotly_chart(fig, use_container_width=True)

        # Key insights
        st.subheader("Key Insights")

        col1, col2, col3 = st.columns(3)

        with col1:
            low_regime = regime_data[regime_data["selic_regime"] == "Low (<7%)"]
            if not low_regime.empty:
                best_low = low_regime.nlargest(1, "annualized_return_pct").iloc[0]
                st.markdown(f"**Best in Low SELIC:**")
                st.markdown(f"{best_low['sector']} ({best_low['annualized_return_pct']:.1f}%)")

        with col2:
            med_regime = regime_data[regime_data["selic_regime"] == "Medium (7-12%)"]
            if not med_regime.empty:
                best_med = med_regime.nlargest(1, "annualized_return_pct").iloc[0]
                st.markdown(f"**Best in Medium SELIC:**")
                st.markdown(f"{best_med['sector']} ({best_med['annualized_return_pct']:.1f}%)")

        with col3:
            high_regime = regime_data[regime_data["selic_regime"] == "High (>=12%)"]
            if not high_regime.empty:
                best_high = high_regime.nlargest(1, "annualized_return_pct").iloc[0]
                st.markdown(f"**Best in High SELIC:**")
                st.markdown(f"{best_high['sector']} ({best_high['annualized_return_pct']:.1f}%)")

    # ==========================================================================
    # Crisis Period Analysis Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("⚠️ Crisis Period Analysis")
    st.markdown("""
    Examine how stocks performed during major market events. Understanding crisis
    behavior helps build resilient portfolios and identify defensive stocks.
    """)

    crisis_data = get_crisis_periods_performance(engine)

    if not crisis_data.empty:
        # Crisis selector
        crises = crisis_data["crisis"].unique().tolist()
        selected_crisis = st.selectbox(
            "Select Crisis Period",
            crises,
            index=0,
        )

        crisis_filtered = crisis_data[crisis_data["crisis"] == selected_crisis]

        if not crisis_filtered.empty:
            col1, col2 = st.columns([1, 2])

            with col1:
                # Crisis info
                first_row = crisis_filtered.iloc[0]
                st.markdown(f"**Period:** {first_row['start_date']} to {first_row['end_date']}")

                avg_return = crisis_filtered["period_return_pct"].mean()
                avg_drawdown = crisis_filtered["max_drawdown_pct"].mean()

                st.metric("Average Return", f"{avg_return:.1f}%")
                st.metric("Average Max Drawdown", f"{avg_drawdown:.1f}%")

            with col2:
                # Best and worst performers
                best = crisis_filtered.nlargest(5, "period_return_pct")[["ticker", "sector", "period_return_pct"]]
                worst = crisis_filtered.nsmallest(5, "period_return_pct")[["ticker", "sector", "period_return_pct"]]

                col_a, col_b = st.columns(2)

                # Safe formatter for crisis performance
                def safe_crisis_format(val):
                    if pd.isna(val):
                        return "N/A"
                    return f"{val:.1f}%"

                with col_a:
                    st.markdown("**Best Performers**")
                    st.dataframe(
                        best.style.format({"period_return_pct": safe_crisis_format}),
                        use_container_width=True,
                        hide_index=True,
                    )

                with col_b:
                    st.markdown("**Worst Performers**")
                    st.dataframe(
                        worst.style.format({"period_return_pct": safe_crisis_format}),
                        use_container_width=True,
                        hide_index=True,
                    )

        # All crises comparison
        st.subheader("Crisis Comparison by Sector")

        sector_crisis = crisis_data.groupby(["crisis", "sector"])["period_return_pct"].mean().reset_index()
        sector_pivot = sector_crisis.pivot(index="sector", columns="crisis", values="period_return_pct")

        fig = go.Figure(data=go.Heatmap(
            z=sector_pivot.values,
            x=sector_pivot.columns.tolist(),
            y=sector_pivot.index.tolist(),
            colorscale=[[0, "#ff4b4b"], [0.5, "#1a1f2c"], [1, "#00d4aa"]],
            zmid=0,
            text=np.round(sector_pivot.values, 1),
            texttemplate="%{text}%",
            textfont={"size": 10},
        ))

        fig.update_layout(
            title="Average Sector Return During Crisis Periods (%)",
            template="plotly_dark",
            height=400,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )

        st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # Rolling Correlations Section
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("🔄 Rolling Correlations Over Time")
    st.markdown("""
    See how the correlation between stock returns and macro indicators changes over time.
    This reveals regime shifts and evolving market dynamics.
    """)

    col1, col2 = st.columns([1, 1])

    with col1:
        corr_stock = st.selectbox(
            "Select Stock",
            available_tickers if available_tickers else ["PETR4.SA"],
            key="corr_stock",
        )

    with col2:
        corr_window = st.slider(
            "Rolling Window (days)",
            min_value=30,
            max_value=180,
            value=60,
            step=10,
        )

    start_date, _ = calculate_period_dates("5Y")
    rolling_corr = get_rolling_correlations(
        engine, corr_stock, window_days=corr_window, start_date=start_date
    )

    if not rolling_corr.empty:
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=rolling_corr["date"],
            y=rolling_corr["corr_selic"],
            name="Correlation with SELIC",
            line=dict(color="#00d4aa", width=2),
        ))

        fig.add_trace(go.Scatter(
            x=rolling_corr["date"],
            y=rolling_corr["corr_usd"],
            name="Correlation with USD/BRL",
            line=dict(color="#ff4b4b", width=2),
        ))

        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

        fig.update_layout(
            title=f"{corr_stock} - {corr_window}-Day Rolling Correlations",
            template="plotly_dark",
            height=400,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#2d3548"),
            yaxis=dict(gridcolor="#2d3548", title="Correlation", range=[-1, 1]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True)

    # ==========================================================================
    # Market Trends Over Time
    # ==========================================================================
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.header("📈 Long-Term Market Trends")
    st.markdown("""
    Visualize how the overall market and key macro indicators have evolved
    over the full historical period.
    """)

    trends_period = st.selectbox(
        "Time Period",
        ["3Y", "5Y", "10Y", "MAX"],
        index=2,
        key="trends_period",
    )

    start_date, end_date = calculate_period_dates(trends_period)
    market_trends = get_market_trends(engine, start_date, end_date)

    if not market_trends.empty:
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.4, 0.3, 0.3],
            subplot_titles=("Market Volatility", "SELIC Rate", "USD/BRL Exchange Rate")
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

        # SELIC
        fig.add_trace(
            go.Scatter(
                x=market_trends["date"],
                y=market_trends["selic_rate"],
                name="SELIC Rate",
                line=dict(color="#ff4b4b", width=2),
            ),
            row=2, col=1
        )

        # USD/BRL
        fig.add_trace(
            go.Scatter(
                x=market_trends["date"],
                y=market_trends["usd_brl"],
                name="USD/BRL",
                line=dict(color="#ffd700", width=2),
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

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    st.info("Make sure the database is running and contains data.")
    st.exception(e)
