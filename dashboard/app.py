"""
app.py
======

Main entry point for the Brazilian Market Analysis Dashboard.

This Streamlit application provides interactive visualizations
for analyzing Brazilian stock market data and macroeconomic indicators.

Features:
    - Market Overview with key metrics and trends
    - Sector Analysis and performance comparison
    - Macro Correlation analysis (SELIC vs stocks)
    - Stock Screener with filtering capabilities

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="Brazilian Market Analytics",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for professional styling
st.markdown(
    """
<style>
    /* Main container */
    .main {
        background-color: #0e1117;
    }

    /* Sidebar styling */
    .css-1d391kg {
        background-color: #1a1f2c;
    }

    /* Metric cards */
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
    }

    /* Headers */
    h1 {
        color: #00d4aa;
        font-weight: 700;
        margin-bottom: 1rem;
    }

    h2 {
        color: #ffffff;
        font-weight: 600;
        border-bottom: 2px solid #00d4aa;
        padding-bottom: 0.5rem;
    }

    /* Cards */
    .metric-card {
        background: linear-gradient(135deg, #1a1f2c 0%, #2d3548 100%);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid #3d4556;
    }

    /* Positive/Negative indicators */
    .positive {
        color: #00d4aa;
    }

    .negative {
        color: #ff4b4b;
    }

    /* Table styling */
    .dataframe {
        font-size: 0.9rem;
    }

    /* Footer */
    .footer {
        position: fixed;
        bottom: 0;
        width: 100%;
        text-align: center;
        padding: 0.5rem;
        font-size: 0.8rem;
        color: #6b7280;
    }
</style>
""",
    unsafe_allow_html=True,
)


def main():
    """Main application entry point."""
    # Sidebar
    with st.sidebar:
        st.image(
            "https://via.placeholder.com/200x80/1a1f2c/00d4aa?text=BR+Market",
            use_column_width=True,
        )

        st.markdown("---")

        st.markdown("### Navigation")
        st.markdown(
            """
        Use the sidebar to navigate between pages:
        - **Market Overview**: Key metrics and trends
        - **Sector Analysis**: Compare sector performance
        - **Macro Correlation**: SELIC vs stock analysis
        - **Stock Screener**: Filter and find stocks
        """
        )

        st.markdown("---")

        st.markdown("### Data Status")
        st.info("Data updated daily at 6 AM BRT")

        st.markdown("---")

        st.markdown(
            """
        **Built with:**
        - Python & Streamlit
        - PostgreSQL
        - Apache Airflow
        - dbt

        [View on GitHub](https://github.com/your-username/brazilian-market-etl)
        """
        )

    # Main content
    st.title("🇧🇷 Brazilian Financial Markets Dashboard")

    st.markdown(
        """
    Welcome to the Brazilian Financial Markets Analytics Dashboard.
    This platform provides comprehensive analysis of Brazilian equities
    and their relationship with key macroeconomic indicators.

    **Getting Started:**

    Navigate using the sidebar to explore different analysis views.
    Each page provides interactive charts and tables for in-depth analysis.
    """
    )

    # Quick metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="IBOV Index",
            value="128,450",
            delta="+1.2%",
            delta_color="normal",
        )

    with col2:
        st.metric(
            label="SELIC Rate",
            value="10.50%",
            delta="-0.25%",
            delta_color="inverse",
        )

    with col3:
        st.metric(
            label="USD/BRL",
            value="R$ 4.95",
            delta="-0.5%",
            delta_color="inverse",
        )

    with col4:
        st.metric(
            label="IPCA (12m)",
            value="4.2%",
            delta="-0.3%",
            delta_color="inverse",
        )

    st.markdown("---")

    # Feature cards
    st.subheader("Analysis Modules")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(
            """
        <div class="metric-card">
            <h3>📊 Market Overview</h3>
            <p>Track the overall market performance with IBOV trends,
            top gainers and losers, and trading volume analysis.</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown(
            """
        <div class="metric-card">
            <h3>🔄 Macro Correlation</h3>
            <p>Analyze how macroeconomic factors like SELIC rate,
            inflation, and currency affect stock performance.</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            """
        <div class="metric-card">
            <h3>🏭 Sector Analysis</h3>
            <p>Compare sector performance, identify sector rotation
            patterns, and find sector-specific opportunities.</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown(
            """
        <div class="metric-card">
            <h3>🔍 Stock Screener</h3>
            <p>Filter stocks by return, volatility, sector, and other
            criteria to find investment opportunities.</p>
        </div>
        """,
            unsafe_allow_html=True,
        )

    # Footer
    st.markdown("---")
    st.markdown(
        """
    <div style="text-align: center; color: #6b7280; font-size: 0.9rem;">
        Brazilian Financial Markets ETL Pipeline | Portfolio Project by Dênio Barbosa Júnior<br>
        Data sources: Yahoo Finance, Brazilian Central Bank (BCB)
    </div>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
