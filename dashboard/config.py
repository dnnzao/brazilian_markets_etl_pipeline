"""
config.py
=========

Dashboard configuration and database connection settings.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

import os
from dataclasses import dataclass

import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass
class DashboardConfig:
    """Dashboard configuration settings."""

    page_title: str = "Brazilian Market Analytics"
    page_icon: str = "📈"
    layout: str = "wide"

    # Color scheme
    primary_color: str = "#00d4aa"
    background_color: str = "#0e1117"
    secondary_bg_color: str = "#1a1f2c"
    text_color: str = "#ffffff"
    positive_color: str = "#00d4aa"
    negative_color: str = "#ff4b4b"

    # Chart defaults
    chart_height: int = 400
    chart_template: str = "plotly_dark"


@st.cache_resource
def get_database_connection() -> Engine:
    """
    Create and cache database connection.

    Returns:
        SQLAlchemy engine with connection pool

    Note:
        Uses st.cache_resource to maintain connection across reruns
    """
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "dataeng")
    password = os.getenv("POSTGRES_PASSWORD", "dataeng123")
    database = os.getenv("POSTGRES_DB", "brazilian_market")

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    return create_engine(connection_string, pool_pre_ping=True)


def get_config() -> DashboardConfig:
    """Get dashboard configuration."""
    return DashboardConfig()
