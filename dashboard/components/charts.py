"""
charts.py
=========

Reusable chart components for the dashboard.

Author: Dênio Barbosa Júnior
Created: 2025-02-07
"""

from typing import Optional, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: Optional[str] = None,
    color: Optional[str] = None,
    height: int = 400,
) -> go.Figure:
    """
    Create a styled line chart.

    Args:
        df: DataFrame with data
        x: Column name for x-axis
        y: Column name for y-axis
        title: Chart title (None or empty string means no title)
        color: Column for color grouping
        height: Chart height in pixels

    Returns:
        Plotly figure object
    """
    # Use None for empty titles to avoid "undefined" in Plotly
    chart_title = title if title else None

    fig = px.line(
        df,
        x=x,
        y=y,
        color=color,
        title=chart_title,
        template="plotly_dark",
    )

    layout_args = dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ffffff"),
        xaxis=dict(gridcolor="#2d3548"),
        yaxis=dict(gridcolor="#2d3548"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
    )

    # Only add title styling if there's actually a title
    if chart_title:
        layout_args["title"] = dict(font=dict(size=18, color="#00d4aa"))

    fig.update_layout(**layout_args)

    return fig


def create_bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: Optional[str] = None,
    color: Optional[str] = None,
    orientation: str = "v",
    height: int = 400,
) -> go.Figure:
    """
    Create a styled bar chart.

    Args:
        df: DataFrame with data
        x: Column name for x-axis
        y: Column name for y-axis
        title: Chart title (None or empty string means no title)
        color: Column for color values
        orientation: 'v' for vertical, 'h' for horizontal
        height: Chart height in pixels

    Returns:
        Plotly figure object
    """
    # Use None for empty titles to avoid "undefined" in Plotly
    chart_title = title if title else None

    fig = px.bar(
        df,
        x=x,
        y=y,
        color=color,
        title=chart_title,
        orientation=orientation,
        template="plotly_dark",
        color_continuous_scale=["#ff4b4b", "#ffffff", "#00d4aa"],
    )

    layout_args = dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ffffff"),
        xaxis=dict(gridcolor="#2d3548"),
        yaxis=dict(gridcolor="#2d3548"),
    )

    # Only add title styling if there's actually a title
    if chart_title:
        layout_args["title"] = dict(font=dict(size=18, color="#00d4aa"))

    fig.update_layout(**layout_args)

    return fig


def create_candlestick_chart(
    df: pd.DataFrame,
    date_col: str = "date",
    open_col: str = "open_price",
    high_col: str = "high_price",
    low_col: str = "low_price",
    close_col: str = "close_price",
    volume_col: Optional[str] = "volume",
    title: Optional[str] = None,
    height: int = 500,
) -> go.Figure:
    """
    Create a candlestick chart with optional volume.

    Args:
        df: DataFrame with OHLCV data
        date_col: Column name for dates
        open_col: Column name for open prices
        high_col: Column name for high prices
        low_col: Column name for low prices
        close_col: Column name for close prices
        volume_col: Column name for volume (optional)
        title: Chart title (None or empty string means no title)
        height: Chart height in pixels

    Returns:
        Plotly figure object
    """
    # Use None for empty titles to avoid "undefined" in Plotly
    chart_title = title if title else None

    if volume_col and volume_col in df.columns:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
        )

        fig.add_trace(
            go.Candlestick(
                x=df[date_col],
                open=df[open_col],
                high=df[high_col],
                low=df[low_col],
                close=df[close_col],
                name="Price",
                increasing_line_color="#00d4aa",
                decreasing_line_color="#ff4b4b",
            ),
            row=1,
            col=1,
        )

        colors = [
            "#00d4aa" if close >= open else "#ff4b4b"
            for close, open in zip(df[close_col], df[open_col])
        ]

        fig.add_trace(
            go.Bar(x=df[date_col], y=df[volume_col], name="Volume", marker_color=colors),
            row=2,
            col=1,
        )
    else:
        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=df[date_col],
                    open=df[open_col],
                    high=df[high_col],
                    low=df[low_col],
                    close=df[close_col],
                    increasing_line_color="#00d4aa",
                    decreasing_line_color="#ff4b4b",
                )
            ]
        )

    layout_args = dict(
        height=height,
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ffffff"),
        xaxis=dict(gridcolor="#2d3548"),
        yaxis=dict(gridcolor="#2d3548"),
        xaxis_rangeslider_visible=False,
    )

    # Only add title styling if there's actually a title
    if chart_title:
        layout_args["title"] = chart_title
        layout_args["title_font"] = dict(size=18, color="#00d4aa")

    fig.update_layout(**layout_args)

    return fig


def create_heatmap(
    df: pd.DataFrame,
    x: str,
    y: str,
    z: str,
    title: Optional[str] = None,
    height: int = 400,
) -> go.Figure:
    """
    Create a correlation heatmap.

    Args:
        df: DataFrame with data
        x: Column name for x-axis categories
        y: Column name for y-axis categories
        z: Column name for values
        title: Chart title (None or empty string means no title)
        height: Chart height in pixels

    Returns:
        Plotly figure object
    """
    # Use None for empty titles to avoid "undefined" in Plotly
    chart_title = title if title else None

    pivot_df = df.pivot(index=y, columns=x, values=z)

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale=[[0, "#ff4b4b"], [0.5, "#1a1f2c"], [1, "#00d4aa"]],
            zmid=0,
        )
    )

    layout_args = dict(
        height=height,
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#ffffff"),
    )

    # Only add title if there's actually a title
    if chart_title:
        layout_args["title"] = chart_title
        layout_args["title_font"] = dict(size=18, color="#00d4aa")

    fig.update_layout(**layout_args)

    return fig
