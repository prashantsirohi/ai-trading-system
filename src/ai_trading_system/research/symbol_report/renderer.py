"""Plotly HTML renderer for single-symbol diagnostic reports."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .dataset import SymbolReportData


STAGE_COLORS = {
    "S1": "rgba(135, 206, 235, 0.16)",
    "S2": "rgba(46, 204, 113, 0.18)",
    "S3": "rgba(245, 176, 65, 0.18)",
    "S4": "rgba(231, 76, 60, 0.14)",
    "UNDEFINED": "rgba(149, 165, 166, 0.10)",
}

DIAGNOSTIC_COLORS = {
    "captured": "#1f9d55",
    "not_emitted": "#e67e22",
    "rejected": "#c0392b",
    "observed": "#7f8c8d",
}


def _cols(frame: pd.DataFrame, candidates: list[str]) -> list[str]:
    return [column for column in candidates if column in frame.columns]


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def _hover_text(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    if frame.empty:
        return []
    text = []
    for _, row in frame.iterrows():
        parts = []
        for column in columns:
            if column not in row:
                continue
            value = row.get(column)
            if value is None or pd.isna(value):
                continue
            parts.append(f"{column}: {value}")
        text.append("<br>".join(parts))
    return text


def _add_stage_regions(fig: go.Figure, stages: pd.DataFrame, *, report_end: pd.Timestamp) -> None:
    if stages.empty or "stage_label" not in stages.columns:
        return
    ordered = stages.sort_values("week_end_date").reset_index(drop=True)
    for idx, row in ordered.iterrows():
        x0 = pd.Timestamp(row["week_end_date"])
        if idx + 1 < len(ordered):
            x1 = pd.Timestamp(ordered.loc[idx + 1, "week_end_date"])
        else:
            x1 = report_end
        label = str(row.get("stage_label") or "UNDEFINED")
        fig.add_vrect(
            x0=x0,
            x1=x1,
            fillcolor=STAGE_COLORS.get(label, STAGE_COLORS["UNDEFINED"]),
            line_width=0,
            layer="below",
            annotation_text=label if idx == 0 or label != str(ordered.loc[idx - 1].get("stage_label")) else None,
            annotation_position="top left",
        )


def _add_optional_line(
    fig: go.Figure,
    frame: pd.DataFrame,
    *,
    column: str,
    row: int,
    name: str | None = None,
    secondary_y: bool = False,
) -> None:
    if column not in frame.columns:
        return
    series = _numeric(frame, column)
    if series.notna().sum() == 0:
        return
    fig.add_trace(
        go.Scatter(
            x=frame["timestamp"],
            y=series,
            mode="lines",
            name=name or column,
            line={"width": 1.4},
        ),
        row=row,
        col=1,
        secondary_y=secondary_y,
    )


def build_figure(data: SymbolReportData) -> go.Figure:
    """Build the interactive diagnostic figure."""
    prices = data.price_features.copy()
    artifacts = data.artifacts.copy()
    diagnostics = data.diagnostics.copy()
    stages = data.stages.copy()

    fig = make_subplots(
        rows=6,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.035,
        row_heights=[0.36, 0.16, 0.13, 0.13, 0.11, 0.11],
        specs=[
            [{"secondary_y": False}],
            [{"secondary_y": True}],
            [{"secondary_y": False}],
            [{"secondary_y": True}],
            [{"secondary_y": False}],
            [{"secondary_y": False}],
        ],
        subplot_titles=(
            "Price, stages, patterns",
            "System rank and composite score",
            "Relative strength / trend / sector",
            "Volume, delivery, volatility",
            "RSI / ADX",
            "MACD",
        ),
    )

    fig.add_trace(
        go.Candlestick(
            x=prices["timestamp"],
            open=prices["open"],
            high=prices["high"],
            low=prices["low"],
            close=prices["close"],
            name="OHLC",
            increasing_line_color="#1f9d55",
            decreasing_line_color="#c0392b",
        ),
        row=1,
        col=1,
    )
    for column in _cols(prices, ["sma_20", "sma_50", "sma_200", "ema_50", "ema_200"]):
        _add_optional_line(fig, prices, column=column, row=1, name=column.upper())

    _add_stage_regions(fig, stages, report_end=pd.Timestamp(data.to_date))

    pattern_points = diagnostics[diagnostics.get("pattern_emitted", False).fillna(False)] if "pattern_emitted" in diagnostics else pd.DataFrame()
    if not pattern_points.empty:
        fig.add_trace(
            go.Scatter(
                x=pd.to_datetime(pattern_points.get("signal_date", pattern_points["timestamp"]), errors="coerce"),
                y=pd.to_numeric(pattern_points.get("breakout_level", pattern_points.get("close")), errors="coerce"),
                mode="markers",
                name="Pattern signal",
                marker={"symbol": "star", "size": 12, "color": "#8e44ad"},
                text=_hover_text(
                    pattern_points,
                    [
                        "run_date",
                        "pattern_family",
                        "pattern_state",
                        "signal_date",
                        "breakout_level",
                        "invalidation_price",
                        "setup_quality",
                    ],
                ),
                hovertemplate="%{text}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    if not diagnostics.empty:
        for status, group in diagnostics.groupby("diagnostic_status"):
            fig.add_trace(
                go.Scatter(
                    x=group["timestamp"],
                    y=pd.to_numeric(group.get("close"), errors="coerce"),
                    mode="markers",
                    name=status.replace("_", " ").title(),
                    marker={
                        "size": 9,
                        "color": DIAGNOSTIC_COLORS.get(status, "#7f8c8d"),
                        "line": {"width": 1, "color": "white"},
                    },
                    text=_hover_text(
                        group,
                        [
                            "run_date",
                            "diagnostic_status",
                            "rank_position",
                            "composite_score_adjusted",
                            "eligible_rank",
                            "rejection_reasons",
                            "stage_label",
                            "pattern_family",
                            "stock_category",
                            "breakout_state",
                        ],
                    ),
                    hovertemplate="%{text}<extra></extra>",
                ),
                row=1,
                col=1,
            )

    if not artifacts.empty:
        if "rank_position" in artifacts.columns:
            fig.add_trace(
                go.Scatter(
                    x=artifacts["timestamp"],
                    y=pd.to_numeric(artifacts["rank_position"], errors="coerce"),
                    mode="lines+markers",
                    name="Rank position",
                    line={"color": "#2c3e50"},
                    hovertext=_hover_text(artifacts, ["run_date", "rank_position", "rank_mode"]),
                    hovertemplate="%{hovertext}<extra></extra>",
                ),
                row=2,
                col=1,
                secondary_y=False,
            )
        if "composite_score_adjusted" in artifacts.columns:
            fig.add_trace(
                go.Scatter(
                    x=artifacts["timestamp"],
                    y=pd.to_numeric(artifacts["composite_score_adjusted"], errors="coerce"),
                    mode="lines+markers",
                    name="Composite adjusted",
                    line={"color": "#16a085"},
                ),
                row=2,
                col=1,
                secondary_y=True,
            )

        for column in _cols(
            artifacts,
            ["rel_strength_score", "trend_score_score", "sector_strength_score", "prox_high_score"],
        ):
            fig.add_trace(
                go.Scatter(
                    x=artifacts["timestamp"],
                    y=pd.to_numeric(artifacts[column], errors="coerce"),
                    mode="lines+markers",
                    name=column,
                ),
                row=3,
                col=1,
            )

        for column in _cols(
            artifacts,
            ["vol_intensity_score", "delivery_pct_score", "volume_zscore_20", "distance_from_pivot_atr"],
        ):
            fig.add_trace(
                go.Scatter(
                    x=artifacts["timestamp"],
                    y=pd.to_numeric(artifacts[column], errors="coerce"),
                    mode="lines+markers",
                    name=column,
                ),
                row=4,
                col=1,
                secondary_y=column in {"volume_zscore_20", "distance_from_pivot_atr"},
            )

    _add_optional_line(fig, prices, column="rsi_14", row=5, name="RSI 14")
    _add_optional_line(fig, prices, column="adx_14", row=5, name="ADX 14")
    _add_optional_line(fig, prices, column="macd_line", row=6, name="MACD")
    _add_optional_line(fig, prices, column="macd_signal_9", row=6, name="MACD signal")
    _add_optional_line(fig, prices, column="macd_histogram", row=6, name="MACD hist")

    fig.update_yaxes(autorange="reversed", title_text="Rank", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Score", row=2, col=1, secondary_y=True)
    fig.update_xaxes(rangeslider_visible=False)
    fig.update_layout(
        title=(
            f"{data.symbol} System Performance Diagnostic "
            f"({data.from_date.isoformat()} to {data.to_date.isoformat()})"
        ),
        template="plotly_white",
        height=1180,
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.01, "xanchor": "left", "x": 0},
        margin={"l": 70, "r": 40, "t": 110, "b": 40},
    )
    return fig


def render_symbol_report(data: SymbolReportData, output_path: Path | str) -> Path:
    """Write a self-contained interactive HTML report."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig = build_figure(data)
    html = fig.to_html(include_plotlyjs=True, full_html=True)
    summary = _summary_html(data)
    html = html.replace("<body>", f"<body>{summary}", 1)
    output.write_text(html, encoding="utf-8")
    return output


def _summary_html(data: SymbolReportData) -> str:
    diagnostics = data.diagnostics
    counts = diagnostics["diagnostic_status"].value_counts().to_dict() if not diagnostics.empty else {}
    artifact_count = len(data.artifacts.index)
    stage_count = len(data.stages.index)
    return f"""
<section style="font-family:Inter,Arial,sans-serif;max-width:1180px;margin:24px auto 0;padding:0 16px;">
  <h1 style="margin:0 0 8px;font-size:28px;">{data.symbol} Diagnostic Report</h1>
  <p style="margin:0 0 12px;color:#555;">
    Read-only view of emitted pipeline artifacts, OHLCV, feature history, weekly stages, and pattern evidence.
  </p>
  <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:10px;">
    <span><b>Artifact dates:</b> {artifact_count}</span>
    <span><b>Stage snapshots:</b> {stage_count}</span>
    <span><b>Captured:</b> {counts.get("captured", 0)}</span>
    <span><b>Rejected:</b> {counts.get("rejected", 0)}</span>
    <span><b>Not emitted:</b> {counts.get("not_emitted", 0)}</span>
  </div>
</section>
"""
