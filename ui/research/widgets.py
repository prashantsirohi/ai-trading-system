"""Reusable Streamlit rendering components for research dashboard visuals."""

from __future__ import annotations

from typing import Dict
import html
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from ui.research.dashboard_helpers import (
    build_breakout_evidence_frame,
    build_factor_attribution_frame,
    build_universe_factor_contributions,
    classify_rank_trend,
    prepare_sector_rotation_frame,
)


def _short_run_id(run_id: str | None) -> str:
    if not run_id:
        return "—"
    parts = str(run_id).split("-")
    return "-".join(parts[-2:]) if len(parts) >= 2 else str(run_id)


def render_ops_health_ribbon(
    snapshot: dict[str, object],
    *,
    dashboard_health: dict[str, object] | None = None,
    data_trust_snapshot: dict[str, object] | None = None,
) -> None:
    """Render top-of-page operational ribbon with stage freshness and DQ status."""
    if not snapshot or not snapshot.get("available"):
        st.info("Operational metadata is unavailable. Control-plane database not found.")
        return

    stages = snapshot.get("stages", {})
    dq_summary = snapshot.get("dq_summary", {})
    st.markdown(
        """
        <style>
        .ops-ribbon-scroll {
            overflow-x: auto;
            overflow-y: hidden;
            padding-bottom: 0.12rem;
            margin-bottom: 0.2rem;
        }
        .ops-ribbon-grid {
            display: flex;
            flex-wrap: nowrap;
            gap: 0.42rem;
            min-width: max-content;
            margin: 0.03rem 0 0.04rem 0;
        }
        .ops-ribbon-card {
            flex: 0 0 12.4rem;
            border: 1px solid rgba(148, 163, 184, 0.35);
            border-radius: 0.62rem;
            padding: 0.38rem 0.52rem;
            background: linear-gradient(180deg, rgba(15, 23, 42, 0.22), rgba(15, 23, 42, 0.08));
        }
        .ops-ribbon-title {
            font-size: 0.66rem;
            letter-spacing: 0.06em;
            color: #94a3b8;
            margin-bottom: 0.16rem;
            text-transform: uppercase;
            font-weight: 700;
        }
        .ops-ribbon-main {
            font-size: 0.86rem;
            font-weight: 600;
            color: #e2e8f0;
            margin-bottom: 0.08rem;
            line-height: 1.3;
            white-space: normal;
            overflow-wrap: anywhere;
        }
        .ops-ribbon-sub {
            font-size: 0.68rem;
            color: #cbd5e1;
            line-height: 1.35;
            white-space: normal;
            overflow-wrap: anywhere;
        }
        .ops-chip {
            display: inline-block;
            font-size: 0.6rem;
            font-weight: 700;
            letter-spacing: 0.03em;
            padding: 0.08rem 0.34rem;
            border-radius: 999px;
            margin-right: 0.22rem;
            color: white;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Ops Health")

    def _chip_color(stale: bool) -> str:
        return "#dc2626" if stale else "#16a34a"

    card_html: list[str] = []
    for stage_name in ("ingest", "features", "rank", "publish"):
        stage = stages.get(stage_name, {}) if isinstance(stages, dict) else {}
        age_hours = stage.get("age_hours")
        age_text = f"{age_hours:.1f}h ago" if isinstance(age_hours, (float, int)) else "n/a"
        stale = bool(stage.get("stale"))
        status_text = "STALE" if stale else "FRESH"
        card_html.append(
            "<div class='ops-ribbon-card'>"
            f"<div class='ops-ribbon-title'>{stage_name.title()}</div>"
            "<div class='ops-ribbon-main'>"
            f"<span class='ops-chip' style='background:{_chip_color(stale)}'>{status_text}</span>"
            f"{_short_run_id(stage.get('run_id'))}"
            "</div>"
            f"<div class='ops-ribbon-sub'>{age_text}</div>"
            "</div>"
        )

    failed_total = int(dq_summary.get("total_failed", 0)) if isinstance(dq_summary, dict) else 0
    severity_map = dq_summary.get("failed_by_severity", {}) if isinstance(dq_summary, dict) else {}
    sev_text = "none" if not severity_map else ", ".join(f"{k}:{v}" for k, v in sorted(severity_map.items()))
    dq_color = "#dc2626" if failed_total > 0 else "#16a34a"
    dq_label = "FAILED" if failed_total > 0 else "PASSED"
    card_html.append(
        "<div class='ops-ribbon-card'>"
        "<div class='ops-ribbon-title'>DQ</div>"
        "<div class='ops-ribbon-main'>"
        f"<span class='ops-chip' style='background:{dq_color}'>{dq_label}</span>{failed_total}"
        "</div>"
        f"<div class='ops-ribbon-sub'>{sev_text}</div>"
        "</div>"
    )

    health_summary = (dashboard_health or {}).get("summary", {}) if isinstance(dashboard_health, dict) else {}
    if health_summary:
        health_status = str((dashboard_health or {}).get("status", "unknown")).upper()
        card_html.append(
            "<div class='ops-ribbon-card'>"
            "<div class='ops-ribbon-title'>Pipeline</div>"
            f"<div class='ops-ribbon-main'>{html.escape(health_status)}</div>"
            "<div class='ops-ribbon-sub'>"
            f"OHLCV {html.escape(str(health_summary.get('latest_ohlcv_date', '—')))}"
            " · "
            f"Delivery {html.escape(str(health_summary.get('latest_delivery_date', '—')))}"
            " · "
            f"Payload {html.escape(str(health_summary.get('payload_age_minutes', '—')))}m"
            "</div>"
            "</div>"
        )

    if isinstance(data_trust_snapshot, dict) and data_trust_snapshot:
        trust_status = str(data_trust_snapshot.get("status", "unknown")).upper()
        validated = str(data_trust_snapshot.get("latest_validated_date", "—"))
        fallback_ratio = float(data_trust_snapshot.get("fallback_ratio_latest", 0.0) or 0.0) * 100.0
        quarantined = int(data_trust_snapshot.get("active_quarantined_symbols", 0) or 0)
        card_html.append(
            "<div class='ops-ribbon-card'>"
            "<div class='ops-ribbon-title'>Trust</div>"
            f"<div class='ops-ribbon-main'>{html.escape(trust_status)}</div>"
            "<div class='ops-ribbon-sub'>"
            f"Validated {html.escape(validated)}"
            " · "
            f"Fallback {fallback_ratio:.1f}%"
            " · "
            f"Q {quarantined}"
            "</div>"
            "</div>"
        )
    st.markdown(
        f"<div class='ops-ribbon-scroll'><div class='ops-ribbon-grid'>{''.join(card_html)}</div></div>",
        unsafe_allow_html=True,
    )

    stale_stages = snapshot.get("stale_stages", [])
    if stale_stages:
        stale_list = ", ".join(str(stage).title() for stage in stale_stages)
        st.caption(f"Stale: {stale_list}")


def render_factor_attribution_widget(
    rows: pd.DataFrame | pd.Series,
    weights: Dict[str, float],
    title: str,
    max_symbols: int = 12,
    show_table: bool = False,
    chart_key: str | None = None,
) -> None:
    """Render compact horizontal factor-attribution bars for one symbol or a small universe."""
    st.markdown(f"**{title}**")

    if isinstance(rows, pd.Series):
        factor_df = build_factor_attribution_frame(rows, weights)
        if factor_df.empty:
            st.info("No factor attribution data available.")
            return

        fig = px.bar(
            factor_df,
            x="contribution_points",
            y="factor",
            orientation="h",
            color="factor",
            text="contribution_points",
            height=300,
        )
        fig.update_layout(
            showlegend=False,
            margin=dict(l=20, r=20, t=10, b=20),
            xaxis_title="Weighted Contribution",
            yaxis_title="",
        )
        plot_kwargs = {"use_container_width": True}
        if chart_key:
            plot_kwargs["key"] = chart_key
        st.plotly_chart(fig, **plot_kwargs)
        if show_table:
            display_df = factor_df.rename(
                columns={
                    "factor": "Factor",
                    "raw_metric": "Raw Metric",
                    "normalized_score": "Normalized Score",
                    "weight_pct": "Weight %",
                    "contribution_points": "Contribution",
                    "contribution_pct": "Contribution %",
                }
            )
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        return

    if rows is None or rows.empty:
        st.info("No ranked universe rows available for attribution.")
        return

    contributions = build_universe_factor_contributions(rows, weights, max_symbols=max_symbols)
    if contributions.empty:
        st.info("No factor contribution rows available.")
        return

    fig = px.bar(
        contributions,
        x="contribution_points",
        y="symbol_id",
        color="factor",
        orientation="h",
        barmode="stack",
        height=max(360, min(680, 26 * contributions["symbol_id"].nunique())),
    )
    fig.update_layout(
        margin=dict(l=20, r=20, t=10, b=20),
        xaxis_title="Composite Score Contribution",
        yaxis_title="Symbol",
        legend_title="Factor",
    )
    plot_kwargs = {"use_container_width": True}
    if chart_key:
        plot_kwargs["key"] = chart_key
    st.plotly_chart(fig, **plot_kwargs)


def render_symbol_rank_history(
    history_df: pd.DataFrame,
    symbol: str,
    chart_key: str | None = None,
) -> None:
    """Render rank-history sparkline for a single symbol."""
    if history_df is None or history_df.empty:
        st.info("No rank history available for this symbol yet.")
        return

    symbol_df = history_df[history_df["symbol_id"] == symbol].copy()
    if symbol_df.empty:
        st.info("No rank history available for this symbol yet.")
        return

    symbol_df = symbol_df.sort_values(["run_order", "run_id"])
    symbol_df = symbol_df.tail(18)
    trend_label = classify_rank_trend(symbol_df["rank_position"].tolist())

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=symbol_df["run_id"],
            y=symbol_df["rank_position"],
            mode="lines+markers",
            line=dict(width=2, color="#0284c7"),
            marker=dict(size=6, color="#0ea5e9"),
            hovertemplate="Run: %{x}<br>Rank: %{y}<extra></extra>",
            name=symbol,
        )
    )
    fig.update_layout(
        height=220,
        margin=dict(l=20, r=20, t=10, b=20),
        xaxis_title="Run",
        yaxis_title="Rank",
        showlegend=False,
    )
    fig.update_yaxes(autorange="reversed")
    plot_kwargs = {"use_container_width": True}
    if chart_key:
        plot_kwargs["key"] = chart_key
    st.plotly_chart(fig, **plot_kwargs)
    st.caption(f"Trend: {trend_label} (lower rank number is better)")


def render_sector_rotation_heatmap(
    sector_df: pd.DataFrame,
    stock_scan_df: pd.DataFrame | None = None,
    chart_key: str | None = None,
) -> None:
    """Render sector rotation as heatmap + sortable detail table."""
    prepared = prepare_sector_rotation_frame(sector_df, stock_scan_df=stock_scan_df)
    if prepared.empty:
        st.info("No sector rotation rows available.")
        return

    display_cols = [
        col
        for col in ["Sector", "RS", "rs_change_20", "Momentum", "RS_rank", "Quadrant", "breadth_buy_pct"]
        if col in prepared.columns
    ]
    if not display_cols:
        display_cols = list(prepared.columns[:8])
    table_df = prepared[display_cols].copy()
    table_df = table_df.rename(
        columns={
            "rs_change_20": "RS Δ20",
            "breadth_buy_pct": "Breadth BUY %",
            "RS_rank": "Sector Rank",
        }
    )

    metric_cols = [col for col in ["RS", "rs_change_20", "Momentum", "breadth_buy_pct"] if col in prepared.columns]

    if metric_cols and "Sector" in prepared.columns:
        heatmap_source = prepared[["Sector"] + metric_cols].copy().set_index("Sector")
        for col in metric_cols:
            heatmap_source[col] = pd.to_numeric(heatmap_source[col], errors="coerce")
        heatmap_source = heatmap_source.replace([float("inf"), float("-inf")], np.nan).dropna(
            how="all", subset=metric_cols
        )
        heatmap_source = heatmap_source[
            ~heatmap_source.index.astype(str).str.lower().isin({"", "nan", "none", "null", "na"})
        ]
        if heatmap_source.empty:
            st.info("No numeric sector metrics available for heatmap.")
            st.dataframe(table_df, use_container_width=True, hide_index=True, height=420)
            return
        if len(heatmap_source) < 2:
            st.info("Only one sector row available; showing table view.")
            st.dataframe(table_df, use_container_width=True, hide_index=True, height=420)
            return
        raw_values = heatmap_source[metric_cols].copy()
        col_median = raw_values.median(axis=0)
        col_std = raw_values.std(axis=0, ddof=0).replace(0, np.nan)
        normalized = ((raw_values - col_median) / col_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        normalized = normalized.clip(-2.5, 2.5)
        try:
            label_map = {
                "RS": "RS",
                "rs_change_20": "RS Δ20",
                "Momentum": "Momentum",
                "breadth_buy_pct": "Breadth BUY %",
            }
            x_labels = [label_map.get(c, c.replace("_", " ").upper()) for c in metric_cols]
            fig = go.Figure(
                data=go.Heatmap(
                    z=normalized.values,
                    customdata=raw_values.values,
                    x=x_labels,
                    y=heatmap_source.index.tolist(),
                    zmid=0.0,
                    zmin=-2.5,
                    zmax=2.5,
                    colorscale=[
                        [0.0, "#b91c1c"],
                        [0.5, "#f8fafc"],
                        [1.0, "#15803d"],
                    ],
                    colorbar=dict(title="Relative"),
                    hovertemplate=(
                        "Sector: %{y}<br>"
                        "Metric: %{x}<br>"
                        "Raw: %{customdata:.2f}<br>"
                        "Relative: %{z:.2f}σ<extra></extra>"
                    ),
                )
            )
            fig.update_layout(
                height=max(360, min(760, 24 * len(heatmap_source) + 120)),
                margin=dict(l=20, r=20, t=10, b=20),
            )
            plot_kwargs = {"use_container_width": True}
            if chart_key:
                plot_kwargs["key"] = chart_key
            st.plotly_chart(fig, **plot_kwargs)
        except Exception as exc:
            st.warning(f"Sector heatmap fallback applied: {exc}")

    st.dataframe(table_df, use_container_width=True, hide_index=True, height=420)


def render_sector_dashboard_links_table(
    sector_df: pd.DataFrame,
    max_rows: int = 40,
) -> None:
    """Render sector table with clickable sector hyperlinks for drilldown view."""
    if sector_df is None or sector_df.empty:
        st.info("No sector dashboard rows in payload.")
        return

    display_cols = [col for col in ["Sector", "RS", "Momentum", "Quadrant", "RS_rank", "Top Stocks"] if col in sector_df.columns]
    if not display_cols:
        display_cols = list(sector_df.columns[:6])
    view_df = sector_df[display_cols].head(max_rows).copy()
    if "RS_rank" in view_df.columns:
        view_df = view_df.rename(columns={"RS_rank": "Sector Rank"})

    if "Sector" not in view_df.columns:
        st.dataframe(view_df, use_container_width=True, hide_index=True, height=360)
        return

    headers = list(view_df.columns)
    rows_html: list[str] = []
    for _, row in view_df.iterrows():
        sector_name = str(row.get("Sector", "")).strip()
        link = f"?view=sector&sector={quote_plus(sector_name)}"
        cells_html: list[str] = [
            f"<td><a href='{link}' target='_self'>{html.escape(sector_name)}</a></td>"
        ]
        for col in headers[1:]:
            value = row.get(col)
            if pd.isna(value):
                text = "—"
            elif isinstance(value, float):
                text = f"{value:.2f}"
            else:
                text = str(value)
            cells_html.append(f"<td>{html.escape(text)}</td>")
        rows_html.append(f"<tr>{''.join(cells_html)}</tr>")

    table_html = (
        "<div style='max-height:360px;overflow:auto;border:1px solid rgba(148,163,184,0.35);border-radius:8px;'>"
        "<table style='width:100%;border-collapse:collapse;font-size:0.84rem;'>"
        "<thead><tr>"
        + "".join(
            f"<th style='text-align:left;padding:0.4rem 0.5rem;border-bottom:1px solid rgba(148,163,184,0.35);'>{html.escape(str(col))}</th>"
            for col in headers
        )
        + "</tr></thead>"
        "<tbody>"
        + "".join(rows_html)
        + "</tbody></table></div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


def render_breakout_evidence_cards(
    breakout_df: pd.DataFrame,
    signal_date: str | None,
    max_cards: int = 8,
) -> None:
    """Render compact breakout evidence cards with reusable verdict badges."""
    evidence = build_breakout_evidence_frame(breakout_df, signal_date=signal_date)
    if evidence.empty:
        st.info("No actionable breakout evidence available.")
        return

    evidence = evidence.sort_values("setup_quality", ascending=False).head(max_cards).reset_index(drop=True)
    cols = st.columns(2)
    for idx, row in evidence.iterrows():
        column = cols[idx % 2]
        with column:
            with st.container(border=True):
                badge = (
                    f"<span style='background:{row.get('verdict_color', '#334155')};"
                    "color:white;padding:2px 8px;border-radius:999px;"
                    "font-size:0.78rem;font-weight:600;'>"
                    f"{row.get('verdict', 'Unknown')}</span>"
                )
                st.markdown(
                    f"**{row.get('symbol_id', '—')}**  \n"
                    f"{badge}",
                    unsafe_allow_html=True,
                )
                st.caption(
                    f"{row.get('breakout_type', '—')} | "
                    f"Base {int(row.get('base_length_days', 0))}D | "
                    f"Signal {row.get('signal_date') or 'n/a'}"
                )
                st.caption(
                    f"Contraction: {pd.to_numeric(row.get('contraction_pct'), errors='coerce'):.1f}% | "
                    f"Vol x: {pd.to_numeric(row.get('volume_ratio'), errors='coerce'):.2f} | "
                    f"Dist 52W: {pd.to_numeric(row.get('near_52w_high_pct'), errors='coerce'):.2f}%"
                )
                st.caption(
                    f"Regime: {row.get('market_regime', 'N/A')} / {row.get('market_bias', 'N/A')} | "
                    f"Tier: {row.get('candidate_tier', 'N/A')} | "
                    f"Setup Quality: {pd.to_numeric(row.get('setup_quality'), errors='coerce'):.1f}"
                )
                trend_reasons = str(row.get("symbol_trend_reasons", "") or "").strip()
                if trend_reasons:
                    st.caption(f"Trend reasons: {trend_reasons}")
                filter_reason = str(row.get("filter_reason", "") or "").strip()
                if filter_reason:
                    st.caption(f"Filter reason: {filter_reason}")
