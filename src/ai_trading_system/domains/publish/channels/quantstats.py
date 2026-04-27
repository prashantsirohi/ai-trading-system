"""QuantStats + pipeline-summary tear sheet publishing."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.platform.logging.logger import logger

if "MPLCONFIGDIR" not in os.environ:
    _mpl_dir = Path(__file__).resolve().parents[5] / "logs" / "matplotlib"
    _mpl_dir.mkdir(parents=True, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = str(_mpl_dir)

try:
    import quantstats as _quantstats  # noqa: F401

    HAS_QUANTSTATS = True
except Exception:
    HAS_QUANTSTATS = False

try:
    import plotly.graph_objects as go
    from plotly.offline import plot as plotly_plot

    HAS_PLOTLY = True
except Exception:
    HAS_PLOTLY = False


@dataclass(frozen=True)
class RankedSnapshot:
    run_id: str
    path: Path
    run_date: pd.Timestamp
    mtime: float


def _parse_run_id(path: Path) -> str:
    parts = path.parts
    if len(parts) < 5:
        return ""
    return str(parts[-4])


def _parse_run_date(run_id: str, fallback_mtime: float) -> pd.Timestamp:
    match = re.match(r"^pipeline-(\d{4}-\d{2}-\d{2})-", str(run_id))
    if match:
        return pd.Timestamp(match.group(1))
    # Keep run-date keys timezone-naive across both parsing paths so
    # snapshot sorting/comparisons never mix naive and tz-aware timestamps.
    return pd.to_datetime(float(fallback_mtime), unit="s", utc=True).tz_localize(None).normalize()


def _latest_ranked_snapshots(
    pipeline_runs_dir: Path,
    max_runs: int = 240,
) -> list[RankedSnapshot]:
    candidates = list(pipeline_runs_dir.glob("*/rank/attempt_*/ranked_signals.csv"))
    if not candidates:
        return []

    latest_by_run: dict[str, RankedSnapshot] = {}
    for path in candidates:
        run_id = _parse_run_id(path)
        if not run_id:
            continue
        mtime = path.stat().st_mtime
        current = latest_by_run.get(run_id)
        if current is None or mtime > current.mtime:
            latest_by_run[run_id] = RankedSnapshot(
                run_id=run_id,
                path=path,
                run_date=_parse_run_date(run_id, mtime),
                mtime=mtime,
            )

    ordered = sorted(
        latest_by_run.values(),
        key=lambda snap: (snap.run_date, snap.mtime, snap.run_id),
    )
    if max_runs > 0:
        ordered = ordered[-max_runs:]
    return ordered


def _latest_rank_artifact_dir(project_root: Path, run_id: str | None) -> Path | None:
    pipeline_runs_dir = project_root / "data" / "pipeline_runs"
    if run_id:
        rank_dir = pipeline_runs_dir / run_id / "rank"
        if not rank_dir.exists():
            return None
        attempts = sorted(
            rank_dir.glob("attempt_*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return attempts[0] if attempts else None

    ranked_candidates = sorted(
        pipeline_runs_dir.glob("*/rank/attempt_*/ranked_signals.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not ranked_candidates:
        return None
    return ranked_candidates[0].parent


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _load_rank_snapshot_frames(
    project_root: Path,
    run_id: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rank_attempt_dir = _latest_rank_artifact_dir(project_root, run_id)
    if rank_attempt_dir is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    ranked = _read_csv_if_exists(rank_attempt_dir / "ranked_signals.csv")
    breakouts = _read_csv_if_exists(rank_attempt_dir / "breakout_scan.csv")
    sector = _read_csv_if_exists(rank_attempt_dir / "sector_dashboard.csv")
    return ranked, breakouts, sector


def build_dashboard_strategy_returns(
    ranked_paths: Sequence[Path | str],
    top_n: int = 20,
    min_overlap: int = 5,
) -> tuple[pd.Series, pd.DataFrame]:
    """Build equal-weight strategy returns from consecutive ranked snapshots."""
    snapshots: list[RankedSnapshot] = []
    for raw_path in ranked_paths:
        path = Path(raw_path)
        if not path.exists():
            continue
        run_id = _parse_run_id(path)
        mtime = path.stat().st_mtime
        snapshots.append(
            RankedSnapshot(
                run_id=run_id or path.stem,
                path=path,
                run_date=_parse_run_date(run_id, mtime),
                mtime=mtime,
            )
        )

    snapshots = sorted(snapshots, key=lambda snap: (snap.run_date, snap.mtime, snap.run_id))
    if len(snapshots) < 2:
        return pd.Series(dtype=float), pd.DataFrame()

    details: list[dict[str, object]] = []
    for prev_snap, next_snap in zip(snapshots, snapshots[1:]):
        try:
            prev_df = pd.read_csv(prev_snap.path, usecols=["symbol_id", "close", "composite_score"])
            next_df = pd.read_csv(next_snap.path, usecols=["symbol_id", "close"])
        except Exception as exc:
            logger.warning("Could not read ranked snapshots %s -> %s: %s", prev_snap.path, next_snap.path, exc)
            continue

        if prev_df.empty or next_df.empty:
            continue

        prev_df.loc[:, "symbol_id"] = prev_df["symbol_id"].astype(str)
        next_df.loc[:, "symbol_id"] = next_df["symbol_id"].astype(str)
        prev_df.loc[:, "close"] = pd.to_numeric(prev_df["close"], errors="coerce")
        next_df.loc[:, "close"] = pd.to_numeric(next_df["close"], errors="coerce")
        prev_df.loc[:, "composite_score"] = pd.to_numeric(prev_df["composite_score"], errors="coerce")

        prev_top = (
            prev_df.dropna(subset=["symbol_id", "close", "composite_score"])
            .sort_values("composite_score", ascending=False)
            .head(max(1, int(top_n)))
            .copy()
        )
        if prev_top.empty:
            continue

        merged = prev_top.merge(
            next_df.rename(columns={"close": "close_next"}),
            on="symbol_id",
            how="inner",
        ).copy()
        merged = merged.dropna(subset=["close", "close_next"]).copy()
        merged = merged.loc[merged["close"] > 0].copy()
        if merged.empty:
            continue

        overlap = int(len(merged))
        if overlap < max(1, int(min_overlap)):
            continue

        per_symbol_ret = (merged["close_next"] / merged["close"]) - 1.0
        period_ret = float(per_symbol_ret.mean())
        details.append(
            {
                "date": next_snap.run_date,
                "run_id": next_snap.run_id,
                "previous_run_id": prev_snap.run_id,
                "return": period_ret,
                "overlap_count": overlap,
                "top_n": int(top_n),
                "coverage_pct": round((overlap / max(1, int(top_n))) * 100.0, 2),
            }
        )

    detail_df = pd.DataFrame(details)
    if detail_df.empty:
        return pd.Series(dtype=float), detail_df

    detail_df = detail_df.sort_values("date").reset_index(drop=True).copy()
    returns = detail_df.set_index("date")["return"].astype(float)
    returns.index = pd.to_datetime(returns.index)
    returns.name = "dashboard_topn_returns"
    return returns, detail_df


def _compute_summary_metrics(returns: pd.Series) -> dict[str, float]:
    if returns.empty:
        return {}
    clean = returns.astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return {}
    equity = (1.0 + clean).cumprod()
    cumulative = float(equity.iloc[-1] - 1.0)
    periods = len(clean)
    annual_factor = 252.0
    cagr = float((equity.iloc[-1] ** (annual_factor / periods)) - 1.0) if periods > 0 and equity.iloc[-1] > 0 else np.nan
    vol = float(clean.std(ddof=0) * np.sqrt(annual_factor))
    mean_ret = float(clean.mean() * annual_factor)
    sharpe = float(mean_ret / vol) if vol > 0 else np.nan
    roll_max = equity.cummax()
    drawdown = (equity / roll_max) - 1.0
    max_dd = float(drawdown.min())
    win_rate = float((clean > 0).mean())
    return {
        "cumulative_return": cumulative,
        "cagr": cagr,
        "annual_volatility": vol,
        "sharpe": sharpe,
        "max_drawdown": max_dd,
        "win_rate": win_rate,
        "observations": float(periods),
    }


def _load_market_breadth_sma200(project_root: Path, start_date: str) -> pd.DataFrame:
    db_path = project_root / "data" / "ohlcv.duckdb"
    if not db_path.exists():
        return pd.DataFrame()

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        df = con.execute(
            f"""
            WITH base AS (
                SELECT
                    CAST(timestamp AS DATE) AS trade_date,
                    symbol_id,
                    close,
                    AVG(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma_200,
                    COUNT(close) OVER (
                        PARTITION BY symbol_id
                        ORDER BY CAST(timestamp AS DATE)
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS obs_200
                FROM _catalog
                WHERE exchange = 'NSE'
                  AND CAST(timestamp AS DATE) >= DATE '{start_date}'
            )
            SELECT
                trade_date,
                SUM(CASE WHEN obs_200 >= 200 THEN 1 ELSE 0 END) AS eligible_200,
                SUM(CASE WHEN obs_200 >= 200 AND close > sma_200 THEN 1 ELSE 0 END) AS above_200_count,
                ROUND(
                    SUM(CASE WHEN obs_200 >= 200 AND close > sma_200 THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN obs_200 >= 200 THEN 1 ELSE 0 END), 0),
                    2
                ) AS pct_above_200
            FROM base
            GROUP BY trade_date
            ORDER BY trade_date
            """
        ).fetchdf()
    finally:
        con.close()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["pct_above_200"] = pd.to_numeric(df["pct_above_200"], errors="coerce")
    return df.dropna(subset=["pct_above_200"])


def _table_html(df: pd.DataFrame, max_rows: int = 25) -> str:
    if df is None or df.empty:
        return "<p class='muted'>No data available.</p>"
    out = df.head(max_rows).copy()
    return out.to_html(index=False, classes="data-table", border=0, justify="left")


def _find_column(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    lookup = {str(col).strip().lower(): str(col) for col in df.columns}
    for candidate in candidates:
        resolved = lookup.get(str(candidate).strip().lower())
        if resolved:
            return resolved
    return None


def _canonicalize_sector_frame(sector_df: pd.DataFrame) -> pd.DataFrame:
    if sector_df is None or sector_df.empty:
        return pd.DataFrame()
    frame = sector_df.copy()
    aliases: dict[str, Sequence[str]] = {
        "Sector": ("Sector", "sector", "sector_name", "industry"),
        "RS": ("RS", "rs", "relative_strength", "sector_rs"),
        "RS_20": ("RS_20", "rs_20", "rs20", "relative_strength_20"),
        "Momentum": ("Momentum", "momentum", "mom", "momentum_score"),
        "RS_rank": ("RS_rank", "rs_rank", "rank", "sector_rank"),
        "Quadrant": ("Quadrant", "quadrant", "state"),
    }
    rename_map: dict[str, str] = {}
    for canonical, candidates in aliases.items():
        if canonical in frame.columns:
            continue
        resolved = _find_column(frame, candidates)
        if resolved and resolved != canonical:
            rename_map[resolved] = canonical
    if rename_map:
        frame = frame.rename(columns=rename_map)
    return frame


def _sector_heatmap_html(sector_df: pd.DataFrame) -> str:
    if sector_df is None or sector_df.empty:
        return "<p class='muted'>No sector rotation rows available.</p>"
    frame = _canonicalize_sector_frame(sector_df)
    if frame.empty:
        return "<p class='muted'>No sector rotation rows available.</p>"
    if "RS_20" in frame.columns and "rs_change_20" not in frame.columns and "RS" in frame.columns:
        frame["rs_change_20"] = pd.to_numeric(frame["RS"], errors="coerce") - pd.to_numeric(frame["RS_20"], errors="coerce")
    metric_cols = [c for c in ["RS", "rs_change_20", "Momentum"] if c in frame.columns]
    if "Sector" not in frame.columns or not metric_cols:
        return _table_html(frame[["Sector"] + metric_cols] if "Sector" in frame.columns else frame)

    frame["Sector"] = frame["Sector"].astype(str)
    heatmap_df = frame[["Sector"] + metric_cols].dropna(how="all", subset=metric_cols)
    if heatmap_df.empty:
        return "<p class='muted'>Sector metrics unavailable for heatmap.</p>"

    if HAS_PLOTLY:
        try:
            raw_values = heatmap_df[metric_cols].apply(pd.to_numeric, errors="coerce")
            col_median = raw_values.median(axis=0)
            col_std = raw_values.std(axis=0, ddof=0).replace(0, np.nan)
            normalized = ((raw_values - col_median) / col_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
            normalized = normalized.clip(-2.5, 2.5)
            label_map = {
                "RS": "RS",
                "rs_change_20": "RS Δ20",
                "Momentum": "Momentum",
            }
            x_labels = [label_map.get(c, c.replace("_", " ").upper()) for c in metric_cols]
            fig = go.Figure(
                data=go.Heatmap(
                    z=normalized.values,
                    customdata=raw_values.values,
                    x=x_labels,
                    y=heatmap_df["Sector"].tolist(),
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
                height=max(360, min(760, 24 * len(heatmap_df) + 120)),
                margin=dict(l=10, r=10, t=20, b=20),
            )
            return plotly_plot(fig, include_plotlyjs=False, output_type="div")
        except Exception:
            return _table_html(heatmap_df)
    return _table_html(heatmap_df)


def _breadth_chart_html(breadth_df: pd.DataFrame) -> str:
    if breadth_df is None or breadth_df.empty:
        return "<p class='muted'>SMA200 breadth history unavailable.</p>"
    if HAS_PLOTLY:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=breadth_df["trade_date"],
                y=breadth_df["pct_above_200"],
                mode="lines",
                name="% Above 200 SMA",
                line=dict(color="#dc2626", width=2),
            )
        )
        fig.add_hline(y=60, line_dash="dash", line_color="#16a34a", opacity=0.55)
        fig.add_hline(y=40, line_dash="dash", line_color="#ea580c", opacity=0.55)
        fig.update_layout(
            height=360,
            margin=dict(l=10, r=10, t=20, b=20),
            xaxis_title="Date",
            yaxis_title="% Above 200 SMA",
            showlegend=False,
        )
        return plotly_plot(fig, include_plotlyjs=False, output_type="div")
    return _table_html(breadth_df[["trade_date", "pct_above_200"]], max_rows=120)


def _enriched_tearsheet_html(
    *,
    run_id: str | None,
    run_date: str | None,
    top_n: int,
    summary_metrics: dict[str, float],
    returns_details: pd.DataFrame,
    ranked_df: pd.DataFrame,
    breakout_df: pd.DataFrame,
    sector_df: pd.DataFrame,
    breadth_df: pd.DataFrame,
    quantstats_core_filename: str | None,
) -> str:
    summary_df = pd.DataFrame(
        [
            {
                "Cumulative Return": f"{summary_metrics.get('cumulative_return', np.nan):.2%}",
                "CAGR": f"{summary_metrics.get('cagr', np.nan):.2%}",
                "Sharpe": f"{summary_metrics.get('sharpe', np.nan):.2f}",
                "Max Drawdown": f"{summary_metrics.get('max_drawdown', np.nan):.2%}",
                "Win Rate": f"{summary_metrics.get('win_rate', np.nan):.2%}",
                "Observations": int(summary_metrics.get("observations", 0)),
            }
        ]
    )

    rank_cols = [
        "symbol_id",
        "composite_score",
        "close",
        "rel_strength_score",
        "vol_intensity_score",
        "trend_score_score",
        "prox_high_score",
        "delivery_pct_score",
        "sector_strength_score",
    ]
    rank_view = ranked_df[[c for c in rank_cols if c in ranked_df.columns]].copy() if not ranked_df.empty else pd.DataFrame()
    if "composite_score" in rank_view.columns:
        rank_view = rank_view.sort_values("composite_score", ascending=False)

    breakout_cols = [
        "symbol_id",
        "setup_family",
        "breakout_tag",
        "setup_quality",
        "breakout_pct",
        "volume_ratio",
        "near_52w_high_pct",
        "adx_14",
    ]
    breakout_view = breakout_df[[c for c in breakout_cols if c in breakout_df.columns]].copy() if not breakout_df.empty else pd.DataFrame()
    if "setup_quality" in breakout_view.columns:
        breakout_view = breakout_view.sort_values("setup_quality", ascending=False)

    sector_view_cols = [c for c in ["Sector", "RS", "RS_20", "Momentum", "RS_rank", "Quadrant"] if c in sector_df.columns]
    sector_view = sector_df[sector_view_cols].copy() if not sector_df.empty else pd.DataFrame()
    if "RS_rank" in sector_view.columns:
        sector_view = sector_view.sort_values("RS_rank", ascending=True)
    elif "RS" in sector_view.columns:
        sector_view = sector_view.sort_values("RS", ascending=False)

    core_link_html = (
        f"<p class=\"small muted\">QuantStats core report: <a href=\"{quantstats_core_filename}\" target=\"_blank\">{quantstats_core_filename}</a></p>"
        if quantstats_core_filename
        else ""
    )

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Dashboard Quant Tear Sheet</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 20px; color: #0f172a; background: #f8fafc; }}
    h1, h2, h3 {{ margin: 0.2rem 0 0.6rem 0; }}
    .muted {{ color: #475569; font-size: 0.95rem; }}
    .card {{ background: white; border: 1px solid #cbd5e1; border-radius: 10px; padding: 14px; margin-bottom: 14px; box-shadow: 0 1px 2px rgba(15,23,42,0.05); }}
    .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
    .data-table {{ border-collapse: collapse; width: 100%; font-size: 0.88rem; }}
    .data-table th, .data-table td {{ border: 1px solid #e2e8f0; padding: 6px 8px; text-align: left; }}
    .data-table th {{ background: #f1f5f9; }}
    .small {{ font-size: 0.84rem; }}
    @media (max-width: 1100px) {{ .grid-2 {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <h1>Quant Dashboard Tear Sheet</h1>
  <p class="muted">run_id=<b>{run_id or "n/a"}</b> | run_date=<b>{run_date or "n/a"}</b> | strategy: top-{int(top_n)} equal-weight forward returns</p>

  <div class="card">
    <h2>Performance Summary</h2>
    {_table_html(summary_df, max_rows=1)}
    {core_link_html}
  </div>

  <div class="grid-2">
    <div class="card">
      <h2>Sector Rotation Heatmap</h2>
      {_sector_heatmap_html(sector_view)}
    </div>
    <div class="card">
      <h2>Market Breadth (% Above SMA200)</h2>
      {_breadth_chart_html(breadth_df)}
    </div>
  </div>

  <div class="grid-2">
    <div class="card">
      <h2>Top Ranked Stocks</h2>
      {_table_html(rank_view, max_rows=30)}
    </div>
    <div class="card">
      <h2>Breakout Candidates</h2>
      {_table_html(breakout_view, max_rows=30)}
    </div>
  </div>

  <div class="card">
    <h2>Return Construction Details</h2>
    {_table_html(returns_details, max_rows=120)}
  </div>
</body>
</html>
""".strip()


def publish_dashboard_quantstats_tearsheet(
    project_root: str | Path,
    run_id: str | None = None,
    run_date: str | None = None,
    top_n: int = 20,
    min_overlap: int = 5,
    max_runs: int = 240,
    output_dir: str | Path | None = None,
    latest_ranked_df: pd.DataFrame | None = None,
    latest_breakout_df: pd.DataFrame | None = None,
    latest_sector_df: pd.DataFrame | None = None,
    breadth_start_date: str = "2018-01-01",
    write_core_quantstats_html: bool = False,
) -> dict[str, object]:
    """Publish an enriched QuantStats tear sheet with pipeline summaries."""
    if write_core_quantstats_html and not HAS_QUANTSTATS:
        return {"ok": False, "error": "quantstats_not_available"}

    root = Path(project_root)
    pipeline_runs_dir = root / "data" / "pipeline_runs"
    if not pipeline_runs_dir.exists():
        return {"ok": False, "error": f"pipeline_runs_dir_missing: {pipeline_runs_dir}"}

    snapshots = _latest_ranked_snapshots(pipeline_runs_dir, max_runs=max_runs)
    paths = [snap.path for snap in snapshots]
    returns, detail_df = build_dashboard_strategy_returns(
        paths,
        top_n=top_n,
        min_overlap=min_overlap,
    )
    if returns.empty or len(returns) < 2:
        return {
            "ok": False,
            "error": "insufficient_rank_history_for_tearsheet",
            "observations": int(len(returns)),
        }

    ranked_df = latest_ranked_df.copy() if isinstance(latest_ranked_df, pd.DataFrame) else pd.DataFrame()
    breakout_df = latest_breakout_df.copy() if isinstance(latest_breakout_df, pd.DataFrame) else pd.DataFrame()
    sector_df = latest_sector_df.copy() if isinstance(latest_sector_df, pd.DataFrame) else pd.DataFrame()
    if ranked_df.empty and breakout_df.empty and sector_df.empty:
        ranked_df, breakout_df, sector_df = _load_rank_snapshot_frames(root, run_id)

    breadth_df = _load_market_breadth_sma200(root, start_date=breadth_start_date)
    summary_metrics = _compute_summary_metrics(returns)

    target_dir = Path(output_dir) if output_dir else root / "reports" / "quantstats"
    target_dir.mkdir(parents=True, exist_ok=True)
    suffix = run_id or run_date or pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    safe_suffix = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(suffix))

    html_path = target_dir / f"dashboard_tearsheet_{safe_suffix}.html"
    csv_path = target_dir / f"dashboard_tearsheet_{safe_suffix}_returns.csv"
    series_path = target_dir / f"dashboard_tearsheet_{safe_suffix}_series.csv"
    meta_path = target_dir / f"dashboard_tearsheet_{safe_suffix}.json"

    detail_df.to_csv(csv_path, index=False)
    pd.DataFrame({"date": returns.index, "return": returns.values}).to_csv(series_path, index=False)

    quantstats_core_path: Path | None = None
    if write_core_quantstats_html:
        quantstats_core_path = target_dir / f"dashboard_tearsheet_{safe_suffix}_quantstats.html"
        rc, _, stderr = _render_quantstats_tearsheet_subprocess(
            returns_series_csv=series_path,
            output_html=quantstats_core_path,
            title=f"Dashboard Top-{int(top_n)} Strategy Tear Sheet",
        )
        if rc != 0:
            return {
                "ok": False,
                "error": "quantstats_subprocess_failed",
                "returncode": int(rc),
                "stderr": stderr[-1200:] if isinstance(stderr, str) else "",
            }

    enriched_html = _enriched_tearsheet_html(
        run_id=run_id,
        run_date=run_date,
        top_n=int(top_n),
        summary_metrics=summary_metrics,
        returns_details=detail_df,
        ranked_df=ranked_df,
        breakout_df=breakout_df,
        sector_df=sector_df,
        breadth_df=breadth_df,
        quantstats_core_filename=quantstats_core_path.name if quantstats_core_path else None,
    )
    html_path.write_text(enriched_html, encoding="utf-8")

    payload: dict[str, Any] = {
        "ok": True,
        "run_id": run_id,
        "run_date": run_date,
        "tearsheet_path": str(html_path),
        "returns_path": str(csv_path),
        "returns_series_path": str(series_path),
        "observations": int(len(returns)),
        "start_date": str(pd.to_datetime(returns.index.min()).date()),
        "end_date": str(pd.to_datetime(returns.index.max()).date()),
        "avg_period_return": float(np.mean(returns.values)),
        "median_overlap_count": float(detail_df["overlap_count"].median()),
        "breadth_points": int(len(breadth_df)),
        "ranked_rows": int(len(ranked_df)),
        "breakout_rows": int(len(breakout_df)),
        "sector_rows": int(len(sector_df)),
        "core_quantstats_enabled": bool(write_core_quantstats_html),
    }
    if quantstats_core_path is not None:
        payload["quantstats_core_path"] = str(quantstats_core_path)
    with meta_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    payload["metadata_path"] = str(meta_path)
    logger.info("Dashboard QuantStats enriched tear sheet generated at %s", html_path)
    return payload


def _render_quantstats_tearsheet_subprocess(
    returns_series_csv: Path,
    output_html: Path,
    title: str,
) -> tuple[int, str, str]:
    """Render QuantStats HTML in a subprocess to isolate runtime crashes."""
    script = textwrap.dedent(
        """
        import os
        import sys
        from pathlib import Path
        import pandas as pd
        import quantstats as qs

        returns_csv = Path(sys.argv[1])
        output_html = Path(sys.argv[2])
        title = sys.argv[3]
        if "MPLCONFIGDIR" not in os.environ:
            mpl_dir = output_html.parent / ".mpl"
            mpl_dir.mkdir(parents=True, exist_ok=True)
            os.environ["MPLCONFIGDIR"] = str(mpl_dir)

        frame = pd.read_csv(returns_csv)
        frame["date"] = pd.to_datetime(frame["date"])
        series = pd.Series(frame["return"].astype(float).values, index=frame["date"])
        series = series.dropna()
        if series.empty:
            raise RuntimeError("No return observations available for QuantStats report")
        qs.reports.html(
            returns=series,
            title=title,
            output=str(output_html),
            compounded=True,
        )
        """
    ).strip()
    subprocess_env = os.environ.copy()
    subprocess_env["MPLBACKEND"] = "Agg"
    subprocess_env["MPLCONFIGDIR"] = str((output_html.parent / ".mpl").resolve())
    subprocess_env["XDG_CACHE_HOME"] = str((output_html.parent / ".cache").resolve())
    subprocess_env["HOME"] = str(output_html.parent.resolve())
    Path(subprocess_env["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    Path(subprocess_env["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [sys.executable, "-c", script, str(returns_series_csv), str(output_html), title],
        capture_output=True,
        text=True,
        env=subprocess_env,
    )
    return proc.returncode, proc.stdout, proc.stderr


__all__ = [
    "build_dashboard_strategy_returns",
    "publish_dashboard_quantstats_tearsheet",
]
