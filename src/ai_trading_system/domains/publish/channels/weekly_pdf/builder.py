"""Builder: orchestrates loading, metric assembly, and rendering."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf import charts, metrics
from ai_trading_system.domains.publish.channels.weekly_pdf.data_loader import (
    WeeklyReportData,
    load_report_data,
)
from ai_trading_system.domains.publish.channels.weekly_pdf.renderer import render
from ai_trading_system.pipeline.contracts import StageContext

logger = logging.getLogger(__name__)


def _df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.where(df.notna(), None).to_dict(orient="records")


def _write_table_csvs(output_dir: Path, tables: Dict[str, pd.DataFrame]) -> Dict[str, str]:
    table_dir = output_dir / "tables"
    table_dir.mkdir(parents=True, exist_ok=True)
    written: Dict[str, str] = {}
    for name, df in tables.items():
        if df is None or df.empty:
            continue
        path = table_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        written[name] = str(path)
    return written


def build_report(
    context: StageContext,
    datasets: Dict[str, Any],
    output_dir: Path,
) -> Dict[str, Any]:
    """Build the weekly report. Returns a manifest dict."""
    data: WeeklyReportData = load_report_data(context, datasets)

    sectors = metrics.sector_leaders(data.sector_dashboard)
    top_ranked = metrics.top_ranked(data.ranked_signals)
    volume_delivery = metrics.volume_delivery_movers(data.ranked_signals)
    weekly_price = metrics.weekly_price_movers(data.ranked_signals)
    volume_shockers = metrics.unusual_volume_shockers(data.ranked_signals)
    tier_a = metrics.tier_a_breakouts(data.breakout_scan)
    tier_b = metrics.tier_b_breakouts(data.breakout_scan)
    patterns = metrics.top_patterns(data.pattern_scan)
    regime = metrics.regime_summary(
        data.rank_summary,
        data.dashboard_payload,
        data.sector_dashboard,
        data.ranked_signals,
        trust_status_fallback=data.trust_status,
    )

    rank_improvers, rank_decliners = metrics.compute_rank_movers(
        data.ranked_signals, data.prior_ranked_signals
    )
    sector_movers = metrics.compute_sector_movers(
        data.sector_dashboard, data.prior_sector_dashboard
    )
    failed_breakouts = metrics.detect_failed_breakouts(
        data.breakout_scan,
        data.prior_breakouts_per_run,
        data.ranked_signals,
    )
    breadth_latest = data.market_breadth.iloc[-1].to_dict() if not data.market_breadth.empty else {}

    chart_paths = _render_charts(
        output_dir=output_dir,
        project_root=getattr(context, "project_root", None),
        run_date=data.run_date,
        breadth=data.market_breadth,
        sectors_full=data.sector_dashboard,
        improvers=rank_improvers,
        decliners=rank_decliners,
        ranked=data.ranked_signals,
        breakouts=data.breakout_scan,
    )

    template_context = {
        "week_ending": data.run_date,
        "run_id": data.run_id,
        "regime": regime,
        "sectors": _df_to_records(sectors),
        "top_ranked": _df_to_records(top_ranked),
        "volume_delivery": _df_to_records(volume_delivery),
        "weekly_price": _df_to_records(weekly_price),
        "volume_shockers": _df_to_records(volume_shockers),
        "tier_a": _df_to_records(tier_a),
        "tier_b": _df_to_records(tier_b),
        "patterns": _df_to_records(patterns),
        "prior_run_id": data.prior_run_id,
        "prior_run_date": data.prior_run_date,
        "rank_improvers": _df_to_records(rank_improvers),
        "rank_decliners": _df_to_records(rank_decliners),
        "sector_movers": _df_to_records(sector_movers),
        "failed_breakouts": _df_to_records(failed_breakouts),
        "breadth_latest": breadth_latest,
        "breadth_rows": _df_to_records(data.market_breadth.tail(10)) if not data.market_breadth.empty else [],
        "charts": chart_paths,
    }

    html_path, pdf_path, pdf_error = render(template_context, output_dir)

    table_paths = _write_table_csvs(
        output_dir,
        {
            "weekly_sector_leaders": sectors,
            "weekly_ranked_top": top_ranked,
            "weekly_volume_delivery_movers": volume_delivery,
            "weekly_price_movers": weekly_price,
            "weekly_unusual_volume_shockers": volume_shockers,
            "weekly_breakouts_tier_a": tier_a,
            "weekly_breakouts_tier_b": tier_b,
            "weekly_patterns": patterns,
            "weekly_rank_improvers": rank_improvers,
            "weekly_rank_decliners": rank_decliners,
            "weekly_sector_movers": sector_movers,
            "weekly_failed_breakouts": failed_breakouts,
            "market_breadth": data.market_breadth,
        },
    )

    manifest = {
        "report_id": f"weekly_pdf-{data.run_id}",
        "week_ending": data.run_date,
        "run_id": data.run_id,
        "trust_status": data.trust_status,
        "regime": regime,
        "html_path": str(html_path),
        "pdf_path": str(pdf_path) if pdf_path else None,
        "pdf_error": pdf_error,
        "tables": table_paths,
        "counts": {
            "sectors": len(sectors),
            "top_ranked": len(top_ranked),
            "volume_delivery": len(volume_delivery),
            "weekly_price": len(weekly_price),
            "volume_shockers": len(volume_shockers),
            "tier_a": len(tier_a),
            "tier_b": len(tier_b),
            "patterns": len(patterns),
            "rank_improvers": len(rank_improvers),
            "rank_decliners": len(rank_decliners),
            "sector_movers": len(sector_movers),
            "failed_breakouts": len(failed_breakouts),
            "breadth_rows": int(len(data.market_breadth)),
        },
        "prior_run_id": data.prior_run_id,
        "prior_run_date": data.prior_run_date,
        "breadth_latest": breadth_latest,
        "charts": chart_paths,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }

    json_path = output_dir / "weekly_market_report.json"
    json_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    manifest["json_path"] = str(json_path)
    return manifest


def _render_charts(
    *,
    output_dir: Path,
    project_root: Optional[Path],
    run_date: str,
    breadth: pd.DataFrame,
    sectors_full: pd.DataFrame,
    improvers: pd.DataFrame,
    decliners: pd.DataFrame,
    ranked: pd.DataFrame,
    breakouts: pd.DataFrame,
) -> Dict[str, Any]:
    """Generate all charts. Each is independently optional."""
    chart_dir = output_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)

    out: Dict[str, Any] = {"breadth": None, "sectors": None, "movers": None, "stocks": []}

    breadth_path = charts.breadth_chart(breadth, chart_dir / "breadth_above_smas.png")
    if breadth_path is not None:
        out["breadth"] = "charts/" + breadth_path.name

    sector_path = charts.sector_rs_bars(sectors_full, chart_dir / "sector_rs.png")
    if sector_path is not None:
        out["sectors"] = "charts/" + sector_path.name

    mover_path = charts.rank_mover_bars(improvers, decliners, chart_dir / "rank_movers.png")
    if mover_path is not None:
        out["movers"] = "charts/" + mover_path.name

    if project_root is None:
        return out
    ohlcv_path = project_root / "data" / "ohlcv.duckdb"
    if not ohlcv_path.exists():
        return out
    end_date = _safe_date(run_date)
    if end_date is None:
        return out

    targets = charts.pick_candle_targets(ranked, improvers, breakouts)
    stocks_dir = chart_dir / "stocks"
    stocks_dir.mkdir(parents=True, exist_ok=True)
    rendered: List[Dict[str, Any]] = []
    for tgt in targets:
        sym = tgt["symbol_id"]
        png = stocks_dir / f"{sym}.png"
        path = charts.candlestick(
            ohlcv_db_path=ohlcv_path,
            symbol_id=sym,
            end_date=end_date,
            output_path=png,
            breakout_level=tgt.get("breakout_level"),
        )
        if path is not None:
            rendered.append({
                "symbol_id": sym,
                "source": tgt.get("source"),
                "path": "charts/stocks/" + path.name,
            })
    out["stocks"] = rendered
    return out


def _safe_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
