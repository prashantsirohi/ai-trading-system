"""Builder: orchestrates loading, metric assembly, and rendering."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from ai_trading_system.domains.publish.channels.weekly_pdf import metrics
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

    template_context = {
        "week_ending": data.run_date,
        "run_id": data.run_id,
        "regime": regime,
        "sectors": _df_to_records(sectors),
        "top_ranked": _df_to_records(top_ranked),
        "volume_delivery": _df_to_records(volume_delivery),
        "tier_a": _df_to_records(tier_a),
        "tier_b": _df_to_records(tier_b),
        "patterns": _df_to_records(patterns),
    }

    html_path, pdf_path, pdf_error = render(template_context, output_dir)

    table_paths = _write_table_csvs(
        output_dir,
        {
            "weekly_sector_leaders": sectors,
            "weekly_ranked_top": top_ranked,
            "weekly_volume_delivery_movers": volume_delivery,
            "weekly_breakouts_tier_a": tier_a,
            "weekly_breakouts_tier_b": tier_b,
            "weekly_patterns": patterns,
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
            "tier_a": len(tier_a),
            "tier_b": len(tier_b),
            "patterns": len(patterns),
        },
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }

    json_path = output_dir / "weekly_market_report.json"
    json_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    manifest["json_path"] = str(json_path)
    return manifest
