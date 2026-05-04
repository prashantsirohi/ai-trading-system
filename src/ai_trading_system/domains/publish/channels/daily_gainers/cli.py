"""Standalone CLI for the daily gainers PDF report."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.domains.publish.channels.daily_gainers.events import (
    attach_events,
    event_to_dict,
)
from ai_trading_system.domains.publish.channels.daily_gainers.gainers import compute_gainers
from ai_trading_system.domains.publish.channels.daily_gainers.llm import generate_insight
from ai_trading_system.domains.publish.channels.daily_gainers.renderer import render
from ai_trading_system.platform.utils.env import load_project_env


def main() -> None:
    load_project_env(Path.cwd())

    parser = argparse.ArgumentParser(description="Build a daily NSE gainers PDF report.")
    parser.add_argument("--as-of", type=_parse_date, default=None, help="Trading date as YYYY-MM-DD.")
    parser.add_argument("--threshold", type=float, default=5.0, help="Minimum close-to-close gain in percent.")
    parser.add_argument("--lookback-days", type=int, default=7, help="Corporate event lookback window.")
    parser.add_argument("--output-dir", type=Path, default=Path("reports/daily_gainers"))
    parser.add_argument("--model", default=None, help="OpenRouter model override.")
    parser.add_argument("--no-llm", action="store_true", help="Skip OpenRouter and use deterministic text.")
    args = parser.parse_args()

    gainers = compute_gainers(Path("data/ohlcv.duckdb"), args.as_of, threshold_pct=args.threshold)
    report_date = gainers.attrs.get("as_of") or args.as_of or date.today()
    symbols = [str(symbol) for symbol in gainers.get("symbol_id", []).tolist()]
    events_by_symbol = attach_events(symbols, as_of=report_date, lookback_days=args.lookback_days)
    insight = (
        {"summary_md": "LLM skipped by --no-llm - see table below.", "per_stock": {}, "status": "skipped_by_flag"}
        if args.no_llm
        else generate_insight(gainers, events_by_symbol, model=args.model)
    )

    context = _build_context(
        gainers=gainers,
        events_by_symbol=events_by_symbol,
        insight=insight,
        report_date=report_date,
        threshold=args.threshold,
        lookback_days=args.lookback_days,
    )
    html_path, pdf_path, pdf_error = render(context, args.output_dir)
    print(f"HTML: {html_path}")
    if pdf_path:
        print(f"PDF: {pdf_path}")
    if pdf_error:
        print(f"PDF warning: {pdf_error}")
    if insight.get("status") != "completed":
        detail = f": {insight.get('error')}" if insight.get("error") else ""
        print(f"LLM warning: {insight.get('status')}{detail}")


def _build_context(
    *,
    gainers,
    events_by_symbol,
    insight: dict[str, Any],
    report_date: date,
    threshold: float,
    lookback_days: int,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    per_stock = insight.get("per_stock") if isinstance(insight.get("per_stock"), dict) else {}
    for row in gainers.to_dict("records"):
        symbol = str(row.get("symbol_id"))
        events = events_by_symbol.get(symbol, [])
        rows.append(
            {
                **row,
                "events": [event_to_dict(event) for event in events],
                "events_count": len(events),
                "top_event": events[0].summary if events else "",
                "takeaway": per_stock.get(symbol, ""),
            }
        )
    return {
        "report_date": report_date,
        "threshold": threshold,
        "lookback_days": lookback_days,
        "gainers": rows,
        "gainers_count": len(rows),
        "summary_md": insight.get("summary_md") or "",
        "llm_status": insight.get("status"),
    }


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


if __name__ == "__main__":
    main()
