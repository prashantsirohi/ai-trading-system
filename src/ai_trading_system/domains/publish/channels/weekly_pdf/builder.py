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
from ai_trading_system.platform.db.paths import get_domain_paths
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
    patterns_best = metrics.best_patterns_by_symbol(data.pattern_scan)
    stage2_report_summary = metrics.stage2_summary_for_report(data.ranked_signals)
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
    events_of_week = _events_of_week(data)
    fund_value_tech = metrics.fund_value_tech_overlap(
        ranked=data.ranked_signals,
        watchlist=data.watchlist_candidates,
        quarterly=data.quarterly_result_scores,
        valuation=data.stock_valuation_bands_latest,
        patterns_best=patterns_best,
    )
    candidate_tracker_view = metrics.candidate_tracker_weekly_view(data.candidate_tracker_current)
    executive_panel = metrics.build_executive_decision_panel(
        ranked=data.ranked_signals,
        watchlist=data.watchlist_candidates,
        rank_improvers=rank_improvers,
        rank_decliners=rank_decliners,
        patterns_best=patterns_best,
        breadth_latest=breadth_latest,
        trust_status=data.trust_status,
    )
    sector_groups = metrics.split_sector_leadership(data.sector_dashboard)
    sector_rotation_summary = metrics.sector_rotation_summary(data.sector_rotation)
    sector_rotation_information = metrics.sector_rotation_information(data.sector_rotation)
    stock_rotation_groups = metrics.split_stock_rotation(data.stock_rotation)
    accumulation_distribution = metrics.accumulation_distribution_tables(data.accumulation_distribution)
    delivery_trends = metrics.delivery_trend_summary(data.accumulation_distribution)
    custom_indices = metrics.custom_indices_summary(data.sector_custom_indices, data.sector_rotation)

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
        universe_valuation=data.universe_valuation_daily,
        valuation_cycle=data.valuation_cycle_features,
        patterns_best=patterns_best,
        fund_value_tech_overlap=fund_value_tech,
        candidate_tracker_current=data.candidate_tracker_current,
    )
    latest_universe = _latest_by_date(data.universe_valuation_daily, "date")
    latest_valuation_cycle = _latest_by_date(data.valuation_cycle_features, "date")
    top_sector_earnings = _sort_desc(data.sector_earnings_leadership, "sector_fundamental_score").head(12)
    clean_great_results, low_base_results = metrics.split_fundamental_results(data.great_results)
    great_results = _sort_desc(data.great_results, "insight_score").head(12)
    clean_great_results = _sort_desc(clean_great_results, "insight_score").head(12)
    low_base_results = _sort_desc(low_base_results, "insight_score").head(12)
    turnarounds = _sort_desc(data.turnaround_candidates, "insight_score").head(12)
    compounders = _sort_desc(data.compounder_candidates, "insight_score").head(12)
    valuation_source = latest_valuation_cycle if not latest_valuation_cycle.empty else latest_universe
    valuation_interpretation = metrics.valuation_cycle_interpretation(valuation_source)
    empty_sections = _empty_sections(
        volume_delivery=volume_delivery,
        volume_shockers=volume_shockers,
        tier_a=tier_a,
        tier_b=tier_b,
        failed_breakouts=failed_breakouts,
        fund_value_tech=fund_value_tech,
    )

    template_context = {
        "week_ending": data.run_date,
        "run_date": data.run_date,
        "run_id": data.run_id,
        "regime": regime,
        "stage2_report_summary": stage2_report_summary,
        "executive_panel": executive_panel,
        "sectors": _df_to_records(sectors),
        "sector_groups": {key: _df_to_records(value) for key, value in sector_groups.items()},
        "sector_rotation_summary": _df_to_records(sector_rotation_summary),
        "sector_rotation_information": _df_to_records(sector_rotation_information),
        "stock_rotation_groups": {key: _df_to_records(value) for key, value in stock_rotation_groups.items()},
        "accumulation_distribution": {key: _df_to_records(value) for key, value in accumulation_distribution.items()},
        "delivery_trends": _df_to_records(delivery_trends),
        "custom_indices": _df_to_records(custom_indices),
        "sector_rotation_payload": data.sector_rotation_payload,
        "top_ranked": _df_to_records(top_ranked),
        "volume_delivery": _df_to_records(volume_delivery),
        "weekly_price": _df_to_records(weekly_price),
        "volume_shockers": _df_to_records(volume_shockers),
        "tier_a": _df_to_records(tier_a),
        "tier_b": _df_to_records(tier_b),
        "patterns": _df_to_records(patterns_best),
        "patterns_detailed": _df_to_records(patterns),
        "fund_value_tech_overlap": _df_to_records(fund_value_tech),
        "candidate_tracker_view": {key: _df_to_records(value) for key, value in candidate_tracker_view.items()},
        "candidate_tracker_enabled": not data.candidate_tracker_current.empty,
        "prior_run_id": data.prior_run_id,
        "prior_run_date": data.prior_run_date,
        "rank_improvers": _df_to_records(rank_improvers),
        "rank_decliners": _df_to_records(rank_decliners),
        "sector_movers": _df_to_records(sector_movers),
        "failed_breakouts": _df_to_records(failed_breakouts),
        "breadth_latest": breadth_latest,
        "breadth_rows": _df_to_records(data.market_breadth.tail(10)) if not data.market_breadth.empty else [],
        "events_of_week": events_of_week,
        "charts": chart_paths,
        "fundamental_summary": data.fundamental_dashboard_payload.get("summary", {}),
        "fundamental_universe": _df_to_records(latest_universe.head(1)),
        "great_results": _df_to_records(great_results),
        "clean_great_results": _df_to_records(clean_great_results),
        "low_base_results": _df_to_records(low_base_results),
        "turnarounds": _df_to_records(turnarounds),
        "compounders": _df_to_records(compounders),
        "sector_earnings": _df_to_records(top_sector_earnings),
        "sector_valuation": _df_to_records(_latest_by_date(data.sector_valuation_daily, "date").head(20)),
        "valuation_cycle": _df_to_records(latest_valuation_cycle.head(20)),
        "valuation_interpretation": valuation_interpretation,
        "empty_sections": empty_sections,
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
            "weekly_patterns": data.pattern_scan,
            "weekly_patterns_best_by_symbol": patterns_best,
            "weekly_rank_improvers": rank_improvers,
            "weekly_rank_decliners": rank_decliners,
            "weekly_sector_movers": sector_movers,
            "weekly_sector_fresh_leaders": sector_groups["fresh_leaders"],
            "weekly_sector_improving": sector_groups["improving_sectors"],
            "weekly_sector_weakening_leaders": sector_groups["weakening_leaders"],
            "weekly_sector_rotation_summary": sector_rotation_summary,
            "weekly_sector_rotation_information": sector_rotation_information,
            "weekly_stock_rotation_improving": stock_rotation_groups["improving"],
            "weekly_stock_rotation_leading": stock_rotation_groups["leading"],
            "weekly_stock_rotation_lagging": stock_rotation_groups["lagging"],
            "weekly_stock_rotation_weakening": stock_rotation_groups["weakening"],
            "weekly_accumulation": accumulation_distribution["accumulation"],
            "weekly_distribution": accumulation_distribution["distribution"],
            "weekly_delivery_trends": delivery_trends,
            "weekly_custom_indices": custom_indices,
            "weekly_failed_breakouts": failed_breakouts,
            "weekly_fund_value_tech_overlap": fund_value_tech,
            "weekly_candidate_tracker_current": data.candidate_tracker_current,
            "market_breadth": data.market_breadth,
            "fundamental_great_results": great_results,
            "fundamental_clean_great_results": clean_great_results,
            "fundamental_low_base_caution": low_base_results,
            "fundamental_turnarounds": turnarounds,
            "fundamental_compounders": compounders,
            "fundamental_sector_earnings": top_sector_earnings,
            "fundamental_sector_valuation": data.sector_valuation_daily,
            "fundamental_universe_valuation": data.universe_valuation_daily,
            "fundamental_valuation_cycle": data.valuation_cycle_features,
        },
    )

    manifest = {
        "report_id": f"weekly_pdf-{data.run_id}",
        "week_ending": data.run_date,
        "run_id": data.run_id,
        "trust_status": data.trust_status,
        "regime": regime,
        "stage2_report_summary": stage2_report_summary,
        "executive_panel": executive_panel,
        "empty_sections": empty_sections,
        "valuation_interpretation": valuation_interpretation,
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
            "patterns_best_by_symbol": len(patterns_best),
            "rank_improvers": len(rank_improvers),
            "rank_decliners": len(rank_decliners),
            "sector_movers": len(sector_movers),
            "failed_breakouts": len(failed_breakouts),
            "fund_value_tech_overlap": len(fund_value_tech),
            "candidate_tracker_rows": len(data.candidate_tracker_current),
            "breadth_rows": int(len(data.market_breadth)),
            "events": int(events_of_week.get("headline_count", 0)),
            "great_results": int(len(great_results)),
            "clean_great_results": int(len(clean_great_results)),
            "low_base_caution_results": int(len(low_base_results)),
            "turnarounds": int(len(turnarounds)),
            "compounders": int(len(compounders)),
            "sector_earnings": int(len(top_sector_earnings)),
            "valuation_cycle": int(len(data.valuation_cycle_features)),
            "sector_rotation": int(len(sector_rotation_summary)),
            "stock_rotation": int(sum(len(frame) for frame in stock_rotation_groups.values())),
            "accumulation": int(len(accumulation_distribution["accumulation"])),
            "distribution": int(len(accumulation_distribution["distribution"])),
            "custom_indices": int(len(custom_indices)),
        },
        "prior_run_id": data.prior_run_id,
        "prior_run_date": data.prior_run_date,
        "breadth_latest": breadth_latest,
        "events_of_week": events_of_week,
        "charts": chart_paths,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }

    json_path = output_dir / "weekly_market_report.json"
    json_path.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    manifest["json_path"] = str(json_path)
    return manifest


def _events_of_week(data: WeeklyReportData) -> dict[str, Any]:
    events = list((data.market_events_snapshot or {}).get("events") or [])
    severity_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    rows = []
    for row in events:
        severity = "high" if row.get("tier") == "A" or row.get("materiality_label") in {"high", "critical"} else "medium"
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        category = str(row.get("category") or "unknown")
        category_counts[category] = category_counts.get(category, 0) + 1
        rows.append({
            "symbol": row.get("symbol"),
            "severity": severity,
            "category": category,
            "headline": row.get("title") or row.get("summary"),
            "materiality_label": row.get("materiality_label"),
            "freshness_days": row.get("freshness_days"),
        })
    rows.sort(key=lambda r: (r["severity"] == "high", str(r.get("headline") or "")), reverse=True)
    return {
        "headline_count": len(events),
        "by_severity": severity_counts,
        "by_category": category_counts,
        "top_events": rows[:10],
    }


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
    universe_valuation: pd.DataFrame,
    valuation_cycle: pd.DataFrame,
    patterns_best: pd.DataFrame,
    fund_value_tech_overlap: pd.DataFrame,
    candidate_tracker_current: pd.DataFrame,
) -> Dict[str, Any]:
    """Generate all charts. Each is independently optional."""
    chart_dir = output_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)

    out: Dict[str, Any] = {"breadth": None, "sectors": None, "movers": None, "stocks": [], "valuation_cycle": None}

    breadth_path = charts.breadth_chart(breadth, chart_dir / "breadth_above_smas.png")
    if breadth_path is not None:
        out["breadth"] = "charts/" + breadth_path.name

    sector_path = charts.sector_rs_bars(sectors_full, chart_dir / "sector_rs.png")
    if sector_path is not None:
        out["sectors"] = "charts/" + sector_path.name

    mover_path = charts.rank_mover_bars(improvers, decliners, chart_dir / "rank_movers.png")
    if mover_path is not None:
        out["movers"] = "charts/" + mover_path.name

    valuation_path = charts.universe_valuation_cycle(
        universe_valuation,
        valuation_cycle,
        chart_dir / "universe_valuation_cycle.png",
    )
    if valuation_path is not None:
        out["valuation_cycle"] = "charts/" + valuation_path.name

    if project_root is None:
        return out
    ohlcv_path = get_domain_paths(project_root=project_root, data_domain="operational").ohlcv_db_path
    if not ohlcv_path.exists():
        return out
    end_date = _safe_date(run_date)
    if end_date is None:
        return out

    targets = charts.pick_candle_targets(
        ranked,
        improvers,
        breakouts,
        patterns_best=patterns_best,
        fund_value_tech_overlap=fund_value_tech_overlap,
        candidate_tracker_current=candidate_tracker_current,
    )
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


def _sort_desc(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    if column not in frame.columns:
        return frame.copy()
    return frame.sort_values(column, ascending=False, na_position="last")


def _latest_by_date(frame: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if frame is None or frame.empty or date_col not in frame.columns:
        return pd.DataFrame()
    df = frame.copy()
    df.loc[:, date_col] = pd.to_datetime(df[date_col], errors="coerce")
    latest = df[date_col].max()
    if pd.isna(latest):
        return df
    return df[df[date_col].eq(latest)]


def _safe_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _empty_sections(
    *,
    volume_delivery: pd.DataFrame,
    volume_shockers: pd.DataFrame,
    tier_a: pd.DataFrame,
    tier_b: pd.DataFrame,
    failed_breakouts: pd.DataFrame,
    fund_value_tech: pd.DataFrame,
) -> Dict[str, str]:
    reasons = {
        "weekly_volume_delivery_movers": "No stock met return_5 >= 5%, delivery >= 40%, and volume expansion rule.",
        "weekly_unusual_volume_shockers": "No stock met high-delivery + unusual-volume + non-negative return rule.",
        "weekly_breakouts_tier_a": "No Tier-A breakout candidates in this run.",
        "weekly_breakouts_tier_b": "No Tier-B breakout candidates in this run.",
        "weekly_failed_breakouts": "No failed breakout detected in the 10-day lookback.",
        "fund_value_tech_overlap": "No overlap between fundamental result winners, valuation support, and technical confirmation.",
    }
    frames = {
        "weekly_volume_delivery_movers": volume_delivery,
        "weekly_unusual_volume_shockers": volume_shockers,
        "weekly_breakouts_tier_a": tier_a,
        "weekly_breakouts_tier_b": tier_b,
        "weekly_failed_breakouts": failed_breakouts,
        "fund_value_tech_overlap": fund_value_tech,
    }
    return {key: reasons[key] for key, frame in frames.items() if frame is None or frame.empty}
