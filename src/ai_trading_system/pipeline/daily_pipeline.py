"""
Daily Pipeline wrapper.

This keeps the historical entrypoint while delegating execution to the
resilient pipeline orchestrator:
1. ingest
2. features
3. rank
4. execute
5. publish

Usage:
    python -m ai_trading_system.pipeline.daily_pipeline
    python -m ai_trading_system.pipeline.daily_pipeline --force
    python -m ai_trading_system.pipeline.daily_pipeline --local-publish
"""

import os
import sys
from datetime import datetime, timedelta

from ai_trading_system.platform.utils.bootstrap import ensure_project_root_on_path
project_root = str(ensure_project_root_on_path(__file__))
from ai_trading_system.platform.utils.env import load_project_env
from ai_trading_system.platform.logging.logger import logger
from ai_trading_system.pipeline.orchestrator import PipelineOrchestrator
from ai_trading_system.platform.utils.data_config import (
    should_truncate_data,
    truncate_old_data,
    data_retention_years,
)
import sqlite3

load_project_env(project_root)

logger.info(f"Environment: {os.getenv('ENV', 'local')}")
logger.info(
    f"Data retention: {data_retention_years() if data_retention_years() else 'All'} years"
)


def is_trading_holiday(date: datetime = None) -> bool:
    """Check if given date is a trading holiday."""
    if date is None:
        date = datetime.now()

    date_str = date.strftime("%Y-%m-%d")

    conn = sqlite3.connect(os.path.join(project_root, "data", "masterdata.db"))
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM nse_holidays WHERE date = ?", (date_str,))
    count = cursor.fetchone()[0]
    conn.close()

    return count > 0


def is_weekend(date: datetime = None) -> bool:
    """Check if given date is Saturday (5) or Sunday (6)."""
    if date is None:
        date = datetime.now()
    return date.weekday() >= 5


def run_portfolio_analysis():
    """Run portfolio analysis from Google Sheets."""
    try:
        from ai_trading_system.domains.publish.portfolio_analyzer import Portfolio, PortfolioManager
        from ai_trading_system.domains.publish.channels.google_sheets import GoogleSheetsManager
        import sqlite3
        import duckdb
        from ai_trading_system.domains.ingest.providers.yfinance import YFinanceCollector

        logger.info("Running portfolio analysis...")

        gs = GoogleSheetsManager()
        if not gs.open_spreadsheet():
            raise RuntimeError(gs.last_error or "Unable to open Google spreadsheet")

        portfolio = Portfolio(name="My Portfolio", initial_cash=0)
        data = []
        price_map = {}

        # Load sector map from stock_details (Sector column)
        conn = sqlite3.connect("data/masterdata.db")
        rows = conn.execute("SELECT Symbol, Sector FROM stock_details").fetchall()
        sector_map = {sym: sector for sym, sector in rows if sector}
        conn.close()

        # Load RS data
        portfolio._sector_map = sector_map
        portfolio.load_rs_data()

        # Read portfolio from Google Sheets
        try:
            ws = gs.get_worksheet("PORTFOLIO")
            if ws:
                # Use get_all_values to read raw data (first 3 columns only)
                values = ws.get_all_values()
                if values and len(values) > 1:
                    data = []
                    for row in values[1:]:
                        if row and len(row) >= 3:
                            try:
                                symbol = str(row[0]).strip()
                                qty = float(row[1]) if row[1] else 0
                                avg_price = float(row[2]) if row[2] else 0
                                # Skip summary rows
                                if (
                                    symbol.lower() in ["total", "summary", ""]
                                    or "positions" in symbol.lower()
                                ):
                                    continue
                                data.append(
                                    {
                                        "Symbol": symbol,
                                        "Qty": qty,
                                        "Avg Price": avg_price,
                                    }
                                )
                            except (ValueError, TypeError):
                                continue
                    logger.info(f"Loaded {len(data)} positions from PORTFOLIO sheet")

                    # Get current prices from Yahoo Finance (more reliable than Dhan)
                    symbols = [d["Symbol"] for d in data if d.get("Symbol")]
                    if symbols:
                        logger.info(f"Fetching current prices from Yahoo Finance...")
                        yfc = YFinanceCollector()
                        price_map = yfc.get_latest_prices(symbols)
                        logger.info(f"Got prices for {len(price_map)} symbols")

                # Add positions
                for d in data:
                    symbol = str(d.get("Symbol", "")).strip()
                    qty = float(d.get("Qty", 0) or 0)
                    avg_price = float(d.get("Avg Price", 0) or 0)
                    if symbol and qty > 0:
                        sector = sector_map.get(symbol, "Other")
                        portfolio.add_position(symbol, qty, avg_price, sector=sector)
                        if symbol in price_map:
                            portfolio.update_position_price(symbol, price_map[symbol])

                logger.info(f"Portfolio loaded: {len(portfolio.positions)} positions")

                # Save portfolio with current prices
                pm = PortfolioManager()
                if not pm.sheets_client:
                    raise RuntimeError("Portfolio manager could not authenticate with Google Sheets")
                saved_portfolio = pm.save_portfolio_to_sheet(portfolio, "PORTFOLIO")
                saved_swot = pm.save_swot_analysis(portfolio, "Portfolio Analysis")
                if not saved_portfolio or not saved_swot:
                    raise RuntimeError("Portfolio or SWOT write failed")
                logger.info("Portfolio and SWOT analysis saved to Google Sheets")
            else:
                logger.info("No PORTFOLIO sheet found, skipping analysis")
        except Exception as e:
            logger.warning(f"Could not read PORTFOLIO sheet: {e}")
            raise

        logger.info("Portfolio analysis complete")
        return {
            "ok": True,
            "positions": len(portfolio.positions),
            "source_rows": len(data),
        }
    except Exception as e:
        logger.warning(f"Portfolio analysis skipped: {e}")
        return {
            "ok": False,
            "error": str(e),
        }


def main(
    force: bool = False,
    local_publish: bool = False,
    smoke: bool = False,
    stages: str = "ingest,features,rank,execute,publish",
    canary: bool = False,
    symbol_limit: int | None = None,
    skip_preflight: bool = False,
    skip_publish_network_checks: bool = False,
    data_domain: str = "operational",
    include_delivery: bool = True,
    publish_quantstats: bool = True,
    quantstats_top_n: int = 20,
    quantstats_min_overlap: int = 5,
    quantstats_max_runs: int = 240,
    quantstats_write_core_html: bool = False,
    validate_bhavcopy_after_ingest: bool = True,
    bhavcopy_validation_date: str | None = None,
    bhavcopy_validation_csv: str | None = None,
    bhavcopy_validation_source: str = "bhavcopy",
    bhavcopy_min_coverage: float = 0.9,
    bhavcopy_max_mismatch_ratio: float = 0.05,
    bhavcopy_close_tolerance_pct: float = 0.01,
    dq_max_unknown_provider_pct: float = 0.0,
    dq_max_unresolved_dates: int = 1,
    dq_max_unresolved_symbol_dates: int = 10,
    dq_max_unresolved_symbol_ratio_pct: float = 1.0,
    dq_features_max_quarantined_symbols: int = 10,
    dq_features_max_quarantined_symbol_ratio_pct: float = 1.0,
    breakout_engine: str = "v2",
    breakout_include_legacy_families: bool = True,
    breakout_market_bias_allowlist: str = "BULLISH,NEUTRAL",
    breakout_min_breadth_score: float = 45.0,
    breakout_sector_rs_min: float | None = None,
    breakout_sector_rs_percentile_min: float | None = 60.0,
    breakout_qualified_min_score: int = 3,
    breakout_symbol_near_high_max_pct: float = 15.0,
    breakout_symbol_trend_gate_enabled: bool = True,
    execution_breakout_linkage: str = "off",
):
    if smoke:
        raise RuntimeError("Smoke mode has been removed because synthetic pipeline data is no longer allowed.")

    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    effective_validation_date = bhavcopy_validation_date or (now - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info(f"DAILY PIPELINE - {today}")
    logger.info("=" * 60)

    # Truncate old data if running in github/prod mode
    if should_truncate_data(data_domain):
        truncate_old_data(data_domain=data_domain)

    if not force:
        if is_trading_holiday(now):
            logger.info(f"⛔ {today} is a trading holiday. Exiting.")
            return

        if is_weekend(now):
            day_name = now.strftime("%A")
            logger.info(f"⛔ {today} is {day_name}. Weekend - exiting.")
            return

    orchestrator = PipelineOrchestrator(project_root)
    result = orchestrator.run_pipeline(
        stage_names=(
            ["ingest", "features", "rank"]
            if canary and stages == "ingest,features,rank,execute,publish"
            else [stage.strip() for stage in stages.split(",") if stage.strip()]
        ),
        run_date=today,
        params={
            "force": force,
            "batch_size": 700,
            "bulk": False,
            "nse_primary": True,
            "local_publish": local_publish,
            "smoke": smoke,
            "canary": canary,
            "symbol_limit": symbol_limit if symbol_limit is not None else (25 if canary else None),
            "preflight": not skip_preflight,
            "preflight_publish_network_checks": not skip_publish_network_checks,
            "data_domain": data_domain,
            "include_delivery": include_delivery,
            "publish_quantstats": publish_quantstats,
            "quantstats_top_n": quantstats_top_n,
            "quantstats_min_overlap": quantstats_min_overlap,
            "quantstats_max_runs": quantstats_max_runs,
            "quantstats_write_core_html": quantstats_write_core_html,
            "validate_bhavcopy_after_ingest": validate_bhavcopy_after_ingest,
            "bhavcopy_validation_required": validate_bhavcopy_after_ingest,
            "bhavcopy_validation_date": effective_validation_date,
            "bhavcopy_validation_csv": bhavcopy_validation_csv,
            "bhavcopy_validation_source": bhavcopy_validation_source,
            "bhavcopy_min_coverage": bhavcopy_min_coverage,
            "bhavcopy_max_mismatch_ratio": bhavcopy_max_mismatch_ratio,
            "bhavcopy_close_tolerance_pct": bhavcopy_close_tolerance_pct,
            "dq_max_unknown_provider_pct": dq_max_unknown_provider_pct,
            "dq_max_unresolved_dates": dq_max_unresolved_dates,
            "dq_max_unresolved_symbol_dates": dq_max_unresolved_symbol_dates,
            "dq_max_unresolved_symbol_ratio_pct": dq_max_unresolved_symbol_ratio_pct,
            "dq_features_max_quarantined_symbols": dq_features_max_quarantined_symbols,
            "dq_features_max_quarantined_symbol_ratio_pct": dq_features_max_quarantined_symbol_ratio_pct,
            "breakout_engine": breakout_engine,
            "breakout_include_legacy_families": breakout_include_legacy_families,
            "breakout_market_bias_allowlist": breakout_market_bias_allowlist,
            "breakout_min_breadth_score": breakout_min_breadth_score,
            "breakout_sector_rs_min": breakout_sector_rs_min,
            "breakout_sector_rs_percentile_min": breakout_sector_rs_percentile_min,
            "breakout_qualified_min_score": breakout_qualified_min_score,
            "breakout_symbol_near_high_max_pct": breakout_symbol_near_high_max_pct,
            "breakout_symbol_trend_gate_enabled": breakout_symbol_trend_gate_enabled,
            "execution_breakout_linkage": execution_breakout_linkage,
        },
    )

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE - run_id={result['run_id']} status={result['status']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force", action="store_true", help="Skip holiday/weekend checks"
    )
    parser.add_argument(
        "--local-publish",
        action="store_true",
        help="Skip networked Telegram/Google Sheets delivery and write a local publish summary instead",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Deprecated. Smoke mode is disabled because synthetic data is not allowed.",
    )
    parser.add_argument(
        "--stages",
        default="ingest,features,rank,execute,publish",
        help="Comma-separated stage list. Example: publish",
    )
    parser.add_argument(
        "--canary",
        action="store_true",
        help="Run a limited real canary flow with a smaller live symbol universe.",
    )
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=None,
        help="Limit live symbol universe size for canary runs.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip local readiness checks before running live stages.",
    )
    parser.add_argument(
        "--skip-publish-network-checks",
        action="store_true",
        help="Skip preflight DNS checks for Telegram/Google publish endpoints.",
    )
    parser.add_argument(
        "--skip-delivery-collect",
        action="store_true",
        help="Skip ingest-stage delivery collection (enabled by default).",
    )
    parser.add_argument(
        "--skip-quantstats",
        action="store_true",
        help="Disable QuantStats dashboard tear sheet generation in publish stage.",
    )
    parser.add_argument(
        "--publish-quantstats",
        action="store_true",
        help="Legacy alias (QuantStats publish is enabled by default).",
    )
    parser.add_argument(
        "--quantstats-top-n",
        type=int,
        default=20,
        help="Top-N ranked symbols used for dashboard tear sheet returns.",
    )
    parser.add_argument(
        "--quantstats-min-overlap",
        type=int,
        default=5,
        help="Minimum symbol overlap between consecutive rank runs.",
    )
    parser.add_argument(
        "--quantstats-max-runs",
        type=int,
        default=240,
        help="Maximum rank runs to inspect for building the tear sheet return stream.",
    )
    parser.add_argument(
        "--quantstats-write-core-html",
        action="store_true",
        help="Also write raw QuantStats core HTML alongside the enriched dashboard tear sheet.",
    )
    parser.add_argument(
        "--disable-bhavcopy-validation",
        action="store_true",
        help="Disable post-ingest bhavcopy validation gate (enabled by default).",
    )
    parser.add_argument(
        "--bhavcopy-validation-date",
        default=None,
        help="Override validation date (YYYY-MM-DD). Defaults to today-1 in local time.",
    )
    parser.add_argument(
        "--bhavcopy-validation-csv",
        default=None,
        help="Optional local bhavcopy CSV path used for post-ingest validation.",
    )
    parser.add_argument(
        "--bhavcopy-validation-source",
        choices=["auto", "bhavcopy", "yfinance"],
        default="bhavcopy",
        help="Reference source for post-ingest validation (default: bhavcopy).",
    )
    parser.add_argument(
        "--bhavcopy-min-coverage",
        type=float,
        default=0.9,
        help="Minimum catalog-vs-bhavcopy coverage ratio required to continue downstream stages.",
    )
    parser.add_argument(
        "--bhavcopy-max-mismatch-ratio",
        type=float,
        default=0.05,
        help="Maximum allowed mismatch ratio vs bhavcopy before pipeline is blocked.",
    )
    parser.add_argument(
        "--bhavcopy-close-tolerance-pct",
        type=float,
        default=0.01,
        help="Relative close-price tolerance used by bhavcopy mismatch detection.",
    )
    parser.add_argument(
        "--dq-max-unknown-provider-pct",
        type=float,
        default=0.0,
        help="Maximum allowed unknown-provider percentage before ingest DQ is blocked.",
    )
    parser.add_argument(
        "--dq-max-unresolved-dates",
        type=int,
        default=1,
        help="Maximum unresolved trade dates tolerated before ingest DQ blocks downstream stages.",
    )
    parser.add_argument(
        "--dq-max-unresolved-symbol-dates",
        type=int,
        default=10,
        help="Maximum unresolved symbol-date pairs tolerated before ingest DQ blocks downstream stages.",
    )
    parser.add_argument(
        "--dq-max-unresolved-symbol-ratio-pct",
        type=float,
        default=1.0,
        help="Maximum unresolved symbol-date ratio (percent of eligible symbols) tolerated before ingest DQ blocks downstream stages.",
    )
    parser.add_argument(
        "--dq-features-max-quarantined-symbols",
        type=int,
        default=10,
        help="Maximum active quarantined symbols tolerated in features trust window before blocking.",
    )
    parser.add_argument(
        "--dq-features-max-quarantined-symbol-ratio-pct",
        type=float,
        default=1.0,
        help="Maximum active quarantined symbol ratio (percent of latest universe) tolerated in features trust window before blocking.",
    )
    parser.add_argument(
        "--breakout-engine",
        choices=["legacy", "v2"],
        default="v2",
        help="Breakout scanner engine mode.",
    )
    parser.add_argument(
        "--disable-breakout-legacy-families",
        action="store_true",
        help="When using breakout-v2, exclude mapped legacy setup families from results.",
    )
    parser.add_argument(
        "--breakout-market-bias-allowlist",
        default="BULLISH,NEUTRAL",
        help="Comma-separated market bias values allowed for qualified breakout states.",
    )
    parser.add_argument(
        "--breakout-min-breadth-score",
        type=float,
        default=45.0,
        help="Minimum breadth score required for breakout qualification.",
    )
    parser.add_argument(
        "--breakout-sector-rs-min",
        type=float,
        default=None,
        help="Optional absolute minimum sector RS value required for breakout qualification.",
    )
    parser.add_argument(
        "--breakout-sector-rs-percentile-min",
        type=float,
        default=60.0,
        help="Minimum sector RS percentile required for breakout qualification.",
    )
    parser.add_argument(
        "--breakout-qualified-min-score",
        type=int,
        default=3,
        help="Minimum breakout score needed to mark a breakout as qualified.",
    )
    parser.add_argument(
        "--breakout-symbol-near-high-max-pct",
        type=float,
        default=15.0,
        help="Maximum allowed distance from 52W high (%%) for Tier-A symbol trend qualification.",
    )
    parser.add_argument(
        "--disable-breakout-symbol-trend-gate",
        action="store_true",
        help="Disable symbol-level trend tier gate for breakout states.",
    )
    parser.add_argument(
        "--execution-breakout-linkage",
        choices=["off", "soft_gate"],
        default="off",
        help="Execution linkage mode for breakout signals.",
    )
    parser.add_argument(
        "--data-domain",
        choices=["operational", "research"],
        default="operational",
        help="Select the storage domain for this wrapper.",
    )
    args = parser.parse_args()

    main(
        force=args.force,
        local_publish=args.local_publish,
        smoke=args.smoke,
        stages=args.stages,
        canary=args.canary,
        symbol_limit=args.symbol_limit,
        skip_preflight=args.skip_preflight,
        skip_publish_network_checks=args.skip_publish_network_checks,
        data_domain=args.data_domain,
        include_delivery=not args.skip_delivery_collect,
        publish_quantstats=not args.skip_quantstats,
        quantstats_top_n=args.quantstats_top_n,
        quantstats_min_overlap=args.quantstats_min_overlap,
        quantstats_max_runs=args.quantstats_max_runs,
        quantstats_write_core_html=args.quantstats_write_core_html,
        validate_bhavcopy_after_ingest=not args.disable_bhavcopy_validation,
        bhavcopy_validation_date=args.bhavcopy_validation_date,
        bhavcopy_validation_csv=args.bhavcopy_validation_csv,
        bhavcopy_validation_source=args.bhavcopy_validation_source,
        bhavcopy_min_coverage=args.bhavcopy_min_coverage,
        bhavcopy_max_mismatch_ratio=args.bhavcopy_max_mismatch_ratio,
        bhavcopy_close_tolerance_pct=args.bhavcopy_close_tolerance_pct,
        dq_max_unknown_provider_pct=args.dq_max_unknown_provider_pct,
        dq_max_unresolved_dates=args.dq_max_unresolved_dates,
        dq_max_unresolved_symbol_dates=args.dq_max_unresolved_symbol_dates,
        dq_max_unresolved_symbol_ratio_pct=args.dq_max_unresolved_symbol_ratio_pct,
        dq_features_max_quarantined_symbols=args.dq_features_max_quarantined_symbols,
        dq_features_max_quarantined_symbol_ratio_pct=args.dq_features_max_quarantined_symbol_ratio_pct,
        breakout_engine=args.breakout_engine,
        breakout_include_legacy_families=not args.disable_breakout_legacy_families,
        breakout_market_bias_allowlist=args.breakout_market_bias_allowlist,
        breakout_min_breadth_score=args.breakout_min_breadth_score,
        breakout_sector_rs_min=args.breakout_sector_rs_min,
        breakout_sector_rs_percentile_min=args.breakout_sector_rs_percentile_min,
        breakout_qualified_min_score=args.breakout_qualified_min_score,
        breakout_symbol_near_high_max_pct=args.breakout_symbol_near_high_max_pct,
        breakout_symbol_trend_gate_enabled=not args.disable_breakout_symbol_trend_gate,
        execution_breakout_linkage=args.execution_breakout_linkage,
    )
