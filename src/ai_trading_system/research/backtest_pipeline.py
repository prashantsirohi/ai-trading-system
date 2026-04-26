"""Research backtest entrypoint using the static research data domain."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from ai_trading_system.analytics.backtester import EventBacktester
from ai_trading_system.analytics.rank_backtester import RankBacktester
from ai_trading_system.platform.db.paths import ensure_domain_layout, research_static_end_date
from ai_trading_system.platform.logging.logger import log_context, logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research backtesting pipeline")
    parser.add_argument("--mode", choices=["event", "rank"], default="rank")
    parser.add_argument("--from-date", help="Inclusive start date for research backtests")
    parser.add_argument("--to-date", help="Inclusive end date. Defaults to prior year end.")
    parser.add_argument("--event-type", default="BREAKOUT")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--rebalance-days", type=int, default=21)
    parser.add_argument("--benchmark-symbol", default="NIFTY_50")
    return parser


def load_benchmark_close_history(
    *,
    ohlcv_db_path: str | Path,
    benchmark_symbol: str,
    exchange: str = "NSE",
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    """Load benchmark close history from the catalog DB."""
    conn = duckdb.connect(str(ohlcv_db_path), read_only=True)
    try:
        clauses = ["symbol_id = ?", "exchange = ?"]
        params: list[object] = [benchmark_symbol, exchange]
        if from_date is not None:
            clauses.append("CAST(timestamp AS DATE) >= ?")
            params.append(pd.to_datetime(from_date).date())
        if to_date is not None:
            clauses.append("CAST(timestamp AS DATE) <= ?")
            params.append(pd.to_datetime(to_date).date())
        rows = conn.execute(
            f"""
            SELECT CAST(timestamp AS DATE) AS date, close
            FROM _catalog
            WHERE {' AND '.join(clauses)}
            ORDER BY date
            """,
            params,
        ).fetchdf()
    finally:
        conn.close()

    if rows.empty:
        return pd.DataFrame(columns=["date", "benchmark_close"])
    rows.loc[:, "date"] = pd.to_datetime(rows["date"])
    rows = rows.rename(columns={"close": "benchmark_close"})
    return rows.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)


def build_strategy_returns_from_equity_curve(equity_curve: pd.DataFrame) -> pd.DataFrame:
    """Convert a backtest equity curve into period returns for comparison."""
    if equity_curve is None or equity_curve.empty:
        return pd.DataFrame(columns=["date", "strategy_return"])
    date_col = "date" if "date" in equity_curve.columns else equity_curve.columns[0]
    capital_col = "capital" if "capital" in equity_curve.columns else "equity"
    if capital_col not in equity_curve.columns:
        return pd.DataFrame(columns=["date", "strategy_return"])

    curve = equity_curve[[date_col, capital_col]].copy()
    curve.loc[:, date_col] = pd.to_datetime(curve[date_col], errors="coerce")
    curve.loc[:, capital_col] = pd.to_numeric(curve[capital_col], errors="coerce")
    curve = curve.dropna(subset=[date_col, capital_col]).sort_values(date_col, kind="stable")
    if curve.empty:
        return pd.DataFrame(columns=["date", "strategy_return"])

    curve.loc[:, "strategy_return"] = curve[capital_col].pct_change(fill_method=None)
    returns = curve.dropna(subset=["strategy_return"])[[date_col, "strategy_return"]].rename(columns={date_col: "date"})
    return returns.reset_index(drop=True)


def compute_benchmark_comparison(
    strategy_returns: pd.DataFrame,
    benchmark_history: pd.DataFrame,
    *,
    benchmark_symbol: str,
) -> dict:
    """Align strategy and benchmark returns and compute benchmark-relative metrics."""
    if strategy_returns is None or strategy_returns.empty:
        return {
            "status": "strategy_returns_unavailable",
            "benchmark_symbol": benchmark_symbol,
            "observations": 0,
            "metrics": {},
        }
    if benchmark_history is None or benchmark_history.empty:
        return {
            "status": "benchmark_unavailable",
            "benchmark_symbol": benchmark_symbol,
            "observations": 0,
            "metrics": {},
        }

    strategy = strategy_returns[["date", "strategy_return"]].copy()
    strategy.loc[:, "date"] = pd.to_datetime(strategy["date"], errors="coerce")
    strategy.loc[:, "strategy_return"] = pd.to_numeric(strategy["strategy_return"], errors="coerce")
    benchmark = benchmark_history[["date", "benchmark_close"]].copy()
    benchmark.loc[:, "date"] = pd.to_datetime(benchmark["date"], errors="coerce")
    benchmark.loc[:, "benchmark_close"] = pd.to_numeric(benchmark["benchmark_close"], errors="coerce")
    benchmark.loc[:, "benchmark_return"] = benchmark["benchmark_close"].pct_change(fill_method=None)

    aligned = strategy.merge(
        benchmark[["date", "benchmark_return"]],
        on="date",
        how="inner",
    ).dropna(subset=["strategy_return", "benchmark_return"]).sort_values("date", kind="stable")
    observations = int(len(aligned))
    if observations < 2:
        return {
            "status": "insufficient_data",
            "benchmark_symbol": benchmark_symbol,
            "observations": observations,
            "metrics": {},
        }

    date_diffs = aligned["date"].diff().dropna().dt.days
    median_days = float(date_diffs.median()) if not date_diffs.empty else 1.0
    periods_per_year = 365.25 / max(median_days, 1.0)

    strategy_series = aligned["strategy_return"]
    benchmark_series = aligned["benchmark_return"]
    active_return = strategy_series - benchmark_series

    benchmark_var = float(benchmark_series.var(ddof=0))
    beta = float(strategy_series.cov(benchmark_series, ddof=0) / benchmark_var) if benchmark_var > 0 else float("nan")
    alpha = float((strategy_series.mean() - (beta * benchmark_series.mean())) * periods_per_year) if np.isfinite(beta) else float("nan")
    tracking_error = float(active_return.std(ddof=0) * np.sqrt(periods_per_year))
    information_ratio = (
        float(active_return.mean() / active_return.std(ddof=0) * np.sqrt(periods_per_year))
        if active_return.std(ddof=0) > 0
        else float("nan")
    )

    return {
        "status": "ok",
        "benchmark_symbol": benchmark_symbol,
        "observations": observations,
        "start_date": aligned["date"].min().date().isoformat(),
        "end_date": aligned["date"].max().date().isoformat(),
        "metrics": {
            "alpha": alpha,
            "beta": beta,
            "information_ratio": information_ratio,
            "tracking_error": tracking_error,
        },
    }


def main() -> None:
    args = build_parser().parse_args()
    project_root = Path(__file__).resolve().parents[3]
    paths = ensure_domain_layout(project_root=project_root, data_domain="research")
    to_date = args.to_date or research_static_end_date()
    from_date = args.from_date or f"{max(date.fromisoformat(to_date).year - 5, 2000)}-01-01"

    with log_context(run_id="research-backtest", stage_name=args.mode):
        logger.info("Starting research backtest mode=%s from=%s to=%s", args.mode, from_date, to_date)
        if args.mode == "event":
            backtester = EventBacktester(
                ohlcv_db_path=str(paths.ohlcv_db_path),
                feature_store_dir=str(paths.feature_store_dir),
                data_domain="research",
            )
            result = backtester.run_event_backtest(
                event_type=args.event_type,
                from_date=from_date,
                to_date=to_date,
            )
            logger.info("Research event backtest complete: %s", result.get("metrics", {}))
            return

        rank_backtester = RankBacktester(
            ohlcv_db_path=str(paths.ohlcv_db_path),
            feature_store_dir=str(paths.feature_store_dir),
            top_n=args.top_n,
            rebalance_days=args.rebalance_days,
            data_domain="research",
        )
        result = rank_backtester.quick_backtest(
            from_date=from_date,
            to_date=to_date,
        )
        strategy_returns = build_strategy_returns_from_equity_curve(result.get("equity_curve", pd.DataFrame()))
        benchmark_history = load_benchmark_close_history(
            ohlcv_db_path=paths.ohlcv_db_path,
            benchmark_symbol=args.benchmark_symbol,
            exchange="NSE",
            from_date=from_date,
            to_date=to_date,
        )
        benchmark_report = compute_benchmark_comparison(
            strategy_returns,
            benchmark_history,
            benchmark_symbol=args.benchmark_symbol,
        )
        logger.info(
            "Research rank backtest complete: metrics=%s benchmark=%s",
            result.get("metrics", {}),
            benchmark_report,
        )


if __name__ == "__main__":
    main()
