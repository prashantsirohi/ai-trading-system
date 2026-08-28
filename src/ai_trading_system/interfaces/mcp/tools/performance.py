"""Realized market, ranked-cohort, and persisted strategy backtest evidence."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from ai_trading_system.interfaces.mcp.context import McpContext
from ai_trading_system.interfaces.mcp.envelope import (
    AS_OF_EXACT,
    AS_OF_LATEST,
    AS_OF_NO_DATA,
    clamp_limit,
    coerce_date,
    envelope,
    json_safe,
)

_PERIOD_DAYS = {"week": 7, "month": 30, "quarter": 90, "year": 365}
_HORIZONS = {5, 10, 20, 60}


def _records(cursor: Any) -> list[dict[str, Any]]:
    names = [item[0] for item in (cursor.description or [])]
    return [
        {name: json_safe(value) for name, value in zip(names, row, strict=True)}
        for row in cursor.fetchall()
    ]


def _exists(conn: Any, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name=?", [table]
    ).fetchone() is not None


def _period_start(period: str, end: date) -> date:
    key = str(period).strip().lower()
    if key == "ytd":
        return date(end.year, 1, 1)
    if key not in _PERIOD_DAYS:
        raise ValueError("period must be week, month, quarter, ytd, or year")
    return end - timedelta(days=_PERIOD_DAYS[key])


def _horizon(value: int) -> int:
    horizon = int(value)
    if horizon not in _HORIZONS:
        raise ValueError("horizon_days must be one of 5, 10, 20, or 60")
    return horizon


def _status(as_of: str | date | None, has_data: bool) -> str:
    return AS_OF_LATEST if as_of is None else (AS_OF_EXACT if has_data else AS_OF_NO_DATA)


def get_market_winners(
    ctx: McpContext,
    *,
    period: str = "month",
    exchange: str = "NSE",
    as_of: str | date | None = None,
    min_return_pct: float | None = None,
    limit: int | None = 50,
) -> dict[str, Any]:
    """Rank realized adjusted-price returns; this is hindsight, not a strategy result."""

    cutoff = coerce_date(as_of)
    if as_of is not None and cutoff is None:
        raise ValueError("as_of must be an ISO date")
    exchange_code = ctx.resolve_exchange(exchange)
    bounded = clamp_limit(limit, default=50, maximum=500)
    with ctx.ohlcv(adjusted_view=True) as conn:
        effective_raw = conn.execute(
            """SELECT MAX(CAST(timestamp AS DATE)) FROM _catalog_feature_source
               WHERE exchange=? AND (? IS NULL OR CAST(timestamp AS DATE)<=?)""",
            [exchange_code, cutoff, cutoff],
        ).fetchone()[0]
        effective = coerce_date(effective_raw)
        if effective is None:
            rows: list[dict[str, Any]] = []
            start = cutoff
        else:
            start = _period_start(period, effective)
            rows = _records(conn.execute(
                """WITH returns AS (
                     SELECT symbol_id,exchange,
                            MIN(CAST(timestamp AS DATE)) AS start_date,
                            MAX(CAST(timestamp AS DATE)) AS end_date,
                            arg_min(close,CAST(timestamp AS DATE)) AS start_close,
                            arg_max(close,CAST(timestamp AS DATE)) AS end_close,
                            COUNT(DISTINCT CAST(timestamp AS DATE)) AS observed_sessions
                     FROM _catalog_feature_source
                     WHERE exchange=? AND CAST(timestamp AS DATE) BETWEEN ? AND ?
                       AND close IS NOT NULL AND close>0
                     GROUP BY symbol_id,exchange
                     HAVING COUNT(DISTINCT CAST(timestamp AS DATE))>=2
                   )
                   SELECT symbol_id,exchange,start_date,end_date,start_close,end_close,
                          observed_sessions,
                          ((end_close/start_close)-1.0)*100.0 AS return_pct
                   FROM returns
                   WHERE (? IS NULL OR ((end_close/start_close)-1.0)*100.0>=?)
                   ORDER BY return_pct DESC,symbol_id LIMIT ?""",
                [exchange_code, start, effective, min_return_pct, min_return_pct, bounded + 1],
            ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    return envelope(
        rows,
        source="ohlcv.duckdb:_catalog_feature_source",
        as_of_status=_status(as_of, bool(rows)),
        as_of_requested=as_of,
        as_of_effective=effective,
        date_fields=("start_date", "end_date"),
        period=period,
        period_start=start,
        exchange=exchange_code,
        price_basis="adjusted",
        evidence_type="realized_market_return_hindsight",
        truncated=truncated,
        notes=["Market winners are hindsight price returns, not backtest trades or recommendations."],
    )


def _cohort_window(conn: Any, period: str, horizon: int, cutoff: date | None) -> tuple[date | None, date | None]:
    matured = f"fwd_{horizon}d_matured_at"
    row = conn.execute(
        f"""SELECT MAX({matured}) FROM rank_cohort_performance_trusted
            WHERE {matured} IS NOT NULL AND (? IS NULL OR {matured}<=?)
              AND (? IS NULL OR CAST(inserted_at AS DATE)<=?)""",
        [cutoff, cutoff, cutoff, cutoff],
    ).fetchone()
    effective = coerce_date(row[0]) if row else None
    return (_period_start(period, effective), effective) if effective else (None, None)


def get_ranked_winners(
    ctx: McpContext,
    *,
    period: str = "month",
    horizon_days: int = 20,
    exchange: str = "NSE",
    as_of: str | date | None = None,
    min_return_pct: float = 0.0,
    deduplicate: bool = True,
    limit: int | None = 50,
) -> dict[str, Any]:
    """Return matured winners among previously ranked cohort rows."""

    cutoff = coerce_date(as_of)
    if as_of is not None and cutoff is None:
        raise ValueError("as_of must be an ISO date")
    horizon = _horizon(horizon_days)
    exchange_code = ctx.resolve_exchange(exchange)
    ret, matured = f"fwd_{horizon}d_return", f"fwd_{horizon}d_matured_at"
    bounded = clamp_limit(limit, default=50, maximum=500)
    with ctx.research_performance() as conn:
        if not _exists(conn, "rank_cohort_performance_trusted"):
            rows, start, effective = [], None, None
        else:
            start, effective = _cohort_window(conn, period, horizon, cutoff)
            if effective is None:
                rows = []
            else:
                base = f"""SELECT run_date,symbol_id,exchange,rank_position,
                                  composite_score,watchlist_bucket,sector_name,
                                  {ret} AS return_pct,{matured} AS matured_at,
                                  source_type,source_run_id,inserted_at,
                                  COUNT(*) OVER(PARTITION BY symbol_id,exchange) AS occurrence_count,
                                  AVG({ret}) OVER(PARTITION BY symbol_id,exchange) AS average_return_pct
                           FROM rank_cohort_performance_trusted
                           WHERE exchange=? AND run_date BETWEEN ? AND ?
                             AND {ret} IS NOT NULL AND {ret}>=? AND {matured}<=?
                             AND (? IS NULL OR CAST(inserted_at AS DATE)<=?)"""
                if deduplicate:
                    query = f"""{base} QUALIFY ROW_NUMBER() OVER(
                        PARTITION BY symbol_id,exchange ORDER BY {ret} DESC,run_date DESC)=1
                        ORDER BY return_pct DESC,symbol_id LIMIT ?"""
                else:
                    query = f"{base} ORDER BY return_pct DESC,run_date DESC,symbol_id LIMIT ?"
                rows = _records(conn.execute(
                    query,
                    [exchange_code, start, effective, min_return_pct, effective,
                     cutoff, cutoff, bounded + 1],
                ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    return envelope(
        rows,
        source="research.duckdb:rank_cohort_performance_trusted",
        as_of_status=_status(as_of, bool(rows)), as_of_requested=as_of,
        as_of_effective=effective, date_fields=("run_date", "matured_at", "inserted_at"),
        period=period, period_start=start, horizon_days=horizon,
        exchange=exchange_code, deduplicate=deduplicate,
        evidence_type="matured_rank_cohort_outcome", truncated=truncated,
    )


def get_rank_performance_summary(
    ctx: McpContext,
    *,
    period: str = "month",
    horizon_days: int = 20,
    exchange: str = "NSE",
    as_of: str | date | None = None,
    rank_bucket: str | None = None,
    sector: str | None = None,
) -> dict[str, Any]:
    """Aggregate trusted matured rank-cohort performance for a period."""

    cutoff = coerce_date(as_of)
    if as_of is not None and cutoff is None:
        raise ValueError("as_of must be an ISO date")
    horizon = _horizon(horizon_days)
    exchange_code = ctx.resolve_exchange(exchange)
    ret, matured = f"fwd_{horizon}d_return", f"fwd_{horizon}d_matured_at"
    with ctx.research_performance() as conn:
        if not _exists(conn, "rank_cohort_performance_trusted"):
            data, start, effective = {}, None, None
        else:
            start, effective = _cohort_window(conn, period, horizon, cutoff)
            if effective is None:
                data = {}
            else:
                clauses = ["exchange=?", "run_date BETWEEN ? AND ?", f"{ret} IS NOT NULL", f"{matured}<=?"]
                params: list[Any] = [exchange_code, start, effective, effective]
                if cutoff:
                    clauses.append("CAST(inserted_at AS DATE)<=?")
                    params.append(cutoff)
                if rank_bucket:
                    clauses.append("watchlist_bucket=?")
                    params.append(rank_bucket)
                if sector:
                    clauses.append("sector_name=?")
                    params.append(sector)
                row = _records(conn.execute(
                    f"""SELECT COUNT(*) AS cohort_rows,COUNT(DISTINCT symbol_id) AS unique_symbols,
                               COUNT(*) FILTER (WHERE {ret}>0) AS winning_rows,
                               100.0*COUNT(*) FILTER (WHERE {ret}>0)/NULLIF(COUNT(*),0) AS win_rate_pct,
                               AVG({ret}) AS average_return_pct,MEDIAN({ret}) AS median_return_pct,
                               quantile_cont({ret},0.25) AS p25_return_pct,
                               quantile_cont({ret},0.75) AS p75_return_pct,
                               MIN({ret}) AS worst_return_pct,MAX({ret}) AS best_return_pct,
                               MIN(run_date) AS first_cohort_date,MAX(run_date) AS last_cohort_date
                        FROM rank_cohort_performance_trusted WHERE {' AND '.join(clauses)}""",
                    params,
                ))[0]
                data = row if row.get("cohort_rows") else {}
    return envelope(
        data,
        source="research.duckdb:rank_cohort_performance_trusted",
        as_of_status=_status(as_of, bool(data)), as_of_requested=as_of,
        as_of_effective=effective, period=period, period_start=start,
        horizon_days=horizon, exchange=exchange_code,
        rank_bucket=rank_bucket, sector=sector,
        evidence_type="trusted_rank_cohort_aggregate",
        notes=["Pending horizons and persisted anomaly rows are excluded."],
    )


def get_symbol_backtest_history(
    ctx: McpContext,
    symbol: str,
    *,
    exchange: str = "NSE",
    horizon_days: int = 20,
    as_of: str | date | None = None,
    limit: int | None = 250,
) -> dict[str, Any]:
    """Return matured rank-cohort outcomes for one listing."""

    cutoff = coerce_date(as_of)
    if as_of is not None and cutoff is None:
        raise ValueError("as_of must be an ISO date")
    horizon = _horizon(horizon_days)
    symbol_id, exchange_code = ctx.normalize_symbol(symbol), ctx.resolve_exchange(exchange)
    ret, matured = f"fwd_{horizon}d_return", f"fwd_{horizon}d_matured_at"
    bounded = clamp_limit(limit, default=250, maximum=1000)
    with ctx.research_performance() as conn:
        if not _exists(conn, "rank_cohort_performance_trusted"):
            rows = []
        else:
            rows = _records(conn.execute(
                f"""SELECT run_date,symbol_id,exchange,rank_position,composite_score,
                            watchlist_bucket,sector_name,{ret} AS return_pct,
                            {matured} AS matured_at,source_type,source_run_id,inserted_at
                     FROM rank_cohort_performance_trusted
                     WHERE UPPER(symbol_id)=? AND exchange=? AND {ret} IS NOT NULL
                       AND (? IS NULL OR {matured}<=?)
                       AND (? IS NULL OR CAST(inserted_at AS DATE)<=?)
                     ORDER BY run_date DESC LIMIT ?""",
                [symbol_id, exchange_code, cutoff, cutoff, cutoff, cutoff, bounded + 1],
            ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    effective = max((coerce_date(row.get("matured_at")) for row in rows), default=None)
    return envelope(
        rows, source="research.duckdb:rank_cohort_performance_trusted",
        as_of_status=_status(as_of, bool(rows)), as_of_requested=as_of,
        as_of_effective=effective, date_fields=("run_date", "matured_at", "inserted_at"),
        symbol=symbol_id, exchange=exchange_code, horizon_days=horizon,
        evidence_type="matured_rank_cohort_outcome", truncated=truncated,
    )


def _resolve_iteration(conn: Any, run_id: str, iteration: str | int) -> int | None:
    if isinstance(iteration, int) or str(iteration).lstrip("-").isdigit():
        return int(iteration)
    token = str(iteration).strip().lower()
    if token == "baseline":
        return -1
    if token != "champion":
        raise ValueError("iteration must be champion, baseline, or an integer")
    row = conn.execute(
        """SELECT i.iteration FROM strategy_iteration_result i
           JOIN strategy_optimization_run r USING(optimization_run_id)
           WHERE i.optimization_run_id=? AND i.fold_index=-1
             AND i.rule_pack_id=r.champion_rule_pack_id
           ORDER BY i.accepted DESC NULLS LAST,i.fitness DESC NULLS LAST LIMIT 1""",
        [run_id],
    ).fetchone()
    return int(row[0]) if row else None


def get_backtest_runs(
    ctx: McpContext,
    *,
    strategy_id: str | None = None,
    status: str | None = "completed",
    as_of: str | date | None = None,
    limit: int | None = 50,
) -> dict[str, Any]:
    """List persisted strategy optimization/backtest runs."""

    cutoff = coerce_date(as_of)
    if as_of is not None and cutoff is None:
        raise ValueError("as_of must be an ISO date")
    bounded = clamp_limit(limit, default=50, maximum=250)
    with ctx.control_plane() as conn:
        if not _exists(conn, "strategy_optimization_run"):
            rows = []
        else:
            clauses, params = [], []
            if strategy_id:
                clauses.append("r.strategy_id=?")
                params.append(strategy_id)
            if status:
                clauses.append("r.status=?")
                params.append(status)
            if cutoff:
                clauses.append("CAST(COALESCE(r.completed_at,r.started_at) AS DATE)<=?")
                params.append(cutoff)
            where = "WHERE " + " AND ".join(clauses) if clauses else ""
            rows = _records(conn.execute(
                f"""SELECT r.optimization_run_id,r.recipe_name,r.strategy_id,r.status,
                            r.from_date,r.to_date,r.seed,r.max_trials,r.started_at,
                            r.completed_at,r.champion_rule_pack_id,
                            (SELECT COUNT(DISTINCT iteration) FROM strategy_iteration_result i
                             WHERE i.optimization_run_id=r.optimization_run_id AND i.iteration>=0) trial_count
                     FROM strategy_optimization_run r {where}
                     ORDER BY r.started_at DESC LIMIT ?""",
                [*params, bounded + 1],
            ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    effective = max((coerce_date(row.get("completed_at") or row.get("started_at")) for row in rows), default=None)
    return envelope(
        rows, source="control_plane.duckdb:strategy_optimization_run",
        as_of_status=_status(as_of, bool(rows)), as_of_requested=as_of,
        as_of_effective=effective, date_fields=("from_date", "to_date", "started_at", "completed_at"),
        strategy_id=strategy_id, status_filter=status,
        evidence_type="persisted_strategy_backtest_run", truncated=truncated,
    )


def get_backtest_result(
    ctx: McpContext,
    optimization_run_id: str,
    *,
    iteration: str | int = "champion",
    as_of: str | date | None = None,
) -> dict[str, Any]:
    """Return run provenance plus aggregate and per-fold metrics."""

    cutoff = coerce_date(as_of)
    if as_of is not None and cutoff is None:
        raise ValueError("as_of must be an ISO date")
    with ctx.control_plane() as conn:
        if not _exists(conn, "strategy_optimization_run"):
            data = {}
        else:
            runs = _records(conn.execute(
                """SELECT optimization_run_id,recipe_name,strategy_id,status,baseline_rule_pack_id,
                          champion_rule_pack_id,from_date,to_date,seed,max_trials,started_at,completed_at
                   FROM strategy_optimization_run WHERE optimization_run_id=?
                     AND (? IS NULL OR CAST(COALESCE(completed_at,started_at) AS DATE)<=?)""",
                [optimization_run_id, cutoff, cutoff],
            ))
            selected = _resolve_iteration(conn, optimization_run_id, iteration) if runs else None
            metrics = [] if selected is None else _records(conn.execute(
                """SELECT iteration,rule_pack_id,fold_index,fold_role,fitness,cagr,sharpe,
                          sortino,max_drawdown_pct,win_rate,profit_factor,trade_count,
                          trades_per_year,total_return_pct,benchmark_return_pct,
                          benchmark_symbol,accepted,rejection_reason,created_at
                   FROM strategy_iteration_result WHERE optimization_run_id=? AND iteration=?
                   ORDER BY fold_index""",
                [optimization_run_id, selected],
            ))
            data = {"run": runs[0], "selected_iteration": selected, "metrics": metrics} if runs else {}
    effective = coerce_date(data.get("run", {}).get("completed_at")) if data else None
    return envelope(
        data, source="control_plane.duckdb:strategy_optimization_run+strategy_iteration_result",
        as_of_status=_status(as_of, bool(data)), as_of_requested=as_of,
        as_of_effective=effective, optimization_run_id=optimization_run_id,
        iteration=iteration, evidence_type="persisted_strategy_backtest_result",
    )


def get_backtest_trades(
    ctx: McpContext,
    optimization_run_id: str,
    *,
    iteration: str | int = "champion",
    outcome: str = "all",
    as_of: str | date | None = None,
    limit: int | None = 100,
) -> dict[str, Any]:
    """Return bounded persisted trades for one strategy backtest iteration."""

    cutoff = coerce_date(as_of)
    if as_of is not None and cutoff is None:
        raise ValueError("as_of must be an ISO date")
    outcome_key = str(outcome).strip().lower()
    if outcome_key not in {"all", "winner", "loser"}:
        raise ValueError("outcome must be all, winner, or loser")
    bounded = clamp_limit(limit, default=100, maximum=1000)
    with ctx.control_plane() as conn:
        run = None if not _exists(conn, "strategy_optimization_run") else conn.execute(
            """SELECT completed_at FROM strategy_optimization_run
               WHERE optimization_run_id=? AND status='completed'
                 AND (? IS NULL OR CAST(completed_at AS DATE)<=?)""",
            [optimization_run_id, cutoff, cutoff],
        ).fetchone()
        selected = _resolve_iteration(conn, optimization_run_id, iteration) if run else None
        if selected is None:
            rows = []
        else:
            outcome_clause = "" if outcome_key == "all" else ("AND pnl_pct>0" if outcome_key == "winner" else "AND pnl_pct<=0")
            rows = _records(conn.execute(
                f"""SELECT symbol_id,exchange,entry_date,entry_price,entry_reason,
                            exit_date,exit_price,exit_reason,bars_held,pnl,pnl_pct,
                            sector,rank_at_entry,score_at_entry,fold_index,rule_pack_id
                     FROM strategy_backtest_trade
                     WHERE optimization_run_id=? AND iteration=? {outcome_clause}
                     ORDER BY pnl_pct DESC NULLS LAST,entry_date DESC LIMIT ?""",
                [optimization_run_id, selected, bounded + 1],
            ))
    truncated = len(rows) > bounded
    rows = rows[:bounded]
    effective = coerce_date(run[0]) if run else None
    return envelope(
        rows, source="control_plane.duckdb:strategy_backtest_trade",
        as_of_status=_status(as_of, bool(rows)), as_of_requested=as_of,
        as_of_effective=effective, date_fields=("entry_date", "exit_date"),
        optimization_run_id=optimization_run_id, selected_iteration=selected,
        outcome=outcome_key, evidence_type="persisted_strategy_backtest_trade",
        truncated=truncated,
    )


__all__ = [
    "get_backtest_result", "get_backtest_runs", "get_backtest_trades",
    "get_market_winners", "get_rank_performance_summary", "get_ranked_winners",
    "get_symbol_backtest_history",
]
