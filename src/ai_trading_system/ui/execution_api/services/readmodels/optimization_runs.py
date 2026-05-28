"""Read models for the optimizer router.

Pure functions over ``data/control_plane.duckdb``. Backs:

    GET /api/execution/optimization/runs
    GET /api/execution/optimization/runs/{run_id}
    GET /api/execution/optimization/runs/{run_id}/trials
    GET /api/execution/optimization/leaderboard
    GET /api/execution/optimization/runs/{run_id}/report

Mirrors the style of ``runs_introspection.py``: never raises on a missing DB
or unknown id — returns a degraded-but-typed payload so the UI can render a
placeholder.

The optimizer tables (``strategy_rule_pack``, ``strategy_optimization_run``,
``strategy_iteration_result``, ``strategy_backtest_trade``) are created by
``src/ai_trading_system/pipeline/migrations/015_strategy_optimizer.sql`` and
written exclusively by ``research/optimization/store.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional

import duckdb

from ai_trading_system.research.optimization.reports import (
    report_dir,
    report_path,
)
from ai_trading_system.ui.execution_api.services.readmodels.latest_operational_snapshot import (
    ExecutionContext,
    get_execution_context,
)


# Metrics callers may use for sorting on /trials or /leaderboard. The values
# must match column names in ``strategy_iteration_result``.
_TRIAL_SORT_COLUMNS = {
    "fitness",
    "cagr",
    "sharpe",
    "max_drawdown_pct",
    "win_rate",
    "trade_count",
    "total_return_pct",
    "iteration",
}

_LEADERBOARD_METRICS = {
    "fitness",
    "cagr",
    "sharpe",
    "win_rate",
    "total_return_pct",
    "trade_count",
}


def _control_plane_path(ctx: ExecutionContext) -> Path:
    return ctx.control_plane_db or (ctx.ohlcv_db.parent / "control_plane.duckdb")


def _connect(project_root: str | Path | None) -> tuple[Optional[duckdb.DuckDBPyConnection], ExecutionContext]:
    """Open the control-plane DB read-only or return ``(None, ctx)`` if missing."""
    ctx = get_execution_context(project_root)
    cp_path = _control_plane_path(ctx)
    if not cp_path.exists():
        return None, ctx
    return duckdb.connect(str(cp_path), read_only=True), ctx


# ---------------------------------------------------------------------------
# Runs list + detail
# ---------------------------------------------------------------------------


def list_runs(
    project_root: str | Path | None,
    *,
    recipe: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
) -> dict[str, Any]:
    conn, _ctx = _connect(project_root)
    if conn is None:
        return {"available": False, "runs": []}
    try:
        where: list[str] = []
        params: list[Any] = []
        if recipe:
            where.append("recipe_name = ?")
            params.append(recipe)
        if status:
            where.append("status = ?")
            params.append(status)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        params.append(int(limit))
        rows = conn.execute(
            f"""
            SELECT
                r.optimization_run_id, r.recipe_name, r.strategy_id, r.status,
                r.from_date, r.to_date, r.seed, r.max_trials,
                r.started_at, r.completed_at, r.champion_rule_pack_id, r.error,
                (SELECT COUNT(DISTINCT iteration)
                   FROM strategy_iteration_result
                  WHERE optimization_run_id = r.optimization_run_id
                    AND iteration >= 0) AS trial_count
            FROM strategy_optimization_run r
            {where_sql}
            ORDER BY r.started_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        conn.close()
    return {
        "available": True,
        "runs": [
            {
                "optimization_run_id": r[0],
                "recipe_name": r[1],
                "strategy_id": r[2],
                "status": r[3],
                "from_date": r[4],
                "to_date": r[5],
                "seed": r[6],
                "max_trials": r[7],
                "started_at": r[8],
                "completed_at": r[9],
                "champion_rule_pack_id": r[10],
                "error": r[11],
                "trial_count": int(r[12] or 0),
            }
            for r in rows
        ],
    }


def _fold_rows_for_pack(
    conn: duckdb.DuckDBPyConnection, optimization_run_id: str, rule_pack_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT fold_index, fitness, cagr, sharpe, max_drawdown_pct,
               win_rate, trade_count, total_return_pct,
               nifty_return_pct AS benchmark_return_pct
        FROM strategy_iteration_result
        WHERE optimization_run_id = ?
          AND rule_pack_id = ?
          AND fold_index >= 0
        ORDER BY fold_index
        """,
        [optimization_run_id, rule_pack_id],
    ).fetchall()
    return [
        {
            "fold_index": r[0],
            "fitness": r[1],
            "cagr": r[2],
            "sharpe": r[3],
            "max_drawdown_pct": r[4],
            "win_rate": r[5],
            "trade_count": r[6],
            "total_return_pct": r[7],
            "benchmark_return_pct": r[8],
        }
        for r in rows
    ]


def get_run_detail(
    project_root: str | Path | None,
    optimization_run_id: str,
) -> Optional[dict[str, Any]]:
    """Return run header + baseline/champion per-fold + report path.

    Returns ``None`` if the run does not exist (router maps to 404). Returns
    ``{"available": False}`` if the DB is missing.
    """
    conn, ctx = _connect(project_root)
    if conn is None:
        return {"available": False, "optimization_run_id": optimization_run_id}
    try:
        run_row = conn.execute(
            """
            SELECT
                optimization_run_id, recipe_name, strategy_id, status,
                from_date, to_date, seed, max_trials, started_at, completed_at,
                error, baseline_rule_pack_id, champion_rule_pack_id
            FROM strategy_optimization_run
            WHERE optimization_run_id = ?
            """,
            [optimization_run_id],
        ).fetchone()
        if run_row is None:
            return None

        (
            run_id,
            recipe_name,
            strategy_id,
            status,
            from_date,
            to_date,
            seed,
            max_trials,
            started_at,
            completed_at,
            error,
            baseline_id,
            champion_id,
        ) = run_row

        baseline_folds = _fold_rows_for_pack(conn, run_id, baseline_id) if baseline_id else []
        champion_folds = _fold_rows_for_pack(conn, run_id, champion_id) if champion_id else []

        champion_lifecycle: Optional[str] = None
        if champion_id:
            row = conn.execute(
                "SELECT lifecycle_status FROM strategy_rule_pack WHERE rule_pack_id = ?",
                [champion_id],
            ).fetchone()
            champion_lifecycle = row[0] if row else None

        trial_count_row = conn.execute(
            """
            SELECT COUNT(DISTINCT iteration)
            FROM strategy_iteration_result
            WHERE optimization_run_id = ? AND iteration >= 0
            """,
            [run_id],
        ).fetchone()
        trial_count = int(trial_count_row[0] or 0)
    finally:
        conn.close()

    rpath = report_path(ctx.project_root, recipe_name, run_id)
    return {
        "available": True,
        "optimization_run_id": run_id,
        "recipe_name": recipe_name,
        "strategy_id": strategy_id,
        "status": status,
        "from_date": from_date,
        "to_date": to_date,
        "seed": seed,
        "max_trials": max_trials,
        "started_at": started_at,
        "completed_at": completed_at,
        "error": error,
        "baseline_rule_pack_id": baseline_id,
        "baseline_folds": baseline_folds,
        "champion_rule_pack_id": champion_id,
        "champion_folds": champion_folds,
        "champion_lifecycle_status": champion_lifecycle,
        "trial_count": trial_count,
        "report_path": str(rpath),
        "report_exists": rpath.exists(),
    }


# ---------------------------------------------------------------------------
# Trials
# ---------------------------------------------------------------------------


def get_trials(
    project_root: str | Path | None,
    optimization_run_id: str,
    *,
    limit: int = 200,
    sort: str = "iteration",
) -> dict[str, Any]:
    """Return per-trial aggregate rows for a run.

    Aggregate rows are those with ``fold_index = -1`` (the per-trial summary
    written by ``OptimizationStore.insert_iteration_result``). The baseline
    marker (``iteration = -1``) is excluded.
    """
    if sort not in _TRIAL_SORT_COLUMNS:
        sort = "iteration"
    direction = "ASC" if sort == "iteration" else "DESC"

    conn, _ctx = _connect(project_root)
    if conn is None:
        return {
            "available": False,
            "optimization_run_id": optimization_run_id,
            "trials": [],
        }
    try:
        rows = conn.execute(
            f"""
            SELECT iteration, rule_pack_id, fitness, cagr, sharpe,
                   max_drawdown_pct, win_rate, trade_count, total_return_pct,
                   accepted, rejection_reason, created_at
            FROM strategy_iteration_result
            WHERE optimization_run_id = ?
              AND fold_index = -1
              AND iteration >= 0
            ORDER BY {sort} {direction} NULLS LAST
            LIMIT ?
            """,
            [optimization_run_id, int(limit)],
        ).fetchall()
    finally:
        conn.close()
    return {
        "available": True,
        "optimization_run_id": optimization_run_id,
        "trials": [
            {
                "iteration": r[0],
                "rule_pack_id": r[1],
                "fitness": r[2],
                "cagr": r[3],
                "sharpe": r[4],
                "max_drawdown_pct": r[5],
                "win_rate": r[6],
                "trade_count": r[7],
                "total_return_pct": r[8],
                "accepted": r[9],
                "rejection_reason": r[10],
                "created_at": r[11],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Leaderboard
# ---------------------------------------------------------------------------


def get_leaderboard(
    project_root: str | Path | None,
    *,
    metric: str = "sharpe",
    top: int = 20,
) -> dict[str, Any]:
    """Return the best champion per recipe across completed runs, ranked by ``metric``.

    For each recipe, pick the most-recent completed run with a champion, then
    rank the resulting set by the selected aggregate metric (read from the
    champion's ``fold_index = -1`` row).
    """
    if metric not in _LEADERBOARD_METRICS:
        metric = "sharpe"

    conn, _ctx = _connect(project_root)
    if conn is None:
        return {"available": False, "metric": metric, "rows": []}
    try:
        rows = conn.execute(
            f"""
            WITH latest_per_recipe AS (
                SELECT recipe_name, MAX(completed_at) AS latest_completed
                FROM strategy_optimization_run
                WHERE status = 'completed' AND champion_rule_pack_id IS NOT NULL
                GROUP BY recipe_name
            ),
            latest_runs AS (
                SELECT r.optimization_run_id, r.recipe_name, r.strategy_id,
                       r.champion_rule_pack_id, r.completed_at
                FROM strategy_optimization_run r
                JOIN latest_per_recipe l
                  ON r.recipe_name = l.recipe_name
                 AND r.completed_at = l.latest_completed
                WHERE r.status = 'completed' AND r.champion_rule_pack_id IS NOT NULL
            )
            SELECT
                lr.recipe_name,
                lr.strategy_id,
                lr.optimization_run_id,
                lr.champion_rule_pack_id,
                COALESCE(p.lifecycle_status, 'unknown') AS champion_lifecycle_status,
                ir.fitness,
                ir.cagr,
                ir.sharpe,
                ir.max_drawdown_pct,
                ir.win_rate,
                ir.trade_count,
                ir.total_return_pct,
                lr.completed_at
            FROM latest_runs lr
            JOIN strategy_iteration_result ir
              ON ir.optimization_run_id = lr.optimization_run_id
             AND ir.rule_pack_id = lr.champion_rule_pack_id
             AND ir.fold_index = -1
            LEFT JOIN strategy_rule_pack p
              ON p.rule_pack_id = lr.champion_rule_pack_id
            ORDER BY ir.{metric} DESC NULLS LAST
            LIMIT ?
            """,
            [int(top)],
        ).fetchall()
    finally:
        conn.close()
    return {
        "available": True,
        "metric": metric,
        "rows": [
            {
                "recipe_name": r[0],
                "strategy_id": r[1],
                "optimization_run_id": r[2],
                "champion_rule_pack_id": r[3],
                "champion_lifecycle_status": r[4],
                "fitness": r[5],
                "cagr": r[6],
                "sharpe": r[7],
                "max_drawdown_pct": r[8],
                "win_rate": r[9],
                "trade_count": r[10],
                "total_return_pct": r[11],
                "completed_at": r[12],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Report content
# ---------------------------------------------------------------------------


def get_report(
    project_root: str | Path | None,
    optimization_run_id: str,
) -> Optional[dict[str, Any]]:
    """Return the auto-written markdown report for a run, or ``None`` if absent."""
    conn, ctx = _connect(project_root)
    if conn is None:
        return None
    try:
        row = conn.execute(
            """
            SELECT recipe_name
            FROM strategy_optimization_run
            WHERE optimization_run_id = ?
            """,
            [optimization_run_id],
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    recipe_name = row[0]
    rpath = report_path(ctx.project_root, recipe_name, optimization_run_id)
    if not rpath.exists():
        return None
    return {
        "optimization_run_id": optimization_run_id,
        "recipe_name": recipe_name,
        "report_path": str(rpath),
        "content": rpath.read_text(),
    }


__all__ = [
    "list_runs",
    "get_run_detail",
    "get_trials",
    "get_leaderboard",
    "get_report",
]
