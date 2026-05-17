"""Persistence layer for optimizer runs, trials, and trades.

Uses the same DuckDB control-plane DB as the rest of the pipeline. Migration
015 creates the required tables; ``RegistryStore`` applies it on first use.

All timestamps written from this module are **naive UTC** — the column type
is ``TIMESTAMP`` (timezone-unaware in DuckDB), and every site below routes
through ``_utc_naive_now()``. This avoids the historical pitfall where
``started_at`` came from DuckDB's ``DEFAULT current_timestamp`` (local time,
IST on the operator box) while ``completed_at`` came from Python's
``datetime.utcnow()`` — producing a 5h30m phantom gap on every run.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

from ai_trading_system.domains.strategy import (
    StrategyRulePack,
    rule_pack_hash,
    save_rule_pack,
)
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.research.backtesting.engine_runner import BacktestResult
from ai_trading_system.research.optimization.evaluator import Metrics


def _utc_naive_now() -> datetime:
    """Current UTC time as a naive ``datetime`` for DuckDB ``TIMESTAMP`` columns.

    ``datetime.utcnow()`` is deprecated in 3.12+; ``datetime.now(timezone.utc)``
    is tz-aware which DuckDB can't bind into a naive ``TIMESTAMP`` column on
    every codepath. Stripping ``tzinfo`` gives us the same value safely.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _ensure_migrations(project_root: Path | str) -> Path:
    """Run migrations (idempotent) and return the control-plane DB path."""
    registry = RegistryStore(project_root=Path(project_root))
    registry._ensure_initialized()
    return registry.db_path


class OptimizationStore:
    """Thin wrapper around DuckDB writes for optimizer tables."""

    def __init__(self, *, project_root: Path | str):
        self.db_path = _ensure_migrations(project_root)

    def _conn(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    # ---- rule packs --------------------------------------------------

    def upsert_rule_pack(
        self,
        pack: StrategyRulePack,
        *,
        parent_id: str | None = None,
        lifecycle_status: str = "draft",
    ) -> str:
        pack_id = rule_pack_hash(pack)
        rule_yaml = yaml.safe_dump(pack.model_dump(), sort_keys=True)
        rule_json = json.dumps(pack.model_dump(), sort_keys=True, separators=(",", ":"))
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO strategy_rule_pack (
                    rule_pack_id, parent_rule_pack_id, strategy_id, version,
                    rule_yaml, rule_json, lifecycle_status, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (rule_pack_id) DO NOTHING
                """,
                [
                    pack_id,
                    parent_id,
                    pack.strategy_id,
                    pack.version,
                    rule_yaml,
                    rule_json,
                    lifecycle_status,
                    pack.description,
                ],
            )
        return pack_id

    def set_lifecycle_status(self, rule_pack_id: str, status: str) -> None:
        # DuckDB's unique-index implementation fights UPDATE on indexed rows;
        # delete + re-insert preserving everything else.
        with self._conn() as con:
            row = con.execute(
                """
                SELECT parent_rule_pack_id, strategy_id, version, rule_yaml,
                       rule_json, description, created_at
                FROM strategy_rule_pack WHERE rule_pack_id = ?
                """,
                [rule_pack_id],
            ).fetchone()
            if row is None:
                return
            con.execute("DELETE FROM strategy_rule_pack WHERE rule_pack_id = ?", [rule_pack_id])
            con.execute(
                """
                INSERT INTO strategy_rule_pack (
                    rule_pack_id, parent_rule_pack_id, strategy_id, version,
                    rule_yaml, rule_json, lifecycle_status, description, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [rule_pack_id, *row[:5], status, row[5], row[6]],
            )

    def get_latest_champion_rule_pack(self, recipe_name: str) -> str | None:
        """Return the champion rule_pack_id for the most-recent completed run of ``recipe_name``.

        Returns ``None`` if no completed run exists for that recipe, or if the
        most recent run has no champion (e.g. all trials were rejected).
        Designed for the ``promote-latest`` operator shortcut so the operator
        does not need to copy-paste a 40-char hash.
        """
        with self._conn() as con:
            row = con.execute(
                """
                SELECT champion_rule_pack_id
                FROM strategy_optimization_run
                WHERE recipe_name = ? AND status = 'completed'
                ORDER BY completed_at DESC NULLS LAST, started_at DESC
                LIMIT 1
                """,
                [recipe_name],
            ).fetchone()
        if row is None:
            return None
        return row[0]

    # ---- runs --------------------------------------------------------

    def create_run(
        self,
        *,
        optimization_run_id: str,
        recipe_name: str,
        strategy_id: str,
        baseline_rule_pack_id: str,
        from_date,
        to_date,
        seed: int,
        max_trials: int,
        recipe_json: str,
        study_storage_uri: str | None = None,
    ) -> None:
        # Set started_at explicitly (UTC-naive) so the DuckDB DDL's
        # ``DEFAULT current_timestamp`` (local time) is never consulted —
        # this is the fix for the started_at/completed_at timezone mismatch.
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO strategy_optimization_run (
                    optimization_run_id, recipe_name, strategy_id,
                    baseline_rule_pack_id, from_date, to_date, seed,
                    max_trials, status, recipe_json, study_storage_uri,
                    started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'running', ?, ?, ?)
                """,
                [
                    optimization_run_id,
                    recipe_name,
                    strategy_id,
                    baseline_rule_pack_id,
                    from_date,
                    to_date,
                    seed,
                    max_trials,
                    recipe_json,
                    study_storage_uri,
                    _utc_naive_now(),
                ],
            )

    def complete_run(
        self,
        *,
        optimization_run_id: str,
        status: str,
        champion_rule_pack_id: str | None,
        error: str | None = None,
    ) -> None:
        # DELETE + INSERT to work around DuckDB unique-index UPDATE limitation.
        with self._conn() as con:
            row = con.execute(
                """
                SELECT recipe_name, strategy_id, baseline_rule_pack_id,
                       from_date, to_date, seed, max_trials, recipe_json, started_at,
                       study_storage_uri
                FROM strategy_optimization_run WHERE optimization_run_id = ?
                """,
                [optimization_run_id],
            ).fetchone()
            if row is None:
                return
            con.execute(
                "DELETE FROM strategy_optimization_run WHERE optimization_run_id = ?",
                [optimization_run_id],
            )
            con.execute(
                """
                INSERT INTO strategy_optimization_run (
                    optimization_run_id, recipe_name, strategy_id,
                    baseline_rule_pack_id, from_date, to_date, seed,
                    max_trials, status, champion_rule_pack_id, recipe_json,
                    error, started_at, completed_at, study_storage_uri
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    optimization_run_id,
                    *row[:7],
                    status,
                    champion_rule_pack_id,
                    row[7],
                    error,
                    row[8],
                    _utc_naive_now(),
                    row[9],
                ],
            )

    def get_run_for_resume(self, optimization_run_id: str) -> dict | None:
        """Return the row needed to resume a study, or ``None`` if not found.

        Wave 5a: the resume CLI looks up the existing row, rehydrates the
        recipe from ``recipe_json``, opens the Optuna journal at
        ``study_storage_uri``, and continues with ``load_if_exists=True``.

        Returns a dict with keys:
          optimization_run_id, recipe_name, strategy_id, baseline_rule_pack_id,
          status, max_trials, recipe_json, study_storage_uri.
        """
        with self._conn() as con:
            row = con.execute(
                """
                SELECT optimization_run_id, recipe_name, strategy_id,
                       baseline_rule_pack_id, status, max_trials, recipe_json,
                       study_storage_uri
                FROM strategy_optimization_run
                WHERE optimization_run_id = ?
                """,
                [optimization_run_id],
            ).fetchone()
        if row is None:
            return None
        return {
            "optimization_run_id": row[0],
            "recipe_name": row[1],
            "strategy_id": row[2],
            "baseline_rule_pack_id": row[3],
            "status": row[4],
            "max_trials": row[5],
            "recipe_json": row[6],
            "study_storage_uri": row[7],
        }

    # ---- per-trial / per-fold ---------------------------------------

    def insert_iteration_result(
        self,
        *,
        optimization_run_id: str,
        iteration: int,
        rule_pack_id: str,
        fold_index: int,
        fold_role: str,
        fitness_value: float | None,
        metrics: Metrics | None,
        benchmark_return_pct: float | None,
        benchmark_symbol: str | None = None,
        accepted: bool,
        rejection_reason: str | None,
    ) -> None:
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO strategy_iteration_result (
                    optimization_run_id, iteration, rule_pack_id, fold_index,
                    fold_role, fitness, cagr, sharpe, sortino, max_drawdown_pct,
                    win_rate, profit_factor, trade_count, trades_per_year,
                    total_return_pct, benchmark_return_pct, benchmark_symbol,
                    accepted, rejection_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    optimization_run_id,
                    iteration,
                    rule_pack_id,
                    fold_index,
                    fold_role,
                    fitness_value,
                    metrics.cagr if metrics else None,
                    metrics.sharpe if metrics else None,
                    metrics.sortino if metrics else None,
                    metrics.max_drawdown_pct if metrics else None,
                    metrics.win_rate if metrics else None,
                    None if metrics is None else (metrics.profit_factor if metrics.profit_factor != float("inf") else None),
                    metrics.trade_count if metrics else None,
                    metrics.trades_per_year if metrics else None,
                    metrics.total_return_pct if metrics else None,
                    benchmark_return_pct,
                    benchmark_symbol,
                    accepted,
                    rejection_reason,
                ],
            )

    def insert_trades(
        self,
        *,
        optimization_run_id: str,
        iteration: int,
        fold_index: int,
        rule_pack_id: str,
        result: BacktestResult,
    ) -> None:
        if not result.trades:
            return
        rows = [
            [
                optimization_run_id,
                iteration,
                fold_index,
                rule_pack_id,
                t.symbol_id,
                t.exchange,
                t.entry_date,
                t.entry_price,
                t.entry_reason,
                t.exit_date,
                t.exit_price,
                t.exit_reason,
                t.bars_held,
                t.pnl,
                t.pnl_pct,
                t.sector,
                t.rank_at_entry,
                t.score_at_entry,
            ]
            for t in result.trades
        ]
        with self._conn() as con:
            con.executemany(
                """
                INSERT INTO strategy_backtest_trade (
                    optimization_run_id, iteration, fold_index, rule_pack_id,
                    symbol_id, exchange, entry_date, entry_price, entry_reason,
                    exit_date, exit_price, exit_reason, bars_held, pnl,
                    pnl_pct, sector, rank_at_entry, score_at_entry
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def export_pack_yaml(self, pack: StrategyRulePack, target_path: Path | str) -> None:
        """Write a rule pack to disk as YAML (used for champion export)."""
        save_rule_pack(pack, target_path)
