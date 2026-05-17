"""Optuna study orchestration with walk-forward evaluation per trial.

Each Optuna trial samples a ``StrategyRulePack`` from ``bounds.build_search_space``,
backtests it on every walk-forward validation fold, applies the acceptance
gate, and returns the mean per-fold fitness for Optuna to optimise. Every
trial (accepted or not) is persisted to the control DB.

Wave 5a adds persistent Optuna storage so a killed study can be resumed:

  * Fresh runs write to ``data/optuna/<run_id>.log`` via Optuna's pure-python
    ``JournalStorage`` (no SQLAlchemy / SQLite-driver dependency).
  * The journal file path is persisted on the ``strategy_optimization_run``
    row in ``study_storage_uri`` so ``resume_optimization`` can find it.
  * ``resume_optimization(run_id, project_root)`` re-opens the journal with
    ``load_if_exists=True`` and continues until ``max_trials`` is hit.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import optuna
from optuna.samplers import TPESampler
from tqdm.auto import tqdm

from ai_trading_system.domains.strategy import (
    StrategyRulePack,
    load_rule_pack,
    rule_pack_hash,
)
from ai_trading_system.domains.strategy.bounds import build_search_space
from ai_trading_system.research.optimization.acceptance import (
    FoldResult,
    aggregate_fitness,
    is_accepted,
)
from ai_trading_system.research.optimization.backtest_adapter import run_backtest
from ai_trading_system.research.optimization.baselines import benchmark_buyhold_return
from ai_trading_system.research.optimization.evaluator import (
    Metrics,
    compute_metrics,
    fitness as compute_fitness,
)
from ai_trading_system.research.optimization.guards import champion_guards
from ai_trading_system.research.optimization.recipe import (
    OptimizationRecipe,
    resolve_baseline_path,
)
from ai_trading_system.research.optimization.reports import (
    report_dir,
    report_path,
    write_report,
)
from ai_trading_system.research.optimization.store import OptimizationStore
from ai_trading_system.research.optimization.walkforward import (
    WalkForwardFold,
    build_folds,
)


logger = logging.getLogger(__name__)


def _evaluate_pack_on_folds(
    pack: StrategyRulePack,
    folds: list[WalkForwardFold],
    *,
    recipe: OptimizationRecipe,
    project_root: Path,
    benchmark_by_fold: dict[int, float | None],
    progress_label: str | None = None,
) -> list[FoldResult]:
    """Backtest one pack on every walk-forward validation window."""
    fold_results: list[FoldResult] = []
    fold_iter = folds
    if progress_label is not None:
        fold_iter = tqdm(
            folds,
            desc=progress_label,
            leave=False,
            unit="fold",
            ncols=80,
        )
    for fold in fold_iter:
        result = run_backtest(
            pack,
            project_root=project_root,
            from_date=fold.val_start,
            to_date=fold.val_end,
            exchange=recipe.exchange,
            benchmark_symbol=recipe.benchmark.symbol,
            starting_equity=recipe.starting_equity,
            commission_bps=recipe.commission_bps,
            slippage_bps=recipe.slippage_bps,
        )
        metrics = compute_metrics(result, starting_equity=recipe.starting_equity)
        fit = compute_fitness(metrics, recipe.fitness_weights)
        fold_results.append(
            FoldResult(
                fold_index=fold.index,
                fitness=fit,
                metrics=metrics,
                benchmark_return_pct=benchmark_by_fold.get(fold.index),
                benchmark_symbol=recipe.benchmark.symbol,
            )
        )
    return fold_results


def _benchmark_returns_per_fold(
    folds: list[WalkForwardFold],
    *,
    recipe: OptimizationRecipe,
    project_root: Path,
) -> dict[int, float | None]:
    out: dict[int, float | None] = {}
    for fold in folds:
        bench = benchmark_buyhold_return(
            project_root,
            benchmark=recipe.benchmark,
            from_date=fold.val_start,
            to_date=fold.val_end,
            exchange=recipe.exchange,
        )
        out[fold.index] = bench.total_return_pct if bench else None
    return out


# Legacy alias for one release; remove once external callers migrate.
_nifty_returns_per_fold = _benchmark_returns_per_fold


def _persist_iteration(
    store: OptimizationStore,
    *,
    optimization_run_id: str,
    iteration: int,
    pack: StrategyRulePack,
    fold_results: list[FoldResult],
    accepted: bool,
    rejection_reason: str | None,
) -> str:
    pack_id = store.upsert_rule_pack(pack)
    for fr in fold_results:
        store.insert_iteration_result(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            rule_pack_id=pack_id,
            fold_index=fr.fold_index,
            fold_role="val",
            fitness_value=fr.fitness,
            metrics=fr.metrics,
            benchmark_return_pct=fr.benchmark_return_pct,
            benchmark_symbol=fr.benchmark_symbol,
            accepted=accepted,
            rejection_reason=rejection_reason,
        )
    # Aggregate row at fold_index=-1.
    if fold_results:
        avg_metrics = _mean_metrics(fold_results)
        store.insert_iteration_result(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            rule_pack_id=pack_id,
            fold_index=-1,
            fold_role="aggregate",
            fitness_value=aggregate_fitness(fold_results),
            metrics=avg_metrics,
            benchmark_return_pct=None,
            benchmark_symbol=fold_results[0].benchmark_symbol if fold_results else None,
            accepted=accepted,
            rejection_reason=rejection_reason,
        )
    return pack_id


def _mean_metrics(folds: list[FoldResult]) -> Metrics:
    n = len(folds)
    if n == 0:
        return Metrics(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0)
    m0 = folds[0].metrics
    return Metrics(
        trade_count=sum(f.metrics.trade_count for f in folds),
        final_equity=m0.final_equity,
        starting_equity=m0.starting_equity,
        total_return_pct=sum(f.metrics.total_return_pct for f in folds) / n,
        cagr=sum(f.metrics.cagr for f in folds) / n,
        sharpe=sum(f.metrics.sharpe for f in folds) / n,
        sortino=sum(f.metrics.sortino for f in folds) / n,
        max_drawdown_pct=sum(f.metrics.max_drawdown_pct for f in folds) / n,
        win_rate=sum(f.metrics.win_rate for f in folds) / n,
        profit_factor=sum(
            (f.metrics.profit_factor if f.metrics.profit_factor != float("inf") else 0.0)
            for f in folds
        ) / n,
        avg_holding_days=sum(f.metrics.avg_holding_days for f in folds) / n,
        turnover_per_year=sum(f.metrics.turnover_per_year for f in folds) / n,
        bars=sum(f.metrics.bars for f in folds),
    )


def _study_storage_uri(project_root: Path, run_id: str) -> str:
    """Canonical journal-file path for a study, relative to project root.

    The path is stored on the run row and is resolved against the
    ``--project-root`` passed at resume time (so a study created on one
    machine can be resumed in a clone with the same data layout).
    """
    return str(Path("data") / "optuna" / f"{run_id}.log")


def _open_journal_study(
    *,
    project_root: Path,
    run_id: str,
    storage_uri: str,
    sampler: TPESampler,
    load_if_exists: bool,
) -> optuna.Study:
    """Create or load an Optuna study backed by a per-run journal file.

    Uses ``JournalStorage`` (pure-python; no SQLAlchemy dep) over a single
    append-only log file. Optuna's recent versions renamed the backend
    class; we accept either name to stay compatible across optuna 4.x.
    """
    abs_path = project_root / storage_uri
    abs_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        from optuna.storages import JournalStorage  # type: ignore[attr-defined]
        try:
            from optuna.storages import JournalFileBackend as _Backend  # type: ignore[attr-defined]
        except ImportError:  # pragma: no cover — optuna < 4.1
            from optuna.storages import JournalFileStorage as _Backend  # type: ignore[attr-defined]
    except ImportError as exc:  # pragma: no cover — should not happen with optuna>=4
        raise RuntimeError(
            "Optuna JournalStorage is unavailable. Install optuna>=4.0."
        ) from exc

    storage = JournalStorage(_Backend(str(abs_path)))
    return optuna.create_study(
        direction="maximize",
        sampler=sampler,
        storage=storage,
        study_name=run_id,
        load_if_exists=load_if_exists,
    )


def _print_run_banner(run_id: str, storage_uri: str, mode: str) -> None:
    """Print a clearly-visible run_id banner so the operator can copy it.

    Goes to stderr (not stdout) so it doesn't pollute the script-readable
    ``optimization_run_id=...`` summary line at the end of a CLI run.
    """
    bar = "=" * 78
    print(bar, file=sys.stderr)
    print(f"  optimizer {mode}", file=sys.stderr)
    print(f"  run_id:        {run_id}", file=sys.stderr)
    print(f"  study journal: {storage_uri}", file=sys.stderr)
    print(f"  resume with:   ai-trading-optimize resume {run_id}", file=sys.stderr)
    print(bar, file=sys.stderr)


def _write_run_report(
    *,
    project_root: Path,
    recipe_name: str,
    optimization_run_id: str,
) -> Path | None:
    """Auto-write the markdown report for a completed run.

    Writes two files under ``reports/optimization/<recipe>/`` (path layout is
    owned by ``reports.report_dir`` / ``reports.report_path`` so the execution
    API readmodel resolves the same paths):
      - ``<run_id>.md``  — immutable per run
      - ``latest.md``    — overwritten each run (operator-friendly bookmark)

    Returns the path to ``<run_id>.md`` (the canonical artifact), or ``None``
    if writing failed. Report failure does NOT propagate — observability never
    blocks the pipeline (same principle as the perf_tracker stage).
    """
    try:
        run_path = report_path(project_root, recipe_name, optimization_run_id)
        latest_path = report_dir(project_root, recipe_name) / "latest.md"
        write_report(project_root, optimization_run_id, run_path)
        # Copy run report content to latest.md so both share an identical file.
        latest_path.write_text(run_path.read_text())
        return run_path
    except Exception as exc:  # noqa: BLE001 — never fail a run on report write
        logger.warning("auto-report write failed for run_id=%s: %s", optimization_run_id, exc)
        return None


def run_optimization(
    recipe: OptimizationRecipe,
    *,
    project_root: Path | str,
    write_report: bool = True,
) -> dict:
    """Execute one Optuna study end-to-end.

    Returns a dict with run_id, champion pack_id (if any), trial count, best
    value, and (when ``write_report=True`` and writing succeeded) the
    auto-written report path.
    """
    project_root = Path(project_root)
    optimization_run_id = uuid.uuid4().hex
    storage_uri = _study_storage_uri(project_root, optimization_run_id)
    store = OptimizationStore(project_root=project_root)

    _print_run_banner(optimization_run_id, storage_uri, mode="start")

    baseline_pack = load_rule_pack(
        resolve_baseline_path(recipe.baseline_pack_path, project_root=project_root)
    )
    baseline_id = store.upsert_rule_pack(baseline_pack, lifecycle_status="backtested")

    folds = build_folds(
        recipe.from_date,
        recipe.to_date,
        train_months=recipe.walkforward.train_months,
        validation_months=recipe.walkforward.validation_months,
        step_months=recipe.walkforward.step_months,
    )
    if not folds:
        raise RuntimeError(
            f"no walk-forward folds fit in [{recipe.from_date}, {recipe.to_date}] "
            f"with train={recipe.walkforward.train_months}m val={recipe.walkforward.validation_months}m"
        )

    benchmark_by_fold = _benchmark_returns_per_fold(
        folds, recipe=recipe, project_root=project_root
    )

    logger.info(
        "optimizer start | run_id=%s strategy=%s folds=%d trials=%d window=%s..%s",
        optimization_run_id,
        recipe.strategy_id,
        len(folds),
        recipe.stopping.max_trials,
        recipe.from_date,
        recipe.to_date,
    )

    # Baseline on the same folds.
    baseline_folds = _evaluate_pack_on_folds(
        baseline_pack,
        folds,
        recipe=recipe,
        project_root=project_root,
        benchmark_by_fold=benchmark_by_fold,
        progress_label="baseline",
    )
    _persist_iteration(
        store,
        optimization_run_id=optimization_run_id,
        iteration=-1,  # baseline marker
        pack=baseline_pack,
        fold_results=baseline_folds,
        accepted=True,
        rejection_reason="baseline",
    )

    store.create_run(
        optimization_run_id=optimization_run_id,
        recipe_name=recipe.name,
        strategy_id=recipe.strategy_id,
        baseline_rule_pack_id=baseline_id,
        from_date=recipe.from_date,
        to_date=recipe.to_date,
        seed=recipe.seed,
        max_trials=recipe.stopping.max_trials,
        recipe_json=json.dumps(
            {**asdict(recipe), "from_date": recipe.from_date.isoformat(), "to_date": recipe.to_date.isoformat()},
            default=str,
        ),
        study_storage_uri=storage_uri,
    )

    # Champion bookkeeping is in closure state so the Optuna callback can update.
    state = {
        "champion_pack": None,
        "champion_folds": None,
        "champion_pack_id": None,
        "iterations_without_improvement": 0,
    }
    started_at = datetime.utcnow()

    progress = tqdm(
        total=recipe.stopping.max_trials,
        desc="optuna",
        unit="trial",
        ncols=100,
    )

    def objective(trial: optuna.Trial) -> float:
        pack = build_search_space(
            trial,
            strategy_id=recipe.strategy_id,
            overrides=recipe.search_space,
        )
        fold_results = _evaluate_pack_on_folds(
            pack,
            folds,
            recipe=recipe,
            project_root=project_root,
            benchmark_by_fold=benchmark_by_fold,
            progress_label=f"trial {trial.number}",
        )
        verdict = is_accepted(
            fold_results,
            champion_folds=state["champion_folds"],
            baseline_folds=baseline_folds,
            thresholds=recipe.acceptance,
        )
        pack_id = _persist_iteration(
            store,
            optimization_run_id=optimization_run_id,
            iteration=trial.number,
            pack=pack,
            fold_results=fold_results,
            accepted=verdict.accepted,
            rejection_reason=None if verdict.accepted else verdict.reason,
        )
        if verdict.accepted:
            state["champion_pack"] = pack
            state["champion_folds"] = fold_results
            state["champion_pack_id"] = pack_id
            state["iterations_without_improvement"] = 0
        else:
            state["iterations_without_improvement"] += 1
        return aggregate_fitness(fold_results)

    def early_stop_callback(study: optuna.Study, trial: optuna.trial.FrozenTrial) -> None:
        # Progress line: trial number, fitness, accepted/rejected, champion status.
        champion_marker = "👑" if state["champion_pack_id"] else " "
        progress.set_postfix_str(
            f"value={trial.value:.4f} no_improve={state['iterations_without_improvement']} {champion_marker}"
            if trial.value is not None
            else f"value=none no_improve={state['iterations_without_improvement']}"
        )
        progress.update(1)
        if state["iterations_without_improvement"] >= recipe.stopping.patience:
            study.stop()
        elapsed_min = (datetime.utcnow() - started_at).total_seconds() / 60.0
        if elapsed_min >= recipe.stopping.max_runtime_minutes:
            study.stop()

    sampler = TPESampler(seed=recipe.seed)
    optuna.logging.set_verbosity(optuna.logging.WARNING)  # don't double up with tqdm
    study = _open_journal_study(
        project_root=project_root,
        run_id=optimization_run_id,
        storage_uri=storage_uri,
        sampler=sampler,
        load_if_exists=False,
    )
    try:
        study.optimize(
            objective,
            n_trials=recipe.stopping.max_trials,
            callbacks=[early_stop_callback],
            gc_after_trial=True,
        )
        # Final champion guards. Promotion is the only way the pack advances
        # past 'backtested'; failing guards keeps the pack but records the
        # reason in the run row.
        guard_reason = None
        if state["champion_pack"] is not None and state["champion_folds"] is not None:
            verdict = champion_guards(state["champion_pack"], state["champion_folds"])
            if verdict.promote:
                store.set_lifecycle_status(
                    state["champion_pack_id"], "walkforward_passed"
                )
            else:
                guard_reason = verdict.reason
        store.complete_run(
            optimization_run_id=optimization_run_id,
            status="completed",
            champion_rule_pack_id=state["champion_pack_id"],
            error=f"champion_guard_failed: {guard_reason}" if guard_reason else None,
        )
    except Exception as exc:  # noqa: BLE001 — surface any failure to caller
        logger.exception("optimization run failed")
        store.complete_run(
            optimization_run_id=optimization_run_id,
            status="failed",
            champion_rule_pack_id=state["champion_pack_id"],
            error=str(exc),
        )
        raise
    finally:
        progress.close()

    logger.info(
        "optimizer done | run_id=%s trials=%d champion=%s",
        optimization_run_id,
        len(study.trials),
        state["champion_pack_id"],
    )

    report_path: Path | None = None
    if write_report:
        report_path = _write_run_report(
            project_root=project_root,
            recipe_name=recipe.name,
            optimization_run_id=optimization_run_id,
        )

    return {
        "optimization_run_id": optimization_run_id,
        "champion_rule_pack_id": state["champion_pack_id"],
        "trials": len(study.trials),
        "best_value": study.best_value if study.best_trial else None,
        "report_path": str(report_path) if report_path else None,
    }


def resume_optimization(
    optimization_run_id: str,
    *,
    project_root: Path | str,
    write_report: bool = True,
) -> dict:
    """Resume an interrupted optimization study by ``optimization_run_id``.

    Looks up the existing ``strategy_optimization_run`` row, rehydrates the
    recipe from ``recipe_json``, re-opens the Optuna journal at
    ``study_storage_uri`` (``load_if_exists=True``), and continues until the
    recipe's ``max_trials`` is hit.

    Idempotent in the face of replay: baseline rows are NOT re-inserted on
    resume (already present in ``strategy_iteration_result``); trial rows
    are protected by the ``(run_id, iteration, fold_index)`` unique index.

    Returns the same dict shape as ``run_optimization``. If the run is
    already ``status='completed'`` the function is a no-op (returns the
    existing champion + trial count).
    """
    project_root = Path(project_root)
    store = OptimizationStore(project_root=project_root)
    row = store.get_run_for_resume(optimization_run_id)
    if row is None:
        raise ValueError(f"unknown optimization_run_id: {optimization_run_id!r}")
    if row["status"] == "completed":
        logger.info("run %s is already completed; nothing to resume", optimization_run_id)
        return {
            "optimization_run_id": optimization_run_id,
            "champion_rule_pack_id": None,  # caller can query store for the row
            "trials": 0,
            "best_value": None,
            "report_path": None,
            "resumed": False,
        }

    storage_uri = row["study_storage_uri"]
    if not storage_uri:
        raise RuntimeError(
            f"run {optimization_run_id} has no study_storage_uri (pre-Wave-5a row?); "
            "cannot resume. Re-run from scratch."
        )
    abs_storage_path = project_root / storage_uri
    if not abs_storage_path.exists():
        raise FileNotFoundError(
            f"Optuna journal file missing for run {optimization_run_id}: {abs_storage_path}. "
            "Was data/optuna/ pruned?"
        )

    # Rehydrate the recipe from the stored JSON snapshot.
    recipe_payload = json.loads(row["recipe_json"])
    recipe = OptimizationRecipe.from_dict(recipe_payload)

    _print_run_banner(optimization_run_id, storage_uri, mode="resume")

    # Re-run the baseline in memory only (no DB persist) so the acceptance
    # gate has its comparison rows. Baseline rows already exist on disk
    # under iteration=-1 from the original run.
    baseline_pack = load_rule_pack(
        resolve_baseline_path(recipe.baseline_pack_path, project_root=project_root)
    )
    folds = build_folds(
        recipe.from_date,
        recipe.to_date,
        train_months=recipe.walkforward.train_months,
        validation_months=recipe.walkforward.validation_months,
        step_months=recipe.walkforward.step_months,
    )
    if not folds:
        raise RuntimeError(
            f"no walk-forward folds fit in [{recipe.from_date}, {recipe.to_date}] "
            f"with train={recipe.walkforward.train_months}m val={recipe.walkforward.validation_months}m"
        )
    benchmark_by_fold = _benchmark_returns_per_fold(
        folds, recipe=recipe, project_root=project_root
    )
    baseline_folds = _evaluate_pack_on_folds(
        baseline_pack, folds, recipe=recipe, project_root=project_root,
        benchmark_by_fold=benchmark_by_fold, progress_label="baseline (resume)",
    )

    # Open the existing journal. Optuna replays it on load; study.trials
    # already contains every prior trial.
    sampler = TPESampler(seed=recipe.seed)
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = _open_journal_study(
        project_root=project_root,
        run_id=optimization_run_id,
        storage_uri=storage_uri,
        sampler=sampler,
        load_if_exists=True,
    )

    completed_trials = len(study.trials)
    remaining = recipe.stopping.max_trials - completed_trials
    logger.info(
        "resume | run_id=%s completed=%d remaining=%d max_trials=%d",
        optimization_run_id, completed_trials, max(0, remaining),
        recipe.stopping.max_trials,
    )

    if remaining <= 0:
        # Nothing left to do — finalise the run row.
        store.complete_run(
            optimization_run_id=optimization_run_id,
            status="completed",
            champion_rule_pack_id=None,  # champion may have been written already
            error=None,
        )
        return {
            "optimization_run_id": optimization_run_id,
            "champion_rule_pack_id": None,
            "trials": completed_trials,
            "best_value": study.best_value if study.best_trial else None,
            "report_path": None,
            "resumed": True,
        }

    state = {
        "champion_pack": None,
        "champion_folds": None,
        "champion_pack_id": None,
        "iterations_without_improvement": 0,
    }
    started_at = datetime.utcnow()
    progress = tqdm(total=remaining, desc="optuna(resume)", unit="trial", ncols=100)

    def objective(trial: optuna.Trial) -> float:
        pack = build_search_space(
            trial, strategy_id=recipe.strategy_id, overrides=recipe.search_space,
        )
        fold_results = _evaluate_pack_on_folds(
            pack, folds, recipe=recipe, project_root=project_root,
            benchmark_by_fold=benchmark_by_fold,
            progress_label=f"trial {trial.number}",
        )
        verdict = is_accepted(
            fold_results, champion_folds=state["champion_folds"],
            baseline_folds=baseline_folds, thresholds=recipe.acceptance,
        )
        pack_id = _persist_iteration(
            store, optimization_run_id=optimization_run_id,
            iteration=trial.number, pack=pack, fold_results=fold_results,
            accepted=verdict.accepted,
            rejection_reason=None if verdict.accepted else verdict.reason,
        )
        if verdict.accepted:
            state["champion_pack"] = pack
            state["champion_folds"] = fold_results
            state["champion_pack_id"] = pack_id
            state["iterations_without_improvement"] = 0
        else:
            state["iterations_without_improvement"] += 1
        return aggregate_fitness(fold_results)

    def early_stop_callback(study: optuna.Study, trial: optuna.trial.FrozenTrial) -> None:
        champion_marker = "👑" if state["champion_pack_id"] else " "
        progress.set_postfix_str(
            f"value={trial.value:.4f} no_improve={state['iterations_without_improvement']} {champion_marker}"
            if trial.value is not None
            else f"value=none no_improve={state['iterations_without_improvement']}"
        )
        progress.update(1)
        if state["iterations_without_improvement"] >= recipe.stopping.patience:
            study.stop()
        elapsed_min = (datetime.utcnow() - started_at).total_seconds() / 60.0
        if elapsed_min >= recipe.stopping.max_runtime_minutes:
            study.stop()

    try:
        study.optimize(
            objective, n_trials=remaining,
            callbacks=[early_stop_callback], gc_after_trial=True,
        )
        guard_reason = None
        if state["champion_pack"] is not None and state["champion_folds"] is not None:
            verdict = champion_guards(state["champion_pack"], state["champion_folds"])
            if verdict.promote:
                store.set_lifecycle_status(
                    state["champion_pack_id"], "walkforward_passed"
                )
            else:
                guard_reason = verdict.reason
        store.complete_run(
            optimization_run_id=optimization_run_id, status="completed",
            champion_rule_pack_id=state["champion_pack_id"],
            error=f"champion_guard_failed: {guard_reason}" if guard_reason else None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("optimization resume failed")
        store.complete_run(
            optimization_run_id=optimization_run_id, status="failed",
            champion_rule_pack_id=state["champion_pack_id"], error=str(exc),
        )
        raise
    finally:
        progress.close()

    logger.info(
        "resume done | run_id=%s total_trials=%d new_trials=%d champion=%s",
        optimization_run_id, len(study.trials), remaining, state["champion_pack_id"],
    )

    report_path: Path | None = None
    if write_report:
        report_path = _write_run_report(
            project_root=project_root, recipe_name=recipe.name,
            optimization_run_id=optimization_run_id,
        )

    return {
        "optimization_run_id": optimization_run_id,
        "champion_rule_pack_id": state["champion_pack_id"],
        "trials": len(study.trials),
        "best_value": study.best_value if study.best_trial else None,
        "report_path": str(report_path) if report_path else None,
        "resumed": True,
    }
