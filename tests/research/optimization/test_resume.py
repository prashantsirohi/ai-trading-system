"""Wave 5a: tests for the resume CLI and the store/migration changes.

Engine-free tests (no Optuna invocations) cover:
  - Migration 018 adds the ``study_storage_uri`` column.
  - ``OptimizationStore.create_run`` persists the URI and
    ``complete_run`` preserves it through its DELETE+INSERT dance.
  - ``OptimizationStore.get_run_for_resume`` returns the right rehydration
    payload (or None for unknown ids).
  - ``ai-trading-optimize resume`` CLI surfaces clear errors for unknown
    run ids, missing journal files, and missing storage_uri (pre-Wave-5a
    rows). Happy-path resume (a real Optuna study) is covered transitively
    by ``test_runner_integration.py`` in environments where optuna is
    installed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ai_trading_system.research.optimization.cli import main as cli_main
from ai_trading_system.research.optimization.store import OptimizationStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bootstrap_store(tmp_path: Path) -> OptimizationStore:
    """Return a fresh OptimizationStore with migrations applied."""
    store = OptimizationStore(project_root=tmp_path)
    # Touching create_run/get_run_for_resume forces _ensure_initialized which
    # runs all migrations 001..018 against a fresh DuckDB under tmp_path.
    return store


def _seed_baseline_pack(store: OptimizationStore) -> str:
    """Insert a minimal rule pack so create_run's FK-like reference is real."""
    from ai_trading_system.domains.strategy import StrategyRulePack
    pack = StrategyRulePack(strategy_id="t", ranking={"weights": {
        "relative_strength": 0.38, "volume_intensity": 0.0,
        "trend_persistence": 0.22, "momentum_acceleration": 0.0,
        "proximity_highs": 0.18, "delivery_pct": 0.0, "sector_strength": 0.22,
    }})
    return store.upsert_rule_pack(pack, lifecycle_status="backtested")


# ---------------------------------------------------------------------------
# Migration + store
# ---------------------------------------------------------------------------


def test_migration_018_adds_study_storage_uri_column(tmp_path: Path) -> None:
    import duckdb
    from ai_trading_system.pipeline.registry import RegistryStore

    # Force migrations to run.
    RegistryStore(project_root=tmp_path)._ensure_initialized()  # noqa: SLF001
    cp_path = tmp_path / "data" / "control_plane.duckdb"
    conn = duckdb.connect(str(cp_path), read_only=True)
    try:
        cols = {
            r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'strategy_optimization_run'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert "study_storage_uri" in cols


def test_create_run_persists_study_storage_uri(tmp_path: Path) -> None:
    store = _bootstrap_store(tmp_path)
    baseline_id = _seed_baseline_pack(store)
    from datetime import date
    store.create_run(
        optimization_run_id="run-A", recipe_name="rA", strategy_id="t",
        baseline_rule_pack_id=baseline_id,
        from_date=date(2024, 1, 1), to_date=date(2024, 6, 1),
        seed=42, max_trials=10, recipe_json="{}",
        study_storage_uri="data/optuna/run-A.log",
    )
    row = store.get_run_for_resume("run-A")
    assert row is not None
    assert row["study_storage_uri"] == "data/optuna/run-A.log"
    assert row["status"] == "running"
    assert row["max_trials"] == 10


def test_create_run_default_storage_uri_is_none(tmp_path: Path) -> None:
    """Backwards compat: old callers that don't pass study_storage_uri get NULL."""
    store = _bootstrap_store(tmp_path)
    baseline_id = _seed_baseline_pack(store)
    from datetime import date
    store.create_run(
        optimization_run_id="run-B", recipe_name="rB", strategy_id="t",
        baseline_rule_pack_id=baseline_id,
        from_date=date(2024, 1, 1), to_date=date(2024, 6, 1),
        seed=42, max_trials=10, recipe_json="{}",
    )
    row = store.get_run_for_resume("run-B")
    assert row is not None
    assert row["study_storage_uri"] is None


def test_complete_run_preserves_study_storage_uri(tmp_path: Path) -> None:
    store = _bootstrap_store(tmp_path)
    baseline_id = _seed_baseline_pack(store)
    from datetime import date
    store.create_run(
        optimization_run_id="run-C", recipe_name="rC", strategy_id="t",
        baseline_rule_pack_id=baseline_id,
        from_date=date(2024, 1, 1), to_date=date(2024, 6, 1),
        seed=42, max_trials=10, recipe_json="{}",
        study_storage_uri="data/optuna/run-C.log",
    )
    store.complete_run(
        optimization_run_id="run-C", status="completed",
        champion_rule_pack_id=None, error=None,
    )
    row = store.get_run_for_resume("run-C")
    assert row is not None
    assert row["status"] == "completed"
    assert row["study_storage_uri"] == "data/optuna/run-C.log"


def test_get_run_for_resume_unknown_returns_none(tmp_path: Path) -> None:
    store = _bootstrap_store(tmp_path)
    assert store.get_run_for_resume("nope") is None


# ---------------------------------------------------------------------------
# CLI dispatch + error paths
# ---------------------------------------------------------------------------


def test_resume_unknown_run_id_returns_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("optuna")  # cli's resume handler imports runner.py
    rc = cli_main(["resume", "does-not-exist", "--project-root", str(tmp_path)])
    assert rc == 2
    out = capsys.readouterr().out
    assert "unknown optimization_run_id" in out


def test_resume_no_storage_uri_returns_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A row without study_storage_uri (pre-Wave-5a) must fail clearly."""
    pytest.importorskip("optuna")
    store = _bootstrap_store(tmp_path)
    baseline_id = _seed_baseline_pack(store)
    from datetime import date
    store.create_run(
        optimization_run_id="run-old", recipe_name="rOld", strategy_id="t",
        baseline_rule_pack_id=baseline_id,
        from_date=date(2024, 1, 1), to_date=date(2024, 6, 1),
        seed=42, max_trials=10, recipe_json="{}",
        # study_storage_uri intentionally omitted.
    )
    rc = cli_main(["resume", "run-old", "--project-root", str(tmp_path)])
    assert rc == 2
    out = capsys.readouterr().out
    assert "no study_storage_uri" in out


def test_resume_missing_journal_file_returns_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("optuna")
    store = _bootstrap_store(tmp_path)
    baseline_id = _seed_baseline_pack(store)
    from datetime import date
    store.create_run(
        optimization_run_id="run-gone", recipe_name="rGone", strategy_id="t",
        baseline_rule_pack_id=baseline_id,
        from_date=date(2024, 1, 1), to_date=date(2024, 6, 1),
        seed=42, max_trials=10, recipe_json="{}",
        study_storage_uri="data/optuna/run-gone.log",  # file not created
    )
    rc = cli_main(["resume", "run-gone", "--project-root", str(tmp_path)])
    assert rc == 2
    out = capsys.readouterr().out
    assert "Optuna journal file missing" in out


def test_resume_completed_run_is_noop(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("optuna")
    store = _bootstrap_store(tmp_path)
    baseline_id = _seed_baseline_pack(store)
    from datetime import date
    store.create_run(
        optimization_run_id="run-done", recipe_name="rDone", strategy_id="t",
        baseline_rule_pack_id=baseline_id,
        from_date=date(2024, 1, 1), to_date=date(2024, 6, 1),
        seed=42, max_trials=10, recipe_json="{}",
        study_storage_uri="data/optuna/run-done.log",
    )
    store.complete_run(
        optimization_run_id="run-done", status="completed",
        champion_rule_pack_id=None, error=None,
    )
    rc = cli_main(["resume", "run-done", "--project-root", str(tmp_path)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "resumed=False" in out


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------


def test_resume_is_a_known_subcommand() -> None:
    from ai_trading_system.research.optimization.cli import _SUBCOMMANDS
    assert "resume" in _SUBCOMMANDS
