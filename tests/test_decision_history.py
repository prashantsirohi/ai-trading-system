from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from ai_trading_system.domains.ranking.decision_history import DecisionHistoryRepository
from ai_trading_system.pipeline.registry import RegistryStore


def _repo(tmp_path: Path) -> tuple[DecisionHistoryRepository, RegistryStore]:
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    return DecisionHistoryRepository(registry), registry


def _context(registry: RegistryStore, *, run_id: str = "run-1", run_date: str = "2026-07-10", mode: str = "LIVE"):
    return SimpleNamespace(
        registry=registry,
        run_id=run_id,
        run_date=run_date,
        attempt_number=1,
        params={"decision_write_mode": mode, "rank_model_version": "rank-v1"},
    )


def test_rank_decision_histories_are_versioned_and_idempotent(tmp_path: Path) -> None:
    repo, registry = _repo(tmp_path)
    context = _context(registry)
    outputs = {
        "ranked_signals": pd.DataFrame([{
            "symbol_id": "ABC", "exchange": "NSE", "composite_score": 80.0,
            "rank": 1, "relative_strength": 90.0, "volume_intensity": 70.0,
            "trend_persistence": 75.0, "proximity_to_highs": 85.0, "sector_strength": 60.0,
        }]),
        "stock_scan": pd.DataFrame([{
            "symbol_id": "ABC", "exchange": "NSE", "stage_label": "STAGE_1",
            "stage_score": 82.0, "close": 101.0, "sma_50": 98.0, "sma_200": 95.0,
        }]),
        "stage1_scan": pd.DataFrame([{
            "symbol_id": "ABC", "exchange": "NSE", "stage1_model_version": "v1",
            "stage1_config_hash": "cfg", "stage1_maturity_score": 71.0,
            "stage1_eligible": True, "stage1_block_reasons": "[]",
        }]),
        "pattern_scan": pd.DataFrame([{
            "symbol_id": "ABC", "exchange": "NSE", "pattern_family": "VCP",
            "pattern_state": "FORMING", "pattern_score": 66.0,
        }]),
    }

    first = repo.persist_rank_outputs(context, outputs)
    outputs["ranked_signals"].loc[0, "composite_score"] = 81.0
    second = repo.persist_rank_outputs(context, outputs)

    assert first["persistence_valid"] and second["persistence_valid"]
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM rank_history").fetchone()[0] == 1
        assert conn.execute("SELECT composite_score FROM rank_history").fetchone()[0] == 81.0
        assert conn.execute("SELECT COUNT(*) FROM stage_history").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM stage1_history").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM pattern_history").fetchone()[0] == 1
    assert len(repo.get_stage1_history("ABC")) == 1
    assert len(repo.get_stage1_cohort("2026-07-10", minimum_score=70)) == 1


def _state(date: str, state: str = "BASE_BUILDING") -> pd.DataFrame:
    return pd.DataFrame([{
        "symbol_id": "ABC", "exchange": "NSE", "trade_date": date,
        "stage1_lifecycle_state": state, "stage1_substate": "STAGE_1_BASE",
        "stage1_maturity_score": 60.0, "stage1_lifecycle_model_version": "life-v1",
        "stage1_lifecycle_config_hash": "life-cfg", "candidate_sources": '["RANK"]',
    }])


def _transition(date: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "symbol_id": "ABC", "exchange": "NSE", "trade_date": date,
        "from_lifecycle_state": None, "to_lifecycle_state": "BASE_BUILDING",
        "transition_type": "LIFECYCLE", "transition_reason_codes": '["NEW_DISCOVERY"]',
        "stage1_lifecycle_model_version": "life-v1", "stage1_lifecycle_config_hash": "life-cfg",
    }])


def test_lifecycle_transaction_is_idempotent_and_replay_safe(tmp_path: Path) -> None:
    repo, registry = _repo(tmp_path)
    live = _context(registry, run_date="2026-07-10")
    first = repo.persist_lifecycle(live, _state("2026-07-10"), _transition("2026-07-10"))
    second = repo.persist_lifecycle(live, _state("2026-07-10"), _transition("2026-07-10"))
    assert first["stage1_transition_rows_inserted"] == 1
    assert second["stage1_transition_rows_inserted"] == 0
    assert second["duplicate_transition_rows_skipped"] == 1
    assert str(repo.get_stage1_current_state("ABC").iloc[0]["as_of_trade_date"].date()) == "2026-07-10"

    replay = _context(registry, run_id="old-replay", run_date="2025-01-03", mode="REPLAY")
    repo.persist_lifecycle(replay, _state("2025-01-03", "ACCUMULATING"), pd.DataFrame())
    current = repo.get_stage1_current_state("ABC").iloc[0]
    assert str(current["as_of_trade_date"].date()) == "2026-07-10"
    assert current["stage1_lifecycle_state"] == "BASE_BUILDING"
    assert len(repo.get_stage1_transitions("ABC")) == 1


def test_live_older_date_cannot_replace_current(tmp_path: Path) -> None:
    repo, registry = _repo(tmp_path)
    repo.persist_lifecycle(_context(registry, run_date="2026-07-10"), _state("2026-07-10"), pd.DataFrame())
    repo.persist_lifecycle(_context(registry, run_id="older", run_date="2026-07-01"), _state("2026-07-01", "REGRESSED"), pd.DataFrame())
    current = repo.get_stage1_current_state("ABC").iloc[0]
    assert str(current["as_of_trade_date"].date()) == "2026-07-10"
    assert current["stage1_lifecycle_state"] == "BASE_BUILDING"


def test_migration_backfills_current_and_is_repeatable(tmp_path: Path) -> None:
    _, registry = _repo(tmp_path)
    migration = (
        Path(__file__).parents[1]
        / "src/ai_trading_system/pipeline/migrations/029_decision_history.sql"
    ).read_text(encoding="utf-8")
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute("DELETE FROM investigator_stage1_current")
        conn.execute("DROP INDEX IF EXISTS uq_investigator_stage1_transition_id")
        conn.execute(
            """INSERT INTO investigator_stage1_transition
               (symbol_id, exchange, trade_date, from_lifecycle_state,
                to_lifecycle_state, transition_type, run_id, attempt_number, transition_id)
               VALUES
               ('DUP', 'NSE', DATE '2026-07-09', NULL, 'BASE_BUILDING', 'LIFECYCLE', 'run-a', 1, 'old-duplicate'),
               ('DUP', 'NSE', DATE '2026-07-09', NULL, 'BASE_BUILDING', 'LIFECYCLE', 'run-a', 2, 'old-duplicate')"""
        )
        conn.execute(
            """INSERT INTO investigator_stage1_state
               (symbol_id, exchange, trade_date, stage1_lifecycle_state,
                stage1_lifecycle_model_version, stage1_lifecycle_config_hash,
                pipeline_run_id, attempt_number)
               VALUES ('BACKFILL', 'NSE', DATE '2026-07-09', 'ACCUMULATING',
                       'life-v1', 'cfg', 'historical-run', 2)"""
        )
        conn.execute(migration)
        conn.execute(migration)
        row = conn.execute(
            "SELECT as_of_trade_date, stage1_lifecycle_state FROM investigator_stage1_current WHERE symbol_id='BACKFILL'"
        ).fetchone()
        history_count = conn.execute(
            "SELECT COUNT(*) FROM investigator_stage1_state WHERE symbol_id='BACKFILL'"
        ).fetchone()[0]
        transition_ids = conn.execute(
            "SELECT COUNT(DISTINCT transition_id) FROM investigator_stage1_transition WHERE symbol_id='DUP'"
        ).fetchone()[0]
    assert str(row[0]) == "2026-07-09"
    assert row[1] == "ACCUMULATING"
    assert history_count == 1
    assert transition_ids == 2
