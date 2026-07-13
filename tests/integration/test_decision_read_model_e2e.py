from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.domains.ranking.decision_history import DecisionHistoryRepository
from ai_trading_system.domains.publish.publish_payloads import build_publish_datasets
from ai_trading_system.pipeline.contracts import StageArtifact
from ai_trading_system.pipeline.registry import RegistryStore
from ai_trading_system.ui.execution_api.app import create_app
from ai_trading_system.ui.execution_api.services.readmodels.decision_reads import (
    DecisionOperatorReadService,
    Stage1LifecycleReadRepository,
)


RUN_DATE = "2026-07-10"
HEADERS = {"x-api-key": "e2e-key"}


def _context(registry: RegistryStore, *, run_id: str, run_date: str, mode: str = "LIVE") -> SimpleNamespace:
    return SimpleNamespace(
        registry=registry,
        run_id=run_id,
        run_date=run_date,
        attempt_number=1,
        params={
            "decision_write_mode": mode,
            "universe_id": "NSE_OPERATIONAL",
            "rank_model_version": "rank-e2e-v1",
            "stage_model_version": "stage-e2e-v1",
            "pattern_model_version": "pattern-e2e-v1",
        },
    )


def _rank_outputs() -> dict[str, pd.DataFrame]:
    return {
        "ranked_signals": pd.DataFrame(
            [
                {"symbol_id": "READY", "exchange": "NSE", "rank": 1, "composite_score": 88.0, "relative_strength": 91.0},
                {"symbol_id": "BLOCK", "exchange": "NSE", "rank": 2, "composite_score": 72.0, "relative_strength": 77.0},
                # Deliberately absent from Stage/Stage-1/pattern to test INCOMPLETE.
                {"symbol_id": "RANKONLY", "exchange": "NSE", "rank": 3, "composite_score": 65.0, "relative_strength": 69.0},
            ]
        ),
        "stock_scan": pd.DataFrame(
            [
                {"symbol_id": "READY", "exchange": "NSE", "stage_label": "STAGE_1", "stage_score": 84.0, "stage_reason": "base repair"},
                {"symbol_id": "BLOCK", "exchange": "NSE", "stage_label": "STAGE_4", "stage_score": 90.0, "stage_reason": "structural decline"},
            ]
        ),
        "stage1_scan": pd.DataFrame(
            [
                {
                    "symbol_id": "READY", "exchange": "NSE", "stage1_model_version": "stage1-e2e-v1",
                    "stage1_config_hash": "stage1-e2e-cfg", "stage1_maturity_score": 78.0,
                    "stage1_emerging_score": 82.0, "stage1_emerging_rank": 1,
                    "structural_repair_score": 80.0, "accumulation_score": 76.0,
                    "rs_acceleration_score": 74.0, "base_quality_score": 79.0,
                    "sector_rotation_score": 65.0, "pattern_readiness_score": 88.0,
                    "golden_cross_progression_score": 71.0, "stage1_eligible": True,
                    "promotion_eligibility": True, "golden_cross_status": "IMMINENT",
                    "distance_to_pivot_pct": 1.5,
                },
                {
                    "symbol_id": "BLOCK", "exchange": "NSE", "stage1_model_version": "stage1-e2e-v1",
                    "stage1_config_hash": "stage1-e2e-cfg", "stage1_maturity_score": 20.0,
                    "stage1_emerging_rank": 2, "stage1_eligible": False,
                    "promotion_eligibility": False, "stage1_block_reasons": '["STRUCTURE"]',
                },
            ]
        ),
        "pattern_scan": pd.DataFrame(
            [
                {"symbol_id": "READY", "exchange": "NSE", "pattern_family": "VCP", "pattern_state": "FORMING", "pattern_score": 80.0},
                {"symbol_id": "READY", "exchange": "NSE", "pattern_family": "CUP", "pattern_state": "READY", "pattern_score": 76.0},
            ]
        ),
    }


def _lifecycle_state() -> pd.DataFrame:
    common = {
        "exchange": "NSE", "trade_date": RUN_DATE,
        "stage1_lifecycle_model_version": "lifecycle-e2e-v1",
        "stage1_lifecycle_config_hash": "lifecycle-e2e-cfg",
        "execution_eligible": False,
    }
    return pd.DataFrame(
        [
            {
                **common, "symbol_id": "READY", "stage1_lifecycle_state": "BREAKOUT_READY",
                "stage1_previous_lifecycle_state": "LATE_STAGE1", "stage1_substate": "STAGE_1_BREAKOUT_READY",
                "stage1_maturity_score": 78.0, "stage1_score_delta_20d": 9.0,
                "stage1_emerging_rank": 1, "emerging_rank_improvement_20d": 12.0,
                "stage1_eligible": True, "promotion_eligibility": True,
                "golden_cross_status": "IMMINENT", "pattern_promotion_state": "CONFIRMED",
                "distance_to_pivot_pct": 1.5, "stage1_evaluation_status": "COMPLETE",
            },
            {
                **common, "symbol_id": "BLOCK", "stage1_lifecycle_state": "BASE_BUILDING",
                "stage1_substate": "NOT_STAGE1", "stage1_maturity_score": 20.0,
                "stage1_emerging_rank": 2, "stage1_eligible": False,
                "promotion_eligibility": False, "stage1_evaluation_status": "STRUCTURALLY_BLOCKED",
            },
        ]
    )


def _transitions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol_id": "READY", "exchange": "NSE", "trade_date": RUN_DATE,
                "from_lifecycle_state": "LATE_STAGE1", "to_lifecycle_state": "BREAKOUT_READY",
                "transition_type": "LIFECYCLE", "transition_summary": "LATE_STAGE1 → BREAKOUT_READY",
                "stage1_score_before": 69.0, "stage1_score_after": 78.0,
                "stage1_lifecycle_model_version": "lifecycle-e2e-v1",
                "stage1_lifecycle_config_hash": "lifecycle-e2e-cfg",
            },
            {
                "symbol_id": "BLOCK", "exchange": "NSE", "trade_date": RUN_DATE,
                "from_lifecycle_state": "ACCUMULATING", "to_lifecycle_state": "REGRESSED",
                "transition_type": "REGRESSION", "transition_summary": "ACCUMULATING → REGRESSED",
                "stage1_lifecycle_model_version": "lifecycle-e2e-v1",
                "stage1_lifecycle_config_hash": "lifecycle-e2e-cfg",
            },
        ]
    )


def _approve_persisted_versions(db_path: Path) -> None:
    specs = (
        ("rank", "rank_history", "rank_model_version", "rank_config_hash"),
        ("stage", "stage_history", "stage_model_version", "stage_config_hash"),
        ("stage1", "stage1_history", "stage1_model_version", "stage1_config_hash"),
        ("pattern", "pattern_history", "pattern_model_version", "pattern_config_hash"),
    )
    with duckdb.connect(str(db_path)) as conn:
        for domain, table, version_column, config_column in specs:
            version, config_hash = conn.execute(
                f"SELECT DISTINCT {version_column}, {config_column} FROM {table}"
            ).fetchone()
            conn.execute(
                """INSERT OR REPLACE INTO decision_model_deployment
                   (decision_domain, model_version, config_hash, environment, effective_from, status, approved_by)
                   VALUES (?, ?, ?, 'production', DATE '2026-01-01', 'approved', 'e2e-test')""",
                [domain, version, config_hash],
            )


@pytest.fixture
def decision_e2e(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, TestClient]:
    data_root = tmp_path / "runtime"
    data_root.mkdir()
    monkeypatch.setenv("DATA_ROOT", str(data_root))
    # DATA_ROOT is honored only for a canonical checkout root; keep the app
    # rooted at the repo while isolating every runtime file under tmp_path.
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(Path.cwd()))
    monkeypatch.setenv("EXECUTION_API_KEY", "e2e-key")
    db_path = data_root / "control_plane.duckdb"
    registry = RegistryStore(tmp_path, db_path=db_path)
    repository = DecisionHistoryRepository(registry)
    context = _context(registry, run_id="e2e-live", run_date=RUN_DATE)

    outputs = _rank_outputs()
    artifact_dir = data_root / "pipeline_runs" / context.run_id / "rank" / "attempt_1"
    artifact_dir.mkdir(parents=True)
    artifact_names = {
        "ranked_signals": "ranked_signals.csv", "stock_scan": "stock_scan.csv",
        "stage1_scan": "stage1_scan.csv", "pattern_scan": "pattern_scan.csv",
    }
    for key, filename in artifact_names.items():
        outputs[key].to_csv(artifact_dir / filename, index=False)

    first_rank = repository.persist_rank_outputs(context, outputs)
    second_rank = repository.persist_rank_outputs(context, outputs)
    first_lifecycle = repository.persist_lifecycle(context, _lifecycle_state(), _transitions())
    second_lifecycle = repository.persist_lifecycle(context, _lifecycle_state(), _transitions())
    assert first_rank["persistence_valid"] and second_rank["persistence_valid"]
    assert first_lifecycle["persistence_valid"] and second_lifecycle["persistence_valid"]
    assert second_lifecycle["duplicate_transition_rows_skipped"] == 2

    current_before = repository.get_stage1_current_state("READY").copy()
    replay = _context(registry, run_id="e2e-replay", run_date="2025-01-03", mode="REPLAY")
    replay_state = _lifecycle_state().loc[lambda frame: frame["symbol_id"].eq("READY")].copy()
    replay_state.loc[:, "trade_date"] = "2025-01-03"
    replay_state.loc[:, "stage1_lifecycle_state"] = "ACCUMULATING"
    repository.persist_lifecycle(replay, replay_state, pd.DataFrame())
    pd.testing.assert_frame_equal(current_before, repository.get_stage1_current_state("READY"))

    _approve_persisted_versions(db_path)
    return data_root, db_path, TestClient(create_app())


def test_rank_investigator_duckdb_to_api_and_ui_contract(decision_e2e: tuple[Path, Path, TestClient]) -> None:
    data_root, db_path, client = decision_e2e
    with duckdb.connect(str(db_path), read_only=True) as conn:
        counts = {
            table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "rank_history", "stage_history", "stage1_history", "pattern_history",
                "investigator_stage1_state", "investigator_stage1_current",
                "investigator_stage1_transition", "decision_model_deployment",
            )
        }
        assert counts["rank_history"] == 3
        assert counts["stage_history"] == 2
        assert counts["stage1_history"] == 2
        assert counts["pattern_history"] == 2
        assert counts["investigator_stage1_current"] == 2
        assert counts["investigator_stage1_transition"] == 2

    reconciliation = Stage1LifecycleReadRepository(Path.cwd(), db_path=db_path).reconciliation()
    assert reconciliation == {
        "missing_current_rows": 0, "duplicate_current_keys": 0,
        "date_mismatches": 0, "state_mismatches": 0, "stale_current_rows": 0,
    }

    current = client.get("/api/execution/investigator/stage1/current?include_blocked=true", headers=HEADERS)
    assert current.status_code == 200
    assert {row["symbol_id"] for row in current.json()["rows"]} == {"READY", "BLOCK"}
    assert next(row for row in current.json()["rows"] if row["symbol_id"] == "READY")["pipeline_run_id"] == "e2e-live"

    analytics = client.get("/api/execution/investigator/stage1/READY/analytics-history", headers=HEADERS)
    patterns = client.get("/api/stocks/READY/pattern-history", headers=HEADERS)
    combined = client.get("/api/stocks/READY/decision-history", headers=HEADERS)
    ranking = client.get("/api/execution/ranking?limit=10", headers=HEADERS)
    diagnostics = client.get("/api/health/decision-read-sources", headers=HEADERS)
    assert analytics.status_code == patterns.status_code == combined.status_code == ranking.status_code == diagnostics.status_code == 200
    assert analytics.json()["rows"][0]["stage1_maturity_score"] == 78.0
    assert {row["pattern_family"] for row in patterns.json()["rows"]} == {"VCP", "CUP"}
    assert combined.json()["aligned"][0]["rank"]["rank_position"] == 1
    assert [row["symbol_id"] for row in ranking.json()["top_ranked"]] == [
        "READY",
        "BLOCK",
        "RANKONLY",
    ]
    assert all(not row["fallback_used"] for row in diagnostics.json()["decision_read_source_summary"])

    service = DecisionOperatorReadService(Path.cwd())
    payload = service.current(trade_date=RUN_DATE)
    by_symbol = {row["symbol_id"]: row for row in payload["rows"]}
    assert by_symbol["READY"]["data_freshness_status"] == "ALIGNED"
    assert by_symbol["RANKONLY"]["data_freshness_status"] == "INCOMPLETE"

    rank_dir = data_root / "pipeline_runs/e2e-live/rank/attempt_1"
    artifacts = {
        name: StageArtifact.from_file(name, rank_dir / filename)
        for name, filename in {
            "ranked_signals": "ranked_signals.csv", "stock_scan": "stock_scan.csv",
            "pattern_scan": "pattern_scan.csv",
        }.items()
    }
    datasets = build_publish_datasets(
        context_artifact_for=lambda name: artifacts.get(name),
        read_artifact=lambda artifact: pd.read_csv(artifact.uri),
        read_json_artifact=lambda _artifact: {},
        ranked_signals_artifact=artifacts["ranked_signals"],
        project_root=Path.cwd(), run_date=RUN_DATE, run_id="e2e-live",
    )
    assert len(datasets["ranked_signals"]) == 3
    assert len(datasets["stock_scan"]) == 2
    assert len(datasets["pattern_scan"]) == 2
    assert all(source["data_source"] == "DUCKDB" for source in datasets["decision_read_source_summary"])
    assert all(not source["fallback_used"] for source in datasets["decision_read_source_summary"])


def test_historical_filters_are_bounded_and_duckdb_only(decision_e2e: tuple[Path, Path, TestClient]) -> None:
    _, _, client = decision_e2e
    response = client.get(
        "/api/stocks/READY/rank-history?from=2026-07-01&to=2026-07-10&exchange=NSE"
        "&model_version=rank-e2e-v1&limit=1&offset=0",
        headers=HEADERS,
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["rows"]) == 1
    assert payload["metadata"]["data_source"] == "DUCKDB"
    assert payload["metadata"]["fallback_used"] is False


def test_current_publish_fallback_is_explicit_when_duckdb_is_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing_root = tmp_path / "missing-runtime"
    missing_root.mkdir()
    monkeypatch.setenv("DATA_ROOT", str(missing_root))
    ranked = tmp_path / "ranked_signals.csv"
    stock = tmp_path / "stock_scan.csv"
    pattern = tmp_path / "pattern_scan.csv"
    pd.DataFrame([{"symbol_id": "FALLBACK", "composite_score": 80.0}]).to_csv(ranked, index=False)
    pd.DataFrame([{"symbol_id": "FALLBACK", "stage_label": "STAGE_1"}]).to_csv(stock, index=False)
    pd.DataFrame([{"symbol_id": "FALLBACK", "pattern_family": "VCP"}]).to_csv(pattern, index=False)
    artifacts = {
        "ranked_signals": StageArtifact.from_file("ranked_signals", ranked),
        "stock_scan": StageArtifact.from_file("stock_scan", stock),
        "pattern_scan": StageArtifact.from_file("pattern_scan", pattern),
    }
    datasets = build_publish_datasets(
        context_artifact_for=lambda name: artifacts.get(name),
        read_artifact=lambda artifact: pd.read_csv(artifact.uri),
        read_json_artifact=lambda _artifact: {},
        ranked_signals_artifact=artifacts["ranked_signals"],
        project_root=Path.cwd(), run_date=RUN_DATE, run_id="fallback-run",
    )
    sources = datasets["decision_read_source_summary"]
    assert {source["domain"] for source in sources} == {"rank", "stage", "pattern"}
    assert all(source["data_source"] == "ARTIFACT_FALLBACK" for source in sources)
    assert all(source["fallback_used"] and source["fallback_run_id"] == "fallback-run" for source in sources)
