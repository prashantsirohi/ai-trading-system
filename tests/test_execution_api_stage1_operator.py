from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.ui.execution_api.app import create_app


HEADERS = {"x-api-key": "test-key"}


def _seed(tmp_path: Path, monkeypatch) -> TestClient:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", "test-key")
    RegistryStore(tmp_path)
    db = tmp_path / "data" / "control_plane.duckdb"
    with duckdb.connect(str(db)) as conn:
        conn.execute("DROP TABLE IF EXISTS investigator_stage1_transition")
        conn.execute("DROP TABLE IF EXISTS investigator_stage1_state")
        migration = Path("src/ai_trading_system/pipeline/migrations/028_investigator_stage1_lifecycle.sql").read_text()
        conn.execute(migration)
        conn.execute("ALTER TABLE investigator_stage1_state ADD COLUMN promotion_eligibility BOOLEAN")
        conn.execute("""INSERT INTO investigator_stage1_state
            (symbol_id, trade_date, stage1_lifecycle_state, stage1_previous_lifecycle_state,
             stage1_substate, stage1_maturity_score, stage1_score_delta_20d,
                 stage1_emerging_rank, emerging_rank_improvement_20d, stage1_first_seen_date,
                 pattern_promotion_state, golden_cross_status,
             distance_to_pivot_pct, stage1_eligible, stage1_evaluation_status,
             promotion_eligibility, execution_eligible)
            VALUES
            ('OLD', '2026-07-10', 'BASE_BUILDING', NULL, 'STAGE_1_BASE', 41, 1, 30, 2, '2026-07-01', 'NONE', 'APPROACHING', 10, TRUE, 'COMPLETE', FALSE, FALSE),
            ('READY', '2026-07-11', 'BREAKOUT_READY', 'LATE_STAGE1', 'STAGE_1_BREAKOUT_READY', 78, 9, 2, 18, '2026-06-20', 'CONFIRMED', 'IMMINENT', 1.5, TRUE, 'COMPLETE', TRUE, FALSE),
            ('LATE', '2026-07-11', 'LATE_STAGE1', 'ACCUMULATING', 'STAGE_1_LATE', 69, 5, 12, 8, '2026-06-25', 'PENDING_3D', 'APPROACHING', 4, TRUE, 'COMPLETE', TRUE, FALSE),
            ('BLOCK', '2026-07-11', 'BASE_BUILDING', NULL, 'NOT_STAGE1', 20, -2, NULL, NULL, '2026-07-11', 'NONE', 'UNKNOWN', NULL, FALSE, 'STRUCTURALLY_BLOCKED', FALSE, FALSE)
        """)
        conn.execute("""INSERT INTO investigator_stage1_transition
            (symbol_id, trade_date, from_lifecycle_state, to_lifecycle_state, stage1_score_before,
             stage1_score_after, emerging_rank_before, emerging_rank_after, transition_type,
             transition_summary)
            VALUES ('READY', '2026-07-11', 'LATE_STAGE1', 'BREAKOUT_READY', 69, 78, 20, 2,
                    'LIFECYCLE', 'LATE_STAGE1 → BREAKOUT_READY')""")
        conn.execute("""INSERT INTO investigator_stage1_transition
            (symbol_id, trade_date, from_lifecycle_state, to_lifecycle_state, transition_type,
             transition_summary, attempt_number)
            VALUES ('DISC', '2026-07-11', NULL, 'BASE_BUILDING', 'LIFECYCLE', 'NEW → BASE_BUILDING', 1),
                   ('DISC', '2026-07-11', NULL, 'BASE_BUILDING', 'LIFECYCLE', 'NEW → BASE_BUILDING', 2)""")
        conn.execute("""INSERT INTO investigator_scores
            (symbol_id, trade_date, close, sector, price_structure_score, volume_delivery_score,
             sector_support_score, execution_eligible)
            VALUES ('READY','2026-07-11',100,'Industrials',80,70,60,FALSE),
                   ('LATE','2026-07-11',90,'Technology',70,65,55,FALSE)""")
        # Current operator views use the physical one-row-per-symbol table.
        conn.execute("UPDATE investigator_stage1_state SET run_id = 'fixture-run', attempt_number = 1")
        migration_029 = Path("src/ai_trading_system/pipeline/migrations/029_decision_history.sql").read_text()
        conn.execute(migration_029)
        migration_030 = Path("src/ai_trading_system/pipeline/migrations/030_decision_model_deployment.sql").read_text()
        conn.execute(migration_030)
    return TestClient(create_app())


def test_stage1_summary_current_filters_and_detail(tmp_path: Path, monkeypatch) -> None:
    client = _seed(tmp_path, monkeypatch)
    summary = client.get("/api/execution/investigator/stage1/summary", headers=HEADERS).json()
    assert summary["as_of"] == "2026-07-11"
    assert summary["active_count"] == 2
    assert summary["breakout_ready_count"] == 1
    current = client.get("/api/execution/investigator/stage1/current", headers=HEADERS).json()
    assert [row["symbol_id"] for row in current["rows"]] == ["READY", "LATE"]
    assert current["rows"][0]["operator_status"] == "WATCH_CLOSELY"
    assert current["rows"][0]["operator_priority"] == "HIGH"
    assert current["rows"][0]["execution_eligible"] is False
    assert "score +9.0 over 20D" in current["rows"][0]["operator_reason"]
    blocked = client.get("/api/execution/investigator/stage1/current?include_blocked=true&operator_status=BLOCKED", headers=HEADERS).json()
    assert [row["symbol_id"] for row in blocked["rows"]] == ["BLOCK"]
    detail = client.get("/api/execution/investigator/stage1/READY", headers=HEADERS).json()
    assert detail["state"] and detail["transitions"]
    assert detail["current"]["symbol_id"] == "READY"
    transitions = client.get("/api/execution/investigator/stage1/transitions", headers=HEADERS).json()
    discovered = [row for row in transitions["rows"] if row["symbol_id"] == "DISC"]
    assert len(discovered) == 1
    assert discovered[0]["attempt_number"] == 2


def test_stage1_missing_database_is_empty(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", "test-key")
    response = TestClient(create_app()).get("/api/execution/investigator/stage1/current", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["rows"] == []
