"""Tests for the optimization router (`/api/execution/optimization/*`).

Seeds a fresh control_plane.duckdb with the four optimizer tables and a
small two-recipe / three-run / six-trial fixture, then exercises every
endpoint. Mirrors the style of `test_execution_api_runs_introspection.py`.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-api-key"}

# Two recipes, three runs (latest one is the leaderboard champion for recipe A).
RECIPE_A = "momentum_breakout_optuna_v1"
RECIPE_B = "mean_reversion_v1"

RUN_A_OLD = "run-a-old"
RUN_A_NEW = "run-a-new"
RUN_B = "run-b-1"

PACK_BASELINE_A = "pack-baseline-a"
PACK_CHAMPION_A_OLD = "pack-champ-a-old"
PACK_CHAMPION_A_NEW = "pack-champ-a-new"
PACK_BASELINE_B = "pack-baseline-b"
PACK_CHAMPION_B = "pack-champ-b"


def _seed_control_plane(cp_path: Path) -> None:
    cp_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(cp_path))
    try:
        # Schema mirrors migration 015_strategy_optimizer.sql.
        conn.execute(
            """
            CREATE TABLE strategy_rule_pack (
                rule_pack_id TEXT NOT NULL,
                parent_rule_pack_id TEXT,
                strategy_id TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                rule_yaml TEXT NOT NULL,
                rule_json TEXT NOT NULL,
                lifecycle_status TEXT NOT NULL DEFAULT 'draft',
                description TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE strategy_optimization_run (
                optimization_run_id TEXT NOT NULL,
                recipe_name TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                baseline_rule_pack_id TEXT NOT NULL,
                from_date DATE NOT NULL,
                to_date DATE NOT NULL,
                seed INTEGER NOT NULL,
                max_trials INTEGER NOT NULL,
                status TEXT NOT NULL,
                champion_rule_pack_id TEXT,
                recipe_json TEXT NOT NULL,
                error TEXT,
                started_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                completed_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE strategy_iteration_result (
                optimization_run_id TEXT NOT NULL,
                iteration INTEGER NOT NULL,
                rule_pack_id TEXT NOT NULL,
                fold_index INTEGER NOT NULL,
                fold_role TEXT,
                fitness DOUBLE,
                cagr DOUBLE,
                sharpe DOUBLE,
                sortino DOUBLE,
                max_drawdown_pct DOUBLE,
                win_rate DOUBLE,
                profit_factor DOUBLE,
                trade_count INTEGER,
                trades_per_year DOUBLE,
                total_return_pct DOUBLE,
                nifty_return_pct DOUBLE,
                accepted BOOLEAN,
                rejection_reason TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
            )
            """
        )

        packs = [
            (PACK_BASELINE_A, "momentum_breakout", "backtested"),
            (PACK_CHAMPION_A_OLD, "momentum_breakout", "shadow"),
            (PACK_CHAMPION_A_NEW, "momentum_breakout", "walkforward_passed"),
            (PACK_BASELINE_B, "mean_reversion", "backtested"),
            (PACK_CHAMPION_B, "mean_reversion", "walkforward_passed"),
        ]
        conn.executemany(
            """
            INSERT INTO strategy_rule_pack
              (rule_pack_id, parent_rule_pack_id, strategy_id, version, rule_yaml, rule_json,
               lifecycle_status, description, created_at)
            VALUES (?, NULL, ?, 1, '{}', '{}', ?, NULL, '2026-04-01 00:00:00')
            """,
            packs,
        )

        runs = [
            (RUN_A_OLD, RECIPE_A, "momentum_breakout", PACK_BASELINE_A, "2024-01-01", "2025-01-01", 42, 20,
             "completed", PACK_CHAMPION_A_OLD, "{}", None, "2026-03-01 00:00:00", "2026-03-01 01:00:00"),
            (RUN_A_NEW, RECIPE_A, "momentum_breakout", PACK_BASELINE_A, "2024-01-01", "2025-06-01", 42, 30,
             "completed", PACK_CHAMPION_A_NEW, "{}", None, "2026-04-15 00:00:00", "2026-04-15 02:00:00"),
            (RUN_B, RECIPE_B, "mean_reversion", PACK_BASELINE_B, "2024-01-01", "2025-01-01", 7, 10,
             "completed", PACK_CHAMPION_B, "{}", None, "2026-04-10 00:00:00", "2026-04-10 01:30:00"),
        ]
        conn.executemany(
            """
            INSERT INTO strategy_optimization_run VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            runs,
        )

        # Iteration rows. Schema requires every fold column; fill the unused
        # ones with sensible NULLs / zeros so the COUNT/aggregation queries
        # behave like production.
        def ir(
            run_id: str,
            iteration: int,
            pack_id: str,
            fold_index: int,
            fitness: float | None,
            cagr: float | None = None,
            sharpe: float | None = None,
            mdd: float | None = None,
            win: float | None = None,
            trades: int | None = None,
            total: float | None = None,
            accepted: bool | None = None,
            rejection: str | None = None,
        ) -> tuple:
            return (
                run_id, iteration, pack_id, fold_index, "val",
                fitness, cagr, sharpe, None, mdd, win, None, trades, None, total, None,
                accepted, rejection, "2026-04-15 01:00:00",
            )

        iterations = [
            # Baseline marker (iteration = -1) for run A NEW — aggregate only.
            ir(RUN_A_NEW, -1, PACK_BASELINE_A, -1, 0.12, cagr=0.08, sharpe=0.5, mdd=-0.10, win=0.45, trades=80, total=0.08, accepted=True, rejection="baseline"),
            # Baseline per-fold rows for run A NEW.
            ir(RUN_A_NEW, -1, PACK_BASELINE_A, 0, 0.10, cagr=0.07, sharpe=0.4, mdd=-0.11, win=0.43, trades=40, total=0.07),
            ir(RUN_A_NEW, -1, PACK_BASELINE_A, 1, 0.14, cagr=0.09, sharpe=0.6, mdd=-0.09, win=0.47, trades=40, total=0.09),

            # Trial aggregate rows for run A NEW (fold_index = -1, iteration >= 0).
            ir(RUN_A_NEW, 0, PACK_CHAMPION_A_NEW, -1, 0.32, cagr=0.20, sharpe=1.4, mdd=-0.08, win=0.55, trades=90, total=0.20, accepted=True, rejection=None),
            ir(RUN_A_NEW, 1, "pack-trial-1", -1, 0.18, cagr=0.10, sharpe=0.7, mdd=-0.12, win=0.48, trades=70, total=0.10, accepted=False, rejection="worst_fold_underperforms_benchmark"),
            # Champion per-fold rows (fold_index >= 0) for run A NEW.
            ir(RUN_A_NEW, 0, PACK_CHAMPION_A_NEW, 0, 0.30, cagr=0.18, sharpe=1.2, mdd=-0.09, win=0.53, trades=45, total=0.18),
            ir(RUN_A_NEW, 0, PACK_CHAMPION_A_NEW, 1, 0.34, cagr=0.22, sharpe=1.6, mdd=-0.07, win=0.57, trades=45, total=0.22),

            # Run A OLD: lower-Sharpe champion (should NOT win the leaderboard for recipe A).
            ir(RUN_A_OLD, 0, PACK_CHAMPION_A_OLD, -1, 0.22, cagr=0.14, sharpe=0.9, mdd=-0.10, win=0.50, trades=60, total=0.14, accepted=True),

            # Run B champion aggregate row.
            ir(RUN_B, 0, PACK_CHAMPION_B, -1, 0.40, cagr=0.30, sharpe=2.1, mdd=-0.05, win=0.60, trades=100, total=0.30, accepted=True),
        ]
        conn.executemany(
            """
            INSERT INTO strategy_iteration_result VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            iterations,
        )
    finally:
        conn.close()


def _seed_report(tmp_path: Path) -> Path:
    """Write an auto-report file matching runner.py's layout for RUN_A_NEW."""
    report_dir = tmp_path / "reports" / "optimization" / RECIPE_A
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{RUN_A_NEW}.md"
    report_path.write_text("# Test report\n\nSeeded for the route test.\n")
    (report_dir / "latest.md").write_text("# Test report\n\nSeeded for the route test.\n")
    return report_path


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    cp_path = tmp_path / "data" / "control_plane.duckdb"
    _seed_control_plane(cp_path)
    _seed_report(tmp_path)
    return TestClient(create_app())


# ---------------------------------------------------------------------------
# /runs (list)
# ---------------------------------------------------------------------------


def test_runs_list_returns_all_seeded(client: TestClient) -> None:
    resp = client.get("/api/execution/optimization/runs", headers=API_HEADERS)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["available"] is True
    ids = [r["optimization_run_id"] for r in body["runs"]]
    assert set(ids) == {RUN_A_OLD, RUN_A_NEW, RUN_B}
    # Newest first.
    assert ids[0] == RUN_A_NEW


def test_runs_list_filter_by_recipe(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/optimization/runs?recipe={RECIPE_B}", headers=API_HEADERS
    )
    assert resp.status_code == 200
    body = resp.json()
    assert [r["optimization_run_id"] for r in body["runs"]] == [RUN_B]
    assert body["runs"][0]["trial_count"] == 1


def test_runs_list_filter_by_status(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/optimization/runs?status=running", headers=API_HEADERS
    )
    assert resp.status_code == 200
    assert resp.json()["runs"] == []


# ---------------------------------------------------------------------------
# /runs/{id} (detail)
# ---------------------------------------------------------------------------


def test_run_detail_happy_path(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/optimization/runs/{RUN_A_NEW}", headers=API_HEADERS
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["recipe_name"] == RECIPE_A
    assert body["status"] == "completed"
    assert body["champion_rule_pack_id"] == PACK_CHAMPION_A_NEW
    assert body["champion_lifecycle_status"] == "walkforward_passed"
    assert len(body["baseline_folds"]) == 2
    assert len(body["champion_folds"]) == 2
    assert body["trial_count"] == 2
    assert body["report_exists"] is True
    assert body["report_path"].endswith(f"/{RUN_A_NEW}.md")


def test_run_detail_unknown_id_returns_404(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/optimization/runs/does-not-exist", headers=API_HEADERS
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# /runs/{id}/trials
# ---------------------------------------------------------------------------


def test_trials_default_sort_by_iteration(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/optimization/runs/{RUN_A_NEW}/trials", headers=API_HEADERS
    )
    assert resp.status_code == 200
    body = resp.json()
    iterations = [t["iteration"] for t in body["trials"]]
    assert iterations == [0, 1]  # baseline (iteration=-1) excluded


def test_trials_sort_by_fitness_descending(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/optimization/runs/{RUN_A_NEW}/trials?sort=fitness",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    fitness_values = [t["fitness"] for t in resp.json()["trials"]]
    assert fitness_values == sorted(fitness_values, reverse=True)


def test_trials_unknown_sort_falls_back_to_iteration(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/optimization/runs/{RUN_A_NEW}/trials?sort=bogus_metric",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    iterations = [t["iteration"] for t in resp.json()["trials"]]
    assert iterations == [0, 1]


# ---------------------------------------------------------------------------
# /leaderboard
# ---------------------------------------------------------------------------


def test_leaderboard_picks_latest_run_per_recipe(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/optimization/leaderboard?metric=sharpe&top=10",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["metric"] == "sharpe"
    recipes = {row["recipe_name"]: row for row in body["rows"]}
    # Should include exactly the latest completed run per recipe — RUN_A_OLD must be excluded.
    assert set(recipes) == {RECIPE_A, RECIPE_B}
    assert recipes[RECIPE_A]["optimization_run_id"] == RUN_A_NEW
    assert recipes[RECIPE_A]["champion_rule_pack_id"] == PACK_CHAMPION_A_NEW
    assert recipes[RECIPE_B]["optimization_run_id"] == RUN_B
    # Sorted by sharpe descending.
    sharpe_vals = [row["sharpe"] for row in body["rows"]]
    assert sharpe_vals == sorted(sharpe_vals, reverse=True)


def test_leaderboard_unknown_metric_falls_back_to_sharpe(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/optimization/leaderboard?metric=bogus",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    assert resp.json()["metric"] == "sharpe"


# ---------------------------------------------------------------------------
# /runs/{id}/report
# ---------------------------------------------------------------------------


def test_report_returns_markdown_content(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/optimization/runs/{RUN_A_NEW}/report", headers=API_HEADERS
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["recipe_name"] == RECIPE_A
    assert body["content"].startswith("# Test report")


def test_report_missing_returns_404(client: TestClient) -> None:
    # RUN_A_OLD has no report seeded.
    resp = client.get(
        f"/api/execution/optimization/runs/{RUN_A_OLD}/report",
        headers=API_HEADERS,
    )
    assert resp.status_code == 404


def test_report_unknown_run_returns_404(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/optimization/runs/does-not-exist/report",
        headers=API_HEADERS,
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /runs/{id}/promote (Wave 5b)
# ---------------------------------------------------------------------------


def _lifecycle_for(tmp_path: Path, rule_pack_id: str) -> str:
    """Read the current lifecycle status of a pack directly from DuckDB."""
    import duckdb
    cp_path = tmp_path / "data" / "control_plane.duckdb"
    conn = duckdb.connect(str(cp_path), read_only=True)
    try:
        row = conn.execute(
            "SELECT lifecycle_status FROM strategy_rule_pack WHERE rule_pack_id = ?",
            [rule_pack_id],
        ).fetchone()
    finally:
        conn.close()
    return row[0] if row else ""


def test_promote_happy_path_advances_lifecycle(
    client: TestClient, tmp_path: Path
) -> None:
    """RUN_A_NEW champion starts at 'walkforward_passed' (seeded). Promote to shadow."""
    resp = client.post(
        f"/api/execution/optimization/runs/{RUN_A_NEW}/promote",
        headers=API_HEADERS,
        json={"to": "shadow"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["rule_pack_id"] == PACK_CHAMPION_A_NEW
    assert body["previous_status"] == "walkforward_passed"
    assert body["new_status"] == "shadow"
    # Verify the lifecycle actually moved in the DB.
    assert _lifecycle_for(tmp_path, PACK_CHAMPION_A_NEW) == "shadow"


def test_promote_default_to_is_shadow(client: TestClient) -> None:
    """Empty body should default to {to: shadow}."""
    resp = client.post(
        f"/api/execution/optimization/runs/{RUN_A_NEW}/promote",
        headers=API_HEADERS,
        json={},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["new_status"] == "shadow"


def test_promote_backwards_returns_422(client: TestClient) -> None:
    """RUN_A_OLD champion is at 'shadow' (seeded). Asking for 'walkforward_passed' is backwards."""
    resp = client.post(
        f"/api/execution/optimization/runs/{RUN_A_OLD}/promote",
        headers=API_HEADERS,
        json={"to": "walkforward_passed"},
    )
    assert resp.status_code == 422
    assert "backwards" in resp.json()["detail"]


def test_promote_unknown_status_returns_422(client: TestClient) -> None:
    resp = client.post(
        f"/api/execution/optimization/runs/{RUN_A_NEW}/promote",
        headers=API_HEADERS,
        json={"to": "not_a_real_status"},
    )
    assert resp.status_code == 422
    assert "unknown lifecycle status" in resp.json()["detail"]


def test_promote_unknown_run_returns_404(client: TestClient) -> None:
    resp = client.post(
        "/api/execution/optimization/runs/does-not-exist/promote",
        headers=API_HEADERS,
        json={"to": "shadow"},
    )
    assert resp.status_code == 404
