"""Tests for the PR #6 ranking-detail endpoints.

Covers:

  * ``GET /api/execution/ranking/{symbol}`` (latest + pinned to ``run_id``)
  * ``GET /api/execution/ranking/{symbol}/history``
  * ``GET /api/execution/workspace/snapshot``
"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-api-key"}
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "artifacts" / "rank"
RUN_ID_LATEST = "pipeline-2026-04-10-latest"
RUN_ID_OLD = "pipeline-2026-04-09-old"


def _seed_run_attempt(
    project_root: Path, run_id: str, *, top_symbol: str = "AAA"
) -> Path:
    """Drop the rank-fixture artifacts into ``runs_dir/{run_id}/rank/attempt_1``.

    ``shutil.copy2`` preserves source mtime, so we explicitly stamp every
    seeded file with the current time. Without this, the snapshot selector
    (which sorts by ``dashboard_payload.json`` mtime) would tie or mis-rank
    the runs, since the fixture files share an old mtime.
    """

    rank_dir = project_root / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    for filename in (
        "ranked_signals.csv",
        "breakout_scan.csv",
        "pattern_scan.csv",
        "stock_scan.csv",
        "sector_dashboard.csv",
        "dashboard_payload.json",
    ):
        src = FIXTURE_ROOT / filename
        if src.exists():
            shutil.copy2(src, rank_dir / filename)

    # Custom ranked_signals: top symbol controls who sits at rank 1.
    if top_symbol != "AAA":
        ranked_csv = rank_dir / "ranked_signals.csv"
        ranked_csv.write_text(
            "symbol_id,exchange,close,composite_score,sector_name\n"
            f"{top_symbol},NSE,98.0,82.0,Technology\n"
            "AAA,NSE,104.0,75.0,Finance\n"
        )

    now = time.time()
    for child in rank_dir.iterdir():
        os.utime(child, (now, now))
    return rank_dir


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Project root with two seeded runs — old (older mtime) + latest."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])

    # Seed older run first; bump mtimes back so it sorts behind the latest.
    old_dir = _seed_run_attempt(tmp_path, RUN_ID_OLD, top_symbol="BBB")
    past = time.time() - 7200
    for child in old_dir.rglob("*"):
        os.utime(child, (past, past))
    os.utime(old_dir, (past, past))

    _seed_run_attempt(tmp_path, RUN_ID_LATEST)

    return TestClient(create_app())


# ---------------------------------------------------------------------------
# /ranking/{symbol}
# ---------------------------------------------------------------------------


def test_ranking_detail_latest_happy_path(client: TestClient) -> None:
    resp = client.get("/api/execution/ranking/AAA", headers=API_HEADERS)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["available"] is True
    assert body["symbol"] == "AAA"
    # Latest run has AAA at rank 1.
    assert body["run_id"] == RUN_ID_LATEST
    assert body["ranking"]["rank_position"] == 1
    assert body["ranking"]["composite_score"] == 88.5
    assert body["ranking"]["sector_name"] == "Finance"

    lifecycle = body["lifecycle"]
    assert lifecycle["rank"] == "TOP 5"
    assert lifecycle["execution"] == "ELIGIBLE"

    decision = body["decision"]
    assert decision["verdict"] == "BUY CANDIDATE"

    # raw_row preserves the source DataFrame row for advanced UI displays.
    assert body["raw_row"]["symbol_id"] == "AAA"
    assert "operator_context" in body
    assert body["operator_context"]["stage_label"] is None
    assert body["operator_context"]["top_pattern_family"] == "cup_handle"
    assert body["operator_context"]["explanation"] == ["Top setup: cup_handle (confirmed)."]


def test_ranking_detail_pinned_to_run_id(client: TestClient) -> None:
    """Pinning to the older run swaps the leader: BBB sits at rank 1."""

    resp = client.get(
        f"/api/execution/ranking/BBB?run_id={RUN_ID_OLD}",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["run_id"] == RUN_ID_OLD
    assert body["ranking"]["rank_position"] == 1


def test_ranking_detail_pinned_to_unknown_run(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/ranking/AAA?run_id=does-not-exist",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is False
    assert body["run_id"] == "does-not-exist"
    assert body["ranking"] is None


def test_ranking_detail_unknown_symbol_still_200(client: TestClient) -> None:
    resp = client.get("/api/execution/ranking/ZZZ", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is False
    assert body["ranking"] is None
    assert body["lifecycle"]["rank"] == "OUT"


def test_ranking_detail_factor_block_categorises_columns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Factor extraction maps numeric *_score columns onto Canvas buckets."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    rank_dir = (
        tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-10-rich" / "rank" / "attempt_1"
    )
    rank_dir.mkdir(parents=True, exist_ok=True)
    (rank_dir / "ranked_signals.csv").write_text(
        "symbol_id,composite_score,rs_score,volume_score,trend_score,sector_score,leftover_score\n"
        "AAA,88.5,0.92,0.81,0.74,0.65,0.4\n"
    )
    # Stub other frames so the snapshot loader doesn't fail.
    for empty in ("breakout_scan.csv", "pattern_scan.csv", "stock_scan.csv", "sector_dashboard.csv"):
        (rank_dir / empty).write_text("symbol_id\n")
    (rank_dir / "dashboard_payload.json").write_text("{}")

    client = TestClient(create_app())
    resp = client.get("/api/execution/ranking/AAA", headers=API_HEADERS)
    assert resp.status_code == 200
    factors = resp.json()["factors"]
    assert set(factors).issuperset({"rs", "volume", "trend", "sector"})
    assert factors["rs"]["value"] == pytest.approx(0.92)
    # Anything that doesn't match the four canonical patterns lands in `other`.
    assert "other" in factors


# ---------------------------------------------------------------------------
# /ranking/{symbol}/history
# ---------------------------------------------------------------------------


def test_ranking_history_walks_all_runs_newest_first(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/ranking/AAA/history?limit=10",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    assert body["symbol"] == "AAA"

    runs = [entry["run_id"] for entry in body["history"]]
    # Newest first.
    assert runs == [RUN_ID_LATEST, RUN_ID_OLD]

    # AAA is rank 1 in latest, rank 2 in old.
    assert body["history"][0]["rank_position"] == 1
    assert body["history"][1]["rank_position"] == 2

    # Run dates inferred from the run_id slug (YYYY-MM-DD).
    assert body["history"][0]["run_date"] == "2026-04-10"
    assert body["history"][1]["run_date"] == "2026-04-09"


def test_ranking_history_handles_symbol_not_in_some_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Symbol absent from one run's frame surfaces as ``rank_position: null``."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    # Run A: only AAA.
    a = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-10-A" / "rank" / "attempt_1"
    a.mkdir(parents=True, exist_ok=True)
    (a / "ranked_signals.csv").write_text(
        "symbol_id,composite_score\nAAA,90.0\n"
    )
    # Run B: only BBB.
    b = tmp_path / "data" / "pipeline_runs" / "pipeline-2026-04-09-B" / "rank" / "attempt_1"
    b.mkdir(parents=True, exist_ok=True)
    (b / "ranked_signals.csv").write_text(
        "symbol_id,composite_score\nBBB,85.0\n"
    )

    client = TestClient(create_app())
    resp = client.get("/api/execution/ranking/AAA/history", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    by_run = {h["run_id"]: h for h in body["history"]}
    assert by_run["pipeline-2026-04-10-A"]["rank_position"] == 1
    assert by_run["pipeline-2026-04-09-B"]["rank_position"] is None


def test_ranking_history_missing_runs_dir_returns_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)

    client = TestClient(create_app())
    resp = client.get("/api/execution/ranking/AAA/history", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is False
    assert body["history"] == []


# ---------------------------------------------------------------------------
# /workspace/snapshot — Control Tower compact
# ---------------------------------------------------------------------------


def test_workspace_snapshot_compact_top_actions(client: TestClient) -> None:
    resp = client.get(
        "/api/execution/workspace/snapshot?top_n=2",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["available"] is True
    assert isinstance(body["top_actions"], list)
    assert len(body["top_actions"]) == 2
    # Latest run has AAA, BBB at top.
    symbols = [a["symbol"] for a in body["top_actions"]]
    assert symbols[0] == "AAA"

    counts = body["counts"]
    assert counts["ranked"] >= 2

    leaders = body["sector_leaders"]
    assert isinstance(leaders, list)
    # ``sector_dashboard.csv`` fixture has 1 row; the endpoint returns
    # ``min(top_n, available_rows)`` so we assert the upper bound and that
    # we got at least one leader through.
    assert 1 <= len(leaders) <= 2
    assert "Sector" in leaders[0] or "sector_name" in leaders[0]


def test_workspace_snapshot_compact_no_payload_returns_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)

    client = TestClient(create_app())
    resp = client.get("/api/execution/workspace/snapshot", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is False
    assert body["top_actions"] == []
    assert body["sector_leaders"] == []


# ---------------------------------------------------------------------------
# Existing /ranking endpoint must still resolve — no path-collision regression
# ---------------------------------------------------------------------------


def test_existing_ranking_list_endpoint_still_works(client: TestClient) -> None:
    """The list endpoint sits at the same prefix; verify it is matched first."""

    resp = client.get("/api/execution/ranking?limit=5", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    # The list snapshot uses ``top_ranked`` as the canonical key.
    assert "top_ranked" in body
