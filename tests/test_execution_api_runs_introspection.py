"""Tests for the PR #4 runs-introspection endpoints.

Covers ``/api/execution/runs/{run_id}/dq``,
``/api/execution/runs/{run_id}/artifacts``, and the gated
``/api/execution/artifacts/{run_id}/{stage}/{name}`` download path.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.ui.execution_api.app import create_app


API_HEADERS = {"x-api-key": "test-api-key"}
RUN_ID = "pipeline-2026-04-21-demo"


def _seed_control_plane(cp_path: Path) -> None:
    """Create the control_plane.duckdb tables PR #4 reads from."""

    cp_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(cp_path))
    try:
        conn.execute(
            """
            CREATE TABLE dq_result (
                result_id VARCHAR,
                run_id VARCHAR,
                stage_name VARCHAR,
                rule_id VARCHAR,
                severity VARCHAR,
                status VARCHAR,
                failed_count BIGINT,
                message VARCHAR,
                sample_uri VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE pipeline_artifact (
                artifact_id VARCHAR,
                run_id VARCHAR,
                stage_name VARCHAR,
                attempt_number INTEGER,
                artifact_type VARCHAR,
                uri VARCHAR,
                content_hash VARCHAR,
                row_count BIGINT,
                created_at TIMESTAMP
            )
            """
        )
        conn.executemany(
            "INSERT INTO dq_result VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "dq-1",
                    RUN_ID,
                    "ingest",
                    "Volume_Anomalous_Spike",
                    "error",
                    "failed",
                    3,
                    "Volume exceeded 10x 20-day MA without news catalyst.",
                    None,
                    "2026-04-21 14:00:00",
                ),
                (
                    "dq-2",
                    RUN_ID,
                    "features",
                    "Stale_Pricing_Tick",
                    "warn",
                    "failed",
                    1,
                    "Missing 5-minute ticks between 10:15 and 10:20.",
                    None,
                    "2026-04-21 14:01:00",
                ),
                (
                    "dq-3",
                    RUN_ID,
                    "rank",
                    "Coverage_Sufficiency",
                    "warn",
                    "passed",
                    0,
                    "Coverage 92%.",
                    None,
                    "2026-04-21 14:02:00",
                ),
                # Different run — must NOT leak into our results.
                (
                    "dq-4",
                    "other-run",
                    "ingest",
                    "Other_Rule",
                    "error",
                    "failed",
                    0,
                    "Should not show up.",
                    None,
                    "2026-04-21 14:03:00",
                ),
            ],
        )
    finally:
        conn.close()


def _register_artifact(
    cp_path: Path,
    *,
    artifact_id: str,
    run_id: str,
    stage: str,
    uri: str,
    artifact_type: str = "json",
    attempt: int = 1,
) -> None:
    conn = duckdb.connect(str(cp_path))
    try:
        conn.execute(
            "INSERT INTO pipeline_artifact VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                artifact_id,
                run_id,
                stage,
                attempt,
                artifact_type,
                uri,
                None,
                None,
                "2026-04-21 14:00:00",
            ],
        )
    finally:
        conn.close()


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Set up an isolated project root with seeded control_plane.duckdb + artifacts."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])

    runs_dir = tmp_path / "data" / "pipeline_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    cp_path = tmp_path / "data" / "control_plane.duckdb"

    _seed_control_plane(cp_path)

    # Create real on-disk artifacts that the registry will reference.
    ingest_dir = runs_dir / RUN_ID / "ingest" / "attempt_1"
    ingest_dir.mkdir(parents=True, exist_ok=True)
    (ingest_dir / "ingest_summary.json").write_text('{"ok": true}')

    rank_dir = runs_dir / RUN_ID / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    (rank_dir / "ranked_signals.csv").write_text("symbol,score\nAAA,90\n")

    # Register both with absolute URIs (matches how the pipeline writes today).
    _register_artifact(
        cp_path,
        artifact_id="art-1",
        run_id=RUN_ID,
        stage="ingest",
        uri=str(ingest_dir / "ingest_summary.json"),
    )
    _register_artifact(
        cp_path,
        artifact_id="art-2",
        run_id=RUN_ID,
        stage="rank",
        uri=str(rank_dir / "ranked_signals.csv"),
        artifact_type="csv",
    )

    return TestClient(create_app())


# ---------------------------------------------------------------------------
# /runs/{run_id}/dq
# ---------------------------------------------------------------------------


def test_dq_results_happy_path(client: TestClient) -> None:
    resp = client.get(f"/api/execution/runs/{RUN_ID}/dq", headers=API_HEADERS)
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["available"] is True
    assert body["run_id"] == RUN_ID
    assert body["filters"] == {"severity": None, "stage": None}

    # Three rules for our run; the "other-run" row must be excluded.
    assert len(body["results"]) == 3
    rule_ids = [r["rule_id"] for r in body["results"]]
    assert "Other_Rule" not in rule_ids

    # error rows are sorted ahead of warn rows.
    assert body["results"][0]["severity"] == "error"

    summary = body["summary"]
    assert summary["total"] == 3
    assert summary["total_failed"] == 2
    assert summary["total_passed"] == 1
    assert summary["counts_by_severity"]["error"]["failed"] == 1
    assert summary["counts_by_severity"]["warn"]["failed"] == 1
    assert summary["counts_by_severity"]["warn"]["passed"] == 1


def test_dq_results_severity_filter(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/runs/{RUN_ID}/dq?severity=warn",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert {r["severity"] for r in body["results"]} == {"warn"}
    assert body["filters"] == {"severity": "warn", "stage": None}


def test_dq_results_stage_filter(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/runs/{RUN_ID}/dq?stage=ingest",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert {r["stage_name"] for r in body["results"]} == {"ingest"}


def test_dq_results_missing_db_returns_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without control_plane.duckdb, the endpoint degrades gracefully."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)

    client = TestClient(create_app())
    resp = client.get(f"/api/execution/runs/{RUN_ID}/dq", headers=API_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"available": False, "run_id": RUN_ID, "results": []}


# ---------------------------------------------------------------------------
# /runs/{run_id}/artifacts
# ---------------------------------------------------------------------------


def test_artifacts_list_happy_path(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/runs/{RUN_ID}/artifacts", headers=API_HEADERS
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["available"] is True
    assert body["run_id"] == RUN_ID
    assert body["total"] == 2
    assert body["counts_by_stage"] == {"ingest": 1, "rank": 1}

    by_name = {a["name"]: a for a in body["artifacts"]}
    assert set(by_name) == {"ingest_summary.json", "ranked_signals.csv"}

    download_url = by_name["ingest_summary.json"]["download_url"]
    assert download_url == f"/api/execution/artifacts/{RUN_ID}/ingest/ingest_summary.json"


# ---------------------------------------------------------------------------
# /artifacts/{run_id}/{stage}/{name} — gated download
# ---------------------------------------------------------------------------


def test_artifact_download_happy_path(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/artifacts/{RUN_ID}/ingest/ingest_summary.json",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"ok": True}


def test_artifact_download_404_unknown_run(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/artifacts/no-such-run/ingest/ingest_summary.json",
        headers=API_HEADERS,
    )
    assert resp.status_code == 404


def test_artifact_download_404_basename_mismatch(client: TestClient) -> None:
    """Registry has art-1 but the requested basename differs."""

    resp = client.get(
        f"/api/execution/artifacts/{RUN_ID}/ingest/something_else.json",
        headers=API_HEADERS,
    )
    assert resp.status_code == 404


def test_artifact_download_blocks_path_traversal(client: TestClient) -> None:
    """Layer-1 regex check should bounce ``..`` injections."""

    # FastAPI's default routing percent-decodes; using the literal "..%2f" form
    # ensures the segment reaches our handler intact and gets rejected.
    resp = client.get(
        f"/api/execution/artifacts/{RUN_ID}/ingest/..%2Fevil.txt",
        headers=API_HEADERS,
    )
    # Either 404 (segment regex rejects) or 400 from path-containment, but
    # never 200.
    assert resp.status_code in (400, 404)


def test_artifact_download_blocks_unsafe_run_id(client: TestClient) -> None:
    resp = client.get(
        f"/api/execution/artifacts/..%2Fother/ingest/ingest_summary.json",
        headers=API_HEADERS,
    )
    assert resp.status_code in (400, 404)


def test_artifact_download_404_when_file_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Registry has the row but the file vanished — must surface as 404."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])

    cp_path = tmp_path / "data" / "control_plane.duckdb"
    runs_dir = tmp_path / "data" / "pipeline_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    _seed_control_plane(cp_path)

    # Register an artifact whose URI does NOT exist on disk.
    phantom_uri = str(runs_dir / RUN_ID / "ingest" / "attempt_1" / "ghost.json")
    _register_artifact(
        cp_path,
        artifact_id="art-ghost",
        run_id=RUN_ID,
        stage="ingest",
        uri=phantom_uri,
    )

    client = TestClient(create_app())
    resp = client.get(
        f"/api/execution/artifacts/{RUN_ID}/ingest/ghost.json",
        headers=API_HEADERS,
    )
    assert resp.status_code == 404


def test_artifact_download_blocks_escape_via_registry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even if the registry points outside pipeline_runs_dir, layer-3 must reject."""

    monkeypatch.setenv("AI_TRADING_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("EXECUTION_API_KEY", API_HEADERS["x-api-key"])

    cp_path = tmp_path / "data" / "control_plane.duckdb"
    (tmp_path / "data" / "pipeline_runs").mkdir(parents=True, exist_ok=True)
    _seed_control_plane(cp_path)

    # Plant a file outside the runs sandbox and register it.
    outside = tmp_path / "outside" / "secret.json"
    outside.parent.mkdir(parents=True, exist_ok=True)
    outside.write_text('{"leaked": true}')

    _register_artifact(
        cp_path,
        artifact_id="art-escape",
        run_id=RUN_ID,
        stage="ingest",
        uri=str(outside),
    )

    client = TestClient(create_app())
    resp = client.get(
        f"/api/execution/artifacts/{RUN_ID}/ingest/secret.json",
        headers=API_HEADERS,
    )
    assert resp.status_code == 400
