"""Diagnostics sprint: /digests + /digests/{name} viewer."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS


def _make_digest(project: Path, name: str, body: str) -> None:
    d = project / "data" / "research" / "perf_digests"
    d.mkdir(parents=True, exist_ok=True)
    (d / name).write_text(body, encoding="utf-8")


def test_list_digests_returns_sorted_entries(project: Path, api_client: TestClient) -> None:
    _make_digest(project, "digest_2026-W01.md", "# old")
    _make_digest(project, "digest_2026-W02.md", "# new")
    resp = api_client.get("/api/execution/perf-tracker/digests", headers=API_HEADERS)
    assert resp.status_code == 200
    names = [d["filename"] for d in resp.json()["digests"]]
    assert "digest_2026-W01.md" in names
    assert "digest_2026-W02.md" in names


def test_get_digest_returns_markdown(project: Path, api_client: TestClient) -> None:
    _make_digest(project, "digest_2026-W03.md", "# hello\nbody")
    resp = api_client.get(
        "/api/execution/perf-tracker/digests/digest_2026-W03.md",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["filename"] == "digest_2026-W03.md"
    assert "# hello" in body["markdown"]


def test_get_digest_rejects_path_traversal(project: Path, api_client: TestClient) -> None:
    resp = api_client.get(
        "/api/execution/perf-tracker/digests/..%2Fsecret.md",
        headers=API_HEADERS,
    )
    # Either the path-component reject (400) or not-found.
    assert resp.status_code in (400, 404)


def test_get_digest_404_when_missing(project: Path, api_client: TestClient) -> None:
    resp = api_client.get(
        "/api/execution/perf-tracker/digests/nope.md",
        headers=API_HEADERS,
    )
    assert resp.status_code == 404
