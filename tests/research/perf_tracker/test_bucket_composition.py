"""Diagnostics sprint: /buckets/composition averages + missing-column handling."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def test_composition_emits_available_columns_and_nulls_for_missing(
    project: Path,
    api_client: TestClient,
) -> None:
    today = date(2026, 5, 8)
    rows = []
    for s in range(5):
        rows.append({
            "run_date": today,
            "symbol_id": f"COMP{s}",
            "rank_position": s + 1,
            "watchlist_bucket": "CORE_MOMENTUM",
            "composite_score": float(s + 1),
            "composite_score_adjusted": float(s + 1),
            "factor_rs": float(s + 1) / 10,
            "factor_trend": float(s + 1) / 10,
            "factor_prox": float(s + 1) / 10,
        })
    insert_perf_rows(project, rows)
    resp = api_client.get(
        "/api/execution/perf-tracker/buckets/composition",
        headers=API_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    # Available columns should include composite_score; factor_stage isn't on the table.
    assert "composite_score" in body["available_columns"]
    assert "factor_stage" in body["missing_columns"]
    cm = next(r for r in body["composition"] if r["bucket"] == "CORE_MOMENTUM")
    assert cm["n"] == 5
    # Average composite_score of (1..5) is 3.0
    assert cm["avg_composite_score"] == 3.0
    # Missing column shows as null
    assert cm["avg_factor_stage"] is None
