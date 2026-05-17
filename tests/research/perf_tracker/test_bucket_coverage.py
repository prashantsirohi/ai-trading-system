"""Diagnostics sprint: /bucket-coverage extended fields."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def _seed(project: Path) -> None:
    today = date(2026, 5, 8)
    rows = []
    # CORE_MOMENTUM: 5 dates × 4 symbols = 20 rows, half missing fwd_5d.
    for d in range(5):
        for s in range(4):
            run_date = today - timedelta(days=d)
            rows.append({
                "run_date": run_date,
                "symbol_id": f"CM{s}",
                "rank_position": s + 1,
                "watchlist_bucket": "CORE_MOMENTUM",
                "fwd_5d_return": 1.0 if (d + s) % 2 == 0 else None,
                "fwd_20d_return": 1.0,
            })
    # EARLY_STAGE2: 2 dates × 2 symbols = 4 rows, all fwd_5d present.
    for d in range(2):
        for s in range(2):
            run_date = today - timedelta(days=d)
            rows.append({
                "run_date": run_date,
                "symbol_id": f"ES{s}",
                "rank_position": s + 5,
                "watchlist_bucket": "EARLY_STAGE2",
                "fwd_5d_return": 2.0,
                "fwd_20d_return": 2.0,
            })
    insert_perf_rows(project, rows)


def test_bucket_coverage_returns_extended_fields(project: Path, api_client: TestClient) -> None:
    _seed(project)
    resp = api_client.get("/api/execution/perf-tracker/bucket-coverage", headers=API_HEADERS)
    assert resp.status_code == 200, resp.text
    rows = {r["bucket"]: r for r in resp.json()["buckets"]}
    cm = rows["CORE_MOMENTUM"]
    es = rows["EARLY_STAGE2"]

    assert cm["rows"] == 20
    assert cm["dates"] == 5
    assert cm["symbols_count"] == 4
    assert es["rows"] == 4
    assert es["symbols_count"] == 2

    total = sum(r["pct_of_all_rows"] or 0 for r in rows.values())
    assert abs(total - 1.0) < 1e-6

    # CORE_MOMENTUM has half rows missing fwd_5d, so coverage ≈ 0.5.
    assert abs((cm["pct_with_fwd_5d"] or 0) - 0.5) < 1e-6
    # All rows have fwd_20d.
    assert abs((cm["pct_with_fwd_20d"] or 0) - 1.0) < 1e-6
    assert abs((es["pct_with_fwd_5d"] or 0) - 1.0) < 1e-6
