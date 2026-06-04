"""Phase 2 quality guardrails for trusted perf-tracker analytics."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.research.perf_tracker.backfill import (
    _latest_attempt_per_date,
    _validate_operational_rows,
    build_rows_from_ranked_frame,
)
from ai_trading_system.research.perf_tracker.schema import open_research_db
from tests.research.perf_tracker.conftest import API_HEADERS, insert_perf_rows


def test_flagged_rows_remain_visible_in_health_but_not_cohorts(
    project: Path,
    api_client: TestClient,
) -> None:
    rows = [
        {
            "run_date": date(2026, 5, 8),
            "symbol_id": "GOOD",
            "rank_position": 1,
            "fwd_5d_return": 2.0,
            "fwd_20d_return": 4.0,
        },
        {
            "run_date": date(2026, 5, 8),
            "symbol_id": "BAD",
            "rank_position": 2,
            "fwd_5d_return": -98.0,
            "fwd_20d_return": -98.0,
        },
    ]
    insert_perf_rows(project, rows)
    with open_research_db(project_root=project, read_only=False) as con:
        con.execute(
            """
            UPDATE rank_cohort_performance
            SET fwd_5d_anomaly = TRUE,
                fwd_return_anomaly = TRUE,
                data_quality_status = 'quarantined'
            WHERE symbol_id = 'BAD'
            """
        )

    coverage = api_client.get(
        "/api/execution/perf-tracker/coverage",
        headers=API_HEADERS,
    ).json()
    assert coverage["raw_rows"] == 2
    assert coverage["rows"] == 1
    assert coverage["excluded_rows"] == 1
    assert coverage["anomaly_rows"] == 1

    cohorts = api_client.get(
        "/api/execution/perf-tracker/cohorts?lookback_days=0",
        headers=API_HEADERS,
    ).json()
    top10 = next(row for row in cohorts["cohorts"] if row["cohort"] == "top-10")
    assert top10["n_total"] == 1
    assert top10["avg_20d"] == 4.0


def test_ranked_frame_records_provenance() -> None:
    ranked = pd.DataFrame([{
        "symbol_id": "RELIANCE",
        "exchange": "NSE",
        "composite_score": 90.0,
    }])

    out = build_rows_from_ranked_frame(
        "2026-05-08",
        ranked,
        source_type="pipeline",
        source_run_id="pipeline-2026-05-08-example",
        source_artifact_path="/tmp/ranked_signals.csv",
    )

    assert out.loc[0, "source_type"] == "pipeline"
    assert out.loc[0, "source_run_id"] == "pipeline-2026-05-08-example"
    assert out.loc[0, "source_artifact_path"] == "/tmp/ranked_signals.csv"
    assert out.loc[0, "data_quality_status"] == "trusted"


def test_operational_validation_rejects_fixture_symbols() -> None:
    rows = pd.DataFrame([{
        "run_date": "2026-05-08",
        "symbol_id": "DRIFT123",
        "exchange": "NSE",
    }])

    with pytest.raises(ValueError, match="fixture-like operational symbols"):
        _validate_operational_rows(rows)


def test_operational_validation_rejects_duplicate_keys() -> None:
    rows = pd.DataFrame([
        {"run_date": "2026-05-08", "symbol_id": "RELIANCE", "exchange": "NSE"},
        {"run_date": "2026-05-08", "symbol_id": "RELIANCE", "exchange": "NSE"},
    ])

    with pytest.raises(ValueError, match="duplicate cohort keys"):
        _validate_operational_rows(rows)


def test_read_only_open_creates_trusted_view_for_existing_db(project: Path) -> None:
    with open_research_db(project_root=project, read_only=False):
        pass
    with open_research_db(project_root=project, read_only=False) as con:
        con.execute("DROP VIEW rank_cohort_performance_trusted")

    with open_research_db(project_root=project, read_only=True) as con:
        count = con.execute("SELECT COUNT(*) FROM rank_cohort_performance_trusted").fetchone()[0]

    assert count == 0


def test_latest_attempt_accepts_custom_pipeline_run_suffix(tmp_path: Path) -> None:
    for run_id in ("pipeline-2026-05-08-aaaaaaaa", "pipeline-2026-05-09-manual-retry"):
        attempt = tmp_path / run_id / "rank" / "attempt_1"
        attempt.mkdir(parents=True)
        pd.DataFrame([{"symbol_id": "RELIANCE", "composite_score": 90.0}]).to_csv(
            attempt / "ranked_signals.csv",
            index=False,
        )

    by_date = _latest_attempt_per_date(tmp_path)

    assert set(by_date) == {"2026-05-08", "2026-05-09"}
    assert by_date["2026-05-09"]["ranked"].as_posix().endswith(
        "pipeline-2026-05-09-manual-retry/rank/attempt_1/ranked_signals.csv"
    )
