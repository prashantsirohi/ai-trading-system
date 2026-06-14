"""Phase 2 quality guardrails for trusted perf-tracker analytics."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ai_trading_system.research.perf_tracker.backfill import (
    _latest_attempt_per_date,
    _validate_operational_rows,
    build_rows_from_ranked_frame,
)
from ai_trading_system.research.perf_tracker.quality import annotate_return_quality
from ai_trading_system.research.perf_tracker.reports import (
    build_ranking_feedback_summary,
    build_research_quality_reports,
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
        "watchlist_bucket": "F4_ACTION_CANDIDATE",
        "sector": "Energy",
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
    assert out.loc[0, "watchlist_bucket"] == "F4_ACTION_CANDIDATE"
    assert out.loc[0, "sector_name"] == "Energy"


def test_extreme_forward_returns_get_reasoned_quarantine() -> None:
    rows = pd.DataFrame([
        {
            "run_date": "2026-05-08",
            "symbol_id": "RELIANCE",
            "exchange": "NSE",
            "fwd_5d_return": 1.0,
            "fwd_20d_return": 85.0,
        },
        {
            "run_date": "2026-05-08",
            "symbol_id": "INFY",
            "exchange": "NSE",
            "fwd_5d_return": 1.0,
            "fwd_20d_return": 5.0,
        },
    ])

    out = annotate_return_quality(rows)

    quarantined = out.set_index("symbol_id").loc["RELIANCE"]
    trusted = out.set_index("symbol_id").loc["INFY"]
    assert quarantined["data_quality_status"] == "quarantined"
    assert bool(quarantined["fwd_return_anomaly"]) is True
    assert "extreme_fwd_20d_return" in quarantined["data_quality_reason"]
    assert "manual_review_extreme_return" in quarantined["data_quality_reason"]
    assert trusted["data_quality_status"] == "trusted"
    assert pd.isna(trusted["data_quality_reason"])


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


def test_research_quality_reports_emit_segments_and_excluded_rows(project: Path) -> None:
    insert_perf_rows(
        project,
        [
            {
                "run_date": date(2026, 5, 1),
                "symbol_id": "AAA1",
                "rank_position": 1,
                "watchlist_bucket": "F4_ACTION_CANDIDATE",
                "sector_name": "Pharma",
                "fwd_5d_return": 2.0,
                "fwd_10d_return": 3.0,
                "fwd_20d_return": 5.0,
            },
            {
                "run_date": date(2026, 5, 1),
                "symbol_id": "BBB1",
                "rank_position": 18,
                "watchlist_bucket": "F3_FUND_VALUE_TECH_READY",
                "sector_name": "IT",
                "fwd_5d_return": -1.0,
                "fwd_10d_return": 1.0,
                "fwd_20d_return": 2.0,
            },
            {
                "run_date": date(2026, 5, 1),
                "symbol_id": "CCC1",
                "rank_position": 80,
                "sector_name": "Industrial",
                "fwd_5d_return": -90.0,
                "fwd_20d_return": -95.0,
            },
        ],
    )
    with open_research_db(project_root=project, read_only=False) as con:
        con.execute(
            """
            UPDATE rank_cohort_performance
            SET fwd_5d_anomaly = TRUE,
                fwd_return_anomaly = TRUE,
                data_quality_status = 'quarantined',
                data_quality_reason = 'extreme_fwd_5d_return|manual_review_extreme_return'
            WHERE symbol_id = 'CCC1'
            """
        )

    reports = build_research_quality_reports(project_root=project)

    frames = reports["frames"]
    assert set(frames) == {
        "rank_bucket_performance",
        "sector_performance",
        "repeated_symbol_performance",
        "excluded_rows",
    }
    assert set(frames["rank_bucket_performance"]["rank_bucket"]) >= {"top-10", "rank-11-25"}
    assert "Pharma" in set(frames["sector_performance"]["sector_name"])
    assert frames["excluded_rows"].loc[0, "symbol_id"] == "CCC1"
    assert reports["summary"]["artifact_rows"]["excluded_rows"] == 1


def test_ranking_feedback_summary_detects_rank_edge_and_factor_ic(project: Path) -> None:
    base_date = date(2026, 5, 1)
    rows = []
    for idx in range(80):
        rank_position = idx + 1
        factor_score = float(100 - idx)
        fwd_20d = 8.0 - idx * 0.1
        rows.append({
            "run_date": base_date - timedelta(days=idx % 10),
            "symbol_id": f"EDGE{idx:03d}",
            "rank_position": rank_position,
            "watchlist_bucket": "CORE_MOMENTUM" if rank_position <= 25 else "UNASSIGNED",
            "fwd_5d_return": fwd_20d / 2,
            "fwd_10d_return": fwd_20d * 0.75,
            "fwd_20d_return": fwd_20d,
            "fwd_60d_return": fwd_20d * 1.5,
            "factor_rs": factor_score,
            "factor_vol": -factor_score,
            "factor_trend": factor_score,
        })
    insert_perf_rows(project, rows)

    summary = build_ranking_feedback_summary(project_root=project)

    assert summary["status"] == "ok"
    top10_20d = next(
        row for row in summary["rank_bucket_rows"]
        if row["rank_bucket"] == "top-10" and row["horizon"] == "20d"
    )
    lower_20d = next(
        row for row in summary["rank_bucket_rows"]
        if row["rank_bucket"] == "rank-51-plus" and row["horizon"] == "20d"
    )
    assert top10_20d["avg_return"] > lower_20d["avg_return"]
    rs_20d = next(
        row for row in summary["factor_ic_rows"]
        if row["factor"] == "rs" and row["horizon"] == "20d"
    )
    vol_20d = next(
        row for row in summary["factor_ic_rows"]
        if row["factor"] == "vol" and row["horizon"] == "20d"
    )
    assert rs_20d["signal"] == "positive"
    assert vol_20d["signal"] == "negative"
    assert any(row["decision"] == "increase_candidate" and row["subject"] == "rs" for row in summary["recommendations"])


def test_ranking_feedback_summary_flags_failed_top_bucket_and_excludes_untrusted(project: Path) -> None:
    base_date = date(2026, 5, 1)
    rows = []
    for idx in range(70):
        rank_position = idx + 1
        rows.append({
            "run_date": base_date - timedelta(days=idx % 8),
            "symbol_id": f"FAIL{idx:03d}",
            "rank_position": rank_position,
            "watchlist_bucket": "AVOID_WEAK_CONFIRMATION" if rank_position <= 10 else "CORE_MOMENTUM",
            "fwd_5d_return": -2.0 if rank_position <= 10 else 2.0,
            "fwd_10d_return": -2.5 if rank_position <= 10 else 2.5,
            "fwd_20d_return": -3.0 if rank_position <= 10 else 3.0,
            "fwd_60d_return": -4.0 if rank_position <= 10 else 4.0,
            "factor_rs": float(idx),
        })
    rows.append({
        "run_date": base_date,
        "symbol_id": "QUARANTINED_WINNER",
        "rank_position": 1,
        "watchlist_bucket": "CORE_MOMENTUM",
        "fwd_20d_return": 99.0,
        "factor_rs": 99.0,
    })
    insert_perf_rows(project, rows)
    with open_research_db(project_root=project, read_only=False) as con:
        con.execute(
            """
            UPDATE rank_cohort_performance
            SET data_quality_status = 'quarantined',
                fwd_return_anomaly = TRUE
            WHERE symbol_id = 'QUARANTINED_WINNER'
            """
        )

    summary = build_ranking_feedback_summary(project_root=project)

    top10_20d = next(
        row for row in summary["rank_bucket_rows"]
        if row["rank_bucket"] == "top-10" and row["horizon"] == "20d"
    )
    assert top10_20d["avg_return"] == pytest.approx(-3.0)
    assert any(row["decision"] == "reduce_candidate" for row in summary["recommendations"])
    assert all("QUARANTINED_WINNER" not in str(row) for row in summary["rank_bucket_rows"])


def test_ranking_feedback_summary_missing_db_is_graceful(project: Path) -> None:
    summary = build_ranking_feedback_summary(project_root=project)

    assert summary["status"] == "missing"
    assert summary["rank_bucket_rows"] == []
    assert summary["recommendations"] == []
    assert summary["warnings"]
