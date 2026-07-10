from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.domains.investigator.cohort_performance import upsert_investigator_cohorts
from ai_trading_system.domains.investigator import pattern_scan as pattern_scan_module
from ai_trading_system.domains.investigator import service as investigator_service_module
from ai_trading_system.pipeline.contracts import StageArtifact, StageContext
from ai_trading_system.pipeline.orchestrator import PIPELINE_ORDER
from ai_trading_system.pipeline.stages.investigator import InvestigatorStage
from ai_trading_system.ui.execution_api.services.readmodels.investigator import get_investigator_pattern_history


def _seed_ohlcv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                is_benchmark BOOLEAN,
                instrument_type VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE _delivery (
                symbol_id VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                delivery_pct DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
            ('AAA', 'NSE', '2026-05-06', 98, 101, 97, 100, 1000, false, 'equity'),
            ('AAA', 'NSE', '2026-05-07', 101, 112, 100, 110, 3000, false, 'equity'),
            ('WWW', 'NSE', '2026-05-02', 100, 100, 100, 100, 1000, false, 'equity'),
            ('WWW', 'NSE', '2026-05-03', 101.5, 101.5, 101.5, 101.5, 1000, false, 'equity'),
            ('WWW', 'NSE', '2026-05-04', 103, 103, 103, 103, 1000, false, 'equity'),
            ('WWW', 'NSE', '2026-05-05', 104.5, 104.5, 104.5, 104.5, 1000, false, 'equity'),
            ('WWW', 'NSE', '2026-05-06', 106, 106, 106, 106, 1000, false, 'equity'),
            ('WWW', 'NSE', '2026-05-07', 108.5, 108.5, 108.5, 108.5, 1000, false, 'equity'),
            ('BBB', 'NSE', '2026-05-06', 100, 101, 99, 100, 1000, false, 'equity'),
            ('BBB', 'NSE', '2026-05-07', 100, 103, 99, 102, 3000, false, 'equity'),
            ('EARLY', 'NSE', '2026-05-06', 116, 119, 115, 118, 1000, false, 'equity'),
            ('EARLY', 'NSE', '2026-05-07', 118, 122, 117, 120.5, 2500, false, 'equity')
            """
        )
        stealth_rows = [
            ("STEALTH", "NSE", f"2026-04-{day:02d}", close, close, close, close, 1000.0, False, "equity")
            for day, close in enumerate(
                [
                    100.0,
                    100.2,
                    100.4,
                    100.6,
                    100.8,
                    101.0,
                    101.2,
                    101.4,
                    101.6,
                    101.8,
                    102.0,
                    102.2,
                    102.4,
                    102.6,
                    102.8,
                    105.0,
                    105.7,
                    106.4,
                    107.1,
                    107.8,
                    108.5,
                ],
                start=10,
            )
        ]
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", stealth_rows)
        conn.execute(
            """
            INSERT INTO _delivery VALUES
            ('AAA', 'NSE', '2026-05-07', 65),
            ('WWW', 'NSE', '2026-05-07', 55),
            ('STEALTH', 'NSE', '2026-04-30', 58),
            ('BBB', 'NSE', '2026-05-07', 30),
            ('EARLY', 'NSE', '2026-05-07', 62)
            """
        )
    finally:
        conn.close()


def _rank_artifacts(project_root: Path, run_id: str) -> dict[str, dict[str, StageArtifact]]:
    rank_dir = project_root / "data" / "pipeline_runs" / run_id / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "symbol_id": "TOP",
                "composite_score": 90,
                "close": 130,
                "sma_50": 120,
                "sma_200": 100,
                "sma50_slope_20d_pct": 2.0,
                "sma200_slope_20d_pct": 0.5,
                "near_52w_high_pct": 8.0,
                "relative_strength": 70,
                "trend_persistence": 80,
                "volume_intensity": 90,
                "sector_strength": 65,
                "sector": "Finance",
                "market_cap_cr": 1000,
            },
            {"symbol_id": "BBB", "composite_score": 25, "sector": "IT", "market_cap_cr": 1000},
        ]
    ).to_csv(rank_dir / "ranked_signals.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "composite_score": 82,
                "close": 110,
                "sma_50": 105,
                "sma_200": 90,
                "sma50_slope_20d_pct": 1.5,
                "sma200_slope_20d_pct": 0.4,
                "near_52w_high_pct": 9.0,
                "rank": 42,
                "relative_strength": 70,
                "trend_persistence": 80,
                "volume_intensity": 90,
                "sector_strength": 65,
                "sector": "Finance",
                "market_cap_cr": 1000,
            },
            {"symbol_id": "BBB", "composite_score": 25, "rank": 1200, "sector": "IT", "market_cap_cr": 1000},
        ]
    ).to_csv(rank_dir / "stock_scan.csv", index=False)
    pd.DataFrame([{"symbol_id": "AAA", "breakout_positive": True, "qualified": True}]).to_csv(rank_dir / "breakout_scan.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "pattern_family": "cup_handle",
                "pattern_state": "confirmed",
                "pattern_score": 81,
                "pattern_rank": 3,
                "setup_quality": 74,
            }
        ]
    ).to_csv(rank_dir / "pattern_scan.csv", index=False)
    pd.DataFrame([{"symbol_id": "AAA", "Sector": "Finance", "RS_rank_pct": 80, "Quadrant": "Leading"}]).to_csv(rank_dir / "sector_dashboard.csv", index=False)
    pd.DataFrame(
        [
            {
                "symbol_id": "EARLY",
                "sector_name": "Industrials",
                "close": 120.5,
                "early_accumulation_score": 84.0,
                "early_accumulation_rank": 1,
                "early_purity_bucket": "true_early",
                "top_pattern_family": "cup_handle",
                "top_pattern_age_days": 4,
                "base_pattern_freshness_score": 86,
                "above_200dma_reclaim_score": 72,
                "delivery_accumulation_score": 68,
                "momentum_recovery_score": 71,
                "volume_confirmation_score": 75,
                "active_rank_pctile": 55,
                "composite_score": 65,
                "volume_ratio_20": 2.5,
                "delivery_pct": 62,
                "breakout_state": "",
                "graduation_status": "pattern_confirmed",
                "watchlist_reason": "Fresh base with improving confirmation",
            }
        ]
    ).to_csv(rank_dir / "early_accumulation_scan.csv", index=False)
    (rank_dir / "dashboard_payload.json").write_text(json.dumps({"summary": {"run_date": "2026-05-07"}}), encoding="utf-8")
    return {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", rank_dir / "ranked_signals.csv", row_count=2, attempt_number=1),
            "stock_scan": StageArtifact.from_file("stock_scan", rank_dir / "stock_scan.csv", row_count=2, attempt_number=1),
            "breakout_scan": StageArtifact.from_file("breakout_scan", rank_dir / "breakout_scan.csv", row_count=1, attempt_number=1),
            "pattern_scan": StageArtifact.from_file("pattern_scan", rank_dir / "pattern_scan.csv", row_count=1, attempt_number=1),
            "sector_dashboard": StageArtifact.from_file("sector_dashboard", rank_dir / "sector_dashboard.csv", row_count=1, attempt_number=1),
            "early_accumulation_scan": StageArtifact.from_file("early_accumulation_scan", rank_dir / "early_accumulation_scan.csv", row_count=1, attempt_number=1),
            "dashboard_payload": StageArtifact.from_file("dashboard_payload", rank_dir / "dashboard_payload.json", row_count=1, attempt_number=1),
        }
    }


def test_investigator_stage_writes_artifacts_and_tables(tmp_path: Path) -> None:
    run_id = "pipeline-2026-05-07-test"
    _seed_ohlcv(tmp_path / "data" / "ohlcv.duckdb")
    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id=run_id,
        run_date="2026-05-07",
        stage_name="investigator",
        attempt_number=1,
        registry=registry,
        params={},
        artifacts=_rank_artifacts(tmp_path, run_id),
    )

    result = InvestigatorStage().run(context)

    output_dir = tmp_path / "data" / "pipeline_runs" / run_id / "investigator" / "attempt_1"
    assert (output_dir / "daily_gainer_log.csv").exists()
    assert (output_dir / "investigator_scores.csv").exists()
    assert (output_dir / "repeat_tracker.csv").exists()
    assert (output_dir / "active_watchlist.csv").exists()
    assert (output_dir / "trap_log.csv").exists()
    assert (output_dir / "archived_investigator.csv").exists()
    assert (output_dir / "final_3q_gate.csv").exists()
    assert (output_dir / "investigator_performance_summary.csv").exists()
    assert (output_dir / "investigator_performance_summary.json").exists()
    assert (output_dir / "investigator_threshold_recommendations.json").exists()
    assert (output_dir / "investigator_early_accumulation.csv").exists()
    assert (output_dir / "investigator_summary.json").exists()
    assert (output_dir / "investigator_payload.json").exists()
    early = pd.read_csv(output_dir / "investigator_early_accumulation.csv")
    scores = pd.read_csv(output_dir / "investigator_scores.csv")
    assert early.iloc[0]["symbol"] == "EARLY"
    assert early.iloc[0]["sector"] == "Industrials"
    assert early.iloc[0]["early_purity_bucket"] == "true_early"
    assert bool(early.iloc[0]["breakout_qualified"]) is False
    early_score = scores.loc[scores["symbol_id"].eq("EARLY")].iloc[0]
    assert early_score["primary_candidate_source"] == "EARLY_ACCUMULATION"
    assert early_score["candidate_sources"] == "EARLY_ACCUMULATION"
    assert bool(early_score["new_candidate_today"]) is True
    assert result.metadata["total_intake_count"] == 3
    assert result.metadata["investigator_early_accumulation_count"] == 1
    assert result.metadata["candidate_union_rows"] == 4
    assert result.metadata["early_accumulation_only_rows"] == 1
    assert result.metadata["daily_gainer_count"] == 1
    assert result.metadata["weekly_gainer_count"] == 1
    assert result.metadata["stealth_accumulation_count"] == 1
    assert result.metadata["trigger_counts"] == {
        "DAILY_GAINER": 1,
        "WEEKLY_GAINER": 1,
        "STEALTH_ACCUMULATION": 1,
    }
    aaa = scores.loc[scores["symbol_id"].eq("AAA")].iloc[0]
    assert aaa["stage_label"] == "STAGE_2_CONFIRMED"
    assert aaa["pattern_family"] == "CUP_HANDLE"
    assert aaa["pattern_state"] == "CONFIRMED"
    assert result.metadata["stage_pattern_context"]["rank_pattern_reused_rows"] == 1
    assert {artifact.artifact_type for artifact in result.artifacts} >= {
        "daily_gainer_log",
        "investigator_scores",
        "investigator_performance_summary",
        "investigator_performance_summary_json",
        "investigator_threshold_recommendations",
        "investigator_early_accumulation",
        "investigator_summary",
        "investigator_payload",
    }
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM investigator_scores").fetchone()[0] == 4
        row = conn.execute("SELECT composite_score, rank_position FROM investigator_scores WHERE symbol_id = 'AAA'").fetchone()
        assert row == (82.0, 42.0)
        source_row = conn.execute(
            "SELECT primary_candidate_source, candidate_sources, candidate_source_count, new_candidate_today "
            "FROM investigator_scores WHERE symbol_id = 'EARLY'"
        ).fetchone()
        assert source_row == ("EARLY_ACCUMULATION", "EARLY_ACCUMULATION", 1, True)


def test_registry_migration_creates_investigator_cohort_performance(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)

    with registry._reader() as conn:  # noqa: SLF001
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('investigator_cohort_performance')").fetchall()
        }
        score_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('investigator_scores')").fetchall()
        }
        column_info = {
            row[1]: row
            for row in conn.execute("PRAGMA table_info('investigator_cohort_performance')").fetchall()
        }

    assert {
        "trade_date",
        "symbol_id",
        "exchange",
        "trigger_reason",
        "verdict",
        "final_score",
        "hard_trap_flag",
        "credible_trigger",
        "move_tag",
        "sector",
        "close",
        "fwd_3d_return",
        "fwd_5d_return",
        "fwd_10d_return",
        "fwd_20d_return",
        "fwd_3d_matured_at",
        "fwd_5d_matured_at",
        "fwd_10d_matured_at",
        "fwd_20d_matured_at",
        "data_quality_status",
        "inserted_at",
        "updated_at",
        "stage_label",
        "pattern_family",
        "pattern_state",
        "setup_quality_bucket",
        "breakout_type",
        "candidate_tier",
        "qualified_breakout",
    }.issubset(columns)
    assert {
        "stage_label",
        "pattern_family",
        "pattern_state",
        "setup_quality_bucket",
        "breakout_type",
        "candidate_tier",
        "qualified_breakout",
    }.issubset(score_columns)
    assert {
        "candidate_sources",
        "primary_candidate_source",
        "candidate_source_count",
        "new_candidate_today",
    }.issubset(score_columns)
    assert "PENDING" in str(column_info["data_quality_status"][4])


def test_investigator_migrations_are_replay_safe(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)

    registry._apply_migrations()  # noqa: SLF001 - explicitly exercise startup replay behavior

    with registry._reader() as conn:  # noqa: SLF001
        score_columns = {row[1] for row in conn.execute("PRAGMA table_info('investigator_scores')").fetchall()}
        lifecycle_columns = {row[1] for row in conn.execute("PRAGMA table_info('investigator_lifecycle')").fetchall()}
        archive_columns = {row[1] for row in conn.execute("PRAGMA table_info('investigator_archive')").fetchall()}
    source_columns = {
        "candidate_sources",
        "primary_candidate_source",
        "candidate_source_count",
        "new_candidate_today",
    }
    assert source_columns.issubset(score_columns)
    assert source_columns.issubset(lifecycle_columns)
    assert source_columns.issubset(archive_columns)


def test_investigator_stage_reloads_previous_stage1_watchlist(tmp_path: Path, monkeypatch) -> None:
    first_run_id = "pipeline-2026-05-07-first"
    _seed_ohlcv(tmp_path / "data" / "ohlcv.duckdb")
    registry = RegistryStore(tmp_path)
    first_context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id=first_run_id,
        run_date="2026-05-07",
        stage_name="investigator",
        attempt_number=1,
        registry=registry,
        params={},
        artifacts=_rank_artifacts(tmp_path, first_run_id),
    )
    first_result = InvestigatorStage().run(first_context)
    active_artifact = next(artifact for artifact in first_result.artifacts if artifact.artifact_type == "active_watchlist")
    first_active = pd.read_csv(active_artifact.uri)
    assert "EARLY" in set(first_active["symbol_id"])

    monkeypatch.setattr(registry, "get_latest_artifact", lambda **_: [active_artifact])
    second_run_id = "pipeline-2026-05-08-second"
    second_artifacts = _rank_artifacts(tmp_path, second_run_id)
    early_path = Path(second_artifacts["rank"]["early_accumulation_scan"].uri)
    pd.DataFrame(columns=["symbol_id", "early_accumulation_score"]).to_csv(early_path, index=False)
    second_context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id=second_run_id,
        run_date="2026-05-08",
        stage_name="investigator",
        attempt_number=1,
        registry=registry,
        params={},
        artifacts=second_artifacts,
    )

    second_result = InvestigatorStage().run(second_context)
    scores_artifact = next(artifact for artifact in second_result.artifacts if artifact.artifact_type == "investigator_scores")
    second_scores = pd.read_csv(scores_artifact.uri)
    carried = second_scores.loc[second_scores["symbol_id"].eq("EARLY")].iloc[0]

    assert carried["primary_candidate_source"] == "PREVIOUS_WATCHLIST"
    assert carried["candidate_sources"] == "PREVIOUS_WATCHLIST"
    assert bool(carried["new_candidate_today"]) is False
    assert second_result.metadata["previous_watchlist_rows"] >= 1


def test_registry_migration_creates_sprint3_final_gate_columns(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)

    with registry._reader() as conn:  # noqa: SLF001
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('investigator_final_gate')").fetchall()
        }

    assert {
        "invalidation_source",
        "gate_entry_date",
        "days_since_gate_entry",
        "latest_close",
        "invalidation_breached",
        "followthrough_status",
        "exit_triggered",
        "exit_reason",
        "hard_trap_flag",
        "credible_trigger",
    }.issubset(columns)


def test_registry_migration_updates_indexed_investigator_cohort_table(tmp_path: Path) -> None:
    db_path = tmp_path / "data" / "control_plane.duckdb"
    db_path.parent.mkdir(parents=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE investigator_cohort_performance (
                trade_date DATE NOT NULL,
                symbol_id VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL DEFAULT 'NSE',
                trigger_reason VARCHAR,
                verdict VARCHAR,
                final_score DOUBLE,
                hard_trap_flag BOOLEAN,
                credible_trigger BOOLEAN,
                move_tag VARCHAR,
                sector VARCHAR,
                close DOUBLE,
                fwd_3d_return DOUBLE,
                fwd_5d_return DOUBLE,
                fwd_10d_return DOUBLE,
                fwd_20d_return DOUBLE,
                fwd_3d_matured_at DATE,
                fwd_5d_matured_at DATE,
                fwd_10d_matured_at DATE,
                fwd_20d_matured_at DATE,
                data_quality_status VARCHAR,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (trade_date, symbol_id, exchange)
            )
            """
        )
        conn.execute(
            "CREATE INDEX idx_investigator_cohort_data_quality_status "
            "ON investigator_cohort_performance(data_quality_status)"
        )
    finally:
        conn.close()

    registry = RegistryStore(tmp_path)

    with registry._reader() as conn:  # noqa: SLF001
        column_info = {
            row[1]: row
            for row in conn.execute("PRAGMA table_info('investigator_cohort_performance')").fetchall()
        }
        indexes = {
            row[0]
            for row in conn.execute(
                """
                SELECT index_name
                FROM duckdb_indexes()
                WHERE table_name = 'investigator_cohort_performance'
                """
            ).fetchall()
        }

    assert "PENDING" in str(column_info["data_quality_status"][4])
    assert "idx_investigator_cohort_data_quality_status" in indexes


def test_investigator_cohort_upsert_is_idempotent_and_pending(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    final_gate = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-07",
                "verdict": "HIGH_CONVICTION",
                "final_score": 88,
                "hard_trap_flag": False,
                "credible_trigger": True,
            }
        ]
    )
    scores = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "trade_date": "2026-05-07",
                "trigger_reason": "DAILY_GAINER",
                "move_tag": "SECTOR_ROTATION",
                "sector": "Finance",
                "stage_label": "STAGE_2_CONFIRMED",
                "pattern_family": "CUP_HANDLE",
                "candidate_tier": "A",
                "qualified_breakout": True,
                "close": 110.0,
            }
        ]
    )

    with registry._writer() as conn:  # noqa: SLF001
        assert upsert_investigator_cohorts(conn, final_gate, scores) == 1
        assert upsert_investigator_cohorts(conn, final_gate, scores) == 1

    with registry._reader() as conn:  # noqa: SLF001
        rows = conn.execute(
            """
            SELECT
                COUNT(*),
                MIN(trigger_reason),
                MIN(move_tag),
                MIN(sector),
                MIN(stage_label),
                MIN(pattern_family),
                MIN(candidate_tier),
                BOOL_OR(qualified_breakout),
                MIN(close),
                MIN(data_quality_status),
                COUNT(fwd_3d_return),
                COUNT(fwd_20d_return)
            FROM investigator_cohort_performance
            WHERE symbol_id = 'AAA'
            """
        ).fetchone()

    assert rows == (
        1,
        "DAILY_GAINER",
        "SECTOR_ROTATION",
        "Finance",
        "STAGE_2_CONFIRMED",
        "CUP_HANDLE",
        "A",
        True,
        110.0,
        "PENDING",
        0,
        0,
    )


def test_investigator_stage_persists_final_gate_cohorts(tmp_path: Path, monkeypatch) -> None:
    run_id = "pipeline-2026-05-07-cohort"
    _seed_ohlcv(tmp_path / "data" / "ohlcv.duckdb")
    registry = RegistryStore(tmp_path)

    def fake_final_gate(scores: pd.DataFrame) -> pd.DataFrame:
        source = scores.loc[scores["symbol_id"].eq("AAA")].copy()
        return pd.DataFrame(
            [
                {
                    "symbol_id": "AAA",
                    "trade_date": source.iloc[0]["trade_date"],
                    "verdict": "HIGH_CONVICTION",
                    "final_score": 88,
                    "thesis": "Cohort seed test",
                    "invalidation_level": "100",
                    "invalidation_source": "low",
                    "exit_plan": "Exit on invalidation breach, failed 3-session follow-through, or investigator score below 55.",
                    "gate_status": "PENDING",
                    "hard_trap_flag": False,
                    "credible_trigger": True,
                }
            ]
        )

    monkeypatch.setattr(investigator_service_module, "final_gate", fake_final_gate)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id=run_id,
        run_date="2026-05-07",
        stage_name="investigator",
        attempt_number=1,
        registry=registry,
        params={},
        artifacts=_rank_artifacts(tmp_path, run_id),
    )

    InvestigatorStage().run(context)

    with registry._reader() as conn:  # noqa: SLF001
        row = conn.execute(
            """
            SELECT COUNT(*), MIN(symbol_id), MIN(data_quality_status), COUNT(fwd_5d_return)
            FROM investigator_cohort_performance
            """
        ).fetchone()

    assert row == (1, "AAA", "PENDING", 0)


def test_investigator_stage_scans_non_s2_active_s1_pattern_candidate(tmp_path: Path, monkeypatch) -> None:
    run_id = "pipeline-2026-05-07-s1"
    _seed_ohlcv(tmp_path / "data" / "ohlcv.duckdb")
    registry = RegistryStore(tmp_path)
    captured: dict[str, object] = {}

    def fake_load_pattern_frame(*args, **kwargs):
        captured["load_symbols"] = kwargs["symbols"]
        return pd.DataFrame(
            [
                {"symbol_id": "STEALTH", "timestamp": "2026-05-07", "close": 108.5},
            ]
        )

    def fake_build_pattern_signals(*args, **kwargs):
        captured["stage2_only"] = kwargs["stage2_only"]
        captured["write_pattern_cache"] = kwargs["write_pattern_cache"]
        assert "STEALTH" in kwargs["symbols"]
        assert "AAA" not in kwargs["symbols"]
        return pd.DataFrame(
            [
                {
                    "symbol_id": "STEALTH",
                    "pattern_family": "round_bottom",
                    "pattern_state": "watchlist",
                    "pattern_lifecycle_state": "watchlist",
                    "pattern_score": 72.0,
                    "setup_quality": 62.0,
                    "stage2_score": 42.0,
                    "stage2_label": "watchlist",
                    "breakout_level": 120.0,
                    "watchlist_trigger_level": 118.0,
                    "invalidation_price": 101.0,
                    "is_combined_volume_confirmation": True,
                    "breakout_volume_ratio": 1.4,
                }
            ]
        )

    monkeypatch.setattr(pattern_scan_module, "load_pattern_frame", fake_load_pattern_frame)
    monkeypatch.setattr(pattern_scan_module, "build_pattern_signals", fake_build_pattern_signals)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id=run_id,
        run_date="2026-05-07",
        stage_name="investigator",
        attempt_number=1,
        registry=registry,
        params={},
        artifacts=_rank_artifacts(tmp_path, run_id),
    )

    InvestigatorStage().run(context)

    output_dir = tmp_path / "data" / "pipeline_runs" / run_id / "investigator" / "attempt_1"
    active = pd.read_csv(output_dir / "active_watchlist.csv")
    scores = pd.read_csv(output_dir / "investigator_scores.csv")
    pattern_scan = pd.read_csv(output_dir / "investigator_pattern_scan.csv")
    payload = json.loads((output_dir / "investigator_payload.json").read_text(encoding="utf-8"))

    assert "STEALTH" in set(active["symbol_id"].astype(str))
    assert "STEALTH" in set(pattern_scan["symbol_id"].astype(str))
    assert captured["stage2_only"] is False
    assert captured["write_pattern_cache"] is False
    assert str(scores.loc[scores["symbol_id"].eq("STEALTH"), "execution_eligible"].iloc[0]).lower() in {"false", "0"}
    assert active.loc[active["symbol_id"].eq("STEALTH"), "s1_promotion_state"].iloc[0] == "S1_TO_S2_TRANSITION"
    assert payload["pattern_confirmation"]["scanned_count"] == 1
    assert payload["pattern_confirmation"]["s1_to_s2_transition"] == 1
    queue_row = next(row for row in payload["decision_queue"] if row["symbol_id"] == "STEALTH")
    assert queue_row["pattern_state"] == "watchlist"
    assert queue_row["s1_promotion_state"] == "S1_TO_S2_TRANSITION"
    assert queue_row["promotion_reason"]
    with registry._reader() as conn:  # noqa: SLF001
        row = conn.execute(
            """
            SELECT
                trade_date,
                symbol_id,
                pattern_family,
                stage2_label,
                breakout_level,
                watchlist_trigger_level,
                invalidation_price,
                s1_promotion_state,
                source_investigator,
                source_ranked
            FROM investigator_pattern_scan
            WHERE symbol_id = 'STEALTH'
            """
        ).fetchone()
    assert row == (
        pd.Timestamp("2026-05-07").date(),
        "STEALTH",
        "round_bottom",
        "watchlist",
        120.0,
        118.0,
        101.0,
        "S1_TO_S2_TRANSITION",
        True,
        False,
    )


def test_investigator_pattern_history_keeps_multiple_states_across_dates(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)
    with registry._writer() as conn:  # noqa: SLF001
        conn.execute(
            """
            INSERT INTO investigator_pattern_scan (
                run_id,
                attempt_number,
                artifact_uri,
                trade_date,
                symbol_id,
                pattern_family,
                pattern_state,
                pattern_lifecycle_state,
                pattern_score,
                setup_quality,
                stage2_score,
                s1_promotion_state,
                promotion_reason,
                source_investigator,
                source_ranked
            )
            VALUES
                ('run-2026-05-01', 1, 'artifact://old', '2026-05-01', 'KIRLPNU', 'round_bottom', 'watchlist', 'watchlist', 52, 40, 30, 'S1_ACCUMULATION', 'Accumulation evidence', true, false),
                ('run-2026-05-07', 1, 'artifact://mid', '2026-05-07', 'KIRLPNU', 'round_bottom', 'watchlist', 'watchlist', 72, 63, 55, 'S1_TO_S2_TRANSITION', 'Transition evidence', true, false),
                ('run-2026-05-14', 1, 'artifact://new', '2026-05-14', 'KIRLPNU', 'round_bottom', 'confirmed', 'confirmed', 80, 70, 75, 'S2_CONFIRMED', 'Confirmed evidence', true, true)
            """
        )

    history = get_investigator_pattern_history("kirlpnu", 20, as_of="2026-05-14", project_root=tmp_path)

    assert history["symbol_id"] == "KIRLPNU"
    assert [row["s1_promotion_state"] for row in history["history"]] == [
        "S2_CONFIRMED",
        "S1_TO_S2_TRANSITION",
        "S1_ACCUMULATION",
    ]


def test_pipeline_order_places_investigator_after_rank() -> None:
    assert PIPELINE_ORDER.index("investigator") == PIPELINE_ORDER.index("rank") + 1
