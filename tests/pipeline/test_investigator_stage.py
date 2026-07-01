from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.domains.investigator import pattern_scan as pattern_scan_module
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
            ('BBB', 'NSE', '2026-05-07', 100, 103, 99, 102, 3000, false, 'equity')
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
            ('BBB', 'NSE', '2026-05-07', 30)
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
    pd.DataFrame([{"symbol_id": "AAA", "Sector": "Finance", "RS_rank_pct": 80, "Quadrant": "Leading"}]).to_csv(rank_dir / "sector_dashboard.csv", index=False)
    (rank_dir / "dashboard_payload.json").write_text(json.dumps({"summary": {"run_date": "2026-05-07"}}), encoding="utf-8")
    return {
        "rank": {
            "ranked_signals": StageArtifact.from_file("ranked_signals", rank_dir / "ranked_signals.csv", row_count=2, attempt_number=1),
            "stock_scan": StageArtifact.from_file("stock_scan", rank_dir / "stock_scan.csv", row_count=2, attempt_number=1),
            "breakout_scan": StageArtifact.from_file("breakout_scan", rank_dir / "breakout_scan.csv", row_count=1, attempt_number=1),
            "sector_dashboard": StageArtifact.from_file("sector_dashboard", rank_dir / "sector_dashboard.csv", row_count=1, attempt_number=1),
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
    assert (output_dir / "investigator_summary.json").exists()
    assert (output_dir / "investigator_payload.json").exists()
    assert result.metadata["total_intake_count"] == 3
    assert result.metadata["daily_gainer_count"] == 1
    assert result.metadata["weekly_gainer_count"] == 1
    assert result.metadata["stealth_accumulation_count"] == 1
    assert result.metadata["trigger_counts"] == {
        "DAILY_GAINER": 1,
        "WEEKLY_GAINER": 1,
        "STEALTH_ACCUMULATION": 1,
    }
    assert {artifact.artifact_type for artifact in result.artifacts} >= {
        "daily_gainer_log",
        "investigator_scores",
        "investigator_summary",
        "investigator_payload",
    }
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM investigator_scores").fetchone()[0] == 3
        row = conn.execute("SELECT composite_score, rank_position FROM investigator_scores WHERE symbol_id = 'AAA'").fetchone()
        assert row == (82.0, 42.0)


def test_registry_migration_creates_investigator_cohort_performance(tmp_path: Path) -> None:
    registry = RegistryStore(tmp_path)

    with registry._reader() as conn:  # noqa: SLF001
        columns = {
            row[1]
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
    }.issubset(columns)


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
