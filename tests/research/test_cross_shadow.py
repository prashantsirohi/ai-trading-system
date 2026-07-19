from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.research.pattern_lane_calibration.cross_shadow import (
    load_registry_episodes,
    reconcile,
    write_cross_shadow_bundle,
)


def _make_control_plane(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE candidate_episode(
            symbol_id VARCHAR, exchange VARCHAR, candidate_id VARCHAR,
            episode_started_at TIMESTAMP, opening_reason VARCHAR,
            setup_family VARCHAR, episode_status VARCHAR
        )
        """
    )
    rows = [
        # BEFORE: pattern (07-10) precedes episode (07-15), rank-admitted
        ("BEFORE", "NSE", "c1", "2026-07-15", "RANK_THRESHOLD", "MOMENTUM_LEADER", "OPEN"),
        # SAME day
        ("SAME", "NSE", "c2", "2026-07-17", "QUALIFIED_PATTERN", "BASE_BUILDING", "OPEN"),
        # AFTER: episode (07-05) precedes pattern (07-17)
        ("AFTER", "NSE", "c3", "2026-07-05", "INVESTIGATOR_PROMOTION", "BASE_BUILDING", "OPEN"),
        # REGONLY: episode, no pattern signal
        ("REGONLY", "NSE", "c4", "2026-07-16", "QUALIFIED_BREAKOUT", "BREAKOUT", "OPEN"),
        # SUPP: OPEN episode; pattern emits head_shoulders suppression
        ("SUPP", "NSE", "c5", "2026-07-12", "RANK_VELOCITY", "MOMENTUM_LEADER", "OPEN"),
        # DUP: same symbol+setup_family, two distinct start dates
        ("DUP", "NSE", "c6", "2026-06-01", "QUALIFIED_PATTERN", "BASE_BUILDING", "CLOSED"),
        ("DUP", "NSE", "c7", "2026-07-14", "QUALIFIED_PATTERN", "BASE_BUILDING", "OPEN"),
    ]
    conn.executemany("INSERT INTO candidate_episode VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()


def _signals() -> pd.DataFrame:
    def row(sym, date, family="flat_base", ev="evidence_supported"):
        return {
            "symbol_id": sym, "exchange": "NSE", "as_of_date": "2026-07-17",
            "signal_date": date, "scan_lane_as_of": "stage1_base",
            "pattern_family": family, "r1a_evidence_class": ev, "evidence_origin": "fresh",
        }
    return pd.DataFrame([
        row("BEFORE", "2026-07-10"),
        row("SAME", "2026-07-17"),
        row("AFTER", "2026-07-17"),
        row("PATTERNONLY", "2026-07-17"),
        row("SUPP", "2026-07-17", family="head_shoulders", ev="suppression_only"),
        row("DUP", "2026-07-13"),
    ])


def test_reconcile_all_categories(tmp_path: Path) -> None:
    cp = tmp_path / "control_plane.duckdb"
    _make_control_plane(cp)
    episodes = load_registry_episodes(cp, through_date="2026-07-17")
    cats = reconcile(_signals(), episodes)

    # before/same/after partition the symbols present in BOTH by timing;
    # suppression/duplicate are orthogonal overlays that may co-occur with a
    # timing category (e.g. SUPP/DUP also have signal-after-episode timing).
    assert set(cats["pattern_before_registry"]["symbol_id"]) == {"BEFORE"}
    assert set(cats["same_day_discovery"]["symbol_id"]) == {"SAME"}
    assert set(cats["pattern_after_registry"]["symbol_id"]) == {"AFTER", "SUPP", "DUP"}
    assert set(cats["pattern_only"]["symbol_id"]) == {"PATTERNONLY"}
    assert "REGONLY" in set(cats["registry_only"]["symbol_id"])
    assert "BEFORE" not in set(cats["registry_only"]["symbol_id"])  # in-both excluded
    assert set(cats["suppression_conflict"]["symbol_id"]) == {"SUPP"}
    assert set(cats["possible_duplicate_episode"]["symbol_id"]) == {"DUP"}
    # lead/lag for BEFORE: episode 07-15 minus signal 07-10 = 5 days
    assert int(cats["pattern_before_registry"]["lead_lag_days"].iloc[0]) == 5


def test_write_bundle_is_readonly_and_immutable(tmp_path: Path) -> None:
    cp = tmp_path / "control_plane.duckdb"
    _make_control_plane(cp)
    before = duckdb.connect(str(cp), read_only=True).execute("SELECT COUNT(*) FROM candidate_episode").fetchone()[0]

    signals_csv = tmp_path / "pattern_lane_scan.csv"
    _signals().to_csv(signals_csv, index=False)

    out = tmp_path / "bundle"
    result = write_cross_shadow_bundle(
        pattern_lane_csv=signals_csv, control_plane_db=cp, output_dir=out,
        through_date="2026-07-17", project_root=tmp_path,
    )
    assert result["manifest"]["operational_side_effects"] is False
    assert (out / "cross_shadow_manifest.json").exists()
    assert (out / "cross_shadow_summary.json").exists()
    for name in ("pattern_before_registry", "same_day_discovery", "pattern_after_registry",
                 "pattern_only", "registry_only", "suppression_conflict", "possible_duplicate_episode"):
        assert (out / f"cross_shadow_{name}.csv").exists()

    summary = json.loads((out / "cross_shadow_summary.json").read_text())
    assert summary["category_counts"]["pattern_before_registry"] == 1
    assert summary["pattern_lead_by_episode_opening_reason"].get("RANK_THRESHOLD") == 1

    # registry untouched (read-only)
    after = duckdb.connect(str(cp), read_only=True).execute("SELECT COUNT(*) FROM candidate_episode").fetchone()[0]
    assert after == before


def test_write_bundle_refuses_existing_output(tmp_path: Path) -> None:
    cp = tmp_path / "control_plane.duckdb"
    _make_control_plane(cp)
    signals_csv = tmp_path / "pattern_lane_scan.csv"
    _signals().to_csv(signals_csv, index=False)
    out = tmp_path / "bundle"
    out.mkdir()
    (out / "existing.txt").write_text("x")
    import pytest
    with pytest.raises(FileExistsError):
        write_cross_shadow_bundle(
            pattern_lane_csv=signals_csv, control_plane_db=cp, output_dir=out,
            through_date="2026-07-17", project_root=tmp_path,
        )
