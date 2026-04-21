from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from analytics.patterns.evaluation import _stage2_prescreened, _write_pattern_cache
from ai_trading_system.domains.ranking.patterns.cache import PatternCacheStore


def _sample_signals(signal_date: str = "2026-04-21") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "signal_id": "AAA-flag-confirmed-2026-04-21",
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "flag",
                "pattern_state": "confirmed",
                "signal_date": signal_date,
                "stage2_score": 88.0,
                "stage2_label": "strong_stage2",
                "breakout_level": 101.0,
                "watchlist_trigger_level": 100.5,
                "invalidation_price": 95.0,
                "pattern_score": 91.0,
                "setup_quality": 77.0,
                "width_bars": 18,
            },
            {
                "signal_id": "BBB-vcp-watchlist-2026-04-21",
                "symbol_id": "BBB",
                "exchange": "NSE",
                "pattern_family": "vcp",
                "pattern_state": "watchlist",
                "signal_date": signal_date,
                "stage2_score": 72.0,
                "stage2_label": "stage2",
                "breakout_level": 205.0,
                "watchlist_trigger_level": 204.5,
                "invalidation_price": 190.0,
                "pattern_score": 79.0,
                "setup_quality": 68.0,
                "width_bars": 24,
            },
        ]
    )


def _seed_ohlcv(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
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
                volume DOUBLE
            )
            """
        )
        rows = []
        for i in range(25):
            ts = f"2026-04-{i+1:02d} 15:30:00"
            rows.append(("AAA", "NSE", ts, 100 + i, 101 + i, 99 + i, 100 + i, 1000))
            close_bbb = 200 if i < 24 else 210
            volume_bbb = 1000 if i < 24 else 2500
            rows.append(("BBB", "NSE", ts, close_bbb, close_bbb + 1, close_bbb - 1, close_bbb, volume_bbb))
        conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def test_write_and_read_roundtrip(tmp_path: Path) -> None:
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")
    written = store.write_signals(_sample_signals(), scan_run_id="full:2026-04-21:2", replace_date="2026-04-21")

    cached = store.read_cached_signals(signal_date="2026-04-21")

    assert written == 2
    assert len(cached) == 2
    assert set(cached["symbol_id"]) == {"AAA", "BBB"}
    assert "pattern_score" in cached.columns


def test_replace_date_clears_old_rows(tmp_path: Path) -> None:
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")
    store.write_signals(_sample_signals(), scan_run_id="full:2026-04-21:2", replace_date="2026-04-21")
    replacement = _sample_signals().iloc[[0]].copy()
    replacement.loc[:, "symbol_id"] = "CCC"
    replacement.loc[:, "signal_id"] = "CCC-flag-confirmed-2026-04-21"

    store.write_signals(replacement, scan_run_id="incremental:2026-04-21:1", replace_date="2026-04-21")
    cached = store.read_cached_signals(signal_date="2026-04-21")

    assert cached["symbol_id"].tolist() == ["CCC"]


def test_replace_run_scope_clears_prior_rerun_rows(tmp_path: Path) -> None:
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")
    prior = _sample_signals().copy()
    prior.loc[:, "signal_date"] = ["2026-04-17", "2026-04-15"]
    store.write_signals(
        prior,
        scan_run_id="incremental:2026-04-21:128",
        replace_run_scope="incremental:2026-04-21:",
    )

    replacement = _sample_signals().iloc[[0]].copy()
    replacement.loc[:, "symbol_id"] = "CCC"
    replacement.loc[:, "signal_id"] = "CCC-flag-confirmed-2026-04-21"
    replacement.loc[:, "signal_date"] = "2026-04-17"
    store.write_signals(
        replacement,
        scan_run_id="incremental:2026-04-21:150",
        replace_run_scope="incremental:2026-04-21:",
    )

    conn = duckdb.connect(str(tmp_path / "control_plane.duckdb"), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT scan_run_id, symbol_id, signal_date
            FROM pattern_cache
            ORDER BY signal_date, symbol_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("incremental:2026-04-21:150", "CCC", pd.Timestamp("2026-04-17").date())]


def test_symbols_needing_rescan_filters_by_change(tmp_path: Path) -> None:
    ohlcv_db = tmp_path / "ohlcv.duckdb"
    _seed_ohlcv(ohlcv_db)
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")

    changed = store.symbols_needing_rescan(
        ["AAA", "BBB"],
        ohlcv_db_path=ohlcv_db,
        min_price_change_pct=1.0,
        min_volume_ratio=1.3,
        as_of_date="2026-04-25",
    )

    assert changed == ["BBB"]


def test_latest_full_scan_date_tracks_full_scans(tmp_path: Path) -> None:
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")
    store.write_signals(_sample_signals("2026-04-20"), scan_run_id="full:2026-04-20:2", replace_date="2026-04-20")
    store.write_signals(_sample_signals("2026-04-21"), scan_run_id="incremental:2026-04-21:1", replace_date="2026-04-21")

    assert store.latest_full_scan_date() == "2026-04-20"
    assert store.latest_cached_signal_date(as_of_date="2026-04-22") == "2026-04-21"


def test_write_pattern_cache_replaces_prior_same_day_run_scope(tmp_path: Path) -> None:
    project_root = tmp_path
    store = PatternCacheStore(project_root / "data" / "control_plane.duckdb")
    store.write_signals(
        _sample_signals("2026-04-17"),
        scan_run_id="incremental:2026-04-21:128",
        replace_run_scope="incremental:2026-04-21:",
    )

    replacement = _sample_signals("2026-04-15").iloc[[0]].copy()
    replacement.loc[:, "symbol_id"] = "CCC"
    replacement.loc[:, "signal_id"] = "CCC-flag-confirmed-2026-04-15"

    _write_pattern_cache(
        project_root=project_root,
        data_domain="operational",
        exchange="NSE",
        signal_date="2026-04-21",
        scan_mode="incremental",
        selected_symbols=["AAA", "BBB", "CCC"],
        signals_df=replacement,
    )

    conn = duckdb.connect(str(project_root / "data" / "control_plane.duckdb"), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT scan_run_id, symbol_id, signal_date
            FROM pattern_cache
            ORDER BY signal_date, symbol_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("incremental:2026-04-21:3", "CCC", pd.Timestamp("2026-04-15").date())]


def test_stage2_prescreened_splits_payloads() -> None:
    stage2_frame = pd.DataFrame({"symbol_id": ["AAA"], "stage2_score": [80.0]})
    non_stage2_frame = pd.DataFrame({"symbol_id": ["BBB"], "stage2_score": [50.0]})
    payloads = [
        {"symbol_id": "AAA", "frame": stage2_frame},
        {"symbol_id": "BBB", "frame": non_stage2_frame},
    ]

    stage2, non_stage2 = _stage2_prescreened(payloads, stage2_only=False, min_stage2_score=70.0)
    stage2_only, filtered = _stage2_prescreened(payloads, stage2_only=True, min_stage2_score=70.0)

    assert [item["symbol_id"] for item in stage2] == ["AAA"]
    assert [item["symbol_id"] for item in non_stage2] == ["BBB"]
    assert [item["symbol_id"] for item in stage2_only] == ["AAA"]
    assert filtered == []
