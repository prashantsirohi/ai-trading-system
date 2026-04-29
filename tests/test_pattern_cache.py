from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.analytics.patterns.evaluation import (
    _build_pattern_lifecycle_snapshot,
    _stage2_prescreened,
    _write_pattern_cache,
)
from ai_trading_system.domains.ranking.patterns.cache import (
    ACTIVE_LIFECYCLE_STATES,
    PatternCacheStore,
)


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
                "volume_zscore_20": 2.4,
                "volume_zscore_50": 1.9,
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
                "volume_zscore_20": None,
                "volume_zscore_50": 2.2,
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


def test_write_and_read_roundtrip_uses_daily_snapshot_schema(tmp_path: Path) -> None:
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")
    written = store.write_signals(_sample_signals(), scan_run_id="weekly_full_2026-04-21", replace_date="2026-04-21")

    cached = store.read_snapshot(as_of_date="2026-04-21")

    assert written == 2
    assert len(cached) == 2
    assert set(cached["symbol_id"]) == {"AAA", "BBB"}
    assert set(cached["pattern_lifecycle_state"].astype(str)) == {"confirmed", "watchlist"}
    assert set(cached["as_of_date"].astype(str)) == {"2026-04-21"}
    assert set(cached["fresh_signal_date"].astype(str)) == {"2026-04-21"}
    assert "carry_forward_bars" in cached.columns
    assert float(cached.loc[cached["symbol_id"] == "AAA", "volume_zscore_20"].iloc[0]) == 2.4
    assert float(cached.loc[cached["symbol_id"] == "BBB", "volume_zscore_50"].iloc[0]) == 2.2


def test_latest_snapshot_and_active_loaders_use_as_of_date(tmp_path: Path) -> None:
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")
    store.write_signals(_sample_signals("2026-04-20"), scan_run_id="weekly_full_2026-04-20", replace_date="2026-04-20")
    store.write_signals(_sample_signals("2026-04-21"), scan_run_id="incremental:2026-04-21:2", replace_date="2026-04-21")

    assert store.latest_snapshot_date() == "2026-04-21"
    assert store.latest_cached_signal_date(as_of_date="2026-04-22") == "2026-04-21"
    assert store.latest_full_scan_date() == "2026-04-20"

    active = store.load_latest_active_signals_before(as_of_date="2026-04-22")
    assert set(active["pattern_lifecycle_state"].astype(str)) == set(ACTIVE_LIFECYCLE_STATES)


def test_latest_full_scan_date_includes_weekly_full_and_manual_full(tmp_path: Path) -> None:
    store = PatternCacheStore(tmp_path / "control_plane.duckdb")
    store.write_signals(_sample_signals("2026-04-18"), scan_run_id="full:2026-04-18:2", replace_date="2026-04-18")
    store.write_signals(_sample_signals("2026-04-20"), scan_run_id="weekly_full_2026-04-20", replace_date="2026-04-20")
    store.write_signals(_sample_signals("2026-04-21"), scan_run_id="incremental:2026-04-21:2", replace_date="2026-04-21")

    assert store.latest_full_scan_date() == "2026-04-20"


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


def test_write_pattern_cache_replaces_prior_same_day_snapshot(tmp_path: Path) -> None:
    project_root = tmp_path
    store = PatternCacheStore(project_root / "data" / "control_plane.duckdb")
    store.write_signals(
        _sample_signals("2026-04-17"),
        scan_run_id="incremental:2026-04-21:128",
        replace_run_scope="incremental:2026-04-21:",
        replace_date="2026-04-21",
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

    snapshot = store.read_snapshot(as_of_date="2026-04-21")

    assert snapshot["symbol_id"].tolist() == ["CCC"]
    assert snapshot["as_of_date"].astype(str).tolist() == ["2026-04-21"]


def test_write_pattern_cache_uses_weekly_full_identifier(tmp_path: Path) -> None:
    project_root = tmp_path
    _write_pattern_cache(
        project_root=project_root,
        data_domain="operational",
        exchange="NSE",
        signal_date="2026-04-21",
        scan_mode="weekly_full",
        selected_symbols=["AAA", "BBB"],
        signals_df=_sample_signals("2026-04-21"),
    )

    conn = duckdb.connect(str(project_root / "data" / "control_plane.duckdb"), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT scan_run_id
            FROM pattern_cache
            ORDER BY scan_run_id
            """
        ).fetchall()
    finally:
        conn.close()

    assert rows == [("weekly_full_2026-04-21",)]


def test_build_pattern_lifecycle_snapshot_fresh_overrides_cached_by_merge_key() -> None:
    previous = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "flag",
                "pattern_state": "watchlist",
                "pattern_lifecycle_state": "watchlist",
                "signal_date": "2026-04-14",
                "as_of_date": "2026-04-18",
                "fresh_signal_date": "2026-04-14",
                "first_seen_date": "2026-04-14",
                "last_seen_date": "2026-04-18",
                "carry_forward_bars": 2,
                "invalidation_price": 95.0,
                "pattern_score": 65.0,
            }
        ]
    )
    fresh = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "flag",
                "pattern_state": "confirmed",
                "signal_date": "2026-04-21",
                "invalidation_price": 95.0,
                "pattern_score": 91.0,
            }
        ]
    )

    snapshot = _build_pattern_lifecycle_snapshot(
        fresh_signals_df=fresh,
        previous_snapshot_df=previous,
        market_frame=pd.DataFrame(),
        as_of_date="2026-04-21",
        scan_mode="incremental",
        exchange="NSE",
        watchlist_expiry_bars=10,
        confirmed_expiry_bars=20,
        invalidated_retention_bars=5,
    )

    assert len(snapshot) == 1
    assert snapshot.iloc[0]["pattern_state"] == "confirmed"
    assert snapshot.iloc[0]["pattern_lifecycle_state"] == "confirmed"
    assert str(snapshot.iloc[0]["first_seen_date"]) == "2026-04-14"
    assert int(snapshot.iloc[0]["carry_forward_bars"]) == 0


def test_build_pattern_lifecycle_snapshot_expires_invalidated_rows_after_retention() -> None:
    previous = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "flag",
                "pattern_state": "confirmed",
                "pattern_lifecycle_state": "invalidated",
                "signal_date": "2026-04-01",
                "as_of_date": "2026-04-03",
                "fresh_signal_date": "2026-04-01",
                "first_seen_date": "2026-04-01",
                "last_seen_date": "2026-04-01",
                "invalidated_date": "2026-04-01",
                "carry_forward_bars": 2,
                "invalidation_price": 95.0,
                "pattern_score": 70.0,
            }
        ]
    )

    snapshot = _build_pattern_lifecycle_snapshot(
        fresh_signals_df=pd.DataFrame(),
        previous_snapshot_df=previous,
        market_frame=pd.DataFrame(),
        as_of_date="2026-04-08",
        scan_mode="incremental",
        exchange="NSE",
        watchlist_expiry_bars=10,
        confirmed_expiry_bars=20,
        invalidated_retention_bars=5,
    )

    assert snapshot.iloc[0]["pattern_lifecycle_state"] == "expired"
    assert str(snapshot.iloc[0]["expired_date"]) == "2026-04-08"


def test_build_pattern_lifecycle_snapshot_invalidates_active_row_on_price_breach() -> None:
    previous = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "flag",
                "pattern_state": "confirmed",
                "pattern_lifecycle_state": "confirmed",
                "signal_date": "2026-04-14",
                "as_of_date": "2026-04-18",
                "fresh_signal_date": "2026-04-14",
                "first_seen_date": "2026-04-14",
                "last_seen_date": "2026-04-18",
                "carry_forward_bars": 1,
                "invalidation_price": 95.0,
                "pattern_score": 88.0,
            }
        ]
    )
    market_frame = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "timestamp": pd.Timestamp("2026-04-21"),
                "close": 94.0,
            }
        ]
    )

    snapshot = _build_pattern_lifecycle_snapshot(
        fresh_signals_df=pd.DataFrame(),
        previous_snapshot_df=previous,
        market_frame=market_frame,
        as_of_date="2026-04-21",
        scan_mode="incremental",
        exchange="NSE",
        watchlist_expiry_bars=10,
        confirmed_expiry_bars=20,
        invalidated_retention_bars=5,
    )

    assert snapshot.iloc[0]["pattern_lifecycle_state"] == "invalidated"
    assert str(snapshot.iloc[0]["invalidated_date"]) == "2026-04-21"


def test_build_pattern_lifecycle_snapshot_expires_watchlist_after_configured_bars() -> None:
    previous = pd.DataFrame(
        [
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "pattern_family": "vcp",
                "pattern_state": "watchlist",
                "pattern_lifecycle_state": "watchlist",
                "signal_date": "2026-04-01",
                "as_of_date": "2026-04-03",
                "fresh_signal_date": "2026-04-01",
                "first_seen_date": "2026-04-01",
                "last_seen_date": "2026-04-06",
                "carry_forward_bars": 4,
                "invalidation_price": 190.0,
                "pattern_score": 75.0,
            }
        ]
    )

    snapshot = _build_pattern_lifecycle_snapshot(
        fresh_signals_df=pd.DataFrame(),
        previous_snapshot_df=previous,
        market_frame=pd.DataFrame(),
        as_of_date="2026-04-20",
        scan_mode="incremental",
        exchange="NSE",
        watchlist_expiry_bars=10,
        confirmed_expiry_bars=20,
        invalidated_retention_bars=5,
    )

    assert snapshot.iloc[0]["pattern_lifecycle_state"] == "expired"
    assert str(snapshot.iloc[0]["expired_date"]) == "2026-04-20"


def test_build_pattern_lifecycle_snapshot_tolerates_missing_pattern_score_on_fresh_rows() -> None:
    fresh = pd.DataFrame(
        [
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "pattern_family": "vcp",
                "pattern_state": "confirmed",
                "signal_date": "2026-04-21",
                "invalidation_price": 190.0,
            }
        ]
    )

    snapshot = _build_pattern_lifecycle_snapshot(
        fresh_signals_df=fresh,
        previous_snapshot_df=pd.DataFrame(),
        market_frame=pd.DataFrame(),
        as_of_date="2026-04-21",
        scan_mode="incremental",
        exchange="NSE",
        watchlist_expiry_bars=10,
        confirmed_expiry_bars=20,
        invalidated_retention_bars=5,
    )

    assert len(snapshot) == 1
    assert snapshot.iloc[0]["symbol_id"] == "BBB"
    assert snapshot.iloc[0]["pattern_lifecycle_state"] == "confirmed"
    assert "pattern_score" in snapshot.columns


def test_build_pattern_lifecycle_snapshot_avoids_all_na_concat_warning() -> None:
    previous = pd.DataFrame(
        [
            {
                "symbol_id": "AAA",
                "exchange": "NSE",
                "pattern_family": "flag",
                "pattern_state": "watchlist",
                "pattern_lifecycle_state": "watchlist",
                "signal_date": "2026-04-14",
                "as_of_date": "2026-04-18",
                "fresh_signal_date": "2026-04-14",
                "first_seen_date": "2026-04-14",
                "last_seen_date": "2026-04-18",
                "carry_forward_bars": 2,
                "invalidation_price": 95.0,
                "pattern_score": None,
                "optional_all_na_context": None,
            }
        ]
    )
    fresh = pd.DataFrame(
        [
            {
                "symbol_id": "BBB",
                "exchange": "NSE",
                "pattern_family": "vcp",
                "pattern_state": "confirmed",
                "signal_date": "2026-04-21",
                "invalidation_price": 190.0,
                "pattern_score": None,
                "optional_all_na_context": None,
            }
        ]
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        snapshot = _build_pattern_lifecycle_snapshot(
            fresh_signals_df=fresh,
            previous_snapshot_df=previous,
            market_frame=pd.DataFrame(),
            as_of_date="2026-04-21",
            exchange="NSE",
            scan_mode="incremental",
            watchlist_expiry_bars=10,
            confirmed_expiry_bars=20,
            invalidated_retention_bars=5,
        )

    assert snapshot["symbol_id"].tolist() == ["BBB", "AAA"]
    assert "pattern_score" in snapshot.columns


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
