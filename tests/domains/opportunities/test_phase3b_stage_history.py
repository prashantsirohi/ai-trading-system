from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.domains.opportunities.coverage import (
    build_sector_coverage,
    build_stage_coverage,
    persist_stage_history,
    read_stock_stage_as_of,
)
from ai_trading_system.domains.opportunities.routing import StageCoverageConfig
from ai_trading_system.pipeline.registry import RegistryStore


def _daily(symbol: str, start: float) -> pd.DataFrame:
    dates = pd.bdate_range("2025-09-01", periods=210)
    close = pd.Series([start + index * 0.2 for index in range(len(dates))])
    return pd.DataFrame({
        "symbol_id": symbol,
        "exchange": "NSE",
        "timestamp": dates,
        "open": close - 0.5,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": 1_000_000,
    })


def test_full_universe_not_reduced_by_rank_and_history_is_idempotent(tmp_path: Path) -> None:
    daily = pd.concat([_daily("AAA", 100.0), _daily("BBB", 200.0)], ignore_index=True)
    stock, exclusions = build_stage_coverage(
        daily,
        as_of="2026-06-19",
        sector_mapping={"AAA": ("tech", "Tech"), "BBB": ("banks", "Banks")},
        config=StageCoverageConfig(minimum_liquidity_score=0.0),
        lock_current_week=True,
    )
    assert set(stock["symbol_id"]) == {"AAA", "BBB"}
    assert exclusions.empty

    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    persist_stage_history(registry, stock, pd.DataFrame(), run_id="run-1", attempt=1)
    persist_stage_history(registry, stock, pd.DataFrame(), run_id="run-1", attempt=1)
    reconstructed = read_stock_stage_as_of(registry, as_of="2026-06-20")
    assert set(reconstructed["symbol_id"]) == {"AAA", "BBB"}
    with registry._reader() as conn:  # noqa: SLF001
        assert conn.execute("SELECT COUNT(*) FROM weekly_stock_stage_history").fetchone()[0] == 2


def test_provisional_and_locked_observations_coexist(tmp_path: Path) -> None:
    daily = _daily("AAA", 100.0)
    provisional, _ = build_stage_coverage(
        daily,
        as_of="2026-06-18",
        sector_mapping={"AAA": ("tech", "Tech")},
        config=StageCoverageConfig(),
        lock_current_week=False,
    )
    locked, _ = build_stage_coverage(
        daily,
        as_of="2026-06-19",
        sector_mapping={"AAA": ("tech", "Tech")},
        config=StageCoverageConfig(),
        lock_current_week=True,
    )
    registry = RegistryStore(tmp_path, db_path=tmp_path / "control_plane.duckdb")
    persist_stage_history(registry, provisional, pd.DataFrame(), run_id="p", attempt=1)
    persist_stage_history(registry, locked, pd.DataFrame(), run_id="l", attempt=1)
    with registry._reader() as conn:  # noqa: SLF001
        statuses = {row[0] for row in conn.execute("SELECT stage_status FROM weekly_stock_stage_history").fetchall()}
    assert statuses == {"provisional", "locked"}


def test_guarded_unknown_sector_has_canonical_zero_confidence() -> None:
    stock = pd.DataFrame([
        {
            "sector_id": "tech", "sector_name": "Tech", "as_of": "2026-06-19",
            "source_week_start": "2026-06-15", "source_week_end": "2026-06-19",
            "effective_stage": "stage_1_basing", "stage_status": "locked",
            "price_vs_weekly_ma_30_pct": 2.0, "weekly_ma_30_slope": 0.2,
            "weekly_ma_30_slope_acceleration": 0.1, "weekly_rs_slope": 1.0,
        }
    ])
    sector = build_sector_coverage(stock, config=StageCoverageConfig())
    assert sector.iloc[0]["effective_stage"] == "unknown"
    assert sector.iloc[0]["stage_confidence_score"] == 0.0
    assert sector.iloc[0]["stage_confidence_band"] == "low"
