"""Phase 8 regressions for hysteresis seeding and confirmed-regime age."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb

from ai_trading_system.analytics.regime.breadth import (
    _confirmed_regime_series,
    _load_recent_raw_snapshots,
    _regime_age_series,
)


def _seed_flat_breadth_db(path: Path, *, days: int = 1305) -> str:
    conn = duckdb.connect(str(path))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            close DOUBLE,
            volume BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE _index_catalog (
            index_code VARCHAR,
            date DATE,
            close DOUBLE
        )
        """
    )
    start = date(2020, 1, 1)
    rows = []
    idx_rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        # Flat close means close == SMA once warm, so pct_above_200dma is 0.
        for symbol in ("AAA", "BBB", "CCC"):
            rows.append((symbol, "NSE", d.isoformat(), 100.0, 1000))
        idx_rows.append(("UNIV_TOP1000", d.isoformat(), 100.0))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?)", rows)
    conn.executemany("INSERT INTO _index_catalog VALUES (?, ?, ?)", idx_rows)
    conn.close()
    return (start + timedelta(days=days - 1)).isoformat()


def _sticky_strong_bull_rules() -> dict:
    return {
        "strong_bull": {
            "enter": {"pct_above_200dma_gte": 0.99},
            "exit": {"pct_above_200dma_gte": 0.0},
        },
        "neutral": {
            "enter": {"pct_above_200dma_gte": 0.0},
            "exit": {"pct_above_200dma_gte": 0.0},
        },
        "__priority__": ["strong_bull", "neutral"],
    }


def test_previous_regime_seed_only_applies_to_final_confirmation_tail(tmp_path: Path) -> None:
    db_path = tmp_path / "breadth.duckdb"
    as_of = _seed_flat_breadth_db(db_path)
    rules = _sticky_strong_bull_rules()

    long_window = _load_recent_raw_snapshots(
        db_path,
        as_of=as_of,
        exchange="NSE",
        index_code="UNIV_TOP1000",
        limit=1000,
        rules=rules,
        previous_regime="strong_bull",
        confirmation_days=3,
    )
    short_window = _load_recent_raw_snapshots(
        db_path,
        as_of=as_of,
        exchange="NSE",
        index_code="UNIV_TOP1000",
        limit=3,
        rules=rules,
        previous_regime="strong_bull",
        confirmation_days=3,
    )

    assert long_window
    # The previous-run seed describes yesterday, not the oldest row in the
    # 5-year velocity replay. Deep warmup rows must classify cold.
    assert long_window[0].raw_regime == "neutral"

    # Today's classification still matches the old short-window behavior:
    # with a sticky strong_bull previous seed, the final confirmation tail
    # remains strong_bull.
    assert long_window[-1].raw_regime == short_window[-1].raw_regime == "strong_bull"
    assert long_window[-1].regime == short_window[-1].regime == "strong_bull"


def test_regime_age_follows_confirmed_chain_not_raw_wobble() -> None:
    raw = ["bull", "bull", "bull", "neutral", "bull"]
    confirmed = _confirmed_regime_series(raw, confirmation_days=3)
    ages = _regime_age_series(confirmed)

    assert confirmed == ["neutral", "bull", "bull", "bull", "bull"]
    # The day-4 raw neutral does not confirm, so the bull age keeps climbing.
    assert ages == [0, 0, 1, 2, 3]
