from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pandas as pd
import requests

from collectors import dhan_collector
from collectors.dhan_collector import DhanCollector, dhan_daily_window_ist, normalize_dhan_timestamps_ist


class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        base = datetime(2026, 4, 8, 6, 30, 0, tzinfo=timezone.utc)
        if tz is None:
            return base.replace(tzinfo=None)
        return base.astimezone(tz)


def test_dhan_daily_window_helper_returns_today_minus_one_to_today_ist() -> None:
    from_date, to_date = dhan_daily_window_ist(datetime(2026, 4, 8, 10, 0, 0))
    assert from_date == "2026-04-07"
    assert to_date == "2026-04-08"


def test_normalize_dhan_timestamps_converts_epoch_to_ist_dates() -> None:
    ts = normalize_dhan_timestamps_ist([1774981800, 1775068200])
    out = pd.to_datetime(ts).dt.date.astype(str).tolist()
    assert out == ["2026-04-01", "2026-04-02"]


def test_fetch_one_daily_defaults_to_fixed_dhan_window(monkeypatch, tmp_path) -> None:
    collector = DhanCollector(
        db_path=str(tmp_path / "ohlcv.duckdb"),
        masterdb_path=str(tmp_path / "masterdata.db"),
        feature_store_dir=str(tmp_path / "feature_store"),
        data_domain="operational",
    )
    monkeypatch.setattr(dhan_collector, "datetime", _FixedDateTime)

    captured = {}

    def fake_fetch_sync(security_id, exchange, from_date, to_date, session, _retry_after_renewal=False):
        captured["security_id"] = security_id
        captured["exchange"] = exchange
        captured["from_date"] = from_date
        captured["to_date"] = to_date
        frame = pd.DataFrame(
            [{"timestamp": "2026-04-08", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1}]
        ).set_index("timestamp")
        return frame

    monkeypatch.setattr(collector, "_fetch_sync", fake_fetch_sync)

    session = requests.Session()
    try:
        frame = asyncio.run(
            collector._fetch_one_daily(
                {"security_id": "7", "exchange": "NSE", "symbol_id": "AARTIIND"},
                session,
            )
        )
    finally:
        session.close()
    assert not frame.empty
    assert captured["from_date"] == "2026-04-07"
    assert captured["to_date"] == "2026-04-08"


def test_run_daily_update_backfills_missing_ranges_and_skips_fully_uptodate(monkeypatch, tmp_path) -> None:
    collector = DhanCollector(
        db_path=str(tmp_path / "ohlcv.duckdb"),
        masterdb_path=str(tmp_path / "masterdata.db"),
        feature_store_dir=str(tmp_path / "feature_store"),
        data_domain="operational",
    )
    monkeypatch.setattr(dhan_collector, "datetime", _FixedDateTime)

    monkeypatch.setattr(
        collector,
        "get_symbols_from_masterdb",
        lambda exchanges=None: [
            {"symbol_id": "AAA", "security_id": "1", "exchange": "NSE"},
            {"symbol_id": "BBB", "security_id": "2", "exchange": "NSE"},
            {"symbol_id": "CCC", "security_id": "3", "exchange": "NSE"},
            {"symbol_id": "DDD", "security_id": "4", "exchange": "NSE"},
        ],
    )
    monkeypatch.setattr(
        collector,
        "_get_last_dates",
        lambda exchanges=None: {
            "AAA": "2026-04-04",  # stale -> should backfill from 2026-04-05
            "BBB": "2026-04-07",  # default one-day window
            "DDD": "2026-04-08",  # fully up-to-date -> should be skipped
        },
    )

    captured: dict[str, object] = {}

    async def fake_fetch_daily_batch(symbols, max_concurrent):
        captured["symbols"] = symbols
        dfs = []
        for info in symbols:
            frame = pd.DataFrame(
                [
                    {
                        "timestamp": pd.Timestamp("2026-04-08"),
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 10,
                    }
                ]
            ).set_index("timestamp")
            frame.attrs["symbol_info"] = info
            dfs.append(frame)
        return dfs, 0

    monkeypatch.setattr(collector, "_fetch_daily_batch", fake_fetch_daily_batch)
    monkeypatch.setattr(collector, "_upsert_ohlcv", lambda dfs: len(dfs))

    result = collector.run_daily_update(
        exchanges=["NSE"],
        batch_size=10,
        max_concurrent=2,
        days_history=7,
        compute_features=False,
    )

    batch_symbols = {str(item["symbol_id"]): item for item in captured["symbols"]}
    assert "DDD" not in batch_symbols
    assert batch_symbols["AAA"]["_from_date"] == "2026-04-05"
    assert batch_symbols["BBB"]["_from_date"] == "2026-04-07"
    assert batch_symbols["CCC"]["_from_date"] == "2026-04-01"
    assert batch_symbols["AAA"]["_to_date"] == "2026-04-08"
    assert batch_symbols["BBB"]["_to_date"] == "2026-04-08"
    assert batch_symbols["CCC"]["_to_date"] == "2026-04-08"

    assert result["window_from_date"] == "2026-04-01"
    assert result["window_to_date"] == "2026-04-08"
    assert result["up_to_date_symbols"] == 1
    assert "AAA" in result["stale_symbols"]
    assert "CCC" in result["no_data_symbols"]
