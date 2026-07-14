from __future__ import annotations

import pandas as pd

from ai_trading_system.domains.ranking.input_snapshot import RankInputSnapshot


class _SnapshotLoader:
    def __init__(self) -> None:
        self.sma_calls = 0

    def load_latest_sma(self, *, date: str) -> pd.DataFrame:
        self.sma_calls += 1
        return pd.DataFrame([{"symbol_id": "SAFE", "timestamp": date, "sma_200": 90.0}])

    def load_sector_inputs(self):
        index = pd.to_datetime(["2026-01-01", "2026-02-01"])
        return (
            pd.DataFrame({"IT": [1.0, 2.0]}, index=index),
            pd.DataFrame({"SAFE": [1.0, 2.0]}, index=index),
            {"SAFE": "IT"},
        )


def test_rank_input_snapshot_caches_frames_and_enforces_sector_cutoff() -> None:
    loader = _SnapshotLoader()
    snapshot = RankInputSnapshot(loader, "2026-01-15", ("NSE",))

    first = snapshot.sma()
    first.loc[:, "sma_200"] = 0.0
    second = snapshot.sma()
    sector_rs, stock_vs_sector, sector_map = snapshot.sector_inputs()

    assert loader.sma_calls == 1
    assert second.iloc[0]["sma_200"] == 90.0
    assert sector_rs.index.max() == pd.Timestamp("2026-01-01")
    assert stock_vs_sector.index.max() == pd.Timestamp("2026-01-01")
    assert sector_map == {"SAFE": "IT"}
