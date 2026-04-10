from __future__ import annotations

from pathlib import Path

import pandas as pd

from collectors import archive_nse_bhavcopy


def test_bhavcopy_filename_formats_trade_date() -> None:
    assert archive_nse_bhavcopy.bhavcopy_filename("2025-04-01") == "nse_01APR2025.csv"


def test_archive_bhavcopy_range_saves_and_skips(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    class DummyCollector:
        def __init__(self, data_dir: str) -> None:
            self.data_dir = data_dir

        def get_bhavcopy(self, trade_date: str) -> pd.DataFrame:
            calls.append(trade_date)
            if trade_date == "2025-04-02":
                return pd.DataFrame()
            return pd.DataFrame(
                [
                    {
                        "SYMBOL": "INFY",
                        "DATE1": trade_date,
                        "OPEN_PRICE": 100.0,
                        "HIGH_PRICE": 110.0,
                        "LOW_PRICE": 95.0,
                        "CLOSE_PRICE": 105.0,
                    }
                ]
            )

    monkeypatch.setattr(archive_nse_bhavcopy, "NSECollector", DummyCollector)
    monkeypatch.setattr(
        archive_nse_bhavcopy,
        "iter_business_dates",
        lambda from_date, to_date: ["2025-04-01", "2025-04-02", "2025-04-03"],
    )

    result = archive_nse_bhavcopy.archive_bhavcopy_range(
        project_root=tmp_path,
        from_date="2025-04-01",
        to_date="2025-04-03",
        force=False,
        delay_seconds=0.0,
    )

    raw_dir = tmp_path / "data" / "raw" / "NSE_EQ"
    assert (raw_dir / "nse_01APR2025.csv").exists()
    assert not (raw_dir / "nse_02APR2025.csv").exists()
    assert (raw_dir / "nse_03APR2025.csv").exists()
    assert result["saved_count"] == 2
    assert result["missing_dates"] == ["2025-04-02"]

    result_2 = archive_nse_bhavcopy.archive_bhavcopy_range(
        project_root=tmp_path,
        from_date="2025-04-01",
        to_date="2025-04-03",
        force=False,
        delay_seconds=0.0,
    )
    assert result_2["skipped_count"] == 2
