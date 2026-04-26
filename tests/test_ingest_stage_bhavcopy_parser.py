from __future__ import annotations

import csv
from pathlib import Path

from ai_trading_system.analytics.registry import RegistryStore
from ai_trading_system.pipeline.contracts import StageContext
from ai_trading_system.pipeline.stages.ingest import IngestStage


def test_ingest_bhavcopy_parser_handles_series_with_whitespace(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "NSE_EQ"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "nse_07APR2026.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["SYMBOL", " SERIES", " CLOSE_PRICE"],
        )
        writer.writeheader()
        writer.writerow({"SYMBOL": "AAA", " SERIES": " EQ ", " CLOSE_PRICE": "123.45"})

    registry = RegistryStore(tmp_path)
    context = StageContext(
        project_root=tmp_path,
        db_path=tmp_path / "data" / "ohlcv.duckdb",
        run_id="pipeline-2026-04-08-test",
        run_date="2026-04-08",
        stage_name="ingest",
        attempt_number=1,
        registry=registry,
        params={"bhavcopy_validation_date": "2026-04-07"},
    )

    frame, source = IngestStage()._load_bhavcopy_close_frame(context, "2026-04-07")
    assert source == "nse_bhavcopy:2026-04-07"
    assert len(frame) == 1
    assert frame.iloc[0]["symbol_id"] == "AAA"
    assert float(frame.iloc[0]["close_bhavcopy"]) == 123.45
