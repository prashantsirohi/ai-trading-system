"""Rank-stage wrapper for sector rotation sidecar artifacts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ai_trading_system.analytics.sector_rotation import SectorRotationResult, compute_sector_rotation


def run_sector_rotation(
    *,
    ohlcv_db_path: str | Path,
    master_db_path: str | Path,
    run_date: str,
    output_dir: str | Path | None = None,
    ranked_df: pd.DataFrame | None = None,
    exchange: str = "NSE",
) -> dict[str, pd.DataFrame]:
    """Compute sector rotation and optionally write its JSON payload sidecar."""
    result: SectorRotationResult = compute_sector_rotation(
        ohlcv_db_path=ohlcv_db_path,
        master_db_path=master_db_path,
        run_date=run_date,
        ranked_df=ranked_df,
        exchange=exchange,
    )
    if output_dir is not None:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        import json

        (output_path / "sector_rotation_payload.json").write_text(
            json.dumps(result.payload, indent=2, default=str),
            encoding="utf-8",
        )
    frames = {
        "sector_rotation": result.sector_rotation,
        "stock_rotation": result.stock_rotation,
        "accumulation_distribution": result.accumulation_distribution,
        "sector_custom_indices": result.sector_custom_indices,
    }
    for frame in frames.values():
        frame.attrs["sector_rotation_metadata"] = result.metadata
    return frames


__all__ = ["run_sector_rotation"]
