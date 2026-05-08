"""Enrich rank-stage sector_dashboard.csv with industry fundamental scores."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

from ai_trading_system.domains.fundamentals.enrich_rank import (
    DEFAULT_INDUSTRY_SCORES_PATH,
    _read_industry_scores,
)
from ai_trading_system.domains.fundamentals.industry_schema import normalize_industry_key


_SECTOR_COLUMN_CANDIDATES = ("industry", "industry_group", "sector", "sector_name")


def enrich_sector_dashboard(
    *,
    rank_dir: str | Path,
    industry_scores: str | Path = DEFAULT_INDUSTRY_SCORES_PATH,
    output: str | Path | None = None,
) -> pd.DataFrame:
    """Enrich sector_dashboard.csv with industry fundamental scores."""

    rank_dir = Path(rank_dir)
    sector_path = rank_dir / "sector_dashboard.csv"
    if not sector_path.exists():
        raise FileNotFoundError(f"sector_dashboard.csv not found under {rank_dir}")
    try:
        sector = pd.read_csv(sector_path)
    except EmptyDataError:
        sector = pd.DataFrame()
    if sector.empty:
        return sector

    industry_frame = _read_industry_scores(industry_scores)

    join_column = next((column for column in _SECTOR_COLUMN_CANDIDATES if column in sector.columns), None)
    sector = sector.copy()
    if join_column is None:
        sector.loc[:, "industry_key"] = ""
    else:
        sector.loc[:, "industry_key"] = sector[join_column].map(normalize_industry_key)

    if industry_frame.empty:
        for column, default in (
            ("industry_fundamental_score", 50.0),
            ("industry_growth_score", 50.0),
            ("industry_quality_score", 50.0),
            ("industry_valuation_score", 50.0),
            ("industry_momentum_score", 50.0),
            ("industry_fundamental_label", "UNKNOWN"),
            ("industry_warning", ""),
        ):
            sector.loc[:, column] = default
        merged = sector
    else:
        merged = sector.merge(industry_frame, on="industry_key", how="left", suffixes=("", "_industry"))
        for column, default in (
            ("industry_fundamental_score", 50.0),
            ("industry_growth_score", 50.0),
            ("industry_quality_score", 50.0),
            ("industry_valuation_score", 50.0),
            ("industry_momentum_score", 50.0),
        ):
            if column not in merged.columns:
                merged.loc[:, column] = default
            merged.loc[:, column] = pd.to_numeric(merged[column], errors="coerce").fillna(default)
        if "industry_fundamental_label" not in merged.columns:
            merged.loc[:, "industry_fundamental_label"] = "UNKNOWN"
        merged.loc[:, "industry_fundamental_label"] = (
            merged["industry_fundamental_label"].fillna("UNKNOWN").replace("", "UNKNOWN")
        )
        warning_values = (
            merged["industry_warning"].astype("object").fillna("")
            if "industry_warning" in merged.columns
            else pd.Series("", index=merged.index, dtype="object")
        )
        merged = merged.drop(columns=["industry_warning"], errors="ignore")
        merged.loc[:, "industry_warning"] = warning_values

    output_path = Path(output) if output is not None else (rank_dir / "sector_dashboard_enriched.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    return merged
