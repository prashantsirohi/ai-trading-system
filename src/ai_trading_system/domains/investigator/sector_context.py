"""Sector support scoring."""

from __future__ import annotations

import pandas as pd


def attach_sector_context(frame: pd.DataFrame, sector_dashboard: pd.DataFrame | None) -> pd.DataFrame:
    out = frame.copy()
    if out.empty:
        out["sector_support_score"] = []
        out["sector_rotation_active"] = []
        return out
    if sector_dashboard is not None and not sector_dashboard.empty:
        sector_col = next((col for col in ("sector", "Sector", "sector_name") if col in sector_dashboard.columns), None)
        candidate_sector_col = next((col for col in ("sector", "Sector", "sector_name") if col in out.columns), None)
        if sector_col and candidate_sector_col:
            sec = sector_dashboard.copy()
            sec.loc[:, "_sector_key"] = sec[sector_col].astype(str).str.upper()
            out.loc[:, "_sector_key"] = out[candidate_sector_col].astype(str).str.upper()
            cols = ["_sector_key"] + [col for col in ("RS", "RS_rank_pct", "rel_strength", "Quadrant", "peer_gainers_count") if col in sec.columns]
            out = out.merge(sec[cols].drop_duplicates("_sector_key"), on="_sector_key", how="left", suffixes=("", "_sector"))
            out = out.drop(columns=["_sector_key"])
    rs = _first_numeric(out, ["RS", "rel_strength_sector"])
    rs_rank_pct = pd.to_numeric(_series(out, "RS_rank_pct"), errors="coerce")
    peer_count = pd.to_numeric(_series(out, "peer_gainers_count"), errors="coerce")
    stock_vs_sector = pd.to_numeric(_series(out, "stock_vs_sector_rs"), errors="coerce")
    quadrant = out.get("Quadrant", pd.Series("", index=out.index)).fillna("").astype(str).str.lower()
    sector_positive = rs.gt(0).fillna(False) | rs_rank_pct.ge(60).fillna(False) | quadrant.isin({"leading", "improving"})
    sector_rotation = peer_count.ge(5).fillna(False) | quadrant.eq("leading")
    score = (
        sector_positive.astype(int) * 4
        + sector_rotation.astype(int) * 3
        + stock_vs_sector.gt(0).fillna(False).astype(int) * 3
    )
    out.loc[:, "sector_support_score"] = score.clip(upper=10)
    out.loc[:, "sector_rotation_active"] = sector_rotation
    out.loc[:, "sector_clustering"] = sector_rotation | peer_count.ge(3).fillna(False)
    return out


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(pd.NA, index=frame.index)


def _first_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    result = pd.Series(pd.NA, index=frame.index, dtype="object")
    for column in columns:
        if column in frame.columns:
            result = result.where(result.notna(), frame[column].astype("object"))
    return pd.to_numeric(result, errors="coerce")
