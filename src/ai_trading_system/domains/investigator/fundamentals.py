"""Optional fundamental support scoring."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import default_fundamentals_duckdb_path
from ai_trading_system.domains.investigator.utils import as_symbol


def load_fundamental_snapshot(
    *,
    project_root: Path,
    symbols: list[str],
    fundamentals_db_path: Path | None = None,
) -> pd.DataFrame:
    path = fundamentals_db_path or default_fundamentals_duckdb_path(project_root)
    if not symbols or not path.exists():
        return pd.DataFrame(columns=["symbol_id", "fundamental_status"])
    with duckdb.connect(str(path), read_only=True) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "company_growth_features" not in tables:
            return pd.DataFrame(columns=["symbol_id", "fundamental_status"])
        rows = conn.execute(
            """
            SELECT *
            FROM (
              SELECT
                symbol AS symbol_id,
                report_date,
                sales_yoy_growth AS revenue_yoy,
                profit_yoy_growth AS pat_yoy,
                opm_yoy_change,
                positive_profit_quarters_4q,
                margin_expansion_quarters_4q,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY report_date DESC) AS rn
              FROM company_growth_features
              WHERE symbol = ANY(?)
            )
            WHERE rn = 1
            """,
            [[as_symbol(symbol) for symbol in symbols]],
        ).fetchdf()
    if rows.empty:
        return pd.DataFrame(columns=["symbol_id", "fundamental_status"])
    rows.loc[:, "symbol_id"] = rows["symbol_id"].map(as_symbol)
    rows.loc[:, "fundamental_status"] = "AVAILABLE"
    return rows.drop(columns=[col for col in ("rn",) if col in rows.columns])


def score_fundamentals(frame: pd.DataFrame, fundamental_snapshot: pd.DataFrame | None) -> pd.DataFrame:
    out = frame.copy()
    if fundamental_snapshot is not None and not fundamental_snapshot.empty:
        # Rank/Stage-1 contexts can carry prior optional fundamentals fields.
        # The Investigator snapshot is authoritative; clear stale merge
        # variants so pandas never produces recursively suffixed duplicates.
        snapshot_columns = set(fundamental_snapshot.columns) - {"symbol_id"}
        stale_columns = [
            column for column in out.columns
            if column in snapshot_columns
            or any(column == f"{name}_{suffix}" for name in snapshot_columns for suffix in ("x", "y"))
        ]
        out = out.drop(columns=stale_columns, errors="ignore")
        out = out.merge(fundamental_snapshot, on="symbol_id", how="left")
    if "fundamental_status" not in out.columns:
        out.loc[:, "fundamental_status"] = "MISSING"
    out.loc[:, "fundamental_status"] = out["fundamental_status"].fillna("MISSING")
    revenue = pd.to_numeric(_series(out, "revenue_yoy"), errors="coerce")
    pat = pd.to_numeric(_series(out, "pat_yoy"), errors="coerce")
    opm = pd.to_numeric(_series(out, "opm_yoy_change"), errors="coerce")
    profit_quarters = pd.to_numeric(_series(out, "positive_profit_quarters_4q"), errors="coerce")
    margin_quarters = pd.to_numeric(_series(out, "margin_expansion_quarters_4q"), errors="coerce")
    score = (
        revenue.gt(0).fillna(False).astype(int) * 5
        + revenue.gt(10).fillna(False).astype(int) * 3
        + pat.gt(0).fillna(False).astype(int) * 5
        + opm.gt(0).fillna(False).astype(int) * 3
        + profit_quarters.ge(3).fillna(False).astype(int) * 2
        + margin_quarters.ge(2).fillna(False).astype(int) * 2
    )
    missing = out["fundamental_status"].astype(str).str.upper().eq("MISSING")
    out.loc[:, "fundamental_score"] = score.where(~missing, 10).clip(lower=0, upper=20)
    out.loc[:, "fa_missing"] = missing
    out.loc[:, "fa_improvement"] = (
        revenue.gt(0).fillna(False) | pat.gt(0).fillna(False) | opm.gt(0).fillna(False)
    )
    out.loc[:, "fa_trigger_confirmed"] = pat.gt(15).fillna(False) | revenue.gt(20).fillna(False)
    return out


def _series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return frame[column]
    return pd.Series(pd.NA, index=frame.index)
