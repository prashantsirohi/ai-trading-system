"""Read-only data loading for the fundamental opportunity report."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


@dataclass(frozen=True)
class FundamentalOpportunityInputs:
    as_of: str
    fundamentals: pd.DataFrame = field(default_factory=pd.DataFrame)
    quarterly_results: pd.DataFrame = field(default_factory=pd.DataFrame)
    company_growth: pd.DataFrame = field(default_factory=pd.DataFrame)
    valuation_bands: pd.DataFrame = field(default_factory=pd.DataFrame)
    tracker_current: pd.DataFrame = field(default_factory=pd.DataFrame)
    warnings: list[str] = field(default_factory=list)


def load_inputs(
    *,
    as_of: str | None,
    fundamentals_db_path: str | Path,
    ohlcv_db_path: str | Path,
    tracker_db_path: str | Path | None = None,
    fundamental_scores_path: str | Path | None = None,
    universe_id: str = "UNIV_TOP1000_MCAP",
) -> FundamentalOpportunityInputs:
    warnings: list[str] = []
    resolved_as_of = as_of or _latest_available_date(fundamentals_db_path, ohlcv_db_path) or str(pd.Timestamp.utcnow().date())
    fundamentals = _read_latest_fundamental_scores(fundamental_scores_path, warnings)
    company_growth = _read_latest_company_growth(fundamentals_db_path, resolved_as_of, warnings)
    quarterly = _quarterly_from_growth(company_growth)
    valuation = _read_latest_valuation_bands(ohlcv_db_path, resolved_as_of, universe_id, warnings)
    tracker = _read_tracker_current(tracker_db_path, warnings)

    return FundamentalOpportunityInputs(
        as_of=str(resolved_as_of)[:10],
        fundamentals=fundamentals,
        quarterly_results=quarterly,
        company_growth=company_growth,
        valuation_bands=valuation,
        tracker_current=tracker,
        warnings=warnings,
    )


def assemble_classifier_frame(inputs: FundamentalOpportunityInputs) -> pd.DataFrame:
    base = _symbol_frame(inputs.fundamentals)
    for other in (
        _symbol_frame(inputs.company_growth),
        _symbol_frame(inputs.quarterly_results),
        _symbol_frame(inputs.valuation_bands),
        _symbol_frame(inputs.tracker_current),
    ):
        if other.empty:
            continue
        if base.empty:
            base = other.copy()
            continue
        add = other.drop_duplicates("symbol")
        add_cols = [column for column in add.columns if column == "symbol" or column not in base.columns]
        base = base.merge(add.loc[:, add_cols], on="symbol", how="outer")
    return base.reset_index(drop=True) if not base.empty else pd.DataFrame(columns=["symbol"])


def _read_latest_fundamental_scores(path: str | Path | None, warnings: list[str]) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    csv_path = Path(path)
    if not csv_path.exists():
        warnings.append(f"fundamental scores missing: {csv_path}")
        return pd.DataFrame()
    return _safe_read_csv(csv_path, warnings, "fundamental scores")


def _read_latest_company_growth(db_path: str | Path, as_of: str, warnings: list[str]) -> pd.DataFrame:
    path = Path(db_path)
    if not path.exists():
        warnings.append(f"fundamentals DB missing: {path}")
        return pd.DataFrame()
    conn = duckdb.connect(str(path), read_only=True)
    try:
        if not _table_exists(conn, "company_growth_features"):
            warnings.append("company_growth_features table missing")
            return pd.DataFrame()
        return conn.execute(
            """
            SELECT *
            FROM company_growth_features
            WHERE available_at <= CAST(? AS DATE)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY available_at DESC, report_date DESC
            ) = 1
            """,
            [str(as_of)[:10]],
        ).df()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"company growth unavailable: {exc}")
        return pd.DataFrame()
    finally:
        conn.close()


def _quarterly_from_growth(growth: pd.DataFrame) -> pd.DataFrame:
    if growth is None or growth.empty:
        return pd.DataFrame()
    out = growth.copy()
    out.loc[:, "symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    mappings = {
        "sales_yoy_pct": ("sales_yoy_growth", 100.0),
        "sales_qoq_pct": ("sales_qoq_growth", 100.0),
        "operating_profit_yoy_pct": ("operating_profit_yoy_growth", 100.0),
        "operating_profit_qoq_pct": ("operating_profit_qoq_growth", 100.0),
        "profit_yoy_pct": ("profit_yoy_growth", 100.0),
        "profit_qoq_pct": ("profit_qoq_growth", 100.0),
        "opm_yoy_change_bps": ("opm_yoy_change", 100.0),
        "opm_qoq_change_bps": ("opm_qoq_change", 100.0),
        "sales_growth_3y": ("sales_8q_cagr", 100.0),
        "profit_growth_3y": ("profit_8q_cagr", 100.0),
    }
    for output, (source, scale) in mappings.items():
        if source in out.columns and output not in out.columns:
            out.loc[:, output] = pd.to_numeric(out[source], errors="coerce") * scale
    if "opm" not in out.columns and "opm_pct" in out.columns:
        out.loc[:, "opm"] = out["opm_pct"]
    if "net_profit_cr" in out.columns and "free_cash_flow_last_year" not in out.columns:
        out.loc[:, "free_cash_flow_last_year"] = pd.NA
    return out


def _read_latest_valuation_bands(
    db_path: str | Path,
    as_of: str,
    universe_id: str,
    warnings: list[str],
) -> pd.DataFrame:
    path = Path(db_path)
    if not path.exists():
        warnings.append(f"OHLCV DB missing: {path}")
        return pd.DataFrame()
    conn = duckdb.connect(str(path), read_only=True)
    try:
        if not _table_exists(conn, "stock_valuation_bands"):
            warnings.append("stock_valuation_bands table missing")
            return pd.DataFrame()
        return conn.execute(
            """
            SELECT *
            FROM stock_valuation_bands
            WHERE universe_id = ?
              AND date <= CAST(? AS DATE)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY date DESC
            ) = 1
            """,
            [str(universe_id).strip().upper(), str(as_of)[:10]],
        ).df()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"valuation bands unavailable: {exc}")
        return pd.DataFrame()
    finally:
        conn.close()


def _read_tracker_current(path: str | Path | None, warnings: list[str]) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    db_path = Path(path)
    if not db_path.exists():
        return pd.DataFrame()
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        if not _table_exists(conn, "tracked_candidates"):
            return pd.DataFrame()
        frame = conn.execute(
            """
            SELECT
                symbol,
                latest_status AS tracker_status,
                latest_watchlist_bucket AS tracker_watchlist_bucket,
                first_seen_date,
                last_seen_date,
                active
            FROM tracked_candidates
            WHERE active = TRUE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY last_seen_date DESC, updated_at DESC
            ) = 1
            """
        ).df()
        return frame
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"candidate tracker unavailable: {exc}")
        return pd.DataFrame()
    finally:
        conn.close()


def _latest_available_date(fundamentals_db_path: str | Path, ohlcv_db_path: str | Path) -> str | None:
    for path_value, table, column in (
        (fundamentals_db_path, "company_growth_features", "available_at"),
        (ohlcv_db_path, "stock_valuation_bands", "date"),
    ):
        path = Path(path_value)
        if not path.exists():
            continue
        conn = duckdb.connect(str(path), read_only=True)
        try:
            if _table_exists(conn, table):
                value = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0]
                if value is not None:
                    return str(value)[:10]
        except Exception:
            continue
        finally:
            conn.close()
    return None


def _table_exists(conn: duckdb.DuckDBPyConnection, table: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table],
        ).fetchone()[0]
    )


def _safe_read_csv(path: Path, warnings: list[str], label: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"{label} unreadable: {exc}")
        return pd.DataFrame()


def _symbol_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["symbol"])
    out = frame.copy()
    if "symbol" not in out.columns:
        for candidate in ("symbol_id", "ticker", "NSE Code"):
            if candidate in out.columns:
                out.loc[:, "symbol"] = out[candidate]
                break
    if "symbol" not in out.columns:
        return pd.DataFrame(columns=["symbol"])
    out.loc[:, "symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    return out.loc[out["symbol"].ne("")].copy()


__all__ = ["FundamentalOpportunityInputs", "assemble_classifier_frame", "load_inputs"]
