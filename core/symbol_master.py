"""Symbol master governance helpers for canonical mapping and lifecycle checks."""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class SymbolRecord:
    symbol: str
    canonical_symbol: str
    isin: str | None
    status: str
    sector: str | None = None
    industry: str | None = None


class SymbolMaster:
    def __init__(self, frame: pd.DataFrame):
        normalized = frame.copy(deep=True)
        if normalized.empty:
            normalized = pd.DataFrame(
                columns=["symbol", "canonical_symbol", "isin", "status", "sector", "industry"]
            )
        for column in ("symbol", "canonical_symbol", "status"):
            if column not in normalized.columns:
                normalized.loc[:, column] = None
        normalized.loc[:, "symbol"] = normalized["symbol"].astype(str).str.strip().str.upper()
        normalized.loc[:, "canonical_symbol"] = (
            normalized["canonical_symbol"].fillna(normalized["symbol"]).astype(str).str.strip().str.upper()
        )
        normalized.loc[:, "status"] = normalized["status"].fillna("active").astype(str).str.strip().str.lower()
        self.frame = normalized

    @classmethod
    def from_masterdb(cls, db_path: str | Path | None) -> "SymbolMaster":
        if not db_path:
            return cls(pd.DataFrame())
        db_file = Path(str(db_path))
        if not db_file.exists():
            return cls(pd.DataFrame())
        conn = sqlite3.connect(str(db_file))
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            rows: list[dict[str, object]] = []
            if "stock_details" in tables:
                stock_cols = cls._table_columns(conn, "stock_details")
                symbol_col = cls._pick_column(stock_cols, ["Symbol", "symbol", "symbol_id"])
                canonical_col = cls._pick_column(
                    stock_cols,
                    ["canonical_symbol", "CanonicalSymbol", "Symbol", "symbol", "symbol_id"],
                )
                isin_col = cls._pick_column(stock_cols, ["ISIN", "isin"])
                status_col = cls._pick_column(stock_cols, ["status", "Status"])
                sector_col = cls._pick_column(stock_cols, ["Sector", "sector", "Industry Group", "industry_group"])
                industry_col = cls._pick_column(stock_cols, ["Industry", "industry"])
                if symbol_col is None:
                    return cls(pd.DataFrame())
                for row in conn.execute(
                    f"""
                    SELECT
                        {cls._col_expr(symbol_col)} AS symbol,
                        {cls._col_expr(canonical_col, fallback=cls._col_expr(symbol_col))} AS canonical_symbol,
                        {cls._col_expr(isin_col)} AS isin,
                        {cls._col_expr(status_col, fallback="'active'")} AS status,
                        {cls._col_expr(sector_col)} AS sector,
                        {cls._col_expr(industry_col)} AS industry
                    FROM stock_details
                    WHERE {cls._col_expr(symbol_col)} IS NOT NULL
                      AND TRIM({cls._col_expr(symbol_col)}) != ''
                    """
                ).fetchall():
                    rows.append(
                        {
                            "symbol": row[0],
                            "canonical_symbol": row[1],
                            "isin": row[2],
                            "status": row[3],
                            "sector": row[4],
                            "industry": row[5],
                        }
                    )
            elif "symbols" in tables:
                symbol_cols = cls._table_columns(conn, "symbols")
                symbol_col = cls._pick_column(symbol_cols, ["symbol_id", "symbol", "Symbol"])
                canonical_col = cls._pick_column(
                    symbol_cols,
                    ["canonical_symbol", "CanonicalSymbol", "symbol_id", "symbol", "Symbol"],
                )
                isin_col = cls._pick_column(symbol_cols, ["isin", "ISIN"])
                status_col = cls._pick_column(symbol_cols, ["status", "Status"])
                sector_col = cls._pick_column(symbol_cols, ["sector", "Sector"])
                industry_col = cls._pick_column(symbol_cols, ["industry", "Industry"])
                if symbol_col is None:
                    return cls(pd.DataFrame())
                for row in conn.execute(
                    f"""
                    SELECT
                        {cls._col_expr(symbol_col)} AS symbol,
                        {cls._col_expr(canonical_col, fallback=cls._col_expr(symbol_col))} AS canonical_symbol,
                        {cls._col_expr(isin_col)} AS isin,
                        {cls._col_expr(status_col, fallback="'active'")} AS status,
                        {cls._col_expr(sector_col)} AS sector,
                        {cls._col_expr(industry_col)} AS industry
                    FROM symbols
                    WHERE {cls._col_expr(symbol_col)} IS NOT NULL
                      AND TRIM({cls._col_expr(symbol_col)}) != ''
                    """
                ).fetchall():
                    rows.append(
                        {
                            "symbol": row[0],
                            "canonical_symbol": row[1],
                            "isin": row[2],
                            "status": row[3],
                            "sector": row[4],
                            "industry": row[5],
                        }
                    )
        except sqlite3.Error:
            return cls(pd.DataFrame())
        finally:
            conn.close()
        return cls(pd.DataFrame(rows))

    def canonicalize(self, symbol: str) -> str:
        query = str(symbol or "").strip().upper()
        if not query or self.frame.empty:
            return query
        rows = self.frame[self.frame["symbol"] == query]
        if rows.empty:
            return query
        value = rows.iloc[0].get("canonical_symbol")
        return str(value).strip().upper() if value else query

    def isin_for(self, symbol: str) -> str | None:
        canonical = self.canonicalize(symbol)
        if not canonical or self.frame.empty:
            return None
        rows = self.frame[self.frame["canonical_symbol"] == canonical]
        if rows.empty:
            return None
        value = rows.iloc[0].get("isin")
        return None if pd.isna(value) else str(value)

    def is_active(self, symbol: str) -> bool:
        canonical = self.canonicalize(symbol)
        if not canonical or self.frame.empty:
            return True
        rows = self.frame[self.frame["canonical_symbol"] == canonical]
        if rows.empty:
            return True
        return str(rows.iloc[0].get("status", "active")).strip().lower() == "active"

    def filter_active(self, symbols: list[str]) -> list[str]:
        return [symbol for symbol in symbols if self.is_active(symbol)]

    def canonicalize_symbol_rows(self, symbol_rows: list[dict]) -> list[dict]:
        if not symbol_rows:
            return []
        output: list[dict] = []
        seen: set[tuple[str, str]] = set()
        for row in symbol_rows:
            symbol_id = str(row.get("symbol_id", "")).strip().upper()
            canonical = self.canonicalize(symbol_id) or symbol_id
            if not canonical or not self.is_active(canonical):
                continue
            normalized = dict(row)
            normalized["symbol_id"] = canonical
            if not normalized.get("isin"):
                normalized["isin"] = self.isin_for(canonical)
            key = (str(normalized.get("exchange", "NSE")), canonical)
            if key in seen:
                continue
            seen.add(key)
            output.append(normalized)
        return output

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
        rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        return {str(row[1]) for row in rows}

    @staticmethod
    def _pick_column(columns: set[str], candidates: list[str]) -> str | None:
        for candidate in candidates:
            if candidate in columns:
                return candidate
        return None

    @staticmethod
    def _col_expr(column: str | None, *, fallback: str = "NULL") -> str:
        if not column:
            return fallback
        safe = str(column).replace('"', '""')
        return f'"{safe}"'
