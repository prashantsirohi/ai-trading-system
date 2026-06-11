"""SQLite store for Screener.in company financials."""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import DEFAULT_STATEMENT_BASIS
from ai_trading_system.platform.db.paths import get_domain_paths

logger = logging.getLogger(__name__)


PREDEFINED_METRICS = {
    "sales": ("Sales", "P&L", "income_statement", "INR", "cr", True),
    "expenses": ("Expenses", "P&L", "income_statement", "INR", "cr", False),
    "operating_profit": ("Operating profit", "P&L", "income_statement", "INR", "cr", True),
    "other_income": ("Other income", "P&L", "income_statement", "INR", "cr", True),
    "depreciation": ("Depreciation", "P&L", "income_statement", "INR", "cr", False),
    "interest": ("Interest", "P&L", "income_statement", "INR", "cr", False),
    "profit_before_tax": ("Profit before tax", "P&L", "income_statement", "INR", "cr", True),
    "tax_expense": ("Tax", "P&L", "income_statement", "INR", "cr", False),
    "net_profit": ("Net profit", "P&L", "income_statement", "INR", "cr", True),
    "dividend_amount": ("Dividend Amount", "P&L", "income_statement", "INR", "cr", True),
    "eps": ("EPS", "P&L", "income_statement", "INR", "units", True),
    "dividend_payout_pct": ("Dividend Payout", "Ratio", "income_statement", "percent", "units", True),
    "opm_pct": ("OPM", "Ratio", "income_statement", "percent", "units", True),
    "equity_share_capital": ("Equity Share Capital", "Equity", "balance_sheet", "INR", "cr", True),
    "reserves": ("Reserves", "Equity", "balance_sheet", "INR", "cr", True),
    "borrowings": ("Borrowings", "Liability", "balance_sheet", "INR", "cr", False),
    "other_liabilities": ("Other Liabilities", "Liability", "balance_sheet", "INR", "cr", False),
    "total_liabilities": ("Total Liabilities", "Liability", "balance_sheet", "INR", "cr", None),
    "net_block": ("Net Block", "Asset", "balance_sheet", "INR", "cr", True),
    "capital_work_in_progress": ("Capital Work in Progress", "Asset", "balance_sheet", "INR", "cr", True),
    "investments": ("Investments", "Asset", "balance_sheet", "INR", "cr", True),
    "other_assets": ("Other Assets", "Asset", "balance_sheet", "INR", "cr", True),
    "total_assets": ("Total Assets", "Asset", "balance_sheet", "INR", "cr", None),
    "receivables": ("Receivables", "Asset", "balance_sheet", "INR", "cr", None),
    "inventory": ("Inventory", "Asset", "balance_sheet", "INR", "cr", None),
    "cash_and_bank": ("Cash & Bank", "Asset", "balance_sheet", "INR", "cr", True),
    "equity_shares_outstanding": ("No. of Equity Shares", "Equity", "balance_sheet", "count", "units", None),
    "new_bonus_shares": ("New Bonus Shares", "Equity", "balance_sheet", "count", "units", None),
    "adjusted_equity_shares_cr": ("Adjusted Equity Shares in Cr", "Equity", "balance_sheet", "count", "cr", None),
    "cash_from_operations": ("Cash from Operating Activity", "Cash Flow", "cash_flow", "INR", "cr", True),
    "cash_from_investing": ("Cash from Investing Activity", "Cash Flow", "cash_flow", "INR", "cr", None),
    "cash_from_financing": ("Cash from Financing Activity", "Cash Flow", "cash_flow", "INR", "cr", None),
    "net_cash_flow": ("Net Cash Flow", "Cash Flow", "cash_flow", "INR", "cr", True),
}

_READMODEL_TABLES = {
    "screener_financials",
    "screener_market_valuation",
    "screener_company_snapshot",
    "screener_factor_snapshot",
}


RAW_LABEL_MAPPING = {
    "sales": "sales",
    "expenses": "expenses",
    "operating profit": "operating_profit",
    "other income": "other_income",
    "depreciation": "depreciation",
    "interest": "interest",
    "profit before tax": "profit_before_tax",
    "tax": "tax_expense",
    "net profit": "net_profit",
    "dividend amount": "dividend_amount",
    "eps": "eps",
    "dividend payout": "dividend_payout_pct",
    "opm": "opm_pct",
    "equity share capital": "equity_share_capital",
    "reserves": "reserves",
    "borrowings": "borrowings",
    "other liabilities": "other_liabilities",
    "total liabilities": "total_liabilities",
    "net block": "net_block",
    "capital work in progress": "capital_work_in_progress",
    "investments": "investments",
    "other assets": "other_assets",
    "total assets": "total_assets",
    "receivables": "receivables",
    "inventory": "inventory",
    "cash & bank": "cash_and_bank",
    "no. of equity shares": "equity_shares_outstanding",
    "new bonus shares": "new_bonus_shares",
    "adjusted equity shares in cr": "adjusted_equity_shares_cr",
    "cash from operating activity": "cash_from_operations",
    "cash from investing activity": "cash_from_investing",
    "cash from financing activity": "cash_from_financing",
    "net cash flow": "net_cash_flow",
}


def default_screener_db_path(project_root: Path | str | None = None) -> Path:
    return get_domain_paths(project_root).fundamentals_dir / "screener_financials.db"


class ScreenerFinancialsStore:
    """Repository for Screener Excel financials stored in SQLite."""

    def __init__(self, db_path: str | Path | None = None, *, initialize: bool = True):
        self.db_path = Path(db_path) if db_path is not None else default_screener_db_path()
        if initialize:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.init_db()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_company_snapshot (
                    symbol TEXT NOT NULL,
                    as_of_date DATE NOT NULL,
                    face_value REAL,
                    market_cap_cr REAL,
                    source TEXT NOT NULL DEFAULT 'screener',
                    sync_batch_id TEXT,
                    synced_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (symbol, as_of_date, source)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_market_valuation (
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    price REAL,
                    market_cap_cr REAL,
                    pe REAL,
                    pb REAL,
                    ev_ebitda REAL,
                    dividend_yield REAL,
                    source TEXT NOT NULL DEFAULT 'screener',
                    sync_batch_id TEXT,
                    synced_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (symbol, date, source)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_metric_catalog (
                    metric_id TEXT PRIMARY KEY,
                    metric_name TEXT NOT NULL,
                    category TEXT,
                    statement_type TEXT,
                    unit TEXT,
                    scale TEXT,
                    higher_is_better BOOLEAN,
                    source TEXT DEFAULT 'screener'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_financials (
                    symbol TEXT NOT NULL,
                    period_type TEXT NOT NULL,
                    report_date DATE NOT NULL,
                    statement_basis TEXT NOT NULL DEFAULT 'standalone',
                    metric_id TEXT NOT NULL,
                    value REAL,
                    available_at DATE NOT NULL,
                    source TEXT DEFAULT 'screener',
                    sync_batch_id TEXT,
                    synced_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (symbol, period_type, report_date, statement_basis, metric_id, available_at),
                    FOREIGN KEY (metric_id) REFERENCES screener_metric_catalog(metric_id)
                )
                """
            )
            _ensure_sqlite_column(
                conn,
                "screener_financials",
                "statement_basis",
                "TEXT NOT NULL DEFAULT 'standalone'",
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_sync_batch (
                    sync_batch_id TEXT PRIMARY KEY,
                    started_at TIMESTAMP NOT NULL,
                    finished_at TIMESTAMP,
                    status TEXT NOT NULL,
                    symbols_total INTEGER DEFAULT 0,
                    symbols_succeeded INTEGER DEFAULT 0,
                    symbols_failed INTEGER DEFAULT 0,
                    exports_dir TEXT,
                    force INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_sync_error (
                    sync_batch_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    error TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (sync_batch_id, symbol)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_factor_snapshot (
                    symbol TEXT NOT NULL,
                    snapshot_date DATE NOT NULL,
                    factor_name TEXT NOT NULL,
                    factor_value REAL,
                    source TEXT NOT NULL DEFAULT 'screener',
                    synced_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (symbol, snapshot_date, factor_name, source)
                )
                """
            )
            for metric_id, info in PREDEFINED_METRICS.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO screener_metric_catalog (
                        metric_id, metric_name, category, statement_type, unit, scale, higher_is_better
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (metric_id, *info),
                )
            conn.commit()

    def begin_batch(self, sync_batch_id: str, *, symbols_total: int, exports_dir: Path, force: bool) -> None:
        now = _utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO screener_sync_batch (
                    sync_batch_id, started_at, status, symbols_total, symbols_succeeded,
                    symbols_failed, exports_dir, force
                ) VALUES (?, ?, 'running', ?, 0, 0, ?, ?)
                """,
                (sync_batch_id, now, int(symbols_total), str(exports_dir), int(force)),
            )
            conn.commit()

    def finish_batch(self, sync_batch_id: str, *, succeeded: int, failed: int) -> None:
        status = "completed" if failed == 0 else "completed_with_errors"
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE screener_sync_batch
                SET finished_at = ?, status = ?, symbols_succeeded = ?, symbols_failed = ?
                WHERE sync_batch_id = ?
                """,
                (_utc_now(), status, int(succeeded), int(failed), sync_batch_id),
            )
            conn.commit()

    def record_error(self, sync_batch_id: str, symbol: str, error: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO screener_sync_error (
                    sync_batch_id, symbol, error, created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (sync_batch_id, symbol.upper().strip(), str(error), _utc_now()),
            )
            conn.commit()

    def get_synced_symbols(self) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute("SELECT DISTINCT symbol FROM screener_company_snapshot").fetchall()
        return {str(row["symbol"]).upper() for row in rows}

    def save_company_financials(
        self,
        symbol: str,
        data: dict[str, Any],
        *,
        sync_batch_id: str | None = None,
        as_of_date: str | None = None,
    ) -> None:
        symbol = symbol.upper().strip()
        synced_at = _utc_now()
        snapshot_date = as_of_date or datetime.now(timezone.utc).date().isoformat()
        metadata = data.get("metadata", {})
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO screener_company_snapshot (
                    symbol, as_of_date, face_value, market_cap_cr, source, sync_batch_id, synced_at
                ) VALUES (?, ?, ?, ?, 'screener', ?, ?)
                """,
                (symbol, snapshot_date, _to_float(metadata.get("face_value")), _to_float(metadata.get("market_cap_cr")), sync_batch_id, synced_at),
            )
            valuation_rows = self._compute_market_valuations(symbol, data, synced_at, sync_batch_id)
            if valuation_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO screener_market_valuation (
                        symbol, date, price, market_cap_cr, pe, pb, ev_ebitda,
                        dividend_yield, source, sync_batch_id, synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    valuation_rows,
                )
            financial_rows = self._financial_rows(conn, symbol, data, synced_at, sync_batch_id)
            if financial_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO screener_financials (
                        symbol, period_type, report_date, statement_basis, metric_id, value, available_at,
                        source, sync_batch_id, synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    financial_rows,
                )
            conn.commit()
        logger.info("Saved Screener financials for %s (%d metrics)", symbol, len(financial_rows))

    def read_financials_frame(self) -> pd.DataFrame:
        return self._read_table_frame("screener_financials")

    def read_valuations_frame(self) -> pd.DataFrame:
        return self._read_table_frame("screener_market_valuation")

    def read_company_snapshot_frame(self) -> pd.DataFrame:
        return self._read_table_frame("screener_company_snapshot")

    def read_factor_snapshot_frame(self) -> pd.DataFrame:
        return self._read_table_frame("screener_factor_snapshot")

    def _read_table_frame(self, table_name: str) -> pd.DataFrame:
        if table_name not in _READMODEL_TABLES:
            raise ValueError(f"Unsupported Screener table: {table_name}")
        if not self.db_path.exists():
            return pd.DataFrame()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            if row is None:
                return pd.DataFrame()
            return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

    def get_company_data(self, symbol: str) -> dict[str, Any] | None:
        symbol = symbol.upper().strip()
        with self.connect() as conn:
            meta = conn.execute(
                """
                SELECT face_value, market_cap_cr
                FROM screener_company_snapshot
                WHERE symbol = ?
                ORDER BY as_of_date DESC LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            if meta is None:
                return None
            rows = conn.execute(
                """
                SELECT f.period_type, f.report_date, f.value, c.metric_name, c.statement_type
                FROM screener_financials f
                JOIN screener_metric_catalog c ON c.metric_id = f.metric_id
                WHERE f.symbol = ?
                """,
                (symbol,),
            ).fetchall()
            prices = conn.execute(
                "SELECT date, price FROM screener_market_valuation WHERE symbol = ? ORDER BY date",
                (symbol,),
            ).fetchall()
        result: dict[str, Any] = {
            "metadata": {"symbol": symbol, **dict(meta)},
            "profit_loss": {},
            "quarters": {},
            "balance_sheet": {},
            "cash_flow": {},
            "derived": {"prices": {row["date"]: row["price"] for row in prices}},
        }
        for row in rows:
            target = _section_for_row(row["period_type"], row["statement_type"])
            result[target].setdefault(row["metric_name"], {})[row["report_date"]] = row["value"]
        return result

    def _financial_rows(
        self,
        conn: sqlite3.Connection,
        symbol: str,
        data: dict[str, Any],
        synced_at: str,
        sync_batch_id: str | None,
    ) -> list[tuple[Any, ...]]:
        sections = {
            "profit_loss": "annual",
            "quarters": "quarterly",
            "balance_sheet": "annual",
            "cash_flow": "annual",
            "derived": "annual",
        }
        rows: list[tuple[Any, ...]] = []
        for section_key, period_type in sections.items():
            for raw_metric_name, values_by_date in data.get(section_key, {}).items():
                if raw_metric_name.lower().strip() in {"face value", "prices"} or not isinstance(values_by_date, dict):
                    continue
                metric_id, display_name = _normalize_metric(raw_metric_name)
                _ensure_metric(conn, metric_id, display_name)
                for report_date, value in values_by_date.items():
                    numeric = _to_float(value)
                    if numeric is None:
                        continue
                    rows.append(
                        (
                            symbol,
                            period_type,
                            str(report_date)[:10],
                            DEFAULT_STATEMENT_BASIS,
                            metric_id,
                            numeric,
                            _available_at(period_type, str(report_date)[:10]),
                            "screener",
                            sync_batch_id,
                            synced_at,
                        )
                    )
        return rows

    def _compute_market_valuations(
        self,
        symbol: str,
        data: dict[str, Any],
        synced_at: str,
        sync_batch_id: str | None,
    ) -> list[tuple[Any, ...]]:
        pl = data.get("profit_loss", {})
        bs = data.get("balance_sheet", {})
        derived = data.get("derived", {})
        prices = derived.get("prices", {})
        shares = derived.get("Adjusted Equity Shares in Cr", {})
        if not isinstance(prices, dict):
            return []
        rows: list[tuple[Any, ...]] = []
        for date_str, price in prices.items():
            p = _to_float(price)
            if p is None:
                continue
            mcap = _mul(p, _to_float(_dict_get(shares, date_str)))
            net_profit = _to_float(_dict_get(pl.get("Net profit", {}), date_str))
            share_capital = _to_float(_dict_get(bs.get("Equity Share Capital", {}), date_str))
            reserves = _to_float(_dict_get(bs.get("Reserves", {}), date_str))
            borrowings = _to_float(_dict_get(bs.get("Borrowings", {}), date_str)) or 0.0
            cash = _to_float(_dict_get(bs.get("Cash & Bank", {}), date_str)) or 0.0
            operating_profit = _to_float(_dict_get(pl.get("Operating profit", pl.get("Operating Profit", {})), date_str))
            dividend = _to_float(_dict_get(pl.get("Dividend Amount", {}), date_str))
            book = (share_capital or 0.0) + (reserves or 0.0)
            pe = _safe_div(mcap, net_profit)
            pb = _safe_div(mcap, book)
            ev_ebitda = _safe_div((mcap or 0.0) + borrowings - cash, operating_profit)
            dividend_yield = (_safe_div(dividend, mcap) or 0.0) * 100.0 if dividend is not None else None
            rows.append((symbol, str(date_str)[:10], p, mcap, pe, pb, ev_ebitda, dividend_yield, "screener", sync_batch_id, synced_at))
        return rows


def _normalize_metric(raw_name: str) -> tuple[str, str]:
    raw_clean = str(raw_name).lower().strip()
    if raw_clean in RAW_LABEL_MAPPING:
        metric_id = RAW_LABEL_MAPPING[raw_clean]
        return metric_id, PREDEFINED_METRICS[metric_id][0]
    display_name = str(raw_name).strip()
    metric_id = re.sub(r"[^a-z0-9]+", "_", display_name.lower()).strip("_")
    return metric_id, display_name


def _ensure_metric(conn: sqlite3.Connection, metric_id: str, display_name: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO screener_metric_catalog (
            metric_id, metric_name, category, statement_type, unit, scale, higher_is_better
        ) VALUES (?, ?, 'other', 'unknown', 'units', 'units', NULL)
        """,
        (metric_id, display_name),
    )


def _ensure_sqlite_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name not in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


def _available_at(period_type: str, report_date: str) -> str:
    try:
        dt = datetime.strptime(report_date[:10], "%Y-%m-%d")
    except ValueError:
        return report_date
    lag = 45 if period_type == "quarterly" else 90
    return (dt + timedelta(days=lag)).date().isoformat()


def _section_for_row(period_type: str, statement_type: str) -> str:
    if period_type == "quarterly":
        return "quarters"
    if statement_type == "income_statement":
        return "profit_loss"
    if statement_type == "balance_sheet":
        return "balance_sheet"
    if statement_type == "cash_flow":
        return "cash_flow"
    return "derived"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(str(value).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return float(numerator) / float(denominator)


def _mul(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return float(a) * float(b)


def _dict_get(mapping: Any, key: Any) -> Any:
    return mapping.get(key) if isinstance(mapping, dict) else None


__all__ = ["ScreenerFinancialsStore", "default_screener_db_path"]
