"""SQLite store for Screener.in company financials."""

from __future__ import annotations

import logging
import hashlib
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import DEFAULT_STATEMENT_BASIS, normalize_statement_basis
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
    "screener_sync_result",
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

    def __init__(
        self,
        db_path: str | Path | None = None,
        *,
        initialize: bool = True,
        valuation_migration_backup_dir: str | Path | None = None,
    ):
        self.db_path = Path(db_path) if db_path is not None else default_screener_db_path()
        if initialize:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            if self.requires_statement_basis_migration():
                if valuation_migration_backup_dir is None:
                    raise RuntimeError(
                        "Screener financial or valuation tables require a basis-key migration; rerun with an "
                        "explicit statement-basis migration backup directory"
                    )
                self.backup_for_statement_basis_migration(valuation_migration_backup_dir)
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
                    statement_basis TEXT NOT NULL DEFAULT 'standalone',
                    price REAL,
                    market_cap_cr REAL,
                    pe REAL,
                    pb REAL,
                    ev_ebitda REAL,
                    dividend_yield REAL,
                    source TEXT NOT NULL DEFAULT 'screener',
                    sync_batch_id TEXT,
                    synced_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (symbol, date, statement_basis, source)
                )
                """
            )
            _migrate_valuation_basis_table(conn)
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
            for metric_id, info in PREDEFINED_METRICS.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO screener_metric_catalog (
                        metric_id, metric_name, category, statement_type, unit, scale, higher_is_better
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (metric_id, *info),
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
            _migrate_financial_basis_table(conn)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS screener_sync_batch (
                    sync_batch_id TEXT PRIMARY KEY,
                    started_at TIMESTAMP NOT NULL,
                    finished_at TIMESTAMP,
                    status TEXT NOT NULL,
                    symbols_total INTEGER DEFAULT 0,
                    symbols_succeeded INTEGER DEFAULT 0,
                    symbols_skipped INTEGER DEFAULT 0,
                    symbols_failed INTEGER DEFAULT 0,
                    exports_dir TEXT,
                    force INTEGER DEFAULT 0,
                    missing_current_results INTEGER DEFAULT 0,
                    expected_report_date DATE,
                    retry_cooldown_hours REAL
                )
                """
            )
            _ensure_sqlite_column(conn, "screener_sync_batch", "symbols_skipped", "INTEGER DEFAULT 0")
            _ensure_sqlite_column(conn, "screener_sync_batch", "missing_current_results", "INTEGER DEFAULT 0")
            _ensure_sqlite_column(conn, "screener_sync_batch", "expected_report_date", "DATE")
            _ensure_sqlite_column(conn, "screener_sync_batch", "retry_cooldown_hours", "REAL")
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
                CREATE TABLE IF NOT EXISTS screener_sync_result (
                    sync_batch_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    requested_basis TEXT NOT NULL,
                    detected_basis TEXT,
                    export_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (sync_batch_id, symbol),
                    FOREIGN KEY (sync_batch_id) REFERENCES screener_sync_batch(sync_batch_id)
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
            conn.commit()

    def requires_statement_basis_migration(self) -> bool:
        if not self.db_path.exists() or self.db_path.stat().st_size == 0:
            return False
        conn = sqlite3.connect(self.db_path)
        try:
            return any(
                _table_primary_key_missing_basis(conn, table_name)
                for table_name in ("screener_financials", "screener_market_valuation")
            )
        finally:
            conn.close()

    def requires_valuation_basis_migration(self) -> bool:
        """Backward-compatible alias for callers checking the legacy migration gate."""

        return self.requires_statement_basis_migration()

    def backup_for_statement_basis_migration(self, backup_dir: str | Path) -> Path:
        resolved_dir = Path(backup_dir).expanduser().resolve()
        resolved_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = resolved_dir / f"{self.db_path.stem}.pre_statement_basis.{timestamp}{self.db_path.suffix}"
        if backup_path.exists():
            raise FileExistsError(f"Refusing to overwrite migration backup: {backup_path}")
        source = sqlite3.connect(self.db_path)
        destination = sqlite3.connect(backup_path)
        try:
            source.backup(destination)
            integrity = destination.execute("PRAGMA integrity_check").fetchone()
        finally:
            destination.close()
            source.close()
        if not integrity or str(integrity[0]).lower() != "ok":
            backup_path.unlink(missing_ok=True)
            raise RuntimeError("Screener valuation migration backup integrity verification failed")
        checksum = _sha256_file(backup_path)
        backup_path.with_suffix(f"{backup_path.suffix}.sha256").write_text(
            f"{checksum}  {backup_path.name}\n",
            encoding="utf-8",
        )
        return backup_path

    def backup_for_valuation_migration(self, backup_dir: str | Path) -> Path:
        """Backward-compatible alias for the expanded statement-basis migration backup."""

        return self.backup_for_statement_basis_migration(backup_dir)

    def begin_batch(
        self,
        sync_batch_id: str,
        *,
        symbols_total: int,
        exports_dir: Path,
        force: bool,
        missing_current_results: bool = False,
        expected_report_date: str | None = None,
        retry_cooldown_hours: float | None = None,
    ) -> None:
        now = _utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO screener_sync_batch (
                    sync_batch_id, started_at, status, symbols_total, symbols_succeeded,
                    symbols_skipped, symbols_failed, exports_dir, force,
                    missing_current_results, expected_report_date, retry_cooldown_hours
                ) VALUES (?, ?, 'running', ?, 0, 0, 0, ?, ?, ?, ?, ?)
                """,
                (
                    sync_batch_id,
                    now,
                    int(symbols_total),
                    str(exports_dir),
                    int(force),
                    int(missing_current_results),
                    expected_report_date,
                    retry_cooldown_hours,
                ),
            )
            conn.commit()

    def finish_batch(self, sync_batch_id: str, *, succeeded: int, failed: int, skipped: int = 0) -> None:
        status = "completed" if failed == 0 else "completed_with_errors"
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE screener_sync_batch
                SET finished_at = ?, status = ?, symbols_succeeded = ?, symbols_skipped = ?, symbols_failed = ?
                WHERE sync_batch_id = ?
                """,
                (_utc_now(), status, int(succeeded), int(skipped), int(failed), sync_batch_id),
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

    def record_sync_result(
        self,
        sync_batch_id: str,
        symbol: str,
        *,
        requested_basis: str,
        detected_basis: str | None,
        export_path: str | Path,
        status: str = "succeeded",
    ) -> None:
        requested = normalize_statement_basis(requested_basis)
        detected = normalize_statement_basis(detected_basis) if detected_basis is not None else None
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO screener_sync_result (
                    sync_batch_id, symbol, requested_basis, detected_basis, export_path, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sync_batch_id,
                    symbol.upper().strip(),
                    requested,
                    detected,
                    str(export_path),
                    str(status).strip().lower(),
                    _utc_now(),
                ),
            )
            conn.commit()

    def get_synced_symbols(self, *, requested_basis: str = DEFAULT_STATEMENT_BASIS) -> set[str]:
        basis = normalize_statement_basis(requested_basis)
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT symbol
                FROM screener_sync_result
                WHERE requested_basis = ? AND status = 'succeeded'
                """,
                (basis,),
            ).fetchall()
            symbols = {str(row["symbol"]).upper() for row in rows}
            if basis == DEFAULT_STATEMENT_BASIS:
                legacy_rows = conn.execute("SELECT DISTINCT symbol FROM screener_company_snapshot").fetchall()
                symbols.update(str(row["symbol"]).upper() for row in legacy_rows)
        return symbols

    def save_company_financials(
        self,
        symbol: str,
        data: dict[str, Any],
        *,
        statement_basis: str = DEFAULT_STATEMENT_BASIS,
        sync_batch_id: str | None = None,
        as_of_date: str | None = None,
    ) -> None:
        symbol = symbol.upper().strip()
        basis = normalize_statement_basis(statement_basis)
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
            valuation_rows = self._compute_market_valuations(symbol, data, basis, synced_at, sync_batch_id)
            if valuation_rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO screener_market_valuation (
                        symbol, date, statement_basis, price, market_cap_cr, pe, pb, ev_ebitda,
                        dividend_yield, source, sync_batch_id, synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    valuation_rows,
                )
            financial_rows = self._financial_rows(conn, symbol, data, basis, synced_at, sync_batch_id)
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

    def get_company_data(
        self,
        symbol: str,
        *,
        statement_basis: str = DEFAULT_STATEMENT_BASIS,
    ) -> dict[str, Any] | None:
        symbol = symbol.upper().strip()
        basis = normalize_statement_basis(statement_basis)
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
                WHERE f.symbol = ? AND f.statement_basis = ?
                """,
                (symbol, basis),
            ).fetchall()
            prices = conn.execute(
                """
                SELECT date, price FROM screener_market_valuation
                WHERE symbol = ? AND statement_basis = ? ORDER BY date
                """,
                (symbol, basis),
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
        statement_basis: str,
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
                            statement_basis,
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
        statement_basis: str,
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
            rows.append(
                (
                    symbol,
                    str(date_str)[:10],
                    statement_basis,
                    p,
                    mcap,
                    pe,
                    pb,
                    ev_ebitda,
                    dividend_yield,
                    "screener",
                    sync_batch_id,
                    synced_at,
                )
            )
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


def _migrate_valuation_basis_table(conn: sqlite3.Connection) -> None:
    columns = conn.execute("PRAGMA table_info(screener_market_valuation)").fetchall()
    primary_key = [str(row[1]) for row in sorted(columns, key=lambda row: int(row[5])) if int(row[5]) > 0]
    if "statement_basis" in primary_key:
        return
    before_count = int(conn.execute("SELECT COUNT(*) FROM screener_market_valuation").fetchone()[0])
    conn.execute(
        """
        CREATE TABLE screener_market_valuation_basis_new (
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            statement_basis TEXT NOT NULL DEFAULT 'standalone',
            price REAL,
            market_cap_cr REAL,
            pe REAL,
            pb REAL,
            ev_ebitda REAL,
            dividend_yield REAL,
            source TEXT NOT NULL DEFAULT 'screener',
            sync_batch_id TEXT,
            synced_at TIMESTAMP NOT NULL,
            PRIMARY KEY (symbol, date, statement_basis, source)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO screener_market_valuation_basis_new (
            symbol, date, statement_basis, price, market_cap_cr, pe, pb, ev_ebitda,
            dividend_yield, source, sync_batch_id, synced_at
        )
        SELECT symbol, date, 'standalone', price, market_cap_cr, pe, pb, ev_ebitda,
               dividend_yield, source, sync_batch_id, synced_at
        FROM screener_market_valuation
        """
    )
    after_count = int(conn.execute("SELECT COUNT(*) FROM screener_market_valuation_basis_new").fetchone()[0])
    if after_count != before_count:
        raise RuntimeError(
            f"Screener valuation migration row-count mismatch: before={before_count} after={after_count}"
        )
    conn.execute("DROP TABLE screener_market_valuation")
    conn.execute("ALTER TABLE screener_market_valuation_basis_new RENAME TO screener_market_valuation")


def _migrate_financial_basis_table(conn: sqlite3.Connection) -> None:
    if not _table_primary_key_missing_basis(conn, "screener_financials"):
        return
    before_count = int(conn.execute("SELECT COUNT(*) FROM screener_financials").fetchone()[0])
    conn.execute(
        """
        CREATE TABLE screener_financials_basis_new (
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
    conn.execute(
        """
        INSERT INTO screener_financials_basis_new (
            symbol, period_type, report_date, statement_basis, metric_id, value,
            available_at, source, sync_batch_id, synced_at
        )
        SELECT symbol, period_type, report_date,
               coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone'),
               metric_id, value, available_at, source, sync_batch_id, synced_at
        FROM screener_financials
        """
    )
    after_count = int(conn.execute("SELECT COUNT(*) FROM screener_financials_basis_new").fetchone()[0])
    if after_count != before_count:
        raise RuntimeError(
            f"Screener financial migration row-count mismatch: before={before_count} after={after_count}"
        )
    conn.execute("DROP TABLE screener_financials")
    conn.execute("ALTER TABLE screener_financials_basis_new RENAME TO screener_financials")


def _table_primary_key_missing_basis(conn: sqlite3.Connection, table_name: str) -> bool:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    if exists is None:
        return False
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    primary_key = [str(row[1]) for row in sorted(columns, key=lambda row: int(row[5])) if int(row[5]) > 0]
    return "statement_basis" not in primary_key


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
