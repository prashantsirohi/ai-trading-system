"""Validate Screener Excel exports against the mirrored fundamentals DB."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.contracts import DEFAULT_STATEMENT_BASIS
from ai_trading_system.domains.fundamentals.screener_client import ScreenerClient
from ai_trading_system.platform.db.paths import get_domain_paths


METRIC_MAP = {
    "sales": "Sales",
    "expenses": "Expenses",
    "operating_profit": "Operating Profit",
    "net_profit": "Net profit",
}


@dataclass(frozen=True)
class ScreenerExportValidationResult:
    checked_symbols: int
    checked_cells: int
    statement_basis: str
    mismatches: pd.DataFrame


def validate_screener_exports_against_duckdb(
    *,
    fundamentals_db_path: str | Path | None = None,
    exports_dir: str | Path | None = None,
    report_date: str | None = None,
    statement_basis: str = DEFAULT_STATEMENT_BASIS,
    symbols: list[str] | None = None,
    tolerance: float = 0.01,
) -> ScreenerExportValidationResult:
    """Compare quarterly metrics in local Screener exports with DuckDB mirror rows."""

    paths = get_domain_paths()
    db_path = Path(fundamentals_db_path) if fundamentals_db_path is not None else paths.root_dir / "fundamentals.duckdb"
    resolved_exports = Path(exports_dir) if exports_dir is not None else paths.fundamentals_dir / "exports"
    if not db_path.exists():
        raise FileNotFoundError(f"fundamentals DuckDB not found: {db_path}")
    if not resolved_exports.exists():
        raise FileNotFoundError(f"Screener exports dir not found: {resolved_exports}")

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        target_report_date = str(report_date or _latest_quarterly_report_date(conn))[:10]
        resolved_basis = _normalize_statement_basis(statement_basis)
        db_frame = _load_db_quarter(conn, target_report_date, statement_basis=resolved_basis, symbols=symbols)
    finally:
        conn.close()

    if db_frame.empty:
        return ScreenerExportValidationResult(
            checked_symbols=0,
            checked_cells=0,
            statement_basis=resolved_basis,
            mismatches=_mismatch_frame([]),
        )

    client = ScreenerClient(exports_dir=resolved_exports)
    rows: list[dict[str, Any]] = []
    checked_cells = 0
    checked_symbols = 0
    for symbol, db_rows in db_frame.groupby("symbol", sort=True):
        export_path = resolved_exports / f"{symbol}_screener.xlsx"
        if not export_path.exists():
            rows.append(
                {
                    "symbol": symbol,
                    "report_date": target_report_date,
                    "metric_id": "",
                    "statement_basis": resolved_basis,
                    "db_value": None,
                    "export_value": None,
                    "delta": None,
                    "status": "missing_export",
                    "export_path": str(export_path),
                }
            )
            continue
        checked_symbols += 1
        data = client.parse_excel(export_path)
        quarters = data.get("quarters", {})
        for db_row in db_rows.to_dict(orient="records"):
            metric_id = str(db_row["metric_id"])
            export_metric = METRIC_MAP.get(metric_id)
            if export_metric is None:
                continue
            checked_cells += 1
            db_value = _float(db_row["value"])
            export_value = _float((quarters.get(export_metric) or {}).get(target_report_date))
            delta = None if db_value is None or export_value is None else db_value - export_value
            if export_value is None:
                status = "missing_export_metric"
            elif db_value is None:
                status = "missing_db_metric"
            elif abs(delta or 0.0) > float(tolerance):
                status = "mismatch"
            else:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "report_date": target_report_date,
                    "metric_id": metric_id,
                    "statement_basis": resolved_basis,
                    "db_value": db_value,
                    "export_value": export_value,
                    "delta": delta,
                    "status": status,
                    "export_path": str(export_path),
                }
            )
    return ScreenerExportValidationResult(
        checked_symbols=checked_symbols,
        checked_cells=checked_cells,
        statement_basis=resolved_basis,
        mismatches=_mismatch_frame(rows),
    )


def _latest_quarterly_report_date(conn: duckdb.DuckDBPyConnection) -> str:
    row = conn.execute(
        "SELECT MAX(report_date) FROM screener_financials WHERE lower(trim(period_type)) = 'quarterly'"
    ).fetchone()
    if not row or row[0] is None:
        raise RuntimeError("No quarterly Screener rows found in fundamentals DuckDB")
    return str(row[0])[:10]


def _load_db_quarter(
    conn: duckdb.DuckDBPyConnection,
    report_date: str,
    *,
    statement_basis: str,
    symbols: list[str] | None,
) -> pd.DataFrame:
    params: list[Any] = [str(report_date)[:10]]
    basis_expr = (
        "coalesce(nullif(lower(trim(statement_basis)), ''), 'standalone')"
        if _has_column(conn, "screener_financials", "statement_basis")
        else "'standalone'"
    )
    basis_filter = f" AND {basis_expr} = ?"
    params.append(_normalize_statement_basis(statement_basis))
    symbol_filter = ""
    if symbols:
        clean_symbols = [str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()]
        if clean_symbols:
            symbol_filter = " AND upper(trim(symbol)) IN (SELECT * FROM unnest(?))"
            params.append(clean_symbols)
    return conn.execute(
        f"""
        SELECT upper(trim(symbol)) AS symbol, lower(trim(metric_id)) AS metric_id, value
        FROM screener_financials
        WHERE lower(trim(period_type)) = 'quarterly'
          AND report_date = CAST(? AS DATE)
          AND lower(trim(metric_id)) IN ('sales', 'expenses', 'operating_profit', 'net_profit')
          {basis_filter}
          {symbol_filter}
        ORDER BY symbol, metric_id
        """,
        params,
    ).df()


def _mismatch_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=[
            "symbol",
            "report_date",
            "metric_id",
            "statement_basis",
            "db_value",
            "export_value",
            "delta",
            "status",
            "export_path",
        ],
    )


def _float(value: object) -> float | None:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_column(conn: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = ?
            """,
            [table_name, column_name],
        ).fetchone()[0]
    )


def _normalize_statement_basis(value: object) -> str:
    basis = str(value or DEFAULT_STATEMENT_BASIS).strip().lower()
    return basis or DEFAULT_STATEMENT_BASIS


def build_parser() -> argparse.ArgumentParser:
    paths = get_domain_paths()
    parser = argparse.ArgumentParser(description="Validate local Screener exports against fundamentals.duckdb.")
    parser.add_argument("--fundamentals-db-path", default=str(paths.root_dir / "fundamentals.duckdb"))
    parser.add_argument("--exports-dir", default=str(paths.fundamentals_dir / "exports"))
    parser.add_argument("--report-date", default=None, help="Quarter report date; defaults to latest quarterly DB date.")
    parser.add_argument("--statement-basis", default=DEFAULT_STATEMENT_BASIS, help="Statement basis to validate.")
    parser.add_argument("--symbol", action="append", dest="symbols", help="Limit to one symbol; repeatable.")
    parser.add_argument("--tolerance", type=float, default=0.01)
    parser.add_argument("--output-csv", default=None, help="Write mismatch details to this CSV path.")
    parser.add_argument("--symbols-output", default=None, help="Write distinct mismatch symbols to this text file.")
    parser.add_argument("--fail-on-mismatch", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    result = validate_screener_exports_against_duckdb(
        fundamentals_db_path=args.fundamentals_db_path,
        exports_dir=args.exports_dir,
        report_date=args.report_date,
        statement_basis=args.statement_basis,
        symbols=args.symbols,
        tolerance=args.tolerance,
    )
    print(
        f"checked_symbols={result.checked_symbols} checked_cells={result.checked_cells} "
        f"statement_basis={result.statement_basis}"
    )
    print(f"mismatches={len(result.mismatches)}")
    if not result.mismatches.empty:
        print(result.mismatches.to_string(index=False))
    if args.output_csv:
        output_csv = Path(args.output_csv)
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        result.mismatches.to_csv(output_csv, index=False)
        print(f"wrote_mismatches={output_csv}")
    if args.symbols_output:
        symbols_output = Path(args.symbols_output)
        symbols_output.parent.mkdir(parents=True, exist_ok=True)
        symbols = (
            result.mismatches.loc[result.mismatches["symbol"].notna(), "symbol"]
            .astype(str)
            .str.upper()
            .str.strip()
            .drop_duplicates()
            .sort_values()
            .tolist()
        )
        symbols_output.write_text("\n".join(symbols) + ("\n" if symbols else ""), encoding="utf-8")
        print(f"wrote_mismatch_symbols={symbols_output} count={len(symbols)}")
    if args.fail_on_mismatch and not result.mismatches.empty:
        raise SystemExit(1)


__all__ = [
    "METRIC_MAP",
    "ScreenerExportValidationResult",
    "build_parser",
    "main",
    "validate_screener_exports_against_duckdb",
]


if __name__ == "__main__":
    main()
