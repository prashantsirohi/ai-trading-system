from __future__ import annotations

from pathlib import Path

import duckdb
from openpyxl import Workbook

from ai_trading_system.domains.fundamentals import screener_export_validation
from ai_trading_system.domains.fundamentals.screener_export_validation import (
    validate_screener_exports_against_duckdb,
)


def _write_export(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Data Sheet"
    rows = {
        40: ["Quarters"],
        41: ["Report Date", None, None, "2025-03-31", "2026-03-31"],
        42: ["Sales", None, None, 156.09, 185.57],
        43: ["Expenses", None, None, 80.77, 138.41],
        49: ["Net profit", None, None, 49.23, 43.96],
        50: ["Operating Profit", None, None, 75.32, 47.16],
    }
    for row_idx, values in rows.items():
        for col_idx, value in enumerate(values, start=1):
            ws.cell(row_idx, col_idx, value)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def _create_db(path: Path, *, sales: float) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE screener_financials (
                symbol VARCHAR,
                period_type VARCHAR,
                report_date DATE,
                metric_id VARCHAR,
                value DOUBLE
            )
            """
        )
        conn.executemany(
            "INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?)",
            [
                ("AAA", "quarterly", "2026-03-31", "sales", sales),
                ("AAA", "quarterly", "2026-03-31", "expenses", 138.41),
                ("AAA", "quarterly", "2026-03-31", "operating_profit", 47.16),
                ("AAA", "quarterly", "2026-03-31", "net_profit", 43.96),
            ],
        )
    finally:
        conn.close()


def _create_basis_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE screener_financials (
                symbol VARCHAR,
                period_type VARCHAR,
                report_date DATE,
                statement_basis VARCHAR,
                metric_id VARCHAR,
                value DOUBLE
            )
            """
        )
        rows = [
            ("AAA", "quarterly", "2026-03-31", "standalone", "sales", 185.57),
            ("AAA", "quarterly", "2026-03-31", "standalone", "expenses", 138.41),
            ("AAA", "quarterly", "2026-03-31", "standalone", "operating_profit", 47.16),
            ("AAA", "quarterly", "2026-03-31", "standalone", "net_profit", 43.96),
            ("AAA", "quarterly", "2026-03-31", "consolidated", "sales", 999.0),
            ("AAA", "quarterly", "2026-03-31", "consolidated", "expenses", 999.0),
            ("AAA", "quarterly", "2026-03-31", "consolidated", "operating_profit", 999.0),
            ("AAA", "quarterly", "2026-03-31", "consolidated", "net_profit", 999.0),
        ]
        conn.executemany("INSERT INTO screener_financials VALUES (?, ?, ?, ?, ?, ?)", rows)
    finally:
        conn.close()


def test_validate_screener_exports_against_duckdb_detects_shifted_quarter_values(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.duckdb"
    exports_dir = tmp_path / "exports"
    _create_db(db_path, sales=156.09)
    _write_export(exports_dir / "AAA_screener.xlsx")

    result = validate_screener_exports_against_duckdb(
        fundamentals_db_path=db_path,
        exports_dir=exports_dir,
        report_date="2026-03-31",
    )

    assert result.checked_symbols == 1
    assert result.checked_cells == 4
    mismatch = result.mismatches.set_index(["symbol", "metric_id"]).loc[("AAA", "sales")]
    assert mismatch["db_value"] == 156.09
    assert mismatch["export_value"] == 185.57
    assert mismatch["status"] == "mismatch"


def test_validate_screener_exports_against_duckdb_passes_when_export_matches_db(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.duckdb"
    exports_dir = tmp_path / "exports"
    _create_db(db_path, sales=185.57)
    _write_export(exports_dir / "AAA_screener.xlsx")

    result = validate_screener_exports_against_duckdb(
        fundamentals_db_path=db_path,
        exports_dir=exports_dir,
        report_date="2026-03-31",
    )

    assert result.checked_symbols == 1
    assert result.checked_cells == 4
    assert result.statement_basis == "standalone"
    assert result.mismatches.empty


def test_validate_screener_exports_against_duckdb_compares_selected_basis(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.duckdb"
    exports_dir = tmp_path / "exports"
    _create_basis_db(db_path)
    _write_export(exports_dir / "AAA_screener.xlsx")

    standalone = validate_screener_exports_against_duckdb(
        fundamentals_db_path=db_path,
        exports_dir=exports_dir,
        report_date="2026-03-31",
        statement_basis="standalone",
    )
    consolidated = validate_screener_exports_against_duckdb(
        fundamentals_db_path=db_path,
        exports_dir=exports_dir,
        report_date="2026-03-31",
        statement_basis="consolidated",
    )

    assert standalone.mismatches.empty
    assert len(consolidated.mismatches) == 4
    assert set(consolidated.mismatches["statement_basis"]) == {"consolidated"}


def test_validation_cli_writes_mismatch_and_symbol_outputs(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "fundamentals.duckdb"
    exports_dir = tmp_path / "exports"
    mismatch_csv = tmp_path / "mismatches.csv"
    symbols_txt = tmp_path / "symbols.txt"
    _create_db(db_path, sales=156.09)
    _write_export(exports_dir / "AAA_screener.xlsx")

    monkeypatch.setattr(
        "sys.argv",
        [
            "screener_export_validation",
            "--fundamentals-db-path",
            str(db_path),
            "--exports-dir",
            str(exports_dir),
            "--report-date",
            "2026-03-31",
            "--output-csv",
            str(mismatch_csv),
            "--symbols-output",
            str(symbols_txt),
        ],
    )

    screener_export_validation.main()

    assert mismatch_csv.exists()
    assert symbols_txt.read_text(encoding="utf-8").splitlines() == ["AAA"]
