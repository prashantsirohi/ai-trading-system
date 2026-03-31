import os
import sqlite3
import csv
import importlib
from pathlib import Path

import pytest


build_table_module = importlib.util.find_spec("match_and_create_table")
if build_table_module is None:
    pytestmark = pytest.mark.skip(reason="Legacy match_and_create_table module is not packaged in the current app layout.")
else:
    from match_and_create_table import build_table


def _setup_test_db(db_path, symbols):
    if db_path.exists():
        db_path.unlink()
    conn = sqlite3.connect(db_path.as_posix())
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS symbols (
        symbol_id TEXT PRIMARY KEY,
        security_id TEXT,
        symbol_name TEXT,
        exchange TEXT,
        instrument_type TEXT,
        isin TEXT,
        lot_size INTEGER,
        tick_size REAL,
        freeze_quantity INTEGER,
        sector TEXT,
        industry TEXT,
        nse_symbol TEXT,
        bse_symbol TEXT,
        last_updated TEXT
    )""")
    for sid, sym in symbols:
        cur.execute(
            "INSERT INTO symbols (symbol_id, security_id, symbol_name, exchange, nse_symbol) VALUES (?,?,?,?,?)",
            (sym, sid, sym, "NSE", sym),
        )
    conn.commit()
    conn.close()


def _write_csv(csv_path, rows):
    # Write CSV with header matching expected in implementation
    header = ["Name", "Symbol", "Industry Group", "Industry", "MCAP"]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            if isinstance(r, dict):
                writer.writerow(r)
            else:
                # tuple: (name,symbol,industry_group,industry,mcap)
                name, symbol, ig, ind, mcap = r
                writer.writerow(
                    {
                        "Name": name,
                        "Symbol": symbol,
                        "Industry Group": ig,
                        "Industry": ind,
                        "MCAP": mcap,
                    }
                )


def test_build_table_populates_from_csv(tmp_path: Path):
    # Arrange
    db_path = tmp_path / "masterdata.db"
    csv_path = tmp_path / "all-stock-non-sme.csv"
    # Seed symbols: ABC -> id 1, DEF -> id 2
    _setup_test_db(db_path, [(1, "ABC"), (2, "DEF")])
    _write_csv(
        csv_path,
        [
            ("ABC Company", "ABC", "Technology", "Software", 100.5),
            ("DEF Company", "DEF", "Technology", "Hardware", 200.0),
        ],
    )
    # Act
    inserted, unmatched = build_table(
        str(csv_path), str(db_path), table_name="stock_details"
    )

    # Assert
    conn = sqlite3.connect(db_path.as_posix())
    cur = conn.cursor()
    cur.execute(
        'SELECT "Security_id", "Name", "Symbol", "Industry Group", "Industry", "MCAP" FROM "stock_details" ORDER BY "Security_id"'
    )
    rows = cur.fetchall()
    conn.close()

    assert inserted == 2
    assert unmatched == []
    assert rows == [
        ("1", "ABC Company", "ABC", "Technology", "Software", 100.5),
        ("2", "DEF Company", "DEF", "Technology", "Hardware", 200.0),
    ]
