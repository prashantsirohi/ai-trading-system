#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CSV to SQLite join: create stock_details table from CSV by symbol lookup.

This module exposes a function build_table(csv_path, db_path, table_name='stock_details')
that reads the CSV with header:
  Name,Symbol,Industry Group,Industry,MCAP
and creates a new table in the SQLite DB containing:
  Security_id (from symbols.security_id),
  Name, Symbol, Industry Group, Industry, MCAP
Rows are inserted only if the CSV Symbol can be found in the symbols table
(via nse_symbol or symbol_id column).
"""

from __future__ import annotations

import csv
import sqlite3
import os
from typing import Optional


def build_table(
    csv_path: str,
    db_path: str,
    table_name: str = "stock_details",
    verbose: bool = False,
) -> tuple[int, list]:
    """Read CSV and populate a new table with mapped Security_id.

    Returns (inserted_count, unmatched_symbols).
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"DB file not found: {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
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
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_symbols_nse ON symbols(nse_symbol)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_symbols_exchange ON symbols(exchange)")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        "Security_id" TEXT,
        "Name" TEXT,
        "Symbol" TEXT,
        "Industry Group" TEXT,
        "Industry" TEXT,
        "MCAP" REAL
    )
    """.strip()
    cur.execute(create_sql)
    cur.execute(f'DELETE FROM "{table_name}"')

    insert_sql = f"""
    INSERT INTO "{table_name}" (
        "Security_id",
        "Name",
        "Symbol",
        "Industry Group",
        "Industry",
        "MCAP"
    ) VALUES (?,?,?,?,?,?)
    """.strip()

    required_cols = ["Name", "Symbol", "Industry Group", "Industry", "MCAP"]
    inserted = 0
    unmatched = []

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or any(
            col not in reader.fieldnames for col in required_cols
        ):
            raise ValueError(f"CSV must contain headers: {', '.join(required_cols)}")

        for row in reader:
            symbol = row.get("Symbol")
            name = row.get("Name")
            industry_group = row.get("Industry Group")
            industry = row.get("Industry")
            mcap_raw = row.get("MCAP")

            if symbol is None:
                continue

            mcap = None
            if mcap_raw is not None and mcap_raw.strip():
                try:
                    mcap = float(mcap_raw.strip())
                except ValueError:
                    pass

            cur.execute(
                'SELECT "security_id" FROM symbols WHERE "nse_symbol" = ? LIMIT 1',
                (symbol,),
            )
            res = cur.fetchone()
            if not res:
                cur.execute(
                    'SELECT "security_id" FROM symbols WHERE "symbol_id" = ? LIMIT 1',
                    (symbol,),
                )
                res = cur.fetchone()
            if not res:
                unmatched.append((symbol, name))
                if verbose:
                    print(f"  UNMATCHED: {symbol} - {name}")
                continue

            security_id = res[0]
            cur.execute(
                insert_sql,
                (security_id, name, symbol, industry_group, industry, mcap),
            )
            inserted += 1

    conn.commit()
    conn.close()
    return inserted, unmatched


def main(argv: Optional[list] = None) -> tuple[int, list]:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to input CSV")
    parser.add_argument("--db", required=True, help="Path to SQLite masterdata DB")
    parser.add_argument("--table", default="stock_details", help="Output table name")
    parser.add_argument(
        "--verbose", action="store_true", help="Print unmatched symbols"
    )
    args = parser.parse_args(argv)
    return build_table(args.csv, args.db, args.table, verbose=args.verbose)


if __name__ == "__main__":
    inserted, unmatched = main()
    print(f"Inserted {inserted} rows")
    if unmatched:
        print(f"\nUnmatched ({len(unmatched)}):")
        for sym, name in unmatched:
            print(f"  {sym} - {name}")
