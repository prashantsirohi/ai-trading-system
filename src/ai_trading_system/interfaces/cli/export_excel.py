"""Export master symbol metadata to Excel."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from ai_trading_system.platform.db.paths import get_domain_paths


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export masterdata symbols to Excel")
    default_paths = get_domain_paths(Path(__file__).resolve().parents[4], "operational")
    parser.add_argument(
        "--master-db",
        default=str(default_paths.master_db_path),
        help="Path to masterdata SQLite database",
    )
    parser.add_argument(
        "--output",
        default=str(default_paths.data_root / "masterdata.xlsx"),
        help="Path to output Excel workbook",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    master_db = Path(args.master_db)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(master_db.as_posix())
    try:
        query = """
            SELECT
                Symbol AS symbol_id,
                Company_Name AS symbol_name,
                exchange,
                Sector AS sector,
                "Industry Group" AS industry
            FROM stock_details
            WHERE Symbol IS NOT NULL
            ORDER BY Sector, Symbol
        """
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    df.to_excel(output, index=False, engine="openpyxl")
    print(f"Exported {len(df)} symbols to {output}")


if __name__ == "__main__":
    main()
