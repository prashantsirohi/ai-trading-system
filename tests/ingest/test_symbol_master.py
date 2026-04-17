from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from core.symbol_master import SymbolMaster


def test_symbol_master_canonicalize_and_active_filter() -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "ABC",
                "canonical_symbol": "ABC",
                "isin": "INE000A01010",
                "status": "active",
            },
            {
                "symbol": "XYZOLD",
                "canonical_symbol": "XYZ",
                "isin": "INE000X01010",
                "status": "active",
            },
            {
                "symbol": "DELISTED",
                "canonical_symbol": "DELISTED",
                "status": "delisted",
            },
        ]
    )
    master = SymbolMaster(frame)
    assert master.canonicalize("xyzold") == "XYZ"
    assert master.isin_for("XYZOLD") == "INE000X01010"
    assert master.is_active("DELISTED") is False
    assert master.filter_active(["ABC", "DELISTED"]) == ["ABC"]


def test_symbol_master_from_masterdb_handles_missing_optional_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "masterdata.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE stock_details (
                Security_id TEXT,
                Symbol TEXT,
                exchange TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO stock_details (Security_id, Symbol, exchange)
            VALUES ('101', 'ABC', 'NSE')
            """
        )
        conn.commit()
    finally:
        conn.close()

    master = SymbolMaster.from_masterdb(db_path)
    assert master.canonicalize("ABC") == "ABC"
    assert master.is_active("ABC") is True
