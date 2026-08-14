from __future__ import annotations

import json
import sqlite3
from datetime import date

from ai_trading_system.domains.research_screener.issuer_classification import (
    ExistingIssuerClassificationProvider,
    route_company_type,
)


def test_company_type_routes_from_explicit_sector_and_industry():
    assert route_company_type("Banks", "Private Sector Bank") == "BANK"
    assert route_company_type("Finance", "Non Banking Financial Company (NBFC)") == "FINANCIAL_INSTITUTION"
    assert route_company_type("Insurance", "General Insurance") == "FINANCIAL_INSTITUTION"
    assert route_company_type("Capital Markets", "Stockbroking & Allied") == "FINANCIAL_INSTITUTION"
    assert route_company_type("Capital Markets", "Exchange and Data Platform") == "MARKET_INFRASTRUCTURE"
    assert route_company_type("Capital Markets", "Depositories, Clearing Houses and Other Intermediaries") == "MARKET_INFRASTRUCTURE"
    assert route_company_type("Industrial Products", "Packaging") == "INDUSTRIAL"
    assert route_company_type("Banks", None) == "UNCLASSIFIED"


def test_snapshot_requires_one_exact_isin_and_freezes_evidence(tmp_path):
    database = tmp_path / "masterdata.db"
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE symbols (
               isin TEXT, symbol_id TEXT, symbol_name TEXT, sector TEXT,
               industry TEXT, last_updated TEXT
           )"""
    )
    connection.executemany(
        "INSERT INTO symbols VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("INEBANK00001", "TESTBANK", "Test Bank", "Banks", "Private Sector Bank", "2026-08-11T10:00:00"),
            ("INEDUPE00001", "DUPE1", "Duplicate One", "Finance", "Investment Company", "2026-08-11T10:00:00"),
            ("INEDUPE00001", "DUPE2", "Duplicate Two", "Finance", "Investment Company", "2026-08-11T10:00:00"),
            ("INEFUTURE001", "FUTURE", "Future", "Banks", "Private Sector Bank", "2026-08-13T10:00:00"),
        ],
    )
    connection.commit()
    connection.close()
    cohort = [
        {"isin": "INEBANK00001", "symbol": "TESTBANK"},
        {"isin": "INEDUPE00001", "symbol": "DUPE1"},
        {"isin": "INEFUTURE001", "symbol": "FUTURE"},
        {"isin": "INEMISSING01", "symbol": "MISSING"},
    ]

    snapshot, raw = ExistingIssuerClassificationProvider(database).snapshot(
        cohort, date(2026, 8, 12),
    )

    assert snapshot["INEBANK00001"]["company_type"] == "BANK"
    assert snapshot["INEBANK00001"]["source_row_hash"]
    assert snapshot["INEDUPE00001"]["reason"] == "EXACT_ISIN_AMBIGUOUS"
    assert snapshot["INEFUTURE001"]["reason"] == "CLASSIFICATION_OBSERVED_AFTER_CUTOFF"
    assert snapshot["INEMISSING01"]["reason"] == "EXACT_ISIN_NOT_MASTERED"
    frozen = json.loads(raw)
    assert frozen["schema_version"] == "issuer-classification-snapshot-v1"
    assert len(frozen["rows"]) == 4
