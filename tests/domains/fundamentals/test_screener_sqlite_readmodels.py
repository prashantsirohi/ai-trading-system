from __future__ import annotations

import copy
import sqlite3
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ai_trading_system.domains.fundamentals.analytical_store import (
    ensure_fundamentals_analytical_schema,
    migrate_fundamentals_analytical_basis_keys,
    mirror_screener_financials,
)
from ai_trading_system.domains.fundamentals.screener_readmodels import (
    build_raw_factor_frame,
    build_scores_from_screener_db,
    refresh_fundamental_readmodels,
)
from ai_trading_system.domains.fundamentals.screener_store import ScreenerFinancialsStore


def _company_data() -> dict:
    return {
        "metadata": {"face_value": 10, "market_cap_cr": 1200},
        "profit_loss": {
            "Sales": {"2023-03-31": 700, "2024-03-31": 850, "2025-03-31": 1000, "2026-03-31": 1250},
            "Operating profit": {"2023-03-31": 150, "2024-03-31": 200, "2025-03-31": 260, "2026-03-31": 340},
            "Net profit": {"2023-03-31": 90, "2024-03-31": 130, "2025-03-31": 180, "2026-03-31": 240},
            "OPM": {"2023-03-31": 21, "2024-03-31": 23, "2025-03-31": 26, "2026-03-31": 27},
        },
        "quarters": {
            "Net profit": {
                "2025-06-30": 42,
                "2025-09-30": 45,
                "2025-12-31": 50,
                "2026-03-31": 55,
                "2026-06-30": 70,
            }
        },
        "balance_sheet": {
            "Equity Share Capital": {"2026-03-31": 100},
            "Reserves": {"2026-03-31": 900},
            "Borrowings": {"2026-03-31": 100},
            "Cash & Bank": {"2026-03-31": 50},
        },
        "cash_flow": {
            "Cash from Operating Activity": {"2026-03-31": 260},
            "Cash from Investing Activity": {"2026-03-31": -80},
        },
        "derived": {
            "Adjusted Equity Shares in Cr": {"2026-03-31": 10},
            "prices": {"2026-03-31": 120},
        },
    }


def test_screener_sqlite_refreshes_score_and_trend_readmodels(tmp_path: Path) -> None:
    db_path = tmp_path / "screener_financials.db"
    latest_output = tmp_path / "fundamental_scores_latest.csv"
    trends_output = tmp_path / "fundamental_trends_latest.csv"
    store = ScreenerFinancialsStore(db_path)
    store.save_company_financials("AAA", _company_data(), as_of_date="2026-05-25")

    scores = refresh_fundamental_readmodels(
        db_path=db_path,
        latest_output=latest_output,
        trends_output=trends_output,
        snapshot_date="2026-05-25",
    )

    assert latest_output.exists()
    assert trends_output.exists()
    assert scores.loc[0, "symbol"] == "AAA"
    assert pd.read_csv(latest_output).loc[0, "screener_snapshot_date"] == "2026-05-25"
    assert {"fundamental_score", "fundamental_tier", "hard_red_flag"}.issubset(scores.columns)
    with store.connect() as conn:
        basis = [row["statement_basis"] for row in conn.execute("SELECT DISTINCT statement_basis FROM screener_financials").fetchall()]
    assert basis == ["standalone"]


def test_screener_duckdb_mirror_preserves_standalone_basis(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "screener_financials.db"
    duckdb_path = tmp_path / "fundamentals.duckdb"
    store = ScreenerFinancialsStore(sqlite_path)
    store.save_company_financials("AAA", _company_data(), as_of_date="2026-05-25")

    rows = mirror_screener_financials(screener_db_path=sqlite_path, fundamentals_db_path=duckdb_path)

    assert rows > 0
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        basis = conn.execute("SELECT DISTINCT statement_basis FROM screener_financials").fetchall()
    finally:
        conn.close()
    assert basis == [("standalone",)]


def test_duckdb_resolved_view_prefers_one_consolidated_basis_per_symbol(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "screener_financials.db"
    duckdb_path = tmp_path / "fundamentals.duckdb"
    store = ScreenerFinancialsStore(sqlite_path)
    standalone = _company_data()
    consolidated = copy.deepcopy(standalone)
    consolidated["quarters"]["Net profit"]["2026-06-30"] = 140
    store.save_company_financials("AAA", standalone, statement_basis="standalone")
    store.save_company_financials("AAA", consolidated, statement_basis="consolidated")
    store.save_company_financials("BBB", standalone, statement_basis="standalone")

    mirror_screener_financials(screener_db_path=sqlite_path, fundamentals_db_path=duckdb_path)

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        resolution = conn.execute(
            "SELECT symbol, statement_basis, basis_resolution_reason FROM screener_statement_basis_resolution ORDER BY symbol"
        ).fetchall()
        resolved_bases = conn.execute(
            "SELECT symbol, list_sort(list_distinct(list(statement_basis))) FROM screener_financials_resolved GROUP BY symbol ORDER BY symbol"
        ).fetchall()
    finally:
        conn.close()
    assert resolution == [
        ("AAA", "consolidated", "consolidated_current"),
        ("BBB", "standalone", "standalone_fallback_no_consolidated_quarterly"),
    ]
    assert resolved_bases == [("AAA", ["consolidated"]), ("BBB", ["standalone"])]


def test_analytical_derived_key_migration_is_backup_gated_and_preserves_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.duckdb"
    backup_path = tmp_path / "fundamentals.backup.duckdb"
    conn = duckdb.connect(str(db_path))
    try:
        ensure_fundamentals_analytical_schema(conn)
        conn.execute("DROP VIEW company_growth_features_resolved")
        for table, legacy_key in (
            ("fundamental_period_facts", "symbol, period_type, report_date"),
            ("company_growth_features", "symbol, report_date"),
        ):
            ddl = conn.execute(
                "SELECT sql FROM duckdb_tables() WHERE table_name = ?",
                [table],
            ).fetchone()[0]
            conn.execute(f"DROP TABLE {table}")
            conn.execute(
                ddl.replace(
                    "PRIMARY KEY(symbol, period_type, report_date, statement_basis)"
                    if table == "fundamental_period_facts"
                    else "PRIMARY KEY(symbol, report_date, statement_basis)",
                    f"PRIMARY KEY({legacy_key})",
                )
            )
        conn.execute(
            """
            INSERT INTO fundamental_period_facts
                (symbol, period_type, report_date, statement_basis, available_at, sales_cr)
            VALUES ('AAA', 'quarterly', DATE '2026-03-31', 'standalone', DATE '2026-05-01', 100)
            """
        )
        conn.execute(
            """
            INSERT INTO company_growth_features
                (symbol, report_date, statement_basis, available_at, sales_cr)
            VALUES ('AAA', DATE '2026-03-31', 'standalone', DATE '2026-05-01', 100)
            """
        )

        with pytest.raises(RuntimeError, match="primary key omits statement_basis"):
            ensure_fundamentals_analytical_schema(conn)
        with pytest.raises(RuntimeError, match="verified DuckDB backup"):
            migrate_fundamentals_analytical_basis_keys(conn, verified_backup_path=backup_path)

        backup_path.write_bytes(b"verified-test-backup")
        counts = migrate_fundamentals_analytical_basis_keys(conn, verified_backup_path=backup_path)
        keys = {
            table: conn.execute(
                """
                SELECT constraint_column_names FROM duckdb_constraints()
                WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'
                """,
                [table],
            ).fetchone()[0]
            for table in counts
        }
        rows = conn.execute(
            "SELECT symbol, statement_basis, sales_cr FROM fundamental_period_facts"
        ).fetchall()
    finally:
        conn.close()

    assert counts == {"fundamental_period_facts": 1, "company_growth_features": 1}
    assert keys == {
        "fundamental_period_facts": ["symbol", "period_type", "report_date", "statement_basis"],
        "company_growth_features": ["symbol", "report_date", "statement_basis"],
    }
    assert rows == [("AAA", "standalone", 100.0)]


def test_resolved_policy_falls_back_from_stale_or_shallow_consolidated_history(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "screener_financials.db"
    duckdb_path = tmp_path / "fundamentals.duckdb"
    store = ScreenerFinancialsStore(sqlite_path)

    stale = copy.deepcopy(_company_data())
    stale["quarters"]["Net profit"].pop("2026-06-30")
    stale["quarters"]["Net profit"]["2025-03-31"] = 35

    shallow = copy.deepcopy(_company_data())
    shallow["profit_loss"] = {
        metric: {"2026-03-31": values["2026-03-31"]}
        for metric, values in shallow["profit_loss"].items()
    }
    shallow["quarters"]["Net profit"] = {"2026-06-30": 140}

    for symbol in ("STALE", "SHALLOW"):
        store.save_company_financials(symbol, _company_data(), statement_basis="standalone")
    store.save_company_financials("STALE", stale, statement_basis="consolidated")
    store.save_company_financials("SHALLOW", shallow, statement_basis="consolidated")

    raw = build_raw_factor_frame(store)
    mirror_screener_financials(screener_db_path=sqlite_path, fundamentals_db_path=duckdb_path)
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        resolution = conn.execute(
            """
            SELECT symbol, statement_basis, basis_resolution_reason
            FROM screener_statement_basis_resolution
            ORDER BY symbol
            """
        ).fetchall()
    finally:
        conn.close()

    assert raw[["symbol", "statement_basis", "basis_resolution_reason"]].to_records(index=False).tolist() == [
        ("SHALLOW", "standalone", "standalone_fallback_insufficient_consolidated_history"),
        ("STALE", "standalone", "standalone_fallback_newer_quarter"),
    ]
    assert resolution == [
        ("SHALLOW", "standalone", "standalone_fallback_insufficient_consolidated_history"),
        ("STALE", "standalone", "standalone_fallback_newer_quarter"),
    ]


def test_missing_screener_db_readmodel_returns_empty_without_creating_db(tmp_path: Path) -> None:
    db_path = tmp_path / "missing" / "screener_financials.db"

    scores = build_scores_from_screener_db(db_path=db_path, snapshot_date="2026-05-25")

    assert scores.empty
    assert not db_path.exists()


def test_schema_less_screener_db_readmodel_returns_empty(tmp_path: Path) -> None:
    db_path = tmp_path / "screener_financials.db"
    db_path.touch()

    scores = build_scores_from_screener_db(db_path=db_path, snapshot_date="2026-05-25")

    assert scores.empty


def test_financial_and_valuation_rows_coexist_by_statement_basis(tmp_path: Path) -> None:
    db_path = tmp_path / "screener_financials.db"
    store = ScreenerFinancialsStore(db_path)
    standalone = _company_data()
    consolidated = copy.deepcopy(standalone)
    consolidated["profit_loss"]["Sales"]["2026-03-31"] = 2500
    consolidated["profit_loss"]["Net profit"]["2026-03-31"] = 300

    store.save_company_financials("AAA", standalone, statement_basis="standalone", as_of_date="2026-05-25")
    store.save_company_financials("AAA", consolidated, statement_basis="consolidated", as_of_date="2026-05-25")

    with store.connect() as conn:
        sales = conn.execute(
            """
            SELECT statement_basis, value FROM screener_financials
            WHERE symbol = ? AND period_type = 'annual' AND report_date = ? AND metric_id = 'sales'
            ORDER BY statement_basis
            """,
            ("AAA", "2026-03-31"),
        ).fetchall()
        valuations = conn.execute(
            """
            SELECT statement_basis, pe FROM screener_market_valuation
            WHERE symbol = ? AND date = ? ORDER BY statement_basis
            """,
            ("AAA", "2026-03-31"),
        ).fetchall()
    assert [(row["statement_basis"], row["value"]) for row in sales] == [
        ("consolidated", 2500.0),
        ("standalone", 1250.0),
    ]
    assert len(valuations) == 2
    assert valuations[0]["pe"] != valuations[1]["pe"]


def test_legacy_basis_key_migrations_require_and_verify_backup(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE screener_market_valuation (
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
            INSERT INTO screener_market_valuation
            VALUES ('AAA', '2026-03-31', 100, 1000, 10, 2, 8, 1, 'screener', 'old', '2026-05-01')
            """
        )
        conn.execute(
            """
            CREATE TABLE screener_financials (
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
                PRIMARY KEY (symbol, period_type, report_date, metric_id, available_at)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO screener_financials
            VALUES ('AAA', 'quarterly', '2026-03-31', 'standalone', 'sales', 100, '2026-05-15',
                    'screener', 'old', '2026-05-01')
            """
        )

    with pytest.raises(RuntimeError, match="migration backup directory"):
        ScreenerFinancialsStore(db_path)

    backup_dir = tmp_path / "backups"
    store = ScreenerFinancialsStore(db_path, valuation_migration_backup_dir=backup_dir)

    backups = list(backup_dir.glob("legacy.pre_statement_basis.*.db"))
    assert len(backups) == 1
    assert backups[0].with_suffix(".db.sha256").exists()
    with store.connect() as conn:
        row = conn.execute(
            "SELECT symbol, statement_basis, pe FROM screener_market_valuation"
        ).fetchone()
        primary_key = [
            item["name"]
            for item in sorted(conn.execute("PRAGMA table_info(screener_market_valuation)").fetchall(), key=lambda x: x["pk"])
            if item["pk"]
        ]
        financial_row = conn.execute(
            "SELECT symbol, statement_basis, value FROM screener_financials"
        ).fetchone()
        financial_primary_key = [
            item["name"]
            for item in sorted(conn.execute("PRAGMA table_info(screener_financials)").fetchall(), key=lambda x: x["pk"])
            if item["pk"]
        ]
    assert tuple(row) == ("AAA", "standalone", 10.0)
    assert primary_key == ["symbol", "date", "statement_basis", "source"]
    assert tuple(financial_row) == ("AAA", "standalone", 100.0)
    assert financial_primary_key == [
        "symbol",
        "period_type",
        "report_date",
        "statement_basis",
        "metric_id",
        "available_at",
    ]


def test_readmodel_default_prefers_current_consolidated_and_explicit_standalone_remains_available(tmp_path: Path) -> None:
    db_path = tmp_path / "screener_financials.db"
    store = ScreenerFinancialsStore(db_path)
    standalone = _company_data()
    consolidated = copy.deepcopy(standalone)
    consolidated["profit_loss"]["Sales"]["2026-03-31"] = 2500
    consolidated["profit_loss"]["Net profit"]["2026-03-31"] = 300
    store.save_company_financials("AAA", standalone, statement_basis="standalone", as_of_date="2026-05-25")
    store.save_company_financials("AAA", consolidated, statement_basis="consolidated", as_of_date="2026-05-25")

    default_frame = build_raw_factor_frame(store)
    standalone_frame = build_raw_factor_frame(store, statement_basis_policy="standalone")

    assert default_frame.loc[0, "statement_basis"] == "consolidated"
    assert default_frame.loc[0, "basis_resolution_reason"] == "consolidated_current"
    assert standalone_frame.loc[0, "statement_basis"] == "standalone"
    assert default_frame.loc[0, "sales_growth_3y"] != standalone_frame.loc[0, "sales_growth_3y"]
