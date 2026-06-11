from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.analytical_store import mirror_screener_financials
from ai_trading_system.domains.fundamentals.screener_readmodels import build_scores_from_screener_db, refresh_fundamental_readmodels
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
