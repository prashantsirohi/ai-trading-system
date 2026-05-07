from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.fundamentals.import_screener import import_screener_file


SCREENER_COLUMNS = [
    "Name",
    "BSE Code",
    "NSE Code",
    "ISIN Code",
    "Industry Group",
    "Industry",
    "Current Price",
    "Market Capitalization",
    "Price to Earning",
    "Forward PE",
    "PEG 3 Years Growth",
    "YOY Quarterly profit growth",
    "Profit growth 3Years",
    "Sales growth 3Years",
    "Sales growth 5Years",
    "Piotroski score",
    "EVEBITDA",
    "EPS QoQ Growth",
    "Debt to equity",
    "Profit growth 5Years",
    "Return on capital employed",
    "Return on equity",
    "Sales growth",
    "Profit growth",
    "Price to Sales",
    "Price to book value",
    "Pledged percentage",
    "Promoter holding",
    "DII holding",
    "FII holding",
    "Public holding",
    "OPM",
    "OPM last year",
    "Sales 2quarters back",
    "Sales 3quarters back",
    "Net profit 2quarters back",
    "Net profit 3quarters back",
    "Cash from operations last year",
    "Cash from investing last year",
    "Cash from financing last year",
    "Free cash flow last year",
    "Is not SME",
]


def _row(name: str, symbol: str) -> dict[str, object]:
    values = {column: 1 for column in SCREENER_COLUMNS}
    values.update(
        {
            "Name": name,
            "BSE Code": "500001",
            "NSE Code": symbol,
            "ISIN Code": "INE000A01000",
            "Industry Group": "Capital Goods",
            "Industry": "Industrial Products",
            "Current Price": 100,
            "Market Capitalization": 1000,
            "Price to Earning": 18,
            "Forward PE": 15,
            "PEG 3 Years Growth": 1,
            "YOY Quarterly profit growth": 20,
            "Profit growth 3Years": 25,
            "Sales growth 3Years": 20,
            "Sales growth 5Years": 18,
            "Piotroski score": 8,
            "EVEBITDA": 9,
            "Debt to equity": 0.2,
            "Profit growth 5Years": 18,
            "Return on capital employed": 25,
            "Return on equity": 20,
            "Price to Sales": 2,
            "Price to book value": 3,
            "Pledged percentage": 0,
            "Promoter holding": 55,
            "DII holding": 10,
            "FII holding": 12,
            "OPM": 25,
            "OPM last year": 20,
            "Cash from operations last year": 100,
            "Free cash flow last year": 50,
            "Is not SME": 1,
        }
    )
    return values


def test_import_screener_creates_tables_latest_output_and_replaces_snapshot(tmp_path: Path) -> None:
    csv_path = tmp_path / "screener.csv"
    db_path = tmp_path / "fundamentals.duckdb"
    latest_output = tmp_path / "fundamental_scores_latest.csv"
    trends_output = tmp_path / "fundamental_trends_latest.csv"
    pd.DataFrame([_row("Alpha", " aaa "), _row("Beta", "BBB")], columns=SCREENER_COLUMNS).to_csv(csv_path, index=False)

    first = import_screener_file(
        csv_path=csv_path,
        snapshot_date="2026-05-07",
        db_path=db_path,
        latest_output=latest_output,
        trends_output=trends_output,
    )
    second = import_screener_file(
        csv_path=csv_path,
        snapshot_date="2026-05-07",
        db_path=db_path,
        latest_output=latest_output,
        trends_output=trends_output,
    )

    assert latest_output.exists()
    assert trends_output.exists()
    assert set(first["symbol"]) == {"AAA", "BBB"}
    assert set(second["symbol"]) == {"AAA", "BBB"}

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        assert {"fundamental_snapshot", "fundamental_scores", "fundamental_trends"}.issubset(tables)
        assert conn.execute("SELECT COUNT(*) FROM fundamental_snapshot").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM fundamental_scores").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM fundamental_trends").fetchone()[0] == 2
        assert conn.execute("SELECT symbol FROM fundamental_snapshot ORDER BY symbol").fetchall() == [("AAA",), ("BBB",)]
    finally:
        conn.close()


def test_import_screener_computes_trends_against_previous_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "fundamentals.duckdb"
    first_csv = tmp_path / "first.csv"
    second_csv = tmp_path / "second.csv"
    latest_output = tmp_path / "fundamental_scores_latest.csv"
    trends_output = tmp_path / "fundamental_trends_latest.csv"
    first = _row("Alpha", "AAA")
    first["Return on capital employed"] = 12
    first["Return on equity"] = 10
    second = _row("Alpha", "AAA")
    second["Return on capital employed"] = 30
    second["Return on equity"] = 24
    pd.DataFrame([first], columns=SCREENER_COLUMNS).to_csv(first_csv, index=False)
    pd.DataFrame([second], columns=SCREENER_COLUMNS).to_csv(second_csv, index=False)

    import_screener_file(
        csv_path=first_csv,
        snapshot_date="2026-04-01",
        db_path=db_path,
        latest_output=latest_output,
        trends_output=trends_output,
    )
    import_screener_file(
        csv_path=second_csv,
        snapshot_date="2026-05-07",
        db_path=db_path,
        latest_output=latest_output,
        trends_output=trends_output,
    )

    trends = pd.read_csv(trends_output)
    assert trends.loc[0, "symbol"] == "AAA"
    assert trends.loc[0, "prev_snapshot_date"] == "2026-04-01"
    assert trends.loc[0, "fundamental_score_delta"] > 0
