from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb

from ai_trading_system.domains.execution.adapters import PaperExecutionAdapter
from ai_trading_system.domains.execution.models import OrderIntent
from ai_trading_system.domains.execution.portfolio import closed_trade_ref, open_position_trade_ref
from ai_trading_system.domains.execution.service import ExecutionService
from ai_trading_system.domains.execution.store import ExecutionStore
from ai_trading_system.ui.execution_api.services.research_data_access import (
    build_portfolio_candidate_frame,
    load_portfolio_workspace_report,
    load_trade_report,
    save_trade_journal_note,
)


def _write_masterdata_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            'CREATE TABLE stock_details (Security_id INT, Name TEXT, Symbol TEXT, "Industry Group" TEXT, Industry TEXT, MCAP REAL, Sector TEXT, exchange TEXT)'
        )
        conn.execute(
            "INSERT INTO stock_details VALUES (1, 'Alpha Ltd', 'ALPHA', 'Capital Goods', 'Industrial', 1.0, 'Industrial', 'NSE')"
        )
        conn.execute(
            "INSERT INTO stock_details VALUES (2, 'Beta Ltd', 'BETA', 'Banks', 'Finance', 1.0, 'Finance', 'NSE')"
        )
        conn.commit()
    finally:
        conn.close()


def _write_rank_artifacts(base_dir: Path) -> None:
    rank_dir = base_dir / "data" / "pipeline_runs" / "pipeline-2026-04-09-abcd1234" / "rank" / "attempt_1"
    rank_dir.mkdir(parents=True, exist_ok=True)
    (rank_dir / "ranked_signals.csv").write_text(
        "symbol_id,exchange,close,composite_score,sector_name\n"
        "ALPHA,NSE,100.0,88.0,Industrial\n"
        "BETA,NSE,200.0,72.0,Finance\n",
        encoding="utf-8",
    )
    (rank_dir / "breakout_scan.csv").write_text(
        "symbol_id,breakout_tag,breakout_state,candidate_tier,breakout_score,breakout_rank,symbol_trend_reasons,filter_reason\n"
        "ALPHA,high_52w_breakout,qualified,A,6,1,ABOVE_SMA200,\n"
        "BETA,resistance_breakout_50d,filtered_by_symbol_trend,C,2,2,BELOW_SMA200,tier_c\n",
        encoding="utf-8",
    )
    (rank_dir / "dashboard_payload.json").write_text(
        '{"ranked_signals":[{"symbol_id":"ALPHA","exchange":"NSE","close":100.0,"composite_score":88.0}]}',
        encoding="utf-8",
    )


def _write_ohlcv_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    try:
        conn.execute(
            """
            CREATE TABLE _catalog (
                symbol_id TEXT,
                exchange TEXT,
                timestamp TIMESTAMP,
                close DOUBLE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO _catalog VALUES
                ('ALPHA', 'NSE', TIMESTAMP '2026-04-09 00:00:00', 110.0),
                ('BETA', 'NSE', TIMESTAMP '2026-04-09 00:00:00', 180.0)
            """
        )
    finally:
        conn.close()


def test_trade_notes_persist_and_report_uses_trade_refs(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))

    service.submit_order(
        OrderIntent(symbol_id="ALPHA", exchange="NSE", quantity=10, side="BUY"),
        market_price=100.0,
    )
    save_trade_journal_note(
        str(tmp_path),
        trade_ref=open_position_trade_ref("ALPHA", "NSE"),
        symbol_id="ALPHA",
        exchange="NSE",
        thesis="Breakout with strong rank",
        setup_note="Near 52-week high",
        tags="A,breakout",
    )

    report = load_trade_report(str(tmp_path))
    open_positions = report["open_positions"]

    assert len(open_positions) == 1
    assert open_positions.iloc[0]["trade_ref"] == "open:NSE:ALPHA"
    assert open_positions.iloc[0]["thesis"] == "Breakout with strong rank"
    assert open_positions.iloc[0]["setup_note"] == "Near 52-week high"


def test_build_portfolio_candidate_frame_merges_rank_breakout_and_masterdata(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_masterdata_db(data_dir / "masterdata.db")
    _write_rank_artifacts(tmp_path)

    frame = build_portfolio_candidate_frame(str(tmp_path))

    assert frame["symbol_id"].tolist() == ["ALPHA", "BETA"]
    assert frame.iloc[0]["company_name"] == "Alpha Ltd"
    assert frame.iloc[0]["breakout_tag"] == "high_52w_breakout"
    assert bool(frame.iloc[0]["has_breakout"]) is True
    assert "tradingview.com" in frame.iloc[0]["tradingview_url"]


def test_workspace_report_enriches_positions_and_sell_suggestions(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_masterdata_db(data_dir / "masterdata.db")
    _write_rank_artifacts(tmp_path)
    _write_ohlcv_db(data_dir / "ohlcv.duckdb")

    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))
    service.submit_order(
        OrderIntent(symbol_id="BETA", exchange="NSE", quantity=5, side="BUY"),
        market_price=200.0,
    )

    report = load_portfolio_workspace_report(str(tmp_path), top_rank_limit=1)
    open_positions = report["open_positions"]

    assert len(open_positions) == 1
    assert open_positions.iloc[0]["symbol_id"] == "BETA"
    assert open_positions.iloc[0]["sell_suggestion"] == "REVIEW"
    assert "tier_c" in str(open_positions.iloc[0]["sell_reason"])


def test_closed_trade_ref_note_can_be_persisted(tmp_path: Path) -> None:
    store = ExecutionStore(tmp_path)
    service = ExecutionService(store, PaperExecutionAdapter(slippage_bps=0))

    service.submit_order(
        OrderIntent(symbol_id="ALPHA", exchange="NSE", quantity=10, side="BUY"),
        market_price=100.0,
    )
    sell_result = service.submit_order(
        OrderIntent(symbol_id="ALPHA", exchange="NSE", quantity=10, side="SELL"),
        market_price=110.0,
    )

    fill_id = sell_result["fills"][0]["fill_id"]
    trade_ref = closed_trade_ref(fill_id)
    save_trade_journal_note(
        str(tmp_path),
        trade_ref=trade_ref,
        symbol_id="ALPHA",
        exchange="NSE",
        exit_note="Booked profit into resistance",
    )

    notes = store.list_trade_notes()
    assert any(row["trade_ref"] == trade_ref and row["exit_note"] == "Booked profit into resistance" for row in notes)
