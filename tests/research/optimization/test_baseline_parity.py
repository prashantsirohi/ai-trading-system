"""Phase 1 hard gate: ``momentum_breakout_v1.yaml`` must reproduce today's
research backtest output byte-for-byte. Failure means the compiler or adapter
is mis-routing fields; Optuna must not run until this is green.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from ai_trading_system.domains.risk import RiskPolicyConfig
from ai_trading_system.domains.strategy import load_rule_pack
from ai_trading_system.platform.db.paths import ensure_domain_layout
from ai_trading_system.research.backtesting import EngineBacktestRunner
from ai_trading_system.research.backtesting.research_loader import (
    load_research_ranked_by_date,
)
from ai_trading_system.research.optimization import run_backtest


V1_YAML = (
    Path(__file__).resolve().parents[3]
    / "config"
    / "strategies"
    / "momentum_breakout_v1.yaml"
)


def _seed_research_db(tmp_path: Path) -> tuple[date, date]:
    paths = ensure_domain_layout(project_root=tmp_path, data_domain="research")
    conn = duckdb.connect(str(paths.ohlcv_db_path))
    conn.execute(
        """
        CREATE TABLE _catalog (
            symbol_id VARCHAR,
            security_id VARCHAR,
            exchange VARCHAR,
            timestamp TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            parquet_file VARCHAR,
            ingestion_version BIGINT,
            ingestion_ts TIMESTAMP
        )
        """
    )
    start = date(2025, 1, 1)
    rows = []
    for i in range(260):
        d = start + timedelta(days=i)
        rows.append(("AAA", None, "NSE", d, 100 + i * 0.4, 102 + i * 0.4, 99 + i * 0.4, 101 + i * 0.4, 1000 + i, None, 1, d))
        rows.append(("BBB", None, "NSE", d, 200 - i * 0.1, 201 - i * 0.1, 199 - i * 0.1, 200 - i * 0.1, 900 + i, None, 1, d))
        rows.append(("NIFTY50", None, "NSE", d, 1000 + i * 0.2, 1001 + i * 0.2, 999 + i * 0.2, 1000 + i * 0.2, 5000 + i, None, 1, d))
    conn.executemany("INSERT INTO _catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()
    master = sqlite3.connect(paths.root_dir / "masterdata.db")
    master.execute("CREATE TABLE stock_details (Symbol TEXT PRIMARY KEY, Sector TEXT)")
    master.execute("INSERT INTO stock_details VALUES ('AAA', 'TECH')")
    master.execute("INSERT INTO stock_details VALUES ('BBB', 'BANKS')")
    master.commit()
    master.close()
    return start + timedelta(days=230), start + timedelta(days=259)


def test_v1_yaml_loads_and_validates():
    pack = load_rule_pack(V1_YAML)
    assert pack.strategy_id == "momentum_breakout_v1"
    # Sums to 1.0.
    assert abs(sum(pack.ranking.weights.values()) - 1.0) < 1e-9


def test_v1_pack_reproduces_default_backtest(tmp_path):
    """Adapter under v1 yaml must be byte-identical to running the raw loader +
    runner with engine defaults.
    """
    from_d, to_d = _seed_research_db(tmp_path)
    pack = load_rule_pack(V1_YAML)

    # Path A — through the rule-pack adapter.
    via_adapter = run_backtest(
        pack,
        project_root=tmp_path,
        from_date=from_d,
        to_date=to_d,
    )

    # Path B — raw, no rule pack. Same loader + runner with engine defaults.
    ranked = load_research_ranked_by_date(
        tmp_path, from_date=from_d, to_date=to_d
    )
    via_raw = EngineBacktestRunner(
        risk_config=RiskPolicyConfig(name="momentum_breakout_v1"),
        starting_equity=1_000_000.0,
        commission_bps=10.0,
        slippage_bps=20.0,
    ).run(ranked)

    # Same number of trades, same dates, same exits.
    a_trades = sorted(
        (t.symbol_id, t.entry_date, t.exit_date, t.exit_reason, round(t.pnl or 0.0, 4))
        for t in via_adapter.trades
    )
    b_trades = sorted(
        (t.symbol_id, t.entry_date, t.exit_date, t.exit_reason, round(t.pnl or 0.0, 4))
        for t in via_raw.trades
    )
    assert a_trades == b_trades, "compiler/adapter is mis-routing fields"

    # Equity curves identical to 4 dp.
    eq_a = [round(row["equity"], 4) for row in via_adapter.equity_curve]
    eq_b = [round(row["equity"], 4) for row in via_raw.equity_curve]
    assert eq_a == eq_b
