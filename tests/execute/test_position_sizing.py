from __future__ import annotations

import pandas as pd

from ai_trading_system.analytics.risk_manager import compute_atr_position_size as risk_compute_atr_position_size
from ai_trading_system.analytics.risk_manager import RiskManager
from ai_trading_system.domains.execution.policies import compute_atr_position_size as policy_compute_atr_position_size


def test_compute_atr_position_size_available_and_consistent() -> None:
    qty_policy = policy_compute_atr_position_size(
        capital=100_000.0,
        risk_per_trade=0.01,
        entry_price=100.0,
        atr=2.0,
        atr_multiple=2.0,
    )
    qty_risk = risk_compute_atr_position_size(
        capital=100_000.0,
        risk_per_trade=0.01,
        entry_price=100.0,
        atr=2.0,
        atr_multiple=2.0,
    )

    assert qty_policy == 250
    assert qty_risk == 250


def test_risk_manager_atr_fallback_uses_parameterized_query(monkeypatch, tmp_path) -> None:
    class _Conn:
        def __init__(self) -> None:
            self.calls = []
            self.closed = False

        def execute(self, query, params=None):
            self.calls.append((query, params))
            return self

        def fetchone(self):
            return (1.5,)

        def close(self) -> None:
            self.closed = True

    conn = _Conn()
    manager = RiskManager(ohlcv_db_path=str(tmp_path / "ohlcv.duckdb"), feature_store_dir=str(tmp_path / "features"))
    monkeypatch.setattr(manager, "_get_conn", lambda: conn)
    monkeypatch.setattr("ai_trading_system.analytics.risk_manager.os.path.exists", lambda _path: False)
    monkeypatch.setattr("ai_trading_system.analytics.risk_manager.pd.read_parquet", lambda _path: pd.DataFrame())

    atr = manager._get_atr("ABC'; DROP TABLE _catalog; --", exchange="NSE", period=14)

    assert atr == 1.5
    assert len(conn.calls) == 1
    query, params = conn.calls[0]
    assert "WHERE symbol_id = ? AND exchange = ?" in query
    assert params == ("ABC'; DROP TABLE _catalog; --", "NSE")
    assert conn.closed is True
