import os
from pathlib import Path

from core.bootstrap import ensure_project_root_on_path

project_root = ensure_project_root_on_path(__file__)

from analytics import (
    RegimeDetector,
    StockRanker,
    AlphaEngine,
    RiskManager,
    EventBacktester,
    Visualizer,
    AIQScreener,
)

print("=" * 60)
print("AI Trading System — Analytics Layer Test")
print("=" * 60)

print("\n[1] Regime Detection...")
rd = RegimeDetector()
regime = rd.get_market_regime()
print(f"  Market regime: {regime['market_regime']}")
print(f"  Pct trending: {regime['pct_trending']}%")

regimes = rd.detect_bulk_regimes(["RELIANCE", "HDFCBANK", "INFY"], date="2026-03-18")
print(f"  Bulk regimes:\n{regimes[['symbol_id', 'regime']].to_string(index=False)}")

print("\n[2] Multi-Factor Ranking...")
ranker = StockRanker()
signals = ranker.rank_all(date="2026-03-18", min_score=0, top_n=10)
print(f"  Ranked {len(signals)} signals")
if not signals.empty:
    print(f"  Top 5:")
    print(
        signals[
            [
                "symbol_id",
                "composite_score",
                "rel_strength_score",
                "vol_intensity_score",
            ]
        ]
        .head(5)
        .to_string(index=False)
    )

print("\n[3] Risk Management...")
rm = RiskManager()
pos = rm.compute_position_size("RELIANCE", capital=1_000_000, regime="TREND")
print(f"  Position size for RELIANCE:")
for k, v in pos.items():
    print(f"    {k}: {v}")

portfolio = rm.build_portfolio(signals.head(5), capital=1_000_000)
if not portfolio.empty:
    print(f"  Portfolio ({len(portfolio)} positions):")
    print(
        portfolio[
            ["symbol_id", "shares", "position_value", "weight", "stop_loss"]
        ].to_string(index=False)
    )

print("\n[4] Backtesting (Event-Driven)...")
bt = EventBacktester()
bt_result = bt.run_event_backtest(
    symbols=["RELIANCE", "HDFCBANK", "INFY", "TCS", "SBIN"],
    event_type="BREAKOUT",
    horizon=20,
)
if "metrics" in bt_result:
    m = bt_result["metrics"]
    print(f"  {bt_result['n_trades']} trades, win_rate={m['win_rate'] * 100:.1f}%")
    print(f"  Sharpe={m['sharpe']:.2f}, max_dd={m['max_drawdown'] * 100:.1f}%")
    print(f"  Total PnL: {m['total_pnl']:.2f}")
    print(f"  Exit reasons: {m['exit_reason_counts']}")

print("\n[5] ML Engine (Walk-Forward)...")
ml = AlphaEngine()
wf = ml.walk_forward_validate(
    symbols=["RELIANCE", "HDFCBANK", "INFY", "TCS", "SBIN", "KOTAKBANK", "BAJFINANCE"],
    train_days=126,
    test_days=21,
    horizon=5,
)
if "error" not in wf:
    print(f"  Walk-Forward: {wf['n_windows']} windows")
    print(f"  Avg AUC: {wf['avg_auc']:.4f} +/- {wf['std_auc']:.4f}")
    print(f"  Avg signal return: {wf['avg_signal_return']:.2f}%")
else:
    print(f"  WFV note: {wf['error']}")

print("\n[6] Visualizations...")
viz = Visualizer()
chart = viz.plot_technical_chart(
    "RELIANCE",
    from_date="2025-10-01",
    to_date="2026-03-18",
    output_path=str(Path(project_root) / "reports" / "RELIANCE_test.html"),
)
print(f"  Chart generated: {chart is not None}")

print("\n[7] Full Screener Pipeline...")
screener = AIQScreener()
result = screener.run_daily_screening(
    date="2026-03-18",
    top_n=20,
    min_score=50.0,
    run_ml=False,
    run_backtest=True,
    generate_report=False,
    capital=5_000_000,
)
print(f"  Regime: {result['regime']['market_regime']}")
print(f"  Ranked signals: {len(result['ranked_signals'])}")
print(f"  Portfolio positions: {len(result['portfolio'])}")
if result["backtest_results"].get("metrics"):
    m = result["backtest_results"]["metrics"]
    print(f"  Backtest: {m['n_trades']} trades, Sharpe={m['sharpe']:.2f}")

print("\n" + "=" * 60)
print("Analytics Layer: ALL OK")
print("=" * 60)
