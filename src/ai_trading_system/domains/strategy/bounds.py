"""Search-space construction for Optuna trials.

Phase 1/3 narrow scope:
- Ranking weights (6 floats, constrained to sum to 1.0 via Dirichlet-style
  sampling + normalisation).
- Risk knobs already wired through ``RiskPolicyConfig``: stop, exit, sizing,
  constraints.

Each knob has explicit bounds; nothing is unbounded. Optuna's TPE sampler
explores within these. Add new knobs deliberately — every search-space field
is also a fitness degree-of-freedom.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ai_trading_system.domains.strategy.rule_pack import FACTOR_KEYS, StrategyRulePack

if TYPE_CHECKING:
    import optuna


def build_search_space(trial: "optuna.Trial", *, strategy_id: str) -> StrategyRulePack:
    """Sample one ``StrategyRulePack`` from a constrained search space."""
    # Ranking weights: sample one float per factor in [0, 1], then normalise.
    raw_weights = {
        key: trial.suggest_float(f"w_{key}", 0.0, 1.0)
        for key in FACTOR_KEYS
    }
    total = sum(raw_weights.values())
    if total <= 0:
        # Degenerate sample — fall back to equal weights.
        weights = {k: 1.0 / len(FACTOR_KEYS) for k in FACTOR_KEYS}
    else:
        weights = {k: v / total for k, v in raw_weights.items()}

    # Risk knobs — keep bounds tight; widen once Optuna's first study converges.
    risk = {
        "stop": {
            "method": trial.suggest_categorical(
                "stop_method", ["atr", "percent", "swing_low"]
            ),
            "atr_multiple": trial.suggest_float("stop_atr_multiple", 1.5, 3.5),
            "stop_pct": trial.suggest_float("stop_pct", 0.04, 0.12),
        },
        "exit": {
            "dma_exit_window": trial.suggest_categorical("dma_exit_window", [11, 20, 50]),
            "max_hold_rank": trial.suggest_int("max_hold_rank", 30, 80),
            "time_stop_days": trial.suggest_int("time_stop_days", 20, 90),
            "rank_deterioration_bars": trial.suggest_int(
                "rank_deterioration_bars", 2, 6
            ),
        },
        "sizing": {
            "risk_per_trade_pct": trial.suggest_float("risk_per_trade_pct", 0.5, 1.5),
            "max_position_pct": trial.suggest_float("max_position_pct", 6.0, 15.0),
        },
        "constraints": {
            "max_concurrent_positions": trial.suggest_int(
                "max_concurrent_positions", 6, 20
            ),
            "max_sector_exposure_pct": trial.suggest_float(
                "max_sector_exposure_pct", 20.0, 40.0
            ),
        },
    }

    return StrategyRulePack(
        strategy_id=strategy_id,
        ranking={"weights": weights},
        risk=risk,
    )
