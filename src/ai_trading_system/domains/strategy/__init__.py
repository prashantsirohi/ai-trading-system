"""Strategy rule pack: declarative, YAML-driven knobs the optimizer searches over.

Phase 1 narrow scope — only knobs already supported cleanly by the rank-mode
backtester: ranking weights, basic entry/exit/risk via ``RiskPolicyConfig``.
Pattern detection, breakout-tier filters, and event types are out of scope.
"""

from ai_trading_system.domains.strategy.rule_pack import (
    RankingConfig,
    StrategyRulePack,
)
from ai_trading_system.domains.strategy.io import (
    load_rule_pack,
    rule_pack_hash,
    save_rule_pack,
)
from ai_trading_system.domains.strategy.compiler import (
    to_ranking_weights,
    to_risk_policy_config,
)

__all__ = [
    "RankingConfig",
    "StrategyRulePack",
    "load_rule_pack",
    "rule_pack_hash",
    "save_rule_pack",
    "to_ranking_weights",
    "to_risk_policy_config",
]
