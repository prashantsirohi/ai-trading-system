"""Rule-pack compiler: map declarative knobs to engine-ready inputs."""

from __future__ import annotations

from ai_trading_system.domains.risk.config import RiskPolicyConfig
from ai_trading_system.domains.strategy.rule_pack import StrategyRulePack


def to_ranking_weights(pack: StrategyRulePack) -> dict[str, float]:
    """Weights dict suitable for ``compute_factor_scores(weights=...)``."""
    return dict(pack.ranking.weights)


def to_risk_policy_config(pack: StrategyRulePack) -> RiskPolicyConfig:
    """Build a ``RiskPolicyConfig`` from the pack's risk section.

    Empty risk dict → engine defaults (matches current production behavior).
    """
    payload = dict(pack.risk or {})
    payload.setdefault("name", pack.strategy_id)
    return RiskPolicyConfig.from_dict(payload)
