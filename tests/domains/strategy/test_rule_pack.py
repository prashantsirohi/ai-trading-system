"""Strategy rule pack: schema, compiler, and io tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.domains.strategy import (
    StrategyRulePack,
    load_rule_pack,
    rule_pack_hash,
    save_rule_pack,
    to_ranking_weights,
    to_risk_policy_config,
)


def test_default_weights_sum_to_one():
    pack = StrategyRulePack(strategy_id="t")
    assert abs(sum(pack.ranking.weights.values()) - 1.0) < 1e-9


def test_weights_must_sum_to_one():
    with pytest.raises(ValueError, match="must sum to 1.0"):
        StrategyRulePack(
            strategy_id="t",
            ranking={"weights": {"relative_strength": 0.5, "sector_strength": 0.2}},
        )


def test_unknown_factor_rejected():
    with pytest.raises(ValueError, match="unknown ranking factors"):
        StrategyRulePack(
            strategy_id="t",
            ranking={
                "weights": {
                    "relative_strength": 0.5,
                    "trend_persistence": 0.3,
                    "sector_strength": 0.2,
                    "bogus_factor": 0.0,
                }
            },
        )


def test_extra_top_level_field_rejected():
    with pytest.raises(ValueError):
        StrategyRulePack(strategy_id="t", typo_field=123)


def test_compiler_routes_weights():
    pack = StrategyRulePack(
        strategy_id="t",
        ranking={
            "weights": {
                "relative_strength": 0.50,
                "trend_persistence": 0.30,
                "sector_strength": 0.20,
            }
        },
    )
    weights = to_ranking_weights(pack)
    assert weights["relative_strength"] == 0.50
    assert weights["volume_intensity"] == 0.0


def test_compiler_routes_risk_overrides():
    pack = StrategyRulePack(
        strategy_id="t",
        risk={"sizing": {"max_position_pct": 8.0}, "constraints": {"max_concurrent_positions": 15}},
    )
    cfg = to_risk_policy_config(pack)
    assert cfg.sizing.max_position_pct == 8.0
    assert cfg.constraints.max_concurrent_positions == 15
    # Untouched fields stay at default.
    assert cfg.constraints.max_sector_exposure_pct == 30.0


def test_yaml_round_trip(tmp_path):
    pack = StrategyRulePack(strategy_id="round_trip", description="x")
    path = tmp_path / "pack.yaml"
    save_rule_pack(pack, path)
    loaded = load_rule_pack(path)
    assert loaded == pack


def test_rule_pack_hash_is_stable():
    a = StrategyRulePack(strategy_id="same")
    b = StrategyRulePack(strategy_id="same")
    assert rule_pack_hash(a) == rule_pack_hash(b)
    c = StrategyRulePack(strategy_id="different")
    assert rule_pack_hash(a) != rule_pack_hash(c)


def test_rule_pack_hash_changes_when_weights_change():
    a = StrategyRulePack(strategy_id="t")
    b = StrategyRulePack(
        strategy_id="t",
        ranking={
            "weights": {
                "relative_strength": 0.40,
                "volume_intensity": 0.0,
                "trend_persistence": 0.20,
                "momentum_acceleration": 0.0,
                "proximity_highs": 0.18,
                "delivery_pct": 0.0,
                "sector_strength": 0.22,
            }
        },
    )
    assert rule_pack_hash(a) != rule_pack_hash(b)
