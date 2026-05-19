"""Schema validation for regime rule YAML files.

The rules loader has historically silently ignored typos — unknown metric
keys defaulted to 0.0 and any threshold failed quietly. validate_regime_rules
catches these at boot so a bad config fails the pipeline immediately instead
of mis-classifying every day as ``neutral`` (or whatever the fallback path
returns).
"""

from __future__ import annotations

import pytest

from ai_trading_system.analytics.regime.breadth import (
    classify_regime,
    validate_regime_rules,
)


# ── Positive cases ────────────────────────────────────────────────────────


def test_validate_accepts_current_active_rules() -> None:
    """Mirrors the shipped active_regime_rules.yaml content exactly."""
    rules = {
        "rules": {
            "risk_off": {"pct_above_200dma_lt": 0.40},
            "neutral": {
                "pct_above_200dma_gte": 0.40,
                "pct_above_200dma_lt": 0.55,
            },
            "bull": {
                "pct_above_200dma_gte": 0.55,
                "top1000_above_200dma": True,
            },
            "strong_bull": {
                "pct_above_200dma_gte": 0.70,
                "pct_above_50dma_gte": 0.65,
                "top1000_above_200dma": True,
                "top1000_above_50dma": True,
            },
        }
    }
    validate_regime_rules(rules)  # must not raise


def test_validate_no_op_on_empty_or_missing_rules_block() -> None:
    validate_regime_rules({})
    validate_regime_rules({"rules": None})
    validate_regime_rules({"some_other_top_level_key": 1})


def test_classify_with_validated_rules_matches_default_path() -> None:
    """End-to-end: load → validate → classify produces the expected regimes."""
    rules = {
        "rules": {
            "risk_off": {"pct_above_200dma_lt": 0.40},
            "bull": {
                "pct_above_200dma_gte": 0.55,
                "top1000_above_200dma": True,
            },
        }
    }
    validate_regime_rules(rules)
    assert classify_regime({"pct_above_200dma": 0.30}, rules["rules"]) == "risk_off"
    assert (
        classify_regime(
            {"pct_above_200dma": 0.60, "top1000_above_200dma": True},
            rules["rules"],
        )
        == "bull"
    )


# ── Negative cases ────────────────────────────────────────────────────────


def test_unknown_metric_key_raises() -> None:
    rules = {"rules": {"bull": {"pct_above_500dma_gte": 0.55}}}
    with pytest.raises(ValueError, match="unknown metric 'pct_above_500dma'"):
        validate_regime_rules(rules)


def test_numeric_metric_missing_suffix_raises() -> None:
    rules = {"rules": {"bull": {"pct_above_200dma": 0.55}}}
    with pytest.raises(ValueError, match="requires a comparison suffix"):
        validate_regime_rules(rules)


def test_boolean_metric_with_comparison_suffix_raises() -> None:
    """The original bug: top1000_above_200dma_gte: 0.55 was 'True >= 0.55'."""
    rules = {"rules": {"bull": {"top1000_above_200dma_gte": 0.55}}}
    with pytest.raises(TypeError, match="boolean metric .* cannot be compared"):
        validate_regime_rules(rules)


def test_numeric_metric_with_non_numeric_value_raises() -> None:
    rules = {"rules": {"bull": {"pct_above_200dma_gte": "high"}}}
    with pytest.raises(TypeError, match="expected numeric value"):
        validate_regime_rules(rules)


def test_boolean_metric_with_non_bool_value_raises() -> None:
    rules = {"rules": {"bull": {"top1000_above_200dma": "yes"}}}
    with pytest.raises(TypeError, match="expected bool value"):
        validate_regime_rules(rules)


def test_non_mapping_regime_block_raises() -> None:
    rules = {"rules": {"bull": ["pct_above_200dma_gte: 0.55"]}}
    with pytest.raises(TypeError, match="expected mapping"):
        validate_regime_rules(rules)


# ── Shipped config file ───────────────────────────────────────────────────


def test_shipped_active_regime_rules_validate() -> None:
    """Load the actual file from disk via load_regime_rules; must not raise."""
    from pathlib import Path

    from ai_trading_system.analytics.regime.breadth import load_regime_rules

    repo_root = Path(__file__).resolve().parents[1]
    rules = load_regime_rules(repo_root)
    # load_regime_rules calls validate_regime_rules internally; getting
    # here means the shipped YAML is valid under the new schema.
    assert isinstance(rules, dict)
