"""Phase 4 hysteresis tests for classify_regime.

Hysteresis design: a regime block can carry nested ``enter:`` (strict,
applied when moving INTO the regime) and ``exit:`` (looser, applied
when already in the regime) sub-blocks. classify_regime first asks
"does the previous_regime's exit still hold?"; only if no, it walks
the priority list applying enter predicates.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ai_trading_system.analytics.regime.breadth import (
    classify_regime,
    resolve_previous_regime,
    validate_regime_rules,
)


# ── Hysteresis classification ────────────────────────────────────────────


def _hysteresis_rules() -> dict:
    """Two-regime rule set sufficient for transition testing."""
    return {
        "bull": {
            "enter": {"pct_above_200dma_gte": 0.55},
            "exit": {"pct_above_200dma_gte": 0.50},
        },
        "neutral": {
            "enter": {"pct_above_200dma_gte": 0.30},
            "exit": {"pct_above_200dma_gte": 0.28},
        },
        "__priority__": ["bull", "neutral"],
    }


def test_stays_in_bull_when_exit_still_holds() -> None:
    """At 200DMA=0.53, bull.enter fails but bull.exit (0.50) holds."""
    out = classify_regime(
        {"pct_above_200dma": 0.53},
        rules=_hysteresis_rules(),
        previous_regime="bull",
    )
    assert out == "bull"


def test_falls_out_of_bull_when_exit_fails() -> None:
    """At 200DMA=0.45, bull.exit (>=0.50) fails — fall to neutral."""
    out = classify_regime(
        {"pct_above_200dma": 0.45},
        rules=_hysteresis_rules(),
        previous_regime="bull",
    )
    assert out == "neutral"


def test_enters_bull_only_when_enter_holds() -> None:
    """At 200DMA=0.53 (between enter=0.55 and exit=0.50), entering from
    neutral must REJECT — enter requires 0.55+. Without hysteresis, this
    would have flipped to bull."""
    out = classify_regime(
        {"pct_above_200dma": 0.53},
        rules=_hysteresis_rules(),
        previous_regime="neutral",
    )
    assert out == "neutral"


def test_enters_bull_when_enter_holds() -> None:
    out = classify_regime(
        {"pct_above_200dma": 0.58},
        rules=_hysteresis_rules(),
        previous_regime="neutral",
    )
    assert out == "bull"


def test_oscillation_between_thresholds_does_not_flip() -> None:
    """Synthetic 30-day series oscillating between 0.54 and 0.56 should
    keep us in bull throughout, after the initial entry."""
    rules = _hysteresis_rules()
    regime = "bull"  # already in bull
    sequence = [0.54, 0.56, 0.54, 0.56, 0.53, 0.55, 0.54, 0.56, 0.53, 0.55]
    transitions = 0
    for pct in sequence:
        new = classify_regime(
            {"pct_above_200dma": pct}, rules=rules, previous_regime=regime
        )
        if new != regime:
            transitions += 1
        regime = new
    # Hysteresis should keep us in bull for the whole oscillation
    assert transitions == 0
    assert regime == "bull"


def test_without_previous_regime_uses_enter_only() -> None:
    """Cold start (no previous_regime) ignores exit blocks — only enter
    predicates determine classification. At 200DMA=0.53, enter for bull
    fails (needs 0.55), so we land in neutral."""
    out = classify_regime(
        {"pct_above_200dma": 0.53},
        rules=_hysteresis_rules(),
        previous_regime=None,
    )
    assert out == "neutral"


def test_flat_rule_blocks_still_work_without_enter_exit_subblocks() -> None:
    """Backward compat: a regime block without enter/exit subblocks
    behaves as legacy (the whole block applies to both enter and exit
    paths). No hysteresis but no breakage either."""
    flat_rules = {
        "bull": {"pct_above_200dma_gte": 0.55, "top1000_above_200dma": True},
        "neutral": {"pct_above_200dma_gte": 0.30},
        "__priority__": ["bull", "neutral"],
    }
    # At 200DMA=0.53, bull's flat predicates fail → neutral, regardless
    # of previous_regime
    assert (
        classify_regime(
            {"pct_above_200dma": 0.53, "top1000_above_200dma": True},
            rules=flat_rules,
            previous_regime="bull",
        )
        == "neutral"
    )


# ── Validator handles enter/exit sub-blocks ─────────────────────────────


def test_validator_accepts_enter_exit_subblocks() -> None:
    rules = {
        "rules": {
            "bull": {
                "enter": {"pct_above_200dma_gte": 0.55, "top1000_above_200dma": True},
                "exit": {"pct_above_200dma_gte": 0.50, "top1000_above_200dma": True},
            }
        }
    }
    validate_regime_rules(rules)  # must not raise


def test_validator_rejects_typo_in_enter_subblock() -> None:
    rules = {
        "rules": {
            "bull": {
                "enter": {"pct_above_500dma_gte": 0.55},
            }
        }
    }
    with pytest.raises(ValueError, match="unknown metric 'pct_above_500dma'"):
        validate_regime_rules(rules)


def test_validator_rejects_non_dict_enter_block() -> None:
    rules = {"rules": {"bull": {"enter": "tight"}}}
    with pytest.raises(TypeError, match="expected mapping"):
        validate_regime_rules(rules)


# ── Previous-regime resolver ────────────────────────────────────────────


def test_resolve_previous_regime_reads_dashboard_payload(tmp_path: Path) -> None:
    payload = tmp_path / "dashboard_payload.json"
    payload.write_text(
        json.dumps({"market_regime": {"regime": "cautious_bull"}}), encoding="utf-8"
    )

    artifact = MagicMock()
    artifact.uri = str(payload)
    registry = MagicMock()
    registry.get_latest_artifact.return_value = [artifact]

    assert resolve_previous_regime(registry) == "cautious_bull"
    registry.get_latest_artifact.assert_called_once_with(
        stage_name="rank",
        artifact_type="dashboard_payload",
        limit=1,
        exclude_run_id=None,
    )


def test_resolve_previous_regime_returns_none_when_no_artifacts() -> None:
    registry = MagicMock()
    registry.get_latest_artifact.return_value = []
    assert resolve_previous_regime(registry) is None


def test_resolve_previous_regime_returns_none_on_missing_file(tmp_path: Path) -> None:
    artifact = MagicMock()
    artifact.uri = str(tmp_path / "does_not_exist.json")
    registry = MagicMock()
    registry.get_latest_artifact.return_value = [artifact]
    assert resolve_previous_regime(registry) is None


def test_resolve_previous_regime_returns_none_on_malformed_json(tmp_path: Path) -> None:
    payload = tmp_path / "broken.json"
    payload.write_text("{not valid json", encoding="utf-8")

    artifact = MagicMock()
    artifact.uri = str(payload)
    registry = MagicMock()
    registry.get_latest_artifact.return_value = [artifact]
    assert resolve_previous_regime(registry) is None


def test_resolve_previous_regime_returns_none_when_field_absent(tmp_path: Path) -> None:
    payload = tmp_path / "no_regime.json"
    payload.write_text(json.dumps({"summary": {}}), encoding="utf-8")

    artifact = MagicMock()
    artifact.uri = str(payload)
    registry = MagicMock()
    registry.get_latest_artifact.return_value = [artifact]
    assert resolve_previous_regime(registry) is None


def test_resolve_previous_regime_passes_exclude_run_id() -> None:
    registry = MagicMock()
    registry.get_latest_artifact.return_value = []
    resolve_previous_regime(registry, exclude_run_id="run-123")
    registry.get_latest_artifact.assert_called_once_with(
        stage_name="rank",
        artifact_type="dashboard_payload",
        limit=1,
        exclude_run_id="run-123",
    )


def test_resolve_previous_regime_returns_none_for_null_registry() -> None:
    assert resolve_previous_regime(None) is None
