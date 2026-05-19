"""Phase 5 — regime-aware factor weights tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from ai_trading_system.domains.ranking.composite import (
    DEFAULT_FACTOR_WEIGHTS,
    load_factor_weights,
)


# ── No regime supplied → default block ────────────────────────────────────


def test_load_default_block_when_no_regime() -> None:
    """Calling without a regime should match the historical (pre-Phase-5)
    behavior: the shipped default block wins. The default values in the
    yaml match the legacy json contents, so downstream callers that
    omit ``regime=`` see no change."""
    w = load_factor_weights()
    assert w["relative_strength"] == DEFAULT_FACTOR_WEIGHTS["relative_strength"]
    assert w["trend_persistence"] == DEFAULT_FACTOR_WEIGHTS["trend_persistence"]
    assert w["sector_strength"] == DEFAULT_FACTOR_WEIGHTS["sector_strength"]


# ── Per-regime overlays ───────────────────────────────────────────────────


def test_risk_off_emphasizes_defensive_factors() -> None:
    """risk_off should weight trend_persistence and proximity_highs more
    than the default block (defensive bias)."""
    default = load_factor_weights()
    risk_off = load_factor_weights(regime="risk_off")
    assert risk_off["trend_persistence"] > default["trend_persistence"]
    assert risk_off["proximity_highs"] >= default["proximity_highs"]
    # And reduces relative_strength weight (don't chase momentum in risk_off)
    assert risk_off["relative_strength"] < default["relative_strength"]


def test_strong_bull_emphasizes_volume_and_leadership() -> None:
    """strong_bull should activate volume_intensity (zero in default)
    and bias toward relative_strength."""
    default = load_factor_weights()
    strong_bull = load_factor_weights(regime="strong_bull")
    assert default["volume_intensity"] == 0.0
    assert strong_bull["volume_intensity"] > 0.10  # activated
    assert strong_bull["delivery_pct"] > 0.0  # also activated


def test_cautious_bull_distinct_from_bull() -> None:
    cautious = load_factor_weights(regime="cautious_bull")
    bull = load_factor_weights(regime="bull")
    # The two profiles should be measurably different
    assert cautious != bull
    # cautious leans proximity_highs higher (leadership-aware)
    assert cautious["proximity_highs"] > bull["proximity_highs"]


def test_weight_blocks_sum_to_approximately_one() -> None:
    """Each per-regime block should sum to ~1.0 — maintenance discipline."""
    for regime in ("risk_off", "cautious_bull", "bull", "strong_bull"):
        w = load_factor_weights(regime=regime)
        total = sum(w[k] for k in DEFAULT_FACTOR_WEIGHTS if k != "above_200dma")
        # above_200dma is a registered dormant factor (default 0.0); per-regime
        # blocks don't override it. Sum the *active* factors only.
        assert 0.95 <= total <= 1.05, f"{regime}: weights sum to {total:.3f}"


# ── Fallbacks ─────────────────────────────────────────────────────────────


def test_unknown_regime_falls_back_to_default() -> None:
    """An unknown regime name (e.g. typo or a future regime not yet in
    the YAML) should silently fall back to the default block — adding
    new regimes shouldn't break existing rankers."""
    unknown = load_factor_weights(regime="panic")
    default = load_factor_weights()
    assert unknown == default


def test_explicit_config_path_overrides_built_in(tmp_path: Path) -> None:
    """When config_path is provided, the shipped YAML is ignored."""
    custom = tmp_path / "custom_weights.yaml"
    custom.write_text(
        "default:\n"
        "  relative_strength: 0.10\n"
        "  trend_persistence: 0.90\n",
        encoding="utf-8",
    )
    w = load_factor_weights(config_path=custom)
    assert w["relative_strength"] == 0.10
    assert w["trend_persistence"] == 0.90


def test_explicit_config_path_supports_regime_overlay(tmp_path: Path) -> None:
    custom = tmp_path / "custom_weights.yaml"
    custom.write_text(
        "default:\n"
        "  relative_strength: 0.30\n"
        "  trend_persistence: 0.50\n"
        "risk_off:\n"
        "  relative_strength: 0.10\n",
        encoding="utf-8",
    )
    default = load_factor_weights(config_path=custom)
    risk_off = load_factor_weights(config_path=custom, regime="risk_off")
    assert default["relative_strength"] == 0.30
    assert risk_off["relative_strength"] == 0.10  # overlay applied
    assert risk_off["trend_persistence"] == 0.50  # inherited from default


def test_legacy_flat_json_still_loads(tmp_path: Path) -> None:
    """Pre-Phase-5 JSON files (flat keys, no regime sections) must still
    work — the file is read as the default block."""
    import json

    legacy = tmp_path / "legacy.json"
    legacy.write_text(
        json.dumps(
            {
                "relative_strength": 0.40,
                "trend_persistence": 0.30,
                "proximity_highs": 0.20,
                "sector_strength": 0.10,
            }
        ),
        encoding="utf-8",
    )
    w = load_factor_weights(config_path=legacy)
    assert w["relative_strength"] == 0.40
    assert w["trend_persistence"] == 0.30


def test_malformed_yaml_falls_back_to_defaults(tmp_path: Path) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text(": : :", encoding="utf-8")
    w = load_factor_weights(config_path=broken)
    # Should not raise; returns built-in defaults
    assert w["relative_strength"] == DEFAULT_FACTOR_WEIGHTS["relative_strength"]


# ── Ranker integration ──────────────────────────────────────────────────


def test_ranker_rank_all_accepts_regime_kwarg() -> None:
    """Smoke test the StockRanker.rank_all signature — regime kwarg must
    be accepted without breaking the existing API."""
    from inspect import signature

    from ai_trading_system.domains.ranking.ranker import StockRanker

    params = signature(StockRanker.rank_all).parameters
    assert "regime" in params
    assert params["regime"].default is None  # backward compat: optional
