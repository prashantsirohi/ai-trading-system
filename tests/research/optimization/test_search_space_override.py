"""Wave 4: recipe-level search-space overrides.

Covers three concerns:

1. ``SearchSpaceOverride.from_dict`` rejects unknown parameter names, bad
   shapes, low>=high, and categorical values outside the default whitelist.
2. ``build_search_space(trial, overrides=...)`` passes the overridden
   low/high/choices into Optuna's suggest_* calls (recorded on a fake
   trial), and falls back to defaults for any unspecified parameter.
3. ``load_recipe`` end-to-end accepts a recipe with a ``search_space:`` block
   and surfaces the parsed override; recipes without the block still produce
   ``recipe.search_space is None``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.domains.strategy.bounds import (
    KNOWN_PARAMS,
    build_search_space,
)
from ai_trading_system.research.optimization.recipe import (
    CategoricalChoices,
    FloatBound,
    IntBound,
    SearchSpaceOverride,
    load_recipe,
)


# ---------------------------------------------------------------------------
# Fake Optuna trial: records every suggest_* call so we can assert the args
# the override changed (low/high/choices) without depending on Optuna itself.
# ---------------------------------------------------------------------------


class _FakeTrial:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, tuple]] = []

    def suggest_float(self, name: str, low: float, high: float) -> float:
        self.calls.append(("float", name, (low, high)))
        return (low + high) / 2.0

    def suggest_int(self, name: str, low: int, high: int) -> int:
        self.calls.append(("int", name, (low, high)))
        return (low + high) // 2

    def suggest_categorical(self, name: str, choices: list[Any]) -> Any:
        self.calls.append(("categorical", name, tuple(choices)))
        return choices[0]


def _calls_for(trial: _FakeTrial, name: str) -> tuple[str, str, tuple]:
    matches = [c for c in trial.calls if c[1] == name]
    assert matches, f"no suggest_* call recorded for {name!r}"
    return matches[0]


# ---------------------------------------------------------------------------
# SearchSpaceOverride.from_dict — validator
# ---------------------------------------------------------------------------


def test_from_dict_none_returns_none() -> None:
    assert SearchSpaceOverride.from_dict(None) is None
    assert SearchSpaceOverride.from_dict({}) is None


def test_from_dict_float_bound_happy() -> None:
    ov = SearchSpaceOverride.from_dict({"stop_pct": {"low": 0.02, "high": 0.05}})
    assert isinstance(ov.overrides["stop_pct"], FloatBound)
    assert ov.overrides["stop_pct"] == FloatBound(0.02, 0.05)


def test_from_dict_int_bound_happy() -> None:
    ov = SearchSpaceOverride.from_dict({"max_hold_rank": {"low": 40, "high": 60}})
    assert isinstance(ov.overrides["max_hold_rank"], IntBound)
    assert ov.overrides["max_hold_rank"] == IntBound(40, 60)


def test_from_dict_categorical_str_happy() -> None:
    ov = SearchSpaceOverride.from_dict({"stop_method": {"choices": ["atr", "percent"]}})
    assert isinstance(ov.overrides["stop_method"], CategoricalChoices)
    assert ov.overrides["stop_method"].choices == ("atr", "percent")


def test_from_dict_categorical_int_happy() -> None:
    ov = SearchSpaceOverride.from_dict({"dma_exit_window": {"choices": [11, 20]}})
    assert ov.overrides["dma_exit_window"].choices == (11, 20)


def test_from_dict_unknown_parameter_errors() -> None:
    with pytest.raises(ValueError, match="unknown parameter 'bogus_key'"):
        SearchSpaceOverride.from_dict({"bogus_key": {"low": 0, "high": 1}})


def test_from_dict_missing_low_or_high_errors() -> None:
    with pytest.raises(ValueError, match="requires keys 'low' and 'high'"):
        SearchSpaceOverride.from_dict({"stop_pct": {"low": 0.02}})


def test_from_dict_low_not_less_than_high_errors() -> None:
    with pytest.raises(ValueError, match="low .* must be < high"):
        SearchSpaceOverride.from_dict({"stop_pct": {"low": 0.05, "high": 0.05}})


def test_from_dict_categorical_missing_choices_errors() -> None:
    with pytest.raises(ValueError, match="requires key 'choices'"):
        SearchSpaceOverride.from_dict({"stop_method": {"low": 0, "high": 1}})


def test_from_dict_categorical_empty_choices_errors() -> None:
    with pytest.raises(ValueError, match="non-empty list"):
        SearchSpaceOverride.from_dict({"stop_method": {"choices": []}})


def test_from_dict_categorical_outside_default_errors() -> None:
    with pytest.raises(ValueError, match="not in defaults"):
        SearchSpaceOverride.from_dict({"stop_method": {"choices": ["atr", "rocket_ship"]}})


def test_from_dict_non_mapping_value_errors() -> None:
    with pytest.raises(ValueError, match="must be a mapping"):
        SearchSpaceOverride.from_dict({"stop_pct": [0.02, 0.05]})


# ---------------------------------------------------------------------------
# build_search_space — uses override low/high/choices when present
# ---------------------------------------------------------------------------


def test_build_search_space_uses_overridden_float_bounds() -> None:
    overrides = SearchSpaceOverride.from_dict({"stop_pct": {"low": 0.02, "high": 0.025}})
    trial = _FakeTrial()
    build_search_space(trial, strategy_id="x", overrides=overrides)
    kind, name, args = _calls_for(trial, "stop_pct")
    assert kind == "float"
    assert args == (0.02, 0.025)


def test_build_search_space_uses_overridden_int_bounds() -> None:
    overrides = SearchSpaceOverride.from_dict({"max_hold_rank": {"low": 50, "high": 55}})
    trial = _FakeTrial()
    build_search_space(trial, strategy_id="x", overrides=overrides)
    kind, name, args = _calls_for(trial, "max_hold_rank")
    assert kind == "int"
    assert args == (50, 55)


def test_build_search_space_uses_overridden_categorical() -> None:
    overrides = SearchSpaceOverride.from_dict({"dma_exit_window": {"choices": [11, 20]}})
    trial = _FakeTrial()
    build_search_space(trial, strategy_id="x", overrides=overrides)
    kind, name, args = _calls_for(trial, "dma_exit_window")
    assert kind == "categorical"
    assert args == (11, 20)


def test_build_search_space_falls_back_to_defaults_when_not_overridden() -> None:
    overrides = SearchSpaceOverride.from_dict({"stop_pct": {"low": 0.02, "high": 0.05}})
    trial = _FakeTrial()
    build_search_space(trial, strategy_id="x", overrides=overrides)
    # stop_atr_multiple was NOT overridden — should use default (1.5, 3.5).
    _, _, args = _calls_for(trial, "stop_atr_multiple")
    assert args == (KNOWN_PARAMS["stop_atr_multiple"].default_low, KNOWN_PARAMS["stop_atr_multiple"].default_high)


def test_build_search_space_without_overrides_matches_defaults() -> None:
    """Backwards compat: calling without overrides= must use defaults for every param."""
    trial = _FakeTrial()
    build_search_space(trial, strategy_id="x")
    # Every parameter in KNOWN_PARAMS must have been sampled once.
    sampled_names = {c[1] for c in trial.calls}
    assert sampled_names == set(KNOWN_PARAMS.keys())
    # Spot-check a float default.
    _, _, args = _calls_for(trial, "stop_pct")
    assert args == (
        KNOWN_PARAMS["stop_pct"].default_low,
        KNOWN_PARAMS["stop_pct"].default_high,
    )


def test_build_search_space_none_overrides_is_safe() -> None:
    """``overrides=None`` is the default; sanity-check it doesn't blow up."""
    trial = _FakeTrial()
    build_search_space(trial, strategy_id="x", overrides=None)
    assert len(trial.calls) == len(KNOWN_PARAMS)


# ---------------------------------------------------------------------------
# load_recipe end-to-end: search_space block round-trips through the YAML
# ---------------------------------------------------------------------------


_RECIPE_WITHOUT_SEARCH_SPACE = """
name: demo
strategy_id: demo
baseline_pack_path: demo_v1
from_date: "2024-01-01"
to_date: "2024-06-01"
"""

_RECIPE_WITH_SEARCH_SPACE = """
name: demo
strategy_id: demo
baseline_pack_path: demo_v1
from_date: "2024-01-01"
to_date: "2024-06-01"
search_space:
  stop_pct:
    low: 0.02
    high: 0.05
  stop_method:
    choices: ["atr", "percent"]
  dma_exit_window:
    choices: [11, 20]
"""


def test_load_recipe_no_search_space_section_keeps_none(tmp_path: Path) -> None:
    p = tmp_path / "r.yaml"
    p.write_text(_RECIPE_WITHOUT_SEARCH_SPACE)
    recipe = load_recipe(p)
    assert recipe.search_space is None


def test_load_recipe_parses_search_space_section(tmp_path: Path) -> None:
    p = tmp_path / "r.yaml"
    p.write_text(_RECIPE_WITH_SEARCH_SPACE)
    recipe = load_recipe(p)
    assert recipe.search_space is not None
    assert recipe.search_space.overrides["stop_pct"] == FloatBound(0.02, 0.05)
    assert recipe.search_space.overrides["stop_method"].choices == ("atr", "percent")
    assert recipe.search_space.overrides["dma_exit_window"].choices == (11, 20)


def test_load_recipe_with_unknown_param_fails_fast(tmp_path: Path) -> None:
    p = tmp_path / "r.yaml"
    p.write_text(
        _RECIPE_WITHOUT_SEARCH_SPACE
        + "\nsearch_space:\n  bogus_param:\n    low: 0\n    high: 1\n"
    )
    with pytest.raises(ValueError, match="unknown parameter 'bogus_param'"):
        load_recipe(p)
