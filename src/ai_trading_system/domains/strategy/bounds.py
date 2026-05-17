"""Search-space construction for Optuna trials.

Phase 1/3 narrow scope:
- Ranking weights (one float per FACTOR_KEYS, constrained to sum to 1.0 via
  Dirichlet-style sampling + normalisation).
- Risk knobs already wired through ``RiskPolicyConfig``: stop, exit, sizing,
  constraints.

Each knob has explicit bounds; nothing is unbounded. Optuna's TPE sampler
explores within these. Add new knobs deliberately — every search-space field
is also a fitness degree-of-freedom.

Wave 4 adds **recipe-level overrides**: the recipe YAML may narrow any
parameter's bounds via a ``search_space:`` section (validated in
``research.optimization.recipe.SearchSpaceOverride``). Defaults below are
the single source of truth — both for the run-time fallback when no
override is supplied, and for the recipe-side validator (which rejects
unknown parameter names and categorical choices that aren't a subset of
the defaults).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from ai_trading_system.domains.strategy.rule_pack import FACTOR_KEYS, StrategyRulePack

if TYPE_CHECKING:
    import optuna


ParamKind = Literal["float", "int", "categorical_str", "categorical_int"]


@dataclass(frozen=True)
class ParamSpec:
    """Description of one search-space parameter.

    Used by ``build_search_space`` (to choose the right ``trial.suggest_*``
    call) and by ``SearchSpaceOverride.from_dict`` (to type-check overrides
    coming from the recipe YAML).
    """

    kind: ParamKind
    # Numeric defaults: low/high range. ``None`` for categorical params.
    default_low: float | int | None = None
    default_high: float | int | None = None
    # Categorical defaults: tuple of allowed values. ``None`` for numeric params.
    default_choices: tuple[Any, ...] | None = None


# Single source of truth for the parameter surface area. Recipe overrides
# may narrow these but not introduce new keys (typos must fail loudly).
KNOWN_PARAMS: dict[str, ParamSpec] = {
    # Ranking factor weights — one float per FACTOR_KEYS, normalised to sum=1.
    **{f"w_{k}": ParamSpec("float", 0.0, 1.0) for k in FACTOR_KEYS},
    # Stop.
    "stop_method": ParamSpec(
        "categorical_str", default_choices=("atr", "percent", "swing_low")
    ),
    "stop_atr_multiple": ParamSpec("float", 1.5, 3.5),
    "stop_pct": ParamSpec("float", 0.04, 0.12),
    # Exit.
    "dma_exit_window": ParamSpec("categorical_int", default_choices=(11, 20, 50)),
    "max_hold_rank": ParamSpec("int", 30, 80),
    "time_stop_days": ParamSpec("int", 20, 90),
    "rank_deterioration_bars": ParamSpec("int", 2, 6),
    # Sizing.
    "risk_per_trade_pct": ParamSpec("float", 0.5, 1.5),
    "max_position_pct": ParamSpec("float", 6.0, 15.0),
    # Constraints.
    "max_concurrent_positions": ParamSpec("int", 6, 20),
    "max_sector_exposure_pct": ParamSpec("float", 20.0, 40.0),
}


# Override container types live in ``research.optimization.recipe`` to keep the
# recipe layer's validation logic self-contained. We just need a duck-typed
# accessor here: ``overrides.overrides[name]`` returns one of the override
# dataclasses below, or the lookup misses and we fall back to defaults.
# Importing the concrete types would create a cycle (recipe -> bounds for
# KNOWN_PARAMS; bounds -> recipe for the types). Treat them structurally.


def _suggest_float(
    trial: "optuna.Trial", name: str, overrides: dict
) -> float:
    spec = KNOWN_PARAMS[name]
    o = overrides.get(name)
    if o is not None:
        return trial.suggest_float(name, o.low, o.high)
    return trial.suggest_float(name, float(spec.default_low), float(spec.default_high))


def _suggest_int(
    trial: "optuna.Trial", name: str, overrides: dict
) -> int:
    spec = KNOWN_PARAMS[name]
    o = overrides.get(name)
    if o is not None:
        return trial.suggest_int(name, o.low, o.high)
    return trial.suggest_int(name, int(spec.default_low), int(spec.default_high))


def _suggest_categorical(
    trial: "optuna.Trial", name: str, overrides: dict
) -> Any:
    spec = KNOWN_PARAMS[name]
    o = overrides.get(name)
    if o is not None:
        return trial.suggest_categorical(name, list(o.choices))
    return trial.suggest_categorical(name, list(spec.default_choices or ()))


def build_search_space(
    trial: "optuna.Trial",
    *,
    strategy_id: str,
    overrides: Any | None = None,
) -> StrategyRulePack:
    """Sample one ``StrategyRulePack`` from a constrained search space.

    ``overrides`` is an optional ``SearchSpaceOverride`` from the recipe;
    when ``None`` (or its ``overrides`` dict is empty), every parameter uses
    the defaults in ``KNOWN_PARAMS``. When supplied, per-parameter overrides
    replace the defaults; any parameter not in the override map still uses
    its default.

    Backwards compatible: callers that pass only ``trial`` and ``strategy_id``
    (e.g. pre-Wave-4 code) behave identically to before.
    """
    o_map: dict = getattr(overrides, "overrides", {}) if overrides is not None else {}

    # Ranking weights: sample one float per factor, then normalise to sum 1.0.
    raw_weights = {
        key: _suggest_float(trial, f"w_{key}", o_map) for key in FACTOR_KEYS
    }
    total = sum(raw_weights.values())
    if total <= 0:
        # Degenerate sample — fall back to equal weights.
        weights = {k: 1.0 / len(FACTOR_KEYS) for k in FACTOR_KEYS}
    else:
        weights = {k: v / total for k, v in raw_weights.items()}

    risk = {
        "stop": {
            "method": _suggest_categorical(trial, "stop_method", o_map),
            "atr_multiple": _suggest_float(trial, "stop_atr_multiple", o_map),
            "stop_pct": _suggest_float(trial, "stop_pct", o_map),
        },
        "exit": {
            "dma_exit_window": _suggest_categorical(trial, "dma_exit_window", o_map),
            "max_hold_rank": _suggest_int(trial, "max_hold_rank", o_map),
            "time_stop_days": _suggest_int(trial, "time_stop_days", o_map),
            "rank_deterioration_bars": _suggest_int(
                trial, "rank_deterioration_bars", o_map
            ),
        },
        "sizing": {
            "risk_per_trade_pct": _suggest_float(trial, "risk_per_trade_pct", o_map),
            "max_position_pct": _suggest_float(trial, "max_position_pct", o_map),
        },
        "constraints": {
            "max_concurrent_positions": _suggest_int(
                trial, "max_concurrent_positions", o_map
            ),
            "max_sector_exposure_pct": _suggest_float(
                trial, "max_sector_exposure_pct", o_map
            ),
        },
    }

    return StrategyRulePack(
        strategy_id=strategy_id,
        ranking={"weights": weights},
        risk=risk,
    )


__all__ = [
    "KNOWN_PARAMS",
    "ParamKind",
    "ParamSpec",
    "build_search_space",
]
