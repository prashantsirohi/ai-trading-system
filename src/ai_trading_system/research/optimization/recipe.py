"""OptimizationRecipe: declarative config for one Optuna study.

Mirrors the ``ResearchRecipe`` pattern in ``ai_trading_system.research.recipes``.
Every knob the runner reads lives here so studies are reproducible from one
YAML file.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml

from ai_trading_system.research.optimization.acceptance import AcceptanceThresholds
from ai_trading_system.research.optimization.evaluator import FitnessWeights


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Benchmark:
    """Generic benchmark config. Replaces a flat ``benchmark_symbol`` string.

    ``source`` decides where the symbol's price series is loaded from:
    - ``"index_catalog"`` (default): ``_index_catalog WHERE index_code = symbol``.
    - ``"catalog"``: ``_catalog WHERE symbol_id = symbol`` (legacy stock-as-benchmark).

    ``blend`` is the weight applied to the benchmark-relative excess-return
    signal inside ranking RS blending. 0.0 disables the blend.
    """

    symbol: str = "UNIV_TOP1000"
    source: Literal["index_catalog", "catalog"] = "index_catalog"
    blend: float = 0.35


@dataclass(frozen=True)
class WalkForwardConfig:
    train_months: int = 12
    validation_months: int = 3
    step_months: int = 3


@dataclass(frozen=True)
class StoppingConfig:
    max_trials: int = 50
    patience: int = 8
    max_runtime_minutes: int = 120


# --- Search-space overrides (Wave 4) --------------------------------------
#
# Recipe YAML may narrow any parameter's bounds via a ``search_space:`` block.
# Validation lives here so the recipe loader catches typos/bad shapes at load
# time, well before any Optuna trial runs.


@dataclass(frozen=True)
class FloatBound:
    low: float
    high: float


@dataclass(frozen=True)
class IntBound:
    low: int
    high: int


@dataclass(frozen=True)
class CategoricalChoices:
    """Narrowed list of allowed values for a categorical parameter.

    Recipes can only narrow (subset of defaults), never widen — widening a
    categorical set introduces a new value the engine may not understand,
    which would be a silent footgun.
    """

    choices: tuple


SearchSpaceValue = FloatBound | IntBound | CategoricalChoices


@dataclass(frozen=True)
class SearchSpaceOverride:
    """Per-parameter overrides for the Optuna search space.

    Built by ``from_dict`` against the ``KNOWN_PARAMS`` whitelist in
    ``ai_trading_system.domains.strategy.bounds``. Unknown parameter names
    are a hard error: the typo would otherwise silently leave the default
    bounds in place and the operator would never know.
    """

    overrides: dict[str, SearchSpaceValue]

    @classmethod
    def from_dict(cls, payload: dict | None) -> "SearchSpaceOverride | None":
        if not payload:
            return None
        # Lazy import to avoid a tight cycle between recipe / bounds. ``bounds``
        # itself does not import optuna at runtime (only behind TYPE_CHECKING),
        # so this stays light.
        from ai_trading_system.domains.strategy.bounds import KNOWN_PARAMS

        result: dict[str, SearchSpaceValue] = {}
        known = sorted(KNOWN_PARAMS)
        for key, raw in payload.items():
            if key not in KNOWN_PARAMS:
                raise ValueError(
                    f"search_space: unknown parameter {key!r}. "
                    f"Known parameters: {known}"
                )
            spec = KNOWN_PARAMS[key]
            if not isinstance(raw, dict):
                raise ValueError(
                    f"search_space[{key!r}] must be a mapping (got {type(raw).__name__})"
                )
            if spec.kind in ("float", "int"):
                if "low" not in raw or "high" not in raw:
                    raise ValueError(
                        f"search_space[{key!r}] requires keys 'low' and 'high' for {spec.kind} parameter"
                    )
                low, high = raw["low"], raw["high"]
                if low >= high:
                    raise ValueError(
                        f"search_space[{key!r}]: low ({low}) must be < high ({high})"
                    )
                if spec.kind == "float":
                    result[key] = FloatBound(low=float(low), high=float(high))
                else:
                    result[key] = IntBound(low=int(low), high=int(high))
            else:  # categorical_str | categorical_int
                if "choices" not in raw:
                    raise ValueError(
                        f"search_space[{key!r}] requires key 'choices' for {spec.kind} parameter"
                    )
                raw_choices = raw["choices"]
                if not isinstance(raw_choices, (list, tuple)) or len(raw_choices) == 0:
                    raise ValueError(
                        f"search_space[{key!r}].choices must be a non-empty list"
                    )
                allowed = set(spec.default_choices or ())
                bad = [c for c in raw_choices if c not in allowed]
                if bad:
                    raise ValueError(
                        f"search_space[{key!r}].choices contains values not in defaults "
                        f"{sorted(allowed)}: {bad}"
                    )
                result[key] = CategoricalChoices(choices=tuple(raw_choices))
        return cls(overrides=result)


@dataclass(frozen=True)
class OptimizationRecipe:
    name: str
    strategy_id: str
    baseline_pack_path: str
    from_date: date
    to_date: date
    exchange: str = "NSE"
    benchmark: Benchmark = field(default_factory=Benchmark)
    starting_equity: float = 1_000_000.0
    commission_bps: float = 10.0
    slippage_bps: float = 35.0  # plan recommends 35 for Indian mid-caps
    seed: int = 42
    walkforward: WalkForwardConfig = field(default_factory=WalkForwardConfig)
    fitness_weights: FitnessWeights = field(default_factory=FitnessWeights)
    acceptance: AcceptanceThresholds = field(default_factory=AcceptanceThresholds)
    stopping: StoppingConfig = field(default_factory=StoppingConfig)
    description: str = ""
    # Wave 4: optional recipe-level search-space overrides. ``None`` = use
    # the hardcoded defaults from ``bounds.KNOWN_PARAMS``.
    search_space: SearchSpaceOverride | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "OptimizationRecipe":
        def _coerce_date(value: Any) -> date:
            if isinstance(value, date):
                return value
            return date.fromisoformat(str(value))

        def _section(section_cls, raw: dict | None):
            if not raw:
                return section_cls()
            from dataclasses import fields as _fields
            allowed = {f.name for f in _fields(section_cls)}
            return section_cls(**{k: v for k, v in raw.items() if k in allowed})

        def _parse_benchmark(raw: Any) -> Benchmark:
            # Structured form: benchmark: {symbol, source, blend}
            if isinstance(raw, dict):
                return _section(Benchmark, raw)
            # Legacy flat form: benchmark_symbol: NAME — accepted for one release.
            legacy = payload.get("benchmark_symbol")
            if legacy:
                logger.warning(
                    "OptimizationRecipe: 'benchmark_symbol: %s' is deprecated; "
                    "use structured 'benchmark: {symbol, source, blend}'",
                    legacy,
                )
                return Benchmark(symbol=str(legacy))
            return Benchmark()

        return cls(
            name=str(payload["name"]),
            strategy_id=str(payload["strategy_id"]),
            baseline_pack_path=str(payload["baseline_pack_path"]),
            from_date=_coerce_date(payload["from_date"]),
            to_date=_coerce_date(payload["to_date"]),
            exchange=str(payload.get("exchange", "NSE")),
            benchmark=_parse_benchmark(payload.get("benchmark")),
            starting_equity=float(payload.get("starting_equity", 1_000_000.0)),
            commission_bps=float(payload.get("commission_bps", 10.0)),
            slippage_bps=float(payload.get("slippage_bps", 35.0)),
            seed=int(payload.get("seed", 42)),
            walkforward=_section(WalkForwardConfig, payload.get("walkforward")),
            fitness_weights=_section(FitnessWeights, payload.get("fitness_weights")),
            acceptance=_section(AcceptanceThresholds, payload.get("acceptance")),
            stopping=_section(StoppingConfig, payload.get("stopping")),
            description=str(payload.get("description", "")),
            search_space=SearchSpaceOverride.from_dict(payload.get("search_space")),
        )


def load_recipe(path: Path | str) -> OptimizationRecipe:
    payload = yaml.safe_load(Path(path).read_text()) or {}
    return OptimizationRecipe.from_dict(payload)


def resolve_baseline_path(
    baseline_pack_path: str,
    *,
    project_root: Path | str | None = None,
) -> Path:
    """Resolve a recipe's ``baseline_pack_path`` to an absolute Path.

    - If the value contains a path separator or ends in ``.yaml``/``.yml``,
      treat as a literal path (current behaviour). Absolute paths pass through;
      relative paths are resolved against ``project_root`` (or cwd).
    - Otherwise look up ``<project_root>/config/strategies/<value>.yaml``.

    This keeps the recipe YAML operator-friendly (``baseline_pack_path: momentum_breakout_v1``)
    while preserving the literal-path form used by existing recipes (which pass
    e.g. ``config/strategies/momentum_breakout_v1.yaml``).
    """
    root = Path(project_root) if project_root is not None else Path(".")
    value = baseline_pack_path
    if "/" in value or value.endswith((".yaml", ".yml")):
        p = Path(value)
        return p if p.is_absolute() else (root / p)
    return root / "config" / "strategies" / f"{value}.yaml"
