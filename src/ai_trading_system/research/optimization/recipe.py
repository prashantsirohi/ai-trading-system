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
