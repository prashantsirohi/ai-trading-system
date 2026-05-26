"""Wraps research_loader + engine_runner. Takes a RulePack, returns a result."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from functools import lru_cache
from pathlib import Path

import pandas as pd

from ai_trading_system.analytics.regime import (
    MarketRegimeSnapshot,
    RegimeProfile,
    compute_market_regime_snapshot,
    load_regime_profile,
)
from ai_trading_system.domains.strategy import (
    StrategyRulePack,
    to_ranking_weights,
    to_risk_policy_config,
)
from ai_trading_system.domains.risk.config import RiskPolicyConfig
from ai_trading_system.research.backtesting import EngineBacktestRunner
from ai_trading_system.research.backtesting.engine_runner import BacktestResult
from ai_trading_system.research.backtesting.research_loader import (
    DEFAULT_BENCHMARK_SYMBOL,
    load_research_ranked_by_date,
)


def run_backtest(
    pack: StrategyRulePack,
    *,
    project_root: Path | str,
    from_date: date,
    to_date: date,
    exchange: str = "NSE",
    benchmark_symbol: str = DEFAULT_BENCHMARK_SYMBOL,
    benchmark_source: str = "index_catalog",
    starting_equity: float = 1_000_000.0,
    commission_bps: float = 10.0,
    slippage_bps: float = 20.0,
    regime_rules_path: str | None = None,
    regime_profile_path: str | None = None,
    benchmark_return_pct: float | None = None,
) -> BacktestResult:
    """Run the engine-driven research backtest under the given rule pack."""
    ranked_by_date = load_research_ranked_by_date(
        project_root,
        from_date=from_date,
        to_date=to_date,
        exchange=exchange,
        benchmark_symbol=benchmark_symbol,
        benchmark_source=benchmark_source,
        weights_override=to_ranking_weights(pack),
    )
    base_risk_config = to_risk_policy_config(pack)
    profile_by_date: dict[date, RegimeProfile] = {}
    if regime_profile_path:
        ranked_by_date, profile_by_date = _apply_regime_rank_controls(
            ranked_by_date,
            project_root=Path(project_root),
            regime_rules_path=regime_rules_path,
            regime_profile_path=regime_profile_path,
            exchange=exchange,
        )
    runner = EngineBacktestRunner(
        risk_config=base_risk_config,
        starting_equity=starting_equity,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        risk_config_by_date={
            d: _risk_config_from_profile(base_risk_config, profile)
            for d, profile in profile_by_date.items()
        },
    )
    return runner.run(ranked_by_date)


def _apply_regime_rank_controls(
    ranked_by_date: dict[date, pd.DataFrame],
    *,
    project_root: Path,
    regime_rules_path: str | None,
    regime_profile_path: str,
    exchange: str,
) -> tuple[dict[date, pd.DataFrame], dict[date, RegimeProfile]]:
    profile_by_date: dict[date, RegimeProfile] = {}
    output: dict[date, pd.DataFrame] = {}
    from ai_trading_system.platform.db.paths import get_domain_paths

    db_path = get_domain_paths(project_root=project_root, data_domain="research").ohlcv_db_path
    for as_of, frame in ranked_by_date.items():
        snapshot = _cached_snapshot(
            str(db_path),
            str(project_root),
            str(regime_rules_path or ""),
            str(as_of),
            exchange,
        )
        profile = load_regime_profile(
            snapshot.regime,
            project_root=project_root,
            profile_path=regime_profile_path,
        )
        if profile is None:
            output[as_of] = frame
            continue
        profile_by_date[as_of] = profile
        output[as_of] = _filter_rank_frame(frame, profile)
    return output, profile_by_date


@lru_cache(maxsize=4096)
def _cached_snapshot(
    db_path: str,
    project_root: str,
    regime_rules_path: str,
    as_of: str,
    exchange: str,
) -> MarketRegimeSnapshot:
    return compute_market_regime_snapshot(
        db_path,
        as_of=as_of,
        project_root=project_root,
        rules_path=regime_rules_path or None,
        exchange=exchange,
    )


def _filter_rank_frame(frame: pd.DataFrame, profile: RegimeProfile) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame
    output = frame.copy()
    score_col = "composite_score_adjusted" if "composite_score_adjusted" in output.columns else "composite_score"
    if score_col in output.columns:
        output = output.loc[pd.to_numeric(output[score_col], errors="coerce").fillna(0.0) >= profile.min_score].copy()
    if "eligible_rank" in output.columns:
        output = output.sort_values("eligible_rank", kind="stable")
    elif "rank" in output.columns:
        output = output.sort_values("rank", kind="stable")
    return output.head(profile.rank_top_n).reset_index(drop=True)


def _risk_config_from_profile(base: RiskPolicyConfig, profile: RegimeProfile) -> RiskPolicyConfig:
    return replace(
        base,
        name=f"{base.name}_{profile.name}_{profile.regime}",
        stop=replace(base.stop, atr_multiple=profile.atr_stop_mult),
        constraints=replace(
            base.constraints,
            max_concurrent_positions=profile.max_positions,
            max_stock_weight_pct=profile.max_single_stock_weight * 100.0,
            max_sector_exposure_pct=profile.max_sector_exposure * 100.0,
        ),
        sizing=replace(
            base.sizing,
            max_position_pct=profile.max_single_stock_weight * 100.0,
        ),
    )
