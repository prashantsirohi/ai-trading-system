"""Pattern detection helpers for research and operational workflows."""

from ai_trading_system.analytics.patterns.contracts import (
    PatternBacktestConfig,
    PatternEvent,
    PatternScanConfig,
    PatternSignal,
    PatternTrade,
)
from ai_trading_system.domains.ranking.patterns.cache import PatternCacheStore
from ai_trading_system.analytics.patterns.evaluation import (
    build_pattern_events,
    build_pattern_signals,
    ensure_pattern_event_chart,
    render_pattern_review,
    run_pattern_backtest,
)

__all__ = [
    "PatternBacktestConfig",
    "PatternEvent",
    "PatternScanConfig",
    "PatternSignal",
    "PatternTrade",
    "PatternCacheStore",
    "build_pattern_events",
    "build_pattern_signals",
    "ensure_pattern_event_chart",
    "run_pattern_backtest",
    "render_pattern_review",
]
