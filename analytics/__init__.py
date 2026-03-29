"""Lazy analytics package exports.

This avoids importing heavy optional dependencies when callers only need a
subset of analytics modules such as the pipeline registry or DQ helpers.
"""

from importlib import import_module

__all__ = [
    "RegimeDetector",
    "StockRanker",
    "AlphaEngine",
    "RiskManager",
    "EventBacktester",
    "Visualizer",
    "AIQScreener",
]

_MODULE_MAP = {
    "RegimeDetector": (".regime_detector", "RegimeDetector"),
    "StockRanker": (".ranker", "StockRanker"),
    "AlphaEngine": (".ml_engine", "AlphaEngine"),
    "RiskManager": (".risk_manager", "RiskManager"),
    "EventBacktester": (".backtester", "EventBacktester"),
    "Visualizer": (".visualizations", "Visualizer"),
    "AIQScreener": (".screener", "AIQScreener"),
}


def __getattr__(name):
    if name not in _MODULE_MAP:
        raise AttributeError(name)
    module_name, attr_name = _MODULE_MAP[name]
    module = import_module(module_name, __name__)
    return getattr(module, attr_name)
