"""Dashboard package with lightweight lazy exports."""

from __future__ import annotations

__all__ = ["load_features", "plot_candlestick_with_features", "StockRanker"]


def __getattr__(name: str):
    if name in {"load_features", "plot_candlestick_with_features", "StockRanker"}:
        from ui.research.app import StockRanker, load_features, plot_candlestick_with_features

        exports = {
            "load_features": load_features,
            "plot_candlestick_with_features": plot_candlestick_with_features,
            "StockRanker": StockRanker,
        }
        return exports[name]
    raise AttributeError(name)
