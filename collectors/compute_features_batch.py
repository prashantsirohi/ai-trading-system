"""Compatibility shim for canonical batch feature computation module.

Canonical module:
- ai_trading_system.domains.features.compute_features_batch
"""

from ai_trading_system.domains.features.compute_features_batch import *  # noqa: F401,F403
from ai_trading_system.domains.features import compute_features_batch as _compute_features_batch
import sys as _sys

_sys.modules[__name__] = _compute_features_batch

if __name__ == "__main__":
    _compute_features_batch.main()
