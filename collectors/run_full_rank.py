"""Compatibility shim for canonical full ranking runner.

Canonical module:
- ai_trading_system.domains.ranking.run_full_rank
"""

from ai_trading_system.domains.ranking.run_full_rank import *  # noqa: F401,F403
from ai_trading_system.domains.ranking import run_full_rank as _run_full_rank
import sys as _sys

_sys.modules[__name__] = _run_full_rank

if __name__ == "__main__":
    _run_full_rank.main()
