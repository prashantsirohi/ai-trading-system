"""Compatibility shim for canonical reset/re-ingest/validate utilities.

Canonical module:
- ai_trading_system.domains.ingest.reset_reingest_validate
"""

from ai_trading_system.domains.ingest.reset_reingest_validate import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import reset_reingest_validate as _reset_reingest_validate
import sys as _sys

_sys.modules[__name__] = _reset_reingest_validate

if __name__ == "__main__":
    _reset_reingest_validate.main()
