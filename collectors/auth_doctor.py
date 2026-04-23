"""Compatibility shim for canonical auth doctor utility.

Canonical module:
- ai_trading_system.domains.ingest.auth_doctor
"""

from ai_trading_system.domains.ingest.auth_doctor import *  # noqa: F401,F403
from ai_trading_system.domains.ingest import auth_doctor as _auth_doctor
import sys as _sys

_sys.modules[__name__] = _auth_doctor

if __name__ == "__main__":
    _auth_doctor.main()
