from ai_trading_system.domains.ingest.trust import *  # noqa
from ai_trading_system.domains.ingest import trust as _trust
import sys as _sys

_sys.modules[__name__] = _trust
