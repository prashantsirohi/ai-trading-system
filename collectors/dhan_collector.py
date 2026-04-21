from ai_trading_system.domains.ingest.providers.dhan import *  # noqa
from ai_trading_system.domains.ingest.providers import dhan as _dhan
import sys as _sys

_sys.modules[__name__] = _dhan
