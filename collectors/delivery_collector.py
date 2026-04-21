from ai_trading_system.domains.ingest.delivery import *  # noqa
from ai_trading_system.domains.ingest import delivery as _delivery
import sys as _sys

_sys.modules[__name__] = _delivery
