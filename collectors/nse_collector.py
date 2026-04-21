from ai_trading_system.domains.ingest.providers.nse import *  # noqa
from ai_trading_system.domains.ingest.providers import nse as _nse
import sys as _sys

_sys.modules[__name__] = _nse
